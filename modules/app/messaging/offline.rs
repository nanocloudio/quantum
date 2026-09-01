//! offline — persistent per-session FIFO for disconnected subscribers.
//!
//! Holds MSG_TOPIC_DELIVER envelopes verbatim while a session is offline
//! and replays them in sequence order on reconnect, so the receiving side
//! runs the standard delivery path unchanged.
//!
//! ## Per-step bound
//!
//! `on_enqueue` handles ONE envelope per call ([`ENQUEUE_BUDGET`] per
//! step); `on_reconnect` drains ONE session per call ([`RECONNECT_BUDGET`]
//! per step) and stops early on output backpressure, parking the
//! remainder for the next tick. `step` sweeps all [`MAX_ENTRIES`] slots at
//! most once per `gc_interval_ms`.

use super::abi::SyscallTable;
use super::wire;

/// Capacity tuning: 128 inline-stored envelopes × 1024 bytes ≈ 128 KiB of
/// module state, sized to hold typical MQTT subscriber deliveries
/// (small-to-medium payloads). Larger envelopes are refused at enqueue
/// time; the publisher's durability ack still fires, but the subscriber
/// won't see that specific message after reconnect.
/// Offline-queue entries held per node, across all sessions.
///
/// 128 total was a placeholder: one reconnecting subscriber with a
/// backlog could consume the whole queue, so a second session's
/// messages were refused while the first held every slot. At ~1 KiB
/// per envelope, 4096 costs ~4 MiB.
const MAX_ENTRIES: usize = 4096;
const MAX_ENVELOPE: usize = 1024;

/// Envelopes admitted per step. Matches the standalone drain bound.
pub const ENQUEUE_BUDGET: u8 = 8;
/// Reconnect signals served per step. Matches the standalone drain bound.
pub const RECONNECT_BUDGET: u8 = 4;

#[repr(C)]
#[derive(Clone, Copy)]
struct QueueEntry {
    session_slot: u32,
    sequence: u64,
    /// Length of the stored MSG_TOPIC_DELIVER envelope.
    env_len: u16,
    enqueue_ms: u64,
    expiry_ms: u64,
    active: u8,
    /// Full delivery envelope, replayed verbatim on drain.
    env: [u8; MAX_ENVELOPE],
}

impl QueueEntry {
    const fn zero() -> Self {
        Self {
            session_slot: 0,
            sequence: 0,
            env_len: 0,
            enqueue_ms: 0,
            expiry_ms: 0,
            active: 0,
            env: [0u8; MAX_ENVELOPE],
        }
    }
}

#[repr(C)]
pub struct Offline {
    pub out_drain: i32,

    ttl_ms: u64,
    gc_interval_ms: u64,
    last_gc_ms: u64,
    next_sequence: u64,
    earliest_index: u64,

    entries: [QueueEntry; MAX_ENTRIES],
    enqueued: u32,
    drained: u32,
    expired: u32,
    /// Envelopes that didn't fit MAX_ENVELOPE or arrived with the queue
    /// full — surfaces the subscriber-side data loss the persistent-session
    /// design is supposed to prevent.
    rejected: u32,

    /// Drain scratch: `[session:u32][env_len:u16][env...]`.
    buf: [u8; MAX_ENVELOPE + 8],
}

pub fn init(o: &mut Offline) {
    o.out_drain = -1;
    o.ttl_ms = 259_200_000;
    o.gc_interval_ms = 60_000;
    o.last_gc_ms = 0;
    o.next_sequence = 1;
    o.earliest_index = 0;
    o.enqueued = 0;
    o.drained = 0;
    o.expired = 0;
    o.rejected = 0;
    for i in 0..MAX_ENTRIES {
        o.entries[i] = QueueEntry::zero();
    }
}

/// MSG_APPLY_RESET_FANOUT: drop every queued envelope. Snapshots repopulate.
pub fn on_reset(o: &mut Offline) {
    for i in 0..MAX_ENTRIES {
        o.entries[i] = QueueEntry::zero();
    }
}

/// MSG_OFFLINE_ENQUEUE: `[session:u32 LE][env_len:u16 LE][env...]`, where
/// `env` is the original MSG_TOPIC_DELIVER envelope assembled by
/// topic_engine and replayed verbatim on drain.
/// Drop every queued delivery for `session_slot`, and report how many
/// went.
///
/// Called when the session owning that slot is released because its
/// shard moved. This is a CORRECTNESS requirement, not housekeeping:
/// slots are recycled, so a queue left behind on a released slot would
/// be drained to whatever session is allocated that slot next — the
/// same recycled-identity failure the `conn_id` binding guard exists to
/// prevent, and with the same signature of one client receiving
/// another's messages.
pub fn release_slot(o: &mut Offline, session_slot: u32) -> u32 {
    let mut released = 0u32;
    for i in 0..MAX_ENTRIES {
        if o.entries[i].active == 0 || o.entries[i].session_slot != session_slot {
            continue;
        }
        o.entries[i] = QueueEntry::zero();
        released = released.wrapping_add(1);
    }
    released
}

pub fn on_enqueue(o: &mut Offline, payload: &[u8], now: u64) {
    let plen = payload.len();
    if plen < 6 {
        return;
    }
    let session = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let env_len = u16::from_le_bytes([payload[4], payload[5]]) as usize;
    if 6 + env_len > plen || env_len > MAX_ENVELOPE {
        o.rejected = o.rejected.wrapping_add(1);
        return;
    }

    for i in 0..MAX_ENTRIES {
        if o.entries[i].active == 0 {
            o.entries[i].session_slot = session;
            o.entries[i].sequence = o.next_sequence;
            o.entries[i].env_len = env_len as u16;
            o.entries[i].enqueue_ms = now;
            o.entries[i].expiry_ms = now.wrapping_add(o.ttl_ms);
            o.entries[i].active = 1;
            // Bounds verified above; ptr copy avoids the copy_from_slice
            // panic-path that the bare-metal link doesn't resolve.
            // SAFETY: `env_len <= MAX_ENVELOPE` and `6 + env_len <= plen`.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    payload.as_ptr().add(6),
                    o.entries[i].env.as_mut_ptr(),
                    env_len,
                );
            }
            o.next_sequence = o.next_sequence.wrapping_add(1);
            o.enqueued = o.enqueued.wrapping_add(1);
            return;
        }
    }
    o.rejected = o.rejected.wrapping_add(1);
}

/// MSG_OFFLINE_RECONNECT: `[session_slot:u32 LE]`. Emits every queued
/// envelope for the session in sequence order as MSG_OFFLINE_DRAIN
/// `[session:u32][env_len:u16][env...]` — the same shape as enqueue, so
/// the receiving side reuses the delivery path.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_reconnect(o: &mut Offline, sys: &SyscallTable, payload: &[u8]) {
    if payload.len() < 4 {
        return;
    }
    let session = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);

    loop {
        let mut lowest: Option<(usize, u64)> = None;
        for i in 0..MAX_ENTRIES {
            if o.entries[i].active == 1 && o.entries[i].session_slot == session {
                match lowest {
                    None => lowest = Some((i, o.entries[i].sequence)),
                    Some((_, cur)) if o.entries[i].sequence < cur => {
                        lowest = Some((i, o.entries[i].sequence));
                    }
                    _ => {}
                }
            }
        }
        let idx = match lowest {
            Some((i, _)) => i,
            None => break,
        };

        let env_len = o.entries[idx].env_len as usize;
        let total = 6 + env_len;
        if total > o.buf.len() {
            // Shouldn't reach with the constants above; bail.
            o.entries[idx].active = 0;
            continue;
        }
        o.buf[0..4].copy_from_slice(&session.to_le_bytes());
        o.buf[4..6].copy_from_slice(&(env_len as u16).to_le_bytes());
        // SAFETY: `total <= o.buf.len()` checked above.
        unsafe {
            core::ptr::copy_nonoverlapping(
                o.entries[idx].env.as_ptr(),
                o.buf.as_mut_ptr().add(6),
                env_len,
            );
        }
        let expected = (wire::ENVELOPE_HDR + total) as i32;
        // SAFETY: caller guarantees `sys` is live.
        let written = unsafe {
            wire::channel_write_msg(sys, o.out_drain, wire::MSG_OFFLINE_DRAIN, &o.buf[..total])
        };
        if written != expected {
            // Output ring backpressured. Leave the entry parked for the
            // next reconnect tick and stop draining.
            break;
        }
        o.entries[idx].active = 0;
        o.drained = o.drained.wrapping_add(1);
    }
}

/// Periodic expiry sweep; also republishes the retention floor.
pub fn step(o: &mut Offline, now: u64) {
    if now.wrapping_sub(o.last_gc_ms) < o.gc_interval_ms {
        return;
    }
    o.last_gc_ms = now;
    let mut min_seq = u64::MAX;
    for i in 0..MAX_ENTRIES {
        if o.entries[i].active == 1 {
            if o.entries[i].expiry_ms <= now {
                o.entries[i].active = 0;
                o.expired = o.expired.wrapping_add(1);
            } else if o.entries[i].sequence < min_seq {
                min_seq = o.entries[i].sequence;
            }
        }
    }
    if min_seq != u64::MAX {
        o.earliest_index = min_seq;
    }
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(o: &Offline, m: &mut [u8; super::METRIC_BYTES]) -> usize {
    m[0..4].copy_from_slice(&o.enqueued.to_le_bytes());
    m[4..8].copy_from_slice(&o.drained.to_le_bytes());
    m[8..12].copy_from_slice(&o.expired.to_le_bytes());
    m[12..20].copy_from_slice(&o.earliest_index.to_le_bytes());
    let mut active = 0u32;
    for i in 0..MAX_ENTRIES {
        if o.entries[i].active == 1 {
            active += 1;
        }
    }
    m[20..24].copy_from_slice(&active.to_le_bytes());
    24
}
