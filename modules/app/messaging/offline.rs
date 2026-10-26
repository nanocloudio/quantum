//! offline — per-session FIFO of the deliveries a session cannot take now.
//!
//! Holds MSG_TOPIC_DELIVER envelopes verbatim while a session is offline,
//! or while a connected one is waiting on its client to acknowledge, and
//! replays them in sequence order when the worker drains the session, so
//! the receiving side runs the standard delivery path unchanged.
//!
//! ## Per-step bound
//!
//! `on_enqueue` handles ONE envelope per call ([`ENQUEUE_BUDGET`] per
//! step); `on_reconnect` asks for ONE session's drain per call
//! ([`RECONNECT_BUDGET`] per step), bounded by the credit its request
//! carries, which [`drain`] answers — on output backpressure the rest is
//! owed (`hold::Owed::Drain`) and drained on before the bus is read
//! again. `step` sweeps every entry at most once per
//! `gc_interval_ms`.
//!
//! The envelopes themselves are held by the offline queue core
//! (`offline_queue_core.rs`): any size a delivery can have, per session in
//! arrival order.

use super::abi::SyscallTable;
use super::hold;
use super::wire;

/// The queue's storage: chunked envelopes, per-session FIFOs.
mod queue {
    include!("../../common/cores/offline_queue_core.rs");
}

/// The largest envelope queued: any delivery a worker can make.
pub const MAX_ENVELOPE: usize = queue::OFFLINE_ENVELOPE_MAX;
const _: () = assert!(MAX_ENVELOPE >= wire::DELIVERY_ENVELOPE_MAX);

/// Envelopes admitted per step. Matches the standalone drain bound.
pub const ENQUEUE_BUDGET: u8 = 8;
/// Reconnect signals served per step. Matches the standalone drain bound.
pub const RECONNECT_BUDGET: u8 = 4;

pub struct Offline {
    pub out_drain: i32,

    ttl_ms: u64,
    gc_interval_ms: u64,
    last_gc_ms: u64,
    earliest_index: u64,

    q: queue::OfflineQueue,
    enqueued: u32,
    drained: u32,
    expired: u32,
    /// Envelopes refused — larger than any delivery, for a slot past the
    /// table, or with the queue full: the subscriber-side loss the
    /// persistent-session design exists to prevent, so it is counted.
    rejected: u32,

    /// Drain scratch: `[session:u32][env_len:u16][env...]`.
    buf: [u8; MAX_ENVELOPE + 6],
}

pub fn init(o: &mut Offline) {
    o.out_drain = -1;
    o.ttl_ms = 259_200_000;
    o.gc_interval_ms = 60_000;
    o.last_gc_ms = 0;
    o.earliest_index = 0;
    o.enqueued = 0;
    o.drained = 0;
    o.expired = 0;
    o.rejected = 0;
    o.q.init();
}

/// MSG_APPLY_RESET_FANOUT: drop every queued envelope. Snapshots repopulate.
pub fn on_reset(o: &mut Offline) {
    o.q.clear();
}

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
    o.q.release(session_slot)
}

/// MSG_OFFLINE_ENQUEUE: `[session:u32 LE][env_len:u16 LE][env...]`, where
/// `env` is the original MSG_TOPIC_DELIVER envelope assembled by
/// topic_engine and replayed verbatim on drain.
pub fn on_enqueue(o: &mut Offline, payload: &[u8], now: u64) {
    if payload.len() < 6 {
        return;
    }
    let session = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let env_len = usize::from(u16::from_le_bytes([payload[4], payload[5]]));
    if 6 + env_len > payload.len() {
        o.rejected = o.rejected.wrapping_add(1);
        return;
    }
    match o.q.push(
        session,
        &payload[6..6 + env_len],
        now.wrapping_add(o.ttl_ms),
    ) {
        Ok(()) => o.enqueued = o.enqueued.wrapping_add(1),
        Err(_) => o.rejected = o.rejected.wrapping_add(1),
    }
}

/// MSG_OFFLINE_RECONNECT: `[session_slot:u32 LE][max:u16 LE]`. The
/// drain it asks for — at most `max` of the session's queued envelopes,
/// in sequence order, as MSG_OFFLINE_DRAIN
/// `[session:u32][env_len:u16][env...]` (the same shape as enqueue, so the
/// receiving side reuses the delivery path), then MSG_OFFLINE_DRAINED.
/// `max` is the room the worker reserved for them. One that names its
/// session without a credit drains nothing and is answered with the end
/// alone.
pub fn on_reconnect(payload: &[u8]) -> hold::Owed {
    if payload.len() < 4 {
        return hold::Owed::None;
    }
    hold::Owed::Drain {
        session: u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]),
        left: if payload.len() >= 6 {
            u16::from_le_bytes([payload[4], payload[5]])
        } else {
            0
        },
    }
}

/// Envelopes queued for `session`.
pub fn queued(o: &Offline, session: u32) -> u32 {
    o.q.queued(session)
}

/// Emit `session`'s queued envelopes oldest first while `left` allows
/// and `out_drain` takes them. `Some(remaining)` once `left` is spent or
/// none is left; `None` when the edge stopped it — the envelope it had no
/// room for stays queued, first in line for the next call.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn drain(
    o: &mut Offline,
    sys: &SyscallTable,
    session: u32,
    left: &mut u16,
) -> Option<u32> {
    loop {
        if *left == 0 || o.out_drain < 0 {
            return Some(o.q.queued(session));
        }
        let Some(env_len) = o.q.front(session, &mut o.buf[6..]) else {
            return Some(0);
        };
        let total = 6 + env_len;
        o.buf[0..4].copy_from_slice(&session.to_le_bytes());
        o.buf[4..6].copy_from_slice(&(env_len as u16).to_le_bytes());
        // SAFETY: caller guarantees `sys` is live.
        let written = unsafe {
            let poll = (sys.channel_poll)(o.out_drain, 0x02);
            poll > 0
                && (poll as u32 & 0x02) != 0
                && wire::channel_write_msg(
                    sys,
                    o.out_drain,
                    wire::MSG_OFFLINE_DRAIN,
                    &o.buf[..total],
                ) > 0
        };
        if !written {
            return None;
        }
        o.q.pop_front(session);
        o.drained = o.drained.wrapping_add(1);
        *left -= 1;
    }
}

/// Periodic expiry sweep; also republishes the retention floor.
pub fn step(o: &mut Offline, now: u64) {
    if now.wrapping_sub(o.last_gc_ms) < o.gc_interval_ms {
        return;
    }
    o.last_gc_ms = now;
    o.expired = o.expired.wrapping_add(o.q.expire(now));
    if let Some(min_seq) = o.q.earliest() {
        o.earliest_index = min_seq;
    }
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(o: &Offline, m: &mut [u8; super::METRIC_BYTES]) -> usize {
    m[0..4].copy_from_slice(&o.enqueued.to_le_bytes());
    m[4..8].copy_from_slice(&o.drained.to_le_bytes());
    m[8..12].copy_from_slice(&o.expired.to_le_bytes());
    m[12..20].copy_from_slice(&o.earliest_index.to_le_bytes());
    m[20..24].copy_from_slice(&(o.q.live() as u32).to_le_bytes());
    24
}
