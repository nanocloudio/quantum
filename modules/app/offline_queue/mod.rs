//! Offline Queue — Persistent FIFO for disconnected/throttled sessions.
//!
//! Stores per-session message queues with expiry. Drains on reconnect.
//! Maintains earliest_offline_queue_index for WAL compaction floor.
//!
//! Foundation candidate: protocol-agnostic store-and-forward queue.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK is include!'d wholesale and each module consumes only a subset; pending upstream allow attributes in target/fluxor/fluxor-abi/sdk/"
)]

use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "see file-level allow: SDK surface is shared across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

/// Capacity tuning: 128 inline-stored envelopes × 1024 bytes ≈ 128 KiB of
/// module state, sized to hold typical MQTT subscriber deliveries
/// (small-to-medium payloads). Larger envelopes are refused at enqueue
/// time; the publisher's durability ack still fires, but the subscriber
/// won't see that specific message after reconnect.
const MAX_ENTRIES: usize = 128;
const MAX_ENVELOPE: usize = 1024;

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
            session_slot: 0, sequence: 0,
            env_len: 0,
            enqueue_ms: 0, expiry_ms: 0, active: 0,
            env: [0u8; MAX_ENVELOPE],
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_enqueue: i32,
    in_reconnect: i32,     // in[1]: reconnect signals from session_processor
    out_drain: i32,
    out_metrics: i32,

    ttl_ms: u64,
    gc_interval_ms: u64,
    last_gc_ms: u64,
    next_sequence: u64,
    earliest_index: u64,
    drain_cursor: u32,     // round-robin drain position for fairness

    entries: [QueueEntry; MAX_ENTRIES],
    enqueued: u32,
    drained: u32,
    expired: u32,
    /// Envelopes that didn't fit MAX_ENVELOPE or arrived with the queue
    /// full — surfaces the subscriber-side data loss the persistent-session
    /// design is supposed to prevent.
    rejected: u32,

    buf: [u8; MAX_ENVELOPE + 8],
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 { core::mem::size_of::<ModuleState>() as u32 }

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32, out_chan: i32, _ctrl_chan: i32,
    _params: *const u8, _params_len: usize,
    state: *mut u8, state_size: usize, syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() { return -1; }
        if state_size < core::mem::size_of::<ModuleState>() { return -2; }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_enqueue = in_chan;
        s.out_drain = out_chan;
        s.in_reconnect = dev_channel_port(sys, 0, 1);
        s.out_metrics = dev_channel_port(sys, 1, 1);
        s.ttl_ms = 259_200_000;
        s.gc_interval_ms = 60_000;
        s.next_sequence = 1;
        for i in 0..MAX_ENTRIES {
            s.entries[i] = QueueEntry::zero();
        }
        dev_log(sys, 3, b"[ofl] init".as_ptr(), 9);
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // Enqueue requests share a bus with the rest of the messaging
        // infrastructure. Body: [session:u32 LE][env_len:u16 LE][env...]
        // where env is the original MSG_TOPIC_DELIVER envelope assembled
        // by topic_engine. We replay the envelope verbatim on drain so
        // session_processor can route it through the standard delivery
        // path on reconnect.
        if s.in_enqueue >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_enqueue, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_enqueue, &mut s.buf);
                if mt == wire::MSG_APPLY_RESET_FANOUT {
                    // Phase 4 apply-pipeline reset: drop every queued
                    // envelope. Phase 5 snapshots will repopulate.
                    for i in 0..MAX_ENTRIES {
                        s.entries[i] = QueueEntry::zero();
                    }
                    continue;
                }
                if mt != wire::MSG_OFFLINE_ENQUEUE { continue; }
                let plen = plen as usize;
                if plen < 6 { continue; }

                let session = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                let env_len = u16::from_le_bytes([s.buf[4], s.buf[5]]) as usize;
                if 6 + env_len > plen || env_len > MAX_ENVELOPE {
                    s.rejected = s.rejected.wrapping_add(1);
                    continue;
                }

                let mut placed = false;
                for i in 0..MAX_ENTRIES {
                    if s.entries[i].active == 0 {
                        s.entries[i].session_slot = session;
                        s.entries[i].sequence = s.next_sequence;
                        s.entries[i].env_len = env_len as u16;
                        s.entries[i].enqueue_ms = now;
                        s.entries[i].expiry_ms = now.wrapping_add(s.ttl_ms);
                        s.entries[i].active = 1;
                        let env_ptr = s.entries[i].env.as_mut_ptr();
                        let src_ptr = s.buf.as_ptr().add(6);
                        // Bounds verified above; ptr copy avoids the
                        // copy_from_slice panic-path that the bare-metal
                        // link doesn't resolve.
                        core::ptr::copy_nonoverlapping(src_ptr, env_ptr, env_len);
                        s.next_sequence = s.next_sequence.wrapping_add(1);
                        s.enqueued = s.enqueued.wrapping_add(1);
                        placed = true;
                        break;
                    }
                }
                if !placed {
                    s.rejected = s.rejected.wrapping_add(1);
                }
            }
        }

        // Drain-on-reconnect: shared bus, filtered on MSG_OFFLINE_RECONNECT.
        // Body: [session_slot:u32 LE].
        if s.in_reconnect >= 0 {
            for _ in 0..4 {
                let poll = (sys.channel_poll)(s.in_reconnect, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_reconnect, &mut s.buf);
                if mt != wire::MSG_OFFLINE_RECONNECT { continue; }
                if plen < 4 { continue; }
                let session = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);

                // Emit every queued envelope for this session in sequence
                // order. Drain body: [session:u32][env_len:u16][env...] —
                // same shape as enqueue so session_processor can reuse the
                // delivery path on the receiving side.
                loop {
                    let mut lowest: Option<(usize, u64)> = None;
                    for i in 0..MAX_ENTRIES {
                        if s.entries[i].active == 1 && s.entries[i].session_slot == session {
                            match lowest {
                                None => lowest = Some((i, s.entries[i].sequence)),
                                Some((_, cur)) if s.entries[i].sequence < cur => {
                                    lowest = Some((i, s.entries[i].sequence));
                                }
                                _ => {}
                            }
                        }
                    }
                    let idx = match lowest { Some((i, _)) => i, None => break };

                    let env_len = s.entries[idx].env_len as usize;
                    let total = 6 + env_len;
                    if total > s.buf.len() {
                        // Shouldn't reach with the constants above; bail.
                        s.entries[idx].active = 0;
                        continue;
                    }
                    s.buf[0..4].copy_from_slice(&session.to_le_bytes());
                    s.buf[4..6].copy_from_slice(&(env_len as u16).to_le_bytes());
                    let buf_dst = s.buf.as_mut_ptr().add(6);
                    let env_src = s.entries[idx].env.as_ptr();
                    core::ptr::copy_nonoverlapping(env_src, buf_dst, env_len);
                    let expected = (wire::ENVELOPE_HDR + total) as i32;
                    let written = wire::channel_write_msg(
                        sys, s.out_drain, wire::MSG_OFFLINE_DRAIN,
                        &s.buf[..total],
                    );
                    if written != expected {
                        // Output ring backpressured. Leave the entry parked
                        // for the next reconnect tick and stop draining.
                        break;
                    }
                    s.entries[idx].active = 0;
                    s.drained = s.drained.wrapping_add(1);
                }
            }
        }

        // Periodic GC
        if now.wrapping_sub(s.last_gc_ms) >= s.gc_interval_ms {
            s.last_gc_ms = now;
            let mut min_seq = u64::MAX;
            for i in 0..MAX_ENTRIES {
                if s.entries[i].active == 1 {
                    if s.entries[i].expiry_ms <= now {
                        s.entries[i].active = 0;
                        s.expired = s.expired.wrapping_add(1);
                    } else if s.entries[i].sequence < min_seq {
                        min_seq = s.entries[i].sequence;
                    }
                }
            }
            if min_seq != u64::MAX {
                s.earliest_index = min_seq;
            }
        }

        // Metrics
        if s.out_metrics >= 0 && now.wrapping_sub(s.last_gc_ms) < 10 {
            let mut m = [0u8; 24];
            m[0..4].copy_from_slice(&s.enqueued.to_le_bytes());
            m[4..8].copy_from_slice(&s.drained.to_le_bytes());
            m[8..12].copy_from_slice(&s.expired.to_le_bytes());
            m[12..20].copy_from_slice(&s.earliest_index.to_le_bytes());
            let mut active = 0u32;
            for i in 0..MAX_ENTRIES {
                if s.entries[i].active == 1 { active += 1; }
            }
            m[20..24].copy_from_slice(&active.to_le_bytes());
            let poll = (sys.channel_poll)(s.out_metrics, 0x02);
            if poll > 0 && (poll as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, s.out_metrics, wire::MSG_METRICS, &m);
            }
        }

        0
    }
}
