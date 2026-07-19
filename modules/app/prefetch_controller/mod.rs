//! Prefetch Controller — Per-session consumer credit allocation.
//!
//! Maintains per-session credit windows. Scales credits based on
//! delivery-lag signals from session_processor. Emits updated credit
//! windows back to session_processor for consumer flow control.

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

const MAX_CONSUMERS: usize = 1024;

#[repr(C)]
#[derive(Clone, Copy)]
struct ConsumerCredit {
    session_slot: u32,
    credit: u32,
    prefetch_max: u32,
    active: u8,
}

impl ConsumerCredit {
    const fn zero() -> Self {
        Self { session_slot: 0, credit: 10, prefetch_max: 10, active: 0 }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_lag: i32,
    out_credits: i32,
    out_metrics: i32,

    default_prefetch: u32,
    adaptive: u8,
    lag_threshold: u32,

    consumers: [ConsumerCredit; MAX_CONSUMERS],
    credit_updates: u32,
    last_emit_ms: u64,
    buf: [u8; 256],
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
        s.in_lag = in_chan;
        s.out_credits = out_chan;
        s.out_metrics = dev_channel_port(sys, 1, 1);
        s.default_prefetch = 10;
        s.adaptive = 1;
        // Trigger reduction once the consumer's backlog (sub_outstanding,
        // capped by `credit` ≤ default_prefetch) is within ~80% of its
        // current cap. A 1000-entry threshold could never be reached when
        // the cap itself is 10, leaving the adaptive halving unreachable.
        s.lag_threshold = (s.default_prefetch * 4) / 5;
        for i in 0..MAX_CONSUMERS {
            s.consumers[i] = ConsumerCredit::zero();
        }
        dev_log(sys, 3, b"[pfc] init".as_ptr(), 10);
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

        // Drain delivery-lag signals: [session_slot: u32] [lag: u32]
        if s.in_lag >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_lag, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_lag, &mut s.buf);
                // lag_in shares the session_processor.forward_out bus with
                // MSG_ACK_REGISTER; only act on lag signals.
                if mt != wire::MSG_LAG_SIGNAL { continue; }
                if plen < 8 { continue; }
                let slot = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                let lag = u32::from_le_bytes([s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);

                // Find or allocate consumer slot
                let mut found: Option<usize> = None;
                for i in 0..MAX_CONSUMERS {
                    if s.consumers[i].active == 1 && s.consumers[i].session_slot == slot {
                        found = Some(i);
                        break;
                    }
                }
                if found.is_none() {
                    for i in 0..MAX_CONSUMERS {
                        if s.consumers[i].active == 0 {
                            s.consumers[i] = ConsumerCredit {
                                session_slot: slot,
                                credit: s.default_prefetch,
                                prefetch_max: s.default_prefetch,
                                active: 1,
                            };
                            found = Some(i);
                            break;
                        }
                    }
                }
                if let Some(i) = found {
                    // Adaptive scaling: halve credit when lag exceeds threshold, restore otherwise
                    if s.adaptive == 1 {
                        if lag > s.lag_threshold {
                            s.consumers[i].credit = (s.consumers[i].credit / 2).max(1);
                        } else if s.consumers[i].credit < s.consumers[i].prefetch_max {
                            s.consumers[i].credit = (s.consumers[i].credit + 1).min(s.consumers[i].prefetch_max);
                        }
                    }
                    s.credit_updates = s.credit_updates.wrapping_add(1);

                    // Emit updated credits to session_processor
                    let mut cm = [0u8; 8];
                    cm[0..4].copy_from_slice(&slot.to_le_bytes());
                    cm[4..8].copy_from_slice(&s.consumers[i].credit.to_le_bytes());
                    if s.out_credits >= 0 {
                        let poll_out = (sys.channel_poll)(s.out_credits, 0x02);
                        if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                            wire::channel_write_msg(sys, s.out_credits, wire::MSG_PREFETCH_CREDIT, &cm);
                        }
                    }
                }
            }
        }

        // Periodic metrics
        if now.wrapping_sub(s.last_emit_ms) >= 1000 && s.out_metrics >= 0 {
            s.last_emit_ms = now;
            let mut m = [0u8; 8];
            m[0..4].copy_from_slice(&s.credit_updates.to_le_bytes());
            let mut active = 0u32;
            for i in 0..MAX_CONSUMERS {
                if s.consumers[i].active == 1 { active += 1; }
            }
            m[4..8].copy_from_slice(&active.to_le_bytes());
            let poll = (sys.channel_poll)(s.out_metrics, 0x02);
            if poll > 0 && (poll as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, s.out_metrics, wire::MSG_METRICS, &m);
            }
        }

        0
    }
}
