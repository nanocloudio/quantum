//! prefetch — per-consumer credit windows driven by delivery lag.
//!
//! Maintains a credit window per subscribing session. Credit halves when
//! the consumer's backlog exceeds `lag_threshold` and recovers one at a
//! time up to `prefetch_max`, so a slow consumer throttles itself without
//! stalling the shared delivery path.
//!
//! ## Per-step bound
//!
//! `on_lag` handles ONE signal per call ([`LAG_BUDGET`] per step) and
//! emits at most one credit update for it.

use super::abi::SyscallTable;
use super::wire;

const MAX_CONSUMERS: usize = 1024;

/// Lag signals admitted per step. Matches the standalone drain bound.
pub const LAG_BUDGET: u8 = 16;

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
        Self {
            session_slot: 0,
            credit: 10,
            prefetch_max: 10,
            active: 0,
        }
    }
}

#[repr(C)]
pub struct Prefetch {
    pub out_credits: i32,

    default_prefetch: u32,
    adaptive: u8,
    lag_threshold: u32,

    consumers: [ConsumerCredit; MAX_CONSUMERS],
    credit_updates: u32,
}

pub fn init(p: &mut Prefetch) {
    p.out_credits = -1;
    p.default_prefetch = 10;
    p.adaptive = 1;
    // Trigger reduction once the consumer's backlog (sub_outstanding,
    // capped by `credit` ≤ default_prefetch) is within ~80% of its
    // current cap. A 1000-entry threshold could never be reached when the
    // cap itself is 10, leaving the adaptive halving unreachable.
    p.lag_threshold = (p.default_prefetch * 4) / 5;
    p.credit_updates = 0;
    for i in 0..MAX_CONSUMERS {
        p.consumers[i] = ConsumerCredit::zero();
    }
}

/// MSG_LAG_SIGNAL: `[session_slot:u32][lag:u32]`. Rescales the
/// consumer's credit and emits MSG_PREFETCH_CREDIT
/// `[session_slot:u32][credit:u32]`.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_lag(p: &mut Prefetch, sys: &SyscallTable, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let slot = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let lag = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);

    let mut found: Option<usize> = None;
    for i in 0..MAX_CONSUMERS {
        if p.consumers[i].active == 1 && p.consumers[i].session_slot == slot {
            found = Some(i);
            break;
        }
    }
    if found.is_none() {
        for i in 0..MAX_CONSUMERS {
            if p.consumers[i].active == 0 {
                p.consumers[i] = ConsumerCredit {
                    session_slot: slot,
                    credit: p.default_prefetch,
                    prefetch_max: p.default_prefetch,
                    active: 1,
                };
                found = Some(i);
                break;
            }
        }
    }
    let Some(i) = found else {
        return;
    };

    // Adaptive scaling: halve credit when lag exceeds the threshold,
    // recover one at a time otherwise.
    if p.adaptive == 1 {
        if lag > p.lag_threshold {
            p.consumers[i].credit = (p.consumers[i].credit / 2).max(1);
        } else if p.consumers[i].credit < p.consumers[i].prefetch_max {
            p.consumers[i].credit = (p.consumers[i].credit + 1).min(p.consumers[i].prefetch_max);
        }
    }
    p.credit_updates = p.credit_updates.wrapping_add(1);

    let mut cm = [0u8; 8];
    cm[0..4].copy_from_slice(&slot.to_le_bytes());
    cm[4..8].copy_from_slice(&p.consumers[i].credit.to_le_bytes());
    if p.out_credits >= 0 {
        // SAFETY: caller guarantees `sys` is live.
        unsafe {
            let poll_out = (sys.channel_poll)(p.out_credits, 0x02);
            if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, p.out_credits, wire::MSG_PREFETCH_CREDIT, &cm);
            }
        }
    }
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(p: &Prefetch, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&p.credit_updates.to_le_bytes());
    let mut active = 0u32;
    for i in 0..MAX_CONSUMERS {
        if p.consumers[i].active == 1 {
            active += 1;
        }
    }
    m[4..8].copy_from_slice(&active.to_le_bytes());
    8
}
