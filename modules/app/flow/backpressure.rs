//! backpressure — translates admission's credit envelope into
//! protocol-native backpressure signals.
//!
//! Tracks the current entry/byte credit envelope from admission, and
//! turns each rejected proposal into a structured MSG_BP_SIGNAL the
//! session state machine renders as the protocol's own flow-control
//! primitive. Publishes a periodic status summary back to the gateway.
//!
//! ## Per-step bound
//!
//! `on_envelope` and `on_rejected` each handle ONE frame per call
//! ([`ENVELOPE_BUDGET`] / [`REJECTED_BUDGET`] per step). `step` emits at
//! most one status frame per [`STATUS_INTERVAL_MS`].

use super::abi::SyscallTable;
use super::types::{BP_PERMANENT_DURABILITY, BP_TRANSIENT};
use super::wire;

/// Envelope updates admitted per step. The standalone module drained
/// these in an unbounded loop; the envelope is latest-wins, so deferring
/// the tail to the next step loses nothing and gives the composite a
/// bounded step (standards fluxor-modules.md §8 rule 5).
pub const ENVELOPE_BUDGET: u8 = 16;
/// Rejections admitted per step. Matches the standalone drain bound.
pub const REJECTED_BUDGET: u8 = 16;

/// Status-emission cadence to the gateway.
const STATUS_INTERVAL_MS: u64 = 1000;

#[repr(C)]
pub struct Backpressure {
    pub out_signals: i32,
    pub out_status: i32,

    pause_ack_depth: u32,
    drop_qos0_depth: u32,
    retained_buffer_bytes: u32,

    current_entry_credits: i32,
    current_byte_credits: i32,
    rejections: u32,
    signals_emitted: u32,
    last_status_ms: u64,
}

pub fn init(b: &mut Backpressure) {
    b.out_signals = -1;
    b.out_status = -1;
    b.pause_ack_depth = 10_000;
    b.drop_qos0_depth = 5_000;
    b.retained_buffer_bytes = 16 * 1024 * 1024;
    b.current_entry_credits = 0;
    b.current_byte_credits = 0;
    b.rejections = 0;
    b.signals_emitted = 0;
    b.last_status_ms = 0;
}

/// MSG_THROTTLE_ENVELOPE: `[entry_credits:i32][byte_credits:i32]`.
pub fn on_envelope(b: &mut Backpressure, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    b.current_entry_credits = i32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    b.current_byte_credits = i32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
}

/// A rejected proposal. Emits MSG_BP_SIGNAL
/// `[reason:u8][entry_credits:i32][byte_credits:i32]`; the reason
/// distinguishes a transient credit stall from a durability rejection so
/// the session state machine can pick the protocol's own signal.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_rejected(b: &mut Backpressure, sys: &SyscallTable, payload: &[u8]) {
    if payload.is_empty() {
        return;
    }
    b.rejections = b.rejections.wrapping_add(1);

    let reason = if b.current_entry_credits <= 0 || b.current_byte_credits <= 0 {
        BP_TRANSIENT
    } else {
        BP_PERMANENT_DURABILITY
    };
    let mut sig = [0u8; 9];
    sig[0] = reason;
    sig[1..5].copy_from_slice(&b.current_entry_credits.to_le_bytes());
    sig[5..9].copy_from_slice(&b.current_byte_credits.to_le_bytes());

    if b.out_signals >= 0 {
        // SAFETY: caller guarantees `sys` is live.
        unsafe {
            let poll_out = (sys.channel_poll)(b.out_signals, 0x02);
            if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, b.out_signals, wire::MSG_BP_SIGNAL, &sig);
                b.signals_emitted = b.signals_emitted.wrapping_add(1);
            }
        }
    }
}

/// Periodic status to the gateway.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn step(b: &mut Backpressure, sys: &SyscallTable, now: u64) {
    if now.wrapping_sub(b.last_status_ms) < STATUS_INTERVAL_MS || b.out_status < 0 {
        return;
    }
    b.last_status_ms = now;
    let mut m = [0u8; 16];
    m[0..4].copy_from_slice(&b.current_entry_credits.to_le_bytes());
    m[4..8].copy_from_slice(&b.current_byte_credits.to_le_bytes());
    m[8..12].copy_from_slice(&b.rejections.to_le_bytes());
    m[12..16].copy_from_slice(&b.signals_emitted.to_le_bytes());
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let poll = (sys.channel_poll)(b.out_status, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            wire::channel_write_msg(sys, b.out_status, wire::MSG_THROTTLE_ENVELOPE, &m);
        }
    }
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(b: &Backpressure, m: &mut [u8; 28]) -> usize {
    m[0..4].copy_from_slice(&b.rejections.to_le_bytes());
    m[4..8].copy_from_slice(&b.signals_emitted.to_le_bytes());
    m[8..12].copy_from_slice(&b.current_entry_credits.to_le_bytes());
    m[12..16].copy_from_slice(&b.current_byte_credits.to_le_bytes());
    16
}
