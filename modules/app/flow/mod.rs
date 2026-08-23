//! Flow — publish acknowledgement, backpressure and consumer credit.
//!
//! One graph module composed of three components (standards
//! `fluxor-modules.md` §8):
//!
//!   - [`ack`]          — durable-publish inflight tracking keyed by
//!     `(partition_id, wal_index)`, ack emission on quorum durability,
//!     and redelivery with exponential backoff.
//!   - [`backpressure`] — admission's credit envelope translated into
//!     protocol-native backpressure signals, plus a periodic status
//!     summary to the gateway.
//!   - [`prefetch`]     — per-consumer credit windows scaled by delivery
//!     lag.
//!
//! The components share no state and exchange no messages: each closes a
//! different flow-control loop around the session state machine. What
//! they share is `forward_in` — the session's outbound bus carries both
//! MSG_ACK_REGISTER and MSG_LAG_SIGNAL, which the standalone modules
//! each read off their own copy of the same fanned edge.
//!
//! Replies leave on two ports because they land on two different inputs
//! of the session state machine: `ack_out` carries MSG_ACK_EMIT and
//! MSG_ACK_REDELIVER, `flow_out` carries MSG_BP_SIGNAL and
//! MSG_PREFETCH_CREDIT.
//!
//! ## Dispatch table
//!
//! Intra-step order is owned here. It preserves the standalone ordering
//! within the ack component — registrations, then proofs, then the ack
//! sweep — because a registration that arrives in the same step as its
//! proof must be able to ack immediately.
//!
//!   1. `forward_in` drain (≤[`FORWARD_BUDGET`]), demuxed by frame type:
//!      MSG_ACK_REGISTER → `ack::on_register` (≤16/step),
//!      MSG_LAG_SIGNAL   → `prefetch::on_lag` (≤16/step, emits the
//!      credit update inline as the standalone module did). The drain
//!      stops before reading whenever the ack table is full, so a
//!      registration is never consumed without a slot to hold it; the
//!      records stay in the channel as backpressure.
//!   2. `durability_in` drain (≤64/step) → `ack::on_durability`.
//!   3. `ack::step_acks` — emit MSG_ACK_EMIT for every inflight entry
//!      its partition's durable high-water mark has reached. Runs ONLY
//!      when `durability_in` is wired: without proofs the high-water
//!      marks never advance, and the standalone module gated the same
//!      sweep behind the same condition.
//!   4. `envelope_in` drain (≤16/step) → `backpressure::on_envelope`.
//!   5. `rejected_in` drain (≤16/step) → `backpressure::on_rejected`.
//!   6. `ack::step_timeouts` — redelivery scan, at most once per
//!      `scan_interval_ms`.
//!   7. `backpressure::step` — status to the gateway, at most once per
//!      second. `prefetch` has no periodic work.
//!   8. Metrics emission, one component-tagged frame per component,
//!      once per [`METRICS_INTERVAL_MS`].
//!
//! Step effects are reported per component at their original sites; the
//! module step itself always returns 0.

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
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/types.rs"]
mod types;

mod ack;
mod backpressure;
mod prefetch;

/// Kernel step ABI: 0=Continue, 1=Done, 2=Burst, 3=Ready. Returning
/// Burst re-runs the domain's exec rotation within the same tick, so an
/// ack raised by a durability proof reaches the session state machine in
/// this tick rather than the next. Burst only when a record was
/// consumed, so idle ticks are unchanged.
const STEP_BURST: i32 = 2;

/// Records drained from `forward_in` per step. Sized to the sum of the
/// two consumers' budgets so neither type starves the other.
const FORWARD_BUDGET: u8 = 32;

/// Largest record any of the four inputs carries, with headroom.
const READ_BUF: usize = 256;

/// Component identity stamped into the metric envelope's `metric_id`
/// field so governance's telemetry keys per component rather than by payload
/// hash (standards `fluxor-modules.md` §8 rule 8).
const METRIC_ID_ACK: u8 = 0x11;
const METRIC_ID_BACKPRESSURE: u8 = 0x12;
const METRIC_ID_PREFETCH: u8 = 0x13;

/// Telemetry cadence, matched to governance's telemetry rollup interval so
/// each component contributes exactly one sample per rollup window.
const METRICS_INTERVAL_MS: u64 = 10_000;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_forward: i32,
    in_durability: i32,
    in_envelope: i32,
    in_rejected: i32,
    out_metrics: i32,
    last_metrics_ms: u64,

    ack: ack::Ack,
    backpressure: backpressure::Backpressure,
    prefetch: prefetch::Prefetch,

    /// Module-owned read buffer. Each record is read once here and
    /// dispatched as a borrowed payload.
    buf: [u8; READ_BUF],
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

/// PIC module ABI entry: one-time process-wide init, before any instance
/// exists.
///
/// # Safety
/// `syscalls` is a kernel-owned table whose function pointers reach live
/// kernel routines for the lifetime of the process.
#[no_mangle]
#[link_section = ".text.module_init"]
pub unsafe extern "C" fn module_init(_syscalls: *const c_void) {}

/// PIC module ABI entry: construct module state in `state` (kernel-allocated
/// from the manifest-declared `state_size`).
///
/// # Safety
/// `state` / `params` / `syscalls` are kernel-owned buffers passed across the
/// module ABI. The kernel guarantees `state` is at least `state_size` bytes,
/// `params` is at least `params_len` bytes, and `state` is zero-initialised.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    // SAFETY: per the module ABI (target/fluxor/fluxor-abi/sdk/abi.rs),
    // the kernel passes a valid, exclusively-borrowed `state` of at least
    // `module_state_size()` bytes, and a `syscalls` table whose function
    // pointers reach live kernel routines. The dereferences and syscall
    // invocations below rely on those guarantees.
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<ModuleState>() {
            return -2;
        }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_forward = in_chan;
        s.in_durability = dev_channel_port(sys, 0, 1);
        s.in_envelope = dev_channel_port(sys, 0, 2);
        s.in_rejected = dev_channel_port(sys, 0, 3);
        s.out_metrics = dev_channel_port(sys, 1, 3);
        s.last_metrics_ms = 0;

        ack::init(&mut s.ack);
        backpressure::init(&mut s.backpressure);
        prefetch::init(&mut s.prefetch);

        // out[0] ack_out lands on the session's ack input; out[1]
        // flow_out on its flow input; out[2] status on the gateway.
        s.ack.out_ack = out_chan;
        s.ack.out_redeliver = out_chan;
        s.backpressure.out_signals = dev_channel_port(sys, 1, 1);
        s.prefetch.out_credits = dev_channel_port(sys, 1, 1);
        s.backpressure.out_status = dev_channel_port(sys, 1, 2);

        dev_log(sys, 3, b"[ack] init".as_ptr(), 10);
        dev_log(sys, 3, b"[bp] init".as_ptr(), 9);
        dev_log(sys, 3, b"[pfc] init".as_ptr(), 10);
        0
    }
}

/// Emit one component's metrics under the dimensional envelope
/// `[dim_flag=1][tenant:u32][protocol:u8][prg:u16][metric_id][payload]`.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
unsafe fn emit_metrics(sys: &SyscallTable, chan: i32, metric_id: u8, payload: &[u8]) {
    if chan < 0 {
        return;
    }
    let mut env = [0u8; 9 + 24];
    env[0] = 1; // dimensional
                // tenant/protocol/prg are unset here: these are node-scoped component
                // counters, not per-tenant series.
    env[8] = metric_id;
    let total = 9 + payload.len();
    if total > env.len() {
        return;
    }
    env[9..total].copy_from_slice(payload);
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let poll = (sys.channel_poll)(chan, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            wire::channel_write_msg(sys, chan, wire::MSG_METRICS, &env[..total]);
        }
    }
}

/// PIC module ABI entry: run one scheduler step against this instance.
///
/// # Safety
/// `state` is the kernel-owned buffer a prior `module_new` initialised, and is
/// exclusively borrowed for the duration of the call.
#[no_mangle]
#[link_section = ".text.module_step"]
pub unsafe extern "C" fn module_step(state: *mut u8) -> i32 {
    // SAFETY: per the module ABI (target/fluxor/fluxor-abi/sdk/abi.rs),
    // the kernel passes a valid, exclusively-borrowed `state` of at least
    // `module_state_size()` bytes, and a `syscalls` table whose function
    // pointers reach live kernel routines. The dereferences and syscall
    // invocations below rely on those guarantees.
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // ── 1: session outbound bus, demuxed to ack and prefetch ─────
        let mut registers = 0u8;
        let mut lags = 0u8;
        let mut worked = 0u32;
        if s.in_forward >= 0 {
            for _ in 0..FORWARD_BUDGET {
                // A registration read off the bus with no slot to hold
                // it would be work the broker has already accepted and
                // can no longer track. Stop draining instead: the
                // record stays in the channel, the session state
                // machine sees the edge back up, and it parks the
                // registration in the slot it reserved at admission.
                if ack::is_full(&s.ack) {
                    ack::note_full_stall(&mut s.ack);
                    break;
                }
                // Every budget decision precedes consumption. A record's
                // type is not known until it is read, so once ANY
                // per-type budget is spent this step, the drain stops
                // rather than take a record it might not be able to
                // handle. The record stays in the channel for the next
                // step; nothing is read and discarded.
                if registers >= ack::REGISTER_BUDGET || lags >= prefetch::LAG_BUDGET {
                    break;
                }
                let poll = (sys.channel_poll)(s.in_forward, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_forward, &mut s.buf);
                let plen = plen as usize;
                if plen > s.buf.len() {
                    continue;
                }
                worked += 1;
                match mt {
                    wire::MSG_ACK_REGISTER => {
                        registers += 1;
                        let _ = ack::on_register(&mut s.ack, &s.buf[..plen], now);
                    }
                    wire::MSG_LAG_SIGNAL => {
                        lags += 1;
                        prefetch::on_lag(&mut s.prefetch, sys, &s.buf[..plen]);
                    }
                    // Shared bus: traffic addressed to other consumers.
                    _ => {}
                }
            }
        }

        // ── 2-3: durability proofs, then the ack sweep ───────────────
        if s.in_durability >= 0 {
            for _ in 0..ack::DURABILITY_BUDGET {
                let poll = (sys.channel_poll)(s.in_durability, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (_, plen) = wire::channel_read_msg(sys, s.in_durability, &mut s.buf);
                let plen = plen as usize;
                if plen > s.buf.len() {
                    continue;
                }
                worked += 1;
                ack::on_durability(&mut s.ack, &s.buf[..plen]);
            }
            ack::step_acks(&mut s.ack, sys);
        }

        // ── 4: admission credit envelope ─────────────────────────────
        if s.in_envelope >= 0 {
            for _ in 0..backpressure::ENVELOPE_BUDGET {
                let poll = (sys.channel_poll)(s.in_envelope, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_envelope, &mut s.buf);
                let plen = plen as usize;
                if mt != wire::MSG_THROTTLE_ENVELOPE || plen > s.buf.len() {
                    continue;
                }
                backpressure::on_envelope(&mut s.backpressure, &s.buf[..plen]);
            }
        }

        // ── 5: rejected proposals → protocol-native signals ──────────
        if s.in_rejected >= 0 {
            for _ in 0..backpressure::REJECTED_BUDGET {
                let poll = (sys.channel_poll)(s.in_rejected, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (_, plen) = wire::channel_read_msg(sys, s.in_rejected, &mut s.buf);
                let plen = plen as usize;
                if plen == 0 || plen > s.buf.len() {
                    continue;
                }
                worked += 1;
                backpressure::on_rejected(&mut s.backpressure, sys, &s.buf[..plen]);
            }
        }

        // ── 6-7: periodic work ───────────────────────────────────────
        ack::step_timeouts(&mut s.ack, sys, now);
        backpressure::step(&mut s.backpressure, sys, now);

        // ── 8: component-tagged telemetry ────────────────────────────
        if now.wrapping_sub(s.last_metrics_ms) >= METRICS_INTERVAL_MS {
            s.last_metrics_ms = now;
            let mut m = [0u8; 28];
            let n = ack::metrics(&s.ack, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_ACK, &m[..n]);
            let n = backpressure::metrics(&s.backpressure, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_BACKPRESSURE, &m[..n]);
            let n = prefetch::metrics(&s.prefetch, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_PREFETCH, &m[..n]);
        }

        if worked > 0 {
            STEP_BURST
        } else {
            0
        }
    }
}
