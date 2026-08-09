//! Messaging — dedup, offline queues and retained messages.
//!
//! One graph module composed of three components (standards
//! `fluxor-modules.md` §8):
//!
//!   - [`dedup`]    — sharded exactly-once dedup with expiry and a
//!     WAL-compaction retention floor.
//!   - [`offline`]  — per-session FIFO holding delivery envelopes while a
//!     subscriber is disconnected, replayed in sequence order on
//!     reconnect.
//!   - [`retained`] — MQTT retained-message store, wildcard-matched on
//!     SUBSCRIBE.
//!
//! The components share no state and exchange no messages: each serves a
//! request/response pair on behalf of the session state machine. What
//! they share is the request bus — `op_in` carries all five operation
//! types multiplexed by `msg_type`, and `result_out` carries all three
//! reply types back.
//!
//! ## Dispatch table
//!
//! One drain of `op_in` per step, at most [`OP_BUDGET`] records, demuxed
//! by frame type. Per-component budgets below are the components' own
//! declared per-step bounds; a record whose budget is exhausted is left
//! on the ring for the next step, and the drain stops so ordering within
//! a type is preserved.
//!
//!   1. MSG_APPLY_RESET_FANOUT → `dedup::on_reset`, `offline::on_reset` and
//!      `retained::on_reset`. Unbudgeted: a reset is a control frame that
//!      all three components must observe.
//!   2. MSG_DEDUP_CHECK        → `dedup::on_check`      (≤16/step)
//!   3. MSG_OFFLINE_ENQUEUE    → `offline::on_enqueue`  (≤8/step)
//!   4. MSG_OFFLINE_RECONNECT  → `offline::on_reconnect`(≤4/step)
//!   5. MSG_RETAINED_WRITE     → `retained::on_write`   (≤8/step)
//!   6. MSG_RETAINED_READ      → `retained::on_read`    (≤8/step)
//!   7. `dedup::step`   — periodic expiry sweep.
//!   8. `offline::step` — periodic expiry sweep + retention floor.
//!      `retained` has no periodic work.
//!   9. Metrics emission, one component-tagged frame per component,
//!      once per [`METRICS_INTERVAL_MS`].
//!
//! Any other frame type on the bus is ignored — `op_in` is a shared bus
//! and carries traffic addressed to other consumers.
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

mod dedup;
mod offline;
mod retained;

/// Kernel step ABI: 0=Continue, 1=Done, 2=Burst, 3=Ready. Returning
/// Burst re-runs the domain's exec rotation within the same tick, so a
/// reply reaches the session state machine in this tick rather than the
/// next. Burst only when a record was consumed, so idle ticks are
/// unchanged.
const STEP_BURST: i32 = 2;

/// Records drained from `op_in` per step. Sized to the sum of the
/// component budgets (16 + 8 + 4 + 8 + 8) so a step can satisfy every
/// component at its bound without one type starving another.
const OP_BUDGET: u8 = 44;

/// Largest record the bus carries: a retained write of a max-length
/// topic and payload, plus its fixed header.
const READ_BUF: usize = 23 + 256 + 1024;

/// Component identity stamped into the metric envelope's `metric_id`
/// field so governance's telemetry keys per component rather than by payload
/// hash (standards `fluxor-modules.md` §8 rule 8).
const METRIC_ID_DEDUP: u8 = 0x01;
const METRIC_ID_OFFLINE: u8 = 0x02;
const METRIC_ID_RETAINED: u8 = 0x03;

/// Telemetry cadence, matched to governance's telemetry rollup interval so
/// each component contributes exactly one sample per rollup window.
const METRICS_INTERVAL_MS: u64 = 10_000;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_op: i32,
    out_metrics: i32,
    last_metrics_ms: u64,

    dedup: dedup::Dedup,
    offline: offline::Offline,
    retained: retained::Retained,

    /// Module-owned read buffer. Each record is read once here and
    /// dispatched as a borrowed payload; components own only their own
    /// output scratch.
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
        s.in_op = in_chan;
        s.out_metrics = dev_channel_port(sys, 1, 1);
        s.last_metrics_ms = 0;

        dedup::init(&mut s.dedup);
        offline::init(&mut s.offline);
        retained::init(&mut s.retained);

        // All three components reply on out[0]; the consumer demuxes by
        // frame type exactly as it did across the three separate edges.
        s.dedup.out_result = out_chan;
        s.offline.out_drain = out_chan;
        s.retained.out_read = out_chan;

        dev_log(sys, 3, b"[dedup] init".as_ptr(), 12);
        dev_log(sys, 3, b"[ofl] init".as_ptr(), 10);
        dev_log(sys, 3, b"[ret] init".as_ptr(), 10);
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

        // ── 1-6: one drain, demuxed to the owning component ──────────
        let mut checks = 0u8;
        let mut enqueues = 0u8;
        let mut reconnects = 0u8;
        let mut writes = 0u8;
        let mut reads = 0u8;
        let mut worked = 0u32;

        if s.in_op >= 0 {
            for _ in 0..OP_BUDGET {
                let poll = (sys.channel_poll)(s.in_op, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_op, &mut s.buf);
                let plen = plen as usize;
                if plen > s.buf.len() {
                    continue;
                }

                worked += 1;
                match mt {
                    wire::MSG_APPLY_RESET_FANOUT => {
                        dedup::on_reset(&mut s.dedup);
                        offline::on_reset(&mut s.offline);
                        retained::on_reset(&mut s.retained);
                    }
                    wire::MSG_DEDUP_CHECK => {
                        if checks >= dedup::CHECK_BUDGET {
                            break;
                        }
                        checks += 1;
                        dedup::on_check(&mut s.dedup, sys, &s.buf[..plen], now);
                    }
                    wire::MSG_OFFLINE_ENQUEUE => {
                        if enqueues >= offline::ENQUEUE_BUDGET {
                            break;
                        }
                        enqueues += 1;
                        offline::on_enqueue(&mut s.offline, &s.buf[..plen], now);
                    }
                    wire::MSG_OFFLINE_RECONNECT => {
                        if reconnects >= offline::RECONNECT_BUDGET {
                            break;
                        }
                        reconnects += 1;
                        offline::on_reconnect(&mut s.offline, sys, &s.buf[..plen]);
                    }
                    wire::MSG_RETAINED_WRITE => {
                        if writes >= retained::WRITE_BUDGET {
                            break;
                        }
                        writes += 1;
                        retained::on_write(&mut s.retained, &s.buf[..plen], now);
                    }
                    wire::MSG_RETAINED_READ => {
                        if reads >= retained::READ_BUDGET {
                            break;
                        }
                        reads += 1;
                        retained::on_read(&mut s.retained, sys, &s.buf[..plen]);
                    }
                    // Shared bus: traffic addressed to other consumers.
                    _ => {}
                }
            }
        }

        // ── 7-8: periodic sweeps ─────────────────────────────────────
        dedup::step(&mut s.dedup, now);
        offline::step(&mut s.offline, now);

        // ── 9: component-tagged telemetry, one frame per component ───
        if now.wrapping_sub(s.last_metrics_ms) >= METRICS_INTERVAL_MS {
            s.last_metrics_ms = now;
            let mut m = [0u8; 24];
            let n = dedup::metrics(&s.dedup, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_DEDUP, &m[..n]);
            let n = offline::metrics(&s.offline, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_OFFLINE, &m[..n]);
            let n = retained::metrics(&s.retained, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_RETAINED, &m[..n]);
        }

        if worked > 0 {
            STEP_BURST
        } else {
            0
        }
    }
}
