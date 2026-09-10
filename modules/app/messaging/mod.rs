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
//! A node running an anchor-preserved handoff pair
//! (docs/architecture/session_continuity.md) has two session workers
//! sharing this module, so there is a bus per worker: `op_in2` /
//! `result_out2` for worker 1. A reply always goes back on the port of
//! the bus that carried the request, which is what keeps the two
//! workers from both acting on one answer. Both are unwired on a
//! one-worker node.
//!
//! ## Dispatch table
//!
//! One drain of each worker bus per step, at most [`OP_BUDGET`] records
//! from each, demuxed by frame type. The per-component budgets below
//! are shared across the two buses, so the module's per-step work is
//! bounded whatever the worker split is. Per-component budgets below are the components' own
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

/// Placement: shared with `session_processor` and `topic_engine` so all
/// three resolve a shard to the same owner.
#[allow(
    dead_code,
    reason = "the core is compiled verbatim into every module that shares it; \
              this module uses the placement half, topic_engine the routing \
              half. Trimming it per consumer would fork the one definition \
              the core exists to provide"
)]
#[path = "../../common/cores/edge_routing_core.rs"]
mod edge;

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

/// Metric payload capacity every component in this module writes into.
const METRIC_BYTES: usize = 36;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_op: i32,
    /// Session worker 1's op bus and its replies (`op_in2` /
    /// `result_out2`). A reply goes back to the worker that asked, so
    /// two workers sharing this module never both act on one. Unwired
    /// on a one-worker node.
    in_op2: i32,
    out_result: i32,
    out_result2: i32,
    out_metrics: i32,
    last_metrics_ms: u64,

    dedup: dedup::Dedup,
    offline: offline::Offline,
    retained: retained::Retained,

    /// Placement, learned over the op bus from `session_processor`.
    view: edge::EdgeMap,
    /// Retained entries dropped because their shard was reassigned.
    retained_released: u32,
    /// Queued deliveries dropped with a released session slot.
    offline_released: u32,

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
        // Declared last in the manifest: in[1] / out[2].
        s.in_op2 = dev_channel_port(sys, 0, 1);
        s.out_result = out_chan;
        s.out_result2 = dev_channel_port(sys, 1, 2);
        s.last_metrics_ms = 0;

        dedup::init(&mut s.dedup);
        offline::init(&mut s.offline);
        retained::init(&mut s.retained);
        // Single PRG, epoch 0, until placement arrives: a module that
        // has heard none owns everything, which is the single-node
        // behaviour and releases nothing.
        s.view = edge::EdgeMap::new(0, 0);
        s.retained_released = 0;
        s.offline_released = 0;

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
    let mut env = [0u8; 9 + METRIC_BYTES];
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

        // One drain per worker bus. A component answers on the result
        // port of the bus the request came in on, so two workers
        // sharing this module never both act on one reply.
        //
        // The per-type budgets are declared once and spent across both
        // buses, which bounds the module's step rather than each
        // worker's. Worker 0's bus is drained first and can therefore
        // spend the step's budget before worker 1's is polled; the
        // budgets refill every step, so this is an ordering preference
        // under saturation, not starvation. It also matches the shape
        // of a handoff pair, where the busy worker and the standby
        // trade places rather than run hot together.
        for (in_chan, out_chan) in [(s.in_op, s.out_result), (s.in_op2, s.out_result2)] {
            if in_chan < 0 || out_chan < 0 {
                continue;
            }
            s.dedup.out_result = out_chan;
            s.offline.out_drain = out_chan;
            s.retained.out_read = out_chan;
            for _ in 0..OP_BUDGET {
                // Every budget decision precedes consumption. A record's
                // type is not known until it is read, so once ANY
                // per-type budget is spent this step, the drain stops
                // rather than take a record it might not be able to
                // handle. Losing a MSG_DEDUP_CHECK this way would leave a
                // committed publish's stash unresolved indefinitely.
                if checks >= dedup::CHECK_BUDGET
                    || enqueues >= offline::ENQUEUE_BUDGET
                    || reconnects >= offline::RECONNECT_BUDGET
                    || writes >= retained::WRITE_BUDGET
                    || reads >= retained::READ_BUDGET
                {
                    break;
                }
                let poll = (sys.channel_poll)(in_chan, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = wire::channel_read_msg(sys, in_chan, &mut s.buf);
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
                        checks += 1;
                        dedup::on_check(&mut s.dedup, sys, &s.buf[..plen], now);
                    }
                    wire::MSG_DEDUP_PHASE => {
                        dedup::on_phase(&mut s.dedup, &s.buf[..plen]);
                    }
                    wire::MSG_OFFLINE_ENQUEUE => {
                        enqueues += 1;
                        offline::on_enqueue(&mut s.offline, &s.buf[..plen], now);
                    }
                    wire::MSG_OFFLINE_RECONNECT => {
                        reconnects += 1;
                        offline::on_reconnect(&mut s.offline, sys, &s.buf[..plen]);
                    }
                    wire::MSG_RETAINED_WRITE => {
                        writes += 1;
                        retained::on_write(&mut s.retained, &s.buf[..plen], now);
                    }
                    wire::MSG_RETAINED_READ => {
                        reads += 1;
                        retained::on_read(&mut s.retained, sys, &s.buf[..plen]);
                    }
                    // Placement, forwarded verbatim by `session_processor`
                    // over this same op bus because this module has no
                    // control-plane port of its own. Parsed with the
                    // shared core, so there is one wire format and one
                    // shard->owner derivation across every module that
                    // reads placement.
                    wire::MSG_PLACEMENT_UPDATE => {
                        if edge::apply_placement_update(&mut s.view.view, &s.buf[..plen])
                            == edge::PlacementUpdate::Applied
                        {
                            let view = &s.view;
                            // `is_local`, not `owns_shard`: a fenced
                            // shard is still ours and must keep its
                            // state, because an aborted migration lifts
                            // the fence and leaves ownership unmoved.
                            s.retained_released = s.retained_released.wrapping_add(
                                retained::release_foreign(&mut s.retained, |sh| view.is_local(sh)),
                            );
                        }
                    }
                    wire::MSG_OFFLINE_RELEASE => {
                        if plen >= 4 {
                            let slot = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                            s.offline_released = s
                                .offline_released
                                .wrapping_add(offline::release_slot(&mut s.offline, slot));
                        }
                    }
                    // Shared bus: traffic addressed to other consumers.
                    _ => {}
                }
            }
        }
        // Leave the reply ports on worker 0 rather than on whichever
        // bus drained last. The sweeps below emit nothing, so this
        // costs nothing today and keeps a future emitter outside the
        // loop from inheriting an arbitrary worker's port.
        s.dedup.out_result = s.out_result;
        s.offline.out_drain = s.out_result;
        s.retained.out_read = s.out_result;

        // ── 7-8: periodic sweeps ─────────────────────────────────────
        dedup::step(&mut s.dedup, now);
        offline::step(&mut s.offline, now);

        // ── 9: component-tagged telemetry, one frame per component ───
        if now.wrapping_sub(s.last_metrics_ms) >= METRICS_INTERVAL_MS {
            s.last_metrics_ms = now;
            let mut m = [0u8; METRIC_BYTES];
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
