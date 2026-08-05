//! Governance — tenancy, disaster recovery, audit and telemetry.
//!
//! One graph module composed of four components (standards
//! `fluxor-modules.md` §8):
//!
//!   - [`tenants`]   — per-tenant quota enforcement, noisy-neighbour
//!     control and forced disconnects.
//!   - [`dr`]        — checkpoint/archive scheduling and the controlled
//!     promotion state machine.
//!   - [`audit`]     — sequence-numbered, HMAC-signed audit records.
//!   - [`telemetry`] — dimensional metric aggregation under a
//!     cardinality limit, rolled up to the operations export surface.
//!
//! These components interact: `tenants` and `dr` are the two largest
//! producers of audit events and both publish their own counters. Those
//! were graph edges into the audit and telemetry modules; they are now
//! [`seam`] rings the dispatch table drains, which is why this composite
//! removes edges the other groupings could not.
//!
//! ## Dispatch table
//!
//! Producers run before consumers so an audit event or metric sample
//! raised this step is signed and folded in the same step, not the next:
//!
//!   1. `tenants::step` — drains `records_in` and `charge_in`, runs the
//!      quota sweep, and pushes audit events and its counters into the
//!      outbox.
//!   2. `dr::step` — drains its three inputs, advances the schedule and
//!      promotion state machine, same outbox.
//!   3. Outbox drain — audit records to `audit::on_event`, metric
//!      samples to `telemetry::on_sample`, so an event raised this step
//!      is signed and folded in the same step rather than the next.
//!      Bounded by the ring capacities.
//!   4. `audit::step` — drains `audit_in` from the external producers
//!      (the session state machine, the operations surface).
//!   5. `telemetry::step` — drains `ingest` from every other module and
//!      emits the periodic rollup on `rollups`.
//!
//! Each component keeps its own input ports: unlike the protocol and
//! messaging composites there is no shared request bus here, so no
//! demux is needed and no component can consume another's records.
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

#[path = "../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs"]
mod sha256;

#[path = "../../common/wire.rs"]
mod wire;

mod audit;
mod dr;
mod seam;
mod telemetry;
mod tenants;

/// Largest seam record, sized to the audit component's event buffer.
const READ_BUF: usize = 512;

/// Seam capacity. `tenants` raises at most one audit event per tenant
/// per sweep plus one counter frame; `dr` at most two audit events and
/// one counter frame per step.
const AUDIT_RING: usize = 512;
const METRIC_RING: usize = 256;

/// Outbox the producing components write into, drained by the dispatch
/// table into the consuming components. Each ring carries the channel
/// layer's own record framing, so replacing a `push` with a
/// `channel_write_msg` lifts a component back out unchanged.
#[repr(C)]
pub struct Outbox {
    pub audit: seam::SeamRing<AUDIT_RING>,
    pub metrics: seam::SeamRing<METRIC_RING>,
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    tenants: tenants::Tenants,
    dr: dr::Dr,
    audit: audit::Audit,
    telemetry: telemetry::Telemetry,

    outbox: Outbox,

    /// Module-owned read buffer. Each record is read once here and
    /// dispatched as a borrowed payload.
    buf: [u8; READ_BUF],
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
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

        tenants::init(&mut s.tenants);
        dr::init(&mut s.dr);
        audit::init(&mut s.audit, sys);
        telemetry::init(&mut s.telemetry);

        s.telemetry.in_ingest = in_chan;
        s.audit.in_event = dev_channel_port(sys, 0, 1);
        s.tenants.in_records = dev_channel_port(sys, 0, 2);
        s.tenants.in_charge = dev_channel_port(sys, 0, 3);
        s.dr.in_wal_signal = dev_channel_port(sys, 0, 4);
        s.dr.in_snapshot_resp = dev_channel_port(sys, 0, 5);
        s.dr.in_promotion_req = dev_channel_port(sys, 0, 6);

        s.telemetry.out_rollups = out_chan;
        s.audit.out_signed = dev_channel_port(sys, 1, 1);
        s.tenants.out_quota = dev_channel_port(sys, 1, 2);
        s.tenants.out_disconnect = dev_channel_port(sys, 1, 3);
        s.dr.out_snapshot_req = dev_channel_port(sys, 1, 4);
        s.dr.out_promotion = dev_channel_port(sys, 1, 5);

        s.outbox.audit.reset();
        s.outbox.metrics.reset();

        dev_log(sys, 3, b"[tenant] init".as_ptr(), 13);
        dev_log(sys, 3, b"[dr] init".as_ptr(), 9);
        dev_log(sys, 3, b"[audit] init".as_ptr(), 12);
        dev_log(sys, 3, b"[metr] init".as_ptr(), 11);
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    // SAFETY: per the module ABI (target/fluxor/fluxor-abi/sdk/abi.rs),
    // the kernel passes a valid, exclusively-borrowed `state` of at least
    // `module_state_size()` bytes, and a `syscalls` table whose function
    // pointers reach live kernel routines. The dereferences and syscall
    // invocations below rely on those guarantees.
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // ── 1-2: producers ───────────────────────────────────────────
        tenants::step(&mut s.tenants, sys, &mut s.outbox);
        dr::step(&mut s.dr, sys, &mut s.outbox);

        // ── 3: outbox → consumers, same step ─────────────────────────
        let mut rec = [0u8; READ_BUF];
        while let Some((_mt, len)) = s.outbox.audit.pop(&mut rec) {
            let n = (len as usize).min(rec.len());
            audit::on_event(&mut s.audit, sys, &rec[..n], now);
        }
        while let Some((_mt, len)) = s.outbox.metrics.pop(&mut rec) {
            let n = (len as usize).min(rec.len());
            telemetry::on_sample(&mut s.telemetry, &rec[..n], now);
        }

        // ── 4-5: external producers + periodic rollup ────────────────
        audit::step(&mut s.audit, sys);
        telemetry::step(&mut s.telemetry, sys);

        0
    }
}

