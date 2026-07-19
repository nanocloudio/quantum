//! Metrics Aggregator — High-cardinality dimensional metrics.
//!
//! Aggregates metrics from Quantum-specific modules with per-tenant,
//! per-protocol, per-PRG dimensions. Enforces cardinality limits.
//! Emits rollups to telemetry_agg for Prometheus export.

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

const MAX_KEYS: usize = 4096;

#[repr(C)]
#[derive(Clone, Copy)]
struct MetricKey {
    // Composite key: tenant (u32) | protocol (u8) | prg (u16) | metric_id (u8)
    tenant: u32,
    protocol: u8,
    prg: u16,
    metric_id: u8,
    key_hash: u64,
    counter: u64,
    sum: u64,           // for histogram-like aggregations
    last_update_ms: u64,
    active: u8,
}

impl MetricKey {
    const fn zero() -> Self {
        Self {
            tenant: 0, protocol: 0, prg: 0, metric_id: 0,
            key_hash: 0, counter: 0, sum: 0, last_update_ms: 0, active: 0,
        }
    }
}

/// Hash the dimensional components into a single u64 key.
fn dim_hash(tenant: u32, protocol: u8, prg: u16, metric_id: u8) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in tenant.to_le_bytes().iter()
        .chain(core::iter::once(&protocol))
        .chain(prg.to_le_bytes().iter())
        .chain(core::iter::once(&metric_id))
    {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_ingest: i32,
    out_rollups: i32,

    cardinality_limit: u32,
    rollup_interval_ms: u32,
    last_rollup_ms: u64,

    keys: [MetricKey; MAX_KEYS],
    total_ingested: u64,
    cardinality_violations: u32,
    rollups_emitted: u32,

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
        s.in_ingest = in_chan;
        s.out_rollups = out_chan;
        s.cardinality_limit = 100_000;
        s.rollup_interval_ms = 10_000;
        for i in 0..MAX_KEYS {
            s.keys[i] = MetricKey::zero();
        }
        dev_log(sys, 3, b"[metr] init".as_ptr(), 11);
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

        // Drain ingest. Metric envelope for dimensional routing:
        // [dim_flag: u8][if flag: tenant:u32][protocol:u8][prg:u16][metric_id:u8][...raw payload]
        // If dim_flag == 0, fall back to payload-hash key (legacy path).
        if s.in_ingest >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_ingest, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, plen) = wire::channel_read_msg(sys, s.in_ingest, &mut s.buf);
                if plen == 0 { continue; }
                let plen = plen as usize;
                s.total_ingested = s.total_ingested.wrapping_add(1);

                let (tenant, protocol, prg, metric_id, key_hash) = if plen >= 9 && s.buf[0] == 1 {
                    // Dimensional envelope
                    let tenant = u32::from_le_bytes([s.buf[1], s.buf[2], s.buf[3], s.buf[4]]);
                    let protocol = s.buf[5];
                    let prg = u16::from_le_bytes([s.buf[6], s.buf[7]]);
                    let metric_id = s.buf[8];
                    (tenant, protocol, prg, metric_id, dim_hash(tenant, protocol, prg, metric_id))
                } else {
                    let h = wire::fnv1a_64(&s.buf[..plen]);
                    (0u32, 0u8, 0u16, 0u8, h)
                };

                let idx = (key_hash as usize) & (MAX_KEYS - 1);

                if s.keys[idx].active == 1 && s.keys[idx].key_hash == key_hash {
                    s.keys[idx].counter = s.keys[idx].counter.wrapping_add(1);
                    // Track sum of first u64 in payload as a crude accumulator
                    if plen >= 17 {
                        let v = u64::from_le_bytes([s.buf[9], s.buf[10], s.buf[11], s.buf[12], s.buf[13], s.buf[14], s.buf[15], s.buf[16]]);
                        s.keys[idx].sum = s.keys[idx].sum.wrapping_add(v);
                    }
                    s.keys[idx].last_update_ms = now;
                } else if s.keys[idx].active == 0 {
                    s.keys[idx] = MetricKey {
                        tenant, protocol, prg, metric_id,
                        key_hash, counter: 1, sum: 0, last_update_ms: now, active: 1,
                    };
                } else {
                    s.cardinality_violations = s.cardinality_violations.wrapping_add(1);
                }
            }
        }

        // Periodic rollup emission
        if now.wrapping_sub(s.last_rollup_ms) >= s.rollup_interval_ms as u64 && s.out_rollups >= 0 {
            s.last_rollup_ms = now;
            let mut active_keys = 0u32;
            let mut total_counts = 0u64;
            for i in 0..MAX_KEYS {
                if s.keys[i].active == 1 {
                    active_keys += 1;
                    total_counts = total_counts.wrapping_add(s.keys[i].counter);
                }
            }
            let mut m = [0u8; 24];
            m[0..8].copy_from_slice(&s.total_ingested.to_le_bytes());
            m[8..12].copy_from_slice(&active_keys.to_le_bytes());
            m[12..20].copy_from_slice(&total_counts.to_le_bytes());
            m[20..24].copy_from_slice(&s.cardinality_violations.to_le_bytes());
            let poll = (sys.channel_poll)(s.out_rollups, 0x02);
            if poll > 0 && (poll as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, s.out_rollups, wire::MSG_METRICS_ROLLUP, &m);
                s.rollups_emitted = s.rollups_emitted.wrapping_add(1);
            }
        }

        0
    }
}
