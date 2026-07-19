//! Tenant Manager — Tenant policy enforcement.
//!
//! Consumes tenant records from cp_bridge. Maintains per-tenant token
//! bucket state, detects sustained overage (noisy-neighbor), and emits
//! quota signals to throttle_gate and disconnect events to session_processor.

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

const MAX_TENANTS: usize = 256;

#[repr(C)]
#[derive(Clone, Copy)]
struct TenantState {
    tenant_id: u32,
    max_publish_rate: u32,   // msgs/sec
    current_tokens: i32,
    token_cap: i32,
    overage_started_ms: u64,  // 0 if not in overage
    last_refill_ms: u64,
    active: u8,
}

impl TenantState {
    const fn zero() -> Self {
        Self {
            tenant_id: 0, max_publish_rate: 10_000,
            current_tokens: 10_000, token_cap: 10_000,
            overage_started_ms: 0, last_refill_ms: 0,
            active: 0,
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_records: i32,
    in_charge: i32,       // in[1]: charge events from publish path
    out_quota: i32,
    out_disconnect: i32,
    out_audit: i32,
    out_metrics: i32,

    noisy_neighbor_threshold_s: u32,

    tenants: [TenantState; MAX_TENANTS],
    records_applied: u32,
    quota_violations: u32,
    disconnects: u32,
    charges: u64,
    last_tick_ms: u64,

    buf: [u8; 128],
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
        s.in_records = in_chan;
        s.out_quota = out_chan;
        s.in_charge = dev_channel_port(sys, 0, 1);
        s.out_disconnect = dev_channel_port(sys, 1, 1);
        s.out_audit = dev_channel_port(sys, 1, 2);
        s.out_metrics = dev_channel_port(sys, 1, 3);
        s.noisy_neighbor_threshold_s = 60;
        for i in 0..MAX_TENANTS {
            s.tenants[i] = TenantState::zero();
        }
        dev_log(sys, 3, b"[ten] init".as_ptr(), 9);
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

        // Drain tenant records: [tenant_id:u32][max_publish_rate:u32]
        if s.in_records >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_records, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, plen) = wire::channel_read_msg(sys, s.in_records, &mut s.buf);
                if plen < 8 { continue; }
                let tenant_id = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                let rate = u32::from_le_bytes([s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);

                // Upsert
                let mut found = false;
                for i in 0..MAX_TENANTS {
                    if s.tenants[i].active == 1 && s.tenants[i].tenant_id == tenant_id {
                        s.tenants[i].max_publish_rate = rate;
                        s.tenants[i].token_cap = rate as i32;
                        found = true;
                        break;
                    }
                }
                if !found {
                    for i in 0..MAX_TENANTS {
                        if s.tenants[i].active == 0 {
                            s.tenants[i] = TenantState {
                                tenant_id, max_publish_rate: rate,
                                current_tokens: rate as i32, token_cap: rate as i32,
                                overage_started_ms: 0, last_refill_ms: now,
                                active: 1,
                            };
                            break;
                        }
                    }
                }
                s.records_applied = s.records_applied.wrapping_add(1);
            }
        }

        // Drain charge events: [tenant_id:u32][cost:u32]
        if s.in_charge >= 0 {
            for _ in 0..32 {
                let poll = (sys.channel_poll)(s.in_charge, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, plen) = wire::channel_read_msg(sys, s.in_charge, &mut s.buf);
                if plen < 8 { continue; }
                let tenant_id = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                let cost = u32::from_le_bytes([s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);
                for i in 0..MAX_TENANTS {
                    if s.tenants[i].active == 1 && s.tenants[i].tenant_id == tenant_id {
                        s.tenants[i].current_tokens = s.tenants[i].current_tokens.saturating_sub(cost as i32);
                        s.charges = s.charges.wrapping_add(1);
                        break;
                    }
                }
            }
        }

        // Per-second tick: refill tokens, emit quota signals, detect noisy-neighbor
        if now.wrapping_sub(s.last_tick_ms) >= 1000 {
            let elapsed_s = (now.wrapping_sub(s.last_tick_ms) / 1000) as i32;
            s.last_tick_ms = now;

            for i in 0..MAX_TENANTS {
                if s.tenants[i].active == 0 { continue; }
                // Refill tokens at max_publish_rate per second
                let refill = elapsed_s.saturating_mul(s.tenants[i].max_publish_rate as i32);
                s.tenants[i].current_tokens = (s.tenants[i].current_tokens.saturating_add(refill))
                    .min(s.tenants[i].token_cap);

                // Check for overage
                if s.tenants[i].current_tokens < 0 {
                    if s.tenants[i].overage_started_ms == 0 {
                        s.tenants[i].overage_started_ms = now;
                    }
                    let overage_age = now.wrapping_sub(s.tenants[i].overage_started_ms) / 1000;
                    if overage_age >= s.noisy_neighbor_threshold_s as u64 {
                        // Emit disconnect event
                        let mut d = [0u8; 4];
                        d.copy_from_slice(&s.tenants[i].tenant_id.to_le_bytes());
                        if s.out_disconnect >= 0 {
                            let poll_out = (sys.channel_poll)(s.out_disconnect, 0x02);
                            if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                                wire::channel_write_msg(sys, s.out_disconnect, wire::MSG_TENANT_DISCONNECT, &d);
                                s.disconnects = s.disconnects.wrapping_add(1);
                            }
                        }
                        // Emit audit event
                        if s.out_audit >= 0 {
                            let poll_a = (sys.channel_poll)(s.out_audit, 0x02);
                            if poll_a > 0 && (poll_a as u32 & 0x02) != 0 {
                                let mut ev = [0u8; 8];
                                ev[0] = 4;  // AUDIT_THROTTLE
                                ev[1..5].copy_from_slice(&s.tenants[i].tenant_id.to_le_bytes());
                                wire::channel_write_msg(sys, s.out_audit, wire::MSG_AUDIT_EVENT, &ev);
                            }
                        }
                        s.tenants[i].overage_started_ms = 0;
                    }
                    s.quota_violations = s.quota_violations.wrapping_add(1);
                } else {
                    s.tenants[i].overage_started_ms = 0;
                }

                // Emit quota signal: [tenant_id:u32][tokens:i32]
                let mut q = [0u8; 8];
                q[0..4].copy_from_slice(&s.tenants[i].tenant_id.to_le_bytes());
                q[4..8].copy_from_slice(&s.tenants[i].current_tokens.to_le_bytes());
                if s.out_quota >= 0 {
                    let poll_out = (sys.channel_poll)(s.out_quota, 0x02);
                    if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                        wire::channel_write_msg(sys, s.out_quota, wire::MSG_TENANT_QUOTA, &q);
                    }
                }
            }

            if s.out_metrics >= 0 {
                let mut m = [0u8; 12];
                m[0..4].copy_from_slice(&s.records_applied.to_le_bytes());
                m[4..8].copy_from_slice(&s.quota_violations.to_le_bytes());
                m[8..12].copy_from_slice(&s.disconnects.to_le_bytes());
                let poll = (sys.channel_poll)(s.out_metrics, 0x02);
                if poll > 0 && (poll as u32 & 0x02) != 0 {
                    wire::channel_write_msg(sys, s.out_metrics, wire::MSG_METRICS, &m);
                }
            }
        }

        0
    }
}
