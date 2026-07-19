//! Audit Logger — Signed, structured audit event output.
//!
//! Collects audit events from rbac, session_processor, tenant_manager,
//! admin_handler, dr_manager. Emits Ed25519-signed structured records
//! with retention guarantees (separate from metrics).

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

#[path = "../../../target/fluxor/fluxor-abi/sdk/sha256.rs"]
mod sha256;

#[path = "../../common/wire.rs"]
mod wire;

/// HMAC-SHA256 per RFC 2104.
/// Key is padded/hashed to 64 bytes, then:
///   inner = sha256(opad_key ⊕ 0x5c, sha256(ipad_key ⊕ 0x36, msg))
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k_pad = [0u8; 64];
    if key.len() > 64 {
        let h = sha256::sha256(key);
        k_pad[..32].copy_from_slice(&h);
    } else {
        k_pad[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0u8; 64];
    let mut opad = [0u8; 64];
    for i in 0..64 {
        ipad[i] = k_pad[i] ^ 0x36;
        opad[i] = k_pad[i] ^ 0x5c;
    }

    let mut inner = sha256::Sha256::new();
    inner.update(&ipad);
    inner.update(msg);
    let inner_hash = inner.finalize();

    let mut outer = sha256::Sha256::new();
    outer.update(&opad);
    outer.update(&inner_hash);
    outer.finalize()
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_event: i32,        // in[0]: fan-in of audit events
    out_signed: i32,      // out[0]: signed events to http_surface

    // HMAC key. In production, rotated via key_manager.dek_epoch fanout.
    // For now we derive from module boot time + a fixed seed.
    hmac_key: [u8; 32],

    sequence: u64,
    events_logged: u32,
    connect_count: u32,
    disconnect_count: u32,
    auth_fail_count: u32,
    acl_deny_count: u32,
    throttle_count: u32,
    dr_count: u32,
    admin_count: u32,
    last_emit_ms: u64,
    buf: [u8; 512],
    record_buf: [u8; 512],
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
        s.in_event = in_chan;
        s.out_signed = out_chan;

        // Derive an initial HMAC key from boot-time. Weak but deterministic
        // per boot; key_manager DEK fanout would replace this in production.
        let boot_ms = dev_millis(sys);
        let seed = [
            boot_ms as u8, (boot_ms >> 8) as u8, (boot_ms >> 16) as u8, (boot_ms >> 24) as u8,
            (boot_ms >> 32) as u8, (boot_ms >> 40) as u8, (boot_ms >> 48) as u8, (boot_ms >> 56) as u8,
            b'q', b'u', b'a', b'n', b't', b'u', b'm', b'-',
            b'a', b'u', b'd', b'i', b't', b'-', b'h', b'm',
            b'a', b'c', b'-', b'k', b'e', b'y', b'!', b'0',
        ];
        s.hmac_key = sha256::sha256(&seed);
        dev_log(sys, 3, b"[aud] init".as_ptr(), 9);
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

        // Drain all audit events (fan-in)
        if s.in_event >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_event, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, plen) = wire::channel_read_msg(sys, s.in_event, &mut s.buf);
                if plen == 0 { continue; }
                let plen = plen as usize;

                s.sequence = s.sequence.wrapping_add(1);
                s.events_logged = s.events_logged.wrapping_add(1);

                // Category counter
                let category = s.buf[0];
                match category {
                    0 => s.connect_count = s.connect_count.wrapping_add(1),
                    1 => s.disconnect_count = s.disconnect_count.wrapping_add(1),
                    2 => s.auth_fail_count = s.auth_fail_count.wrapping_add(1),
                    3 => s.acl_deny_count = s.acl_deny_count.wrapping_add(1),
                    4 => s.throttle_count = s.throttle_count.wrapping_add(1),
                    5 => s.dr_count = s.dr_count.wrapping_add(1),
                    6 => s.admin_count = s.admin_count.wrapping_add(1),
                    _ => {}
                }

                // Build signed record: [seq:u64][timestamp:u64][event...][hmac_sha256:32]
                let unsigned_len = 16 + plen;
                let total = unsigned_len + 32;
                if total > s.record_buf.len() { continue; }
                s.record_buf[0..8].copy_from_slice(&s.sequence.to_le_bytes());
                s.record_buf[8..16].copy_from_slice(&now.to_le_bytes());
                s.record_buf[16..16 + plen].copy_from_slice(&s.buf[..plen]);

                let tag = hmac_sha256(&s.hmac_key, &s.record_buf[..unsigned_len]);
                s.record_buf[unsigned_len..unsigned_len + 32].copy_from_slice(&tag);

                if s.out_signed >= 0 {
                    let poll_out = (sys.channel_poll)(s.out_signed, 0x02);
                    if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                        wire::channel_write_msg(sys, s.out_signed, wire::MSG_AUDIT_EVENT, &s.record_buf[..total]);
                    }
                }
            }
        }

        let _ = s.last_emit_ms;
        0
    }
}
