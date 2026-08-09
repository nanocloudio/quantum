//! audit — signed, tamper-evident audit events.
//!
//! Assigns each event a monotonic sequence number and an HMAC over the
//! record, so a gap or an edit is detectable downstream. Tamper-evident
//! and compliance-bound, distinct from the best-effort counters the
//! operations surface exports.
//!
//! ## Per-step bound
//!
//! `on_event` handles ONE event per call.

use super::abi::SyscallTable;
use super::sha256;
use super::{dev_channel_port, dev_log, dev_millis, wire};

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
pub struct Audit {
    pub in_event: i32,   // in[0]: fan-in of audit events
    pub out_signed: i32, // out[0]: signed events to gateway

    // HMAC key. In production, rotated via durability's DEK epoch fanout.
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

/// Component defaults. Channel handles are assigned by the
/// composite after this returns.
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn init(s: &mut Audit, sys: &SyscallTable) {
    // Derive an initial HMAC key from boot-time. Weak but deterministic
    // per boot; durability's DEK fanout would replace this in production.
    // SAFETY: caller guarantees `sys` is live.
    let boot_ms = unsafe { dev_millis(sys) };
    let seed = [
        boot_ms as u8,
        (boot_ms >> 8) as u8,
        (boot_ms >> 16) as u8,
        (boot_ms >> 24) as u8,
        (boot_ms >> 32) as u8,
        (boot_ms >> 40) as u8,
        (boot_ms >> 48) as u8,
        (boot_ms >> 56) as u8,
        b'q',
        b'u',
        b'a',
        b'n',
        b't',
        b'u',
        b'm',
        b'-',
        b'a',
        b'u',
        b'd',
        b'i',
        b't',
        b'-',
        b'h',
        b'm',
        b'a',
        b'c',
        b'-',
        b'k',
        b'e',
        b'y',
        b'!',
        b'0',
    ];
    s.hmac_key = sha256::sha256(&seed);
}

/// One step of this component.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn step(s: &mut Audit, sys: &SyscallTable) {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let now = dev_millis(sys);

        // Drain all audit events (fan-in). Events raised by the sibling
        // components this step arrived through the outbox and already went
        // through `on_event`; this drains the external producers.
        if s.in_event >= 0 {
            let mut rec = [0u8; 512];
            loop {
                let poll = (sys.channel_poll)(s.in_event, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (_, plen) = wire::channel_read_msg(sys, s.in_event, &mut rec);
                if plen == 0 {
                    break;
                }
                let n = (plen as usize).min(rec.len());
                on_event(s, sys, &rec[..n], now);
            }
        }

        let _ = s.last_emit_ms;
    }
}

/// Sign and emit one audit event. Assigns the next sequence number and
/// an HMAC over `[seq][timestamp][event]`, so a gap or an edit is
/// detectable downstream.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_event(s: &mut Audit, sys: &SyscallTable, payload: &[u8], now: u64) {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let plen = payload.len().min(s.buf.len());
        if plen == 0 {
            return;
        }
        s.buf[..plen].copy_from_slice(&payload[..plen]);

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
        if total > s.record_buf.len() {
            return;
        }
        s.record_buf[0..8].copy_from_slice(&s.sequence.to_le_bytes());
        s.record_buf[8..16].copy_from_slice(&now.to_le_bytes());
        s.record_buf[16..16 + plen].copy_from_slice(&s.buf[..plen]);

        let tag = hmac_sha256(&s.hmac_key, &s.record_buf[..unsigned_len]);
        s.record_buf[unsigned_len..unsigned_len + 32].copy_from_slice(&tag);

        if s.out_signed >= 0 {
            let poll_out = (sys.channel_poll)(s.out_signed, 0x02);
            if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                wire::channel_write_msg(
                    sys,
                    s.out_signed,
                    wire::MSG_AUDIT_EVENT,
                    &s.record_buf[..total],
                );
            }
        }
    }
}
