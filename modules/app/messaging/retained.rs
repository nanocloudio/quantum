//! retained — MQTT retained-message store.
//!
//! Stores topic + payload inline so the read path can echo them back
//! without a separate content-addressed payload store. An empty payload
//! on `retain=1` clears the entry (MQTT 3.1.1 §3.3.1.3).
//!
//! ## Per-step bound
//!
//! `on_write` handles ONE write per call ([`WRITE_BUDGET`] per step).
//! `on_read` handles ONE subscription filter per call ([`READ_BUDGET`] per
//! step) and emits up to [`MAX_RETAINED`] responses for it — a wildcard
//! SUBSCRIBE matches every retained entry on the filter, which MQTT 3.1.1
//! §3.3.1.6 / MQTT 5 §3.3.1.5 both require.

use super::abi::SyscallTable;
use super::wire;

const MAX_RETAINED: usize = 64;
const MAX_RETAINED_TOPIC: usize = 256;
const MAX_RETAINED_PAYLOAD: usize = 1024;

/// Scratch buffer cap, sized to hold the largest response envelope this
/// component emits: 23-byte header + topic + payload.
const SCRATCH_BUF: usize = 23 + MAX_RETAINED_TOPIC + MAX_RETAINED_PAYLOAD;

/// Writes admitted per step. Matches the standalone drain bound.
pub const WRITE_BUDGET: u8 = 8;
/// Read filters served per step. Matches the standalone drain bound.
pub const READ_BUDGET: u8 = 8;

#[repr(C)]
#[derive(Clone, Copy)]
struct RetainedEntry {
    tenant: u32,
    topic_hash: u64,
    topic_len: u16,
    topic: [u8; MAX_RETAINED_TOPIC],
    payload_len: u32,
    payload: [u8; MAX_RETAINED_PAYLOAD],
    updated_at_ms: u64,
    active: u8,
}

impl RetainedEntry {
    const fn zero() -> Self {
        Self {
            tenant: 0,
            topic_hash: 0,
            topic_len: 0,
            topic: [0; MAX_RETAINED_TOPIC],
            payload_len: 0,
            payload: [0; MAX_RETAINED_PAYLOAD],
            updated_at_ms: 0,
            active: 0,
        }
    }
}

#[repr(C)]
pub struct Retained {
    pub out_read: i32,

    entries: [RetainedEntry; MAX_RETAINED],
    writes: u32,
    reads: u32,
    hits: u32,
    /// Response scratch.
    buf: [u8; SCRATCH_BUF],
}

pub fn init(r: &mut Retained) {
    r.out_read = -1;
    r.writes = 0;
    r.reads = 0;
    r.hits = 0;
    for i in 0..MAX_RETAINED {
        r.entries[i] = RetainedEntry::zero();
    }
}

/// MSG_APPLY_RESET_FANOUT: drop every retained entry.
pub fn on_reset(r: &mut Retained) {
    for i in 0..MAX_RETAINED {
        r.entries[i] = RetainedEntry::zero();
    }
}

/// MSG_RETAINED_WRITE: `[tenant:u32 LE][topic_hash:u64 LE]`
/// `[topic_len:u16 LE][topic_bytes][payload_len:u32 LE][payload_bytes]`.
pub fn on_write(r: &mut Retained, payload: &[u8], now: u64) {
    let plen = payload.len();
    // tenant(4) + topic_hash(8) + topic_len(2) + payload_len(4)
    if plen < 18 {
        return;
    }

    let tenant = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let topic_hash = u64::from_le_bytes([
        payload[4],
        payload[5],
        payload[6],
        payload[7],
        payload[8],
        payload[9],
        payload[10],
        payload[11],
    ]);
    let topic_len = u16::from_le_bytes([payload[12], payload[13]]) as usize;
    if 14 + topic_len + 4 > plen || topic_len > MAX_RETAINED_TOPIC {
        return;
    }
    let payload_len_off = 14 + topic_len;
    let payload_len = u32::from_le_bytes([
        payload[payload_len_off],
        payload[payload_len_off + 1],
        payload[payload_len_off + 2],
        payload[payload_len_off + 3],
    ]) as usize;
    if payload_len > MAX_RETAINED_PAYLOAD {
        return;
    }
    let payload_off = payload_len_off + 4;
    if payload_off + payload_len > plen {
        return;
    }

    // Empty payload on `retain=1` clears the retained entry per
    // MQTT 3.1.1 §3.3.1.3.
    if payload_len == 0 {
        for i in 0..MAX_RETAINED {
            if r.entries[i].active == 1
                && r.entries[i].tenant == tenant
                && r.entries[i].topic_hash == topic_hash
            {
                r.entries[i] = RetainedEntry::zero();
                break;
            }
        }
        r.writes = r.writes.wrapping_add(1);
        return;
    }

    // Upsert.
    let mut slot: Option<usize> = None;
    for i in 0..MAX_RETAINED {
        if r.entries[i].active == 1
            && r.entries[i].tenant == tenant
            && r.entries[i].topic_hash == topic_hash
        {
            slot = Some(i);
            break;
        }
    }
    if slot.is_none() {
        for i in 0..MAX_RETAINED {
            if r.entries[i].active == 0 {
                slot = Some(i);
                break;
            }
        }
    }
    let Some(i) = slot else {
        return;
    };

    r.entries[i].tenant = tenant;
    r.entries[i].topic_hash = topic_hash;
    r.entries[i].topic_len = topic_len as u16;
    r.entries[i].payload_len = payload_len as u32;
    // SAFETY: both lengths are bounds-checked against the entry capacities
    // and against `plen` above.
    unsafe {
        core::ptr::copy_nonoverlapping(
            payload.as_ptr().add(14),
            r.entries[i].topic.as_mut_ptr(),
            topic_len,
        );
        core::ptr::copy_nonoverlapping(
            payload.as_ptr().add(payload_off),
            r.entries[i].payload.as_mut_ptr(),
            payload_len,
        );
    }
    r.entries[i].updated_at_ms = now;
    r.entries[i].active = 1;
    r.writes = r.writes.wrapping_add(1);
}

/// MSG_RETAINED_READ request: `[tenant:u32 LE][session_slot:u32 LE]`
/// `[sub_qos:u8][pattern_len:u16 LE][pattern_bytes]`.
///
/// Emits one MSG_RETAINED_READ response per matching entry:
/// `[tenant:u32 LE][topic_hash:u64 LE][session_slot:u32 LE][sub_qos:u8]`
/// `[topic_len:u16 LE][topic_bytes][payload_len:u32 LE][payload_bytes]`.
/// The echoed `(session_slot, sub_qos)` lets the consumer attribute the
/// delivery without a separate in-flight lookup table.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_read(r: &mut Retained, sys: &SyscallTable, payload: &[u8]) {
    let plen = payload.len();
    if plen < 11 {
        return;
    }
    let tenant = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let session_slot = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
    let sub_qos = payload[8];
    let pattern_len = u16::from_le_bytes([payload[9], payload[10]]) as usize;
    if 11 + pattern_len > plen || pattern_len > MAX_RETAINED_TOPIC {
        return;
    }
    // Stash the pattern in a stack buffer so `r.buf` is free as response
    // scratch without overlapping borrows.
    let mut pattern = [0u8; MAX_RETAINED_TOPIC];
    // SAFETY: `pattern_len <= MAX_RETAINED_TOPIC` and `11 + pattern_len <= plen`.
    unsafe {
        core::ptr::copy_nonoverlapping(payload.as_ptr().add(11), pattern.as_mut_ptr(), pattern_len);
    }
    r.reads = r.reads.wrapping_add(1);

    for i in 0..MAX_RETAINED {
        if r.entries[i].active == 0 || r.entries[i].tenant != tenant {
            continue;
        }
        let entry_topic_len = r.entries[i].topic_len as usize;
        let entry_topic = &r.entries[i].topic[..entry_topic_len];
        if !wire::mqtt_topic_match(&pattern[..pattern_len], entry_topic) {
            continue;
        }
        let topic_hash = r.entries[i].topic_hash;
        let topic_len = entry_topic_len;
        let payload_len = r.entries[i].payload_len as usize;
        let total = 17 + 2 + topic_len + 4 + payload_len;
        if total > r.buf.len() {
            continue;
        }

        r.buf[0..4].copy_from_slice(&tenant.to_le_bytes());
        r.buf[4..12].copy_from_slice(&topic_hash.to_le_bytes());
        r.buf[12..16].copy_from_slice(&session_slot.to_le_bytes());
        r.buf[16] = sub_qos;
        r.buf[17..19].copy_from_slice(&(topic_len as u16).to_le_bytes());
        let payload_len_off = 19 + topic_len;
        // SAFETY: `total <= r.buf.len()` checked above bounds every write.
        unsafe {
            core::ptr::copy_nonoverlapping(
                r.entries[i].topic.as_ptr(),
                r.buf.as_mut_ptr().add(19),
                topic_len,
            );
        }
        r.buf[payload_len_off..payload_len_off + 4]
            .copy_from_slice(&(payload_len as u32).to_le_bytes());
        // SAFETY: as above.
        unsafe {
            core::ptr::copy_nonoverlapping(
                r.entries[i].payload.as_ptr(),
                r.buf.as_mut_ptr().add(payload_len_off + 4),
                payload_len,
            );
        }

        if r.out_read >= 0 {
            // SAFETY: caller guarantees `sys` is live.
            unsafe {
                let poll_out = (sys.channel_poll)(r.out_read, 0x02);
                if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                    wire::channel_write_msg(
                        sys,
                        r.out_read,
                        wire::MSG_RETAINED_READ,
                        &r.buf[..total],
                    );
                    r.hits = r.hits.wrapping_add(1);
                }
            }
        }
        // Keep iterating — a wildcard subscription matches every
        // retained entry on the filter.
    }
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(r: &Retained, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&r.writes.to_le_bytes());
    m[4..8].copy_from_slice(&r.reads.to_le_bytes());
    m[8..12].copy_from_slice(&r.hits.to_le_bytes());
    let mut active = 0u32;
    for i in 0..MAX_RETAINED {
        if r.entries[i].active == 1 {
            active += 1;
        }
    }
    m[12..16].copy_from_slice(&active.to_le_bytes());
    m[16..24].copy_from_slice(&0u64.to_le_bytes());
    24
}
