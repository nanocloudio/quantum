//! Retained Store — Topic-indexed retained message storage.
//!
//! Stores one retained payload per (tenant, topic). New subscriptions
//! query this store for topics they subscribe to and receive the
//! retained payload (if any) immediately.
//!
//! The entry stores the topic bytes and payload bytes inline so the
//! delivery path doesn't need a content-addressed store hop — the
//! gap doc considered both options; inline is the simpler first
//! ship and the MQTT retained-message contract is for small
//! last-known-value payloads, not arbitrary blobs.

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

const MAX_RETAINED: usize = 64;
const MAX_RETAINED_TOPIC: usize = 256;
const MAX_RETAINED_PAYLOAD: usize = 1024;

/// Scratch buffer cap, sized to hold the largest request or response
/// envelope this module emits or consumes. Response is the larger of
/// the two: 23-byte header + topic + payload.
const SCRATCH_BUF: usize = 23 + MAX_RETAINED_TOPIC + MAX_RETAINED_PAYLOAD;

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
            tenant: 0, topic_hash: 0,
            topic_len: 0, topic: [0; MAX_RETAINED_TOPIC],
            payload_len: 0, payload: [0; MAX_RETAINED_PAYLOAD],
            updated_at_ms: 0, active: 0,
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_write: i32,
    in_read: i32,
    out_read: i32,
    out_metrics: i32,

    entries: [RetainedEntry; MAX_RETAINED],
    writes: u32,
    reads: u32,
    hits: u32,
    buf: [u8; SCRATCH_BUF],
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
        s.in_write = in_chan;
        s.out_read = out_chan;
        s.in_read = dev_channel_port(sys, 0, 1);
        s.out_metrics = dev_channel_port(sys, 1, 1);
        for i in 0..MAX_RETAINED {
            s.entries[i] = RetainedEntry::zero();
        }
        dev_log(sys, 3, b"[ret] init".as_ptr(), 9);
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

        // ── Writes ────────────────────────────────────────────────────
        // Body: [tenant:u32 LE][topic_hash:u64 LE][topic_len:u16 LE]
        //       [topic_bytes][payload_len:u32 LE][payload_bytes]
        // Stores topic + payload inline so the read path can echo them
        // back to session_processor without a separate content-addressed
        // payload store. Empty payload is a clear (MQTT 3.1.1 §3.3.1.3).
        if s.in_write >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_write, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_write, &mut s.buf);
                let plen = plen as usize;
                if mt == wire::MSG_APPLY_RESET_FANOUT {
                    for i in 0..MAX_RETAINED {
                        s.entries[i] = RetainedEntry::zero();
                    }
                    continue;
                }
                if mt != wire::MSG_RETAINED_WRITE { continue; }
                if plen < 18 { continue; } // tenant(4)+topic_hash(8)+topic_len(2)+payload_len(4)

                let tenant = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                let topic_hash = u64::from_le_bytes([
                    s.buf[4],  s.buf[5],  s.buf[6],  s.buf[7],
                    s.buf[8],  s.buf[9],  s.buf[10], s.buf[11],
                ]);
                let topic_len = u16::from_le_bytes([s.buf[12], s.buf[13]]) as usize;
                if 14 + topic_len + 4 > plen { continue; }
                if topic_len > MAX_RETAINED_TOPIC { continue; }
                let payload_len_off = 14 + topic_len;
                let payload_len = u32::from_le_bytes([
                    s.buf[payload_len_off],
                    s.buf[payload_len_off + 1],
                    s.buf[payload_len_off + 2],
                    s.buf[payload_len_off + 3],
                ]) as usize;
                if payload_len > MAX_RETAINED_PAYLOAD { continue; }
                let payload_off = payload_len_off + 4;
                if payload_off + payload_len > plen { continue; }

                // Empty payload on `retain=1` clears the retained entry
                // per MQTT 3.1.1 §3.3.1.3.
                if payload_len == 0 {
                    for i in 0..MAX_RETAINED {
                        if s.entries[i].active == 1
                            && s.entries[i].tenant == tenant
                            && s.entries[i].topic_hash == topic_hash
                        {
                            s.entries[i] = RetainedEntry::zero();
                            break;
                        }
                    }
                    s.writes = s.writes.wrapping_add(1);
                    continue;
                }

                // Upsert.
                let mut slot: Option<usize> = None;
                for i in 0..MAX_RETAINED {
                    if s.entries[i].active == 1
                        && s.entries[i].tenant == tenant
                        && s.entries[i].topic_hash == topic_hash
                    {
                        slot = Some(i);
                        break;
                    }
                }
                if slot.is_none() {
                    for i in 0..MAX_RETAINED {
                        if s.entries[i].active == 0 {
                            slot = Some(i);
                            break;
                        }
                    }
                }
                let Some(i) = slot else { continue; };

                s.entries[i].tenant = tenant;
                s.entries[i].topic_hash = topic_hash;
                s.entries[i].topic_len = topic_len as u16;
                core::ptr::copy_nonoverlapping(
                    s.buf.as_ptr().add(14),
                    s.entries[i].topic.as_mut_ptr(),
                    topic_len,
                );
                s.entries[i].payload_len = payload_len as u32;
                core::ptr::copy_nonoverlapping(
                    s.buf.as_ptr().add(payload_off),
                    s.entries[i].payload.as_mut_ptr(),
                    payload_len,
                );
                s.entries[i].updated_at_ms = now;
                s.entries[i].active = 1;
                s.writes = s.writes.wrapping_add(1);
            }
        }

        // ── Reads ─────────────────────────────────────────────────────
        // Request:  [tenant:u32 LE][session_slot:u32 LE][sub_qos:u8]
        //           [pattern_len:u16 LE][pattern_bytes]
        // Response (one per matching entry): the read response shape is
        //           [tenant:u32 LE][topic_hash:u64 LE]
        //           [session_slot:u32 LE][sub_qos:u8]
        //           [topic_len:u16 LE][topic_bytes]
        //           [payload_len:u32 LE][payload_bytes]
        // The echoed (session_slot, sub_qos) lets session_processor's
        // messaging_in handler attribute the delivery without a
        // separate in-flight lookup table. The matcher uses
        // `wire::mqtt_topic_match` so a wildcard SUBSCRIBE (`test/+`,
        // `test/#`) gets every retained entry whose topic matches —
        // MQTT 3.1.1 §3.3.1.6 / MQTT 5 §3.3.1.5 both require this.
        if s.in_read >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_read, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_read, &mut s.buf);
                if mt != wire::MSG_RETAINED_READ { continue; }
                let plen = plen as usize;
                if plen < 11 { continue; }
                let tenant = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                let session_slot = u32::from_le_bytes([
                    s.buf[4], s.buf[5], s.buf[6], s.buf[7],
                ]);
                let sub_qos = s.buf[8];
                let pattern_len = u16::from_le_bytes([s.buf[9], s.buf[10]]) as usize;
                if 11 + pattern_len > plen || pattern_len > MAX_RETAINED_TOPIC {
                    continue;
                }
                // Stash the pattern in a stack buffer so we can reuse
                // `s.buf` as the response scratch space without
                // overlapping borrows.
                let mut pattern = [0u8; MAX_RETAINED_TOPIC];
                core::ptr::copy_nonoverlapping(
                    s.buf.as_ptr().add(11),
                    pattern.as_mut_ptr(),
                    pattern_len,
                );
                s.reads = s.reads.wrapping_add(1);

                for i in 0..MAX_RETAINED {
                    if s.entries[i].active == 0
                        || s.entries[i].tenant != tenant
                    {
                        continue;
                    }
                    let entry_topic_len = s.entries[i].topic_len as usize;
                    let entry_topic = &s.entries[i].topic[..entry_topic_len];
                    if !wire::mqtt_topic_match(&pattern[..pattern_len], entry_topic) {
                        continue;
                    }
                    let topic_hash = s.entries[i].topic_hash;
                    let topic_len = entry_topic_len;
                    let payload_len = s.entries[i].payload_len as usize;
                    let total = 17 + 2 + topic_len + 4 + payload_len;
                    if total > s.buf.len() { continue; }

                    s.buf[0..4].copy_from_slice(&tenant.to_le_bytes());
                    s.buf[4..12].copy_from_slice(&topic_hash.to_le_bytes());
                    s.buf[12..16].copy_from_slice(&session_slot.to_le_bytes());
                    s.buf[16] = sub_qos;

                    s.buf[17..19].copy_from_slice(&(topic_len as u16).to_le_bytes());
                    core::ptr::copy_nonoverlapping(
                        s.entries[i].topic.as_ptr(),
                        s.buf.as_mut_ptr().add(19),
                        topic_len,
                    );
                    let payload_len_off = 19 + topic_len;
                    s.buf[payload_len_off..payload_len_off + 4]
                        .copy_from_slice(&(payload_len as u32).to_le_bytes());
                    core::ptr::copy_nonoverlapping(
                        s.entries[i].payload.as_ptr(),
                        s.buf.as_mut_ptr().add(payload_len_off + 4),
                        payload_len,
                    );

                    if s.out_read >= 0 {
                        let poll_out = (sys.channel_poll)(s.out_read, 0x02);
                        if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                            wire::channel_write_msg(
                                sys, s.out_read, wire::MSG_RETAINED_READ,
                                &s.buf[..total],
                            );
                            s.hits = s.hits.wrapping_add(1);
                        }
                    }
                    // Keep iterating — a wildcard subscription
                    // matches every retained entry on the filter.
                }
            }
        }

        0
    }
}
