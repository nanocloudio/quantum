//! Dedup Engine — Sharded message deduplication with expiry.
//!
//! 16 shards indexed by hash(DedupKey). Each shard has a fixed slot table
//! and maintains an expiry horizon. Periodic GC evicts expired entries
//! and updates earliest_dedupe_index for WAL compaction floor.
//!
//! Foundation candidate: protocol-agnostic; usable for any exactly-once pipeline.

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

const SHARD_COUNT: usize = 16;
const SLOTS_PER_SHARD: usize = 128;

#[repr(C)]
#[derive(Clone, Copy)]
struct DedupEntry {
    tenant: u32,
    stream_hash: u64,
    epoch: u32,
    msg_id: u32,
    publish_index: u64,
    expiry_ms: u64,
    active: u8,
}

impl DedupEntry {
    const fn zero() -> Self {
        Self { tenant: 0, stream_hash: 0, epoch: 0, msg_id: 0, publish_index: 0, expiry_ms: 0, active: 0 }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_check: i32,
    out_result: i32,
    out_metrics: i32,

    ttl_ms: u64,
    gc_interval_ms: u64,
    last_gc_ms: u64,

    shards: [[DedupEntry; SLOTS_PER_SHARD]; SHARD_COUNT],
    checks: u32,
    hits: u32,
    inserts: u32,
    evicted: u32,
    earliest_index: u64,
    buf: [u8; 128],
}

fn shard_of(key_hash: u64) -> usize {
    (key_hash as usize) & (SHARD_COUNT - 1)
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
        s.in_check = in_chan;
        s.out_result = out_chan;
        s.out_metrics = dev_channel_port(sys, 1, 1);
        s.ttl_ms = 259_200_000;
        s.gc_interval_ms = 60_000;
        for shard in 0..SHARD_COUNT {
            for i in 0..SLOTS_PER_SHARD {
                s.shards[shard][i] = DedupEntry::zero();
            }
        }
        dev_log(sys, 3, b"[dedup] init".as_ptr(), 12);
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

        // Drain dedup checks
        if s.in_check >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_check, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_check, &mut s.buf);
                // check_in is a shared bus from session_processor.messaging_out;
                // skip retained writes/reads and any other tenant on the wire.
                if mt == wire::MSG_APPLY_RESET_FANOUT {
                    // Phase 4 apply-pipeline reset: wipe all shards. Phase 5
                    // snapshots will repopulate; until then the next
                    // MSG_DEDUP_CHECK after a reset records the entry fresh.
                    for shard in 0..SHARD_COUNT {
                        for i in 0..SLOTS_PER_SHARD {
                            s.shards[shard][i] = DedupEntry::zero();
                        }
                    }
                    continue;
                }
                if mt != wire::MSG_DEDUP_CHECK { continue; }
                if plen < 20 { continue; }
                let (tenant, stream_hash, epoch, msg_id) = wire::decode_dedup_key(&s.buf);
                let key_hash = stream_hash
                    .wrapping_add(epoch as u64)
                    .wrapping_add((msg_id as u64).rotate_left(17));
                let shard = shard_of(key_hash);

                s.checks = s.checks.wrapping_add(1);

                // Look up in shard
                let mut duplicate = false;
                let mut free_slot: Option<usize> = None;
                for i in 0..SLOTS_PER_SHARD {
                    let e = &s.shards[shard][i];
                    if e.active == 1
                        && e.tenant == tenant
                        && e.stream_hash == stream_hash
                        && e.epoch == epoch
                        && e.msg_id == msg_id
                    {
                        duplicate = true;
                        break;
                    }
                    if e.active == 0 && free_slot.is_none() {
                        free_slot = Some(i);
                    }
                }

                if duplicate {
                    s.hits = s.hits.wrapping_add(1);
                } else if let Some(i) = free_slot {
                    s.shards[shard][i] = DedupEntry {
                        tenant, stream_hash, epoch, msg_id,
                        publish_index: 0,
                        expiry_ms: now.wrapping_add(s.ttl_ms),
                        active: 1,
                    };
                    s.inserts = s.inserts.wrapping_add(1);
                }

                // Emit result: [dedup_key (20)] [duplicate: u8]
                let mut r = [0u8; 21];
                r[..20].copy_from_slice(&s.buf[..20]);
                r[20] = duplicate as u8;
                if s.out_result >= 0 {
                    let poll_out = (sys.channel_poll)(s.out_result, 0x02);
                    if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                        wire::channel_write_msg(sys, s.out_result, wire::MSG_DEDUP_RESULT, &r);
                    }
                }
            }
        }

        // Periodic GC
        if now.wrapping_sub(s.last_gc_ms) >= s.gc_interval_ms {
            s.last_gc_ms = now;
            for shard in 0..SHARD_COUNT {
                for i in 0..SLOTS_PER_SHARD {
                    if s.shards[shard][i].active == 1 && s.shards[shard][i].expiry_ms <= now {
                        s.shards[shard][i].active = 0;
                        s.evicted = s.evicted.wrapping_add(1);
                    }
                }
            }
        }

        // Metrics
        if s.out_metrics >= 0 {
            let mut m = [0u8; 24];
            m[0..4].copy_from_slice(&s.checks.to_le_bytes());
            m[4..8].copy_from_slice(&s.hits.to_le_bytes());
            m[8..12].copy_from_slice(&s.inserts.to_le_bytes());
            m[12..16].copy_from_slice(&s.evicted.to_le_bytes());
            m[16..24].copy_from_slice(&s.earliest_index.to_le_bytes());
            let poll = (sys.channel_poll)(s.out_metrics, 0x02);
            if poll > 0 && (poll as u32 & 0x02) != 0 && now.wrapping_sub(s.last_gc_ms) < 10 {
                // Only emit metrics occasionally to avoid flood
                wire::channel_write_msg(sys, s.out_metrics, wire::MSG_METRICS, &m);
            }
        }

        0
    }
}
