//! dedup — sharded message deduplication with expiry.
//!
//! 16 shards indexed by hash(DedupKey). Each shard has a fixed slot table
//! and maintains an expiry horizon. Periodic GC evicts expired entries
//! and updates `earliest_index` for the WAL compaction floor.
//!
//! Protocol-agnostic; usable for any exactly-once pipeline.
//!
//! ## Per-step bound
//!
//! `on_check` handles ONE check per call; the dispatch table admits at
//! most [`CHECK_BUDGET`] per step. `step` sweeps all 2048 slots at most
//! once per `gc_interval_ms`.

use super::abi::SyscallTable;
use super::wire;

const SHARD_COUNT: usize = 16;
const SLOTS_PER_SHARD: usize = 128;

/// Checks admitted per step. Matches the standalone drain bound.
pub const CHECK_BUDGET: u8 = 16;

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
        Self {
            tenant: 0,
            stream_hash: 0,
            epoch: 0,
            msg_id: 0,
            publish_index: 0,
            expiry_ms: 0,
            active: 0,
        }
    }
}

#[repr(C)]
pub struct Dedup {
    pub out_result: i32,

    ttl_ms: u64,
    gc_interval_ms: u64,
    last_gc_ms: u64,

    shards: [[DedupEntry; SLOTS_PER_SHARD]; SHARD_COUNT],
    checks: u32,
    hits: u32,
    inserts: u32,
    evicted: u32,
    earliest_index: u64,
}

fn shard_of(key_hash: u64) -> usize {
    (key_hash as usize) & (SHARD_COUNT - 1)
}

pub fn init(d: &mut Dedup) {
    d.out_result = -1;
    d.ttl_ms = 259_200_000;
    d.gc_interval_ms = 60_000;
    d.last_gc_ms = 0;
    d.checks = 0;
    d.hits = 0;
    d.inserts = 0;
    d.evicted = 0;
    d.earliest_index = 0;
    for shard in 0..SHARD_COUNT {
        for i in 0..SLOTS_PER_SHARD {
            d.shards[shard][i] = DedupEntry::zero();
        }
    }
}

/// MSG_APPLY_RESET_FANOUT: wipe all shards. Snapshots repopulate; until
/// then the next MSG_DEDUP_CHECK after a reset records the entry fresh.
pub fn on_reset(d: &mut Dedup) {
    for shard in 0..SHARD_COUNT {
        for i in 0..SLOTS_PER_SHARD {
            d.shards[shard][i] = DedupEntry::zero();
        }
    }
}

/// MSG_DEDUP_CHECK: `[dedup_key(20)]`. Emits MSG_DEDUP_RESULT
/// `[dedup_key(20)][duplicate:u8]` on `out_result`.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_check(d: &mut Dedup, sys: &SyscallTable, payload: &[u8], now: u64) {
    if payload.len() < 20 {
        return;
    }
    let (tenant, stream_hash, epoch, msg_id) = wire::decode_dedup_key(payload);
    let key_hash = stream_hash
        .wrapping_add(epoch as u64)
        .wrapping_add((msg_id as u64).rotate_left(17));
    let shard = shard_of(key_hash);

    d.checks = d.checks.wrapping_add(1);

    let mut duplicate = false;
    let mut free_slot: Option<usize> = None;
    for i in 0..SLOTS_PER_SHARD {
        let e = &d.shards[shard][i];
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
        d.hits = d.hits.wrapping_add(1);
    } else if let Some(i) = free_slot {
        d.shards[shard][i] = DedupEntry {
            tenant,
            stream_hash,
            epoch,
            msg_id,
            publish_index: 0,
            expiry_ms: now.wrapping_add(d.ttl_ms),
            active: 1,
        };
        d.inserts = d.inserts.wrapping_add(1);
    }

    let mut r = [0u8; 21];
    r[..20].copy_from_slice(&payload[..20]);
    r[20] = duplicate as u8;
    if d.out_result >= 0 {
        // SAFETY: caller guarantees `sys` is live.
        unsafe {
            let poll_out = (sys.channel_poll)(d.out_result, 0x02);
            if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, d.out_result, wire::MSG_DEDUP_RESULT, &r);
            }
        }
    }
}

/// Periodic expiry sweep.
pub fn step(d: &mut Dedup, now: u64) {
    if now.wrapping_sub(d.last_gc_ms) < d.gc_interval_ms {
        return;
    }
    d.last_gc_ms = now;
    for shard in 0..SHARD_COUNT {
        for i in 0..SLOTS_PER_SHARD {
            if d.shards[shard][i].active == 1 && d.shards[shard][i].expiry_ms <= now {
                d.shards[shard][i].active = 0;
                d.evicted = d.evicted.wrapping_add(1);
            }
        }
    }
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(d: &Dedup, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&d.checks.to_le_bytes());
    m[4..8].copy_from_slice(&d.hits.to_le_bytes());
    m[8..12].copy_from_slice(&d.inserts.to_le_bytes());
    m[12..16].copy_from_slice(&d.evicted.to_le_bytes());
    m[16..24].copy_from_slice(&d.earliest_index.to_le_bytes());
    24
}

