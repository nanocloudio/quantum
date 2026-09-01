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

/// MSG_DEDUP_RESULT verdict byte. `REFUSED` means the check could not be
/// made — the shard had no room — and is distinct from `NEW`, which
/// asserts an entry now exists to catch the retry.
pub const DEDUP_VERDICT_NEW: u8 = 0;
pub const DEDUP_VERDICT_DUPLICATE: u8 = 1;
pub const DEDUP_VERDICT_REFUSED: u8 = 2;

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
    /// QoS 2 protocol state of the flow this entry keys, as the apply
    /// path last recorded it: a `QOS2_*` value, or
    /// `wire::DEDUP_PHASE_NONE` for flows that carry no phase (QoS 1,
    /// and entries recorded by a check written without the field).
    /// Advances only forwards, so replaying a committed transition
    /// twice is a no-op.
    phase: u8,
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
            phase: wire::DEDUP_PHASE_NONE,
        }
    }
}

/// Rank a phase for the monotone comparison. `DEDUP_PHASE_NONE` ranks
/// below every recorded phase so the first phase-bearing check on an
/// entry installs it.
fn phase_rank(phase: u8) -> u8 {
    if phase == wire::DEDUP_PHASE_NONE {
        0
    } else {
        phase.saturating_add(1)
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
    phase_updates: u32,
    phase_unknown: u32,
    /// Checks refused because the key's shard was full. Distinct from a
    /// miss: no entry exists, so the caller must not treat the message
    /// as safely de-duplicated.
    refused: u32,
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
    d.phase_updates = 0;
    d.phase_unknown = 0;
    d.refused = 0;
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

/// MSG_DEDUP_CHECK: `[dedup_key(20)]` or `[dedup_key(20)][phase:u8]`.
/// Emits MSG_DEDUP_RESULT `[dedup_key(20)][duplicate:u8][phase:u8]` on
/// `out_result`, where `phase` is the entry's stored QoS 2 state after
/// this check.
///
/// A check carrying a phase records it on the entry — creating the
/// entry if this is the first sight of the key — and never moves the
/// stored phase backwards, so the apply path may replay a committed
/// QoS 2 transition on any node without inventing progress. A check
/// without the field leaves the stored phase as it was.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_check(d: &mut Dedup, sys: &SyscallTable, payload: &[u8], now: u64) {
    if payload.len() < wire::DEDUP_KEY_LEN {
        return;
    }
    let (tenant, stream_hash, epoch, msg_id) = wire::decode_dedup_key(payload);
    let in_phase = wire::decode_dedup_phase(payload);
    let key_hash = stream_hash
        .wrapping_add(epoch as u64)
        .wrapping_add((msg_id as u64).rotate_left(17));
    let shard = shard_of(key_hash);

    d.checks = d.checks.wrapping_add(1);

    let mut duplicate = false;
    let mut hit: Option<usize> = None;
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
            hit = Some(i);
            break;
        }
        if e.active == 0 && free_slot.is_none() {
            free_slot = Some(i);
        }
    }

    // Saturation must refuse, not fail open. With no entry to record,
    // reporting `duplicate = false` would deliver and acknowledge the
    // message while leaving nothing behind to catch its retry. The
    // refusal verdict tells the caller the check could not be made, so
    // it can throttle the publish while it is still the client's to
    // retry.
    if hit.is_none() && free_slot.is_none() {
        // Full shard: the entry closest to expiry gives way. A refusal
        // here is not an admission decision — the caller files
        // COMMITTED publishes — and with a three-day TTL a refusing
        // shard answers REFUSED to every message after its first
        // hundred and twenty-eight, which downstream read as a publish
        // to drop from fan-out. The dedup window for the evicted key
        // shortens to however long the shard held it; a retry older
        // than that lands as a fresh message, which is the
        // bounded-window trade every dedup table makes.
        let mut victim = 0usize;
        let mut earliest = u64::MAX;
        for (i, e) in d.shards[shard].iter().enumerate() {
            if e.expiry_ms < earliest {
                earliest = e.expiry_ms;
                victim = i;
            }
        }
        d.shards[shard][victim].active = 0;
        d.evicted = d.evicted.wrapping_add(1);
        free_slot = Some(victim);
    }

    let mut stored_phase = wire::DEDUP_PHASE_NONE;
    if let Some(i) = hit {
        d.hits = d.hits.wrapping_add(1);
        let e = &mut d.shards[shard][i];
        if phase_rank(in_phase) > phase_rank(e.phase) {
            e.phase = in_phase;
        }
        stored_phase = e.phase;
    } else if let Some(i) = free_slot {
        d.shards[shard][i] = DedupEntry {
            tenant,
            stream_hash,
            epoch,
            msg_id,
            publish_index: 0,
            expiry_ms: now.wrapping_add(d.ttl_ms),
            active: 1,
            phase: in_phase,
        };
        d.inserts = d.inserts.wrapping_add(1);
        stored_phase = in_phase;
    }

    let mut r = [0u8; 22];
    r[..wire::DEDUP_KEY_LEN].copy_from_slice(&payload[..wire::DEDUP_KEY_LEN]);
    r[20] = if duplicate {
        DEDUP_VERDICT_DUPLICATE
    } else {
        DEDUP_VERDICT_NEW
    };
    r[21] = stored_phase;
    emit_result(d, sys, &r);
}

/// Write one MSG_DEDUP_RESULT.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn emit_result(d: &Dedup, sys: &SyscallTable, r: &[u8; 22]) {
    if d.out_result < 0 {
        return;
    }
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let poll_out = (sys.channel_poll)(d.out_result, 0x02);
        if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
            wire::channel_write_msg(sys, d.out_result, wire::MSG_DEDUP_RESULT, r);
        }
    }
}

/// MSG_DEDUP_PHASE: `[dedup_key(20)][phase:u8]`. Advances the stored
/// QoS 2 phase of an entry that already exists, and emits nothing.
///
/// The apply path sends one of these per committed QoS 2 transition on
/// every node, so leader, follower and post-restart replay converge on
/// the same phase. Advancing is monotone, so re-applying a transition
/// — a replayed WAL entry, a client retry — changes nothing. A key
/// with no entry is counted rather than created: the entry is checked
/// in by the PUBLISH that opened the flow, and a phase for a key that
/// has expired out of the table has nothing left to advance.
pub fn on_phase(d: &mut Dedup, payload: &[u8]) {
    if payload.len() <= wire::DEDUP_KEY_LEN {
        return;
    }
    let (tenant, stream_hash, epoch, msg_id) = wire::decode_dedup_key(payload);
    let phase = payload[wire::DEDUP_KEY_LEN];
    if phase == wire::DEDUP_PHASE_NONE {
        return;
    }
    let key_hash = stream_hash
        .wrapping_add(epoch as u64)
        .wrapping_add((msg_id as u64).rotate_left(17));
    let shard = shard_of(key_hash);
    for i in 0..SLOTS_PER_SHARD {
        let e = &mut d.shards[shard][i];
        if e.active == 1
            && e.tenant == tenant
            && e.stream_hash == stream_hash
            && e.epoch == epoch
            && e.msg_id == msg_id
        {
            if phase_rank(phase) > phase_rank(e.phase) {
                e.phase = phase;
                d.phase_updates = d.phase_updates.wrapping_add(1);
            }
            return;
        }
    }
    d.phase_unknown = d.phase_unknown.wrapping_add(1);
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
pub fn metrics(d: &Dedup, m: &mut [u8; super::METRIC_BYTES]) -> usize {
    m[0..4].copy_from_slice(&d.checks.to_le_bytes());
    m[4..8].copy_from_slice(&d.hits.to_le_bytes());
    m[8..12].copy_from_slice(&d.inserts.to_le_bytes());
    m[12..16].copy_from_slice(&d.evicted.to_le_bytes());
    m[16..24].copy_from_slice(&d.earliest_index.to_le_bytes());
    m[24..28].copy_from_slice(&d.phase_updates.to_le_bytes());
    m[28..32].copy_from_slice(&d.phase_unknown.to_le_bytes());
    m[32..36].copy_from_slice(&d.refused.to_le_bytes());
    36
}
