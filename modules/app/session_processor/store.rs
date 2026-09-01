//! store — the protocol-neutral durable-publish and log primitive.
//!
//! Two structures the session state machine shares across every
//! protocol:
//!
//!   - `parts` — a per-(topic, partition) byte ring holding applied
//!     entries, read back by Kafka Fetch/ListOffsets and by AMQP
//!     Basic.Get, Cancel and the delivery pump.
//!   - `inflight` — durable-publish inflight slots, allocated by Kafka
//!     produce and AMQP publish alike and resolved when quorum
//!     durability lands. `proto` carries KIN_PROTO_* so the resolver
//!     knows which response shape to emit.
//!
//! Both are deliberately protocol-neutral: a per-protocol split of this
//! state is impossible without duplicating it (see
//! [session_decomposition.md](../../../docs/architecture/session_decomposition.md)).
//!
//! ## Per-step bound
//!
//! Every entry point is O(parts) or O(inflight) over fixed tables and
//! returns without blocking. `fetch_into` walks at most 4096 ring
//! entries per call, the bound the standalone path already carried.

use super::{
    KAFKA_INFLIGHT, KAFKA_MAX_TOPIC, KAFKA_SLOT_BASE, KENTRY_HDR, KFLAG_BATCH, KIN_EPOCH_MASK,
    KIN_EPOCH_SHIFT, KIN_KI_MASK, KIN_MAX_PARTS, KSTORE_BYTES, KSTORE_PARTS, KWRAP,
};

/// Component state. Owned exclusively by this subtree; the dispatch
/// table and the other components reach it only through the entry
/// points below.
#[repr(C)]
pub struct Store {
    pub parts: [KPart; KSTORE_PARTS],
    pub inflight: [KafkaInflight; KAFKA_INFLIGHT],
    /// Response bodies parked until a slot's proposals are durable.
    ///
    /// Kept beside the inflight table rather than inside it: only the
    /// deferred-response protocols use one, and widening every entry by
    /// `KIN_RESP_MAX` would cost the produce path memory it never
    /// touches.
    resp: [[u8; KIN_RESP_MAX]; KAFKA_INFLIGHT],
    resp_len: [u16; KAFKA_INFLIGHT],
    pub evictions: u32,
    /// Records dropped by key compaction.
    pub compacted: u32,
    pub full: u32,
    pub batches_applied: u32,
}

/// Wipe the message store. The log is purely apply-derived, so a reset
/// drops it and lets replay repopulate — logical offsets are
/// deterministic in log order.
pub fn reset_parts(s: &mut Store) {
    for p in s.parts.iter_mut() {
        *p = KPart::zero();
    }
}

/// Fields a caller supplies when opening an inflight slot. The publish
/// paths differ only in which of these are meaningful: Kafka sets
/// `api_ver`/`acks`/`kafka_corr` and may span partitions, AMQP sets
/// `channel`/`delivery_tag` and is always single-partition.
#[derive(Clone, Copy)]
pub struct InflightOpen {
    pub conn_id: u8,
    pub proto: u8,
    pub topic_len: u8,
    pub n_parts: u8,
    pub n_done: u8,
    pub api_ver: i16,
    pub acks: i16,
    pub kafka_corr: i32,
    pub channel: u16,
    pub delivery_tag: u64,
    pub ts_ms: u64,
}

/// Open a previously allocated slot. `topic` is truncated to the slot's
/// capacity; per-partition offsets start unassigned (-1).
pub fn inflight_open(s: &mut Store, ki: usize, o: InflightOpen, topic: &[u8]) {
    if ki >= KAFKA_INFLIGHT {
        return;
    }
    let e = &mut s.inflight[ki];
    e.active = 1;
    e.conn_id = o.conn_id;
    e.proto = o.proto;
    e.topic_len = o.topic_len;
    e.n_parts = o.n_parts;
    e.n_done = o.n_done;
    e.n_applied = 0;
    e.api_ver = o.api_ver;
    e.acks = o.acks;
    e.kafka_corr = o.kafka_corr;
    e.channel = o.channel;
    e.delivery_tag = o.delivery_tag;
    e.ts_ms = o.ts_ms;
    for i in 0..KIN_MAX_PARTS {
        e.part_offs[i] = -1;
        e.part_errs[i] = 0;
        e.part_ids[i] = 0;
    }
    let n = if topic.len() > KAFKA_MAX_TOPIC {
        KAFKA_MAX_TOPIC
    } else {
        topic.len()
    };
    e.topic[..n].copy_from_slice(&topic[..n]);
}

/// Seed one partition slot of a multi-partition produce: its Kafka
/// partition id and any error decided before the proposal was made.
/// The offset stays unassigned until durability reports it.
pub fn inflight_set_part(s: &mut Store, ki: usize, pi: usize, id: i32, err: i16) {
    if ki < KAFKA_INFLIGHT && pi < KIN_MAX_PARTS {
        s.inflight[ki].part_ids[pi] = id;
        s.inflight[ki].part_errs[pi] = err;
    }
}

/// True when `ki` is live and still carries `epoch` — the epoch check is
/// what makes a durability round-trip for a freed-and-reused slot safe
/// to drop rather than misattribute.
pub fn inflight_valid(s: &Store, ki: usize, epoch: u32) -> bool {
    ki < KAFKA_INFLIGHT && s.inflight[ki].active == 1 && s.inflight[ki].epoch == epoch
}

/// Release an inflight slot. The next allocation bumps its epoch, so any
/// round-trip still outstanding for this occupant is invalidated.
pub fn inflight_free(s: &mut Store, ki: usize) {
    if ki < KAFKA_INFLIGHT {
        s.inflight[ki].active = 0;
        // Drop any parked response with the slot: a stale body left
        // behind would be emitted by whichever request reuses the slot.
        s.resp_len[ki] = 0;
    }
}

pub fn inflight_proto(s: &Store, ki: usize) -> u8 {
    if ki < KAFKA_INFLIGHT {
        s.inflight[ki].proto
    } else {
        0
    }
}

/// Record the assigned LOGICAL offset for one partition of a
/// multi-partition produce — the offset a consumer will fetch this data
/// at, decided on the apply side.
pub fn inflight_set_part_offset(s: &mut Store, ki: usize, pidx: usize, offset: i64) {
    if ki < KAFKA_INFLIGHT && pidx < KIN_MAX_PARTS {
        s.inflight[ki].part_offs[pidx] = offset;
    }
}

/// Record a per-partition error decided on the apply side (an
/// idempotence verdict), without disturbing the partition id seeded at
/// propose time.
pub fn inflight_set_part_err(s: &mut Store, ki: usize, pidx: usize, err: i16) {
    if ki < KAFKA_INFLIGHT && pidx < KIN_MAX_PARTS {
        s.inflight[ki].part_errs[pidx] = err;
    }
}

/// Largest deferred response body a slot can hold.
///
/// An OffsetCommit response is small — per topic a name plus a count,
/// per partition six bytes — so 512 covers the shapes real consumers
/// send. A response that does not fit is answered immediately instead
/// of being gated, and counted, rather than silently truncated.
pub const KIN_RESP_MAX: usize = 512;

/// Park a response body until the slot's proposals are durable.
pub fn inflight_set_response(s: &mut Store, ki: usize, body: &[u8]) -> bool {
    if ki >= KAFKA_INFLIGHT || body.len() > KIN_RESP_MAX {
        return false;
    }
    s.resp[ki][..body.len()].copy_from_slice(body);
    s.resp_len[ki] = body.len() as u16;
    true
}

/// The parked response body, if any.
pub fn inflight_response(s: &Store, ki: usize) -> &[u8] {
    if ki >= KAFKA_INFLIGHT {
        return &[];
    }
    &s.resp[ki][..s.resp_len[ki] as usize]
}

/// Count one partition as durable. Returns true once every partition in
/// the request has landed, which is when the response may fire.
pub fn inflight_complete_part(s: &mut Store, ki: usize) -> bool {
    if ki >= KAFKA_INFLIGHT {
        return false;
    }
    if s.inflight[ki].n_done < s.inflight[ki].n_parts {
        s.inflight[ki].n_done += 1;
    }
    s.inflight[ki].n_done >= s.inflight[ki].n_parts
}

/// Mark one partition of a produce APPLIED — its logical offset (or its
/// idempotence verdict) is now known.
///
/// Durability and apply are independent post-commit signals with no
/// ordering between them, and the ack routinely arrives FIRST. A
/// response released on durability alone therefore reports an offset
/// apply had not yet assigned. Counting both is what lets the response
/// carry the real one.
pub fn inflight_apply_part(s: &mut Store, ki: usize) {
    if ki < KAFKA_INFLIGHT && s.inflight[ki].n_applied < s.inflight[ki].n_parts {
        s.inflight[ki].n_applied += 1;
    }
}

/// True when every partition of this produce is BOTH quorum-durable and
/// applied — the point at which the response is both safe and complete.
pub fn inflight_ready(s: &Store, ki: usize) -> bool {
    ki < KAFKA_INFLIGHT
        && s.inflight[ki].active == 1
        && s.inflight[ki].n_done >= s.inflight[ki].n_parts
        && s.inflight[ki].n_applied >= s.inflight[ki].n_parts
}

/// True when slot `i` is live and older than `timeout_ms`. The caller
/// decides what a timeout means per protocol (Kafka producers retry on
/// their own request timeout; AMQP confirm-mode publishers wait
/// indefinitely and need an explicit Nack), so expiry is reported here
/// and acted on there.
pub fn inflight_expired(s: &Store, i: usize, now: u64, timeout_ms: u64) -> bool {
    i < KAFKA_INFLIGHT
        && s.inflight[i].active == 1
        && now.wrapping_sub(s.inflight[i].ts_ms) > timeout_ms
}

/// A copy of slot `i`'s record — the component's published shape, the
/// same bytes a port would have carried.
pub fn inflight_get(s: &Store, i: usize) -> Option<KafkaInflight> {
    if i < KAFKA_INFLIGHT && s.inflight[i].active == 1 {
        Some(s.inflight[i])
    } else {
        None
    }
}

/// Stamp a Kafka record batch's base offset into the copy already
/// written to the ring. `push` assigns the logical offset; the batch
/// header carries it at a fixed position in the entry data, so the
/// authoritative value is written back here rather than by the caller
/// reaching into the ring.
pub fn stamp_base_offset(s: &mut Store, pi: usize, pos: usize, offset: u64) {
    if pi >= KSTORE_PARTS || pos + 8 > KSTORE_BYTES {
        return;
    }
    // Byte-wise: `copy_from_slice` emits a length-mismatch panic path the
    // bare-metal PIC link cannot resolve, even though the bound above
    // makes the lengths equal.
    let be = (offset as i64).to_be_bytes();
    for (i, b) in be.iter().enumerate() {
        s.parts[pi].ring[pos + i] = *b;
    }
}

/// Find the oldest raw (non-batch) entry at or past `cursor` in
/// partition `pi`, walking the ring from the tail. Returns
/// `(data_pos, len, offset, remaining_after)`, where `remaining_after`
/// counts further matching entries behind it — AMQP Basic.Get reports
/// that as queue depth; the delivery pump ignores it.
///
/// Batch entries are skipped: they are Kafka record batches, which the
/// queue-semantics readers must not hand out as messages.
pub fn next_raw_entry(s: &Store, pi: usize, cursor: u64) -> Option<(u32, u16, u64, u32)> {
    if pi >= KSTORE_PARTS {
        return None;
    }
    let part = &s.parts[pi];
    let cap = KSTORE_BYTES as u32;
    let mut pos = part.tail;
    let mut left = part.used;
    let mut walked = 0u32;
    let mut found: Option<(u32, u16, u64)> = None;
    let mut remaining_after = 0u32;
    while left > 0 && walked < 4096 {
        walked += 1;
        let (data_pos, len, flags, offset, _nrec) = entry_at(part, pos);
        let start = if pos + 2 > cap
            || u16::from_le_bytes([part.ring[pos as usize], part.ring[pos as usize + 1]]) == KWRAP
        {
            0
        } else {
            pos
        };
        let consumed =
            (data_pos + len as u32) - start + if start == 0 && pos != 0 { cap - pos } else { 0 };
        if flags & KFLAG_BATCH == 0 && offset >= cursor {
            if found.is_none() {
                found = Some((data_pos, len, offset));
            } else {
                remaining_after += 1;
            }
        }
        left = left.saturating_sub(consumed);
        let next = data_pos + len as u32;
        pos = if next >= cap { 0 } else { next };
    }
    found.map(|(d, l, o)| (d, l, o, remaining_after))
}

/// Copy `len` bytes of entry data at ring position `pos` in partition
/// `pi` into `dst`. Returns false — copying nothing — if either range
/// escapes its bounds.
///
/// The copy happens here rather than through a borrowed slice because a
/// caller-side `copy_from_slice` emits a length-mismatch panic path the
/// bare-metal PIC link cannot resolve.
pub fn copy_entry_into(s: &Store, pi: usize, pos: usize, len: usize, dst: &mut [u8]) -> bool {
    if pi >= KSTORE_PARTS || pos + len > KSTORE_BYTES || len > dst.len() {
        return false;
    }
    dst[..len].copy_from_slice(&s.parts[pi].ring[pos..(len + pos)]);
    true
}

// ── Log accessors ───────────────────────────────────────────────────

pub fn next_offset(s: &Store, pi: usize) -> u64 {
    if pi < KSTORE_PARTS {
        s.parts[pi].next_offset
    } else {
        0
    }
}

/// Oldest offset that still logically exists — the floor a cold read
/// must not go below. Distinct from `log_start`, which is only the
/// ring's tail.
#[cfg(feature = "kafka")]
pub fn retention_floor(s: &Store, pi: usize) -> u64 {
    if pi >= KSTORE_PARTS || s.parts[pi].active == 0 {
        return 0;
    }
    s.parts[pi].retention_floor
}

/// The lowest index of raft partition `partition` this store still
/// needs, or `None` when it needs nothing from that log (no active
/// Kafka partition, or none of them holds an anchor there).
///
/// A record below `retention_floor` has been deleted by POLICY and will
/// never be served again, so the entry that carried it is expendable.
/// One below the oldest still-needed index is therefore the highest
/// index it is safe to compact that log at.
///
/// Per raft partition, because raft indexes are per-log: a floor that
/// mixed two logs' indexes would retire live segments in one of them.
#[cfg(feature = "kafka")]
pub fn oldest_needed_raft_index(s: &Store, partition: u16) -> Option<u64> {
    let mut lowest: Option<u64> = None;
    for pi in 0..KSTORE_PARTS {
        if s.parts[pi].active == 0 {
            continue;
        }
        // Seek the FLOOR, not the log start: `log_start` also advances
        // on capacity eviction, and a record evicted from the RAM ring
        // is still served from the WAL by a cold read. Compacting to
        // the ring's tail would delete exactly what the cold-read path
        // exists to fetch.
        let needed = s.parts[pi]
            .offset_index
            .needed_from(s.parts[pi].retention_floor, partition);
        let idx = match needed {
            // No anchor covers it — cannot prove anything is expendable,
            // so claim everything. Fail closed: a floor guessed too high
            // deletes entries a consumer can still ask for.
            Err(()) => return Some(0),
            Ok(None) => continue,
            Ok(Some(idx)) => idx,
        };
        lowest = Some(match lowest {
            Some(cur) if cur <= idx => cur,
            _ => idx,
        });
    }
    lowest
}

/// Every raft partition the store's anchors name, as a bitmap.
#[cfg(feature = "kafka")]
pub fn raft_partitions_seen(s: &Store) -> u64 {
    (0..KSTORE_PARTS)
        .filter(|&pi| s.parts[pi].active != 0)
        .fold(0u64, |m, pi| m | s.parts[pi].offset_index.partitions_seen())
}

pub fn log_start(s: &Store, pi: usize) -> u64 {
    if pi < KSTORE_PARTS {
        s.parts[pi].log_start
    } else {
        0
    }
}

pub fn get_cursor(s: &Store, pi: usize) -> u64 {
    if pi < KSTORE_PARTS {
        s.parts[pi].get_cursor
    } else {
        0
    }
}

pub fn set_get_cursor(s: &mut Store, pi: usize, v: u64) {
    if pi < KSTORE_PARTS {
        s.parts[pi].get_cursor = v;
    }
}

pub fn is_empty(s: &Store, pi: usize) -> bool {
    pi >= KSTORE_PARTS || s.parts[pi].used == 0
}

pub fn init(s: &mut Store) {
    for p in s.parts.iter_mut() {
        *p = KPart::zero();
    }
    for e in s.inflight.iter_mut() {
        *e = KafkaInflight::zero();
    }
    s.evictions = 0;
    s.compacted = 0;
    s.full = 0;
    s.batches_applied = 0;
}

/// Allocate an inflight slot, bumping its reuse epoch. Returns
/// `(index, wire_slot)` where `wire_slot` encodes the epoch so a stale
/// durability round-trip for a freed-and-reused slot is detected.
pub fn inflight_alloc(s: &mut Store) -> Option<(usize, u32)> {
    for i in 0..KAFKA_INFLIGHT {
        if s.inflight[i].active == 0 {
            let epoch = s.inflight[i].epoch.wrapping_add(1);
            s.inflight[i].epoch = epoch;
            return Some((i, kin_encode_slot(i, epoch)));
        }
    }
    None
}

#[inline]
pub fn kin_encode_slot(ki: usize, epoch: u32) -> u32 {
    KAFKA_SLOT_BASE | ((epoch & KIN_EPOCH_MASK) << KIN_EPOCH_SHIFT) | (ki as u32 & KIN_KI_MASK)
}

#[inline]
pub fn kin_decode_slot(ss: u32) -> (usize, u32) {
    (
        (ss & KIN_KI_MASK) as usize,
        (ss >> KIN_EPOCH_SHIFT) & KIN_EPOCH_MASK,
    )
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct KafkaInflight {
    pub active: u8,
    pub conn_id: u8,
    pub topic_len: u8,
    /// KIN_PROTO_* — this table is shared by every protocol whose
    /// publish ack is durability-gated through the ack component.
    pub proto: u8,
    /// Reuse generation. Bumped on every allocation; encoded into the
    /// wire `session_slot` so a stale durability round-trip for a
    /// freed-and-reused slot is detected and dropped. See the module
    /// header comment on EPOCH TAGGING.
    pub epoch: u32,
    /// Multi-partition aggregation (Kafka produce): partitions in this
    /// request and how many have reached durability.
    pub n_parts: u8,
    pub n_done: u8,
    pub api_ver: i16,
    pub acks: i16,
    pub kafka_corr: i32,
    /// AMQP channel (proto == KIN_PROTO_AMQP only).
    pub channel: u16,
    /// AMQP confirm-mode delivery tag (proto == KIN_PROTO_AMQP only).
    pub delivery_tag: u64,
    pub ts_ms: u64,
    pub part_ids: [i32; KIN_MAX_PARTS],
    pub part_offs: [i64; KIN_MAX_PARTS],
    pub part_errs: [i16; KIN_MAX_PARTS],
    /// Partitions whose apply has landed. Paired with `n_done`
    /// (durability) — see `inflight_ready`.
    pub n_applied: u8,
    pub topic: [u8; KAFKA_MAX_TOPIC],
}

impl KafkaInflight {
    pub const fn zero() -> Self {
        Self {
            active: 0,
            conn_id: 0,
            topic_len: 0,
            proto: 0,
            epoch: 0,
            n_parts: 0,
            n_done: 0,
            api_ver: 0,
            acks: 0,
            kafka_corr: 0,
            channel: 0,
            delivery_tag: 0,
            ts_ms: 0,
            part_ids: [0; KIN_MAX_PARTS],
            part_offs: [-1; KIN_MAX_PARTS],
            part_errs: [0; KIN_MAX_PARTS],
            n_applied: 0,
            topic: [0; KAFKA_MAX_TOPIC],
        }
    }
}

#[repr(C)]
pub struct KPart {
    active: u8,
    topic_len: u8,
    pub partition: u16,
    /// High watermark: next logical offset to assign.
    pub next_offset: u64,
    /// Oldest offset still resident in the ring. Advances on EVERY
    /// eviction, including the capacity eviction `push` does when the
    /// ring fills.
    pub log_start: u64,
    /// Oldest offset that still logically EXISTS. Advances only when a
    /// POLICY deletes records — time retention or key compaction —
    /// never on capacity eviction.
    ///
    /// The distinction is what keeps retention honest now that cold
    /// reads exist. Both cases put an offset below `log_start`, but they
    /// mean opposite things: a record evicted for capacity is still on
    /// disk and may be served from the WAL, while a record deleted by
    /// policy must NOT be — a broker that promises deletion and then
    /// serves the data has not deleted it. Cold reads are therefore
    /// bounded below by this, not by `log_start`.
    pub retention_floor: u64,
    /// AMQP Basic.Get consumption cursor (queue semantics over the log;
    /// single-consumer v1 — each Get pops the next raw entry).
    pub get_cursor: u64,
    /// Ring write position (next byte to write).
    pub head: u32,
    /// Ring read position (oldest entry).
    pub tail: u32,
    /// Bytes currently used (0 == empty; disambiguates head == tail).
    pub used: u32,
    topic: [u8; KAFKA_MAX_TOPIC],
    /// Sparse Kafka-offset -> raft-index anchors, so a Fetch for an
    /// offset that has aged out of the ring can still be located in the
    /// WAL. The ring is a cache of the raft log, not the log itself.
    #[cfg(feature = "kafka")]
    pub offset_index: super::kafka_log::OffsetIndex,
    pub ring: [u8; KSTORE_BYTES],
}

impl KPart {
    pub const fn zero() -> Self {
        Self {
            active: 0,
            topic_len: 0,
            partition: 0,
            #[cfg(feature = "kafka")]
            offset_index: super::kafka_log::OffsetIndex::new(),
            next_offset: 0,
            log_start: 0,
            retention_floor: 0,
            get_cursor: 0,
            head: 0,
            tail: 0,
            used: 0,
            topic: [0; KAFKA_MAX_TOPIC],
            ring: [0; KSTORE_BYTES],
        }
    }
}

pub fn find(s: &Store, topic: &[u8], partition: u16) -> Option<usize> {
    for i in 0..KSTORE_PARTS {
        let p = &s.parts[i];
        if p.active == 1 && p.partition == partition && &p.topic[..p.topic_len as usize] == topic {
            return Some(i);
        }
    }
    None
}

pub fn find_or_create(s: &mut Store, topic: &[u8], partition: u16) -> Option<usize> {
    if topic.is_empty() || topic.len() > KAFKA_MAX_TOPIC {
        return None;
    }
    if let Some(i) = find(s, topic, partition) {
        return Some(i);
    }
    for i in 0..KSTORE_PARTS {
        if s.parts[i].active == 0 {
            let p = &mut s.parts[i];
            p.active = 1;
            p.topic_len = topic.len() as u8;
            p.partition = partition;
            p.topic[..topic.len()].copy_from_slice(topic);
            p.next_offset = 0;
            p.log_start = 0;
            p.retention_floor = 0;
            p.get_cursor = 0;
            #[cfg(feature = "kafka")]
            {
                p.offset_index = super::kafka_log::OffsetIndex::new();
            }
            p.head = 0;
            p.tail = 0;
            p.used = 0;
            return Some(i);
        }
    }
    None
}

/// Read the entry header at ring position `pos`. Returns
/// `(next_pos_of_data, len, flags, offset, nrec)`; a KWRAP sentinel is
/// resolved to position 0 first. Caller guarantees `used > 0` and `pos`
/// addresses a real entry boundary (tail or a previous walk result).
pub fn entry_at(p: &KPart, mut pos: u32) -> (u32, u16, u8, u64, u32) {
    let cap = KSTORE_BYTES as u32;
    if pos + 2 > cap
        || u16::from_le_bytes([p.ring[pos as usize], p.ring[pos as usize + 1]]) == KWRAP
    {
        pos = 0;
    }
    let b = pos as usize;
    let len = u16::from_le_bytes([p.ring[b], p.ring[b + 1]]);
    let flags = p.ring[b + 2];
    let offset = u64::from_le_bytes([
        p.ring[b + 3],
        p.ring[b + 4],
        p.ring[b + 5],
        p.ring[b + 6],
        p.ring[b + 7],
        p.ring[b + 8],
        p.ring[b + 9],
        p.ring[b + 10],
    ]);
    let nrec = u32::from_le_bytes([
        p.ring[b + 11],
        p.ring[b + 12],
        p.ring[b + 13],
        p.ring[b + 14],
    ]);
    (pos + KENTRY_HDR as u32, len, flags, offset, nrec)
}

/// The broker-clock append time of the entry at `pos`.
pub fn entry_appended_ms(p: &KPart, pos: u32) -> u64 {
    let (data_pos, _, _, _, _) = entry_at(p, pos);
    let h = data_pos as usize - KENTRY_HDR + 15;
    u64::from_le_bytes([
        p.ring[h],
        p.ring[h + 1],
        p.ring[h + 2],
        p.ring[h + 3],
        p.ring[h + 4],
        p.ring[h + 5],
        p.ring[h + 6],
        p.ring[h + 7],
    ])
}

/// Position immediately after the entry starting at `pos` (post-wrap-resolve).
pub fn entry_end(p: &KPart, pos: u32) -> u32 {
    let (data_pos, len, _, _, _) = entry_at(p, pos);
    data_pos + len as u32
}

/// Drop entries from the tail that have aged past `retention_ms`, up to
/// `budget` per call. Returns how many went.
///
/// Time retention, as distinct from the capacity eviction `push`
/// already does when the ring fills. Without it the only retention
/// policy is "until RAM runs out", so a low-rate topic keeps records
/// for ever and a high-rate one silently keeps minutes — the retention
/// a deployment gets depends on its traffic rather than on what it
/// asked for.
///
/// Walks from the TAIL and stops at the first entry still inside the
/// window: the log is offset-ordered, so an entry newer than the window
/// means every entry after it is too. That also keeps this O(evicted)
/// rather than O(ring) on the common tick where nothing expires.
///
/// The timestamp is read from the stored RecordBatch itself
/// (`batch_first_timestamp`), so the ring's own entry header is
/// unchanged — a format change here would invalidate every WAL that
/// already exists.
///
/// `budget` bounds the work per call: expiring a whole partition in one
/// tick would be an unbounded memmove inside a step that everything
/// else is waiting on.
/// How many offsets apart the WAL anchors are. 64 anchors at this
/// spacing span 32k offsets per partition — comfortably more than the
/// ring holds, which is the point: the index has to outlive the cache
/// it backstops.
#[cfg(feature = "kafka")]
pub const OFFSET_ANCHOR_INTERVAL: u64 = 512;

/// Record that `offset` in partition `pi` was carried by raft entry
/// `raft_index` of raft partition `raft_partition`. Cheap and
/// unconditional — the index anchors sparsely and ignores the rest.
#[cfg(feature = "kafka")]
pub fn note_offset_anchor(
    s: &mut Store,
    pi: usize,
    offset: u64,
    raft_index: u64,
    raft_partition: u16,
) {
    if pi >= KSTORE_PARTS || s.parts[pi].active == 0 || raft_index == 0 {
        return;
    }
    s.parts[pi].offset_index.note_append(
        offset,
        raft_index,
        raft_partition,
        OFFSET_ANCHOR_INTERVAL,
    );
}

/// The raft `(partition, index)` to start reading from to reach
/// `offset` in partition `pi`, or `None` when this index cannot locate
/// it.
#[cfg(feature = "kafka")]
pub fn seek_offset(s: &Store, pi: usize, offset: u64) -> Option<(u16, u64)> {
    if pi >= KSTORE_PARTS || s.parts[pi].active == 0 {
        return None;
    }
    s.parts[pi].offset_index.seek(offset)
}

/// Apply time retention to one partition slot. No-op for an inactive
/// slot, so the caller can sweep the whole table without first asking
/// which slots are live.
#[cfg(feature = "kafka")]
pub fn expire_partition(
    s: &mut Store,
    pi: usize,
    now_ms: u64,
    retention_ms: u64,
    budget: u32,
) -> u32 {
    if pi >= KSTORE_PARTS || s.parts[pi].active == 0 {
        return 0;
    }
    let n = expire_tail(&mut s.parts[pi], now_ms, retention_ms, budget);
    s.evictions = s.evictions.wrapping_add(n);
    n
}

#[cfg(feature = "kafka")]
fn expire_tail(p: &mut KPart, now_ms: u64, retention_ms: u64, budget: u32) -> u32 {
    if retention_ms == 0 {
        return 0;
    }
    let mut dropped = 0u32;
    while dropped < budget && p.used > 0 {
        let age = super::kafka_log::entry_age_ms(entry_appended_ms(p, p.tail), now_ms);
        if !super::kafka_log::retention_expired(age, retention_ms) {
            break;
        }
        evict_tail(p);
        p.retention_floor = p.log_start;
        dropped += 1;
    }
    dropped
}

/// Batches examined by one compaction sweep. Bounds both the walk and
/// the key-descriptor table below.
#[cfg(feature = "kafka")]
const COMPACT_SCAN: usize = 32;

/// Drop tail batches whose single keyed record has been superseded by a
/// later record with the same key. Returns how many went.
///
/// ## What this does and does not do
///
/// Kafka compacts by REWRITING segments, dropping individual superseded
/// records from inside a batch. That needs a batch rewriter — new CRC,
/// re-encoded record set — and this store is a byte RING that can only
/// evict from the tail. Rewriting a batch in place is not something the
/// structure supports, and building a rewriter here would reinvent
/// segment compaction one layer up from a data structure that cannot
/// hold the result.
///
/// So this compacts at BATCH granularity, and only for batches carrying
/// exactly ONE keyed record:
///
/// - a multi-record batch is never dropped (its records may not all be
///   superseded, and dropping the batch would take live records with
///   it);
/// - a record with a NULL key is never superseded — a keyless record
///   has no identity for a later one to replace;
/// - only a CONTIGUOUS run at the tail can go, because that is the only
///   thing a ring can evict.
///
/// That last constraint sounds severe and mostly is not: when the same
/// keys are rewritten over time, the OLDEST records are exactly the ones
/// that have been superseded, so the droppable run tends to sit at the
/// tail where the ring can reach it.
///
/// Supersession is decided by EXACT key comparison, never by hash. A
/// hash collision here would drop a live record — silent data loss —
/// and no collision probability is small enough to trade for that when
/// the exact comparison is a byte-range compare the ring can already do.
#[cfg(feature = "kafka")]
pub fn compact_partition(s: &mut Store, pi: usize, budget: u32) -> u32 {
    if pi >= KSTORE_PARTS || s.parts[pi].active == 0 || s.parts[pi].used == 0 {
        return 0;
    }
    // (key ring position, key length) per scanned batch; `None` when the
    // batch is not a single-keyed-record batch and so is never
    // droppable.
    let mut keys: [Option<(u32, u16)>; COMPACT_SCAN] = [None; COMPACT_SCAN];
    let mut n = 0usize;

    {
        let p = &s.parts[pi];
        let mut pos = p.tail;
        // Bounded by COMPACT_SCAN and terminated by reaching the head.
        // Deliberately NOT bounded by a running byte count: `entry_at`
        // resolves a `KWRAP` sentinel by restarting at 0, so `end - pos`
        // goes negative across a wrap and a saturating byte counter
        // silently stops advancing. The entry count is the honest bound.
        while n < COMPACT_SCAN {
            let (data_pos, len, flags, _, _) = entry_at(p, pos);
            let end = data_pos as usize + len as usize;
            if end > p.ring.len() {
                break;
            }
            if flags & KFLAG_BATCH != 0 {
                keys[n] = single_record_key(p, data_pos, len);
            }
            n += 1;
            let next = entry_end(p, pos);
            pos = if next >= KSTORE_BYTES as u32 { 0 } else { next };
            if pos == p.head {
                break;
            }
        }
    }

    // The decision — which batches are superseded, and how many of them
    // the tail can actually reach — lives in the shared core, where it
    // is host-tested. It is the half of compaction that can destroy
    // live data, so it does not live inside this ring walk.
    let mut droppable = [false; COMPACT_SCAN];
    {
        let part = &s.parts[pi];
        super::kafka_log::mark_droppable(
            n,
            |i| keys[i].is_some(),
            |i, j| match (keys[i], keys[j]) {
                (Some((ap, al)), Some((bp, bl))) => al == bl && keys_equal(part, ap, bp, al),
                _ => false,
            },
            &mut droppable,
        );
    }
    let take = super::kafka_log::droppable_prefix(&droppable, n, budget);
    for _ in 0..take {
        evict_tail(&mut s.parts[pi]);
        // Compaction is a policy deletion too: the superseded record is
        // gone for good, not merely out of the ring.
        s.parts[pi].retention_floor = s.parts[pi].log_start;
        s.compacted = s.compacted.wrapping_add(1);
    }
    take
}

/// The key of a batch carrying exactly one keyed record, as a
/// `(ring position, length)` into the ring. `None` for anything else —
/// a multi-record batch, a null key, a tombstone, or a malformed batch.
///
/// Tombstones are excluded deliberately: a delete marker must outlive
/// the records it deletes, so dropping one because a later record
/// shares its key would resurrect the deleted value.
#[cfg(feature = "kafka")]
fn single_record_key(p: &KPart, data_pos: u32, len: u16) -> Option<(u32, u16)> {
    let d = data_pos as usize;
    let end = d + len as usize;
    if end > p.ring.len() {
        return None;
    }
    let body = &p.ring[d..end];
    if super::kafka_log::batch_record_count(body) != Some(1) {
        return None;
    }
    let mut found: Option<(u32, u16)> = None;
    let mut ok_single = true;
    let walked = super::kafka_log::for_each_record_key(body, |k, tomb| match k {
        Some(k) if !tomb => {
            // Offset of `k` within `body`, translated to a ring position.
            let off = (k.as_ptr() as usize) - (body.as_ptr() as usize);
            found = Some((data_pos + off as u32, k.len() as u16));
        }
        _ => ok_single = false,
    });
    if !walked || !ok_single {
        return None;
    }
    found
}

/// Exact byte comparison of two keys held in the ring.
///
/// Byte by byte through the modulo rather than by slicing. `push`
/// RELOCATES an entry that will not fit contiguously — it writes a
/// `KWRAP` sentinel over the dead tail and restarts at 0 — so an entry
/// never straddles the end and a slice would in fact be correct today.
/// The modulo is cheap insurance against that invariant being relaxed
/// later, not a fix for a wrap that currently happens: if `push` ever
/// learns to split an entry, a slicing comparison here would silently
/// compare the wrong bytes and compaction would drop live records.
#[cfg(feature = "kafka")]
fn keys_equal(p: &KPart, a: u32, b: u32, len: u16) -> bool {
    let cap = KSTORE_BYTES as u32;
    for i in 0..len as u32 {
        let ai = ((a + i) % cap) as usize;
        let bi = ((b + i) % cap) as usize;
        if p.ring[ai] != p.ring[bi] {
            return false;
        }
    }
    true
}

/// Evict the oldest entry; updates tail/used/log_start.
fn evict_tail(p: &mut KPart) {
    if p.used == 0 {
        return;
    }
    let cap = KSTORE_BYTES as u32;
    let mut tail = p.tail;
    // Resolve a wrap sentinel: account the dead bytes to the end of ring.
    if tail + 2 > cap
        || u16::from_le_bytes([p.ring[tail as usize], p.ring[tail as usize + 1]]) == KWRAP
    {
        p.used = p.used.saturating_sub(cap - tail);
        tail = 0;
        p.tail = 0;
        if p.used == 0 {
            p.head = 0;
            p.log_start = p.next_offset;
            return;
        }
    }
    let end = entry_end(p, tail);
    p.used = p.used.saturating_sub(end - tail);
    p.tail = if end >= cap { 0 } else { end };
    if p.used == 0 {
        p.head = 0;
        p.tail = 0;
        p.log_start = p.next_offset;
    } else {
        let (_, _, _, off, _) = entry_at(p, p.tail);
        p.log_start = off;
    }
}

/// Append one entry, evicting from the tail as needed. Returns the
/// entry's assigned base offset (the partition's logical offset counter,
/// advanced by `nrec`). For Kafka batches the caller patches the stored
/// batch's baseOffset field afterwards via the returned data position.
pub fn push(
    s: &mut Store,
    part_idx: usize,
    flags: u8,
    nrec: u32,
    data: &[u8],
    appended_ms: u64,
) -> Option<(u64, usize)> {
    let need = (KENTRY_HDR + data.len()) as u32;
    let cap = KSTORE_BYTES as u32;
    if need > cap / 2 {
        return None;
    }
    let evictions_before = {
        let p = &mut s.parts[part_idx];
        // Wrap if the entry doesn't fit contiguously at head.
        if p.head + need > cap {
            let waste = cap - p.head;
            // Free enough space to account the wrap waste.
            while p.used + waste > cap {
                evict_tail(p);
            }
            if p.head + 2 <= cap {
                let w = KWRAP.to_le_bytes();
                p.ring[p.head as usize] = w[0];
                p.ring[p.head as usize + 1] = w[1];
            }
            p.used += waste;
            p.head = 0;
        }
        let mut ev = 0u32;
        while p.used + need > cap {
            evict_tail(p);
            ev += 1;
        }
        ev
    };
    s.evictions = s.evictions.wrapping_add(evictions_before);
    let p = &mut s.parts[part_idx];
    let offset = p.next_offset;
    let b = p.head as usize;
    p.ring[b..b + 2].copy_from_slice(&(data.len() as u16).to_le_bytes());
    p.ring[b + 2] = flags;
    p.ring[b + 3..b + 11].copy_from_slice(&offset.to_le_bytes());
    p.ring[b + 11..b + 15].copy_from_slice(&nrec.to_le_bytes());
    p.ring[b + 15..b + 23].copy_from_slice(&appended_ms.to_le_bytes());
    p.ring[b + KENTRY_HDR..b + KENTRY_HDR + data.len()].copy_from_slice(data);
    if p.used == 0 {
        p.tail = p.head;
        p.log_start = offset;
    }
    p.head += need;
    if p.head >= cap {
        p.head = 0;
    }
    p.used += need;
    p.next_offset = offset + nrec as u64;
    Some((offset, b + KENTRY_HDR))
}

/// Handle a Kafka Fetch request (api_key 1, v0-v5 non-flexible).
/// Serves stored batches from `fetch_offset` up to the response budget;
/// no long-poll (max_wait is ignored — an empty response returns
/// immediately and the client re-polls).
///
/// # Safety
/// Copy every stored Kafka batch in partition store `pi` whose offset
/// range ends past `fetch_offset` into `s.out_buf[dst..]`, capped at
/// `budget_end`. Returns bytes written. Skips raw (AMQP) entries.
///
/// # Safety
pub fn fetch_into(
    s: &Store,
    pi: usize,
    fetch_offset: i64,
    out: &mut [u8],
    dst: usize,
    budget_end: usize,
) -> usize {
    let mut written = 0usize;
    if s.parts[pi].used == 0 {
        return 0;
    }
    let part = &s.parts[pi];
    let mut pos = part.tail;
    let mut walked = 0u32;
    let mut remaining = part.used;
    while remaining > 0 && walked < 4096 {
        walked += 1;
        let (data_pos, len, flags, offset, nrec) = entry_at(part, pos);
        let start = if pos + 2 > KSTORE_BYTES as u32
            || u16::from_le_bytes([part.ring[pos as usize], part.ring[pos as usize + 1]]) == KWRAP
        {
            0
        } else {
            pos
        };
        let consumed = (data_pos + len as u32) - start
            + if start == 0 && pos != 0 {
                KSTORE_BYTES as u32 - pos
            } else {
                0
            };
        if flags & KFLAG_BATCH != 0 && (offset as i64 + nrec as i64) > fetch_offset {
            let l = len as usize;
            if dst + written + l > budget_end {
                break;
            }
            out[dst + written..dst + written + l]
                .copy_from_slice(&part.ring[data_pos as usize..data_pos as usize + l]);
            written += l;
        }
        remaining = remaining.saturating_sub(consumed);
        let next = data_pos + len as u32;
        pos = if next >= KSTORE_BYTES as u32 { 0 } else { next };
    }
    written
}
