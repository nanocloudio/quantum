// Bounded, no_std, no-alloc KAFKA PARTITION LOG core — segment framing,
// the sparse offset/time indexes, watermarks, retention and roll
// decisions. `include!`d by the host test crate and by the modules that
// own a partition's log.
//
// ## Why this exists
//
// Kafka's user-visible log and Raft's replication log are different
// things, and conflating them costs data: an offset that is a WAL index
// is a replication detail leaking into a client-visible identity, and a
// Fetch served from a bounded in-memory ring loses records the broker
// has already acknowledged as soon as a consumer falls behind.
//
// The separation this core encodes:
//
//   * the Raft WAL is the REPLICATION mechanism;
//   * the partition log is the RETAINED USER LOG.
//
// Raft commits an append; the apply path writes it here; Fetch reads
// from here. Retention deletes from here and NEVER from the WAL.
//
// ## What is deliberately NOT here
//
// I/O. Segments live behind the fluxor storage capability contract so
// loam (tiering, S3 offload) and a local-file provider are
// interchangeable — no backend is named here. This core owns segment
// SEMANTICS — offset math, index lookup, retention and roll boundaries
// — which is the part that must be identical whichever provider holds
// the bytes.

/// One log record's fixed header, ahead of the payload:
///   `[len:u32 LE][offset:u64 LE][timestamp_ms:u64 LE][flags:u8]`
///
/// `len` covers the payload only. The offset is stored explicitly
/// rather than inferred from position: a Fetch that lands mid-segment
/// must be able to answer "which offset is this?" without replaying
/// the segment from its base.
pub const LOG_REC_HDR: usize = 21;

/// `flags` bit 0: payload is a Kafka record batch v2 (baseOffset already
/// patched to this record's offset). Clear = opaque payload.
pub const LOG_FLAG_KAFKA_BATCH: u8 = 0x01;

/// Encode a log record header. Returns bytes written, or 0 if `buf` is
/// too small.
pub fn encode_log_record(buf: &mut [u8], len: u32, offset: u64, ts_ms: u64, flags: u8) -> usize {
    if buf.len() < LOG_REC_HDR {
        return 0;
    }
    buf[0..4].copy_from_slice(&len.to_le_bytes());
    buf[4..12].copy_from_slice(&offset.to_le_bytes());
    buf[12..20].copy_from_slice(&ts_ms.to_le_bytes());
    buf[20] = flags;
    LOG_REC_HDR
}

/// Decode a log record header into `(payload_len, offset, ts_ms, flags)`.
/// `None` on a short buffer — a torn record must not read as a
/// zero-length record at offset 0.
pub fn decode_log_record(buf: &[u8]) -> Option<(u32, u64, u64, u8)> {
    if buf.len() < LOG_REC_HDR {
        return None;
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let offset = u64::from_le_bytes([
        buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11],
    ]);
    let ts = u64::from_le_bytes([
        buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
    ]);
    Some((len, offset, ts, buf[20]))
}

/// Sparse index entries per segment. One entry per `index_interval`
/// bytes, so a lookup scans at most that much of the segment.
pub const MAX_INDEX_ENTRIES: usize = 64;

/// A segment's sparse offset and time indexes.
///
/// Both are sparse by design: a dense index would cost more than the
/// data. A lookup finds the greatest indexed position at or below the
/// target and scans forward from there, so the index only has to be
/// monotone, never complete.
#[derive(Clone, Copy)]
pub struct SegmentIndex {
    /// Offset of the segment's first record. Index entries store
    /// offsets relative to this, so a segment needs only u32s.
    pub base_offset: u64,
    rel_offset: [u32; MAX_INDEX_ENTRIES],
    position: [u32; MAX_INDEX_ENTRIES],
    timestamp: [u64; MAX_INDEX_ENTRIES],
    count: u8,
    /// Bytes since the last index entry.
    since_entry: u32,
    /// Bytes appended to this segment.
    pub size_bytes: u32,
    /// Largest timestamp seen. Retention by time compares against this:
    /// a segment is only expendable once its NEWEST record is old.
    pub max_timestamp: u64,
    /// Offset one past the last record.
    pub next_offset: u64,
}

impl SegmentIndex {
    pub const fn new(base_offset: u64) -> Self {
        Self {
            base_offset,
            rel_offset: [0; MAX_INDEX_ENTRIES],
            position: [0; MAX_INDEX_ENTRIES],
            timestamp: [0; MAX_INDEX_ENTRIES],
            count: 0,
            since_entry: 0,
            size_bytes: 0,
            max_timestamp: 0,
            next_offset: base_offset,
        }
    }

    pub fn entries(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.next_offset == self.base_offset
    }

    /// Record an append of `bytes` at `offset`/`ts_ms`, adding an index
    /// entry when `index_interval` bytes have passed since the last.
    ///
    /// Returns false if the index is full AND an entry was due — the
    /// caller must roll the segment rather than let the index go stale,
    /// because a stale index silently lengthens every later lookup scan.
    pub fn on_append(&mut self, offset: u64, ts_ms: u64, bytes: u32, index_interval: u32) -> bool {
        let due = self.count == 0 || self.since_entry >= index_interval;
        if due {
            if self.count as usize >= MAX_INDEX_ENTRIES {
                return false;
            }
            let i = self.count as usize;
            self.rel_offset[i] = (offset - self.base_offset) as u32;
            self.position[i] = self.size_bytes;
            self.timestamp[i] = ts_ms;
            self.count += 1;
            self.since_entry = 0;
        }
        self.since_entry = self.since_entry.saturating_add(bytes);
        self.size_bytes = self.size_bytes.saturating_add(bytes);
        if ts_ms > self.max_timestamp {
            self.max_timestamp = ts_ms;
        }
        self.next_offset = offset + 1;
        true
    }

    /// Byte position to start scanning from to reach `offset`.
    ///
    /// The greatest indexed position at or below `offset`; 0 when the
    /// target precedes the first entry. Never overshoots — overshooting
    /// would skip records a Fetch must return.
    pub fn position_for_offset(&self, offset: u64) -> u32 {
        if offset <= self.base_offset || self.count == 0 {
            return 0;
        }
        let rel = (offset - self.base_offset).min(u32::MAX as u64) as u32;
        let mut pos = 0u32;
        let mut i = 0usize;
        while i < self.count as usize {
            if self.rel_offset[i] <= rel {
                pos = self.position[i];
            } else {
                break;
            }
            i += 1;
        }
        pos
    }

    /// Greatest indexed offset whose timestamp is <= `ts_ms`, for
    /// `ListOffsets` by time. Returns the segment's base when every
    /// indexed record is newer.
    pub fn offset_for_timestamp(&self, ts_ms: u64) -> u64 {
        let mut off = self.base_offset;
        let mut i = 0usize;
        while i < self.count as usize {
            if self.timestamp[i] <= ts_ms {
                off = self.base_offset + self.rel_offset[i] as u64;
            } else {
                break;
            }
            i += 1;
        }
        off
    }
}

/// Whether a segment should be rolled before appending `bytes`.
///
/// Rolls on size or age. Rolling on age matters even for a quiet
/// partition: time-based retention can only delete whole segments, so a
/// segment that never rolls is a segment that never expires.
pub fn should_roll(
    seg: &SegmentIndex,
    bytes: u32,
    now_ms: u64,
    segment_bytes: u32,
    segment_ms: u64,
    first_append_ms: u64,
) -> bool {
    if seg.is_empty() {
        return false; // never roll an empty segment — it would spin
    }
    if seg.size_bytes.saturating_add(bytes) > segment_bytes {
        return true;
    }
    segment_ms > 0 && now_ms.saturating_sub(first_append_ms) >= segment_ms
}

/// A partition log's watermarks.
#[derive(Clone, Copy)]
pub struct Watermarks {
    /// Oldest offset still retained. Advances only by segment deletion.
    pub log_start_offset: u64,
    /// Highest offset that has been committed. Fetch serves BELOW this:
    /// an uncommitted record is one a leader change can still erase.
    pub high_watermark: u64,
    /// Offset one past the last appended record.
    pub log_end_offset: u64,
}

impl Watermarks {
    pub const fn new() -> Self {
        Self {
            log_start_offset: 0,
            high_watermark: 0,
            log_end_offset: 0,
        }
    }

    /// Records a Fetch at `offset` may return, capped at the high
    /// watermark. `None` when the offset is out of range — the caller
    /// answers OFFSET_OUT_OF_RANGE rather than guessing.
    pub fn readable_at(&self, offset: u64) -> Option<u64> {
        if offset < self.log_start_offset || offset > self.high_watermark {
            return None;
        }
        Some(self.high_watermark - offset)
    }
}

impl Default for Watermarks {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether the oldest segment may be deleted for retention.
///
/// Deletion is by WHOLE SEGMENT and never crosses the high watermark: a
/// segment holding uncommitted records is not expendable, and partial
/// truncation would leave a consumer positioned inside a hole.
pub fn segment_expendable(
    seg: &SegmentIndex,
    watermarks: &Watermarks,
    total_bytes: u64,
    now_ms: u64,
    retention_ms: u64,
    retention_bytes: u64,
) -> bool {
    // Never delete a segment that still holds uncommitted records.
    if seg.next_offset > watermarks.high_watermark {
        return false;
    }
    if retention_ms > 0 && now_ms.saturating_sub(seg.max_timestamp) >= retention_ms {
        return true;
    }
    // Size retention: the caller offers the OLDEST segment, so deleting
    // it is precisely what brings the total down. Being over the limit
    // is therefore the whole condition — an earlier draft wrote
    // `total_bytes - seg.size_bytes >= 0`, which is vacuously true for
    // an unsigned type and could underflow.
    retention_bytes > 0 && total_bytes > retention_bytes
}

/// True when a stored record has aged past the retention window.
///
/// `entry_age_ms` is measured on the BROKER's clock, from the moment
/// the record was appended — Kafka's `LogAppendTime`, not the
/// producer's `CreateTime`.
///
/// That choice is forced, not stylistic. The runtime's only clock
/// (`dev_millis`) is MONOTONIC — milliseconds since boot — while a
/// producer's `firstTimestamp` is Unix epoch milliseconds. Subtracting
/// one from the other is meaningless: with a small monotonic `now` and
/// a ~1.8e12 epoch timestamp the difference saturates to zero and
/// NOTHING ever expires, which is precisely how retention silently
/// did nothing at all. Two clocks that are not the same clock cannot be
/// compared, and the broker's is the only one it owns.
///
/// It is also the stronger semantic: append time is immune to producer
/// clock skew entirely, so a producer with a wrong clock can neither
/// keep its records for ever nor have them deleted on arrival.
///
/// `retention_ms == 0` means "retain for ever", the behaviour of a
/// broker with no policy set and the safe default: a misread config
/// must never start deleting data.
#[inline]
pub fn retention_expired(entry_age_ms: u64, retention_ms: u64) -> bool {
    retention_ms > 0 && entry_age_ms >= retention_ms
}

/// Age of a record appended at `appended_ms`, on the broker's monotonic
/// clock.
///
/// Saturating: a stored time AHEAD of `now` cannot happen on a
/// monotonic clock, but reading it as an enormous age (which wrapping
/// subtraction would) would expire the newest records first — the exact
/// opposite of retention — so the impossible case is pinned to age 0
/// rather than left to underflow.
#[inline]
pub fn entry_age_ms(appended_ms: u64, now_ms: u64) -> u64 {
    now_ms.saturating_sub(appended_ms)
}

/// Fixed part of a RecordBatch v2 header, before the first record.
///
/// `baseOffset(8) batchLength(4) partitionLeaderEpoch(4) magic(1)
/// crc(4) attributes(2) lastOffsetDelta(4) firstTimestamp(8)
/// maxTimestamp(8) producerId(8) producerEpoch(2) baseSequence(4)
/// recordCount(4)` = 61.
pub const BATCH_HEADER_LEN: usize = 61;

/// Byte offset of `recordCount` in that header.
pub const BATCH_RECORD_COUNT_OFF: usize = 57;

/// Decode one zigzag varint. Returns `(value, bytes_consumed)`.
///
/// Kafka's record fields are zigzag-encoded signed varints, so a
/// NEGATIVE length is meaningful: -1 is the null sentinel for a key or
/// value, and a null key is precisely what a tombstone is not — a
/// record with a key and a null VALUE is the tombstone. Decoding these
/// as unsigned would turn -1 into a huge length and walk off the batch.
#[inline]
pub fn zigzag_varint(buf: &[u8], pos: usize) -> Option<(i64, usize)> {
    let mut raw: u64 = 0;
    let mut shift = 0u32;
    let mut i = pos;
    loop {
        if i >= buf.len() || shift > 63 {
            return None;
        }
        let b = buf[i];
        raw |= ((b & 0x7f) as u64) << shift;
        i += 1;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    // Zigzag: (n >> 1) ^ -(n & 1)
    let val = ((raw >> 1) as i64) ^ -((raw & 1) as i64);
    Some((val, i - pos))
}

/// Number of records a batch claims to hold, or `None` if `buf` is too
/// short to be a v2 batch.
#[inline]
pub fn batch_record_count(buf: &[u8]) -> Option<i32> {
    if buf.len() < BATCH_HEADER_LEN {
        return None;
    }
    let b = &buf[BATCH_RECORD_COUNT_OFF..BATCH_RECORD_COUNT_OFF + 4];
    Some(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

/// Walk a batch's records, calling `f(key, is_tombstone)` for each.
///
/// `key` is `None` for a null key — such a record can never be
/// compacted away, because compaction is keyed dedup and a record with
/// no key has no identity to be superseded by.
///
/// `is_tombstone` is true for a record with a key and a NULL value:
/// Kafka's delete marker.
///
/// Returns false when the batch is malformed — a truncated varint, a
/// length that runs past the end, or a record count the body cannot
/// support. A malformed batch is never partially reported: the caller
/// gets `false` and should leave the batch alone rather than act on a
/// half-walked record set.
pub fn for_each_record_key(buf: &[u8], mut f: impl FnMut(Option<&[u8]>, bool)) -> bool {
    let Some(count) = batch_record_count(buf) else {
        return false;
    };
    if count < 0 {
        return false;
    }
    let mut pos = BATCH_HEADER_LEN;
    for _ in 0..count {
        let Some((rec_len, n)) = zigzag_varint(buf, pos) else {
            return false;
        };
        pos += n;
        if rec_len < 0 {
            return false;
        }
        let rec_end = pos.saturating_add(rec_len as usize);
        if rec_end > buf.len() {
            return false;
        }
        // attributes(1) timestampDelta(varint) offsetDelta(varint)
        let mut p = pos + 1;
        let Some((_, n)) = zigzag_varint(buf, p) else {
            return false;
        };
        p += n;
        let Some((_, n)) = zigzag_varint(buf, p) else {
            return false;
        };
        p += n;
        let Some((key_len, n)) = zigzag_varint(buf, p) else {
            return false;
        };
        p += n;
        let key = if key_len < 0 {
            None
        } else {
            let ke = p.saturating_add(key_len as usize);
            if ke > rec_end {
                return false;
            }
            let k = &buf[p..ke];
            p = ke;
            Some(k)
        };
        let Some((val_len, _)) = zigzag_varint(buf, p) else {
            return false;
        };
        f(key, key.is_some() && val_len < 0);
        pos = rec_end;
    }
    true
}

/// Mark which scanned batches may be compacted away.
///
/// `n` batches, indexed OLDEST-FIRST. `has_key(i)` is true when batch
/// `i` is a single-keyed-record batch — the only kind that can ever be
/// dropped. `same_key(i, j)` compares two such batches' keys EXACTLY.
///
/// A batch is droppable when a LATER batch carries the same key, i.e.
/// its value has been superseded. Anything without a compactable key is
/// never droppable: a multi-record batch, a null key and a tombstone
/// all report `has_key(i) == false`, and each for a reason that matters
/// —
///
/// - a multi-record batch may hold records that are NOT superseded, and
///   dropping the batch would take them with it;
/// - a null key has no identity for a later record to replace;
/// - a tombstone must outlive what it deletes, so dropping it because a
///   later record shares its key would resurrect the deleted value.
///
/// Returns the number marked. This is the half of compaction that can
/// destroy live data, so it lives here where it is host-testable rather
/// than inside the ring walk.
pub fn mark_droppable(
    n: usize,
    has_key: impl Fn(usize) -> bool,
    same_key: impl Fn(usize, usize) -> bool,
    out: &mut [bool],
) -> usize {
    let n = n.min(out.len());
    let mut marked = 0;
    for (i, slot) in out.iter_mut().enumerate().take(n) {
        *slot = false;
        if !has_key(i) {
            continue;
        }
        for j in (i + 1)..n {
            if has_key(j) && same_key(i, j) {
                *slot = true;
                marked += 1;
                break;
            }
        }
    }
    marked
}

/// How many batches may actually be evicted, given the marks.
///
/// A ring can only evict from the TAIL, so only the leading run of
/// droppable batches can go — the first batch that must be kept stops
/// the sweep, however many droppable batches sit behind it. Bounded by
/// `budget` so one tick cannot walk an unbounded eviction.
pub fn droppable_prefix(droppable: &[bool], n: usize, budget: u32) -> u32 {
    let mut k = 0u32;
    let n = n.min(droppable.len());
    while (k as usize) < n && k < budget && droppable[k as usize] {
        k += 1;
    }
    k
}

/// Sparse map from Kafka offset to the RAFT LOG INDEX that carried it.
///
/// The Kafka log IS the raft WAL — a produce is a replicated proposal —
/// so a record evicted from the in-memory ring is still on disk. What is
/// missing is the way back: a Fetch names a Kafka OFFSET and the WAL is
/// addressed by raft INDEX. Without this map a consumer that falls
/// behind the ring can only be told OFFSET_OUT_OF_RANGE for data the
/// broker still holds.
///
/// Sparse on purpose. One entry per record would cost more than the ring
/// it is meant to outlive; anchors every `interval` offsets let a reader
/// start from the nearest anchor at or below the target and walk
/// forward, which is exactly how Kafka's own `.index` files work.
///
/// Monotone in both fields by construction: offsets are assigned in
/// order by the log and raft indices are assigned in order by the
/// consensus layer, so a non-monotone insert means the caller has
/// confused two partitions' streams and is refused rather than stored.
pub const OFFSET_ANCHORS: usize = 64;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OffsetIndex {
    offsets: [u64; OFFSET_ANCHORS],
    indices: [u64; OFFSET_ANCHORS],
    /// Raft partition each anchor's entry lives in. Raft indexes are
    /// per-log — every partition numbers from 1 — so an index without
    /// its partition names nothing; and a partition's log can only be
    /// read by asking that partition's WAL.
    partitions: [u16; OFFSET_ANCHORS],
    /// Anchors held, oldest first. Saturates: see `note_append`.
    len: u16,
    /// Offset one past the last anchor recorded, so `interval` spacing
    /// survives eviction of older anchors.
    next_anchor_at: u64,
}

impl Default for OffsetIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetIndex {
    pub const fn new() -> Self {
        Self {
            offsets: [0; OFFSET_ANCHORS],
            indices: [0; OFFSET_ANCHORS],
            partitions: [0; OFFSET_ANCHORS],
            len: 0,
            next_anchor_at: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Record that `offset` was carried by raft entry `raft_index` of
    /// raft partition `partition`.
    ///
    /// Anchors only every `interval` offsets; other calls are cheap
    /// no-ops so the caller can hand it every append without a test.
    /// Returns true when an anchor was actually stored.
    ///
    /// When full, the OLDEST anchor is dropped. That is the right end to
    /// lose: old anchors describe records the WAL may itself have
    /// retired, while the newest describe what a lagging consumer is
    /// most likely to ask for next.
    ///
    /// Offsets must advance on every call — one Kafka partition is one
    /// stream. Raft indexes must advance only against the last anchor
    /// of the SAME raft partition: two partitions number their logs
    /// independently, so a stream that moves between them legitimately
    /// sees the index fall.
    pub fn note_append(
        &mut self,
        offset: u64,
        raft_index: u64,
        partition: u16,
        interval: u64,
    ) -> bool {
        let interval = interval.max(1);
        if self.len > 0 {
            let last = self.offsets[self.len as usize - 1];
            // Non-monotone offsets mean two streams have been mixed.
            if offset <= last {
                return false;
            }
            if let Some(i) = self.last_anchor_of(partition) {
                if raft_index <= self.indices[i] {
                    return false;
                }
            }
        }
        if self.len > 0 && offset < self.next_anchor_at {
            return false;
        }
        if self.len as usize == OFFSET_ANCHORS {
            for i in 1..OFFSET_ANCHORS {
                self.offsets[i - 1] = self.offsets[i];
                self.indices[i - 1] = self.indices[i];
                self.partitions[i - 1] = self.partitions[i];
            }
            self.len -= 1;
        }
        let i = self.len as usize;
        self.offsets[i] = offset;
        self.indices[i] = raft_index;
        self.partitions[i] = partition;
        self.len += 1;
        self.next_anchor_at = offset.saturating_add(interval);
        true
    }

    /// The raft `(partition, index)` to start reading from to reach
    /// `offset`: the nearest anchor at or BELOW it.
    ///
    /// `None` when `offset` predates every anchor held — the record may
    /// still be in the WAL, but this index cannot say where, and
    /// guessing would hand the reader someone else's entries.
    pub fn seek(&self, offset: u64) -> Option<(u16, u64)> {
        self.seek_slot(offset)
            .map(|i| (self.partitions[i], self.indices[i]))
    }

    /// Slot of the nearest anchor at or below `offset`.
    fn seek_slot(&self, offset: u64) -> Option<usize> {
        let n = self.len as usize;
        if n == 0 || offset < self.offsets[0] {
            return None;
        }
        let mut best = 0;
        for i in 0..n {
            if self.offsets[i] > offset {
                break;
            }
            best = i;
        }
        Some(best)
    }

    /// Newest anchor carried by raft partition `partition`, if any.
    fn last_anchor_of(&self, partition: u16) -> Option<usize> {
        let n = self.len as usize;
        (0..n).rev().find(|&i| self.partitions[i] == partition)
    }

    /// The lowest index of raft partition `partition` a reader starting
    /// at `offset` can still need: the first anchor of that partition at
    /// or after the seek point. `Ok(None)` when no such anchor exists —
    /// that partition holds nothing this reader will ask for.
    /// `Err(())` when `offset` cannot be located at all, so nothing can
    /// be proved expendable and the caller must claim everything.
    pub fn needed_from(&self, offset: u64, partition: u16) -> Result<Option<u64>, ()> {
        let Some(start) = self.seek_slot(offset) else {
            return Err(());
        };
        let n = self.len as usize;
        Ok((start..n)
            .find(|&i| self.partitions[i] == partition)
            .map(|i| self.indices[i]))
    }

    /// Every raft partition this index holds an anchor for, as a bitmap
    /// over partition ids below 64. Ids at or above 64 set bit 63, the
    /// same saturation the apply cursor table uses.
    pub fn partitions_seen(&self) -> u64 {
        let n = self.len as usize;
        (0..n).fold(0u64, |m, i| m | (1u64 << self.partitions[i].min(63)))
    }

    /// Oldest offset this index can still locate.
    pub fn floor_offset(&self) -> Option<u64> {
        if self.len == 0 {
            None
        } else {
            Some(self.offsets[0])
        }
    }

    pub fn clear(&mut self) {
        self.len = 0;
        self.next_anchor_at = 0;
    }
}

/// Where a follower must truncate to before appending, given the
/// leader's last common offset.
///
/// Standard Kafka/Raft log-matching: everything above the divergence
/// point goes. Returns the new `log_end_offset`.
pub fn truncate_to(last_common_offset: u64, watermarks: &Watermarks) -> u64 {
    if last_common_offset >= watermarks.log_end_offset {
        return watermarks.log_end_offset;
    }
    // Never truncate below the high watermark: those records are
    // committed, and a leader that asks for it is wrong.
    if last_common_offset < watermarks.high_watermark {
        return watermarks.high_watermark;
    }
    last_common_offset
}
