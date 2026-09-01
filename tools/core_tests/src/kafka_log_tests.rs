//! Kafka partition-log core: the boundaries that lose or duplicate data
//! when they are wrong.
//!
//! A log that evicts its oldest records under pressure loses data the
//! broker has already acknowledged, and a consumer that fell behind is
//! the one that pays. The rules below are what make that impossible, so
//! they are tested from the failure side rather than the happy path.

use super::kafka_log::*;

fn seg_with(base: u64, records: &[(u64, u64, u32)], interval: u32) -> SegmentIndex {
    let mut s = SegmentIndex::new(base);
    for (off, ts, bytes) in records {
        assert!(
            s.on_append(*off, *ts, *bytes, interval),
            "index must not fill"
        );
    }
    s
}

// ── Record framing ──────────────────────────────────────────────────────────

#[test]
fn log_record_header_round_trips() {
    let mut buf = [0u8; LOG_REC_HDR];
    assert_eq!(
        encode_log_record(
            &mut buf,
            1234,
            0xDEAD_BEEF,
            1_700_000_000_000,
            LOG_FLAG_KAFKA_BATCH
        ),
        LOG_REC_HDR
    );
    assert_eq!(
        decode_log_record(&buf),
        Some((1234, 0xDEAD_BEEF, 1_700_000_000_000, LOG_FLAG_KAFKA_BATCH))
    );
}

/// A torn record is refused, not read as a zero-length record at offset
/// 0 — which a Fetch would happily return as a real record.
#[test]
fn a_truncated_record_header_is_refused() {
    let mut buf = [0u8; LOG_REC_HDR];
    encode_log_record(&mut buf, 8, 5, 1, 0);
    for short in 0..LOG_REC_HDR {
        assert!(decode_log_record(&buf[..short]).is_none(), "prefix {short}");
    }
    assert!(decode_log_record(&buf).is_some());
}

// ── Offset index ────────────────────────────────────────────────────────────

/// The index never overshoots. A lookup returning a position PAST the
/// target would skip records the Fetch must return — silent data loss
/// that looks like a gap in the consumer's stream.
#[test]
fn index_lookup_never_overshoots() {
    let recs: Vec<(u64, u64, u32)> = (0..40u64).map(|i| (100 + i, 1_000 + i, 50)).collect();
    let s = seg_with(100, &recs, 100);
    // Walk every offset in the segment; the returned position must be
    // at or before the position of that offset's own index entry.
    for target in 100..140u64 {
        let pos = s.position_for_offset(target);
        // Reconstruct the true position of `target` by summing sizes.
        let true_pos = u32::try_from(target - 100).expect("in-segment offset") * 50;
        assert!(
            pos <= true_pos,
            "offset {target}: index said {pos}, record is at {true_pos}"
        );
    }
}

/// An offset at or before the segment base scans from the start.
#[test]
fn lookup_before_the_base_starts_at_zero() {
    let s = seg_with(100, &[(100, 1, 10), (101, 2, 10)], 5);
    assert_eq!(s.position_for_offset(0), 0);
    assert_eq!(s.position_for_offset(100), 0);
}

/// An empty segment yields position 0 for anything — there is nothing
/// to skip.
#[test]
fn empty_segment_lookups_are_safe() {
    let s = SegmentIndex::new(7);
    assert!(s.is_empty());
    assert_eq!(s.position_for_offset(0), 0);
    assert_eq!(s.position_for_offset(9_999), 0);
    assert_eq!(s.offset_for_timestamp(1), 7);
}

/// The index is sparse: entries appear about every `index_interval`
/// bytes, not per record. A dense index would cost more than the data.
#[test]
fn the_index_is_sparse() {
    let recs: Vec<(u64, u64, u32)> = (0..40u64).map(|i| (i, i, 100)).collect();
    let s = seg_with(0, &recs, 1000);
    assert!(s.entries() < 10, "40 records at 100B with a 1000B interval");
    assert!(s.entries() >= 4);
}

/// A full index refuses the append rather than silently going stale —
/// a stale index lengthens every later scan without saying so.
#[test]
fn a_full_index_refuses_rather_than_going_stale() {
    let mut s = SegmentIndex::new(0);
    // interval 0 forces an entry per record.
    for i in 0..MAX_INDEX_ENTRIES as u64 {
        assert!(s.on_append(i, i, 1, 0));
    }
    assert!(
        !s.on_append(MAX_INDEX_ENTRIES as u64, 0, 1, 0),
        "the caller must roll instead"
    );
}

// ── Time index ──────────────────────────────────────────────────────────────

/// `ListOffsets` by time returns the greatest offset at or before the
/// timestamp — never a later one, which would skip messages the
/// consumer asked to start from.
#[test]
fn time_lookup_does_not_overshoot() {
    let recs: Vec<(u64, u64, u32)> = (0..20u64).map(|i| (i, 1_000 + i * 10, 100)).collect();
    let s = seg_with(0, &recs, 0); // entry per record
    assert_eq!(s.offset_for_timestamp(1_000), 0);
    assert_eq!(s.offset_for_timestamp(1_055), 5);
    assert_eq!(s.offset_for_timestamp(1_060), 6);
    assert_eq!(s.offset_for_timestamp(999), 0, "before every record");
    assert_eq!(s.offset_for_timestamp(u64::MAX), 19);
}

// ── Watermarks ──────────────────────────────────────────────────────────────

/// Fetch serves strictly below the high watermark. An uncommitted
/// record is one a leader change can still erase, so returning it makes
/// a consumer observe a write that later un-happens.
#[test]
fn fetch_is_bounded_by_the_high_watermark() {
    let w = Watermarks {
        log_start_offset: 10,
        high_watermark: 50,
        log_end_offset: 73,
    };
    assert_eq!(w.readable_at(10), Some(40));
    assert_eq!(w.readable_at(50), Some(0), "at the watermark, nothing yet");
    assert_eq!(
        w.readable_at(51),
        None,
        "above the watermark is out of range"
    );
    assert_eq!(w.readable_at(73), None, "the log end is not readable");
}

/// An offset below `log_start_offset` — retained data that has since
/// been deleted — is out of range, not silently clamped. Clamping would
/// hand the consumer a different message than it asked for and it would
/// never know it had skipped some.
#[test]
fn an_offset_below_log_start_is_out_of_range() {
    let w = Watermarks {
        log_start_offset: 10,
        high_watermark: 50,
        log_end_offset: 50,
    };
    assert_eq!(w.readable_at(9), None);
    assert_eq!(w.readable_at(0), None);
}

// ── Retention ───────────────────────────────────────────────────────────────

fn wm(start: u64, hw: u64, end: u64) -> Watermarks {
    Watermarks {
        log_start_offset: start,
        high_watermark: hw,
        log_end_offset: end,
    }
}

/// A segment holding uncommitted records is never expendable, whatever
/// retention says. Deleting it would drop records that are still being
/// replicated.
#[test]
fn an_uncommitted_segment_is_never_deleted() {
    let s = seg_with(0, &[(0, 0, 100), (1, 0, 100)], 1000); // next_offset = 2
    let w = wm(0, 1, 2); // high watermark below the segment's end
    assert!(!segment_expendable(&s, &w, 10_000_000, u64::MAX, 1, 1));
}

/// Time retention compares against the segment's NEWEST record. Using
/// the oldest would delete a segment whose tail is still fresh.
#[test]
fn time_retention_uses_the_newest_record() {
    let s = seg_with(0, &[(0, 1_000, 10), (1, 9_000, 10)], 1000);
    let w = wm(0, 2, 2);
    // now - max_timestamp = 10_000 - 9_000 = 1_000
    assert!(
        !segment_expendable(&s, &w, 0, 10_000, 5_000, 0),
        "tail is still fresh"
    );
    assert!(
        segment_expendable(&s, &w, 0, 20_000, 5_000, 0),
        "tail has aged out"
    );
}

/// Size retention only fires above the limit.
#[test]
fn size_retention_fires_only_above_the_limit() {
    let s = seg_with(0, &[(0, 1, 100)], 1000);
    let w = wm(0, 1, 1);
    assert!(!segment_expendable(&s, &w, 500, 1, 0, 1_000));
    assert!(segment_expendable(&s, &w, 2_000, 1, 0, 1_000));
}

/// With both policies off, nothing is ever deleted — retention is
/// opt-in, and a misconfigured topic keeps its data rather than losing
/// it.
#[test]
fn no_retention_policy_deletes_nothing() {
    let s = seg_with(0, &[(0, 1, 100)], 1000);
    let w = wm(0, 1, 1);
    assert!(!segment_expendable(&s, &w, u64::MAX, u64::MAX, 0, 0));
}

// ── Segment roll ────────────────────────────────────────────────────────────

/// An empty segment never rolls — rolling one would spin, creating
/// empty segments forever.
#[test]
fn an_empty_segment_never_rolls() {
    let s = SegmentIndex::new(0);
    assert!(!should_roll(&s, 10_000_000, u64::MAX, 1, 1, 0));
}

/// Rolls on size when the append would exceed the limit.
#[test]
fn rolls_on_size() {
    let s = seg_with(0, &[(0, 1, 900)], 1000);
    assert!(!should_roll(&s, 50, 0, 1000, 0, 0));
    assert!(should_roll(&s, 200, 0, 1000, 0, 0));
}

/// Rolls on age even when small. Time retention deletes whole segments,
/// so a segment that never rolls is a segment that never expires — a
/// quiet partition would retain forever.
#[test]
fn rolls_on_age_so_quiet_partitions_still_expire() {
    let s = seg_with(0, &[(0, 1, 10)], 1000);
    assert!(!should_roll(&s, 1, 500, 1_000_000, 1_000, 0));
    assert!(should_roll(&s, 1, 1_000, 1_000_000, 1_000, 0));
}

// ── Follower truncation ─────────────────────────────────────────────────────

/// A follower truncates to the last common offset.
#[test]
fn truncation_drops_the_divergent_tail() {
    let w = wm(0, 5, 20);
    assert_eq!(truncate_to(12, &w), 12);
}

/// Truncation never goes below the high watermark. Those records are
/// committed; a leader asking for it is wrong, and obeying would lose
/// acknowledged writes.
#[test]
fn truncation_never_crosses_the_high_watermark() {
    let w = wm(0, 15, 20);
    assert_eq!(truncate_to(3, &w), 15, "refused below the watermark");
}

/// A last-common at or past the end is a no-op.
#[test]
fn truncation_at_or_past_the_end_is_a_noop() {
    let w = wm(0, 5, 20);
    assert_eq!(truncate_to(20, &w), 20);
    assert_eq!(truncate_to(99, &w), 20);
}

// ── Offsets are stable and contiguous ───────────────────────────────────────

/// Appending assigns contiguous offsets and `next_offset` tracks them.
/// Contiguity is what lets a consumer detect a gap: a hole in the
/// sequence is data loss, and it must be visible as one.
#[test]
fn offsets_are_contiguous() {
    let mut s = SegmentIndex::new(1_000);
    for i in 0..50u64 {
        assert!(s.on_append(1_000 + i, i, 10, 100));
        assert_eq!(s.next_offset, 1_001 + i);
    }
    assert_eq!(s.base_offset, 1_000);
}

// ── Time-based retention ─────────────────────────────────────────────
//
// Retention is measured on the BROKER's monotonic clock, from append.
// The first version of this compared the runtime's monotonic `now`
// against the producer's Unix-epoch `firstTimestamp`; the difference
// saturated to zero and nothing ever expired. These tests exist to keep
// the two clocks from being conflated again.

#[test]
fn no_policy_retains_everything() {
    assert!(!retention_expired(u64::MAX, 0));
}

#[test]
fn a_record_older_than_the_window_expires() {
    assert!(retention_expired(60_000, 60_000));
    assert!(retention_expired(60_001, 60_000));
}

#[test]
fn a_record_inside_the_window_is_kept() {
    assert!(!retention_expired(59_999, 60_000));
}

#[test]
fn age_is_measured_from_append_on_the_brokers_clock() {
    assert_eq!(entry_age_ms(1_000, 61_000), 60_000);
}

/// Impossible on a monotonic clock, but wrapping subtraction here would
/// expire the NEWEST records first.
#[test]
fn a_stored_time_ahead_of_now_reads_as_age_zero() {
    assert_eq!(entry_age_ms(10_000, 1_000), 0);
    assert!(!retention_expired(entry_age_ms(10_000, 1_000), 1));
}

/// The whole failure this replaced: an epoch-ms value against a
/// monotonic `now` must not be mistaken for a tiny age that never
/// expires — expressed here as the age being computed from the SAME
/// clock on both sides.
#[test]
fn a_realistic_uptime_and_window_expire_correctly() {
    let appended = 30_000; // 30 s after boot
    let now = 30_000 + 3_600_000; // an hour later
    assert!(retention_expired(entry_age_ms(appended, now), 60_000));
    assert!(!retention_expired(entry_age_ms(appended, now), 7_200_000));
}

// ── RecordBatch v2 record walking (compaction input) ─────────────────
//
// Every field here is a ZIGZAG varint, so -1 is the null sentinel for a
// key or a value. Decoding those as unsigned turns -1 into a huge length
// and walks off the batch, which is why the sentinel cases are tested
// before anything else.

fn put_zigzag(out: &mut Vec<u8>, v: i64) {
    let mut raw = u64::from_le_bytes(((v << 1) ^ (v >> 63)).to_le_bytes());
    loop {
        if raw < 0x80 {
            out.push(u8::try_from(raw).unwrap_or(0));
            return;
        }
        out.push(((raw & 0x7f) as u8) | 0x80);
        raw >>= 7;
    }
}

/// One walked record: its key (if any) and whether it is a tombstone.
type SeenKey = (Option<Vec<u8>>, bool);

/// The `(key, value)` pairs a synthetic batch carries; `None` value = tombstone.
type BatchRecords<'a> = &'a [(Option<&'a [u8]>, Option<&'a [u8]>)];

/// A v2 batch whose records are `(key, value)`; `None` value = tombstone.
fn batch(records: BatchRecords) -> Vec<u8> {
    let mut b = vec![0u8; BATCH_HEADER_LEN];
    b[BATCH_RECORD_COUNT_OFF..BATCH_RECORD_COUNT_OFF + 4]
        .copy_from_slice(&i32::try_from(records.len()).unwrap_or(0).to_be_bytes());
    for (k, v) in records {
        let mut r = Vec::new();
        r.push(0u8); // attributes
        put_zigzag(&mut r, 0); // timestampDelta
        put_zigzag(&mut r, 0); // offsetDelta
        match k {
            Some(k) => {
                put_zigzag(&mut r, i64::try_from(k.len()).unwrap_or(0));
                r.extend_from_slice(k);
            }
            None => put_zigzag(&mut r, -1),
        }
        match v {
            Some(v) => {
                put_zigzag(&mut r, i64::try_from(v.len()).unwrap_or(0));
                r.extend_from_slice(v);
            }
            None => put_zigzag(&mut r, -1),
        }
        put_zigzag(&mut b, i64::try_from(r.len()).unwrap_or(0));
        b.extend_from_slice(&r);
    }
    b
}

#[test]
fn zigzag_round_trips_the_values_records_actually_carry() {
    for v in [
        0i64, -1, 1, -2, 2, 63, 64, -64, 300, -300, 100_000, -100_000,
    ] {
        let mut buf = Vec::new();
        put_zigzag(&mut buf, v);
        assert_eq!(zigzag_varint(&buf, 0).map(|(x, _)| x), Some(v), "value {v}");
    }
}

#[test]
fn a_truncated_varint_is_rejected_not_guessed() {
    assert_eq!(zigzag_varint(&[0x80, 0x80, 0x80], 0), None);
    assert_eq!(zigzag_varint(&[], 0), None);
}

#[test]
fn keys_and_tombstones_are_read_from_a_batch() {
    let b = batch(&[
        (Some(b"a".as_ref()), Some(b"1".as_ref())),
        (Some(b"bb".as_ref()), None), // tombstone
        (None, Some(b"3".as_ref())),  // null key
    ]);
    let mut seen: Vec<SeenKey> = Vec::new();
    assert!(for_each_record_key(&b, |k, tomb| seen.push((k.map(<[u8]>::to_vec), tomb))));
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0], (Some(b"a".to_vec()), false));
    assert_eq!(
        seen[1],
        (Some(b"bb".to_vec()), true),
        "key + null value is a tombstone"
    );
    assert_eq!(seen[2], (None, false), "a null key is never a tombstone");
}

/// A record with no key has no identity, so compaction can never
/// supersede it. This is the property the sweep depends on.
#[test]
fn a_null_key_is_reported_as_none() {
    let b = batch(&[(None, Some(b"v".as_ref()))]);
    let mut keys = 0;
    assert!(for_each_record_key(&b, |k, _| {
        if k.is_none() {
            keys += 1;
        }
    }));
    assert_eq!(keys, 1);
}

#[test]
fn an_empty_batch_walks_cleanly() {
    let b = batch(&[]);
    let mut n = 0;
    assert!(for_each_record_key(&b, |_, _| n += 1));
    assert_eq!(n, 0);
}

/// A malformed batch must be reported as such, NOT walked half way —
/// a caller acting on a partial record set would compact away records
/// it never actually saw.
#[test]
fn a_record_count_the_body_cannot_support_is_rejected() {
    let mut b = batch(&[(Some(b"a".as_ref()), Some(b"1".as_ref()))]);
    b[BATCH_RECORD_COUNT_OFF..BATCH_RECORD_COUNT_OFF + 4].copy_from_slice(&99i32.to_be_bytes());
    assert!(!for_each_record_key(&b, |_, _| {}));
}

#[test]
fn a_negative_record_count_is_rejected() {
    let mut b = batch(&[]);
    b[BATCH_RECORD_COUNT_OFF..BATCH_RECORD_COUNT_OFF + 4].copy_from_slice(&(-1i32).to_be_bytes());
    assert!(!for_each_record_key(&b, |_, _| {}));
}

#[test]
fn a_buffer_too_short_for_a_header_is_rejected() {
    assert!(!for_each_record_key(&[0u8; 10], |_, _| {}));
    assert_eq!(batch_record_count(&[0u8; 10]), None);
}

/// A key length running past the record must not be read.
#[test]
fn a_key_length_past_the_record_end_is_rejected() {
    let mut b = batch(&[(Some(b"a".as_ref()), Some(b"1".as_ref()))]);
    // The record's key-length varint sits after len + attributes + 2
    // zero varints; overwrite it with a length far past the record.
    let klen_pos = BATCH_HEADER_LEN + 1 + 1 + 1 + 1;
    b[klen_pos] = 0x7e; // zigzag 0x7e -> 63
    assert!(!for_each_record_key(&b, |_, _| {}));
}

// ── Compaction decisions ─────────────────────────────────────────────
//
// This is the half of compaction that can destroy live data, so every
// "never drop" rule gets its own test. A batch wrongly marked droppable
// is a silently lost record.

/// Keys as small ints; `None` = not compactable (multi-record batch,
/// null key, or tombstone).
fn marks(keys: &[Option<u32>]) -> Vec<bool> {
    let mut out = vec![false; keys.len()];
    mark_droppable(
        keys.len(),
        |i| keys[i].is_some(),
        |i, j| keys[i] == keys[j],
        &mut out,
    );
    out
}

#[test]
fn a_superseded_key_is_droppable_and_the_last_one_is_not() {
    // oldest-first: k1 k2 k1
    let m = marks(&[Some(1), Some(2), Some(1)]);
    assert_eq!(
        m,
        vec![true, false, false],
        "only the older k1 is superseded"
    );
}

#[test]
fn a_key_never_rewritten_is_never_droppable() {
    assert_eq!(
        marks(&[Some(1), Some(2), Some(3)]),
        vec![false, false, false]
    );
}

/// Multi-record batches, null keys and tombstones all present as
/// "no compactable key" and must survive even when a later batch
/// carries the same key.
#[test]
fn a_batch_with_no_compactable_key_is_never_droppable() {
    let m = marks(&[None, Some(1), Some(1)]);
    assert!(!m[0], "no key means no identity to supersede");
    assert!(m[1]);
    assert!(!m[2]);
}

/// A later NON-compactable batch must not count as a supersession: it
/// may be a multi-record batch that happens to contain the key, or a
/// tombstone, and neither licenses dropping the older value here.
#[test]
fn a_later_uncompactable_batch_does_not_supersede() {
    let m = marks(&[Some(1), None]);
    assert_eq!(m, vec![false, false]);
}

#[test]
fn repeated_rewrites_leave_only_the_newest() {
    let m = marks(&[Some(7), Some(7), Some(7), Some(7)]);
    assert_eq!(m, vec![true, true, true, false]);
}

// ── Only the tail run can actually be evicted ────────────────────────

#[test]
fn the_leading_run_of_droppable_batches_is_evicted() {
    assert_eq!(droppable_prefix(&[true, true, false, true], 4, 99), 2);
}

/// The first batch that must be KEPT stops the sweep, however many
/// droppable batches sit behind it — a ring evicts only from the tail.
#[test]
fn a_kept_batch_stops_the_sweep() {
    assert_eq!(droppable_prefix(&[false, true, true], 3, 99), 0);
}

#[test]
fn the_budget_bounds_one_sweep() {
    assert_eq!(droppable_prefix(&[true, true, true, true], 4, 2), 2);
}

#[test]
fn nothing_droppable_evicts_nothing() {
    assert_eq!(droppable_prefix(&[false, false], 2, 99), 0);
    assert_eq!(droppable_prefix(&[], 0, 99), 0);
}

// ── Sparse offset -> raft-index map (cold Fetch from the WAL) ────────
//
// The Kafka log IS the raft WAL, so a record evicted from the ring is
// still on disk; what is missing is the way back from a Kafka OFFSET to
// the raft INDEX that carried it. Every test here is about not handing
// a reader the wrong entries.

#[test]
fn an_empty_index_locates_nothing() {
    let ix = OffsetIndex::new();
    assert!(ix.is_empty());
    assert_eq!(ix.seek(0), None);
    assert_eq!(ix.floor_offset(), None);
}

#[test]
fn an_anchor_is_stored_only_every_interval() {
    let mut ix = OffsetIndex::new();
    assert!(ix.note_append(0, 100, 0, 10), "first append always anchors");
    assert!(!ix.note_append(5, 105, 0, 10), "inside the interval");
    assert!(ix.note_append(10, 110, 0, 10), "at the interval");
    assert_eq!(ix.len(), 2);
}

/// The nearest anchor AT OR BELOW the target — never above it. An anchor
/// above the target names a raft entry past the record, and reading from
/// there skips the very data the consumer asked for.
#[test]
fn seek_returns_the_nearest_anchor_at_or_below() {
    let mut ix = OffsetIndex::new();
    ix.note_append(0, 100, 0, 10);
    ix.note_append(10, 140, 0, 10);
    ix.note_append(20, 175, 0, 10);
    assert_eq!(ix.seek(0), Some((0, 100)));
    assert_eq!(ix.seek(9), Some((0, 100)), "below the second anchor");
    assert_eq!(ix.seek(10), Some((0, 140)), "exactly on an anchor");
    assert_eq!(ix.seek(19), Some((0, 140)));
    assert_eq!(
        ix.seek(25),
        Some((0, 175)),
        "past the last anchor still starts there"
    );
}

/// An offset older than every anchor cannot be located. Returning the
/// first anchor anyway would start the reader AFTER the record it asked
/// for and silently skip it — the same silent-skip failure the
/// out-of-range test exists to prevent.
#[test]
fn an_offset_below_every_anchor_is_not_guessed() {
    let mut ix = OffsetIndex::new();
    ix.note_append(100, 500, 0, 10);
    assert_eq!(ix.seek(99), None);
    assert_eq!(ix.floor_offset(), Some(100));
}

/// Offsets and raft indices are both assigned in order, so a
/// non-monotone insert means two partitions' streams have been mixed.
/// Storing it would make `seek` return another partition's entries.
#[test]
fn non_monotone_inserts_are_refused() {
    let mut ix = OffsetIndex::new();
    assert!(ix.note_append(10, 100, 0, 1));
    assert!(!ix.note_append(10, 101, 0, 1), "same offset");
    assert!(!ix.note_append(9, 102, 0, 1), "offset went backwards");
    assert!(!ix.note_append(11, 100, 0, 1), "raft index went backwards");
    assert!(
        !ix.note_append(11, 99, 0, 1),
        "raft index went backwards further"
    );
    assert_eq!(ix.len(), 1);
    assert!(ix.note_append(11, 101, 0, 1), "both forward is accepted");
}

/// When full the OLDEST anchor goes: old anchors describe records the
/// WAL may itself have retired, the newest describe what a lagging
/// consumer asks for next.
#[test]
fn a_full_index_drops_its_oldest_anchor() {
    let mut ix = OffsetIndex::new();
    for i in 0..OFFSET_ANCHORS as u64 {
        assert!(ix.note_append(i, 1000 + i, 0, 1));
    }
    assert_eq!(ix.len(), OFFSET_ANCHORS);
    assert_eq!(ix.floor_offset(), Some(0));

    ix.note_append(OFFSET_ANCHORS as u64, 1000 + OFFSET_ANCHORS as u64, 0, 1);
    assert_eq!(ix.len(), OFFSET_ANCHORS, "stays at capacity");
    assert_eq!(ix.floor_offset(), Some(1), "oldest anchor went");
    assert_eq!(ix.seek(0), None, "and is no longer locatable");
    assert_eq!(
        ix.seek(OFFSET_ANCHORS as u64),
        Some((0, 1000 + OFFSET_ANCHORS as u64))
    );
}

/// Interval spacing must survive eviction — otherwise a full index
/// starts anchoring every append and thrashes.
#[test]
fn the_interval_still_applies_after_eviction() {
    let mut ix = OffsetIndex::new();
    for i in 0..OFFSET_ANCHORS as u64 {
        ix.note_append(i * 10, 1000 + i, 0, 10);
    }
    let base = OFFSET_ANCHORS as u64 * 10;
    assert!(
        !ix.note_append(base - 5, 9000, 0, 10),
        "inside the interval"
    );
    assert!(ix.note_append(base, 9001, 0, 10));
}

#[test]
fn clear_resets_the_index() {
    let mut ix = OffsetIndex::new();
    ix.note_append(5, 50, 0, 1);
    ix.clear();
    assert!(ix.is_empty());
    assert_eq!(ix.seek(5), None);
    assert!(ix.note_append(1, 1, 0, 1), "usable again after clear");
}

/// Raft indexes are per-log. A stream that moves to another raft
/// partition legitimately sees its index fall, and the index must keep
/// both anchors — each addressed to its own log.
#[test]
fn anchors_remember_their_raft_partition() {
    let mut ix = OffsetIndex::new();
    assert!(ix.note_append(0, 500, 3, 10));
    assert!(ix.note_append(10, 7, 4, 10), "a new partition starts low");
    assert!(
        !ix.note_append(20, 6, 4, 10),
        "but must still advance within it"
    );
    assert_eq!(ix.seek(5), Some((3, 500)));
    assert_eq!(ix.seek(15), Some((4, 7)));
    assert_eq!(ix.partitions_seen(), (1 << 3) | (1 << 4));
}

/// The floor for a raft partition is the first of ITS anchors at or
/// after the seek point; a partition with none there needs nothing.
#[test]
fn needed_from_is_per_partition() {
    let mut ix = OffsetIndex::new();
    ix.note_append(0, 100, 1, 10);
    ix.note_append(10, 110, 1, 10);
    ix.note_append(20, 5, 2, 10);
    ix.note_append(30, 15, 2, 10);
    assert_eq!(
        ix.needed_from(12, 1),
        Ok(Some(110)),
        "anchor at 10 covers 12"
    );
    assert_eq!(
        ix.needed_from(12, 2),
        Ok(Some(5)),
        "partition 2 starts after"
    );
    assert_eq!(
        ix.needed_from(25, 1),
        Ok(None),
        "partition 1 has nothing past 20"
    );
    assert_eq!(ix.needed_from(25, 2), Ok(Some(5)));
    assert_eq!(ix.needed_from(35, 2), Ok(Some(15)));
}

/// An offset below every anchor cannot prove anything expendable.
#[test]
fn needed_from_fails_closed_below_the_index() {
    let mut ix = OffsetIndex::new();
    ix.note_append(100, 50, 0, 10);
    assert_eq!(ix.needed_from(99, 0), Err(()));
}
