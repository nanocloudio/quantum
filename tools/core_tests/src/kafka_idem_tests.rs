//! Tests for the Kafka idempotent-producer sequence core.
//!
//! The property under test is the one the feature exists for: a
//! producer that retries a batch after a lost response must NOT get a
//! second copy appended.

use crate::kafka_idem::*;

const P: u32 = 0;

/// Table capacity as an `i64`, so the eviction tests can use it as a
/// producer id without an unchecked cast.
fn cap() -> i64 {
    i64::try_from(MAX_PRODUCERS).expect("capacity fits in i64")
}
const PID: i64 = 7000;

/// The plain case: sequences arriving in order are all accepted.
#[test]
fn in_order_batches_are_accepted() {
    let mut t = IdemTable::new();
    assert_eq!(t.classify(PID, 0, P, 0, 4), IdemVerdict::Accept);
    t.commit(PID, 0, P, 4, 100);
    assert_eq!(t.classify(PID, 0, P, 5, 9), IdemVerdict::Accept);
    t.commit(PID, 0, P, 9, 105);
    assert_eq!(t.classify(PID, 0, P, 10, 10), IdemVerdict::Accept);
}

/// The whole point. The producer sent 0..=4, we committed it, the
/// response was lost, and it sends 0..=4 again. That must be reported
/// as a duplicate — with the ORIGINAL base offset, so the producer
/// records the position the data actually landed at — and must not be
/// appended a second time.
#[test]
fn retried_batch_is_a_duplicate_at_the_original_offset() {
    let mut t = IdemTable::new();
    assert_eq!(t.classify(PID, 0, P, 0, 4), IdemVerdict::Accept);
    t.commit(PID, 0, P, 4, 100);

    assert_eq!(
        t.classify(PID, 0, P, 0, 4),
        IdemVerdict::Duplicate { base_offset: 100 }
    );
    // Still a duplicate however many times it is retried, and the
    // table's idea of the next expected sequence is untouched.
    assert_eq!(
        t.classify(PID, 0, P, 0, 4),
        IdemVerdict::Duplicate { base_offset: 100 }
    );
    assert_eq!(t.classify(PID, 0, P, 5, 5), IdemVerdict::Accept);
}

/// A gap means batches were lost in flight. Accepting it would leave a
/// hole in the log that nothing ever fills.
#[test]
fn gap_is_out_of_order() {
    let mut t = IdemTable::new();
    t.classify(PID, 0, P, 0, 4);
    t.commit(PID, 0, P, 4, 100);
    assert_eq!(
        t.classify(PID, 0, P, 7, 9),
        IdemVerdict::Reject(ERR_OUT_OF_ORDER_SEQUENCE)
    );
    // And the gap did not advance anything: the expected batch still fits.
    assert_eq!(t.classify(PID, 0, P, 5, 6), IdemVerdict::Accept);
}

/// A batch that straddles the boundary — partly already applied, partly
/// new — cannot be split, so it is refused rather than half-appended.
#[test]
fn partial_overlap_is_refused_not_partially_applied() {
    let mut t = IdemTable::new();
    t.classify(PID, 0, P, 0, 4);
    t.commit(PID, 0, P, 4, 100);
    assert_eq!(
        t.classify(PID, 0, P, 3, 7),
        IdemVerdict::Reject(ERR_OUT_OF_ORDER_SEQUENCE)
    );
}

/// A producer's first batch must start at 0; anything else means we
/// never saw the earlier ones.
#[test]
fn first_batch_must_start_at_zero() {
    let mut t = IdemTable::new();
    assert_eq!(
        t.classify(PID, 0, P, 3, 5),
        IdemVerdict::Reject(ERR_OUT_OF_ORDER_SEQUENCE)
    );
    assert_eq!(t.classify(PID, 0, P, 0, 0), IdemVerdict::Accept);
}

/// Sequences are per-partition: partition 1 starting at 0 is not a
/// duplicate of partition 0's batch 0.
#[test]
fn sequences_are_tracked_per_partition() {
    let mut t = IdemTable::new();
    t.classify(PID, 0, 0, 0, 4);
    t.commit(PID, 0, 0, 4, 100);
    assert_eq!(t.classify(PID, 0, 1, 0, 4), IdemVerdict::Accept);
    t.commit(PID, 0, 1, 4, 500);
    assert_eq!(
        t.classify(PID, 0, 1, 0, 4),
        IdemVerdict::Duplicate { base_offset: 500 }
    );
    assert_eq!(
        t.classify(PID, 0, 0, 0, 4),
        IdemVerdict::Duplicate { base_offset: 100 }
    );
}

/// A restarted producer bumps its epoch and restarts sequences from 0.
/// The old incarnation is then a zombie and must be fenced.
#[test]
fn newer_epoch_fences_the_old_incarnation() {
    let mut t = IdemTable::new();
    t.classify(PID, 0, P, 0, 4);
    t.commit(PID, 0, P, 4, 100);

    assert_eq!(t.classify(PID, 1, P, 0, 2), IdemVerdict::Accept);
    t.commit(PID, 1, P, 2, 105);

    // The zombie's next batch — valid under the old epoch — is refused.
    assert_eq!(
        t.classify(PID, 0, P, 5, 6),
        IdemVerdict::Reject(ERR_INVALID_PRODUCER_EPOCH)
    );
    // The new incarnation carries on.
    assert_eq!(t.classify(PID, 1, P, 3, 3), IdemVerdict::Accept);
}

/// A new epoch that does not restart at 0 means we missed its opening
/// batches.
#[test]
fn new_epoch_must_also_start_at_zero() {
    let mut t = IdemTable::new();
    t.classify(PID, 0, P, 0, 4);
    t.commit(PID, 0, P, 4, 100);
    assert_eq!(
        t.classify(PID, 1, P, 6, 8),
        IdemVerdict::Reject(ERR_OUT_OF_ORDER_SEQUENCE)
    );
}

/// Producers that never opted into idempotence are not subject to any
/// of this — refusing their traffic would be a regression.
#[test]
fn non_idempotent_producers_are_always_accepted() {
    let mut t = IdemTable::new();
    assert_eq!(t.classify(NO_PRODUCER_ID, -1, P, 0, 0), IdemVerdict::Accept);
    assert_eq!(
        t.classify(NO_PRODUCER_ID, -1, P, 99, 99),
        IdemVerdict::Accept
    );
    t.commit(NO_PRODUCER_ID, -1, P, 99, 1);
    // ... and they consume no table space.
    assert!(t.is_empty());
}

/// The table is bounded. Past capacity it evicts the least recently
/// used entry and counts it, because an eviction silently reopens the
/// duplicate window for that producer and operators need to see it.
#[test]
fn table_evicts_lru_and_counts_it() {
    let mut t = IdemTable::new();
    for i in 0..cap() {
        t.commit(i + 1, 0, P, 0, i);
    }
    assert_eq!(t.len(), MAX_PRODUCERS);
    assert_eq!(t.evictions, 0);

    // Touch the first entry so it is no longer the coldest.
    assert_eq!(
        t.classify(1, 0, P, 0, 0),
        IdemVerdict::Duplicate { base_offset: 0 }
    );
    t.commit(1, 0, P, 0, 0);

    t.commit(cap() + 1, 0, P, 0, 9999);
    assert_eq!(t.evictions, 1);
    assert_eq!(t.len(), MAX_PRODUCERS);
    // The recently touched entry survived; the newcomer is present.
    assert_eq!(
        t.classify(1, 0, P, 0, 0),
        IdemVerdict::Duplicate { base_offset: 0 }
    );
    assert_eq!(
        t.classify(cap() + 1, 0, P, 0, 0),
        IdemVerdict::Duplicate { base_offset: 9999 }
    );
}

/// An evicted producer restarts cleanly rather than being wrongly
/// fenced: its next batch is treated as a first batch.
#[test]
fn evicted_producer_restarts_rather_than_erroring() {
    let mut t = IdemTable::new();
    t.commit(PID, 0, P, 4, 100);
    for i in 0..cap() {
        t.commit(i + 1000, 0, P, 0, i);
    }
    assert_eq!(t.classify(PID, 0, P, 0, 0), IdemVerdict::Accept);
}
