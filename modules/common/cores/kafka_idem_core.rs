// Bounded, no_std, no-alloc KAFKA IDEMPOTENT-PRODUCER core — the
// `(producer_id, epoch, sequence)` bookkeeping that turns a producer
// retry from a duplicate into a no-op. `include!`d by the host test
// crate and by the module that owns a partition's produce path.
//
// ## What it is for
//
// The ordinary failure — a producer sends a batch, the broker commits
// it, the response is lost, the producer retries — appends the batch a
// SECOND time unless the broker enforces sequences. The producer did
// everything right; without this the duplicate is the broker's.
//
// Kafka's answer, which clients already implement against:
//
//   * a producer holds a `(producer_id, epoch)` and a per-partition
//     monotone sequence;
//   * the expected next sequence is `last + 1`;
//   * a repeat of an already-accepted sequence is a DUPLICATE and is
//     acknowledged WITHOUT appending — that is what makes the retry
//     safe;
//   * a gap is OUT_OF_ORDER and is refused, because accepting it would
//     silently lose the batches in between;
//   * an older epoch is a zombie producer, fenced by a newer one.
//
// This core owns that decision table. The wire codecs and the append
// belong to the caller.

/// Kafka error codes this core returns. Values are Kafka's, because
/// clients switch on them.
pub const ERR_NONE: i16 = 0;
pub const ERR_OUT_OF_ORDER_SEQUENCE: i16 = 45;
pub const ERR_DUPLICATE_SEQUENCE: i16 = 46;
pub const ERR_INVALID_PRODUCER_EPOCH: i16 = 47;

/// Kafka's "no producer id": a producer that has not opted into
/// idempotence. Batches carrying it are always accepted.
pub const NO_PRODUCER_ID: i64 = -1;

/// Producer sequences tracked per node. Each entry is small; the table
/// is bounded and evicts its least-recently-used entry, which is safe
/// because losing an entry only means the next batch from that producer
/// is treated as a fresh start.
pub const MAX_PRODUCERS: usize = 256;

/// What the caller should do with a batch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IdemVerdict {
    /// Append it, then call `commit` with the last sequence in the batch.
    Accept,
    /// Already appended. Acknowledge with the ORIGINAL offset and do NOT
    /// append — this is the retry path, and appending here is exactly
    /// the duplicate the feature exists to prevent.
    Duplicate { base_offset: i64 },
    /// Refuse with this error code.
    Reject(i16),
}

#[derive(Clone, Copy)]
struct Entry {
    /// 0 = free.
    producer_id: i64,
    partition: u32,
    epoch: i16,
    /// Last sequence accepted. `-1` = none yet.
    last_seq: i32,
    /// Base offset the last accepted batch landed at, so a duplicate can
    /// be answered with the same offset the original got. Answering a
    /// different one would make a retrying producer record the wrong
    /// position.
    last_base_offset: i64,
    /// Monotone tick for LRU eviction.
    used_at: u64,
}

impl Entry {
    const fn empty() -> Self {
        Self {
            producer_id: 0,
            partition: 0,
            epoch: 0,
            last_seq: -1,
            last_base_offset: -1,
            used_at: 0,
        }
    }
}

pub struct IdemTable {
    entries: [Entry; MAX_PRODUCERS],
    clock: u64,
    /// Entries evicted under pressure. Non-zero means some producer's
    /// idempotence window was dropped — its next batch restarts the
    /// sequence, so a retry spanning the eviction can duplicate.
    pub evictions: u32,
}

impl IdemTable {
    pub const fn new() -> Self {
        Self {
            entries: [Entry::empty(); MAX_PRODUCERS],
            clock: 0,
            evictions: 0,
        }
    }

    fn find(&self, producer_id: i64, partition: u32) -> Option<usize> {
        let mut i = 0usize;
        while i < MAX_PRODUCERS {
            let e = &self.entries[i];
            if e.producer_id == producer_id && e.partition == partition && producer_id != 0 {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    /// Classify a batch.
    ///
    /// A non-positive `producer_id` ([`NO_PRODUCER_ID`]) is a
    /// non-idempotent producer: always `Accept`, because it has opted
    /// out and enforcing sequences on it would refuse valid traffic.
    pub fn classify(
        &mut self,
        producer_id: i64,
        epoch: i16,
        partition: u32,
        first_seq: i32,
        last_seq: i32,
    ) -> IdemVerdict {
        if producer_id <= 0 {
            return IdemVerdict::Accept;
        }
        self.clock = self.clock.wrapping_add(1);
        let Some(i) = self.find(producer_id, partition) else {
            // First batch from this producer for this partition. Kafka
            // requires it to start at 0; anything else means we missed
            // the batches before it.
            return if first_seq == 0 {
                IdemVerdict::Accept
            } else {
                IdemVerdict::Reject(ERR_OUT_OF_ORDER_SEQUENCE)
            };
        };
        let e = self.entries[i];
        if epoch < e.epoch {
            // A zombie: a newer incarnation of this producer id has
            // already fenced it.
            return IdemVerdict::Reject(ERR_INVALID_PRODUCER_EPOCH);
        }
        if epoch > e.epoch {
            // A new incarnation fences the old one and restarts at 0.
            return if first_seq == 0 {
                IdemVerdict::Accept
            } else {
                IdemVerdict::Reject(ERR_OUT_OF_ORDER_SEQUENCE)
            };
        }
        let expected = e.last_seq.wrapping_add(1);
        if first_seq == expected {
            IdemVerdict::Accept
        } else if last_seq <= e.last_seq {
            // Wholly within what we already have: the retry path.
            IdemVerdict::Duplicate {
                base_offset: e.last_base_offset,
            }
        } else if first_seq < expected {
            // Overlaps the accepted range but extends past it. Kafka
            // cannot split a batch, and accepting it would re-append the
            // overlap, so this is refused rather than partially applied.
            IdemVerdict::Reject(ERR_OUT_OF_ORDER_SEQUENCE)
        } else {
            // A gap. Accepting would silently lose the batches between.
            IdemVerdict::Reject(ERR_OUT_OF_ORDER_SEQUENCE)
        }
    }

    /// Record an accepted batch. Call ONLY after the append is durable —
    /// recording early would make a retry after a crash look like a
    /// duplicate and be acknowledged without ever having landed.
    pub fn commit(
        &mut self,
        producer_id: i64,
        epoch: i16,
        partition: u32,
        last_seq: i32,
        base_offset: i64,
    ) {
        if producer_id <= 0 {
            return;
        }
        self.clock = self.clock.wrapping_add(1);
        if let Some(i) = self.find(producer_id, partition) {
            let e = &mut self.entries[i];
            e.epoch = epoch;
            e.last_seq = last_seq;
            e.last_base_offset = base_offset;
            e.used_at = self.clock;
            return;
        }
        let slot = self.free_or_lru();
        self.entries[slot] = Entry {
            producer_id,
            partition,
            epoch,
            last_seq,
            last_base_offset: base_offset,
            used_at: self.clock,
        };
    }

    fn free_or_lru(&mut self) -> usize {
        let mut oldest = 0usize;
        let mut i = 0usize;
        while i < MAX_PRODUCERS {
            if self.entries[i].producer_id == 0 {
                return i;
            }
            if self.entries[i].used_at < self.entries[oldest].used_at {
                oldest = i;
            }
            i += 1;
        }
        self.evictions = self.evictions.wrapping_add(1);
        oldest
    }

    /// Live entries, for telemetry.
    pub fn len(&self) -> usize {
        let mut n = 0usize;
        let mut i = 0usize;
        while i < MAX_PRODUCERS {
            if self.entries[i].producer_id != 0 {
                n += 1;
            }
            i += 1;
        }
        n
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for IdemTable {
    fn default() -> Self {
        Self::new()
    }
}
