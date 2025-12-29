//! Transaction coordination for atomic operations.
//!
//! This module provides transaction management:
//! - Two-phase commit protocol
//! - Transaction state machine
//! - Timeout handling
//! - Idempotent producer support
//!
//! Tasks 31-33: Transactions, log entries, tests

use std::collections::HashMap;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Transaction State Machine (Task 31)
// ---------------------------------------------------------------------------

/// Transaction state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// No active transaction.
    Empty,
    /// Transaction started, operations being collected.
    Ongoing,
    /// Preparing to commit (two-phase commit phase 1).
    PrepareCommit,
    /// Preparing to abort.
    PrepareAbort,
    /// Transaction committed.
    Committed,
    /// Transaction aborted.
    Aborted,
    /// Transaction timed out.
    TimedOut,
}

impl TransactionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransactionState::Empty => "empty",
            TransactionState::Ongoing => "ongoing",
            TransactionState::PrepareCommit => "prepare_commit",
            TransactionState::PrepareAbort => "prepare_abort",
            TransactionState::Committed => "committed",
            TransactionState::Aborted => "aborted",
            TransactionState::TimedOut => "timed_out",
        }
    }

    /// Check if this is a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransactionState::Committed | TransactionState::Aborted | TransactionState::TimedOut
        )
    }

    /// Check if operations can be added.
    pub fn can_add_operations(&self) -> bool {
        matches!(self, TransactionState::Ongoing)
    }
}

/// Transaction operation types.
#[derive(Debug, Clone)]
pub enum TransactionOperation {
    /// Produce a message to a topic-partition.
    Produce {
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// Add offsets to the transaction (for consumer groups).
    AddOffsets {
        group_id: String,
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// Custom operation for extensibility.
    Custom {
        operation_type: String,
        data: Vec<u8>,
    },
}

/// Transaction metadata.
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Transaction identifier.
    pub txn_id: String,
    /// Producer ID for idempotent producer.
    pub producer_id: i64,
    /// Producer epoch for fencing.
    pub producer_epoch: i16,
    /// Current state.
    pub state: TransactionState,
    /// Transaction timeout in milliseconds.
    pub timeout_ms: u32,
    /// Operations in this transaction.
    pub operations: Vec<TransactionOperation>,
    /// Partitions involved in this transaction.
    pub partitions: HashMap<(String, i32), i64>, // (topic, partition) -> first_offset
    /// Consumer group offsets to commit.
    pub offsets: HashMap<(String, String, i32), i64>, // (group, topic, partition) -> offset
    /// Transaction start time.
    pub started_at: Instant,
    /// Last activity time.
    pub last_update: Instant,
    /// Sequence number for idempotent operations.
    pub sequence_number: i32,
}

impl Transaction {
    pub fn new(txn_id: String, producer_id: i64, producer_epoch: i16, timeout_ms: u32) -> Self {
        let now = Instant::now();
        Self {
            txn_id,
            producer_id,
            producer_epoch,
            state: TransactionState::Empty,
            timeout_ms,
            operations: Vec::new(),
            partitions: HashMap::new(),
            offsets: HashMap::new(),
            started_at: now,
            last_update: now,
            sequence_number: 0,
        }
    }

    /// Begin the transaction.
    pub fn begin(&mut self) -> Result<(), TransactionError> {
        if self.state != TransactionState::Empty {
            return Err(TransactionError::InvalidState {
                current: self.state,
                expected: TransactionState::Empty,
            });
        }
        self.state = TransactionState::Ongoing;
        self.started_at = Instant::now();
        self.last_update = Instant::now();
        Ok(())
    }

    /// Add a produce operation to the transaction.
    pub fn add_produce(
        &mut self,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Result<(), TransactionError> {
        if !self.state.can_add_operations() {
            return Err(TransactionError::InvalidState {
                current: self.state,
                expected: TransactionState::Ongoing,
            });
        }

        let key = (topic.clone(), partition);
        self.partitions.entry(key).or_insert(offset);
        self.operations.push(TransactionOperation::Produce {
            topic,
            partition,
            offset,
        });
        self.last_update = Instant::now();
        Ok(())
    }

    /// Add offsets to the transaction.
    pub fn add_offsets(
        &mut self,
        group_id: String,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Result<(), TransactionError> {
        if !self.state.can_add_operations() {
            return Err(TransactionError::InvalidState {
                current: self.state,
                expected: TransactionState::Ongoing,
            });
        }

        let key = (group_id.clone(), topic.clone(), partition);
        self.offsets.insert(key, offset);
        self.operations.push(TransactionOperation::AddOffsets {
            group_id,
            topic,
            partition,
            offset,
        });
        self.last_update = Instant::now();
        Ok(())
    }

    /// Prepare to commit (phase 1 of 2PC).
    pub fn prepare_commit(&mut self) -> Result<(), TransactionError> {
        if self.state != TransactionState::Ongoing {
            return Err(TransactionError::InvalidState {
                current: self.state,
                expected: TransactionState::Ongoing,
            });
        }
        self.state = TransactionState::PrepareCommit;
        self.last_update = Instant::now();
        Ok(())
    }

    /// Prepare to abort (phase 1 of 2PC for abort).
    pub fn prepare_abort(&mut self) -> Result<(), TransactionError> {
        if self.state != TransactionState::Ongoing {
            return Err(TransactionError::InvalidState {
                current: self.state,
                expected: TransactionState::Ongoing,
            });
        }
        self.state = TransactionState::PrepareAbort;
        self.last_update = Instant::now();
        Ok(())
    }

    /// Complete commit (phase 2 of 2PC).
    pub fn complete_commit(&mut self) -> Result<(), TransactionError> {
        if self.state != TransactionState::PrepareCommit {
            return Err(TransactionError::InvalidState {
                current: self.state,
                expected: TransactionState::PrepareCommit,
            });
        }
        self.state = TransactionState::Committed;
        self.last_update = Instant::now();
        Ok(())
    }

    /// Complete abort (phase 2 of 2PC for abort).
    pub fn complete_abort(&mut self) -> Result<(), TransactionError> {
        if self.state != TransactionState::PrepareAbort && self.state != TransactionState::Ongoing {
            return Err(TransactionError::InvalidState {
                current: self.state,
                expected: TransactionState::PrepareAbort,
            });
        }
        self.state = TransactionState::Aborted;
        self.last_update = Instant::now();
        Ok(())
    }

    /// Check if the transaction has timed out.
    pub fn is_expired(&self) -> bool {
        self.started_at.elapsed() > Duration::from_millis(self.timeout_ms as u64)
    }

    /// Mark the transaction as timed out.
    pub fn timeout(&mut self) {
        if !self.state.is_terminal() {
            self.state = TransactionState::TimedOut;
            self.last_update = Instant::now();
        }
    }

    /// Reset the transaction for reuse.
    pub fn reset(&mut self) {
        self.state = TransactionState::Empty;
        self.operations.clear();
        self.partitions.clear();
        self.offsets.clear();
        self.sequence_number = 0;
    }

    /// Bump the epoch (for fencing stale producers).
    pub fn bump_epoch(&mut self) {
        self.producer_epoch = self.producer_epoch.wrapping_add(1);
    }

    /// Get the duration since transaction started.
    pub fn duration(&self) -> Duration {
        self.started_at.elapsed()
    }
}

/// Transaction errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionError {
    /// Invalid state transition.
    InvalidState {
        current: TransactionState,
        expected: TransactionState,
    },
    /// Transaction not found.
    NotFound(String),
    /// Producer epoch mismatch (fenced).
    ProducerFenced {
        txn_id: String,
        expected_epoch: i16,
        actual_epoch: i16,
    },
    /// Transaction timed out.
    Timeout(String),
    /// Sequence number out of order.
    OutOfOrderSequence { expected: i32, actual: i32 },
    /// Duplicate sequence number.
    DuplicateSequence(i32),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::InvalidState { current, expected } => {
                write!(
                    f,
                    "invalid state transition: current={}, expected={}",
                    current.as_str(),
                    expected.as_str()
                )
            }
            TransactionError::NotFound(id) => write!(f, "transaction not found: {id}"),
            TransactionError::ProducerFenced {
                txn_id,
                expected_epoch,
                actual_epoch,
            } => {
                write!(
                    f,
                    "producer fenced for {txn_id}: expected epoch {expected_epoch}, got {actual_epoch}"
                )
            }
            TransactionError::Timeout(id) => write!(f, "transaction timed out: {id}"),
            TransactionError::OutOfOrderSequence { expected, actual } => {
                write!(
                    f,
                    "out of order sequence: expected {expected}, got {actual}"
                )
            }
            TransactionError::DuplicateSequence(seq) => {
                write!(f, "duplicate sequence number: {seq}")
            }
        }
    }
}

impl std::error::Error for TransactionError {}

// ---------------------------------------------------------------------------
// Transaction Log Entries (Task 32)
// ---------------------------------------------------------------------------

/// Log entry types for transaction state persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum TransactionLogEntry {
    /// Begin a new transaction.
    Begin {
        txn_id: String,
        producer_id: i64,
        producer_epoch: i16,
        timeout_ms: u32,
    },
    /// Add partitions to transaction.
    AddPartitions {
        txn_id: String,
        partitions: Vec<(String, i32)>,
    },
    /// Add offsets to transaction.
    AddOffsets {
        txn_id: String,
        group_id: String,
        offsets: Vec<(String, i32, i64)>,
    },
    /// Prepare to commit.
    PrepareCommit { txn_id: String },
    /// Prepare to abort.
    PrepareAbort { txn_id: String },
    /// Complete commit.
    Commit { txn_id: String },
    /// Complete abort.
    Abort { txn_id: String },
    /// Transaction expired.
    Expire { txn_id: String },
}

impl TransactionLogEntry {
    pub fn txn_id(&self) -> &str {
        match self {
            TransactionLogEntry::Begin { txn_id, .. }
            | TransactionLogEntry::AddPartitions { txn_id, .. }
            | TransactionLogEntry::AddOffsets { txn_id, .. }
            | TransactionLogEntry::PrepareCommit { txn_id }
            | TransactionLogEntry::PrepareAbort { txn_id }
            | TransactionLogEntry::Commit { txn_id }
            | TransactionLogEntry::Abort { txn_id }
            | TransactionLogEntry::Expire { txn_id } => txn_id,
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction Coordinator
// ---------------------------------------------------------------------------

/// Coordinates transactions.
#[derive(Debug, Clone, Default)]
pub struct TransactionCoordinator {
    /// Active transactions by transaction ID.
    transactions: HashMap<String, Transaction>,
    /// Producer ID to transaction ID mapping.
    producer_txns: HashMap<i64, String>,
    /// Next producer ID to allocate.
    next_producer_id: i64,
    /// Default transaction timeout in milliseconds.
    default_timeout_ms: u32,
}

impl TransactionCoordinator {
    pub fn new() -> Self {
        Self {
            transactions: HashMap::new(),
            producer_txns: HashMap::new(),
            next_producer_id: 1,
            default_timeout_ms: 60_000,
        }
    }

    pub fn with_default_timeout(mut self, timeout_ms: u32) -> Self {
        self.default_timeout_ms = timeout_ms;
        self
    }

    /// Initialize a new producer (idempotent producer init).
    pub fn init_producer(&mut self, txn_id: Option<String>) -> (i64, i16) {
        let producer_id = self.next_producer_id;
        self.next_producer_id += 1;

        if let Some(txn_id) = txn_id {
            let txn = Transaction::new(txn_id.clone(), producer_id, 0, self.default_timeout_ms);
            self.transactions.insert(txn_id.clone(), txn);
            self.producer_txns.insert(producer_id, txn_id);
        }

        (producer_id, 0)
    }

    /// Get or create a transaction.
    pub fn get_or_create(
        &mut self,
        txn_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<&mut Transaction, TransactionError> {
        // Check for existing transaction
        if let Some(existing) = self.transactions.get(txn_id) {
            // Verify producer epoch
            if existing.producer_epoch > producer_epoch {
                return Err(TransactionError::ProducerFenced {
                    txn_id: txn_id.to_string(),
                    expected_epoch: existing.producer_epoch,
                    actual_epoch: producer_epoch,
                });
            }
        }

        // Get or create
        let txn = self
            .transactions
            .entry(txn_id.to_string())
            .or_insert_with(|| {
                Transaction::new(
                    txn_id.to_string(),
                    producer_id,
                    producer_epoch,
                    self.default_timeout_ms,
                )
            });

        // Update epoch if higher
        if producer_epoch > txn.producer_epoch {
            txn.producer_epoch = producer_epoch;
        }

        Ok(txn)
    }

    /// Get an existing transaction.
    pub fn get(&self, txn_id: &str) -> Option<&Transaction> {
        self.transactions.get(txn_id)
    }

    /// Get a mutable transaction.
    pub fn get_mut(&mut self, txn_id: &str) -> Option<&mut Transaction> {
        self.transactions.get_mut(txn_id)
    }

    /// Begin a transaction.
    pub fn begin(
        &mut self,
        txn_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<(), TransactionError> {
        let txn = self.get_or_create(txn_id, producer_id, producer_epoch)?;

        // If previous transaction is terminal, reset
        if txn.state.is_terminal() {
            txn.reset();
        }

        txn.begin()
    }

    /// Commit a transaction.
    pub fn commit(&mut self, txn_id: &str) -> Result<(), TransactionError> {
        let txn = self
            .transactions
            .get_mut(txn_id)
            .ok_or_else(|| TransactionError::NotFound(txn_id.to_string()))?;

        if txn.state == TransactionState::Ongoing {
            txn.prepare_commit()?;
        }

        txn.complete_commit()
    }

    /// Abort a transaction.
    pub fn abort(&mut self, txn_id: &str) -> Result<(), TransactionError> {
        let txn = self
            .transactions
            .get_mut(txn_id)
            .ok_or_else(|| TransactionError::NotFound(txn_id.to_string()))?;

        if txn.state == TransactionState::Ongoing {
            txn.prepare_abort()?;
        }

        txn.complete_abort()
    }

    /// Check and timeout expired transactions.
    pub fn check_timeouts(&mut self) -> Vec<String> {
        let mut expired = Vec::new();

        for (txn_id, txn) in &mut self.transactions {
            if !txn.state.is_terminal() && txn.is_expired() {
                txn.timeout();
                expired.push(txn_id.clone());
            }
        }

        expired
    }

    /// Remove completed/aborted transactions.
    pub fn cleanup(&mut self) -> usize {
        let before = self.transactions.len();
        self.transactions.retain(|_, txn| !txn.state.is_terminal());
        before - self.transactions.len()
    }

    /// List active transactions.
    pub fn list_active(&self) -> impl Iterator<Item = &Transaction> {
        self.transactions
            .values()
            .filter(|txn| !txn.state.is_terminal())
    }

    /// Get transaction count.
    pub fn count(&self) -> usize {
        self.transactions.len()
    }
}

// ---------------------------------------------------------------------------
// Idempotent Producer Support
// ---------------------------------------------------------------------------

/// Tracks sequence numbers for idempotent producers.
#[derive(Debug, Clone, Default)]
pub struct ProducerSequenceTracker {
    /// (producer_id, partition) -> last sequence number.
    sequences: HashMap<(i64, i32), i32>,
    /// Window size for duplicate detection.
    window_size: i32,
}

impl ProducerSequenceTracker {
    pub fn new(window_size: i32) -> Self {
        Self {
            sequences: HashMap::new(),
            window_size,
        }
    }

    /// Check and record a sequence number.
    pub fn check_and_record(
        &mut self,
        producer_id: i64,
        partition: i32,
        sequence: i32,
    ) -> Result<(), TransactionError> {
        let key = (producer_id, partition);

        if let Some(&last_seq) = self.sequences.get(&key) {
            let expected = last_seq.wrapping_add(1);

            if sequence == last_seq {
                return Err(TransactionError::DuplicateSequence(sequence));
            }

            // Allow some window for out-of-order
            let diff = sequence.wrapping_sub(expected);
            if diff > self.window_size || diff < -self.window_size {
                return Err(TransactionError::OutOfOrderSequence {
                    expected,
                    actual: sequence,
                });
            }
        }

        self.sequences.insert(key, sequence);
        Ok(())
    }

    /// Get the last sequence for a producer-partition pair.
    pub fn last_sequence(&self, producer_id: i64, partition: i32) -> Option<i32> {
        self.sequences.get(&(producer_id, partition)).copied()
    }

    /// Clear sequences for a producer.
    pub fn clear_producer(&mut self, producer_id: i64) {
        self.sequences.retain(|(pid, _), _| *pid != producer_id);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_lifecycle() {
        let mut txn = Transaction::new("txn1".to_string(), 1, 0, 60_000);

        assert_eq!(txn.state, TransactionState::Empty);

        txn.begin().unwrap();
        assert_eq!(txn.state, TransactionState::Ongoing);

        txn.add_produce("topic1".to_string(), 0, 0).unwrap();
        assert_eq!(txn.operations.len(), 1);

        txn.prepare_commit().unwrap();
        assert_eq!(txn.state, TransactionState::PrepareCommit);

        txn.complete_commit().unwrap();
        assert_eq!(txn.state, TransactionState::Committed);
        assert!(txn.state.is_terminal());
    }

    #[test]
    fn test_transaction_abort() {
        let mut txn = Transaction::new("txn1".to_string(), 1, 0, 60_000);

        txn.begin().unwrap();
        txn.add_produce("topic1".to_string(), 0, 0).unwrap();
        txn.prepare_abort().unwrap();
        txn.complete_abort().unwrap();

        assert_eq!(txn.state, TransactionState::Aborted);
    }

    #[test]
    fn test_transaction_invalid_state() {
        let mut txn = Transaction::new("txn1".to_string(), 1, 0, 60_000);

        // Can't add operations without beginning
        let result = txn.add_produce("topic1".to_string(), 0, 0);
        assert!(matches!(result, Err(TransactionError::InvalidState { .. })));
    }

    #[test]
    fn test_coordinator_begin_commit() {
        let mut coord = TransactionCoordinator::new();

        coord.begin("txn1", 1, 0).unwrap();
        let txn = coord.get("txn1").unwrap();
        assert_eq!(txn.state, TransactionState::Ongoing);

        coord
            .get_mut("txn1")
            .unwrap()
            .add_produce("topic1".to_string(), 0, 0)
            .unwrap();
        coord.commit("txn1").unwrap();

        let txn = coord.get("txn1").unwrap();
        assert_eq!(txn.state, TransactionState::Committed);
    }

    #[test]
    fn test_producer_fencing() {
        let mut coord = TransactionCoordinator::new();

        coord.begin("txn1", 1, 5).unwrap();

        // Stale epoch should be fenced
        let result = coord.begin("txn1", 1, 3);
        assert!(matches!(
            result,
            Err(TransactionError::ProducerFenced { .. })
        ));
    }

    #[test]
    fn test_sequence_tracking() {
        let mut tracker = ProducerSequenceTracker::new(5);

        tracker.check_and_record(1, 0, 0).unwrap();
        tracker.check_and_record(1, 0, 1).unwrap();

        // Duplicate
        let result = tracker.check_and_record(1, 0, 1);
        assert!(matches!(
            result,
            Err(TransactionError::DuplicateSequence(1))
        ));

        // Out of order (too far)
        let result = tracker.check_and_record(1, 0, 100);
        assert!(matches!(
            result,
            Err(TransactionError::OutOfOrderSequence { .. })
        ));
    }

    #[test]
    fn test_coordinator_init_producer() {
        let mut coord = TransactionCoordinator::new();

        let (pid1, epoch1) = coord.init_producer(Some("txn1".to_string()));
        let (pid2, epoch2) = coord.init_producer(Some("txn2".to_string()));

        assert_eq!(pid1, 1);
        assert_eq!(pid2, 2);
        assert_eq!(epoch1, 0);
        assert_eq!(epoch2, 0);

        assert!(coord.get("txn1").is_some());
        assert!(coord.get("txn2").is_some());
    }
}
