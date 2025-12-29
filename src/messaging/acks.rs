//! Acknowledgment tracking for delivery confirmation.
//!
//! This module provides protocol-agnostic acknowledgment management:
//! - Individual and batch acknowledgments
//! - Unacked message tracking with timeout
//! - Negative acknowledgment (NACK) support
//! - Redelivery scheduling
//!
//! Tasks 34-36: Acks, persistence, tests

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Acknowledgment Types (Task 34)
// ---------------------------------------------------------------------------

/// Acknowledgment mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Automatic acknowledgment on delivery.
    Auto,
    /// Manual acknowledgment required.
    Manual,
    /// Transactional acknowledgment.
    Transactional,
}

impl Default for AckMode {
    fn default() -> Self {
        Self::Manual
    }
}

/// Acknowledgment result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckResult {
    /// Message acknowledged successfully.
    Acked,
    /// Message negatively acknowledged, will be requeued.
    Nacked { requeue: bool },
    /// Message rejected, will not be requeued.
    Rejected,
    /// Acknowledgment timed out.
    Timeout,
}

/// Delivery tracking entry.
#[derive(Debug, Clone)]
pub struct DeliveryEntry {
    /// Unique delivery tag.
    pub delivery_tag: u64,
    /// Message identifier for correlation.
    pub message_id: String,
    /// Queue/topic the message came from.
    pub source: String,
    /// Consumer/client that received the message.
    pub consumer_id: String,
    /// Delivery timestamp.
    pub delivered_at: Instant,
    /// Acknowledgment deadline.
    pub ack_deadline: Instant,
    /// Number of delivery attempts.
    pub delivery_count: u32,
    /// Whether this delivery is part of a transaction.
    pub transactional: bool,
    /// Transaction ID if transactional.
    pub txn_id: Option<String>,
    /// Original message data (for redelivery).
    pub message_data: Vec<u8>,
}

impl DeliveryEntry {
    pub fn new(
        delivery_tag: u64,
        message_id: String,
        source: String,
        consumer_id: String,
        ack_timeout: Duration,
        message_data: Vec<u8>,
    ) -> Self {
        let now = Instant::now();
        Self {
            delivery_tag,
            message_id,
            source,
            consumer_id,
            delivered_at: now,
            ack_deadline: now + ack_timeout,
            delivery_count: 1,
            transactional: false,
            txn_id: None,
            message_data,
        }
    }

    /// Check if acknowledgment has timed out.
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.ack_deadline
    }

    /// Extend the acknowledgment deadline.
    pub fn extend_deadline(&mut self, extension: Duration) {
        self.ack_deadline = Instant::now() + extension;
    }

    /// Time until deadline.
    pub fn time_remaining(&self) -> Option<Duration> {
        self.ack_deadline.checked_duration_since(Instant::now())
    }
}

// ---------------------------------------------------------------------------
// Acknowledgment Tracker (Task 34)
// ---------------------------------------------------------------------------

/// Tracks outstanding acknowledgments for a consumer/channel.
#[derive(Debug, Clone)]
pub struct AckTracker {
    /// Outstanding deliveries by delivery tag.
    deliveries: HashMap<u64, DeliveryEntry>,
    /// Delivery tags in order for batch ack.
    delivery_order: VecDeque<u64>,
    /// Next delivery tag to assign.
    next_tag: u64,
    /// Acknowledgment timeout.
    ack_timeout: Duration,
    /// Maximum unacked messages (0 = unlimited).
    max_unacked: u64,
    /// Acknowledgment mode.
    mode: AckMode,
    /// Consumer identifier.
    consumer_id: String,
}

impl AckTracker {
    pub fn new(consumer_id: String, ack_timeout: Duration) -> Self {
        Self {
            deliveries: HashMap::new(),
            delivery_order: VecDeque::new(),
            next_tag: 1,
            ack_timeout,
            max_unacked: 0,
            mode: AckMode::default(),
            consumer_id,
        }
    }

    pub fn with_max_unacked(mut self, max: u64) -> Self {
        self.max_unacked = max;
        self
    }

    pub fn with_mode(mut self, mode: AckMode) -> Self {
        self.mode = mode;
        self
    }

    /// Track a new delivery.
    pub fn track_delivery(
        &mut self,
        message_id: String,
        source: String,
        message_data: Vec<u8>,
    ) -> Result<u64, AckError> {
        // Check capacity
        if self.max_unacked > 0 && self.deliveries.len() as u64 >= self.max_unacked {
            return Err(AckError::CapacityExceeded {
                current: self.deliveries.len() as u64,
                max: self.max_unacked,
            });
        }

        let tag = self.next_tag;
        self.next_tag = self.next_tag.wrapping_add(1);

        let entry = DeliveryEntry::new(
            tag,
            message_id,
            source,
            self.consumer_id.clone(),
            self.ack_timeout,
            message_data,
        );

        self.deliveries.insert(tag, entry);
        self.delivery_order.push_back(tag);

        Ok(tag)
    }

    /// Acknowledge a single delivery.
    pub fn ack(&mut self, delivery_tag: u64) -> Result<DeliveryEntry, AckError> {
        let entry = self
            .deliveries
            .remove(&delivery_tag)
            .ok_or(AckError::UnknownDeliveryTag(delivery_tag))?;

        self.delivery_order.retain(|&t| t != delivery_tag);
        Ok(entry)
    }

    /// Acknowledge multiple deliveries up to and including the given tag.
    pub fn ack_multiple(&mut self, delivery_tag: u64) -> Vec<DeliveryEntry> {
        let mut acked = Vec::new();

        while let Some(&front_tag) = self.delivery_order.front() {
            if front_tag > delivery_tag {
                break;
            }
            self.delivery_order.pop_front();
            if let Some(entry) = self.deliveries.remove(&front_tag) {
                acked.push(entry);
            }
        }

        acked
    }

    /// Negative acknowledge a delivery.
    pub fn nack(
        &mut self,
        delivery_tag: u64,
        requeue: bool,
    ) -> Result<(DeliveryEntry, AckResult), AckError> {
        let entry = self
            .deliveries
            .remove(&delivery_tag)
            .ok_or(AckError::UnknownDeliveryTag(delivery_tag))?;

        self.delivery_order.retain(|&t| t != delivery_tag);
        Ok((entry, AckResult::Nacked { requeue }))
    }

    /// Negative acknowledge multiple deliveries.
    pub fn nack_multiple(
        &mut self,
        delivery_tag: u64,
        requeue: bool,
    ) -> Vec<(DeliveryEntry, AckResult)> {
        let mut nacked = Vec::new();

        while let Some(&front_tag) = self.delivery_order.front() {
            if front_tag > delivery_tag {
                break;
            }
            self.delivery_order.pop_front();
            if let Some(entry) = self.deliveries.remove(&front_tag) {
                nacked.push((entry, AckResult::Nacked { requeue }));
            }
        }

        nacked
    }

    /// Reject a delivery (no requeue).
    pub fn reject(&mut self, delivery_tag: u64) -> Result<DeliveryEntry, AckError> {
        let entry = self
            .deliveries
            .remove(&delivery_tag)
            .ok_or(AckError::UnknownDeliveryTag(delivery_tag))?;

        self.delivery_order.retain(|&t| t != delivery_tag);
        Ok(entry)
    }

    /// Check for expired acknowledgments.
    pub fn check_timeouts(&mut self) -> Vec<DeliveryEntry> {
        let mut expired = Vec::new();
        let expired_tags: Vec<u64> = self
            .deliveries
            .iter()
            .filter(|(_, e)| e.is_expired())
            .map(|(&tag, _)| tag)
            .collect();

        for tag in expired_tags {
            if let Some(entry) = self.deliveries.remove(&tag) {
                self.delivery_order.retain(|&t| t != tag);
                expired.push(entry);
            }
        }

        expired
    }

    /// Recover all unacked messages (on disconnect).
    pub fn recover_all(&mut self) -> Vec<DeliveryEntry> {
        let mut recovered = Vec::new();
        let tags: Vec<u64> = self.delivery_order.drain(..).collect();

        for tag in tags {
            if let Some(entry) = self.deliveries.remove(&tag) {
                recovered.push(entry);
            }
        }

        recovered
    }

    /// Get unacked count.
    pub fn unacked_count(&self) -> usize {
        self.deliveries.len()
    }

    /// Check if at capacity.
    pub fn is_at_capacity(&self) -> bool {
        self.max_unacked > 0 && self.deliveries.len() as u64 >= self.max_unacked
    }

    /// Get delivery by tag.
    pub fn get(&self, delivery_tag: u64) -> Option<&DeliveryEntry> {
        self.deliveries.get(&delivery_tag)
    }

    /// Get mutable delivery by tag.
    pub fn get_mut(&mut self, delivery_tag: u64) -> Option<&mut DeliveryEntry> {
        self.deliveries.get_mut(&delivery_tag)
    }

    /// Extend deadline for a delivery.
    pub fn extend_deadline(
        &mut self,
        delivery_tag: u64,
        extension: Duration,
    ) -> Result<(), AckError> {
        let entry = self
            .deliveries
            .get_mut(&delivery_tag)
            .ok_or(AckError::UnknownDeliveryTag(delivery_tag))?;
        entry.extend_deadline(extension);
        Ok(())
    }
}

/// Acknowledgment errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckError {
    /// Delivery tag not found.
    UnknownDeliveryTag(u64),
    /// Consumer at capacity.
    CapacityExceeded { current: u64, max: u64 },
    /// Already acknowledged.
    AlreadyAcked(u64),
    /// Invalid for current mode.
    InvalidMode {
        mode: AckMode,
        operation: &'static str,
    },
}

impl std::fmt::Display for AckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AckError::UnknownDeliveryTag(tag) => write!(f, "unknown delivery tag: {tag}"),
            AckError::CapacityExceeded { current, max } => {
                write!(f, "capacity exceeded: {current}/{max}")
            }
            AckError::AlreadyAcked(tag) => write!(f, "already acknowledged: {tag}"),
            AckError::InvalidMode { mode, operation } => {
                write!(f, "invalid mode {:?} for operation: {operation}", mode)
            }
        }
    }
}

impl std::error::Error for AckError {}

// ---------------------------------------------------------------------------
// Redelivery Queue (Task 35)
// ---------------------------------------------------------------------------

/// Entry in the redelivery queue.
#[derive(Debug, Clone)]
pub struct RedeliveryEntry {
    /// Original message identifier.
    pub message_id: String,
    /// Source queue/topic.
    pub source: String,
    /// Message data.
    pub message_data: Vec<u8>,
    /// Number of delivery attempts.
    pub delivery_count: u32,
    /// Time to redeliver.
    pub redeliver_at: Instant,
    /// Reason for redelivery.
    pub reason: RedeliveryReason,
}

/// Reason for redelivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedeliveryReason {
    /// Acknowledgment timed out.
    Timeout,
    /// Consumer disconnected.
    Disconnect,
    /// Explicitly NACKed with requeue.
    Nack,
    /// Transaction aborted.
    TxnAbort,
}

/// Queue for messages pending redelivery.
#[derive(Debug, Clone, Default)]
pub struct RedeliveryQueue {
    /// Pending redeliveries ordered by time.
    entries: VecDeque<RedeliveryEntry>,
    /// Maximum delivery attempts before DLQ.
    max_delivery_attempts: u32,
    /// Base delay between redeliveries.
    base_delay: Duration,
    /// Maximum delay between redeliveries.
    max_delay: Duration,
}

impl RedeliveryQueue {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            max_delivery_attempts: 5,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
        }
    }

    pub fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_delivery_attempts = max;
        self
    }

    pub fn with_delays(mut self, base: Duration, max: Duration) -> Self {
        self.base_delay = base;
        self.max_delay = max;
        self
    }

    /// Schedule a message for redelivery.
    pub fn schedule(&mut self, entry: DeliveryEntry, reason: RedeliveryReason) -> RedeliveryResult {
        let delivery_count = entry.delivery_count + 1;

        if delivery_count > self.max_delivery_attempts {
            return RedeliveryResult::MaxAttemptsExceeded {
                message_id: entry.message_id,
                attempts: delivery_count,
            };
        }

        // Exponential backoff
        let delay_multiplier = 2u32.saturating_pow(delivery_count.saturating_sub(1));
        let delay = self.base_delay * delay_multiplier;
        let delay = delay.min(self.max_delay);

        let redeliver_entry = RedeliveryEntry {
            message_id: entry.message_id,
            source: entry.source,
            message_data: entry.message_data,
            delivery_count,
            redeliver_at: Instant::now() + delay,
            reason,
        };

        // Insert in order
        let pos = self
            .entries
            .iter()
            .position(|e| e.redeliver_at > redeliver_entry.redeliver_at);

        match pos {
            Some(i) => self.entries.insert(i, redeliver_entry),
            None => self.entries.push_back(redeliver_entry),
        }

        RedeliveryResult::Scheduled { delay }
    }

    /// Get messages ready for redelivery.
    pub fn ready(&mut self) -> Vec<RedeliveryEntry> {
        let now = Instant::now();
        let mut ready = Vec::new();

        while let Some(entry) = self.entries.front() {
            if entry.redeliver_at > now {
                break;
            }
            if let Some(e) = self.entries.pop_front() {
                ready.push(e);
            }
        }

        ready
    }

    /// Get pending count.
    pub fn pending_count(&self) -> usize {
        self.entries.len()
    }

    /// Time until next redelivery.
    pub fn next_redelivery_in(&self) -> Option<Duration> {
        self.entries
            .front()
            .and_then(|e| e.redeliver_at.checked_duration_since(Instant::now()))
    }

    /// Clear all pending redeliveries.
    pub fn clear(&mut self) -> Vec<RedeliveryEntry> {
        self.entries.drain(..).collect()
    }
}

/// Result of scheduling a redelivery.
#[derive(Debug, Clone)]
pub enum RedeliveryResult {
    /// Message scheduled for redelivery.
    Scheduled { delay: Duration },
    /// Maximum delivery attempts exceeded.
    MaxAttemptsExceeded { message_id: String, attempts: u32 },
}

// ---------------------------------------------------------------------------
// Publisher Confirms (Task 35)
// ---------------------------------------------------------------------------

/// Tracks publisher confirms (AMQP) / acks (Kafka).
#[derive(Debug, Clone)]
pub struct PublisherConfirmTracker {
    /// Pending confirms by sequence number.
    pending: HashMap<u64, PendingConfirm>,
    /// Next sequence number.
    next_seq: u64,
    /// Confirmed sequence numbers.
    confirmed: Vec<u64>,
    /// Nacked sequence numbers.
    nacked: Vec<u64>,
}

/// Pending publish confirm.
#[derive(Debug, Clone)]
pub struct PendingConfirm {
    /// Sequence number.
    pub seq: u64,
    /// Topic/exchange.
    pub destination: String,
    /// Routing key (if applicable).
    pub routing_key: Option<String>,
    /// Publish timestamp.
    pub published_at: Instant,
    /// Whether mandatory flag was set.
    pub mandatory: bool,
}

impl Default for PublisherConfirmTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PublisherConfirmTracker {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
            next_seq: 1,
            confirmed: Vec::new(),
            nacked: Vec::new(),
        }
    }

    /// Track a publish.
    pub fn track_publish(
        &mut self,
        destination: String,
        routing_key: Option<String>,
        mandatory: bool,
    ) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;

        self.pending.insert(
            seq,
            PendingConfirm {
                seq,
                destination,
                routing_key,
                published_at: Instant::now(),
                mandatory,
            },
        );

        seq
    }

    /// Confirm a publish.
    pub fn confirm(&mut self, seq: u64) -> Option<PendingConfirm> {
        let confirm = self.pending.remove(&seq);
        if confirm.is_some() {
            self.confirmed.push(seq);
        }
        confirm
    }

    /// Confirm multiple publishes.
    pub fn confirm_multiple(&mut self, up_to_seq: u64) -> Vec<PendingConfirm> {
        let mut confirmed = Vec::new();
        let seqs: Vec<u64> = self
            .pending
            .keys()
            .filter(|&&s| s <= up_to_seq)
            .copied()
            .collect();

        for seq in seqs {
            if let Some(c) = self.pending.remove(&seq) {
                self.confirmed.push(seq);
                confirmed.push(c);
            }
        }

        confirmed
    }

    /// Nack a publish.
    pub fn nack(&mut self, seq: u64) -> Option<PendingConfirm> {
        let confirm = self.pending.remove(&seq);
        if confirm.is_some() {
            self.nacked.push(seq);
        }
        confirm
    }

    /// Get pending count.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Drain confirmed sequences.
    pub fn drain_confirmed(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.confirmed)
    }

    /// Drain nacked sequences.
    pub fn drain_nacked(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.nacked)
    }

    /// Get pending publish by sequence.
    pub fn get_pending(&self, seq: u64) -> Option<&PendingConfirm> {
        self.pending.get(&seq)
    }
}

// ---------------------------------------------------------------------------
// Snapshot Support
// ---------------------------------------------------------------------------

/// Snapshot of acknowledgment state for persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AckTrackerSnapshot {
    pub consumer_id: String,
    pub next_tag: u64,
    pub deliveries: Vec<DeliverySnapshot>,
}

/// Snapshot of a delivery entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeliverySnapshot {
    pub delivery_tag: u64,
    pub message_id: String,
    pub source: String,
    pub delivery_count: u32,
    pub message_data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ack_tracker_basic() {
        let mut tracker = AckTracker::new("consumer1".to_string(), Duration::from_secs(30));

        let tag1 = tracker
            .track_delivery("msg1".to_string(), "queue1".to_string(), b"data1".to_vec())
            .unwrap();
        let tag2 = tracker
            .track_delivery("msg2".to_string(), "queue1".to_string(), b"data2".to_vec())
            .unwrap();

        assert_eq!(tracker.unacked_count(), 2);

        let entry = tracker.ack(tag1).unwrap();
        assert_eq!(entry.message_id, "msg1");
        assert_eq!(tracker.unacked_count(), 1);

        tracker.ack(tag2).unwrap();
        assert_eq!(tracker.unacked_count(), 0);
    }

    #[test]
    fn test_ack_multiple() {
        let mut tracker = AckTracker::new("consumer1".to_string(), Duration::from_secs(30));

        let _tag1 = tracker
            .track_delivery("msg1".to_string(), "q".to_string(), vec![])
            .unwrap();
        let tag2 = tracker
            .track_delivery("msg2".to_string(), "q".to_string(), vec![])
            .unwrap();
        let tag3 = tracker
            .track_delivery("msg3".to_string(), "q".to_string(), vec![])
            .unwrap();

        let acked = tracker.ack_multiple(tag2);
        assert_eq!(acked.len(), 2);
        assert_eq!(tracker.unacked_count(), 1);

        // tag3 should still be there
        assert!(tracker.get(tag3).is_some());
    }

    #[test]
    fn test_nack() {
        let mut tracker = AckTracker::new("consumer1".to_string(), Duration::from_secs(30));

        let tag = tracker
            .track_delivery("msg1".to_string(), "q".to_string(), vec![])
            .unwrap();

        let (entry, result) = tracker.nack(tag, true).unwrap();
        assert_eq!(entry.message_id, "msg1");
        assert!(matches!(result, AckResult::Nacked { requeue: true }));
    }

    #[test]
    fn test_capacity_limit() {
        let mut tracker =
            AckTracker::new("consumer1".to_string(), Duration::from_secs(30)).with_max_unacked(2);

        tracker
            .track_delivery("msg1".to_string(), "q".to_string(), vec![])
            .unwrap();
        tracker
            .track_delivery("msg2".to_string(), "q".to_string(), vec![])
            .unwrap();

        let result = tracker.track_delivery("msg3".to_string(), "q".to_string(), vec![]);
        assert!(matches!(result, Err(AckError::CapacityExceeded { .. })));
    }

    #[test]
    fn test_redelivery_queue() {
        let mut queue = RedeliveryQueue::new()
            .with_max_attempts(3)
            .with_delays(Duration::from_millis(10), Duration::from_secs(1));

        let entry = DeliveryEntry::new(
            1,
            "msg1".to_string(),
            "q".to_string(),
            "c".to_string(),
            Duration::from_secs(30),
            vec![],
        );

        let result = queue.schedule(entry, RedeliveryReason::Nack);
        assert!(matches!(result, RedeliveryResult::Scheduled { .. }));

        // Wait for ready - delivery_count goes from 1 to 2, delay is 10ms * 2^1 = 20ms
        std::thread::sleep(Duration::from_millis(25));
        let ready = queue.ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].delivery_count, 2);
    }

    #[test]
    fn test_max_redelivery_attempts() {
        let mut queue = RedeliveryQueue::new().with_max_attempts(2);

        let mut entry = DeliveryEntry::new(
            1,
            "msg1".to_string(),
            "q".to_string(),
            "c".to_string(),
            Duration::from_secs(30),
            vec![],
        );
        entry.delivery_count = 2;

        let result = queue.schedule(entry, RedeliveryReason::Nack);
        assert!(matches!(
            result,
            RedeliveryResult::MaxAttemptsExceeded { attempts: 3, .. }
        ));
    }

    #[test]
    fn test_publisher_confirms() {
        let mut tracker = PublisherConfirmTracker::new();

        let seq1 = tracker.track_publish("exchange".to_string(), Some("key".to_string()), false);
        let seq2 = tracker.track_publish("exchange".to_string(), None, true);

        assert_eq!(tracker.pending_count(), 2);

        tracker.confirm(seq1);
        assert_eq!(tracker.pending_count(), 1);
        assert_eq!(tracker.drain_confirmed(), vec![seq1]);

        tracker.nack(seq2);
        assert_eq!(tracker.pending_count(), 0);
        assert_eq!(tracker.drain_nacked(), vec![seq2]);
    }

    #[test]
    fn test_confirm_multiple() {
        let mut tracker = PublisherConfirmTracker::new();

        tracker.track_publish("ex".to_string(), None, false);
        tracker.track_publish("ex".to_string(), None, false);
        tracker.track_publish("ex".to_string(), None, false);

        let confirmed = tracker.confirm_multiple(2);
        assert_eq!(confirmed.len(), 2);
        assert_eq!(tracker.pending_count(), 1);
    }
}
