//! Prefetch and flow control for consumer throttling.
//!
//! This module provides protocol-agnostic prefetch management:
//! - Count-based prefetch limits
//! - Size-based prefetch limits
//! - Per-channel and global prefetch (AMQP style)
//! - Per-partition prefetch (Kafka style)
//!
//! Tasks 37-40: Prefetch, per-channel, per-partition, tests

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Prefetch Configuration (Task 37)
// ---------------------------------------------------------------------------

/// Prefetch configuration.
#[derive(Debug, Clone, Copy, Default)]
pub struct PrefetchConfig {
    /// Maximum number of unacknowledged messages (0 = unlimited).
    pub count: u32,
    /// Maximum size in bytes of unacknowledged messages (0 = unlimited).
    pub size: u64,
    /// Whether this is a global limit.
    pub global: bool,
}

impl PrefetchConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_count(mut self, count: u32) -> Self {
        self.count = count;
        self
    }

    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }

    pub fn global(mut self) -> Self {
        self.global = true;
        self
    }

    /// Check if unlimited.
    pub fn is_unlimited(&self) -> bool {
        self.count == 0 && self.size == 0
    }
}

// ---------------------------------------------------------------------------
// Prefetch State
// ---------------------------------------------------------------------------

/// Current prefetch state.
#[derive(Debug, Clone, Default)]
pub struct PrefetchState {
    /// Current outstanding message count.
    pub outstanding_count: u32,
    /// Current outstanding message size in bytes.
    pub outstanding_size: u64,
}

impl PrefetchState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a delivery.
    pub fn record_delivery(&mut self, size: u64) {
        self.outstanding_count = self.outstanding_count.saturating_add(1);
        self.outstanding_size = self.outstanding_size.saturating_add(size);
    }

    /// Record an acknowledgment.
    pub fn record_ack(&mut self, size: u64) {
        self.outstanding_count = self.outstanding_count.saturating_sub(1);
        self.outstanding_size = self.outstanding_size.saturating_sub(size);
    }

    /// Record multiple acknowledgments.
    pub fn record_ack_multiple(&mut self, count: u32, size: u64) {
        self.outstanding_count = self.outstanding_count.saturating_sub(count);
        self.outstanding_size = self.outstanding_size.saturating_sub(size);
    }

    /// Check if can deliver based on config.
    pub fn can_deliver(&self, config: &PrefetchConfig, message_size: u64) -> bool {
        // Count check
        if config.count > 0 && self.outstanding_count >= config.count {
            return false;
        }

        // Size check
        if config.size > 0 && self.outstanding_size + message_size > config.size {
            return false;
        }

        true
    }

    /// Get available capacity (count).
    pub fn available_count(&self, config: &PrefetchConfig) -> u32 {
        if config.count == 0 {
            u32::MAX
        } else {
            config.count.saturating_sub(self.outstanding_count)
        }
    }

    /// Get available capacity (size).
    pub fn available_size(&self, config: &PrefetchConfig) -> u64 {
        if config.size == 0 {
            u64::MAX
        } else {
            config.size.saturating_sub(self.outstanding_size)
        }
    }

    /// Reset state.
    pub fn reset(&mut self) {
        self.outstanding_count = 0;
        self.outstanding_size = 0;
    }
}

// ---------------------------------------------------------------------------
// Channel Prefetch Controller (Task 38)
// ---------------------------------------------------------------------------

/// Per-channel prefetch controller (AMQP style).
#[derive(Debug, Clone)]
pub struct ChannelPrefetchController {
    /// Per-consumer prefetch config.
    consumer_config: HashMap<String, PrefetchConfig>,
    /// Per-consumer prefetch state.
    consumer_state: HashMap<String, PrefetchState>,
    /// Global prefetch config (applies to all consumers).
    global_config: Option<PrefetchConfig>,
    /// Global prefetch state.
    global_state: PrefetchState,
    /// Channel identifier.
    channel_id: u16,
}

impl ChannelPrefetchController {
    pub fn new(channel_id: u16) -> Self {
        Self {
            consumer_config: HashMap::new(),
            consumer_state: HashMap::new(),
            global_config: None,
            global_state: PrefetchState::new(),
            channel_id,
        }
    }

    /// Set QoS for a consumer (AMQP basic.qos).
    pub fn set_qos(&mut self, consumer_tag: Option<&str>, config: PrefetchConfig) {
        if config.global || consumer_tag.is_none() {
            self.global_config = Some(config);
        } else if let Some(tag) = consumer_tag {
            self.consumer_config.insert(tag.to_string(), config);
        }
    }

    /// Check if can deliver to a consumer.
    pub fn can_deliver(&self, consumer_tag: &str, message_size: u64) -> bool {
        // Check global limit first
        if let Some(ref global) = self.global_config {
            if !self.global_state.can_deliver(global, message_size) {
                return false;
            }
        }

        // Check per-consumer limit
        if let Some(config) = self.consumer_config.get(consumer_tag) {
            let state = self
                .consumer_state
                .get(consumer_tag)
                .cloned()
                .unwrap_or_default();
            if !state.can_deliver(config, message_size) {
                return false;
            }
        }

        true
    }

    /// Record a delivery.
    pub fn record_delivery(&mut self, consumer_tag: &str, message_size: u64) {
        self.global_state.record_delivery(message_size);

        self.consumer_state
            .entry(consumer_tag.to_string())
            .or_default()
            .record_delivery(message_size);
    }

    /// Record an acknowledgment.
    pub fn record_ack(&mut self, consumer_tag: &str, message_size: u64) {
        self.global_state.record_ack(message_size);

        if let Some(state) = self.consumer_state.get_mut(consumer_tag) {
            state.record_ack(message_size);
        }
    }

    /// Record multiple acknowledgments.
    pub fn record_ack_multiple(&mut self, consumer_tag: &str, count: u32, size: u64) {
        self.global_state.record_ack_multiple(count, size);

        if let Some(state) = self.consumer_state.get_mut(consumer_tag) {
            state.record_ack_multiple(count, size);
        }
    }

    /// Get available credits for a consumer.
    pub fn available_credits(&self, consumer_tag: &str) -> (u32, u64) {
        let mut count = u32::MAX;
        let mut size = u64::MAX;

        // Global limit
        if let Some(ref global) = self.global_config {
            count = count.min(self.global_state.available_count(global));
            size = size.min(self.global_state.available_size(global));
        }

        // Per-consumer limit
        if let Some(config) = self.consumer_config.get(consumer_tag) {
            let state = self
                .consumer_state
                .get(consumer_tag)
                .cloned()
                .unwrap_or_default();
            count = count.min(state.available_count(config));
            size = size.min(state.available_size(config));
        }

        (count, size)
    }

    /// Remove a consumer.
    pub fn remove_consumer(&mut self, consumer_tag: &str) {
        // Transfer outstanding to global before removing
        if let Some(state) = self.consumer_state.remove(consumer_tag) {
            // State was already counted in global, so just remove from consumer map
            let _ = state;
        }
        self.consumer_config.remove(consumer_tag);
    }

    /// Get channel ID.
    pub fn channel_id(&self) -> u16 {
        self.channel_id
    }

    /// Get global state snapshot.
    pub fn global_snapshot(&self) -> &PrefetchState {
        &self.global_state
    }
}

// ---------------------------------------------------------------------------
// Partition Prefetch Controller (Task 39)
// ---------------------------------------------------------------------------

/// Partition identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionId {
    pub topic: String,
    pub partition: i32,
}

impl PartitionId {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// Kafka-style fetch configuration.
#[derive(Debug, Clone, Copy)]
pub struct FetchConfig {
    /// Maximum number of records to fetch per poll.
    pub max_poll_records: u32,
    /// Minimum bytes to fetch (wait for this much data).
    pub fetch_min_bytes: u32,
    /// Maximum bytes to fetch per partition.
    pub fetch_max_bytes_per_partition: u32,
    /// Maximum bytes to fetch total.
    pub fetch_max_bytes: u32,
    /// Maximum wait time in milliseconds.
    pub fetch_max_wait_ms: u32,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            max_poll_records: 500,
            fetch_min_bytes: 1,
            fetch_max_bytes_per_partition: 1024 * 1024, // 1MB
            fetch_max_bytes: 50 * 1024 * 1024,          // 50MB
            fetch_max_wait_ms: 500,
        }
    }
}

/// Per-partition fetch state.
#[derive(Debug, Clone, Default)]
pub struct PartitionFetchState {
    /// Current position (offset).
    pub position: i64,
    /// High watermark.
    pub high_watermark: i64,
    /// Last fetched offset.
    pub last_fetched_offset: i64,
    /// Records buffered.
    pub buffered_records: u32,
    /// Bytes buffered.
    pub buffered_bytes: u64,
    /// Whether fetch is paused.
    pub paused: bool,
}

impl PartitionFetchState {
    pub fn new(position: i64) -> Self {
        Self {
            position,
            high_watermark: position,
            last_fetched_offset: position - 1,
            buffered_records: 0,
            buffered_bytes: 0,
            paused: false,
        }
    }

    /// Calculate lag.
    pub fn lag(&self) -> i64 {
        (self.high_watermark - self.position).max(0)
    }

    /// Check if has buffered data.
    pub fn has_buffered(&self) -> bool {
        self.buffered_records > 0
    }
}

/// Per-partition prefetch controller (Kafka style).
#[derive(Debug, Clone)]
pub struct PartitionPrefetchController {
    /// Fetch configuration.
    config: FetchConfig,
    /// Per-partition state.
    partitions: HashMap<PartitionId, PartitionFetchState>,
    /// Total buffered records.
    total_buffered_records: u32,
    /// Total buffered bytes.
    total_buffered_bytes: u64,
}

impl PartitionPrefetchController {
    pub fn new(config: FetchConfig) -> Self {
        Self {
            config,
            partitions: HashMap::new(),
            total_buffered_records: 0,
            total_buffered_bytes: 0,
        }
    }

    /// Assign partitions.
    pub fn assign(&mut self, partitions: Vec<(PartitionId, i64)>) {
        for (pid, offset) in partitions {
            self.partitions
                .insert(pid, PartitionFetchState::new(offset));
        }
    }

    /// Revoke partitions.
    pub fn revoke(&mut self, partitions: &[PartitionId]) {
        for pid in partitions {
            if let Some(state) = self.partitions.remove(pid) {
                self.total_buffered_records = self
                    .total_buffered_records
                    .saturating_sub(state.buffered_records);
                self.total_buffered_bytes = self
                    .total_buffered_bytes
                    .saturating_sub(state.buffered_bytes);
            }
        }
    }

    /// Seek a partition to an offset.
    pub fn seek(&mut self, partition: &PartitionId, offset: i64) {
        if let Some(state) = self.partitions.get_mut(partition) {
            // Clear buffer on seek
            self.total_buffered_records = self
                .total_buffered_records
                .saturating_sub(state.buffered_records);
            self.total_buffered_bytes = self
                .total_buffered_bytes
                .saturating_sub(state.buffered_bytes);

            state.position = offset;
            state.last_fetched_offset = offset - 1;
            state.buffered_records = 0;
            state.buffered_bytes = 0;
        }
    }

    /// Pause a partition.
    pub fn pause(&mut self, partition: &PartitionId) {
        if let Some(state) = self.partitions.get_mut(partition) {
            state.paused = true;
        }
    }

    /// Resume a partition.
    pub fn resume(&mut self, partition: &PartitionId) {
        if let Some(state) = self.partitions.get_mut(partition) {
            state.paused = false;
        }
    }

    /// Check if should fetch more records.
    pub fn should_fetch(&self) -> bool {
        // Don't fetch if at max poll records
        if self.total_buffered_records >= self.config.max_poll_records {
            return false;
        }

        // Don't fetch if at max bytes
        if self.total_buffered_bytes >= self.config.fetch_max_bytes as u64 {
            return false;
        }

        // Fetch if any partition has capacity and isn't paused
        self.partitions
            .values()
            .any(|s| !s.paused && !s.has_buffered())
    }

    /// Build fetch request for partitions that need data.
    pub fn build_fetch_request(&self) -> Vec<FetchPartitionRequest> {
        let mut requests = Vec::new();
        let mut remaining_bytes = self.config.fetch_max_bytes as u64 - self.total_buffered_bytes;

        for (pid, state) in &self.partitions {
            if state.paused {
                continue;
            }

            let max_bytes = (self.config.fetch_max_bytes_per_partition as u64).min(remaining_bytes);

            if max_bytes == 0 {
                break;
            }

            requests.push(FetchPartitionRequest {
                topic: pid.topic.clone(),
                partition: pid.partition,
                fetch_offset: state.last_fetched_offset + 1,
                max_bytes: max_bytes as u32,
            });

            remaining_bytes = remaining_bytes.saturating_sub(max_bytes);
        }

        requests
    }

    /// Record fetched records.
    pub fn record_fetch(
        &mut self,
        partition: &PartitionId,
        records: u32,
        bytes: u64,
        last_offset: i64,
        high_watermark: i64,
    ) {
        if let Some(state) = self.partitions.get_mut(partition) {
            state.buffered_records = state.buffered_records.saturating_add(records);
            state.buffered_bytes = state.buffered_bytes.saturating_add(bytes);
            state.last_fetched_offset = last_offset;
            state.high_watermark = high_watermark;

            self.total_buffered_records = self.total_buffered_records.saturating_add(records);
            self.total_buffered_bytes = self.total_buffered_bytes.saturating_add(bytes);
        }
    }

    /// Record consumed records (poll).
    pub fn record_consume(
        &mut self,
        partition: &PartitionId,
        records: u32,
        bytes: u64,
        new_position: i64,
    ) {
        if let Some(state) = self.partitions.get_mut(partition) {
            state.buffered_records = state.buffered_records.saturating_sub(records);
            state.buffered_bytes = state.buffered_bytes.saturating_sub(bytes);
            state.position = new_position;

            self.total_buffered_records = self.total_buffered_records.saturating_sub(records);
            self.total_buffered_bytes = self.total_buffered_bytes.saturating_sub(bytes);
        }
    }

    /// Get partition state.
    pub fn partition_state(&self, partition: &PartitionId) -> Option<&PartitionFetchState> {
        self.partitions.get(partition)
    }

    /// Get total lag across all partitions.
    pub fn total_lag(&self) -> i64 {
        self.partitions.values().map(|s| s.lag()).sum()
    }

    /// Get partition count.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Get config.
    pub fn config(&self) -> &FetchConfig {
        &self.config
    }

    /// Update config.
    pub fn set_config(&mut self, config: FetchConfig) {
        self.config = config;
    }
}

/// Request to fetch from a partition.
#[derive(Debug, Clone)]
pub struct FetchPartitionRequest {
    pub topic: String,
    pub partition: i32,
    pub fetch_offset: i64,
    pub max_bytes: u32,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetch_state() {
        let config = PrefetchConfig::new().with_count(10);
        let mut state = PrefetchState::new();

        assert!(state.can_deliver(&config, 100));

        for _ in 0..10 {
            state.record_delivery(100);
        }

        assert!(!state.can_deliver(&config, 100));
        assert_eq!(state.outstanding_count, 10);

        state.record_ack(100);
        assert!(state.can_deliver(&config, 100));
    }

    #[test]
    fn test_prefetch_size_limit() {
        let config = PrefetchConfig::new().with_size(1000);
        let mut state = PrefetchState::new();

        assert!(state.can_deliver(&config, 500));
        state.record_delivery(500);

        assert!(state.can_deliver(&config, 500));
        state.record_delivery(500);

        // At limit, can't deliver more
        assert!(!state.can_deliver(&config, 100));
    }

    #[test]
    fn test_channel_prefetch_controller() {
        let mut ctrl = ChannelPrefetchController::new(1);

        // Set global QoS
        ctrl.set_qos(None, PrefetchConfig::new().with_count(5).global());

        // Should be able to deliver
        assert!(ctrl.can_deliver("consumer1", 100));

        // Record deliveries
        for _ in 0..5 {
            ctrl.record_delivery("consumer1", 100);
        }

        // At limit
        assert!(!ctrl.can_deliver("consumer1", 100));

        // Ack one
        ctrl.record_ack("consumer1", 100);
        assert!(ctrl.can_deliver("consumer1", 100));
    }

    #[test]
    fn test_channel_prefetch_per_consumer() {
        let mut ctrl = ChannelPrefetchController::new(1);

        // Set per-consumer QoS
        ctrl.set_qos(Some("c1"), PrefetchConfig::new().with_count(2));
        ctrl.set_qos(Some("c2"), PrefetchConfig::new().with_count(3));

        // c1 can have 2
        ctrl.record_delivery("c1", 100);
        ctrl.record_delivery("c1", 100);
        assert!(!ctrl.can_deliver("c1", 100));

        // c2 can have 3
        ctrl.record_delivery("c2", 100);
        ctrl.record_delivery("c2", 100);
        assert!(ctrl.can_deliver("c2", 100));
        ctrl.record_delivery("c2", 100);
        assert!(!ctrl.can_deliver("c2", 100));
    }

    #[test]
    fn test_partition_prefetch_controller() {
        let config = FetchConfig {
            max_poll_records: 100,
            ..Default::default()
        };
        let mut ctrl = PartitionPrefetchController::new(config);

        let p0 = PartitionId::new("topic1", 0);
        let p1 = PartitionId::new("topic1", 1);

        ctrl.assign(vec![(p0.clone(), 0), (p1.clone(), 0)]);

        assert!(ctrl.should_fetch());

        let requests = ctrl.build_fetch_request();
        assert_eq!(requests.len(), 2);

        // Record fetch
        ctrl.record_fetch(&p0, 50, 5000, 49, 100);
        assert_eq!(ctrl.total_buffered_records, 50);

        // Still should fetch (not at max)
        assert!(ctrl.should_fetch());

        // Fill up
        ctrl.record_fetch(&p1, 50, 5000, 49, 100);
        assert!(!ctrl.should_fetch()); // At max_poll_records
    }

    #[test]
    fn test_partition_pause_resume() {
        let mut ctrl = PartitionPrefetchController::new(FetchConfig::default());

        let p0 = PartitionId::new("topic1", 0);
        ctrl.assign(vec![(p0.clone(), 0)]);

        assert!(ctrl.should_fetch());

        ctrl.pause(&p0);
        let requests = ctrl.build_fetch_request();
        assert!(requests.is_empty());

        ctrl.resume(&p0);
        let requests = ctrl.build_fetch_request();
        assert_eq!(requests.len(), 1);
    }

    #[test]
    fn test_partition_seek() {
        let mut ctrl = PartitionPrefetchController::new(FetchConfig::default());

        let p0 = PartitionId::new("topic1", 0);
        ctrl.assign(vec![(p0.clone(), 0)]);

        ctrl.record_fetch(&p0, 10, 1000, 9, 100);
        assert_eq!(ctrl.total_buffered_records, 10);

        ctrl.seek(&p0, 50);
        assert_eq!(ctrl.total_buffered_records, 0);
        assert_eq!(ctrl.partition_state(&p0).unwrap().position, 50);
    }

    #[test]
    fn test_partition_lag() {
        let mut ctrl = PartitionPrefetchController::new(FetchConfig::default());

        let p0 = PartitionId::new("topic1", 0);
        ctrl.assign(vec![(p0.clone(), 0)]);

        ctrl.record_fetch(&p0, 10, 1000, 9, 100);
        assert_eq!(ctrl.partition_state(&p0).unwrap().lag(), 100);

        ctrl.record_consume(&p0, 5, 500, 5);
        assert_eq!(ctrl.partition_state(&p0).unwrap().lag(), 95);
    }
}
