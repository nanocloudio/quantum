//! Offline queue controls for MQTT workload.
//!
//! This module provides the offline queue management with feature flag support,
//! allowing non-MQTT workloads to disable queue management per workload/tenant.

use crate::offline::OfflineEntry;
use std::collections::{HashMap, VecDeque};

/// Offline queue statistics for telemetry.
#[derive(Debug, Clone, Default)]
pub struct OfflineQueueStats {
    /// Number of entries across all queues.
    pub entry_count: u64,
    /// Total bytes of payloads.
    pub total_bytes: u64,
    /// Earliest enqueue index.
    pub earliest_index: u64,
    /// Number of queues (clients with offline entries).
    pub queue_count: usize,
}

/// Result hint when offline queueing is skipped.
#[derive(Debug, Clone)]
pub enum OfflineSkipHint {
    /// Queue was skipped because the feature is disabled.
    FeatureDisabled,
    /// Queue was skipped due to capacity limits.
    CapacityExceeded,
    /// Entry was routed to dead-letter queue instead.
    DeadLettered,
}

/// Trait for workload-specific offline queue management.
pub trait OfflineQueueManager: Send + Sync {
    /// Check if offline queues are enabled.
    fn is_enabled(&self) -> bool;

    /// Enable or disable offline queues.
    fn set_enabled(&mut self, enabled: bool);

    /// Enqueue an entry for a client.
    fn enqueue(&mut self, client_id: &str, entry: OfflineEntry) -> Result<(), OfflineSkipHint>;

    /// Dequeue an entry for a client.
    fn dequeue(&mut self, client_id: &str) -> Option<OfflineEntry>;

    /// Peek at the next entry without removing it.
    fn peek(&self, client_id: &str) -> Option<&OfflineEntry>;

    /// Clear entries up to a given index.
    fn clear_upto(&mut self, client_id: &str, upto_index: u64);

    /// Get all entries for a client.
    fn entries(&self, client_id: &str) -> Option<&VecDeque<OfflineEntry>>;

    /// Get mutable entries for a client.
    fn entries_mut(&mut self, client_id: &str) -> Option<&mut VecDeque<OfflineEntry>>;

    /// Remove a client's queue entirely.
    fn remove_client(&mut self, client_id: &str);

    /// Get statistics.
    fn stats(&self) -> OfflineQueueStats;

    /// Hydrate from persisted entries.
    fn hydrate(&mut self, entries: HashMap<String, Vec<OfflineEntry>>);

    /// Snapshot for persistence.
    fn snapshot(&self) -> HashMap<String, Vec<OfflineEntry>>;

    /// Get earliest offline queue index across all clients.
    fn earliest_index(&self) -> u64;
}

/// MQTT offline queue manager with feature flag support.
pub struct MqttOfflineManager {
    /// Per-client offline queues.
    queues: HashMap<String, VecDeque<OfflineEntry>>,
    /// Whether offline queues are enabled.
    enabled: bool,
    /// Maximum entries per client (0 = unlimited).
    max_entries_per_client: usize,
    /// Maximum total bytes per client (0 = unlimited).
    max_bytes_per_client: u64,
    /// Current bytes per client.
    bytes_per_client: HashMap<String, u64>,
    /// Total entries across all queues.
    total_entries: u64,
    /// Total bytes across all queues.
    total_bytes: u64,
}

impl MqttOfflineManager {
    /// Create a new MQTT offline manager.
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
            enabled: true,
            max_entries_per_client: 0,
            max_bytes_per_client: 0,
            bytes_per_client: HashMap::new(),
            total_entries: 0,
            total_bytes: 0,
        }
    }

    /// Create with capacity limits.
    pub fn with_limits(max_entries_per_client: usize, max_bytes_per_client: u64) -> Self {
        Self {
            queues: HashMap::new(),
            enabled: true,
            max_entries_per_client,
            max_bytes_per_client,
            bytes_per_client: HashMap::new(),
            total_entries: 0,
            total_bytes: 0,
        }
    }

    /// Set maximum entries per client.
    pub fn set_max_entries_per_client(&mut self, max: usize) {
        self.max_entries_per_client = max;
    }

    /// Set maximum bytes per client.
    pub fn set_max_bytes_per_client(&mut self, max: u64) {
        self.max_bytes_per_client = max;
    }

    /// Check if capacity would be exceeded.
    fn would_exceed_capacity(&self, client_id: &str, entry: &OfflineEntry) -> bool {
        if self.max_entries_per_client > 0 {
            if let Some(queue) = self.queues.get(client_id) {
                if queue.len() >= self.max_entries_per_client {
                    return true;
                }
            }
        }

        if self.max_bytes_per_client > 0 {
            let current_bytes = self.bytes_per_client.get(client_id).copied().unwrap_or(0);
            if current_bytes.saturating_add(entry.payload.len() as u64) > self.max_bytes_per_client
            {
                return true;
            }
        }

        false
    }

    /// Record bytes for a client.
    fn record_bytes(&mut self, client_id: &str, bytes: u64) {
        *self
            .bytes_per_client
            .entry(client_id.to_string())
            .or_default() += bytes;
        self.total_bytes = self.total_bytes.saturating_add(bytes);
    }

    /// Subtract bytes for a client.
    fn subtract_bytes(&mut self, client_id: &str, bytes: u64) {
        if let Some(client_bytes) = self.bytes_per_client.get_mut(client_id) {
            *client_bytes = client_bytes.saturating_sub(bytes);
        }
        self.total_bytes = self.total_bytes.saturating_sub(bytes);
    }

    /// Compute earliest index from all queues.
    fn compute_earliest_index(&self) -> u64 {
        self.queues
            .values()
            .filter_map(|q| q.front().map(|e| e.enqueue_index))
            .min()
            .unwrap_or(0)
    }
}

impl Default for MqttOfflineManager {
    fn default() -> Self {
        Self::new()
    }
}

impl OfflineQueueManager for MqttOfflineManager {
    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    fn enqueue(&mut self, client_id: &str, entry: OfflineEntry) -> Result<(), OfflineSkipHint> {
        if !self.enabled {
            return Err(OfflineSkipHint::FeatureDisabled);
        }

        if self.would_exceed_capacity(client_id, &entry) {
            return Err(OfflineSkipHint::CapacityExceeded);
        }

        let bytes = entry.payload.len() as u64;
        self.queues
            .entry(client_id.to_string())
            .or_default()
            .push_back(entry);
        self.record_bytes(client_id, bytes);
        self.total_entries = self.total_entries.saturating_add(1);

        Ok(())
    }

    fn dequeue(&mut self, client_id: &str) -> Option<OfflineEntry> {
        if !self.enabled {
            return None;
        }

        if let Some(queue) = self.queues.get_mut(client_id) {
            if let Some(entry) = queue.pop_front() {
                let bytes = entry.payload.len() as u64;
                self.subtract_bytes(client_id, bytes);
                self.total_entries = self.total_entries.saturating_sub(1);
                return Some(entry);
            }
        }

        None
    }

    fn peek(&self, client_id: &str) -> Option<&OfflineEntry> {
        if !self.enabled {
            return None;
        }
        self.queues.get(client_id).and_then(|q| q.front())
    }

    fn clear_upto(&mut self, client_id: &str, upto_index: u64) {
        if !self.enabled {
            return;
        }

        if let Some(queue) = self.queues.get_mut(client_id) {
            let mut removed_bytes = 0u64;
            let mut removed_count = 0u64;

            queue.retain(|entry| {
                if entry.enqueue_index <= upto_index {
                    removed_bytes += entry.payload.len() as u64;
                    removed_count += 1;
                    false
                } else {
                    true
                }
            });

            self.subtract_bytes(client_id, removed_bytes);
            self.total_entries = self.total_entries.saturating_sub(removed_count);
        }
    }

    fn entries(&self, client_id: &str) -> Option<&VecDeque<OfflineEntry>> {
        self.queues.get(client_id)
    }

    fn entries_mut(&mut self, client_id: &str) -> Option<&mut VecDeque<OfflineEntry>> {
        self.queues.get_mut(client_id)
    }

    fn remove_client(&mut self, client_id: &str) {
        if let Some(queue) = self.queues.remove(client_id) {
            let bytes: u64 = queue.iter().map(|e| e.payload.len() as u64).sum();
            self.total_entries = self.total_entries.saturating_sub(queue.len() as u64);
            self.total_bytes = self.total_bytes.saturating_sub(bytes);
            self.bytes_per_client.remove(client_id);
        }
    }

    fn stats(&self) -> OfflineQueueStats {
        OfflineQueueStats {
            entry_count: self.total_entries,
            total_bytes: self.total_bytes,
            earliest_index: self.compute_earliest_index(),
            queue_count: self.queues.len(),
        }
    }

    fn hydrate(&mut self, entries: HashMap<String, Vec<OfflineEntry>>) {
        self.queues.clear();
        self.bytes_per_client.clear();
        self.total_entries = 0;
        self.total_bytes = 0;

        for (client_id, client_entries) in entries {
            let mut queue = VecDeque::new();
            let mut bytes = 0u64;

            for entry in client_entries {
                bytes += entry.payload.len() as u64;
                queue.push_back(entry);
            }

            self.total_entries += queue.len() as u64;
            self.total_bytes += bytes;
            self.bytes_per_client.insert(client_id.clone(), bytes);
            self.queues.insert(client_id, queue);
        }
    }

    fn snapshot(&self) -> HashMap<String, Vec<OfflineEntry>> {
        self.queues
            .iter()
            .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
            .collect()
    }

    fn earliest_index(&self) -> u64 {
        self.compute_earliest_index()
    }
}

/// No-op offline queue manager for workloads that don't need offline queues.
pub struct NoopOfflineManager;

impl OfflineQueueManager for NoopOfflineManager {
    fn is_enabled(&self) -> bool {
        false
    }

    fn set_enabled(&mut self, _enabled: bool) {}

    fn enqueue(&mut self, _client_id: &str, _entry: OfflineEntry) -> Result<(), OfflineSkipHint> {
        Err(OfflineSkipHint::FeatureDisabled)
    }

    fn dequeue(&mut self, _client_id: &str) -> Option<OfflineEntry> {
        None
    }

    fn peek(&self, _client_id: &str) -> Option<&OfflineEntry> {
        None
    }

    fn clear_upto(&mut self, _client_id: &str, _upto_index: u64) {}

    fn entries(&self, _client_id: &str) -> Option<&VecDeque<OfflineEntry>> {
        None
    }

    fn entries_mut(&mut self, _client_id: &str) -> Option<&mut VecDeque<OfflineEntry>> {
        None
    }

    fn remove_client(&mut self, _client_id: &str) {}

    fn stats(&self) -> OfflineQueueStats {
        OfflineQueueStats::default()
    }

    fn hydrate(&mut self, _entries: HashMap<String, Vec<OfflineEntry>>) {}

    fn snapshot(&self) -> HashMap<String, Vec<OfflineEntry>> {
        HashMap::new()
    }

    fn earliest_index(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workloads::mqtt::Qos;

    fn make_entry(client_id: &str, index: u64, payload: &[u8]) -> OfflineEntry {
        OfflineEntry {
            client_id: client_id.to_string(),
            topic: "test/topic".to_string(),
            qos: Qos::AtLeastOnce,
            retain: false,
            enqueue_index: index,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn test_mqtt_offline_manager_enqueue_dequeue() {
        let mut manager = MqttOfflineManager::new();

        let entry = make_entry("client1", 1, b"hello");
        manager.enqueue("client1", entry).unwrap();

        let dequeued = manager.dequeue("client1").unwrap();
        assert_eq!(dequeued.enqueue_index, 1);
        assert_eq!(dequeued.payload, b"hello");

        assert!(manager.dequeue("client1").is_none());
    }

    #[test]
    fn test_mqtt_offline_manager_disabled() {
        let mut manager = MqttOfflineManager::new();
        manager.set_enabled(false);

        let entry = make_entry("client1", 1, b"hello");
        let result = manager.enqueue("client1", entry);
        assert!(matches!(result, Err(OfflineSkipHint::FeatureDisabled)));

        assert!(manager.dequeue("client1").is_none());
    }

    #[test]
    fn test_mqtt_offline_manager_capacity_limit() {
        let mut manager = MqttOfflineManager::with_limits(2, 0);

        manager
            .enqueue("client1", make_entry("client1", 1, b"a"))
            .unwrap();
        manager
            .enqueue("client1", make_entry("client1", 2, b"b"))
            .unwrap();

        let result = manager.enqueue("client1", make_entry("client1", 3, b"c"));
        assert!(matches!(result, Err(OfflineSkipHint::CapacityExceeded)));
    }

    #[test]
    fn test_mqtt_offline_manager_clear_upto() {
        let mut manager = MqttOfflineManager::new();

        manager
            .enqueue("client1", make_entry("client1", 1, b"a"))
            .unwrap();
        manager
            .enqueue("client1", make_entry("client1", 2, b"b"))
            .unwrap();
        manager
            .enqueue("client1", make_entry("client1", 3, b"c"))
            .unwrap();

        manager.clear_upto("client1", 2);

        let stats = manager.stats();
        assert_eq!(stats.entry_count, 1);

        let entry = manager.dequeue("client1").unwrap();
        assert_eq!(entry.enqueue_index, 3);
    }

    #[test]
    fn test_mqtt_offline_manager_stats() {
        let mut manager = MqttOfflineManager::new();

        manager
            .enqueue("client1", make_entry("client1", 1, b"hello"))
            .unwrap();
        manager
            .enqueue("client2", make_entry("client2", 2, b"world!"))
            .unwrap();

        let stats = manager.stats();
        assert_eq!(stats.entry_count, 2);
        assert_eq!(stats.total_bytes, 11);
        assert_eq!(stats.queue_count, 2);
        assert_eq!(stats.earliest_index, 1);
    }

    #[test]
    fn test_mqtt_offline_manager_snapshot_hydrate() {
        let mut manager = MqttOfflineManager::new();

        manager
            .enqueue("client1", make_entry("client1", 1, b"hello"))
            .unwrap();
        manager
            .enqueue("client1", make_entry("client1", 2, b"world"))
            .unwrap();

        let snapshot = manager.snapshot();

        let mut new_manager = MqttOfflineManager::new();
        new_manager.hydrate(snapshot);

        let stats = new_manager.stats();
        assert_eq!(stats.entry_count, 2);
        assert_eq!(stats.total_bytes, 10);
    }

    #[test]
    fn test_noop_offline_manager() {
        let mut manager = NoopOfflineManager;

        let entry = make_entry("client1", 1, b"hello");
        let result = manager.enqueue("client1", entry);
        assert!(matches!(result, Err(OfflineSkipHint::FeatureDisabled)));

        assert!(!manager.is_enabled());
        assert!(manager.dequeue("client1").is_none());
    }
}
