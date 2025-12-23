//! Dedupe persistence interface for MQTT workload.
//!
//! This module defines the `DedupeStore` trait and provides the MQTT-specific
//! implementation that wraps the existing `DedupeTable`.

use crate::dedupe::{DedupeError, DedupeKey, DedupeTable, Direction, PersistedDedupeEntry};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Statistics about the dedupe store for telemetry and compaction.
#[derive(Debug, Clone, Default)]
pub struct DedupeStats {
    /// Number of entries in the dedupe table.
    pub entry_count: usize,
    /// Current floor index (earliest WAL index referenced).
    pub floor: u64,
    /// Timestamp of the oldest entry (if available).
    pub oldest_entry_ts: Option<Instant>,
}

/// Trait for workload-specific dedupe storage.
///
/// Workloads implement this trait to provide dedupe functionality.
/// MQTT uses `(client_id, direction, packet_id, session_epoch)` as the key.
/// Other workloads can define queue offsets, transactional ids, etc.
pub trait DedupeStore: Send + Sync {
    /// The record type for this dedupe store (must be serializable).
    type Record: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    /// Record a dedupe entry; returns true if this is a duplicate.
    fn record(&mut self, key: DedupeKey, wal_index: u64) -> Result<bool, DedupeError>;

    /// Update the WAL index for an existing entry.
    fn update(&mut self, key: &DedupeKey, wal_index: u64);

    /// Hydrate the store from persisted records.
    fn hydrate(&mut self, entries: &[Self::Record]);

    /// Snapshot the store for persistence.
    fn snapshot(&self) -> Vec<Self::Record>;

    /// Get the current floor (earliest WAL index referenced).
    fn floor(&self) -> u64;

    /// Get statistics for telemetry and compaction.
    fn stats(&self) -> DedupeStats;

    /// Check if a key is a duplicate without recording.
    fn is_duplicate(&self, key: &DedupeKey) -> bool;

    /// Get the WAL index for a key if it exists.
    fn wal_index(&self, key: &DedupeKey) -> Option<u64>;
}

/// MQTT-specific dedupe store wrapping the existing `DedupeTable`.
pub struct MqttDedupeStore {
    table: DedupeTable,
    enabled: bool,
}

impl MqttDedupeStore {
    /// Create a new MQTT dedupe store.
    pub fn new(per_session_cap: usize, per_tenant_cap: usize, horizon: Duration) -> Self {
        Self {
            table: DedupeTable::new(per_session_cap, per_tenant_cap, horizon),
            enabled: true,
        }
    }

    /// Create a dedupe store with default MQTT parameters.
    pub fn default_mqtt() -> Self {
        let horizon = Duration::from_secs(72 * 60 * 60); // 72 hours
        Self::new(4_096, 64_000_000, horizon)
    }

    /// Enable or disable the dedupe store.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Check if the dedupe store is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Record dedupe entry for a specific direction.
    pub fn record_direction(
        &mut self,
        tenant_id: &str,
        client_id: &str,
        session_epoch: u64,
        message_id: u16,
        direction: Direction,
        wal_index: u64,
    ) -> Result<bool, DedupeError> {
        if !self.enabled {
            return Ok(false);
        }
        let key = DedupeKey {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            session_epoch,
            message_id,
            direction,
        };
        self.record(key, wal_index)
    }

    /// Update dedupe floor for a publish after commit.
    pub fn update_publish_floor(
        &mut self,
        tenant_id: &str,
        client_id: &str,
        session_epoch: u64,
        message_id: u16,
        wal_index: u64,
    ) {
        if !self.enabled {
            return;
        }
        let key = DedupeKey {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            session_epoch,
            message_id,
            direction: Direction::Publish,
        };
        self.update(&key, wal_index);
    }

    /// Check if a publish is a duplicate.
    pub fn is_publish_duplicate(
        &self,
        tenant_id: &str,
        client_id: &str,
        session_epoch: u64,
        message_id: u16,
    ) -> bool {
        if !self.enabled {
            return false;
        }
        let key = DedupeKey {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            session_epoch,
            message_id,
            direction: Direction::Publish,
        };
        self.is_duplicate(&key)
    }

    /// Get the WAL index for an ack.
    pub fn ack_wal_index(
        &self,
        tenant_id: &str,
        client_id: &str,
        session_epoch: u64,
        message_id: u16,
    ) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        let key = DedupeKey {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            session_epoch,
            message_id,
            direction: Direction::Ack,
        };
        self.wal_index(&key)
    }

    /// Hydrate from instant (used during replay).
    pub fn hydrate_with_instant(&mut self, entries: &[PersistedDedupeEntry], now: Instant) {
        self.table.hydrate(entries, now);
    }
}

impl Default for MqttDedupeStore {
    fn default() -> Self {
        Self::default_mqtt()
    }
}

impl DedupeStore for MqttDedupeStore {
    type Record = PersistedDedupeEntry;

    fn record(&mut self, key: DedupeKey, wal_index: u64) -> Result<bool, DedupeError> {
        if !self.enabled {
            return Ok(false);
        }
        if self.table.is_duplicate(&key) {
            return Ok(true);
        }
        self.table.record(key, wal_index)?;
        Ok(false)
    }

    fn update(&mut self, key: &DedupeKey, wal_index: u64) {
        if self.enabled {
            self.table.update_wal_index(key, wal_index);
        }
    }

    fn hydrate(&mut self, entries: &[Self::Record]) {
        self.table.hydrate(entries, Instant::now());
    }

    fn snapshot(&self) -> Vec<Self::Record> {
        self.table.snapshot()
    }

    fn floor(&self) -> u64 {
        self.table.earliest_index()
    }

    fn stats(&self) -> DedupeStats {
        DedupeStats {
            entry_count: self.snapshot().len(),
            floor: self.floor(),
            oldest_entry_ts: None, // Could be enhanced to track this
        }
    }

    fn is_duplicate(&self, key: &DedupeKey) -> bool {
        if !self.enabled {
            return false;
        }
        self.table.is_duplicate(key)
    }

    fn wal_index(&self, key: &DedupeKey) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        self.table.wal_index(key)
    }
}

/// Envelope for storing dedupe records with schema metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupeEnvelope {
    /// Workload label.
    pub workload: String,
    /// Schema version.
    pub schema_version: u16,
    /// Serialized dedupe records.
    pub payload: Vec<u8>,
}

impl DedupeEnvelope {
    /// Create an envelope for MQTT dedupe records.
    pub fn mqtt(records: &[PersistedDedupeEntry]) -> Self {
        Self {
            workload: "mqtt".to_string(),
            schema_version: 1,
            payload: bincode::serialize(records).unwrap_or_default(),
        }
    }

    /// Decode MQTT dedupe records from envelope.
    pub fn decode_mqtt(&self) -> anyhow::Result<Vec<PersistedDedupeEntry>> {
        if self.workload != "mqtt" {
            anyhow::bail!("expected mqtt workload, got {}", self.workload);
        }
        bincode::deserialize(&self.payload)
            .map_err(|e| anyhow::anyhow!("failed to decode mqtt dedupe records: {}", e))
    }
}

/// No-op dedupe store for workloads that don't need dedupe.
pub struct NoopDedupeStore;

impl DedupeStore for NoopDedupeStore {
    type Record = ();

    fn record(&mut self, _key: DedupeKey, _wal_index: u64) -> Result<bool, DedupeError> {
        Ok(false)
    }

    fn update(&mut self, _key: &DedupeKey, _wal_index: u64) {}

    fn hydrate(&mut self, _entries: &[Self::Record]) {}

    fn snapshot(&self) -> Vec<Self::Record> {
        Vec::new()
    }

    fn floor(&self) -> u64 {
        0
    }

    fn stats(&self) -> DedupeStats {
        DedupeStats::default()
    }

    fn is_duplicate(&self, _key: &DedupeKey) -> bool {
        false
    }

    fn wal_index(&self, _key: &DedupeKey) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_dedupe_store_record() {
        let mut store = MqttDedupeStore::default_mqtt();
        let key = DedupeKey {
            tenant_id: "tenant1".to_string(),
            client_id: "client1".to_string(),
            session_epoch: 1,
            message_id: 100,
            direction: Direction::Publish,
        };

        // First record should not be a duplicate
        assert!(!store.record(key.clone(), 1).unwrap());

        // Second record with same key should be a duplicate
        assert!(store.record(key.clone(), 2).unwrap());
    }

    #[test]
    fn test_mqtt_dedupe_store_disabled() {
        let mut store = MqttDedupeStore::default_mqtt();
        store.set_enabled(false);

        let key = DedupeKey {
            tenant_id: "tenant1".to_string(),
            client_id: "client1".to_string(),
            session_epoch: 1,
            message_id: 100,
            direction: Direction::Publish,
        };

        // When disabled, nothing is recorded
        assert!(!store.record(key.clone(), 1).unwrap());
        assert!(!store.record(key.clone(), 2).unwrap());
        assert!(!store.is_duplicate(&key));
    }

    #[test]
    fn test_mqtt_dedupe_store_snapshot_hydrate() {
        let mut store = MqttDedupeStore::default_mqtt();
        let key = DedupeKey {
            tenant_id: "tenant1".to_string(),
            client_id: "client1".to_string(),
            session_epoch: 1,
            message_id: 100,
            direction: Direction::Publish,
        };

        store.record(key.clone(), 42).unwrap();
        let snapshot = store.snapshot();

        let mut new_store = MqttDedupeStore::default_mqtt();
        new_store.hydrate(&snapshot);

        assert!(new_store.is_duplicate(&key));
    }

    #[test]
    fn test_noop_dedupe_store() {
        let mut store = NoopDedupeStore;
        let key = DedupeKey {
            tenant_id: "tenant1".to_string(),
            client_id: "client1".to_string(),
            session_epoch: 1,
            message_id: 100,
            direction: Direction::Publish,
        };

        // Noop store never records duplicates
        assert!(!store.record(key.clone(), 1).unwrap());
        assert!(!store.record(key.clone(), 2).unwrap());
        assert!(!store.is_duplicate(&key));
    }
}
