//! Retained message plugin for MQTT workload.
//!
//! This module defines the `RetainedPlugin` trait and provides the MQTT-specific
//! implementation that stores retained messages per topic.

use crate::workloads::mqtt::storage::RetainedRecord;
use std::collections::BTreeMap;
use std::time::SystemTime;

/// Trait for workload-specific retained message handling.
///
/// Workloads provide an implementation if they need retained data;
/// otherwise they use `NoopRetained`.
pub trait RetainedPlugin: Send + Sync {
    /// Store a retained message for a topic.
    fn store(&mut self, topic: &str, payload: &[u8], qos: u8, retain: bool) -> anyhow::Result<()>;

    /// Fetch the retained message for a topic.
    fn fetch(&self, topic: &str) -> Option<RetainedRecord>;

    /// Delete the retained message for a topic.
    fn delete(&mut self, topic: &str) -> anyhow::Result<()>;

    /// Hydrate the plugin state from a snapshot.
    fn hydrate(&mut self, snapshot: &[RetainedRecord]);

    /// Snapshot the plugin state for persistence.
    fn snapshot(&self) -> Vec<RetainedRecord>;

    /// Check if the plugin is enabled.
    fn is_enabled(&self) -> bool;

    /// Get the number of retained messages.
    fn count(&self) -> usize;

    /// Get total bytes of retained messages.
    fn total_bytes(&self) -> u64;

    /// Iterate over all retained messages.
    fn iter(&self) -> Box<dyn Iterator<Item = (&String, &RetainedRecord)> + '_>;
}

/// MQTT retained message plugin implementation.
pub struct MqttRetainedPlugin {
    /// Retained messages stored per topic (BTreeMap for deterministic ordering).
    messages: BTreeMap<String, RetainedRecord>,
    /// Whether the plugin is enabled.
    enabled: bool,
    /// Total bytes of retained payloads.
    total_bytes: u64,
}

impl MqttRetainedPlugin {
    /// Create a new MQTT retained plugin.
    pub fn new() -> Self {
        Self {
            messages: BTreeMap::new(),
            enabled: true,
            total_bytes: 0,
        }
    }

    /// Enable or disable the plugin.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Get a retained message payload for a topic.
    pub fn get_payload(&self, topic: &str) -> Option<&[u8]> {
        self.messages.get(topic).map(|r| r.payload.as_slice())
    }

    /// Store a retained message with timestamp.
    fn store_with_timestamp(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: u8,
        timestamp: Option<u64>,
    ) -> anyhow::Result<()> {
        // If payload is empty, treat as delete
        if payload.is_empty() {
            return self.delete(topic);
        }

        // Update total bytes (subtract old, add new)
        if let Some(old) = self.messages.get(topic) {
            self.total_bytes = self.total_bytes.saturating_sub(old.payload.len() as u64);
        }
        self.total_bytes = self.total_bytes.saturating_add(payload.len() as u64);

        let record = RetainedRecord {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos,
            retain_ts: timestamp.or_else(|| {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .ok()
                    .map(|d| d.as_secs())
            }),
        };

        self.messages.insert(topic.to_string(), record);
        Ok(())
    }
}

impl Default for MqttRetainedPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl RetainedPlugin for MqttRetainedPlugin {
    fn store(&mut self, topic: &str, payload: &[u8], qos: u8, _retain: bool) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        self.store_with_timestamp(topic, payload, qos, None)
    }

    fn fetch(&self, topic: &str) -> Option<RetainedRecord> {
        if !self.enabled {
            return None;
        }
        self.messages.get(topic).cloned()
    }

    fn delete(&mut self, topic: &str) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if let Some(old) = self.messages.remove(topic) {
            self.total_bytes = self.total_bytes.saturating_sub(old.payload.len() as u64);
        }
        Ok(())
    }

    fn hydrate(&mut self, snapshot: &[RetainedRecord]) {
        self.messages.clear();
        self.total_bytes = 0;
        for record in snapshot {
            self.total_bytes = self.total_bytes.saturating_add(record.payload.len() as u64);
            self.messages.insert(record.topic.clone(), record.clone());
        }
    }

    fn snapshot(&self) -> Vec<RetainedRecord> {
        self.messages.values().cloned().collect()
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn count(&self) -> usize {
        self.messages.len()
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&String, &RetainedRecord)> + '_> {
        Box::new(self.messages.iter())
    }
}

/// No-op retained plugin for workloads that don't need retained messages.
pub struct NoopRetained;

impl RetainedPlugin for NoopRetained {
    fn store(
        &mut self,
        _topic: &str,
        _payload: &[u8],
        _qos: u8,
        _retain: bool,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn fetch(&self, _topic: &str) -> Option<RetainedRecord> {
        None
    }

    fn delete(&mut self, _topic: &str) -> anyhow::Result<()> {
        Ok(())
    }

    fn hydrate(&mut self, _snapshot: &[RetainedRecord]) {}

    fn snapshot(&self) -> Vec<RetainedRecord> {
        Vec::new()
    }

    fn is_enabled(&self) -> bool {
        false
    }

    fn count(&self) -> usize {
        0
    }

    fn total_bytes(&self) -> u64 {
        0
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&String, &RetainedRecord)> + '_> {
        Box::new(std::iter::empty())
    }
}

/// Metrics for retained plugin telemetry.
#[derive(Debug, Clone, Default)]
pub struct RetainedMetrics {
    /// Number of retained messages.
    pub entry_count: usize,
    /// Total bytes of retained payloads.
    pub total_bytes: u64,
    /// Number of evictions due to TTL or quota.
    pub evictions: u64,
}

impl RetainedMetrics {
    /// Create metrics from a plugin.
    pub fn from_plugin<P: RetainedPlugin + ?Sized>(plugin: &P) -> Self {
        Self {
            entry_count: plugin.count(),
            total_bytes: plugin.total_bytes(),
            evictions: 0,
        }
    }
}

/// Handle for workload access to retained plugin with telemetry.
pub struct RetainedPluginHandle<P: RetainedPlugin> {
    plugin: P,
    /// Delivery counter for metrics.
    deliveries: u64,
}

impl<P: RetainedPlugin> RetainedPluginHandle<P> {
    /// Create a new handle wrapping a plugin.
    pub fn new(plugin: P) -> Self {
        Self {
            plugin,
            deliveries: 0,
        }
    }

    /// Store a retained message.
    pub fn store(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
    ) -> anyhow::Result<()> {
        self.plugin.store(topic, payload, qos, retain)
    }

    /// Fetch and record delivery.
    pub fn fetch_for_delivery(&mut self, topic: &str) -> Option<RetainedRecord> {
        let result = self.plugin.fetch(topic);
        if result.is_some() {
            self.deliveries = self.deliveries.saturating_add(1);
        }
        result
    }

    /// Delete a retained message.
    pub fn delete(&mut self, topic: &str) -> anyhow::Result<()> {
        self.plugin.delete(topic)
    }

    /// Hydrate from snapshot.
    pub fn hydrate(&mut self, snapshot: &[RetainedRecord]) {
        self.plugin.hydrate(snapshot);
    }

    /// Get snapshot.
    pub fn snapshot(&self) -> Vec<RetainedRecord> {
        self.plugin.snapshot()
    }

    /// Get delivery count for metrics.
    pub fn delivery_count(&self) -> u64 {
        self.deliveries
    }

    /// Get metrics.
    pub fn metrics(&self) -> RetainedMetrics {
        RetainedMetrics::from_plugin(&self.plugin)
    }

    /// Check if enabled.
    pub fn is_enabled(&self) -> bool {
        self.plugin.is_enabled()
    }

    /// Get inner plugin reference.
    pub fn plugin(&self) -> &P {
        &self.plugin
    }

    /// Get mutable inner plugin reference.
    pub fn plugin_mut(&mut self) -> &mut P {
        &mut self.plugin
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_retained_plugin_store_fetch() {
        let mut plugin = MqttRetainedPlugin::new();

        plugin.store("test/topic", b"hello", 1, true).unwrap();

        let record = plugin.fetch("test/topic").unwrap();
        assert_eq!(record.topic, "test/topic");
        assert_eq!(record.payload, b"hello");
        assert_eq!(record.qos, 1);
    }

    #[test]
    fn test_mqtt_retained_plugin_delete() {
        let mut plugin = MqttRetainedPlugin::new();

        plugin.store("test/topic", b"hello", 1, true).unwrap();
        assert!(plugin.fetch("test/topic").is_some());

        plugin.delete("test/topic").unwrap();
        assert!(plugin.fetch("test/topic").is_none());
    }

    #[test]
    fn test_mqtt_retained_plugin_empty_payload_deletes() {
        let mut plugin = MqttRetainedPlugin::new();

        plugin.store("test/topic", b"hello", 1, true).unwrap();
        assert!(plugin.fetch("test/topic").is_some());

        // Empty payload should delete
        plugin.store("test/topic", b"", 1, true).unwrap();
        assert!(plugin.fetch("test/topic").is_none());
    }

    #[test]
    fn test_mqtt_retained_plugin_snapshot_hydrate() {
        let mut plugin = MqttRetainedPlugin::new();

        plugin.store("topic1", b"payload1", 1, true).unwrap();
        plugin.store("topic2", b"payload2", 0, true).unwrap();

        let snapshot = plugin.snapshot();
        assert_eq!(snapshot.len(), 2);

        let mut new_plugin = MqttRetainedPlugin::new();
        new_plugin.hydrate(&snapshot);

        assert!(new_plugin.fetch("topic1").is_some());
        assert!(new_plugin.fetch("topic2").is_some());
    }

    #[test]
    fn test_mqtt_retained_plugin_disabled() {
        let mut plugin = MqttRetainedPlugin::new();
        plugin.set_enabled(false);

        plugin.store("test/topic", b"hello", 1, true).unwrap();
        assert!(plugin.fetch("test/topic").is_none());
    }

    #[test]
    fn test_mqtt_retained_plugin_total_bytes() {
        let mut plugin = MqttRetainedPlugin::new();

        plugin.store("topic1", b"hello", 1, true).unwrap();
        assert_eq!(plugin.total_bytes(), 5);

        plugin.store("topic2", b"world!", 1, true).unwrap();
        assert_eq!(plugin.total_bytes(), 11);

        plugin.delete("topic1").unwrap();
        assert_eq!(plugin.total_bytes(), 6);
    }

    #[test]
    fn test_noop_retained() {
        let mut plugin = NoopRetained;

        plugin.store("test/topic", b"hello", 1, true).unwrap();
        assert!(plugin.fetch("test/topic").is_none());
        assert_eq!(plugin.count(), 0);
        assert!(!plugin.is_enabled());
    }
}
