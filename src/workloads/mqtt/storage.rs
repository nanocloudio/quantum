//! MQTT-specific storage types separated from generic storage.
//!
//! This module defines `MqttSnapshot` (for checkpoints) and related types that
//! keep MQTT state isolated from the generic `ClustorStorage` layer.

use super::protocol::Qos;
use super::session::{PersistedInflight, PersistedOutbound};
use crate::dedupe::PersistedDedupeEntry;
use crate::offline::OfflineEntry;
use crate::prg::workload::CapabilityFlags;
use crate::routing::ForwardSeqEntry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metadata section of an MQTT snapshot (always required).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttSnapshotMetadata {
    /// Schema version for forward/backward compatibility.
    pub schema_version: u16,
    /// Last applied Raft/WAL index.
    pub applied_index: u64,
    /// Committed index for durability tracking.
    pub committed_index: u64,
    /// Capability bits indicating which optional sections are present.
    pub capability_bits: u32,
}

impl MqttSnapshotMetadata {
    pub const CURRENT_SCHEMA_VERSION: u16 = 1;

    pub fn new(applied_index: u64, committed_index: u64, capabilities: CapabilityFlags) -> Self {
        Self {
            schema_version: Self::CURRENT_SCHEMA_VERSION,
            applied_index,
            committed_index,
            capability_bits: capabilities.bits(),
        }
    }

    pub fn capabilities(&self) -> CapabilityFlags {
        CapabilityFlags::from_bits_truncate(self.capability_bits)
    }

    pub fn has_offline_queues(&self) -> bool {
        self.capabilities()
            .contains(CapabilityFlags::OFFLINE_QUEUES)
    }

    pub fn has_dedupe(&self) -> bool {
        self.capabilities().contains(CapabilityFlags::DEDUPE)
    }

    pub fn has_retained(&self) -> bool {
        self.capabilities().contains(CapabilityFlags::RETAINED)
    }
}

/// Session metadata section (always required).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttSessionSection {
    pub client_id: String,
    pub session_epoch: u64,
    pub keep_alive: u16,
    pub session_expiry_interval: Option<u32>,
    pub last_packet_at: Option<u64>,
    /// Dedupe floor for compaction.
    pub dedupe_floor: u64,
    /// Offline queue floor for compaction.
    pub offline_floor: u64,
    /// Earliest offline queue index.
    pub earliest_offline: u64,
    /// Forward chain floor for cross-PRG coordination.
    pub forward_chain_floor: u64,
    /// Effective product floor computed from all components.
    pub effective_product_floor: u64,
    /// Inflight QoS tracking.
    pub inflight: Vec<PersistedInflight>,
    /// Outbound delivery tracking.
    pub outbound: Vec<PersistedOutbound>,
    /// Dedupe entries for this session.
    pub dedupe: Vec<PersistedDedupeEntry>,
    /// Forward sequence tracking.
    pub forward_seq: Vec<ForwardSeqEntry>,
    /// Pointer to offline queue section (index into offline map).
    #[serde(default)]
    pub offline_queue_key: Option<String>,
}

/// Topics section for retained QoS1 publishes waiting for fanout (optional).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttTopicsSection {
    pub messages: Vec<StoredMessage>,
}

/// Stored message for topic fanout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: Qos,
    pub retained: bool,
}

/// Retained message record for plugin state (optional).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedRecord {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain_ts: Option<u64>,
}

/// Retained plugin state section (optional).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttRetainedSection {
    pub records: HashMap<String, RetainedRecord>,
}

/// Offline queue section keyed by client (optional).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttOfflineSection {
    pub queues: HashMap<String, Vec<OfflineEntry>>,
}

/// Subscription entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttSubscription {
    pub client_id: String,
    pub qos: Qos,
    #[serde(default)]
    pub shared_group: Option<String>,
}

/// Subscriptions section.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttSubscriptionsSection {
    pub subscriptions: HashMap<String, Vec<MqttSubscription>>,
}

/// Complete MQTT snapshot with clearly defined sections.
///
/// Each optional section sits behind capability flags so storage can skip
/// writing/reading bytes for workloads that disable them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqttSnapshot {
    /// Required: Snapshot version, applied index, capability bits.
    pub metadata: MqttSnapshotMetadata,
    /// Required: Session metadata (keepalive, inflight, dedupe table, offline pointers).
    pub sessions: HashMap<String, MqttSessionSection>,
    /// Required: Subscriptions index.
    pub subscriptions: MqttSubscriptionsSection,
    /// Optional (flagged via capability): Retained QoS1 publishes waiting for fanout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topics: Option<MqttTopicsSection>,
    /// Optional: Plugin state for retained topics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained: Option<MqttRetainedSection>,
    /// Optional: Offline queue payloads keyed by client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offline: Option<MqttOfflineSection>,
}

impl MqttSnapshot {
    /// Create a new snapshot with the given capabilities.
    pub fn new(capabilities: CapabilityFlags) -> Self {
        let mut snapshot = Self {
            metadata: MqttSnapshotMetadata::new(0, 0, capabilities),
            ..Default::default()
        };

        // Initialize optional sections based on capabilities
        if capabilities.contains(CapabilityFlags::OFFLINE_QUEUES) {
            snapshot.offline = Some(MqttOfflineSection::default());
        }
        if capabilities.contains(CapabilityFlags::RETAINED) {
            snapshot.retained = Some(MqttRetainedSection::default());
            snapshot.topics = Some(MqttTopicsSection::default());
        }

        snapshot
    }

    /// Convert to a workload snapshot envelope for storage.
    pub fn to_envelope(&self) -> WorkloadSnapshotEnvelope {
        let payload = bincode::serialize(self).unwrap_or_default();
        WorkloadSnapshotEnvelope {
            workload_label: "mqtt".to_string(),
            schema_version: MqttSnapshotMetadata::CURRENT_SCHEMA_VERSION,
            capability_bits: self.metadata.capability_bits,
            payload,
        }
    }

    /// Restore from a workload snapshot envelope.
    pub fn from_envelope(env: &WorkloadSnapshotEnvelope) -> anyhow::Result<Self> {
        if env.workload_label != "mqtt" {
            anyhow::bail!(
                "workload label mismatch: expected 'mqtt', got '{}'",
                env.workload_label
            );
        }
        bincode::deserialize(&env.payload)
            .map_err(|e| anyhow::anyhow!("failed to deserialize MQTT snapshot: {}", e))
    }

    /// Check if this snapshot has offline queue capability.
    pub fn has_offline_queues(&self) -> bool {
        self.metadata.has_offline_queues() && self.offline.is_some()
    }

    /// Check if this snapshot has retained message capability.
    pub fn has_retained(&self) -> bool {
        self.metadata.has_retained() && self.retained.is_some()
    }

    /// Get session by client ID.
    pub fn session(&self, client_id: &str) -> Option<&MqttSessionSection> {
        self.sessions.get(client_id)
    }

    /// Get mutable session by client ID.
    pub fn session_mut(&mut self, client_id: &str) -> Option<&mut MqttSessionSection> {
        self.sessions.get_mut(client_id)
    }

    /// Get offline entries for a client.
    pub fn offline_entries(&self, client_id: &str) -> Option<&Vec<OfflineEntry>> {
        self.offline.as_ref().and_then(|o| o.queues.get(client_id))
    }

    /// Get mutable offline entries for a client.
    pub fn offline_entries_mut(&mut self, client_id: &str) -> Option<&mut Vec<OfflineEntry>> {
        self.offline
            .as_mut()
            .and_then(|o| o.queues.get_mut(client_id))
    }

    /// Insert or update offline entries for a client.
    pub fn set_offline_entries(&mut self, client_id: String, entries: Vec<OfflineEntry>) {
        if let Some(ref mut offline) = self.offline {
            offline.queues.insert(client_id, entries);
        }
    }

    /// Get retained message for a topic.
    pub fn retained_message(&self, topic: &str) -> Option<&RetainedRecord> {
        self.retained.as_ref().and_then(|r| r.records.get(topic))
    }

    /// Store a retained message.
    pub fn store_retained(&mut self, topic: String, record: RetainedRecord) {
        if let Some(ref mut retained) = self.retained {
            retained.records.insert(topic, record);
        }
    }

    /// Delete a retained message.
    pub fn delete_retained(&mut self, topic: &str) {
        if let Some(ref mut retained) = self.retained {
            retained.records.remove(topic);
        }
    }

    /// Get subscriptions for a topic filter.
    pub fn subscriptions_for(&self, filter: &str) -> Option<&Vec<MqttSubscription>> {
        self.subscriptions.subscriptions.get(filter)
    }

    /// Add a subscription.
    pub fn add_subscription(&mut self, filter: String, subscription: MqttSubscription) {
        let entry = self.subscriptions.subscriptions.entry(filter).or_default();
        if !entry.iter().any(|s| {
            s.client_id == subscription.client_id && s.shared_group == subscription.shared_group
        }) {
            entry.push(subscription);
        }
    }

    /// Remove a subscription.
    pub fn remove_subscription(
        &mut self,
        filter: &str,
        client_id: &str,
        shared_group: Option<&str>,
    ) {
        if let Some(list) = self.subscriptions.subscriptions.get_mut(filter) {
            list.retain(|sub| {
                !(sub.client_id == client_id && sub.shared_group.as_deref() == shared_group)
            });
        }
    }

    /// Add a stored message to the topics section.
    pub fn add_topic_message(&mut self, msg: StoredMessage) {
        if let Some(ref mut topics) = self.topics {
            topics.messages.push(msg);
        }
    }

    /// Recompute effective product floors for all sessions.
    pub fn recompute_effective_product_floors(&mut self, clustor_floor: u64) {
        for session in self.sessions.values_mut() {
            let offline_floor = session.earliest_offline.max(session.offline_floor);
            session.effective_product_floor = clustor_floor
                .min(session.dedupe_floor)
                .min(offline_floor)
                .min(session.forward_chain_floor);
        }
    }
}

/// Generic envelope for storing workload snapshots.
///
/// `ClustorStorage` stores `(WorkloadSnapshotEnvelope, DurabilityProof)` and
/// records metadata. No MQTT modules appear in the storage crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadSnapshotEnvelope {
    /// Workload label (e.g., "mqtt").
    pub workload_label: String,
    /// Schema version for this workload.
    pub schema_version: u16,
    /// Capability bits indicating which optional sections are present.
    pub capability_bits: u32,
    /// Serialized workload-specific snapshot.
    pub payload: Vec<u8>,
}

impl WorkloadSnapshotEnvelope {
    /// Create a new envelope for a workload.
    pub fn new(
        workload_label: impl Into<String>,
        schema_version: u16,
        capability_bits: u32,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            workload_label: workload_label.into(),
            schema_version,
            capability_bits,
            payload,
        }
    }

    /// Get the workload label.
    pub fn workload_label(&self) -> &str {
        &self.workload_label
    }

    /// Get the schema version.
    pub fn schema_version(&self) -> u16 {
        self.schema_version
    }

    /// Get capability flags.
    pub fn capabilities(&self) -> CapabilityFlags {
        CapabilityFlags::from_bits_truncate(self.capability_bits)
    }
}
