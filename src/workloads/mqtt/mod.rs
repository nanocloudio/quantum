//! MQTT workload implementation.
//!
//! This module provides the MQTT protocol workload that implements the
//! `PrgWorkload` trait, along with supporting abstractions for:
//! - Protocol parsing and packet handling
//! - Session management
//! - Subscription handling
//! - Dedupe persistence
//! - Retained message plugin
//! - Offline queue management
//! - Backpressure policies and flow control
//! - Topic alias management (MQTT 5.0)
//! - Message expiry interval handling (MQTT 5.0)
//! - MQTT 5.0 properties (request/response, subscription identifiers)
//! - Flow control (receive maximum)
//! - Session lifecycle (disconnect, client ID assignment)
//! - Protocol metrics and monitoring

pub mod alias;
pub mod backpressure;
pub mod dedupe;
pub mod expiry;
pub mod flow_control;
pub mod lifecycle;
pub mod metrics;
pub mod offline;
pub mod properties;
pub mod protocol;
pub mod retained;
pub mod session;
pub mod storage;
pub mod subscriptions;

// Re-export protocol types at module level for convenience
pub use protocol::*;

pub use self::protocol::Qos;
use crate::forwarding::ForwardRequest;
use crate::offline::OfflineEntry;
use crate::prg::workload::{
    ApplyContext, CapabilityFlags, PrgWorkload, ReadyContext, WorkloadDescriptor, WorkloadError,
    WorkloadFeatures, WorkloadInit, WorkloadResult,
};
use crate::prg::{PersistedPrgState, PersistedSessionState, StoredMessage, Subscription};
use crate::routing::{PrgId, RoutingEpoch};
use crate::storage::compaction::ProductFloors;

pub use self::backpressure::{MqttBackpressurePolicy, MqttDedupeHook, MqttOfflineQueueHook};
pub use self::dedupe::{DedupeStats, DedupeStore, MqttDedupeStore, NoopDedupeStore};
pub use self::offline::{
    MqttOfflineManager, NoopOfflineManager, OfflineQueueManager, OfflineQueueStats, OfflineSkipHint,
};
pub use self::retained::{
    MqttRetainedPlugin, NoopRetained, RetainedMetrics, RetainedPlugin, RetainedPluginHandle,
};
pub use self::storage::{
    MqttOfflineSection, MqttRetainedSection, MqttSessionSection, MqttSnapshot,
    MqttSnapshotMetadata, MqttSubscription, MqttSubscriptionsSection, MqttTopicsSection,
    RetainedRecord, StoredMessage as MqttStoredMessage, WorkloadSnapshotEnvelope,
};

/// Apply commands representing MQTT state transitions.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MqttApply {
    PersistSession {
        session: PersistedSessionState,
    },
    CommitPublish {
        topic: String,
        payload: Vec<u8>,
        qos: Qos,
        retain: bool,
    },
    RecordOffline {
        client_id: String,
        entry: OfflineEntry,
    },
    ClearOffline {
        client_id: String,
        upto_index: u64,
    },
    AddSubscription {
        filter: String,
        subscription: Subscription,
    },
    RemoveSubscription {
        filter: String,
        client_id: String,
        shared_group: Option<String>,
    },
    ForwardIntent {
        target_prg: PrgId,
        seq: u64,
        routing_epoch: u64,
        local_epoch: u64,
        allow_grace: bool,
        publish: ForwardPublishPayload,
    },
}

impl MqttApply {
    pub fn label(&self) -> &'static str {
        match self {
            MqttApply::PersistSession { .. } => "persist_session",
            MqttApply::CommitPublish { .. } => "commit_publish",
            MqttApply::RecordOffline { .. } => "record_offline",
            MqttApply::ClearOffline { .. } => "clear_offline",
            MqttApply::AddSubscription { .. } => "add_subscription",
            MqttApply::RemoveSubscription { .. } => "remove_subscription",
            MqttApply::ForwardIntent { .. } => "forward_intent",
        }
    }
}

/// Payload encoded inside forward intents for remote publish delivery.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForwardPublishPayload {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: Qos,
    pub retain: bool,
}

impl ForwardPublishPayload {
    pub fn into_envelope(self) -> Result<Vec<u8>, WorkloadError> {
        bincode::serialize(&self).map_err(|err| {
            WorkloadError::Permanent(format!("forward payload encode failed: {err:?}").into())
        })
    }

    pub fn from_envelope(bytes: &[u8]) -> Result<Self, WorkloadError> {
        bincode::deserialize(bytes).map_err(|err| {
            WorkloadError::Permanent(format!("forward payload decode failed: {err:?}").into())
        })
    }

    pub fn into_entry(self) -> MqttApply {
        MqttApply::CommitPublish {
            topic: self.topic,
            payload: self.payload,
            qos: self.qos,
            retain: self.retain,
        }
    }
}

/// MQTT workload implementing the PrgWorkload trait.
///
/// This workload manages MQTT-specific state including:
/// - Session metadata and dedupe tables
/// - Offline queues for QoS > 0 messages
/// - Retained message store
/// - Subscription index
#[derive(Debug, Clone)]
pub struct MqttWorkload {
    state: PersistedPrgState,
    descriptor: WorkloadDescriptor,
    features: WorkloadFeatures,
}

impl MqttWorkload {
    pub fn new(state: PersistedPrgState) -> Self {
        Self {
            state,
            descriptor: Self::descriptor_static(),
            features: WorkloadFeatures::default(),
        }
    }

    pub fn descriptor_static() -> WorkloadDescriptor {
        WorkloadDescriptor::new(
            "mqtt",
            1,
            CapabilityFlags::OFFLINE_QUEUES | CapabilityFlags::DEDUPE | CapabilityFlags::RETAINED,
        )
    }

    /// Get a reference to the internal state.
    pub fn state(&self) -> &PersistedPrgState {
        &self.state
    }

    /// Get a mutable reference to the internal state.
    pub fn state_mut(&mut self) -> &mut PersistedPrgState {
        &mut self.state
    }

    /// Check if offline queues are enabled.
    pub fn offline_queues_enabled(&self) -> bool {
        self.features.enable_offline_queues
    }

    /// Check if dedupe is enabled.
    pub fn dedupe_enabled(&self) -> bool {
        self.features.enable_dedupe
    }

    /// Check if retained messages are enabled.
    pub fn retained_enabled(&self) -> bool {
        self.features.enable_retained
    }

    fn persist_session(&mut self, session: PersistedSessionState, ctx: &ApplyContext) {
        self.state
            .sessions
            .insert(session.client_id.clone(), session.clone());
        self.state
            .recompute_effective_product_floors(ctx.ack_contract().clustor_floor());
    }

    fn commit_publish(&mut self, topic: String, payload: Vec<u8>, qos: Qos, retain: bool) {
        self.state.topics.push(StoredMessage {
            topic: topic.clone(),
            payload: payload.clone(),
            qos,
            retained: retain,
        });
        if retain && self.retained_enabled() {
            self.state.retained.insert(topic, payload);
        }
    }

    fn record_offline(&mut self, client_id: String, entry: OfflineEntry) {
        if !self.offline_queues_enabled() {
            return;
        }
        self.state.offline.entry(client_id).or_default().push(entry);
    }

    fn clear_offline(&mut self, client_id: String, upto_index: u64) {
        if !self.offline_queues_enabled() {
            return;
        }
        if let Some(entries) = self.state.offline.get_mut(&client_id) {
            entries.retain(|entry| entry.enqueue_index > upto_index);
        }
    }

    fn add_subscription(&mut self, filter: String, subscription: Subscription) {
        let entry = self.state.subscriptions.entry(filter).or_default();
        if !entry.iter().any(|s| {
            s.client_id == subscription.client_id && s.shared_group == subscription.shared_group
        }) {
            entry.push(subscription);
        }
    }

    fn remove_subscription(&mut self, filter: String, client_id: String, shared: Option<String>) {
        if let Some(list) = self.state.subscriptions.get_mut(&filter) {
            list.retain(|sub| !(sub.client_id == client_id && sub.shared_group == shared));
        }
    }

    /// Compute product floors from current state.
    pub fn compute_product_floors(&self, clustor_floor: u64) -> ProductFloors {
        let mut local_dedup = 0;
        let mut earliest_offline = 0;
        let mut forward_chain = 0;

        for session in self.state.sessions.values() {
            local_dedup = local_dedup.max(session.dedupe_floor);
            earliest_offline =
                earliest_offline.max(session.earliest_offline.max(session.offline_floor));
            forward_chain = forward_chain.max(session.forward_chain_floor);
        }

        ProductFloors {
            clustor_floor,
            local_dedup_floor: local_dedup,
            earliest_offline_queue_index: earliest_offline,
            forward_chain_floor: forward_chain,
        }
    }

    /// Get offline entries for a client.
    pub fn offline_entries(&self, client_id: &str) -> Option<&Vec<OfflineEntry>> {
        self.state.offline.get(client_id)
    }

    /// Get retained message for a topic.
    pub fn retained(&self, topic: &str) -> Option<&Vec<u8>> {
        self.state.retained.get(topic)
    }

    /// Get subscriptions for a topic filter.
    pub fn subscriptions(&self, filter: &str) -> Option<&Vec<Subscription>> {
        self.state.subscriptions.get(filter)
    }

    /// Get session state for a client.
    pub fn session(&self, client_id: &str) -> Option<&PersistedSessionState> {
        self.state.sessions.get(client_id)
    }
}

impl Default for MqttWorkload {
    fn default() -> Self {
        Self::new(PersistedPrgState::default())
    }
}

impl PrgWorkload for MqttWorkload {
    type Snapshot = PersistedPrgState;
    type LogEntry = MqttApply;

    fn descriptor(&self) -> WorkloadDescriptor {
        self.descriptor
    }

    fn init(&mut self, init: WorkloadInit<'_>) -> WorkloadResult<()> {
        self.features = init.features.clone();
        if let Err(err) = init.schemas.ensure_workload(&self.descriptor) {
            return WorkloadResult::from_error(WorkloadError::Permanent(
                format!("schema registry error: {err}").into(),
            ));
        }
        WorkloadResult::ok(())
    }

    fn hydrate(&mut self, snapshot: &Self::Snapshot) -> WorkloadResult<()> {
        self.state = snapshot.clone();
        WorkloadResult::ok(())
    }

    fn build_snapshot(&self) -> WorkloadResult<Self::Snapshot> {
        WorkloadResult::ok(self.state.clone())
    }

    fn apply(&mut self, entry: Self::LogEntry, ctx: &mut ApplyContext) -> WorkloadResult<()> {
        match entry {
            MqttApply::PersistSession { session } => self.persist_session(session, ctx),
            MqttApply::CommitPublish {
                topic,
                payload,
                qos,
                retain,
            } => self.commit_publish(topic, payload, qos, retain),
            MqttApply::RecordOffline { client_id, entry } => {
                self.record_offline(client_id, entry);
            }
            MqttApply::ClearOffline {
                client_id,
                upto_index,
            } => self.clear_offline(client_id, upto_index),
            MqttApply::AddSubscription {
                filter,
                subscription,
            } => self.add_subscription(filter, subscription),
            MqttApply::RemoveSubscription {
                filter,
                client_id,
                shared_group,
            } => self.remove_subscription(filter, client_id, shared_group),
            MqttApply::ForwardIntent {
                target_prg,
                seq,
                routing_epoch,
                local_epoch,
                allow_grace,
                publish,
            } => {
                return self.forward_publish_intent(
                    target_prg,
                    seq,
                    routing_epoch,
                    local_epoch,
                    allow_grace,
                    publish,
                    ctx,
                )
            }
        }
        WorkloadResult::ok(())
    }

    fn on_ready(&mut self, _ready: &mut ReadyContext) -> WorkloadResult<()> {
        WorkloadResult::ok(())
    }
}

impl MqttWorkload {
    #[allow(clippy::too_many_arguments)]
    fn forward_publish_intent(
        &self,
        target_prg: PrgId,
        seq: u64,
        routing_epoch: u64,
        local_epoch: u64,
        allow_grace: bool,
        publish: ForwardPublishPayload,
        ctx: &mut ApplyContext,
    ) -> WorkloadResult<()> {
        let request = ForwardRequest {
            session_prg: ctx.prg_id().clone(),
            topic_prg: target_prg,
            seq,
            routing_epoch: RoutingEpoch(routing_epoch),
            local_epoch: RoutingEpoch(local_epoch),
            allow_grace,
        };
        let envelope = match publish.into_envelope() {
            Ok(bytes) => bytes,
            Err(err) => return WorkloadResult::from_error(err),
        };
        ctx.record_forward_intent(request, envelope, self.descriptor.label);
        WorkloadResult::ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DurabilityConfig;
    use crate::flow::BackpressureState;
    use crate::forwarding::ForwardingEngine;
    use crate::prg::workload::{
        ApplyMeta, CompactionHandle, SchemaRegistryHandle, WorkloadMetrics,
    };
    use crate::replication::consensus::AckContract;
    use std::sync::Arc;
    use std::time::Duration;

    fn make_apply_context() -> ApplyContext {
        let cfg = DurabilityConfig::default();
        let ack = Arc::new(AckContract::new(&cfg));
        let metrics = WorkloadMetrics::new(Arc::new(BackpressureState::default()));
        let compaction = CompactionHandle::default();
        let schemas = SchemaRegistryHandle::default();
        let prg_id = PrgId {
            tenant_id: "test".to_string(),
            partition_index: 0,
        };
        let meta = ApplyMeta::new(1, 1, 0);
        let forwarding = ForwardingEngine::new(
            AckContract::new(&cfg),
            Arc::new(BackpressureState::default()),
            Duration::from_secs(600),
        );

        ApplyContext::new(
            ack,
            metrics,
            compaction,
            schemas,
            prg_id,
            meta,
            tracing::info_span!("test"),
            forwarding,
        )
    }

    #[test]
    fn test_mqtt_workload_commit_publish() {
        let mut workload = MqttWorkload::default();
        let mut ctx = make_apply_context();

        let entry = MqttApply::CommitPublish {
            topic: "test/topic".to_string(),
            payload: b"hello".to_vec(),
            qos: Qos::AtLeastOnce,
            retain: false,
        };

        workload.apply(entry, &mut ctx).into_result().unwrap();
        assert_eq!(workload.state.topics.len(), 1);
        assert_eq!(workload.state.topics[0].topic, "test/topic");
    }

    #[test]
    fn test_mqtt_workload_commit_publish_retained() {
        let mut workload = MqttWorkload::default();
        workload.features.enable_retained = true;
        let mut ctx = make_apply_context();

        let entry = MqttApply::CommitPublish {
            topic: "test/topic".to_string(),
            payload: b"hello".to_vec(),
            qos: Qos::AtLeastOnce,
            retain: true,
        };

        workload.apply(entry, &mut ctx).into_result().unwrap();
        assert!(workload.retained("test/topic").is_some());
    }

    #[test]
    fn test_mqtt_workload_offline_queues() {
        let mut workload = MqttWorkload::default();
        workload.features.enable_offline_queues = true;
        let mut ctx = make_apply_context();

        let entry = MqttApply::RecordOffline {
            client_id: "client1".to_string(),
            entry: OfflineEntry {
                client_id: "client1".to_string(),
                topic: "test/topic".to_string(),
                qos: Qos::AtLeastOnce,
                retain: false,
                enqueue_index: 1,
                payload: b"hello".to_vec(),
            },
        };

        workload.apply(entry, &mut ctx).into_result().unwrap();
        assert!(workload.offline_entries("client1").is_some());
        assert_eq!(workload.offline_entries("client1").unwrap().len(), 1);
    }

    #[test]
    fn test_mqtt_workload_subscriptions() {
        let mut workload = MqttWorkload::default();
        let mut ctx = make_apply_context();

        let entry = MqttApply::AddSubscription {
            filter: "test/#".to_string(),
            subscription: Subscription {
                client_id: "client1".to_string(),
                qos: Qos::AtLeastOnce,
                shared_group: None,
            },
        };

        workload.apply(entry, &mut ctx).into_result().unwrap();
        assert!(workload.subscriptions("test/#").is_some());
        assert_eq!(workload.subscriptions("test/#").unwrap().len(), 1);
    }

    #[test]
    fn test_mqtt_workload_snapshot_hydrate() {
        let mut workload = MqttWorkload::default();
        let mut ctx = make_apply_context();

        // Add some state
        let entry = MqttApply::CommitPublish {
            topic: "test/topic".to_string(),
            payload: b"hello".to_vec(),
            qos: Qos::AtLeastOnce,
            retain: false,
        };
        workload.apply(entry, &mut ctx).into_result().unwrap();

        // Snapshot
        let snapshot = workload.build_snapshot().into_result().unwrap();

        // Create new workload and hydrate
        let mut new_workload = MqttWorkload::default();
        new_workload.hydrate(&snapshot).into_result().unwrap();

        assert_eq!(new_workload.state.topics.len(), 1);
    }
}
