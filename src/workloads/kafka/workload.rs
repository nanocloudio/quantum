//! Kafka workload implementation.
//!
//! This module implements `PrgWorkload` for Kafka protocol support,
//! managing topics, partitions, consumer groups, and transactions.

use crate::messaging::{
    ConsumerGroup, GroupCoordinator, GroupMember, GroupState, TopicPartition,
    TransactionCoordinator,
};
use crate::prg::workload::{
    ApplyContext, CapabilityFlags, PrgWorkload, ReadyContext, WorkloadDescriptor, WorkloadFeatures,
    WorkloadInit, WorkloadResult,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::ErrorCode;

// ---------------------------------------------------------------------------
// Workload Descriptor
// ---------------------------------------------------------------------------

/// Kafka workload descriptor.
pub const KAFKA_DESCRIPTOR: WorkloadDescriptor =
    WorkloadDescriptor::new("kafka", 1, CapabilityFlags::OFFLINE_QUEUES);

// ---------------------------------------------------------------------------
// KafkaWorkload
// ---------------------------------------------------------------------------

/// Kafka protocol workload.
#[derive(Debug, Default)]
pub struct KafkaWorkload {
    /// Workload features.
    features: WorkloadFeatures,
    /// Kafka-specific state.
    pub state: KafkaState,
}

/// Kafka workload state.
#[derive(Debug, Default)]
pub struct KafkaState {
    /// Topics and their metadata.
    pub topics: HashMap<String, TopicMetadata>,
    /// Consumer groups.
    pub groups: GroupCoordinator,
    /// Transaction coordinator.
    pub transactions: TransactionCoordinator,
    /// Producer ID allocation.
    pub producer_ids: ProducerIdAllocator,
    /// Idempotent producer state.
    pub idempotent_producers: HashMap<i64, ProducerState>,
    /// Cluster configuration.
    pub config: KafkaConfig,
}

/// Topic metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    /// Topic name.
    pub name: String,
    /// Topic ID (stored as string for serde compatibility).
    pub topic_id: String,
    /// Number of partitions.
    pub num_partitions: u32,
    /// Replication factor.
    pub replication_factor: u16,
    /// Topic configuration.
    pub config: TopicConfig,
    /// Partition states.
    pub partitions: Vec<PartitionState>,
    /// Is internal topic.
    pub is_internal: bool,
    /// Creation timestamp.
    pub created_at: u64,
}

impl TopicMetadata {
    /// Create a new topic with default configuration.
    pub fn new(name: String, num_partitions: u32, replication_factor: u16) -> Self {
        let partitions = (0..num_partitions)
            .map(|i| PartitionState::new(i as i32))
            .collect();

        Self {
            name,
            topic_id: uuid::Uuid::new_v4().to_string(),
            num_partitions,
            replication_factor,
            config: TopicConfig::default(),
            partitions,
            is_internal: false,
            created_at: current_timestamp(),
        }
    }

    /// Get the topic ID as a UUID.
    pub fn topic_uuid(&self) -> Option<uuid::Uuid> {
        uuid::Uuid::parse_str(&self.topic_id).ok()
    }

    /// Get partition state by index.
    pub fn partition(&self, index: i32) -> Option<&PartitionState> {
        self.partitions.get(index as usize)
    }

    /// Get mutable partition state by index.
    pub fn partition_mut(&mut self, index: i32) -> Option<&mut PartitionState> {
        self.partitions.get_mut(index as usize)
    }
}

/// Topic configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    /// Retention time in milliseconds (-1 = forever).
    pub retention_ms: i64,
    /// Retention bytes (-1 = unlimited).
    pub retention_bytes: i64,
    /// Cleanup policy: "delete" or "compact".
    pub cleanup_policy: String,
    /// Segment size in bytes.
    pub segment_bytes: u64,
    /// Min in-sync replicas.
    pub min_insync_replicas: u16,
    /// Max message size.
    pub max_message_bytes: u32,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            retention_ms: 604_800_000, // 7 days
            retention_bytes: -1,
            cleanup_policy: "delete".to_string(),
            segment_bytes: 1_073_741_824, // 1GB
            min_insync_replicas: 1,
            max_message_bytes: 1_048_576, // 1MB
        }
    }
}

/// Partition state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionState {
    /// Partition index.
    pub partition: i32,
    /// Leader ID.
    pub leader_id: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Log start offset.
    pub log_start_offset: i64,
    /// High watermark (committed offset).
    pub high_watermark: i64,
    /// Log end offset (next offset to assign).
    pub log_end_offset: i64,
    /// Replica IDs.
    pub replicas: Vec<i32>,
    /// In-sync replica IDs.
    pub isr: Vec<i32>,
}

impl PartitionState {
    /// Create a new partition state.
    pub fn new(partition: i32) -> Self {
        Self {
            partition,
            leader_id: 1, // Default to node 1
            leader_epoch: 0,
            log_start_offset: 0,
            high_watermark: 0,
            log_end_offset: 0,
            replicas: vec![1],
            isr: vec![1],
        }
    }

    /// Assign offset to a batch of records.
    pub fn assign_offsets(&mut self, record_count: u32) -> i64 {
        let base_offset = self.log_end_offset;
        self.log_end_offset += record_count as i64;
        self.high_watermark = self.log_end_offset; // Simplified: HW = LEO
        base_offset
    }
}

/// Kafka cluster configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// Default number of partitions for new topics.
    pub default_partitions: u32,
    /// Default replication factor.
    pub default_replication_factor: u16,
    /// Auto-create topics on produce/fetch.
    pub auto_create_topics: bool,
    /// Broker ID for this node.
    pub broker_id: i32,
    /// Cluster ID.
    pub cluster_id: String,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            default_partitions: 1,
            default_replication_factor: 1,
            auto_create_topics: true,
            broker_id: 1,
            cluster_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

/// Producer ID allocator.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ProducerIdAllocator {
    /// Next producer ID to allocate.
    next_id: i64,
    /// Producer ID to transactional ID mapping.
    transactional_ids: HashMap<String, i64>,
}

impl ProducerIdAllocator {
    /// Allocate a new producer ID.
    pub fn allocate(&mut self) -> (i64, i16) {
        let id = self.next_id;
        self.next_id += 1;
        (id, 0) // (producer_id, epoch)
    }

    /// Get or allocate producer ID for a transactional ID.
    pub fn get_or_allocate_for_txn(&mut self, transactional_id: &str) -> (i64, i16) {
        if let Some(&id) = self.transactional_ids.get(transactional_id) {
            (id, 0)
        } else {
            let (id, epoch) = self.allocate();
            self.transactional_ids
                .insert(transactional_id.to_string(), id);
            (id, epoch)
        }
    }

    /// Fence a producer (increment epoch).
    pub fn fence(&mut self, _producer_id: i64) -> i16 {
        1 // Return new epoch
    }
}

/// Idempotent producer state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerState {
    /// Producer ID.
    pub producer_id: i64,
    /// Current epoch.
    pub epoch: i16,
    /// Last sequence number per partition.
    pub sequences: HashMap<KafkaTopicPartition, i32>,
}

/// Topic-partition key for Kafka (separate from messaging::TopicPartition for serde).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KafkaTopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl ProducerState {
    /// Create new producer state.
    pub fn new(producer_id: i64, epoch: i16) -> Self {
        Self {
            producer_id,
            epoch,
            sequences: HashMap::new(),
        }
    }

    /// Validate and update sequence number.
    pub fn check_sequence(
        &mut self,
        topic: &str,
        partition: i32,
        first_sequence: i32,
    ) -> Result<(), ErrorCode> {
        let key = KafkaTopicPartition {
            topic: topic.to_string(),
            partition,
        };

        if let Some(&last_seq) = self.sequences.get(&key) {
            let expected = last_seq.wrapping_add(1);
            if first_sequence == expected {
                // OK
            } else if first_sequence == last_seq {
                return Err(ErrorCode::DuplicateSequenceNumber);
            } else {
                return Err(ErrorCode::OutOfOrderSequenceNumber);
            }
        } else if first_sequence != 0 {
            return Err(ErrorCode::OutOfOrderSequenceNumber);
        }

        self.sequences.insert(key, first_sequence);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// KafkaApply - Log Entry Types
// ---------------------------------------------------------------------------

/// Kafka log entry types for Raft replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KafkaApply {
    /// Create a topic.
    CreateTopic {
        name: String,
        num_partitions: u32,
        replication_factor: u16,
        config: Option<TopicConfig>,
    },
    /// Delete a topic.
    DeleteTopic { name: String },
    /// Produce a batch of records.
    ProduceBatch {
        topic: String,
        partition: i32,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        record_count: u32,
        records: Vec<u8>,
    },
    /// Commit consumer group offset.
    CommitOffset {
        group_id: String,
        topic: String,
        partition: i32,
        offset: i64,
        metadata: Option<String>,
    },
    /// Update consumer group state.
    UpdateGroupState {
        group_id: String,
        generation_id: i32,
        state: GroupStateSnapshot,
        leader_id: Option<String>,
        protocol_type: Option<String>,
        protocol_name: Option<String>,
    },
    /// Add member to consumer group.
    JoinGroup {
        group_id: String,
        member_id: String,
        client_id: String,
        client_host: String,
        session_timeout_ms: u32,
        rebalance_timeout_ms: u32,
        protocol_type: String,
        protocols: Vec<(String, Vec<u8>)>,
    },
    /// Remove member from consumer group.
    LeaveGroup { group_id: String, member_id: String },
    /// Sync group assignment.
    SyncGroup {
        group_id: String,
        generation_id: i32,
        member_id: String,
        assignment: Vec<u8>,
    },
    /// Initialize producer ID.
    InitProducerId {
        transactional_id: Option<String>,
        producer_id: i64,
        producer_epoch: i16,
    },
    /// Begin transaction.
    BeginTransaction {
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
    },
    /// Add partitions to transaction.
    AddPartitionsToTxn {
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
        partitions: Vec<KafkaTopicPartition>,
    },
    /// End transaction (commit or abort).
    EndTransaction {
        transactional_id: String,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    },
}

/// Group state for serialization.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum GroupStateSnapshot {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

impl From<GroupState> for GroupStateSnapshot {
    fn from(state: GroupState) -> Self {
        match state {
            GroupState::Empty => Self::Empty,
            GroupState::PreparingRebalance => Self::PreparingRebalance,
            GroupState::CompletingRebalance => Self::CompletingRebalance,
            GroupState::Stable => Self::Stable,
            GroupState::Dead => Self::Dead,
        }
    }
}

impl From<GroupStateSnapshot> for GroupState {
    fn from(state: GroupStateSnapshot) -> Self {
        match state {
            GroupStateSnapshot::Empty => Self::Empty,
            GroupStateSnapshot::PreparingRebalance => Self::PreparingRebalance,
            GroupStateSnapshot::CompletingRebalance => Self::CompletingRebalance,
            GroupStateSnapshot::Stable => Self::Stable,
            GroupStateSnapshot::Dead => Self::Dead,
        }
    }
}

// ---------------------------------------------------------------------------
// KafkaSnapshot
// ---------------------------------------------------------------------------

/// Kafka workload snapshot for persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSnapshot {
    /// Topic metadata.
    pub topics: HashMap<String, TopicMetadata>,
    /// Consumer group state.
    pub groups: Vec<GroupSnapshot>,
    /// Committed offsets.
    pub offsets: Vec<OffsetSnapshot>,
    /// Producer ID allocator state.
    pub producer_allocator: ProducerIdAllocator,
    /// Idempotent producer state.
    pub producer_state: HashMap<i64, ProducerState>,
    /// Cluster configuration.
    pub config: KafkaConfig,
}

/// Consumer group snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub group_id: String,
    pub state: GroupStateSnapshot,
    pub generation_id: i32,
    pub leader_id: Option<String>,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub members: Vec<MemberSnapshot>,
}

/// Group member snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberSnapshot {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignment: Vec<u8>,
}

/// Offset snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetSnapshot {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
}

// ---------------------------------------------------------------------------
// PrgWorkload Implementation
// ---------------------------------------------------------------------------

impl PrgWorkload for KafkaWorkload {
    type Snapshot = KafkaSnapshot;
    type LogEntry = KafkaApply;

    fn descriptor(&self) -> WorkloadDescriptor {
        KAFKA_DESCRIPTOR
    }

    fn init(&mut self, init: WorkloadInit<'_>) -> WorkloadResult<()> {
        self.features = init.features.clone();

        // Initialize with default cluster ID if not set
        if self.state.config.cluster_id.is_empty() {
            self.state.config.cluster_id = uuid::Uuid::new_v4().to_string();
        }

        WorkloadResult::ok(())
    }

    fn hydrate(&mut self, snapshot: &Self::Snapshot) -> WorkloadResult<()> {
        // Restore topics
        self.state.topics = snapshot.topics.clone();

        // Restore producer state
        self.state.producer_ids = snapshot.producer_allocator.clone();
        self.state.idempotent_producers = snapshot.producer_state.clone();

        // Restore config
        self.state.config = snapshot.config.clone();

        // Restore consumer groups
        for group_snap in &snapshot.groups {
            let mut group = ConsumerGroup::new(group_snap.group_id.clone());
            group.state = group_snap.state.into();
            group.generation_id = group_snap.generation_id;
            group.leader_id = group_snap.leader_id.clone();
            group.protocol_type = group_snap.protocol_type.clone();
            group.protocol_name = group_snap.protocol_name.clone();

            for member_snap in &group_snap.members {
                let member = GroupMember::new(
                    member_snap.member_id.clone(),
                    member_snap.client_id.clone(),
                    member_snap.client_host.clone(),
                    30_000,
                    60_000,
                    group_snap.protocol_type.clone().unwrap_or_default(),
                    vec![],
                );
                group.members.insert(member_snap.member_id.clone(), member);
            }

            self.state.groups.get_or_create(&group_snap.group_id);
            if let Some(g) = self.state.groups.get_mut(&group_snap.group_id) {
                *g = group;
            }
        }

        // Restore offsets
        for offset_snap in &snapshot.offsets {
            let tp = TopicPartition::new(&offset_snap.topic, offset_snap.partition);
            self.state.groups.commit_offset(
                &offset_snap.group_id,
                tp,
                offset_snap.offset,
                offset_snap.metadata.clone(),
            );
        }

        WorkloadResult::ok(())
    }

    fn build_snapshot(&self) -> WorkloadResult<Self::Snapshot> {
        let mut groups = Vec::new();
        for group in self.state.groups.list_groups() {
            let members = group
                .members
                .values()
                .map(|m| MemberSnapshot {
                    member_id: m.member_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    assignment: m.assignment.clone(),
                })
                .collect();

            groups.push(GroupSnapshot {
                group_id: group.group_id.clone(),
                state: group.state.into(),
                generation_id: group.generation_id,
                leader_id: group.leader_id.clone(),
                protocol_type: group.protocol_type.clone(),
                protocol_name: group.protocol_name.clone(),
                members,
            });
        }

        // Note: OffsetStore doesn't have a public iterator for all groups
        // This is a simplified version - in production you'd iterate all group offset stores
        let offsets = Vec::new();

        WorkloadResult::ok(KafkaSnapshot {
            topics: self.state.topics.clone(),
            groups,
            offsets,
            producer_allocator: self.state.producer_ids.clone(),
            producer_state: self.state.idempotent_producers.clone(),
            config: self.state.config.clone(),
        })
    }

    fn apply(&mut self, entry: Self::LogEntry, _ctx: &mut ApplyContext) -> WorkloadResult<()> {
        match entry {
            KafkaApply::CreateTopic {
                name,
                num_partitions,
                replication_factor,
                config,
            } => {
                use std::collections::hash_map::Entry;
                if let Entry::Vacant(e) = self.state.topics.entry(name) {
                    let mut topic =
                        TopicMetadata::new(e.key().clone(), num_partitions, replication_factor);
                    if let Some(cfg) = config {
                        topic.config = cfg;
                    }
                    e.insert(topic);
                }
            }

            KafkaApply::DeleteTopic { name } => {
                self.state.topics.remove(&name);
            }

            KafkaApply::ProduceBatch {
                topic,
                partition,
                producer_id,
                producer_epoch,
                base_sequence,
                record_count,
                records: _,
            } => {
                // Validate producer state for idempotent/transactional producers
                if producer_id >= 0 {
                    let state = self
                        .state
                        .idempotent_producers
                        .entry(producer_id)
                        .or_insert_with(|| ProducerState::new(producer_id, producer_epoch));

                    if state.epoch != producer_epoch {
                        // Producer fenced
                        return WorkloadResult::ok(());
                    }

                    if let Err(_e) = state.check_sequence(&topic, partition, base_sequence) {
                        // Sequence error - duplicate or out of order
                        return WorkloadResult::ok(());
                    }
                }

                // Auto-create topic if needed
                if self.state.config.auto_create_topics && !self.state.topics.contains_key(&topic) {
                    let new_topic = TopicMetadata::new(
                        topic.clone(),
                        self.state.config.default_partitions,
                        self.state.config.default_replication_factor,
                    );
                    self.state.topics.insert(topic.clone(), new_topic);
                }

                // Assign offsets to the batch
                if let Some(topic_meta) = self.state.topics.get_mut(&topic) {
                    if let Some(partition_state) = topic_meta.partition_mut(partition) {
                        partition_state.assign_offsets(record_count);
                    }
                }
            }

            KafkaApply::CommitOffset {
                group_id,
                topic,
                partition,
                offset,
                metadata,
            } => {
                let tp = TopicPartition::new(&topic, partition);
                self.state
                    .groups
                    .commit_offset(&group_id, tp, offset, metadata);
            }

            KafkaApply::UpdateGroupState {
                group_id,
                generation_id,
                state,
                leader_id,
                protocol_type,
                protocol_name,
            } => {
                if let Some(group) = self.state.groups.get_mut(&group_id) {
                    group.state = state.into();
                    group.generation_id = generation_id;
                    group.leader_id = leader_id;
                    group.protocol_type = protocol_type;
                    group.protocol_name = protocol_name;
                }
            }

            KafkaApply::JoinGroup {
                group_id,
                member_id,
                client_id,
                client_host,
                session_timeout_ms,
                rebalance_timeout_ms,
                protocol_type,
                protocols,
            } => {
                let group = self.state.groups.get_or_create(&group_id);

                let member = GroupMember::new(
                    member_id.clone(),
                    client_id,
                    client_host,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    protocol_type,
                    protocols,
                );

                group.add_member(member);
            }

            KafkaApply::LeaveGroup {
                group_id,
                member_id,
            } => {
                if let Some(group) = self.state.groups.get_mut(&group_id) {
                    group.remove_member(&member_id);
                }
            }

            KafkaApply::SyncGroup {
                group_id,
                generation_id,
                member_id,
                assignment,
            } => {
                if let Some(group) = self.state.groups.get_mut(&group_id) {
                    if group.generation_id == generation_id {
                        if let Some(member) = group.members.get_mut(&member_id) {
                            member.assignment = assignment;
                        }
                    }
                }
            }

            KafkaApply::InitProducerId {
                transactional_id,
                producer_id,
                producer_epoch,
            } => {
                self.state
                    .idempotent_producers
                    .insert(producer_id, ProducerState::new(producer_id, producer_epoch));

                if let Some(txn_id) = transactional_id {
                    self.state
                        .producer_ids
                        .transactional_ids
                        .insert(txn_id, producer_id);
                }
            }

            KafkaApply::BeginTransaction {
                transactional_id,
                producer_id,
                producer_epoch,
            } => {
                // begin() takes 3 args: txn_id, producer_id, producer_epoch
                let _ =
                    self.state
                        .transactions
                        .begin(&transactional_id, producer_id, producer_epoch);
            }

            KafkaApply::AddPartitionsToTxn {
                transactional_id,
                producer_id: _,
                producer_epoch: _,
                partitions,
            } => {
                // Add partitions via the transaction's add_produce method
                if let Some(txn) = self.state.transactions.get_mut(&transactional_id) {
                    for tp in partitions {
                        let _ = txn.add_produce(tp.topic, tp.partition, 0);
                    }
                }
            }

            KafkaApply::EndTransaction {
                transactional_id,
                producer_id: _,
                producer_epoch: _,
                committed,
            } => {
                if committed {
                    // Commit: prepare_commit and complete_commit via coordinator
                    let _ = self.state.transactions.commit(&transactional_id);
                } else {
                    let _ = self.state.transactions.abort(&transactional_id);
                }
            }
        }

        WorkloadResult::ok(())
    }

    fn on_ready(&mut self, _ready: &mut ReadyContext) -> WorkloadResult<()> {
        WorkloadResult::ok(())
    }
}

// ---------------------------------------------------------------------------
// Utility Functions
// ---------------------------------------------------------------------------

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
        let prg_id = crate::replication::PrgId {
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
    fn test_topic_metadata_creation() {
        let topic = TopicMetadata::new("test-topic".to_string(), 4, 1);

        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.num_partitions, 4);
        assert_eq!(topic.replication_factor, 1);
        assert_eq!(topic.partitions.len(), 4);
    }

    #[test]
    fn test_partition_offset_assignment() {
        let mut partition = PartitionState::new(0);

        let base = partition.assign_offsets(10);
        assert_eq!(base, 0);
        assert_eq!(partition.log_end_offset, 10);
        assert_eq!(partition.high_watermark, 10);

        let base2 = partition.assign_offsets(5);
        assert_eq!(base2, 10);
        assert_eq!(partition.log_end_offset, 15);
    }

    #[test]
    fn test_producer_id_allocation() {
        let mut allocator = ProducerIdAllocator::default();

        let (id1, epoch1) = allocator.allocate();
        let (id2, epoch2) = allocator.allocate();

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(epoch1, 0);
        assert_eq!(epoch2, 0);
    }

    #[test]
    fn test_producer_state_sequence() {
        let mut state = ProducerState::new(1, 0);

        // First sequence should be 0
        assert!(state.check_sequence("test", 0, 0).is_ok());

        // Next should be 1
        assert!(state.check_sequence("test", 0, 1).is_ok());

        // Duplicate should error
        assert!(matches!(
            state.check_sequence("test", 0, 1),
            Err(ErrorCode::DuplicateSequenceNumber)
        ));

        // Out of order should error
        assert!(matches!(
            state.check_sequence("test", 0, 5),
            Err(ErrorCode::OutOfOrderSequenceNumber)
        ));
    }

    #[test]
    fn test_kafka_workload_create_topic() {
        let mut workload = KafkaWorkload::default();

        let entry = KafkaApply::CreateTopic {
            name: "test".to_string(),
            num_partitions: 3,
            replication_factor: 1,
            config: None,
        };

        let mut ctx = make_apply_context();
        workload.apply(entry, &mut ctx).into_result().unwrap();

        assert!(workload.state.topics.contains_key("test"));
        assert_eq!(workload.state.topics["test"].num_partitions, 3);
    }

    #[test]
    fn test_kafka_workload_produce() {
        let mut workload = KafkaWorkload::default();
        workload.state.config.auto_create_topics = true;

        let entry = KafkaApply::ProduceBatch {
            topic: "test".to_string(),
            partition: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            record_count: 5,
            records: vec![],
        };

        let mut ctx = make_apply_context();
        workload.apply(entry, &mut ctx).into_result().unwrap();

        // Topic should be auto-created
        assert!(workload.state.topics.contains_key("test"));
        assert_eq!(
            workload.state.topics["test"].partitions[0].log_end_offset,
            5
        );
    }

    #[test]
    fn test_kafka_workload_snapshot() {
        let mut workload = KafkaWorkload::default();

        // Create a topic
        let entry = KafkaApply::CreateTopic {
            name: "test".to_string(),
            num_partitions: 2,
            replication_factor: 1,
            config: None,
        };

        let mut ctx = make_apply_context();
        workload.apply(entry, &mut ctx).into_result().unwrap();

        // Build snapshot
        let snapshot = workload.build_snapshot().into_result().unwrap();
        assert!(snapshot.topics.contains_key("test"));

        // Create new workload and hydrate
        let mut new_workload = KafkaWorkload::default();
        new_workload.hydrate(&snapshot).into_result().unwrap();

        assert!(new_workload.state.topics.contains_key("test"));
        assert_eq!(new_workload.state.topics["test"].num_partitions, 2);
    }
}
