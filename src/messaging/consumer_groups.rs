//! Consumer group coordination for shared consumption patterns.
//!
//! This module provides protocol-agnostic consumer group management:
//! - Member tracking and heartbeats
//! - Partition/subscription assignment
//! - Offset management
//! - Rebalance coordination
//!
//! Tasks 28-30: Consumer groups, offset tracking, tests

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Consumer Group State (Task 28)
// ---------------------------------------------------------------------------

/// Consumer group state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    /// No members, group is empty.
    Empty,
    /// Members are joining, waiting for all to join.
    PreparingRebalance,
    /// Leader is computing assignments.
    CompletingRebalance,
    /// Group is stable with active members.
    Stable,
    /// Group is dead (expired or administratively deleted).
    Dead,
}

impl GroupState {
    pub fn as_str(&self) -> &'static str {
        match self {
            GroupState::Empty => "empty",
            GroupState::PreparingRebalance => "preparing_rebalance",
            GroupState::CompletingRebalance => "completing_rebalance",
            GroupState::Stable => "stable",
            GroupState::Dead => "dead",
        }
    }
}

/// Assignment strategy for distributing partitions to members.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentStrategy {
    /// Round-robin assignment across all partitions.
    RoundRobin,
    /// Range-based assignment (contiguous partitions per member).
    Range,
    /// Sticky assignment (minimize reassignments).
    Sticky,
}

impl Default for AssignmentStrategy {
    fn default() -> Self {
        Self::RoundRobin
    }
}

/// Member metadata in a consumer group.
#[derive(Debug, Clone)]
pub struct GroupMember {
    /// Unique member identifier.
    pub member_id: String,
    /// Client identifier (may differ from member_id).
    pub client_id: String,
    /// Host/IP address of the member.
    pub client_host: String,
    /// Session timeout in milliseconds.
    pub session_timeout_ms: u32,
    /// Rebalance timeout in milliseconds.
    pub rebalance_timeout_ms: u32,
    /// Protocol type (e.g., "consumer", "connect").
    pub protocol_type: String,
    /// Supported assignment protocols.
    pub protocols: Vec<(String, Vec<u8>)>,
    /// Current partition assignment.
    pub assignment: Vec<u8>,
    /// Last heartbeat time.
    pub last_heartbeat: Instant,
    /// Whether this member is the group leader.
    pub is_leader: bool,
}

impl GroupMember {
    pub fn new(
        member_id: String,
        client_id: String,
        client_host: String,
        session_timeout_ms: u32,
        rebalance_timeout_ms: u32,
        protocol_type: String,
        protocols: Vec<(String, Vec<u8>)>,
    ) -> Self {
        Self {
            member_id,
            client_id,
            client_host,
            session_timeout_ms,
            rebalance_timeout_ms,
            protocol_type,
            protocols,
            assignment: Vec::new(),
            last_heartbeat: Instant::now(),
            is_leader: false,
        }
    }

    /// Check if the member's session has timed out.
    pub fn is_expired(&self) -> bool {
        self.last_heartbeat.elapsed() > Duration::from_millis(self.session_timeout_ms as u64)
    }

    /// Update heartbeat timestamp.
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }
}

/// Consumer group state.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Group identifier.
    pub group_id: String,
    /// Current state.
    pub state: GroupState,
    /// Generation ID (incremented on each rebalance).
    pub generation_id: i32,
    /// Current leader member ID.
    pub leader_id: Option<String>,
    /// Selected assignment protocol.
    pub protocol_name: Option<String>,
    /// Protocol type for this group.
    pub protocol_type: Option<String>,
    /// Members indexed by member ID.
    pub members: HashMap<String, GroupMember>,
    /// Pending members waiting to join.
    pending_members: HashSet<String>,
    /// Assignment strategy.
    pub strategy: AssignmentStrategy,
    /// Session timeout for the group.
    pub session_timeout_ms: u32,
    /// Time of last state change.
    pub state_changed_at: Instant,
}

impl ConsumerGroup {
    pub fn new(group_id: String) -> Self {
        Self {
            group_id,
            state: GroupState::Empty,
            generation_id: 0,
            leader_id: None,
            protocol_name: None,
            protocol_type: None,
            members: HashMap::new(),
            pending_members: HashSet::new(),
            strategy: AssignmentStrategy::default(),
            session_timeout_ms: 30_000,
            state_changed_at: Instant::now(),
        }
    }

    /// Add a member to the group, triggering rebalance if needed.
    pub fn add_member(&mut self, member: GroupMember) -> GroupMemberResult {
        let member_id = member.member_id.clone();

        // Set protocol type on first member
        if self.protocol_type.is_none() {
            self.protocol_type = Some(member.protocol_type.clone());
        }

        // Check protocol type consistency
        if self.protocol_type.as_ref() != Some(&member.protocol_type) {
            return GroupMemberResult::InconsistentProtocol;
        }

        let was_leader = self.leader_id.is_none();
        self.members.insert(member_id.clone(), member);

        // First member becomes leader
        if was_leader {
            self.leader_id = Some(member_id.clone());
            if let Some(m) = self.members.get_mut(&member_id) {
                m.is_leader = true;
            }
        }

        // Trigger rebalance
        self.transition_to(GroupState::PreparingRebalance);

        GroupMemberResult::Joined {
            member_id,
            generation_id: self.generation_id,
            leader_id: self.leader_id.clone().unwrap_or_default(),
            is_leader: was_leader,
        }
    }

    /// Remove a member from the group.
    pub fn remove_member(&mut self, member_id: &str) -> bool {
        if self.members.remove(member_id).is_none() {
            return false;
        }

        self.pending_members.remove(member_id);

        // If leader left, elect new one
        if self.leader_id.as_deref() == Some(member_id) {
            self.leader_id = self.members.keys().next().cloned();
            if let Some(ref leader_id) = self.leader_id {
                if let Some(m) = self.members.get_mut(leader_id) {
                    m.is_leader = true;
                }
            }
        }

        if self.members.is_empty() {
            self.transition_to(GroupState::Empty);
        } else {
            self.transition_to(GroupState::PreparingRebalance);
        }

        true
    }

    /// Process heartbeat from a member.
    pub fn heartbeat(&mut self, member_id: &str, generation_id: i32) -> HeartbeatResult {
        if self.generation_id != generation_id {
            return HeartbeatResult::IllegalGeneration;
        }

        if let Some(member) = self.members.get_mut(member_id) {
            member.heartbeat();

            if self.state == GroupState::PreparingRebalance
                || self.state == GroupState::CompletingRebalance
            {
                return HeartbeatResult::Rebalancing;
            }

            HeartbeatResult::Ok
        } else {
            HeartbeatResult::UnknownMember
        }
    }

    /// Complete the join phase and move to completing rebalance.
    pub fn complete_join(&mut self) {
        if self.state != GroupState::PreparingRebalance {
            return;
        }

        self.generation_id += 1;
        self.pending_members.clear();
        self.select_protocol();
        self.transition_to(GroupState::CompletingRebalance);
    }

    /// Store assignments from the leader and move to stable.
    pub fn sync_group(&mut self, member_id: &str, assignments: Vec<(String, Vec<u8>)>) -> bool {
        // Only leader can sync
        if self.leader_id.as_deref() != Some(member_id) {
            return false;
        }

        if self.state != GroupState::CompletingRebalance {
            return false;
        }

        // Apply assignments
        for (member_id, assignment) in assignments {
            if let Some(member) = self.members.get_mut(&member_id) {
                member.assignment = assignment;
            }
        }

        self.transition_to(GroupState::Stable);
        true
    }

    /// Get assignment for a member.
    pub fn get_assignment(&self, member_id: &str) -> Option<&[u8]> {
        self.members.get(member_id).map(|m| m.assignment.as_slice())
    }

    /// Check for expired members and remove them.
    pub fn check_expirations(&mut self) -> Vec<String> {
        let expired: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| m.is_expired())
            .map(|(id, _)| id.clone())
            .collect();

        for member_id in &expired {
            self.remove_member(member_id);
        }

        expired
    }

    /// Select the protocol to use based on member votes.
    fn select_protocol(&mut self) {
        if self.members.is_empty() {
            self.protocol_name = None;
            return;
        }

        // Find protocols supported by all members
        let mut protocol_votes: HashMap<&str, usize> = HashMap::new();

        for member in self.members.values() {
            for (protocol, _) in &member.protocols {
                *protocol_votes.entry(protocol.as_str()).or_insert(0) += 1;
            }
        }

        // Select the protocol supported by all members with highest votes
        let member_count = self.members.len();
        self.protocol_name = protocol_votes
            .iter()
            .filter(|(_, &count)| count == member_count)
            .max_by_key(|(_, &count)| count)
            .map(|(name, _)| name.to_string());
    }

    fn transition_to(&mut self, state: GroupState) {
        if self.state != state {
            self.state = state;
            self.state_changed_at = Instant::now();
        }
    }

    /// Get member count.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Check if group is empty.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Get all member IDs.
    pub fn member_ids(&self) -> Vec<&str> {
        self.members.keys().map(|s| s.as_str()).collect()
    }
}

/// Result of adding a member to a group.
#[derive(Debug, Clone)]
pub enum GroupMemberResult {
    /// Successfully joined.
    Joined {
        member_id: String,
        generation_id: i32,
        leader_id: String,
        is_leader: bool,
    },
    /// Protocol type doesn't match group.
    InconsistentProtocol,
}

/// Result of a heartbeat request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatResult {
    /// Heartbeat accepted.
    Ok,
    /// Generation ID doesn't match.
    IllegalGeneration,
    /// Member not found in group.
    UnknownMember,
    /// Group is rebalancing.
    Rebalancing,
}

// ---------------------------------------------------------------------------
// Offset Tracking (Task 29)
// ---------------------------------------------------------------------------

/// Committed offset with metadata.
#[derive(Debug, Clone)]
pub struct CommittedOffset {
    /// The committed offset.
    pub offset: i64,
    /// Optional metadata (max 4KB in Kafka).
    pub metadata: Option<String>,
    /// Commit timestamp.
    pub commit_timestamp: u64,
    /// Expiration timestamp (0 = never expires).
    pub expire_timestamp: u64,
    /// Leader epoch at commit time.
    pub leader_epoch: Option<i32>,
}

impl CommittedOffset {
    pub fn new(offset: i64) -> Self {
        Self {
            offset,
            metadata: None,
            commit_timestamp: current_timestamp_ms(),
            expire_timestamp: 0,
            leader_epoch: None,
        }
    }

    pub fn with_metadata(mut self, metadata: String) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_expiry(mut self, expire_ms: u64) -> Self {
        self.expire_timestamp = self.commit_timestamp + expire_ms;
        self
    }

    pub fn is_expired(&self) -> bool {
        self.expire_timestamp > 0 && current_timestamp_ms() > self.expire_timestamp
    }
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Partition identifier (topic + partition index).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

/// Offset store for a consumer group.
#[derive(Debug, Clone, Default)]
pub struct OffsetStore {
    /// Committed offsets: (topic, partition) -> offset.
    offsets: HashMap<TopicPartition, CommittedOffset>,
    /// Default offset retention in milliseconds (0 = forever).
    retention_ms: u64,
}

impl OffsetStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_retention(mut self, retention_ms: u64) -> Self {
        self.retention_ms = retention_ms;
        self
    }

    /// Commit an offset for a partition.
    pub fn commit(&mut self, tp: TopicPartition, offset: i64, metadata: Option<String>) {
        let mut committed = CommittedOffset::new(offset);
        if let Some(m) = metadata {
            committed = committed.with_metadata(m);
        }
        if self.retention_ms > 0 {
            committed = committed.with_expiry(self.retention_ms);
        }
        self.offsets.insert(tp, committed);
    }

    /// Fetch committed offset for a partition.
    pub fn fetch(&self, tp: &TopicPartition) -> Option<&CommittedOffset> {
        self.offsets.get(tp).filter(|o| !o.is_expired())
    }

    /// Remove expired offsets.
    pub fn expire(&mut self) -> usize {
        let before = self.offsets.len();
        self.offsets.retain(|_, v| !v.is_expired());
        before - self.offsets.len()
    }

    /// Get all committed offsets.
    pub fn all(&self) -> impl Iterator<Item = (&TopicPartition, &CommittedOffset)> {
        self.offsets.iter().filter(|(_, o)| !o.is_expired())
    }

    /// Get committed offset count.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Clear all offsets.
    pub fn clear(&mut self) {
        self.offsets.clear();
    }
}

// ---------------------------------------------------------------------------
// Consumer Group Coordinator
// ---------------------------------------------------------------------------

/// Coordinates consumer groups.
#[derive(Debug, Clone, Default)]
pub struct GroupCoordinator {
    /// Groups indexed by group ID.
    groups: HashMap<String, ConsumerGroup>,
    /// Offset stores per group.
    offset_stores: HashMap<String, OffsetStore>,
    /// Default offset retention.
    default_offset_retention_ms: u64,
}

impl GroupCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_offset_retention(mut self, retention_ms: u64) -> Self {
        self.default_offset_retention_ms = retention_ms;
        self
    }

    /// Get or create a consumer group.
    pub fn get_or_create(&mut self, group_id: &str) -> &mut ConsumerGroup {
        self.groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroup::new(group_id.to_string()))
    }

    /// Get a consumer group.
    pub fn get(&self, group_id: &str) -> Option<&ConsumerGroup> {
        self.groups.get(group_id)
    }

    /// Get a mutable consumer group.
    pub fn get_mut(&mut self, group_id: &str) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(group_id)
    }

    /// Remove a consumer group.
    pub fn remove(&mut self, group_id: &str) -> Option<ConsumerGroup> {
        self.offset_stores.remove(group_id);
        self.groups.remove(group_id)
    }

    /// Get or create offset store for a group.
    pub fn offset_store(&mut self, group_id: &str) -> &mut OffsetStore {
        self.offset_stores
            .entry(group_id.to_string())
            .or_insert_with(|| OffsetStore::new().with_retention(self.default_offset_retention_ms))
    }

    /// Commit offset for a group.
    pub fn commit_offset(
        &mut self,
        group_id: &str,
        tp: TopicPartition,
        offset: i64,
        metadata: Option<String>,
    ) {
        self.offset_store(group_id).commit(tp, offset, metadata);
    }

    /// Fetch offset for a group.
    pub fn fetch_offset(&self, group_id: &str, tp: &TopicPartition) -> Option<i64> {
        self.offset_stores
            .get(group_id)
            .and_then(|store| store.fetch(tp))
            .map(|o| o.offset)
    }

    /// List all groups.
    pub fn list_groups(&self) -> impl Iterator<Item = &ConsumerGroup> {
        self.groups.values()
    }

    /// Count groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Expire offsets across all groups.
    pub fn expire_offsets(&mut self) -> usize {
        let mut total = 0;
        for store in self.offset_stores.values_mut() {
            total += store.expire();
        }
        total
    }

    /// Check for expired members across all groups.
    pub fn check_member_expirations(&mut self) -> Vec<(String, Vec<String>)> {
        let mut results = Vec::new();
        for (group_id, group) in &mut self.groups {
            let expired = group.check_expirations();
            if !expired.is_empty() {
                results.push((group_id.clone(), expired));
            }
        }
        results
    }
}

// ---------------------------------------------------------------------------
// Snapshot Support
// ---------------------------------------------------------------------------

/// Snapshot of consumer group state for persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConsumerGroupSnapshot {
    pub group_id: String,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader_id: Option<String>,
    pub members: Vec<GroupMemberSnapshot>,
}

/// Snapshot of a group member.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GroupMemberSnapshot {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: u32,
    pub rebalance_timeout_ms: u32,
    pub protocol_type: String,
    pub assignment: Vec<u8>,
}

/// Snapshot of offset store.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OffsetStoreSnapshot {
    pub offsets: Vec<OffsetEntrySnapshot>,
}

/// Snapshot of a single offset entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OffsetEntrySnapshot {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
    pub commit_timestamp: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_member(id: &str) -> GroupMember {
        GroupMember::new(
            id.to_string(),
            format!("client-{id}"),
            "localhost".to_string(),
            30_000,
            60_000,
            "consumer".to_string(),
            vec![("range".to_string(), vec![])],
        )
    }

    #[test]
    fn test_group_add_member() {
        let mut group = ConsumerGroup::new("test-group".to_string());
        assert_eq!(group.state, GroupState::Empty);

        let result = group.add_member(make_member("member1"));
        assert!(matches!(
            result,
            GroupMemberResult::Joined {
                is_leader: true,
                ..
            }
        ));
        assert_eq!(group.state, GroupState::PreparingRebalance);
        assert_eq!(group.member_count(), 1);
    }

    #[test]
    fn test_group_multiple_members() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        group.add_member(make_member("member1"));
        let result = group.add_member(make_member("member2"));

        assert!(matches!(
            result,
            GroupMemberResult::Joined {
                is_leader: false,
                ..
            }
        ));
        assert_eq!(group.member_count(), 2);
        assert_eq!(group.leader_id, Some("member1".to_string()));
    }

    #[test]
    fn test_group_remove_leader() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        group.add_member(make_member("member1"));
        group.add_member(make_member("member2"));

        group.remove_member("member1");

        assert_eq!(group.member_count(), 1);
        assert_eq!(group.leader_id, Some("member2".to_string()));
    }

    #[test]
    fn test_group_heartbeat() {
        let mut group = ConsumerGroup::new("test-group".to_string());
        group.add_member(make_member("member1"));
        group.complete_join();
        group.sync_group("member1", vec![]);

        let result = group.heartbeat("member1", group.generation_id);
        assert_eq!(result, HeartbeatResult::Ok);

        let result = group.heartbeat("member1", group.generation_id - 1);
        assert_eq!(result, HeartbeatResult::IllegalGeneration);

        let result = group.heartbeat("unknown", group.generation_id);
        assert_eq!(result, HeartbeatResult::UnknownMember);
    }

    #[test]
    fn test_group_lifecycle() {
        let mut group = ConsumerGroup::new("test-group".to_string());

        // Join
        group.add_member(make_member("member1"));
        assert_eq!(group.state, GroupState::PreparingRebalance);

        // Complete join
        group.complete_join();
        assert_eq!(group.state, GroupState::CompletingRebalance);
        assert_eq!(group.generation_id, 1);

        // Sync
        group.sync_group("member1", vec![("member1".to_string(), vec![1, 2, 3])]);
        assert_eq!(group.state, GroupState::Stable);
        assert_eq!(group.get_assignment("member1"), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn test_offset_store() {
        let mut store = OffsetStore::new();

        let tp = TopicPartition::new("test-topic", 0);
        store.commit(tp.clone(), 100, Some("test".to_string()));

        let offset = store.fetch(&tp).unwrap();
        assert_eq!(offset.offset, 100);
        assert_eq!(offset.metadata, Some("test".to_string()));
    }

    #[test]
    fn test_coordinator() {
        let mut coord = GroupCoordinator::new();

        let group = coord.get_or_create("group1");
        group.add_member(make_member("member1"));

        let tp = TopicPartition::new("topic1", 0);
        coord.commit_offset("group1", tp.clone(), 50, None);

        assert_eq!(coord.fetch_offset("group1", &tp), Some(50));
    }
}
