//! Kafka protocol workload implementation.
//!
//! This module provides Kafka protocol compatibility for Quantum, enabling
//! the broker to act as a Kafka-compatible message broker supporting:
//!
//! - Producer API (Produce, Metadata)
//! - Consumer API (Fetch, ListOffsets, OffsetCommit, OffsetFetch)
//! - Consumer Groups (JoinGroup, SyncGroup, Heartbeat, LeaveGroup)
//! - Transactions (InitProducerId, AddPartitionsToTxn, EndTxn)
//! - Admin API (CreateTopics, DeleteTopics, DescribeConfigs)
//!
//! # Architecture
//!
//! The Kafka workload reuses core messaging infrastructure:
//! - `messaging::topics` for partition routing
//! - `messaging::consumer_groups` for group coordination
//! - `messaging::transactions` for exactly-once semantics
//! - `messaging::acks` for producer acknowledgments
//! - `messaging::prefetch` for consumer flow control
//!
//! # Feature Gate
//!
//! This module is gated behind the `kafka` feature flag:
//! ```toml
//! [features]
//! kafka = []
//! ```

pub mod config;
pub mod errors;
pub mod plugin;
pub mod protocol;
pub mod workload;

// Re-export key types
pub use config::*;
pub use errors::*;
pub use plugin::*;
pub use protocol::*;
pub use workload::*;

// ---------------------------------------------------------------------------
// Kafka API Keys
// ---------------------------------------------------------------------------

/// Kafka API key identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    OffsetDelete = 47,
    DescribeClientQuotas = 48,
    AlterClientQuotas = 49,
    DescribeUserScramCredentials = 50,
    AlterUserScramCredentials = 51,
    DescribeQuorum = 55,
    AlterPartition = 56,
    UpdateFeatures = 57,
    Envelope = 58,
    DescribeCluster = 60,
    DescribeProducers = 61,
    UnregisterBroker = 64,
    DescribeTransactions = 65,
    ListTransactions = 66,
    AllocateProducerIds = 67,
}

impl ApiKey {
    /// Parse API key from i16.
    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            0 => Some(Self::Produce),
            1 => Some(Self::Fetch),
            2 => Some(Self::ListOffsets),
            3 => Some(Self::Metadata),
            4 => Some(Self::LeaderAndIsr),
            5 => Some(Self::StopReplica),
            6 => Some(Self::UpdateMetadata),
            7 => Some(Self::ControlledShutdown),
            8 => Some(Self::OffsetCommit),
            9 => Some(Self::OffsetFetch),
            10 => Some(Self::FindCoordinator),
            11 => Some(Self::JoinGroup),
            12 => Some(Self::Heartbeat),
            13 => Some(Self::LeaveGroup),
            14 => Some(Self::SyncGroup),
            15 => Some(Self::DescribeGroups),
            16 => Some(Self::ListGroups),
            17 => Some(Self::SaslHandshake),
            18 => Some(Self::ApiVersions),
            19 => Some(Self::CreateTopics),
            20 => Some(Self::DeleteTopics),
            21 => Some(Self::DeleteRecords),
            22 => Some(Self::InitProducerId),
            23 => Some(Self::OffsetForLeaderEpoch),
            24 => Some(Self::AddPartitionsToTxn),
            25 => Some(Self::AddOffsetsToTxn),
            26 => Some(Self::EndTxn),
            27 => Some(Self::WriteTxnMarkers),
            28 => Some(Self::TxnOffsetCommit),
            29 => Some(Self::DescribeAcls),
            30 => Some(Self::CreateAcls),
            31 => Some(Self::DeleteAcls),
            32 => Some(Self::DescribeConfigs),
            33 => Some(Self::AlterConfigs),
            34 => Some(Self::AlterReplicaLogDirs),
            35 => Some(Self::DescribeLogDirs),
            36 => Some(Self::SaslAuthenticate),
            37 => Some(Self::CreatePartitions),
            38 => Some(Self::CreateDelegationToken),
            39 => Some(Self::RenewDelegationToken),
            40 => Some(Self::ExpireDelegationToken),
            41 => Some(Self::DescribeDelegationToken),
            42 => Some(Self::DeleteGroups),
            43 => Some(Self::ElectLeaders),
            44 => Some(Self::IncrementalAlterConfigs),
            45 => Some(Self::AlterPartitionReassignments),
            46 => Some(Self::ListPartitionReassignments),
            47 => Some(Self::OffsetDelete),
            48 => Some(Self::DescribeClientQuotas),
            49 => Some(Self::AlterClientQuotas),
            50 => Some(Self::DescribeUserScramCredentials),
            51 => Some(Self::AlterUserScramCredentials),
            55 => Some(Self::DescribeQuorum),
            56 => Some(Self::AlterPartition),
            57 => Some(Self::UpdateFeatures),
            58 => Some(Self::Envelope),
            60 => Some(Self::DescribeCluster),
            61 => Some(Self::DescribeProducers),
            64 => Some(Self::UnregisterBroker),
            65 => Some(Self::DescribeTransactions),
            66 => Some(Self::ListTransactions),
            67 => Some(Self::AllocateProducerIds),
            _ => None,
        }
    }

    /// Get the API key as i16.
    pub fn as_i16(self) -> i16 {
        self as i16
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default number of partitions for auto-created topics.
pub const DEFAULT_PARTITIONS: u32 = 1;

/// Default replication factor for auto-created topics.
pub const DEFAULT_REPLICATION_FACTOR: u16 = 1;

/// Maximum record batch size in bytes (default 1MB).
pub const MAX_RECORD_BATCH_SIZE: usize = 1_048_576;

/// Maximum message size in bytes (default 1MB).
pub const MAX_MESSAGE_SIZE: usize = 1_048_576;

/// Consumer group session timeout default (10 seconds).
pub const DEFAULT_SESSION_TIMEOUT_MS: i32 = 10_000;

/// Consumer group rebalance timeout default (60 seconds).
pub const DEFAULT_REBALANCE_TIMEOUT_MS: i32 = 60_000;

/// Heartbeat interval default (3 seconds).
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: i32 = 3_000;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_key_round_trip() {
        for key in [
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::Metadata,
            ApiKey::ApiVersions,
            ApiKey::JoinGroup,
            ApiKey::SyncGroup,
        ] {
            let value = key.as_i16();
            let parsed = ApiKey::from_i16(value);
            assert_eq!(parsed, Some(key));
        }
    }

    #[test]
    fn test_api_key_unknown() {
        assert_eq!(ApiKey::from_i16(-1), None);
        assert_eq!(ApiKey::from_i16(9999), None);
    }
}
