//! Kafka API request types.
//!
//! This module defines request structures for all supported Kafka APIs.

use super::types::*;
use bytes::Bytes;
use std::io::{self, Cursor, Read};

// ---------------------------------------------------------------------------
// ApiVersions Request (API 18)
// ---------------------------------------------------------------------------

/// ApiVersions request.
#[derive(Debug, Clone, Default)]
pub struct ApiVersionsRequest {
    /// Client software name (v3+).
    pub client_software_name: Option<String>,
    /// Client software version (v3+).
    pub client_software_version: Option<String>,
}

impl ApiVersionsRequest {
    /// Parse from bytes (version 0-2).
    pub fn parse_v0(_data: &[u8]) -> io::Result<Self> {
        Ok(Self::default())
    }

    /// Parse from bytes (version 3+, flexible).
    pub fn parse_v3(data: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let client_software_name = read_compact_string(&mut cursor)?;
        let client_software_version = read_compact_string(&mut cursor)?;
        let _tagged_fields = read_tagged_fields(&mut cursor)?;

        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}

// ---------------------------------------------------------------------------
// Metadata Request (API 3)
// ---------------------------------------------------------------------------

/// Metadata request.
#[derive(Debug, Clone, Default)]
pub struct MetadataRequest {
    /// Topics to fetch metadata for (None = all topics).
    pub topics: Option<Vec<MetadataRequestTopic>>,
    /// If true, include cluster-authorized operations.
    pub allow_auto_topic_creation: bool,
    /// Include topic-authorized operations (v8+).
    pub include_topic_authorized_operations: bool,
    /// Include cluster-authorized operations (v8+).
    pub include_cluster_authorized_operations: bool,
}

/// Topic in metadata request.
#[derive(Debug, Clone)]
pub struct MetadataRequestTopic {
    /// Topic ID (v10+).
    pub topic_id: Option<uuid::Uuid>,
    /// Topic name.
    pub name: Option<String>,
}

impl MetadataRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 9;

        let topics = if is_flexible {
            read_compact_array(&mut cursor, |r| {
                let topic_id = if version >= 10 {
                    Some(read_uuid(r)?)
                } else {
                    None
                };
                let name = read_compact_string(r)?;
                let _tagged_fields = read_tagged_fields(r)?;
                Ok(MetadataRequestTopic { topic_id, name })
            })?
        } else {
            read_array(&mut cursor, |r| {
                let name = read_string(r)?;
                Ok(MetadataRequestTopic {
                    topic_id: None,
                    name,
                })
            })?
        };

        let allow_auto_topic_creation = if version >= 4 {
            read_boolean(&mut cursor)?
        } else {
            true
        };

        let include_cluster_authorized_operations = if version >= 8 {
            read_boolean(&mut cursor)?
        } else {
            false
        };

        let include_topic_authorized_operations = if version >= 8 {
            read_boolean(&mut cursor)?
        } else {
            false
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_topic_authorized_operations,
            include_cluster_authorized_operations,
        })
    }
}

// ---------------------------------------------------------------------------
// FindCoordinator Request (API 10)
// ---------------------------------------------------------------------------

/// Coordinator key type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorType {
    Group = 0,
    Transaction = 1,
}

impl CoordinatorType {
    pub fn from_i8(value: i8) -> Option<Self> {
        match value {
            0 => Some(Self::Group),
            1 => Some(Self::Transaction),
            _ => None,
        }
    }
}

/// FindCoordinator request.
#[derive(Debug, Clone)]
pub struct FindCoordinatorRequest {
    /// The key to find coordinator for.
    pub key: String,
    /// Key type (GROUP or TRANSACTION).
    pub key_type: CoordinatorType,
    /// Coordinator keys to search (v4+, batch).
    pub coordinator_keys: Vec<String>,
}

impl FindCoordinatorRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 3;

        let key = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let key_type = if version >= 1 {
            CoordinatorType::from_i8(read_int8(&mut cursor)?).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid coordinator type")
            })?
        } else {
            CoordinatorType::Group
        };

        let coordinator_keys = if version >= 4 {
            read_compact_array(&mut cursor, read_compact_non_nullable_string)?.unwrap_or_default()
        } else {
            vec![]
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            key,
            key_type,
            coordinator_keys,
        })
    }
}

// ---------------------------------------------------------------------------
// Produce Request (API 0)
// ---------------------------------------------------------------------------

/// Required acks for produce request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredAcks {
    /// No acknowledgment.
    None = 0,
    /// Leader acknowledgment only.
    Leader = 1,
    /// Full ISR acknowledgment.
    All = -1,
}

impl RequiredAcks {
    pub fn from_i16(value: i16) -> Self {
        match value {
            0 => Self::None,
            1 => Self::Leader,
            _ => Self::All,
        }
    }
}

/// Produce request.
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    /// Transactional ID (v3+).
    pub transactional_id: Option<String>,
    /// Required acknowledgments.
    pub acks: RequiredAcks,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Topics with data to produce.
    pub topics: Vec<ProduceRequestTopic>,
}

/// Topic data in produce request.
#[derive(Debug, Clone)]
pub struct ProduceRequestTopic {
    /// Topic name.
    pub name: String,
    /// Partitions with data.
    pub partitions: Vec<ProduceRequestPartition>,
}

/// Partition data in produce request.
#[derive(Debug, Clone)]
pub struct ProduceRequestPartition {
    /// Partition index.
    pub partition: i32,
    /// Record batch data.
    pub records: Option<Bytes>,
}

impl ProduceRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 9;

        let transactional_id = if version >= 3 {
            if is_flexible {
                read_compact_string(&mut cursor)?
            } else {
                read_string(&mut cursor)?
            }
        } else {
            None
        };

        let acks = RequiredAcks::from_i16(read_int16(&mut cursor)?);
        let timeout_ms = read_int32(&mut cursor)?;

        let topics = if is_flexible {
            read_compact_array(&mut cursor, |r| {
                let name = read_compact_non_nullable_string(r)?;
                let partitions = read_compact_array(r, |r2| {
                    let partition = read_int32(r2)?;
                    let records = read_compact_bytes(r2)?.map(Bytes::from);
                    let _tagged_fields = read_tagged_fields(r2)?;
                    Ok(ProduceRequestPartition { partition, records })
                })?
                .unwrap_or_default();
                let _tagged_fields = read_tagged_fields(r)?;
                Ok(ProduceRequestTopic { name, partitions })
            })?
            .unwrap_or_default()
        } else {
            read_array(&mut cursor, |r| {
                let name = read_non_nullable_string(r)?;
                let partitions = read_array(r, |r2| {
                    let partition = read_int32(r2)?;
                    let records = read_bytes(r2)?.map(Bytes::from);
                    Ok(ProduceRequestPartition { partition, records })
                })?
                .unwrap_or_default();
                Ok(ProduceRequestTopic { name, partitions })
            })?
            .unwrap_or_default()
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topics,
        })
    }
}

// ---------------------------------------------------------------------------
// Fetch Request (API 1)
// ---------------------------------------------------------------------------

/// Isolation level for fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    #[default]
    ReadUncommitted = 0,
    ReadCommitted = 1,
}

impl IsolationLevel {
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::ReadCommitted,
            _ => Self::ReadUncommitted,
        }
    }
}

/// Fetch request.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    /// Cluster ID (v12+).
    pub cluster_id: Option<String>,
    /// Replica ID (broker-to-broker fetch).
    pub replica_id: i32,
    /// Maximum wait time in milliseconds.
    pub max_wait_ms: i32,
    /// Minimum bytes to return.
    pub min_bytes: i32,
    /// Maximum bytes to return (v3+).
    pub max_bytes: i32,
    /// Isolation level (v4+).
    pub isolation_level: IsolationLevel,
    /// Fetch session ID (v7+).
    pub session_id: i32,
    /// Fetch session epoch (v7+).
    pub session_epoch: i32,
    /// Topics to fetch.
    pub topics: Vec<FetchRequestTopic>,
    /// Forgotten topics (v7+).
    pub forgotten_topics: Vec<FetchForgottenTopic>,
    /// Rack ID (v11+).
    pub rack_id: Option<String>,
}

/// Topic in fetch request.
#[derive(Debug, Clone)]
pub struct FetchRequestTopic {
    /// Topic ID (v13+).
    pub topic_id: Option<uuid::Uuid>,
    /// Topic name.
    pub topic: Option<String>,
    /// Partitions to fetch.
    pub partitions: Vec<FetchRequestPartition>,
}

/// Partition in fetch request.
#[derive(Debug, Clone)]
pub struct FetchRequestPartition {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch (v9+).
    pub current_leader_epoch: i32,
    /// Fetch offset.
    pub fetch_offset: i64,
    /// Last fetched epoch (v12+).
    pub last_fetched_epoch: i32,
    /// Log start offset (v5+).
    pub log_start_offset: i64,
    /// Maximum bytes to fetch.
    pub partition_max_bytes: i32,
}

/// Forgotten topic in fetch request.
#[derive(Debug, Clone)]
pub struct FetchForgottenTopic {
    /// Topic ID (v13+).
    pub topic_id: Option<uuid::Uuid>,
    /// Topic name.
    pub topic: Option<String>,
    /// Partitions to forget.
    pub partitions: Vec<i32>,
}

impl FetchRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 12;

        let cluster_id = if version >= 12 {
            read_compact_string(&mut cursor)?
        } else {
            None
        };

        let replica_id = read_int32(&mut cursor)?;
        let max_wait_ms = read_int32(&mut cursor)?;
        let min_bytes = read_int32(&mut cursor)?;

        let max_bytes = if version >= 3 {
            read_int32(&mut cursor)?
        } else {
            i32::MAX
        };

        let isolation_level = if version >= 4 {
            IsolationLevel::from_i8(read_int8(&mut cursor)?)
        } else {
            IsolationLevel::ReadUncommitted
        };

        let session_id = if version >= 7 {
            read_int32(&mut cursor)?
        } else {
            0
        };

        let session_epoch = if version >= 7 {
            read_int32(&mut cursor)?
        } else {
            -1
        };

        let topics = if is_flexible {
            read_compact_array(&mut cursor, |r| parse_fetch_topic(r, version, true))?
                .unwrap_or_default()
        } else {
            read_array(&mut cursor, |r| parse_fetch_topic(r, version, false))?.unwrap_or_default()
        };

        let forgotten_topics = if version >= 7 {
            if is_flexible {
                read_compact_array(&mut cursor, |r| parse_forgotten_topic(r, version, true))?
                    .unwrap_or_default()
            } else {
                read_array(&mut cursor, |r| parse_forgotten_topic(r, version, false))?
                    .unwrap_or_default()
            }
        } else {
            vec![]
        };

        let rack_id = if version >= 11 {
            if is_flexible {
                read_compact_string(&mut cursor)?
            } else {
                read_string(&mut cursor)?
            }
        } else {
            None
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            cluster_id,
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id,
        })
    }
}

fn parse_fetch_topic<R: Read>(
    r: &mut R,
    version: i16,
    is_flexible: bool,
) -> io::Result<FetchRequestTopic> {
    let topic_id = if version >= 13 {
        Some(read_uuid(r)?)
    } else {
        None
    };

    let topic = if version < 13 {
        if is_flexible {
            read_compact_string(r)?
        } else {
            read_string(r)?
        }
    } else {
        None
    };

    let partitions = if is_flexible {
        read_compact_array(r, |r2| parse_fetch_partition(r2, version, true))?.unwrap_or_default()
    } else {
        read_array(r, |r2| parse_fetch_partition(r2, version, false))?.unwrap_or_default()
    };

    if is_flexible {
        let _tagged_fields = read_tagged_fields(r)?;
    }

    Ok(FetchRequestTopic {
        topic_id,
        topic,
        partitions,
    })
}

fn parse_fetch_partition<R: Read>(
    r: &mut R,
    version: i16,
    is_flexible: bool,
) -> io::Result<FetchRequestPartition> {
    let partition = read_int32(r)?;

    let current_leader_epoch = if version >= 9 { read_int32(r)? } else { -1 };

    let fetch_offset = read_int64(r)?;

    let last_fetched_epoch = if version >= 12 { read_int32(r)? } else { -1 };

    let log_start_offset = if version >= 5 { read_int64(r)? } else { -1 };

    let partition_max_bytes = read_int32(r)?;

    if is_flexible {
        let _tagged_fields = read_tagged_fields(r)?;
    }

    Ok(FetchRequestPartition {
        partition,
        current_leader_epoch,
        fetch_offset,
        last_fetched_epoch,
        log_start_offset,
        partition_max_bytes,
    })
}

fn parse_forgotten_topic<R: Read>(
    r: &mut R,
    version: i16,
    is_flexible: bool,
) -> io::Result<FetchForgottenTopic> {
    let topic_id = if version >= 13 {
        Some(read_uuid(r)?)
    } else {
        None
    };

    let topic = if version < 13 {
        if is_flexible {
            read_compact_string(r)?
        } else {
            read_string(r)?
        }
    } else {
        None
    };

    let partitions = if is_flexible {
        read_compact_array(r, read_int32)?.unwrap_or_default()
    } else {
        read_array(r, read_int32)?.unwrap_or_default()
    };

    if is_flexible {
        let _tagged_fields = read_tagged_fields(r)?;
    }

    Ok(FetchForgottenTopic {
        topic_id,
        topic,
        partitions,
    })
}

// ---------------------------------------------------------------------------
// ListOffsets Request (API 2)
// ---------------------------------------------------------------------------

/// ListOffsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    /// Replica ID (broker-to-broker).
    pub replica_id: i32,
    /// Isolation level (v2+).
    pub isolation_level: IsolationLevel,
    /// Topics to query.
    pub topics: Vec<ListOffsetsRequestTopic>,
}

/// Topic in list offsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequestTopic {
    /// Topic name.
    pub name: String,
    /// Partitions to query.
    pub partitions: Vec<ListOffsetsRequestPartition>,
}

/// Partition in list offsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequestPartition {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch (v4+).
    pub current_leader_epoch: i32,
    /// Timestamp to search (-1 = latest, -2 = earliest).
    pub timestamp: i64,
}

impl ListOffsetsRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 6;

        let replica_id = read_int32(&mut cursor)?;

        let isolation_level = if version >= 2 {
            IsolationLevel::from_i8(read_int8(&mut cursor)?)
        } else {
            IsolationLevel::ReadUncommitted
        };

        let topics = if is_flexible {
            read_compact_array(&mut cursor, |r| {
                let name = read_compact_non_nullable_string(r)?;
                let partitions = read_compact_array(r, |r2| {
                    let partition = read_int32(r2)?;
                    let current_leader_epoch = if version >= 4 { read_int32(r2)? } else { -1 };
                    let timestamp = read_int64(r2)?;
                    let _tagged_fields = read_tagged_fields(r2)?;
                    Ok(ListOffsetsRequestPartition {
                        partition,
                        current_leader_epoch,
                        timestamp,
                    })
                })?
                .unwrap_or_default();
                let _tagged_fields = read_tagged_fields(r)?;
                Ok(ListOffsetsRequestTopic { name, partitions })
            })?
            .unwrap_or_default()
        } else {
            read_array(&mut cursor, |r| {
                let name = read_non_nullable_string(r)?;
                let partitions = read_array(r, |r2| {
                    let partition = read_int32(r2)?;
                    let current_leader_epoch = if version >= 4 { read_int32(r2)? } else { -1 };
                    let timestamp = read_int64(r2)?;
                    Ok(ListOffsetsRequestPartition {
                        partition,
                        current_leader_epoch,
                        timestamp,
                    })
                })?
                .unwrap_or_default();
                Ok(ListOffsetsRequestTopic { name, partitions })
            })?
            .unwrap_or_default()
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            replica_id,
            isolation_level,
            topics,
        })
    }
}

// ---------------------------------------------------------------------------
// JoinGroup Request (API 11)
// ---------------------------------------------------------------------------

/// JoinGroup request.
#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    /// Group ID.
    pub group_id: String,
    /// Session timeout in milliseconds.
    pub session_timeout_ms: i32,
    /// Rebalance timeout in milliseconds (v1+).
    pub rebalance_timeout_ms: i32,
    /// Member ID (empty string if new).
    pub member_id: String,
    /// Group instance ID (v5+, static membership).
    pub group_instance_id: Option<String>,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: String,
    /// Supported protocols.
    pub protocols: Vec<JoinGroupProtocol>,
    /// Reason for joining (v8+).
    pub reason: Option<String>,
}

/// Protocol in join group request.
#[derive(Debug, Clone)]
pub struct JoinGroupProtocol {
    /// Protocol name.
    pub name: String,
    /// Protocol metadata.
    pub metadata: Bytes,
}

impl JoinGroupRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 6;

        let group_id = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let session_timeout_ms = read_int32(&mut cursor)?;

        let rebalance_timeout_ms = if version >= 1 {
            read_int32(&mut cursor)?
        } else {
            session_timeout_ms
        };

        let member_id = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let group_instance_id = if version >= 5 {
            if is_flexible {
                read_compact_string(&mut cursor)?
            } else {
                read_string(&mut cursor)?
            }
        } else {
            None
        };

        let protocol_type = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let protocols = if is_flexible {
            read_compact_array(&mut cursor, |r| {
                let name = read_compact_non_nullable_string(r)?;
                let metadata = read_compact_bytes(r)?.map(Bytes::from).unwrap_or_default();
                let _tagged_fields = read_tagged_fields(r)?;
                Ok(JoinGroupProtocol { name, metadata })
            })?
            .unwrap_or_default()
        } else {
            read_array(&mut cursor, |r| {
                let name = read_non_nullable_string(r)?;
                let metadata = read_bytes(r)?.map(Bytes::from).unwrap_or_default();
                Ok(JoinGroupProtocol { name, metadata })
            })?
            .unwrap_or_default()
        };

        let reason = if version >= 8 {
            read_compact_string(&mut cursor)?
        } else {
            None
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
        })
    }
}

// ---------------------------------------------------------------------------
// SyncGroup Request (API 14)
// ---------------------------------------------------------------------------

/// SyncGroup request.
#[derive(Debug, Clone)]
pub struct SyncGroupRequest {
    /// Group ID.
    pub group_id: String,
    /// Generation ID.
    pub generation_id: i32,
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (v3+).
    pub group_instance_id: Option<String>,
    /// Protocol type (v5+).
    pub protocol_type: Option<String>,
    /// Protocol name (v5+).
    pub protocol_name: Option<String>,
    /// Assignments (from leader).
    pub assignments: Vec<SyncGroupAssignment>,
}

/// Assignment in sync group request.
#[derive(Debug, Clone)]
pub struct SyncGroupAssignment {
    /// Member ID.
    pub member_id: String,
    /// Assignment data.
    pub assignment: Bytes,
}

impl SyncGroupRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 4;

        let group_id = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let generation_id = read_int32(&mut cursor)?;

        let member_id = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let group_instance_id = if version >= 3 {
            if is_flexible {
                read_compact_string(&mut cursor)?
            } else {
                read_string(&mut cursor)?
            }
        } else {
            None
        };

        let protocol_type = if version >= 5 {
            read_compact_string(&mut cursor)?
        } else {
            None
        };

        let protocol_name = if version >= 5 {
            read_compact_string(&mut cursor)?
        } else {
            None
        };

        let assignments = if is_flexible {
            read_compact_array(&mut cursor, |r| {
                let member_id = read_compact_non_nullable_string(r)?;
                let assignment = read_compact_bytes(r)?.map(Bytes::from).unwrap_or_default();
                let _tagged_fields = read_tagged_fields(r)?;
                Ok(SyncGroupAssignment {
                    member_id,
                    assignment,
                })
            })?
            .unwrap_or_default()
        } else {
            read_array(&mut cursor, |r| {
                let member_id = read_non_nullable_string(r)?;
                let assignment = read_bytes(r)?.map(Bytes::from).unwrap_or_default();
                Ok(SyncGroupAssignment {
                    member_id,
                    assignment,
                })
            })?
            .unwrap_or_default()
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        })
    }
}

// ---------------------------------------------------------------------------
// Heartbeat Request (API 12)
// ---------------------------------------------------------------------------

/// Heartbeat request.
#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    /// Group ID.
    pub group_id: String,
    /// Generation ID.
    pub generation_id: i32,
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (v3+).
    pub group_instance_id: Option<String>,
}

impl HeartbeatRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 4;

        let group_id = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let generation_id = read_int32(&mut cursor)?;

        let member_id = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let group_instance_id = if version >= 3 {
            if is_flexible {
                read_compact_string(&mut cursor)?
            } else {
                read_string(&mut cursor)?
            }
        } else {
            None
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
        })
    }
}

// ---------------------------------------------------------------------------
// LeaveGroup Request (API 13)
// ---------------------------------------------------------------------------

/// LeaveGroup request.
#[derive(Debug, Clone)]
pub struct LeaveGroupRequest {
    /// Group ID.
    pub group_id: String,
    /// Member ID (v0-v2).
    pub member_id: Option<String>,
    /// Members leaving (v3+).
    pub members: Vec<LeaveGroupMember>,
}

/// Member in leave group request.
#[derive(Debug, Clone)]
pub struct LeaveGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID.
    pub group_instance_id: Option<String>,
    /// Reason for leaving (v5+).
    pub reason: Option<String>,
}

impl LeaveGroupRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 4;

        let group_id = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let (member_id, members) = if version >= 3 {
            let members = if is_flexible {
                read_compact_array(&mut cursor, |r| {
                    let member_id = read_compact_non_nullable_string(r)?;
                    let group_instance_id = read_compact_string(r)?;
                    let reason = if version >= 5 {
                        read_compact_string(r)?
                    } else {
                        None
                    };
                    let _tagged_fields = read_tagged_fields(r)?;
                    Ok(LeaveGroupMember {
                        member_id,
                        group_instance_id,
                        reason,
                    })
                })?
                .unwrap_or_default()
            } else {
                read_array(&mut cursor, |r| {
                    let member_id = read_non_nullable_string(r)?;
                    let group_instance_id = read_string(r)?;
                    Ok(LeaveGroupMember {
                        member_id,
                        group_instance_id,
                        reason: None,
                    })
                })?
                .unwrap_or_default()
            };
            (None, members)
        } else {
            let member_id = read_non_nullable_string(&mut cursor)?;
            (Some(member_id), vec![])
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            group_id,
            member_id,
            members,
        })
    }
}

// ---------------------------------------------------------------------------
// OffsetCommit Request (API 8)
// ---------------------------------------------------------------------------

/// OffsetCommit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequest {
    /// Group ID.
    pub group_id: String,
    /// Generation ID (v1+).
    pub generation_id: i32,
    /// Member ID (v1+).
    pub member_id: Option<String>,
    /// Group instance ID (v7+).
    pub group_instance_id: Option<String>,
    /// Retention time in milliseconds (v2-v4, deprecated).
    pub retention_time_ms: i64,
    /// Topics with offsets to commit.
    pub topics: Vec<OffsetCommitRequestTopic>,
}

/// Topic in offset commit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequestTopic {
    /// Topic name.
    pub name: String,
    /// Partitions with offsets.
    pub partitions: Vec<OffsetCommitRequestPartition>,
}

/// Partition in offset commit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequestPartition {
    /// Partition index.
    pub partition: i32,
    /// Committed offset.
    pub committed_offset: i64,
    /// Committed leader epoch (v6+).
    pub committed_leader_epoch: i32,
    /// Commit timestamp (v1-v2, deprecated).
    pub commit_timestamp: i64,
    /// Metadata.
    pub metadata: Option<String>,
}

impl OffsetCommitRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 8;

        let group_id = if is_flexible {
            read_compact_non_nullable_string(&mut cursor)?
        } else {
            read_non_nullable_string(&mut cursor)?
        };

        let generation_id = if version >= 1 {
            read_int32(&mut cursor)?
        } else {
            -1
        };

        let member_id = if version >= 1 {
            if is_flexible {
                Some(read_compact_non_nullable_string(&mut cursor)?)
            } else {
                Some(read_non_nullable_string(&mut cursor)?)
            }
        } else {
            None
        };

        let group_instance_id = if version >= 7 {
            if is_flexible {
                read_compact_string(&mut cursor)?
            } else {
                read_string(&mut cursor)?
            }
        } else {
            None
        };

        let retention_time_ms = if (2..=4).contains(&version) {
            read_int64(&mut cursor)?
        } else {
            -1
        };

        let topics = if is_flexible {
            read_compact_array(&mut cursor, |r| {
                let name = read_compact_non_nullable_string(r)?;
                let partitions = read_compact_array(r, |r2| {
                    let partition = read_int32(r2)?;
                    let committed_offset = read_int64(r2)?;
                    let committed_leader_epoch = if version >= 6 { read_int32(r2)? } else { -1 };
                    let commit_timestamp = if (1..=2).contains(&version) {
                        read_int64(r2)?
                    } else {
                        -1
                    };
                    let metadata = read_compact_string(r2)?;
                    let _tagged_fields = read_tagged_fields(r2)?;
                    Ok(OffsetCommitRequestPartition {
                        partition,
                        committed_offset,
                        committed_leader_epoch,
                        commit_timestamp,
                        metadata,
                    })
                })?
                .unwrap_or_default();
                let _tagged_fields = read_tagged_fields(r)?;
                Ok(OffsetCommitRequestTopic { name, partitions })
            })?
            .unwrap_or_default()
        } else {
            read_array(&mut cursor, |r| {
                let name = read_non_nullable_string(r)?;
                let partitions = read_array(r, |r2| {
                    let partition = read_int32(r2)?;
                    let committed_offset = read_int64(r2)?;
                    let committed_leader_epoch = if version >= 6 { read_int32(r2)? } else { -1 };
                    let commit_timestamp = if (1..=2).contains(&version) {
                        read_int64(r2)?
                    } else {
                        -1
                    };
                    let metadata = read_string(r2)?;
                    Ok(OffsetCommitRequestPartition {
                        partition,
                        committed_offset,
                        committed_leader_epoch,
                        commit_timestamp,
                        metadata,
                    })
                })?
                .unwrap_or_default();
                Ok(OffsetCommitRequestTopic { name, partitions })
            })?
            .unwrap_or_default()
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            retention_time_ms,
            topics,
        })
    }
}

// ---------------------------------------------------------------------------
// OffsetFetch Request (API 9)
// ---------------------------------------------------------------------------

/// OffsetFetch request.
#[derive(Debug, Clone)]
pub struct OffsetFetchRequest {
    /// Group ID (v0-v7).
    pub group_id: Option<String>,
    /// Topics to fetch offsets for (None = all).
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
    /// Groups to fetch (v8+).
    pub groups: Vec<OffsetFetchRequestGroup>,
    /// Require stable offsets (v7+).
    pub require_stable: bool,
}

/// Topic in offset fetch request.
#[derive(Debug, Clone)]
pub struct OffsetFetchRequestTopic {
    /// Topic name.
    pub name: String,
    /// Partition indices.
    pub partitions: Vec<i32>,
}

/// Group in offset fetch request (v8+).
#[derive(Debug, Clone)]
pub struct OffsetFetchRequestGroup {
    /// Group ID.
    pub group_id: String,
    /// Topics (None = all).
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
}

impl OffsetFetchRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 6;

        if version >= 8 {
            // Batch format
            let groups = read_compact_array(&mut cursor, |r| {
                let group_id = read_compact_non_nullable_string(r)?;
                let topics = read_compact_array(r, |r2| {
                    let name = read_compact_non_nullable_string(r2)?;
                    let partitions = read_compact_array(r2, read_int32)?.unwrap_or_default();
                    let _tagged_fields = read_tagged_fields(r2)?;
                    Ok(OffsetFetchRequestTopic { name, partitions })
                })?;
                let _tagged_fields = read_tagged_fields(r)?;
                Ok(OffsetFetchRequestGroup { group_id, topics })
            })?
            .unwrap_or_default();

            let require_stable = read_boolean(&mut cursor)?;
            let _tagged_fields = read_tagged_fields(&mut cursor)?;

            Ok(Self {
                group_id: None,
                topics: None,
                groups,
                require_stable,
            })
        } else {
            let group_id = if is_flexible {
                read_compact_non_nullable_string(&mut cursor)?
            } else {
                read_non_nullable_string(&mut cursor)?
            };

            let topics = if is_flexible {
                read_compact_array(&mut cursor, |r| {
                    let name = read_compact_non_nullable_string(r)?;
                    let partitions = read_compact_array(r, read_int32)?.unwrap_or_default();
                    let _tagged_fields = read_tagged_fields(r)?;
                    Ok(OffsetFetchRequestTopic { name, partitions })
                })?
            } else {
                read_array(&mut cursor, |r| {
                    let name = read_non_nullable_string(r)?;
                    let partitions = read_array(r, read_int32)?.unwrap_or_default();
                    Ok(OffsetFetchRequestTopic { name, partitions })
                })?
            };

            let require_stable = if version >= 7 {
                read_boolean(&mut cursor)?
            } else {
                false
            };

            if is_flexible {
                let _tagged_fields = read_tagged_fields(&mut cursor)?;
            }

            Ok(Self {
                group_id: Some(group_id),
                topics,
                groups: vec![],
                require_stable,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// SaslHandshake Request (API 17)
// ---------------------------------------------------------------------------

/// SaslHandshake request.
#[derive(Debug, Clone)]
pub struct SaslHandshakeRequest {
    /// SASL mechanism name.
    pub mechanism: String,
}

impl SaslHandshakeRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], _version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let mechanism = read_non_nullable_string(&mut cursor)?;
        Ok(Self { mechanism })
    }
}

// ---------------------------------------------------------------------------
// SaslAuthenticate Request (API 36)
// ---------------------------------------------------------------------------

/// SaslAuthenticate request.
#[derive(Debug, Clone)]
pub struct SaslAuthenticateRequest {
    /// SASL authentication bytes.
    pub auth_bytes: Bytes,
}

impl SaslAuthenticateRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 2;

        let auth_bytes = if is_flexible {
            read_compact_bytes(&mut cursor)?
                .map(Bytes::from)
                .unwrap_or_default()
        } else {
            read_bytes(&mut cursor)?
                .map(Bytes::from)
                .unwrap_or_default()
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self { auth_bytes })
    }
}

// ---------------------------------------------------------------------------
// InitProducerId Request (API 22)
// ---------------------------------------------------------------------------

/// InitProducerId request.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequest {
    /// Transactional ID (nullable).
    pub transactional_id: Option<String>,
    /// Transaction timeout in milliseconds.
    pub transaction_timeout_ms: i32,
    /// Current producer ID (v3+).
    pub producer_id: i64,
    /// Current producer epoch (v3+).
    pub producer_epoch: i16,
}

impl InitProducerIdRequest {
    /// Parse from bytes.
    pub fn parse(data: &[u8], version: i16) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        let is_flexible = version >= 2;

        let transactional_id = if is_flexible {
            read_compact_string(&mut cursor)?
        } else {
            read_string(&mut cursor)?
        };

        let transaction_timeout_ms = read_int32(&mut cursor)?;

        let producer_id = if version >= 3 {
            read_int64(&mut cursor)?
        } else {
            -1
        };

        let producer_epoch = if version >= 3 {
            read_int16(&mut cursor)?
        } else {
            -1
        };

        if is_flexible {
            let _tagged_fields = read_tagged_fields(&mut cursor)?;
        }

        Ok(Self {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_versions_request_v0() {
        let request = ApiVersionsRequest::parse_v0(&[]).unwrap();
        assert!(request.client_software_name.is_none());
    }

    #[test]
    fn test_coordinator_type() {
        assert_eq!(CoordinatorType::from_i8(0), Some(CoordinatorType::Group));
        assert_eq!(
            CoordinatorType::from_i8(1),
            Some(CoordinatorType::Transaction)
        );
        assert_eq!(CoordinatorType::from_i8(2), None);
    }

    #[test]
    fn test_required_acks() {
        assert_eq!(RequiredAcks::from_i16(0), RequiredAcks::None);
        assert_eq!(RequiredAcks::from_i16(1), RequiredAcks::Leader);
        assert_eq!(RequiredAcks::from_i16(-1), RequiredAcks::All);
        assert_eq!(RequiredAcks::from_i16(-2), RequiredAcks::All);
    }

    #[test]
    fn test_isolation_level() {
        assert_eq!(IsolationLevel::from_i8(0), IsolationLevel::ReadUncommitted);
        assert_eq!(IsolationLevel::from_i8(1), IsolationLevel::ReadCommitted);
    }
}
