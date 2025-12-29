//! Kafka API response types.
//!
//! This module defines response structures for all supported Kafka APIs.

use super::types::*;
use crate::workloads::kafka::ErrorCode;
use bytes::Bytes;
use std::io::{self, Write};

// ---------------------------------------------------------------------------
// ApiVersions Response (API 18)
// ---------------------------------------------------------------------------

/// ApiVersions response.
#[derive(Debug, Clone)]
pub struct ApiVersionsResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// Supported API versions.
    pub api_versions: Vec<ApiVersionsResponseKey>,
    /// Throttle time in milliseconds (v1+).
    pub throttle_time_ms: i32,
}

/// API version entry in response.
#[derive(Debug, Clone)]
pub struct ApiVersionsResponseKey {
    /// API key.
    pub api_key: i16,
    /// Minimum supported version.
    pub min_version: i16,
    /// Maximum supported version.
    pub max_version: i16,
}

impl ApiVersionsResponse {
    /// Encode to bytes (version 0-2).
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();

        write_int16(&mut buf, self.error_code.as_i16())?;

        if version >= 3 {
            // Flexible encoding
            let len = encode_unsigned_varint((self.api_versions.len() + 1) as u32);
            buf.write_all(&len)?;
            for api in &self.api_versions {
                write_int16(&mut buf, api.api_key)?;
                write_int16(&mut buf, api.min_version)?;
                write_int16(&mut buf, api.max_version)?;
                write_empty_tagged_fields(&mut buf)?;
            }
        } else {
            write_int32(&mut buf, self.api_versions.len() as i32)?;
            for api in &self.api_versions {
                write_int16(&mut buf, api.api_key)?;
                write_int16(&mut buf, api.min_version)?;
                write_int16(&mut buf, api.max_version)?;
            }
        }

        if version >= 1 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        if version >= 3 {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// Metadata Response (API 3)
// ---------------------------------------------------------------------------

/// Metadata response.
#[derive(Debug, Clone)]
pub struct MetadataResponse {
    /// Throttle time in milliseconds (v3+).
    pub throttle_time_ms: i32,
    /// Brokers in the cluster.
    pub brokers: Vec<MetadataResponseBroker>,
    /// Cluster ID (v2+).
    pub cluster_id: Option<String>,
    /// Controller ID (v1+).
    pub controller_id: i32,
    /// Topics.
    pub topics: Vec<MetadataResponseTopic>,
    /// Cluster authorized operations (v8+).
    pub cluster_authorized_operations: i32,
}

/// Broker in metadata response.
#[derive(Debug, Clone)]
pub struct MetadataResponseBroker {
    /// Node ID.
    pub node_id: i32,
    /// Host name.
    pub host: String,
    /// Port number.
    pub port: i32,
    /// Rack (v1+).
    pub rack: Option<String>,
}

/// Topic in metadata response.
#[derive(Debug, Clone)]
pub struct MetadataResponseTopic {
    /// Error code.
    pub error_code: ErrorCode,
    /// Topic name.
    pub name: Option<String>,
    /// Topic ID (v10+).
    pub topic_id: Option<uuid::Uuid>,
    /// Is internal topic (v1+).
    pub is_internal: bool,
    /// Partitions.
    pub partitions: Vec<MetadataResponsePartition>,
    /// Topic authorized operations (v8+).
    pub topic_authorized_operations: i32,
}

/// Partition in metadata response.
#[derive(Debug, Clone)]
pub struct MetadataResponsePartition {
    /// Error code.
    pub error_code: ErrorCode,
    /// Partition index.
    pub partition_index: i32,
    /// Leader ID.
    pub leader_id: i32,
    /// Leader epoch (v7+).
    pub leader_epoch: i32,
    /// Replica IDs.
    pub replica_nodes: Vec<i32>,
    /// ISR IDs.
    pub isr_nodes: Vec<i32>,
    /// Offline replica IDs (v5+).
    pub offline_replicas: Vec<i32>,
}

impl MetadataResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 9;

        if version >= 3 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        // Brokers
        if is_flexible {
            let len = encode_unsigned_varint((self.brokers.len() + 1) as u32);
            buf.write_all(&len)?;
        } else {
            write_int32(&mut buf, self.brokers.len() as i32)?;
        }

        for broker in &self.brokers {
            write_int32(&mut buf, broker.node_id)?;
            if is_flexible {
                write_compact_string(&mut buf, Some(&broker.host))?;
            } else {
                write_string(&mut buf, Some(&broker.host))?;
            }
            write_int32(&mut buf, broker.port)?;
            if version >= 1 {
                if is_flexible {
                    write_compact_string(&mut buf, broker.rack.as_deref())?;
                } else {
                    write_string(&mut buf, broker.rack.as_deref())?;
                }
            }
            if is_flexible {
                write_empty_tagged_fields(&mut buf)?;
            }
        }

        // Cluster ID
        if version >= 2 {
            if is_flexible {
                write_compact_string(&mut buf, self.cluster_id.as_deref())?;
            } else {
                write_string(&mut buf, self.cluster_id.as_deref())?;
            }
        }

        // Controller ID
        if version >= 1 {
            write_int32(&mut buf, self.controller_id)?;
        }

        // Topics
        if is_flexible {
            let len = encode_unsigned_varint((self.topics.len() + 1) as u32);
            buf.write_all(&len)?;
        } else {
            write_int32(&mut buf, self.topics.len() as i32)?;
        }

        for topic in &self.topics {
            write_int16(&mut buf, topic.error_code.as_i16())?;

            if is_flexible {
                write_compact_string(&mut buf, topic.name.as_deref())?;
            } else {
                write_string(&mut buf, topic.name.as_deref())?;
            }

            if version >= 10 {
                if let Some(id) = topic.topic_id {
                    write_uuid(&mut buf, id)?;
                } else {
                    write_uuid(&mut buf, uuid::Uuid::nil())?;
                }
            }

            if version >= 1 {
                write_boolean(&mut buf, topic.is_internal)?;
            }

            // Partitions
            if is_flexible {
                let len = encode_unsigned_varint((topic.partitions.len() + 1) as u32);
                buf.write_all(&len)?;
            } else {
                write_int32(&mut buf, topic.partitions.len() as i32)?;
            }

            for partition in &topic.partitions {
                write_int16(&mut buf, partition.error_code.as_i16())?;
                write_int32(&mut buf, partition.partition_index)?;
                write_int32(&mut buf, partition.leader_id)?;

                if version >= 7 {
                    write_int32(&mut buf, partition.leader_epoch)?;
                }

                // Replicas
                if is_flexible {
                    let len = encode_unsigned_varint((partition.replica_nodes.len() + 1) as u32);
                    buf.write_all(&len)?;
                } else {
                    write_int32(&mut buf, partition.replica_nodes.len() as i32)?;
                }
                for &node in &partition.replica_nodes {
                    write_int32(&mut buf, node)?;
                }

                // ISR
                if is_flexible {
                    let len = encode_unsigned_varint((partition.isr_nodes.len() + 1) as u32);
                    buf.write_all(&len)?;
                } else {
                    write_int32(&mut buf, partition.isr_nodes.len() as i32)?;
                }
                for &node in &partition.isr_nodes {
                    write_int32(&mut buf, node)?;
                }

                // Offline replicas
                if version >= 5 {
                    if is_flexible {
                        let len =
                            encode_unsigned_varint((partition.offline_replicas.len() + 1) as u32);
                        buf.write_all(&len)?;
                    } else {
                        write_int32(&mut buf, partition.offline_replicas.len() as i32)?;
                    }
                    for &node in &partition.offline_replicas {
                        write_int32(&mut buf, node)?;
                    }
                }

                if is_flexible {
                    write_empty_tagged_fields(&mut buf)?;
                }
            }

            if version >= 8 {
                write_int32(&mut buf, topic.topic_authorized_operations)?;
            }

            if is_flexible {
                write_empty_tagged_fields(&mut buf)?;
            }
        }

        if (8..=10).contains(&version) {
            write_int32(&mut buf, self.cluster_authorized_operations)?;
        }

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// FindCoordinator Response (API 10)
// ---------------------------------------------------------------------------

/// FindCoordinator response.
#[derive(Debug, Clone)]
pub struct FindCoordinatorResponse {
    /// Throttle time in milliseconds (v1+).
    pub throttle_time_ms: i32,
    /// Error code (v0-v3).
    pub error_code: ErrorCode,
    /// Error message (v1+).
    pub error_message: Option<String>,
    /// Node ID.
    pub node_id: i32,
    /// Host name.
    pub host: String,
    /// Port number.
    pub port: i32,
    /// Coordinators (v4+, batch response).
    pub coordinators: Vec<FindCoordinatorResponseCoordinator>,
}

/// Coordinator in response (v4+).
#[derive(Debug, Clone)]
pub struct FindCoordinatorResponseCoordinator {
    /// Key.
    pub key: String,
    /// Node ID.
    pub node_id: i32,
    /// Host.
    pub host: String,
    /// Port.
    pub port: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
}

impl FindCoordinatorResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 3;

        if version >= 1 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        if version >= 4 {
            // Batch format
            let len = encode_unsigned_varint((self.coordinators.len() + 1) as u32);
            buf.write_all(&len)?;

            for coord in &self.coordinators {
                write_compact_string(&mut buf, Some(&coord.key))?;
                write_int32(&mut buf, coord.node_id)?;
                write_compact_string(&mut buf, Some(&coord.host))?;
                write_int32(&mut buf, coord.port)?;
                write_int16(&mut buf, coord.error_code.as_i16())?;
                write_compact_string(&mut buf, coord.error_message.as_deref())?;
                write_empty_tagged_fields(&mut buf)?;
            }

            write_empty_tagged_fields(&mut buf)?;
        } else {
            write_int16(&mut buf, self.error_code.as_i16())?;

            if version >= 1 {
                if is_flexible {
                    write_compact_string(&mut buf, self.error_message.as_deref())?;
                } else {
                    write_string(&mut buf, self.error_message.as_deref())?;
                }
            }

            write_int32(&mut buf, self.node_id)?;

            if is_flexible {
                write_compact_string(&mut buf, Some(&self.host))?;
            } else {
                write_string(&mut buf, Some(&self.host))?;
            }

            write_int32(&mut buf, self.port)?;

            if is_flexible {
                write_empty_tagged_fields(&mut buf)?;
            }
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// Produce Response (API 0)
// ---------------------------------------------------------------------------

/// Produce response.
#[derive(Debug, Clone)]
pub struct ProduceResponse {
    /// Topics with results.
    pub topics: Vec<ProduceResponseTopic>,
    /// Throttle time in milliseconds (v1+).
    pub throttle_time_ms: i32,
}

/// Topic in produce response.
#[derive(Debug, Clone)]
pub struct ProduceResponseTopic {
    /// Topic name.
    pub name: String,
    /// Partitions with results.
    pub partitions: Vec<ProduceResponsePartition>,
}

/// Partition in produce response.
#[derive(Debug, Clone)]
pub struct ProduceResponsePartition {
    /// Partition index.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Base offset of the produced records.
    pub base_offset: i64,
    /// Log append time (v2+).
    pub log_append_time_ms: i64,
    /// Log start offset (v5+).
    pub log_start_offset: i64,
    /// Record errors (v8+).
    pub record_errors: Vec<ProduceResponseRecordError>,
    /// Error message (v8+).
    pub error_message: Option<String>,
}

/// Record error in produce response.
#[derive(Debug, Clone)]
pub struct ProduceResponseRecordError {
    /// Batch index.
    pub batch_index: i32,
    /// Error message.
    pub batch_index_error_message: Option<String>,
}

impl ProduceResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 9;

        // Topics
        if is_flexible {
            let len = encode_unsigned_varint((self.topics.len() + 1) as u32);
            buf.write_all(&len)?;
        } else {
            write_int32(&mut buf, self.topics.len() as i32)?;
        }

        for topic in &self.topics {
            if is_flexible {
                write_compact_string(&mut buf, Some(&topic.name))?;
            } else {
                write_string(&mut buf, Some(&topic.name))?;
            }

            // Partitions
            if is_flexible {
                let len = encode_unsigned_varint((topic.partitions.len() + 1) as u32);
                buf.write_all(&len)?;
            } else {
                write_int32(&mut buf, topic.partitions.len() as i32)?;
            }

            for partition in &topic.partitions {
                write_int32(&mut buf, partition.partition)?;
                write_int16(&mut buf, partition.error_code.as_i16())?;
                write_int64(&mut buf, partition.base_offset)?;

                if version >= 2 {
                    write_int64(&mut buf, partition.log_append_time_ms)?;
                }

                if version >= 5 {
                    write_int64(&mut buf, partition.log_start_offset)?;
                }

                if version >= 8 {
                    if is_flexible {
                        let len =
                            encode_unsigned_varint((partition.record_errors.len() + 1) as u32);
                        buf.write_all(&len)?;
                    } else {
                        write_int32(&mut buf, partition.record_errors.len() as i32)?;
                    }

                    for error in &partition.record_errors {
                        write_int32(&mut buf, error.batch_index)?;
                        if is_flexible {
                            write_compact_string(
                                &mut buf,
                                error.batch_index_error_message.as_deref(),
                            )?;
                        } else {
                            write_string(&mut buf, error.batch_index_error_message.as_deref())?;
                        }
                        if is_flexible {
                            write_empty_tagged_fields(&mut buf)?;
                        }
                    }

                    if is_flexible {
                        write_compact_string(&mut buf, partition.error_message.as_deref())?;
                    } else {
                        write_string(&mut buf, partition.error_message.as_deref())?;
                    }
                }

                if is_flexible {
                    write_empty_tagged_fields(&mut buf)?;
                }
            }

            if is_flexible {
                write_empty_tagged_fields(&mut buf)?;
            }
        }

        if version >= 1 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// Fetch Response (API 1)
// ---------------------------------------------------------------------------

/// Fetch response.
#[derive(Debug, Clone)]
pub struct FetchResponse {
    /// Throttle time in milliseconds (v1+).
    pub throttle_time_ms: i32,
    /// Error code (v7+).
    pub error_code: ErrorCode,
    /// Session ID (v7+).
    pub session_id: i32,
    /// Topics with results.
    pub topics: Vec<FetchResponseTopic>,
}

/// Topic in fetch response.
#[derive(Debug, Clone)]
pub struct FetchResponseTopic {
    /// Topic ID (v13+).
    pub topic_id: Option<uuid::Uuid>,
    /// Topic name.
    pub topic: Option<String>,
    /// Partitions with results.
    pub partitions: Vec<FetchResponsePartition>,
}

/// Partition in fetch response.
#[derive(Debug, Clone)]
pub struct FetchResponsePartition {
    /// Partition index.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// High watermark.
    pub high_watermark: i64,
    /// Last stable offset (v4+).
    pub last_stable_offset: i64,
    /// Log start offset (v5+).
    pub log_start_offset: i64,
    /// Aborted transactions (v4+).
    pub aborted_transactions: Vec<FetchAbortedTransaction>,
    /// Preferred read replica (v11+).
    pub preferred_read_replica: i32,
    /// Record batches.
    pub records: Option<Bytes>,
}

/// Aborted transaction in fetch response.
#[derive(Debug, Clone)]
pub struct FetchAbortedTransaction {
    /// Producer ID.
    pub producer_id: i64,
    /// First offset.
    pub first_offset: i64,
}

impl FetchResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 12;

        if version >= 1 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        if version >= 7 {
            write_int16(&mut buf, self.error_code.as_i16())?;
            write_int32(&mut buf, self.session_id)?;
        }

        // Topics
        if is_flexible {
            let len = encode_unsigned_varint((self.topics.len() + 1) as u32);
            buf.write_all(&len)?;
        } else {
            write_int32(&mut buf, self.topics.len() as i32)?;
        }

        for topic in &self.topics {
            if version >= 13 {
                write_uuid(&mut buf, topic.topic_id.unwrap_or(uuid::Uuid::nil()))?;
            } else if is_flexible {
                write_compact_string(&mut buf, topic.topic.as_deref())?;
            } else {
                write_string(&mut buf, topic.topic.as_deref())?;
            }

            // Partitions
            if is_flexible {
                let len = encode_unsigned_varint((topic.partitions.len() + 1) as u32);
                buf.write_all(&len)?;
            } else {
                write_int32(&mut buf, topic.partitions.len() as i32)?;
            }

            for partition in &topic.partitions {
                write_int32(&mut buf, partition.partition)?;
                write_int16(&mut buf, partition.error_code.as_i16())?;
                write_int64(&mut buf, partition.high_watermark)?;

                if version >= 4 {
                    write_int64(&mut buf, partition.last_stable_offset)?;
                }

                if version >= 5 {
                    write_int64(&mut buf, partition.log_start_offset)?;
                }

                if version >= 4 {
                    // Aborted transactions
                    if is_flexible {
                        let len = encode_unsigned_varint(
                            (partition.aborted_transactions.len() + 1) as u32,
                        );
                        buf.write_all(&len)?;
                    } else {
                        write_int32(&mut buf, partition.aborted_transactions.len() as i32)?;
                    }

                    for txn in &partition.aborted_transactions {
                        write_int64(&mut buf, txn.producer_id)?;
                        write_int64(&mut buf, txn.first_offset)?;
                        if is_flexible {
                            write_empty_tagged_fields(&mut buf)?;
                        }
                    }
                }

                if version >= 11 {
                    write_int32(&mut buf, partition.preferred_read_replica)?;
                }

                // Records
                if is_flexible {
                    write_compact_bytes(&mut buf, partition.records.as_deref())?;
                } else {
                    write_bytes(&mut buf, partition.records.as_deref())?;
                }

                if is_flexible {
                    write_empty_tagged_fields(&mut buf)?;
                }
            }

            if is_flexible {
                write_empty_tagged_fields(&mut buf)?;
            }
        }

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// ListOffsets Response (API 2)
// ---------------------------------------------------------------------------

/// ListOffsets response.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponse {
    /// Throttle time in milliseconds (v2+).
    pub throttle_time_ms: i32,
    /// Topics with results.
    pub topics: Vec<ListOffsetsResponseTopic>,
}

/// Topic in list offsets response.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponseTopic {
    /// Topic name.
    pub name: String,
    /// Partitions with results.
    pub partitions: Vec<ListOffsetsResponsePartition>,
}

/// Partition in list offsets response.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponsePartition {
    /// Partition index.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Timestamp (v1+, or -1).
    pub timestamp: i64,
    /// Offset.
    pub offset: i64,
    /// Leader epoch (v4+).
    pub leader_epoch: i32,
}

impl ListOffsetsResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 6;

        if version >= 2 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        // Topics
        if is_flexible {
            let len = encode_unsigned_varint((self.topics.len() + 1) as u32);
            buf.write_all(&len)?;
        } else {
            write_int32(&mut buf, self.topics.len() as i32)?;
        }

        for topic in &self.topics {
            if is_flexible {
                write_compact_string(&mut buf, Some(&topic.name))?;
            } else {
                write_string(&mut buf, Some(&topic.name))?;
            }

            // Partitions
            if is_flexible {
                let len = encode_unsigned_varint((topic.partitions.len() + 1) as u32);
                buf.write_all(&len)?;
            } else {
                write_int32(&mut buf, topic.partitions.len() as i32)?;
            }

            for partition in &topic.partitions {
                write_int32(&mut buf, partition.partition)?;
                write_int16(&mut buf, partition.error_code.as_i16())?;

                if version >= 1 {
                    write_int64(&mut buf, partition.timestamp)?;
                }

                write_int64(&mut buf, partition.offset)?;

                if version >= 4 {
                    write_int32(&mut buf, partition.leader_epoch)?;
                }

                if is_flexible {
                    write_empty_tagged_fields(&mut buf)?;
                }
            }

            if is_flexible {
                write_empty_tagged_fields(&mut buf)?;
            }
        }

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// JoinGroup Response (API 11)
// ---------------------------------------------------------------------------

/// JoinGroup response.
#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    /// Throttle time in milliseconds (v2+).
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Generation ID.
    pub generation_id: i32,
    /// Protocol type (v7+).
    pub protocol_type: Option<String>,
    /// Selected protocol name.
    pub protocol_name: Option<String>,
    /// Group leader member ID.
    pub leader: String,
    /// Skip assignment (v9+).
    pub skip_assignment: bool,
    /// This member's ID.
    pub member_id: String,
    /// Group members (only for leader).
    pub members: Vec<JoinGroupResponseMember>,
}

/// Member in join group response.
#[derive(Debug, Clone)]
pub struct JoinGroupResponseMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (v5+).
    pub group_instance_id: Option<String>,
    /// Member metadata.
    pub metadata: Bytes,
}

impl JoinGroupResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 6;

        if version >= 2 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        write_int16(&mut buf, self.error_code.as_i16())?;
        write_int32(&mut buf, self.generation_id)?;

        if version >= 7 {
            write_compact_string(&mut buf, self.protocol_type.as_deref())?;
        }

        if is_flexible {
            write_compact_string(&mut buf, self.protocol_name.as_deref())?;
            write_compact_string(&mut buf, Some(&self.leader))?;
        } else {
            write_string(&mut buf, self.protocol_name.as_deref())?;
            write_string(&mut buf, Some(&self.leader))?;
        }

        if version >= 9 {
            write_boolean(&mut buf, self.skip_assignment)?;
        }

        if is_flexible {
            write_compact_string(&mut buf, Some(&self.member_id))?;
        } else {
            write_string(&mut buf, Some(&self.member_id))?;
        }

        // Members
        if is_flexible {
            let len = encode_unsigned_varint((self.members.len() + 1) as u32);
            buf.write_all(&len)?;
        } else {
            write_int32(&mut buf, self.members.len() as i32)?;
        }

        for member in &self.members {
            if is_flexible {
                write_compact_string(&mut buf, Some(&member.member_id))?;
            } else {
                write_string(&mut buf, Some(&member.member_id))?;
            }

            if version >= 5 {
                if is_flexible {
                    write_compact_string(&mut buf, member.group_instance_id.as_deref())?;
                } else {
                    write_string(&mut buf, member.group_instance_id.as_deref())?;
                }
            }

            if is_flexible {
                write_compact_bytes(&mut buf, Some(&member.metadata))?;
            } else {
                write_bytes(&mut buf, Some(&member.metadata))?;
            }

            if is_flexible {
                write_empty_tagged_fields(&mut buf)?;
            }
        }

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// SyncGroup Response (API 14)
// ---------------------------------------------------------------------------

/// SyncGroup response.
#[derive(Debug, Clone)]
pub struct SyncGroupResponse {
    /// Throttle time in milliseconds (v1+).
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Protocol type (v5+).
    pub protocol_type: Option<String>,
    /// Protocol name (v5+).
    pub protocol_name: Option<String>,
    /// Assignment.
    pub assignment: Bytes,
}

impl SyncGroupResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 4;

        if version >= 1 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        write_int16(&mut buf, self.error_code.as_i16())?;

        if version >= 5 {
            write_compact_string(&mut buf, self.protocol_type.as_deref())?;
            write_compact_string(&mut buf, self.protocol_name.as_deref())?;
        }

        if is_flexible {
            write_compact_bytes(&mut buf, Some(&self.assignment))?;
        } else {
            write_bytes(&mut buf, Some(&self.assignment))?;
        }

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// Heartbeat Response (API 12)
// ---------------------------------------------------------------------------

/// Heartbeat response.
#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    /// Throttle time in milliseconds (v1+).
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl HeartbeatResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 4;

        if version >= 1 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        write_int16(&mut buf, self.error_code.as_i16())?;

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// LeaveGroup Response (API 13)
// ---------------------------------------------------------------------------

/// LeaveGroup response.
#[derive(Debug, Clone)]
pub struct LeaveGroupResponse {
    /// Throttle time in milliseconds (v1+).
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Member responses (v3+).
    pub members: Vec<LeaveGroupResponseMember>,
}

/// Member in leave group response.
#[derive(Debug, Clone)]
pub struct LeaveGroupResponseMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID.
    pub group_instance_id: Option<String>,
    /// Error code.
    pub error_code: ErrorCode,
}

impl LeaveGroupResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 4;

        if version >= 1 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        write_int16(&mut buf, self.error_code.as_i16())?;

        if version >= 3 {
            if is_flexible {
                let len = encode_unsigned_varint((self.members.len() + 1) as u32);
                buf.write_all(&len)?;
            } else {
                write_int32(&mut buf, self.members.len() as i32)?;
            }

            for member in &self.members {
                if is_flexible {
                    write_compact_string(&mut buf, Some(&member.member_id))?;
                    write_compact_string(&mut buf, member.group_instance_id.as_deref())?;
                } else {
                    write_string(&mut buf, Some(&member.member_id))?;
                    write_string(&mut buf, member.group_instance_id.as_deref())?;
                }
                write_int16(&mut buf, member.error_code.as_i16())?;

                if is_flexible {
                    write_empty_tagged_fields(&mut buf)?;
                }
            }
        }

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// OffsetCommit Response (API 8)
// ---------------------------------------------------------------------------

/// OffsetCommit response.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponse {
    /// Throttle time in milliseconds (v3+).
    pub throttle_time_ms: i32,
    /// Topics with results.
    pub topics: Vec<OffsetCommitResponseTopic>,
}

/// Topic in offset commit response.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponseTopic {
    /// Topic name.
    pub name: String,
    /// Partitions with results.
    pub partitions: Vec<OffsetCommitResponsePartition>,
}

/// Partition in offset commit response.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponsePartition {
    /// Partition index.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl OffsetCommitResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 8;

        if version >= 3 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        // Topics
        if is_flexible {
            let len = encode_unsigned_varint((self.topics.len() + 1) as u32);
            buf.write_all(&len)?;
        } else {
            write_int32(&mut buf, self.topics.len() as i32)?;
        }

        for topic in &self.topics {
            if is_flexible {
                write_compact_string(&mut buf, Some(&topic.name))?;
            } else {
                write_string(&mut buf, Some(&topic.name))?;
            }

            // Partitions
            if is_flexible {
                let len = encode_unsigned_varint((topic.partitions.len() + 1) as u32);
                buf.write_all(&len)?;
            } else {
                write_int32(&mut buf, topic.partitions.len() as i32)?;
            }

            for partition in &topic.partitions {
                write_int32(&mut buf, partition.partition)?;
                write_int16(&mut buf, partition.error_code.as_i16())?;

                if is_flexible {
                    write_empty_tagged_fields(&mut buf)?;
                }
            }

            if is_flexible {
                write_empty_tagged_fields(&mut buf)?;
            }
        }

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// OffsetFetch Response (API 9)
// ---------------------------------------------------------------------------

/// OffsetFetch response.
#[derive(Debug, Clone)]
pub struct OffsetFetchResponse {
    /// Throttle time in milliseconds (v3+).
    pub throttle_time_ms: i32,
    /// Topics with results (v0-v7).
    pub topics: Vec<OffsetFetchResponseTopic>,
    /// Error code (v2+).
    pub error_code: ErrorCode,
    /// Groups (v8+).
    pub groups: Vec<OffsetFetchResponseGroup>,
}

/// Topic in offset fetch response.
#[derive(Debug, Clone)]
pub struct OffsetFetchResponseTopic {
    /// Topic name.
    pub name: String,
    /// Partitions with results.
    pub partitions: Vec<OffsetFetchResponsePartition>,
}

/// Partition in offset fetch response.
#[derive(Debug, Clone)]
pub struct OffsetFetchResponsePartition {
    /// Partition index.
    pub partition: i32,
    /// Committed offset.
    pub committed_offset: i64,
    /// Committed leader epoch (v5+).
    pub committed_leader_epoch: i32,
    /// Metadata.
    pub metadata: Option<String>,
    /// Error code.
    pub error_code: ErrorCode,
}

/// Group in offset fetch response (v8+).
#[derive(Debug, Clone)]
pub struct OffsetFetchResponseGroup {
    /// Group ID.
    pub group_id: String,
    /// Topics.
    pub topics: Vec<OffsetFetchResponseTopic>,
    /// Error code.
    pub error_code: ErrorCode,
}

impl OffsetFetchResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 6;

        if version >= 3 {
            write_int32(&mut buf, self.throttle_time_ms)?;
        }

        if version >= 8 {
            // Batch format
            let len = encode_unsigned_varint((self.groups.len() + 1) as u32);
            buf.write_all(&len)?;

            for group in &self.groups {
                write_compact_string(&mut buf, Some(&group.group_id))?;

                let topic_len = encode_unsigned_varint((group.topics.len() + 1) as u32);
                buf.write_all(&topic_len)?;

                for topic in &group.topics {
                    write_compact_string(&mut buf, Some(&topic.name))?;

                    let part_len = encode_unsigned_varint((topic.partitions.len() + 1) as u32);
                    buf.write_all(&part_len)?;

                    for partition in &topic.partitions {
                        write_int32(&mut buf, partition.partition)?;
                        write_int64(&mut buf, partition.committed_offset)?;
                        write_int32(&mut buf, partition.committed_leader_epoch)?;
                        write_compact_string(&mut buf, partition.metadata.as_deref())?;
                        write_int16(&mut buf, partition.error_code.as_i16())?;
                        write_empty_tagged_fields(&mut buf)?;
                    }

                    write_empty_tagged_fields(&mut buf)?;
                }

                write_int16(&mut buf, group.error_code.as_i16())?;
                write_empty_tagged_fields(&mut buf)?;
            }

            write_empty_tagged_fields(&mut buf)?;
        } else {
            // Topics
            if is_flexible {
                let len = encode_unsigned_varint((self.topics.len() + 1) as u32);
                buf.write_all(&len)?;
            } else {
                write_int32(&mut buf, self.topics.len() as i32)?;
            }

            for topic in &self.topics {
                if is_flexible {
                    write_compact_string(&mut buf, Some(&topic.name))?;
                } else {
                    write_string(&mut buf, Some(&topic.name))?;
                }

                // Partitions
                if is_flexible {
                    let len = encode_unsigned_varint((topic.partitions.len() + 1) as u32);
                    buf.write_all(&len)?;
                } else {
                    write_int32(&mut buf, topic.partitions.len() as i32)?;
                }

                for partition in &topic.partitions {
                    write_int32(&mut buf, partition.partition)?;
                    write_int64(&mut buf, partition.committed_offset)?;

                    if version >= 5 {
                        write_int32(&mut buf, partition.committed_leader_epoch)?;
                    }

                    if is_flexible {
                        write_compact_string(&mut buf, partition.metadata.as_deref())?;
                    } else {
                        write_string(&mut buf, partition.metadata.as_deref())?;
                    }

                    write_int16(&mut buf, partition.error_code.as_i16())?;

                    if is_flexible {
                        write_empty_tagged_fields(&mut buf)?;
                    }
                }

                if is_flexible {
                    write_empty_tagged_fields(&mut buf)?;
                }
            }

            if version >= 2 {
                write_int16(&mut buf, self.error_code.as_i16())?;
            }

            if is_flexible {
                write_empty_tagged_fields(&mut buf)?;
            }
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// SaslHandshake Response (API 17)
// ---------------------------------------------------------------------------

/// SaslHandshake response.
#[derive(Debug, Clone)]
pub struct SaslHandshakeResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// Supported mechanisms.
    pub mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    /// Encode to bytes.
    pub fn encode(&self, _version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();

        write_int16(&mut buf, self.error_code.as_i16())?;
        write_int32(&mut buf, self.mechanisms.len() as i32)?;
        for mechanism in &self.mechanisms {
            write_string(&mut buf, Some(mechanism))?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// SaslAuthenticate Response (API 36)
// ---------------------------------------------------------------------------

/// SaslAuthenticate response.
#[derive(Debug, Clone)]
pub struct SaslAuthenticateResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Authentication bytes.
    pub auth_bytes: Bytes,
    /// Session lifetime in milliseconds (v1+).
    pub session_lifetime_ms: i64,
}

impl SaslAuthenticateResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 2;

        write_int16(&mut buf, self.error_code.as_i16())?;

        if is_flexible {
            write_compact_string(&mut buf, self.error_message.as_deref())?;
            write_compact_bytes(&mut buf, Some(&self.auth_bytes))?;
        } else {
            write_string(&mut buf, self.error_message.as_deref())?;
            write_bytes(&mut buf, Some(&self.auth_bytes))?;
        }

        if version >= 1 {
            write_int64(&mut buf, self.session_lifetime_ms)?;
        }

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// InitProducerId Response (API 22)
// ---------------------------------------------------------------------------

/// InitProducerId response.
#[derive(Debug, Clone)]
pub struct InitProducerIdResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
}

impl InitProducerIdResponse {
    /// Encode to bytes.
    pub fn encode(&self, version: i16) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let is_flexible = version >= 2;

        write_int32(&mut buf, self.throttle_time_ms)?;
        write_int16(&mut buf, self.error_code.as_i16())?;
        write_int64(&mut buf, self.producer_id)?;
        write_int16(&mut buf, self.producer_epoch)?;

        if is_flexible {
            write_empty_tagged_fields(&mut buf)?;
        }

        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_versions_response_encode() {
        let response = ApiVersionsResponse {
            error_code: ErrorCode::None,
            api_versions: vec![
                ApiVersionsResponseKey {
                    api_key: 0,
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersionsResponseKey {
                    api_key: 1,
                    min_version: 0,
                    max_version: 13,
                },
            ],
            throttle_time_ms: 0,
        };

        let encoded = response.encode(0).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_metadata_response_encode() {
        let response = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("test-cluster".to_string()),
            controller_id: 1,
            topics: vec![],
            cluster_authorized_operations: 0,
        };

        let encoded = response.encode(3).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_heartbeat_response_encode() {
        let response = HeartbeatResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
        };

        let encoded = response.encode(0).unwrap();
        assert_eq!(encoded.len(), 2); // Just error code
    }

    #[test]
    fn test_sasl_handshake_response_encode() {
        let response = SaslHandshakeResponse {
            error_code: ErrorCode::None,
            mechanisms: vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()],
        };

        let encoded = response.encode(0).unwrap();
        assert!(!encoded.is_empty());
    }
}
