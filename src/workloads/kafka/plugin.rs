//! Kafka protocol plugin implementation.
//!
//! This module implements the `ProtocolPlugin` trait for Kafka protocol support,
//! handling connection lifecycle, authentication, and request routing.

use super::protocol::*;
use super::workload::KAFKA_DESCRIPTOR;
use super::{ApiKey, ErrorCode};
use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default Kafka port.
pub const DEFAULT_KAFKA_PORT: u16 = 9092;

/// Maximum request size (100MB).
#[allow(dead_code)]
pub const MAX_REQUEST_SIZE: usize = 100 * 1024 * 1024;

/// Supported SASL mechanisms.
pub const SUPPORTED_SASL_MECHANISMS: &[&str] = &["PLAIN", "SCRAM-SHA-256"];

// ---------------------------------------------------------------------------
// Kafka Protocol Descriptor
// ---------------------------------------------------------------------------

/// Kafka protocol descriptor for plugin registration.
#[derive(Debug, Clone)]
pub struct KafkaProtocolDescriptor {
    /// Protocol name.
    pub name: &'static str,
    /// Default port.
    pub default_port: u16,
    /// Workload label.
    pub workload_label: &'static str,
    /// Workload version.
    pub workload_version: u16,
    /// Requires SNI for tenant identification.
    pub requires_sni: bool,
    /// Supported ALPN values.
    pub supported_alpns: &'static [&'static str],
    /// Rate limit classes.
    pub rate_limit_classes: &'static [&'static str],
    /// Required capabilities.
    pub requires_capabilities: &'static [&'static str],
}

/// Default Kafka protocol descriptor.
pub const KAFKA_PROTOCOL_DESCRIPTOR: KafkaProtocolDescriptor = KafkaProtocolDescriptor {
    name: "kafka",
    default_port: DEFAULT_KAFKA_PORT,
    workload_label: KAFKA_DESCRIPTOR.label,
    workload_version: KAFKA_DESCRIPTOR.version,
    requires_sni: false, // Kafka uses SASL for tenant identification
    supported_alpns: &["kafka"],
    rate_limit_classes: &["kafka.produce", "kafka.fetch", "kafka.admin"],
    requires_capabilities: &["kafka_features"],
};

// ---------------------------------------------------------------------------
// Connection State
// ---------------------------------------------------------------------------

/// Kafka connection state.
#[derive(Debug)]
pub struct KafkaConnectionState {
    /// Client ID from the first request.
    pub client_id: Option<String>,
    /// Negotiated API versions.
    pub api_versions: HashMap<ApiKey, (i16, i16)>,
    /// Authentication state.
    pub auth_state: AuthState,
    /// Tenant ID (extracted from SASL).
    pub tenant_id: Option<String>,
    /// Active consumer group memberships.
    pub group_memberships: HashMap<String, GroupMembership>,
    /// Transaction context.
    pub transaction_ctx: Option<TransactionContext>,
    /// Request metrics.
    pub metrics: ConnectionMetrics,
}

impl Default for KafkaConnectionState {
    fn default() -> Self {
        Self {
            client_id: None,
            api_versions: HashMap::new(),
            auth_state: AuthState::Unauthenticated,
            tenant_id: None,
            group_memberships: HashMap::new(),
            transaction_ctx: None,
            metrics: ConnectionMetrics::default(),
        }
    }
}

/// Authentication state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthState {
    /// Not authenticated.
    Unauthenticated,
    /// SASL handshake completed, awaiting authentication.
    SaslHandshake { mechanism: String },
    /// Fully authenticated.
    Authenticated { username: String, tenant_id: String },
}

/// Group membership for a connection.
#[derive(Debug, Clone)]
pub struct GroupMembership {
    /// Group ID.
    pub group_id: String,
    /// Member ID.
    pub member_id: String,
    /// Generation ID.
    pub generation_id: i32,
    /// Last heartbeat timestamp.
    pub last_heartbeat_ms: u64,
}

/// Transaction context for a connection.
#[derive(Debug, Clone)]
pub struct TransactionContext {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Transaction state.
    pub state: TransactionState,
}

/// Transaction states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// No active transaction.
    Empty,
    /// Transaction in progress.
    Ongoing,
    /// Preparing to commit.
    PrepareCommit,
    /// Preparing to abort.
    PrepareAbort,
}

/// Connection metrics.
#[derive(Debug, Default, Clone)]
pub struct ConnectionMetrics {
    /// Total requests processed.
    pub requests_total: u64,
    /// Total bytes received.
    pub bytes_in: u64,
    /// Total bytes sent.
    pub bytes_out: u64,
    /// Requests by API key.
    pub requests_by_api: HashMap<ApiKey, u64>,
    /// Authentication failures.
    pub auth_failures: u64,
}

// ---------------------------------------------------------------------------
// Request Handler
// ---------------------------------------------------------------------------

/// Kafka request handler context.
pub struct KafkaRequestContext {
    /// Connection state.
    pub conn_state: KafkaConnectionState,
    /// Request decoder.
    pub decoder: KafkaDecoder,
    /// Response encoder.
    pub encoder: KafkaEncoder,
    /// Read buffer.
    pub read_buf: BytesMut,
    /// Write buffer.
    pub write_buf: BytesMut,
}

impl Default for KafkaRequestContext {
    fn default() -> Self {
        Self {
            conn_state: KafkaConnectionState::default(),
            decoder: KafkaDecoder::new(),
            encoder: KafkaEncoder::new(),
            read_buf: BytesMut::with_capacity(64 * 1024),
            write_buf: BytesMut::with_capacity(64 * 1024),
        }
    }
}

impl KafkaRequestContext {
    /// Create a new request context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Process a complete request frame.
    pub fn process_request(&mut self, frame: &[u8]) -> io::Result<Vec<u8>> {
        // Parse request header
        let (header, header_size) = parse_request_header_from_bytes(frame)?;
        let body = &frame[header_size..];

        // Update client ID if not set
        if self.conn_state.client_id.is_none() {
            self.conn_state.client_id = header.client_id.clone();
        }

        // Update metrics
        self.conn_state.metrics.requests_total += 1;
        *self
            .conn_state
            .metrics
            .requests_by_api
            .entry(header.api_key)
            .or_insert(0) += 1;

        // Dispatch based on API key
        let response_body = match header.api_key {
            ApiKey::ApiVersions => self.handle_api_versions(&header, body)?,
            ApiKey::Metadata => self.handle_metadata(&header, body)?,
            ApiKey::SaslHandshake => self.handle_sasl_handshake(&header, body)?,
            ApiKey::SaslAuthenticate => self.handle_sasl_authenticate(&header, body)?,
            ApiKey::FindCoordinator => self.handle_find_coordinator(&header, body)?,
            ApiKey::Produce => self.handle_produce(&header, body)?,
            ApiKey::Fetch => self.handle_fetch(&header, body)?,
            ApiKey::ListOffsets => self.handle_list_offsets(&header, body)?,
            ApiKey::OffsetCommit => self.handle_offset_commit(&header, body)?,
            ApiKey::OffsetFetch => self.handle_offset_fetch(&header, body)?,
            ApiKey::JoinGroup => self.handle_join_group(&header, body)?,
            ApiKey::SyncGroup => self.handle_sync_group(&header, body)?,
            ApiKey::Heartbeat => self.handle_heartbeat(&header, body)?,
            ApiKey::LeaveGroup => self.handle_leave_group(&header, body)?,
            ApiKey::InitProducerId => self.handle_init_producer_id(&header, body)?,
            _ => self.handle_unsupported(&header)?,
        };

        // Build response frame
        let is_flexible = is_flexible_version(header.api_key, header.api_version);
        let response_header = ResponseHeader::new(header.correlation_id);
        let header_bytes = encode_response_header_to_bytes(&response_header, is_flexible);

        // Combine header and body
        let mut response = Vec::with_capacity(4 + header_bytes.len() + response_body.len());
        let total_len = header_bytes.len() + response_body.len();
        response.put_u32(total_len as u32);
        response.extend_from_slice(&header_bytes);
        response.extend_from_slice(&response_body);

        Ok(response)
    }

    // -----------------------------------------------------------------------
    // API Handlers
    // -----------------------------------------------------------------------

    fn handle_api_versions(&mut self, header: &RequestHeader, _body: &[u8]) -> io::Result<Vec<u8>> {
        let versions = supported_api_versions();

        let response = ApiVersionsResponse {
            error_code: ErrorCode::None,
            api_versions: versions
                .iter()
                .map(|v| ApiVersionsResponseKey {
                    api_key: v.api_key.as_i16(),
                    min_version: v.min_version,
                    max_version: v.max_version,
                })
                .collect(),
            throttle_time_ms: 0,
        };

        response.encode(header.api_version)
    }

    fn handle_metadata(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        // Parse request
        let _request = MetadataRequest::parse(body, header.api_version)?;

        // Build response with default broker info
        let response = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("quantum-cluster".to_string()),
            controller_id: 1,
            topics: vec![], // Topics would be fetched from workload state
            cluster_authorized_operations: -1,
        };

        response.encode(header.api_version)
    }

    fn handle_sasl_handshake(
        &mut self,
        header: &RequestHeader,
        body: &[u8],
    ) -> io::Result<Vec<u8>> {
        let request = SaslHandshakeRequest::parse(body, header.api_version)?;

        let (error_code, mechanisms) =
            if SUPPORTED_SASL_MECHANISMS.contains(&request.mechanism.as_str()) {
                self.conn_state.auth_state = AuthState::SaslHandshake {
                    mechanism: request.mechanism.clone(),
                };
                (
                    ErrorCode::None,
                    SUPPORTED_SASL_MECHANISMS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                )
            } else {
                (
                    ErrorCode::UnsupportedSaslMechanism,
                    SUPPORTED_SASL_MECHANISMS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                )
            };

        let response = SaslHandshakeResponse {
            error_code,
            mechanisms,
        };

        response.encode(header.api_version)
    }

    fn handle_sasl_authenticate(
        &mut self,
        header: &RequestHeader,
        body: &[u8],
    ) -> io::Result<Vec<u8>> {
        let request = SaslAuthenticateRequest::parse(body, header.api_version)?;

        // Parse PLAIN mechanism: \0username\0password
        let auth_bytes = &request.auth_bytes;
        let (error_code, error_message, session_lifetime_ms) =
            if let Some((username, tenant_id)) = parse_plain_auth(auth_bytes) {
                self.conn_state.auth_state = AuthState::Authenticated {
                    username: username.clone(),
                    tenant_id: tenant_id.clone(),
                };
                self.conn_state.tenant_id = Some(tenant_id);
                (ErrorCode::None, None, 3_600_000) // 1 hour session
            } else {
                self.conn_state.metrics.auth_failures += 1;
                (
                    ErrorCode::SaslAuthenticationFailed,
                    Some("Invalid credentials".to_string()),
                    0,
                )
            };

        let response = SaslAuthenticateResponse {
            error_code,
            error_message,
            auth_bytes: bytes::Bytes::new(),
            session_lifetime_ms,
        };

        response.encode(header.api_version)
    }

    fn handle_find_coordinator(
        &mut self,
        header: &RequestHeader,
        body: &[u8],
    ) -> io::Result<Vec<u8>> {
        let _request = FindCoordinatorRequest::parse(body, header.api_version)?;

        // Return self as coordinator
        let response = FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            error_message: None,
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            coordinators: vec![],
        };

        response.encode(header.api_version)
    }

    fn handle_produce(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        let request = ProduceRequest::parse(body, header.api_version)?;

        // Build response - in a real implementation this would route to the workload
        let topics: Vec<ProduceResponseTopic> = request
            .topics
            .iter()
            .map(|t| ProduceResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| ProduceResponsePartition {
                        partition: p.partition,
                        error_code: ErrorCode::None,
                        base_offset: 0,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                        record_errors: vec![],
                        error_message: None,
                    })
                    .collect(),
            })
            .collect();

        let response = ProduceResponse {
            topics,
            throttle_time_ms: 0,
        };

        response.encode(header.api_version)
    }

    fn handle_fetch(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        let request = FetchRequest::parse(body, header.api_version)?;

        // Build empty response - would fetch from workload
        let topics: Vec<FetchResponseTopic> = request
            .topics
            .iter()
            .map(|t| FetchResponseTopic {
                topic_id: t.topic_id,
                topic: t.topic.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| FetchResponsePartition {
                        partition: p.partition,
                        error_code: ErrorCode::None,
                        high_watermark: 0,
                        last_stable_offset: 0,
                        log_start_offset: 0,
                        aborted_transactions: vec![],
                        preferred_read_replica: -1,
                        records: None,
                    })
                    .collect(),
            })
            .collect();

        let response = FetchResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            session_id: 0,
            topics,
        };

        response.encode(header.api_version)
    }

    fn handle_list_offsets(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        let request = ListOffsetsRequest::parse(body, header.api_version)?;

        let topics: Vec<ListOffsetsResponseTopic> = request
            .topics
            .iter()
            .map(|t| ListOffsetsResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| ListOffsetsResponsePartition {
                        partition: p.partition,
                        error_code: ErrorCode::None,
                        timestamp: -1,
                        offset: 0,
                        leader_epoch: 0,
                    })
                    .collect(),
            })
            .collect();

        let response = ListOffsetsResponse {
            throttle_time_ms: 0,
            topics,
        };

        response.encode(header.api_version)
    }

    fn handle_offset_commit(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        let request = OffsetCommitRequest::parse(body, header.api_version)?;

        let topics: Vec<OffsetCommitResponseTopic> = request
            .topics
            .iter()
            .map(|t| OffsetCommitResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| OffsetCommitResponsePartition {
                        partition: p.partition,
                        error_code: ErrorCode::None,
                    })
                    .collect(),
            })
            .collect();

        let response = OffsetCommitResponse {
            throttle_time_ms: 0,
            topics,
        };

        response.encode(header.api_version)
    }

    fn handle_offset_fetch(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        let request = OffsetFetchRequest::parse(body, header.api_version)?;

        let topics: Vec<OffsetFetchResponseTopic> = request
            .topics
            .as_ref()
            .map(|ts| {
                ts.iter()
                    .map(|t| OffsetFetchResponseTopic {
                        name: t.name.clone(),
                        partitions: t
                            .partitions
                            .iter()
                            .map(|&p| OffsetFetchResponsePartition {
                                partition: p,
                                committed_offset: -1,
                                committed_leader_epoch: -1,
                                metadata: None,
                                error_code: ErrorCode::None,
                            })
                            .collect(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        let response = OffsetFetchResponse {
            throttle_time_ms: 0,
            topics,
            error_code: ErrorCode::None,
            groups: vec![],
        };

        response.encode(header.api_version)
    }

    fn handle_join_group(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        let request = JoinGroupRequest::parse(body, header.api_version)?;

        // Generate member ID if empty
        let member_id = if request.member_id.is_empty() {
            format!("{}-{}", request.group_id, uuid::Uuid::new_v4())
        } else {
            request.member_id.clone()
        };

        // Track membership
        self.conn_state.group_memberships.insert(
            request.group_id.clone(),
            GroupMembership {
                group_id: request.group_id.clone(),
                member_id: member_id.clone(),
                generation_id: 1,
                last_heartbeat_ms: current_time_ms(),
            },
        );

        let response = JoinGroupResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            generation_id: 1,
            protocol_type: Some(request.protocol_type.clone()),
            protocol_name: request.protocols.first().map(|p| p.name.clone()),
            leader: member_id.clone(),
            skip_assignment: false,
            member_id: member_id.clone(),
            members: vec![JoinGroupResponseMember {
                member_id: member_id.clone(),
                group_instance_id: None,
                metadata: request
                    .protocols
                    .first()
                    .map(|p| p.metadata.clone())
                    .unwrap_or_default(),
            }],
        };

        response.encode(header.api_version)
    }

    fn handle_sync_group(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        let request = SyncGroupRequest::parse(body, header.api_version)?;

        // Get assignment for this member
        let assignment = request
            .assignments
            .iter()
            .find(|a| a.member_id == request.member_id)
            .map(|a| a.assignment.clone())
            .unwrap_or_default();

        let response = SyncGroupResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            protocol_type: request.protocol_type,
            protocol_name: request.protocol_name,
            assignment,
        };

        response.encode(header.api_version)
    }

    fn handle_heartbeat(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        let request = HeartbeatRequest::parse(body, header.api_version)?;

        // Update heartbeat timestamp
        if let Some(membership) = self.conn_state.group_memberships.get_mut(&request.group_id) {
            membership.last_heartbeat_ms = current_time_ms();
        }

        let response = HeartbeatResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
        };

        response.encode(header.api_version)
    }

    fn handle_leave_group(&mut self, header: &RequestHeader, body: &[u8]) -> io::Result<Vec<u8>> {
        let request = LeaveGroupRequest::parse(body, header.api_version)?;

        // Remove membership
        self.conn_state.group_memberships.remove(&request.group_id);

        let response = LeaveGroupResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            members: vec![],
        };

        response.encode(header.api_version)
    }

    fn handle_init_producer_id(
        &mut self,
        header: &RequestHeader,
        body: &[u8],
    ) -> io::Result<Vec<u8>> {
        let _request = InitProducerIdRequest::parse(body, header.api_version)?;

        // Allocate producer ID - use timestamp-based ID (would be from workload in prod)
        let producer_id = current_time_ms() as i64;

        let response = InitProducerIdResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            producer_id,
            producer_epoch: 0,
        };

        response.encode(header.api_version)
    }

    fn handle_unsupported(&mut self, _header: &RequestHeader) -> io::Result<Vec<u8>> {
        // Return error response
        let mut buf = Vec::new();
        use super::protocol::types::write_int16;
        write_int16(&mut buf, ErrorCode::UnsupportedVersion.as_i16())?;
        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// Helper Functions
// ---------------------------------------------------------------------------

/// Parse PLAIN SASL authentication.
/// Format: \0username\0password
/// Username format: tenant/user
fn parse_plain_auth(auth_bytes: &[u8]) -> Option<(String, String)> {
    // Skip leading null byte
    let data = if auth_bytes.first() == Some(&0) {
        &auth_bytes[1..]
    } else {
        auth_bytes
    };

    // Split on null bytes
    let parts: Vec<&[u8]> = data.split(|&b| b == 0).collect();
    if parts.len() >= 2 {
        let username = String::from_utf8_lossy(parts[0]).to_string();
        // Password is parts[1], but we don't validate it here

        // Extract tenant from username (format: tenant/user or just user)
        let (tenant_id, _user) = if let Some(pos) = username.find('/') {
            (username[..pos].to_string(), username[pos + 1..].to_string())
        } else {
            ("default".to_string(), username.clone())
        };

        Some((username, tenant_id))
    } else {
        None
    }
}

/// Get current time in milliseconds.
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Connection Handler (for use with async runtime)
// ---------------------------------------------------------------------------

/// Handle a Kafka connection.
pub async fn handle_kafka_connection<S>(mut stream: S, _tenant_id: Option<String>) -> io::Result<()>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let mut ctx = KafkaRequestContext::new();
    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        // Read more data
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            // Connection closed
            break;
        }

        ctx.conn_state.metrics.bytes_in += n as u64;

        // Try to decode frames
        while let Some(frame) = ctx.decoder.decode(&mut buf)? {
            // Process the request
            let response = ctx.process_request(&frame)?;

            // Write response
            ctx.conn_state.metrics.bytes_out += response.len() as u64;
            stream.write_all(&response).await?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Kafka Protocol Plugin (stub for ProtocolPlugin trait)
// ---------------------------------------------------------------------------

/// Kafka protocol plugin.
#[derive(Debug, Clone)]
pub struct KafkaPlugin {
    /// Protocol descriptor.
    pub descriptor: KafkaProtocolDescriptor,
}

impl Default for KafkaPlugin {
    fn default() -> Self {
        Self {
            descriptor: KAFKA_PROTOCOL_DESCRIPTOR,
        }
    }
}

impl KafkaPlugin {
    /// Create a new Kafka plugin.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the protocol descriptor.
    pub fn descriptor(&self) -> &KafkaProtocolDescriptor {
        &self.descriptor
    }

    /// Get supported ALPNs.
    pub fn alpns(&self) -> &[&'static str] {
        self.descriptor.supported_alpns
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_plugin_default() {
        let plugin = KafkaPlugin::default();
        assert_eq!(plugin.descriptor().name, "kafka");
        assert_eq!(plugin.descriptor().default_port, 9092);
    }

    #[test]
    fn test_connection_state_default() {
        let state = KafkaConnectionState::default();
        assert!(state.client_id.is_none());
        assert!(matches!(state.auth_state, AuthState::Unauthenticated));
    }

    #[test]
    fn test_parse_plain_auth() {
        // Format: \0username\0password
        let auth = b"\0tenant/user\0password";
        let result = parse_plain_auth(auth);
        assert!(result.is_some());

        let (username, tenant_id) = result.unwrap();
        assert_eq!(username, "tenant/user");
        assert_eq!(tenant_id, "tenant");
    }

    #[test]
    fn test_parse_plain_auth_no_tenant() {
        let auth = b"\0user\0password";
        let result = parse_plain_auth(auth);
        assert!(result.is_some());

        let (username, tenant_id) = result.unwrap();
        assert_eq!(username, "user");
        assert_eq!(tenant_id, "default");
    }

    #[test]
    fn test_request_context_api_versions() {
        let mut ctx = KafkaRequestContext::new();

        // Build ApiVersions request
        let mut request = Vec::new();
        request.extend_from_slice(&(18_i16).to_be_bytes()); // api_key
        request.extend_from_slice(&(0_i16).to_be_bytes()); // api_version
        request.extend_from_slice(&(1_i32).to_be_bytes()); // correlation_id
        request.extend_from_slice(&(-1_i16).to_be_bytes()); // client_id (null)

        let response = ctx.process_request(&request).unwrap();
        assert!(!response.is_empty());
    }

    #[test]
    fn test_request_context_sasl_handshake() {
        let mut ctx = KafkaRequestContext::new();

        // Build SaslHandshake request
        let mut request = Vec::new();
        request.extend_from_slice(&(17_i16).to_be_bytes()); // api_key
        request.extend_from_slice(&(0_i16).to_be_bytes()); // api_version
        request.extend_from_slice(&(1_i32).to_be_bytes()); // correlation_id
        request.extend_from_slice(&(-1_i16).to_be_bytes()); // client_id (null)
                                                            // mechanism string
        let mechanism = "PLAIN";
        request.extend_from_slice(&(mechanism.len() as i16).to_be_bytes());
        request.extend_from_slice(mechanism.as_bytes());

        let response = ctx.process_request(&request).unwrap();
        assert!(!response.is_empty());

        // Check auth state changed
        assert!(matches!(
            ctx.conn_state.auth_state,
            AuthState::SaslHandshake { .. }
        ));
    }

    #[test]
    fn test_connection_metrics() {
        let mut metrics = ConnectionMetrics::default();
        assert_eq!(metrics.requests_total, 0);

        metrics.requests_total += 1;
        *metrics.requests_by_api.entry(ApiKey::Metadata).or_insert(0) += 1;

        assert_eq!(metrics.requests_total, 1);
        assert_eq!(metrics.requests_by_api.get(&ApiKey::Metadata), Some(&1));
    }

    #[test]
    fn test_protocol_descriptor() {
        assert_eq!(KAFKA_PROTOCOL_DESCRIPTOR.name, "kafka");
        assert_eq!(KAFKA_PROTOCOL_DESCRIPTOR.default_port, 9092);
        assert!(KAFKA_PROTOCOL_DESCRIPTOR.supported_alpns.contains(&"kafka"));
    }
}
