//! MQTT session lifecycle management.
//!
//! This module implements session lifecycle features for MQTT 5.0:
//! - Server-initiated disconnect with reason codes
//! - Assigned client identifier generation
//! - Session takeover handling
//!
//! Tasks 17-19: Server-initiated disconnect, assigned client identifier.

use std::collections::HashMap;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Server-Initiated Disconnect (Task 17)
// ---------------------------------------------------------------------------

/// Disconnect reason codes as defined in MQTT 5.0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DisconnectReasonCode {
    /// Normal disconnection.
    NormalDisconnection = 0x00,
    /// Disconnect with Will Message.
    DisconnectWithWillMessage = 0x04,
    /// Unspecified error.
    UnspecifiedError = 0x80,
    /// Malformed Packet.
    MalformedPacket = 0x81,
    /// Protocol Error.
    ProtocolError = 0x82,
    /// Implementation specific error.
    ImplementationSpecificError = 0x83,
    /// Not authorized.
    NotAuthorized = 0x87,
    /// Server busy.
    ServerBusy = 0x89,
    /// Server shutting down.
    ServerShuttingDown = 0x8B,
    /// Keep Alive timeout.
    KeepAliveTimeout = 0x8D,
    /// Session taken over.
    SessionTakenOver = 0x8E,
    /// Topic Filter invalid.
    TopicFilterInvalid = 0x8F,
    /// Topic Name invalid.
    TopicNameInvalid = 0x90,
    /// Receive Maximum exceeded.
    ReceiveMaximumExceeded = 0x93,
    /// Topic Alias invalid.
    TopicAliasInvalid = 0x94,
    /// Packet too large.
    PacketTooLarge = 0x95,
    /// Message rate too high.
    MessageRateTooHigh = 0x96,
    /// Quota exceeded.
    QuotaExceeded = 0x97,
    /// Administrative action.
    AdministrativeAction = 0x98,
    /// Payload format invalid.
    PayloadFormatInvalid = 0x99,
    /// Retain not supported.
    RetainNotSupported = 0x9A,
    /// QoS not supported.
    QosNotSupported = 0x9B,
    /// Use another server.
    UseAnotherServer = 0x9C,
    /// Server moved.
    ServerMoved = 0x9D,
    /// Shared Subscriptions not supported.
    SharedSubscriptionsNotSupported = 0x9E,
    /// Connection rate exceeded.
    ConnectionRateExceeded = 0x9F,
    /// Maximum connect time.
    MaximumConnectTime = 0xA0,
    /// Subscription Identifiers not supported.
    SubscriptionIdentifiersNotSupported = 0xA1,
    /// Wildcard Subscriptions not supported.
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl DisconnectReasonCode {
    /// Get the reason string for this disconnect code.
    pub fn reason_string(&self) -> &'static str {
        match self {
            Self::NormalDisconnection => "Normal disconnection",
            Self::DisconnectWithWillMessage => "Disconnect with Will Message",
            Self::UnspecifiedError => "Unspecified error",
            Self::MalformedPacket => "Malformed Packet",
            Self::ProtocolError => "Protocol Error",
            Self::ImplementationSpecificError => "Implementation specific error",
            Self::NotAuthorized => "Not authorized",
            Self::ServerBusy => "Server busy",
            Self::ServerShuttingDown => "Server shutting down",
            Self::KeepAliveTimeout => "Keep Alive timeout",
            Self::SessionTakenOver => "Session taken over",
            Self::TopicFilterInvalid => "Topic Filter invalid",
            Self::TopicNameInvalid => "Topic Name invalid",
            Self::ReceiveMaximumExceeded => "Receive Maximum exceeded",
            Self::TopicAliasInvalid => "Topic Alias invalid",
            Self::PacketTooLarge => "Packet too large",
            Self::MessageRateTooHigh => "Message rate too high",
            Self::QuotaExceeded => "Quota exceeded",
            Self::AdministrativeAction => "Administrative action",
            Self::PayloadFormatInvalid => "Payload format invalid",
            Self::RetainNotSupported => "Retain not supported",
            Self::QosNotSupported => "QoS not supported",
            Self::UseAnotherServer => "Use another server",
            Self::ServerMoved => "Server moved",
            Self::SharedSubscriptionsNotSupported => "Shared Subscriptions not supported",
            Self::ConnectionRateExceeded => "Connection rate exceeded",
            Self::MaximumConnectTime => "Maximum connect time",
            Self::SubscriptionIdentifiersNotSupported => "Subscription Identifiers not supported",
            Self::WildcardSubscriptionsNotSupported => "Wildcard Subscriptions not supported",
        }
    }

    /// Check if this is a success code.
    pub fn is_success(&self) -> bool {
        (*self as u8) < 0x80
    }

    /// Check if this is an error code.
    pub fn is_error(&self) -> bool {
        (*self as u8) >= 0x80
    }
}

/// Server-initiated disconnect request.
#[derive(Debug, Clone)]
pub struct DisconnectRequest {
    /// Reason code.
    pub reason: DisconnectReasonCode,
    /// Optional reason string.
    pub reason_string: Option<String>,
    /// Optional server reference (for UseAnotherServer/ServerMoved).
    pub server_reference: Option<String>,
    /// Session expiry interval override.
    pub session_expiry_interval: Option<u32>,
}

impl DisconnectRequest {
    pub fn new(reason: DisconnectReasonCode) -> Self {
        Self {
            reason,
            reason_string: None,
            server_reference: None,
            session_expiry_interval: None,
        }
    }

    pub fn with_reason_string(mut self, reason: String) -> Self {
        self.reason_string = Some(reason);
        self
    }

    pub fn with_server_reference(mut self, reference: String) -> Self {
        self.server_reference = Some(reference);
        self
    }

    pub fn with_session_expiry(mut self, interval: u32) -> Self {
        self.session_expiry_interval = Some(interval);
        self
    }

    /// Create a disconnect for server shutdown.
    pub fn server_shutdown() -> Self {
        Self::new(DisconnectReasonCode::ServerShuttingDown)
    }

    /// Create a disconnect for session takeover.
    pub fn session_takeover() -> Self {
        Self::new(DisconnectReasonCode::SessionTakenOver)
    }

    /// Create a disconnect for keep-alive timeout.
    pub fn keep_alive_timeout() -> Self {
        Self::new(DisconnectReasonCode::KeepAliveTimeout)
    }

    /// Create a disconnect for quota exceeded.
    pub fn quota_exceeded() -> Self {
        Self::new(DisconnectReasonCode::QuotaExceeded)
    }

    /// Create a disconnect for administrative action.
    pub fn administrative_action(reason: String) -> Self {
        Self::new(DisconnectReasonCode::AdministrativeAction).with_reason_string(reason)
    }

    /// Create a disconnect to redirect to another server.
    pub fn use_another_server(server: String) -> Self {
        Self::new(DisconnectReasonCode::UseAnotherServer).with_server_reference(server)
    }
}

// ---------------------------------------------------------------------------
// Assigned Client Identifier (Task 18)
// ---------------------------------------------------------------------------

/// Client identifier generator.
#[derive(Debug, Clone)]
pub struct ClientIdGenerator {
    /// Prefix for generated client IDs.
    prefix: String,
    /// Counter for uniqueness.
    counter: u64,
    /// Node identifier for distributed uniqueness.
    node_id: u16,
}

impl Default for ClientIdGenerator {
    fn default() -> Self {
        Self::new("auto")
    }
}

impl ClientIdGenerator {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            counter: 0,
            node_id: 0,
        }
    }

    pub fn with_node_id(mut self, node_id: u16) -> Self {
        self.node_id = node_id;
        self
    }

    /// Generate a new unique client identifier.
    ///
    /// Format: {prefix}-{node_id}-{timestamp_hex}-{counter_hex}
    pub fn generate(&mut self) -> String {
        self.counter = self.counter.wrapping_add(1);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        format!(
            "{}-{:04x}-{:012x}-{:08x}",
            self.prefix, self.node_id, timestamp, self.counter
        )
    }

    /// Validate a client identifier.
    ///
    /// Per MQTT spec:
    /// - Must be 1-23 characters for MQTT 3.1
    /// - Can be longer in MQTT 3.1.1 and 5.0
    /// - Should only contain alphanumeric characters (relaxed in 5.0)
    pub fn validate(client_id: &str) -> ClientIdValidation {
        if client_id.is_empty() {
            return ClientIdValidation::Empty;
        }

        // Check length - max 65535 bytes in MQTT 5.0
        if client_id.len() > 65535 {
            return ClientIdValidation::TooLong(client_id.len());
        }

        // Check for invalid characters (strict mode)
        let has_invalid = client_id
            .chars()
            .any(|c| !c.is_ascii_alphanumeric() && c != '-' && c != '_' && c != '.');

        if has_invalid {
            ClientIdValidation::InvalidCharacters
        } else {
            ClientIdValidation::Valid
        }
    }

    /// Check if a client ID was generated by this generator.
    pub fn is_generated(&self, client_id: &str) -> bool {
        client_id.starts_with(&format!("{}-", self.prefix))
    }
}

/// Result of client ID validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientIdValidation {
    /// Client ID is valid.
    Valid,
    /// Client ID is empty (needs assignment).
    Empty,
    /// Client ID is too long.
    TooLong(usize),
    /// Client ID contains invalid characters.
    InvalidCharacters,
}

impl ClientIdValidation {
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }

    pub fn needs_assignment(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

// ---------------------------------------------------------------------------
// Session Takeover (Task 19)
// ---------------------------------------------------------------------------

/// Session state for takeover handling.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// Client identifier.
    pub client_id: String,
    /// Whether this is a clean session.
    pub clean_start: bool,
    /// Session expiry interval in seconds.
    pub session_expiry_interval: u32,
    /// Connection timestamp.
    pub connected_at: Instant,
    /// Keep-alive interval in seconds.
    pub keep_alive: u16,
    /// Whether the session is currently connected.
    pub is_connected: bool,
    /// Protocol version (3 = 3.1, 4 = 3.1.1, 5 = 5.0).
    pub protocol_version: u8,
}

impl SessionInfo {
    pub fn new(client_id: String, clean_start: bool, session_expiry_interval: u32) -> Self {
        Self {
            client_id,
            clean_start,
            session_expiry_interval,
            connected_at: Instant::now(),
            keep_alive: 0,
            is_connected: true,
            protocol_version: 5,
        }
    }

    /// Check if the session should persist after disconnect.
    pub fn session_persists(&self) -> bool {
        // In MQTT 5.0, session persists if session_expiry_interval > 0
        // In MQTT 3.1.1, clean_session = false means persist
        if self.protocol_version >= 5 {
            self.session_expiry_interval > 0
        } else {
            !self.clean_start
        }
    }

    /// Get session age in seconds.
    pub fn age_secs(&self) -> u64 {
        self.connected_at.elapsed().as_secs()
    }
}

/// Manages session takeover operations.
#[derive(Debug, Clone, Default)]
pub struct SessionTakeoverManager {
    /// Active sessions by client ID.
    sessions: HashMap<String, SessionInfo>,
    /// Pending takeove operations.
    pending_takevers: HashMap<String, TakeoverOperation>,
}

/// A pending takeover operation.
#[derive(Debug, Clone)]
pub struct TakeoverOperation {
    /// Client ID being taken over.
    pub client_id: String,
    /// New session info.
    pub new_session: SessionInfo,
    /// When the takeover was initiated.
    pub initiated_at: Instant,
}

impl SessionTakeoverManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a client ID has an active session.
    pub fn has_session(&self, client_id: &str) -> bool {
        self.sessions
            .get(client_id)
            .map(|s| s.is_connected)
            .unwrap_or(false)
    }

    /// Get session info for a client.
    pub fn get_session(&self, client_id: &str) -> Option<&SessionInfo> {
        self.sessions.get(client_id)
    }

    /// Register a new session.
    ///
    /// Returns Some(disconnect_request) if the existing session needs to be disconnected.
    pub fn register_session(&mut self, session: SessionInfo) -> Option<DisconnectRequest> {
        let client_id = session.client_id.clone();

        // Check for existing connected session
        if let Some(existing) = self.sessions.get(&client_id) {
            if existing.is_connected {
                // Need to takeover
                let disconnect = DisconnectRequest::session_takeover();

                // Track pending takeover
                self.pending_takevers.insert(
                    client_id.clone(),
                    TakeoverOperation {
                        client_id: client_id.clone(),
                        new_session: session,
                        initiated_at: Instant::now(),
                    },
                );

                return Some(disconnect);
            }
        }

        // No takeover needed, register directly
        self.sessions.insert(client_id, session);
        None
    }

    /// Complete a pending takeover after old session disconnects.
    pub fn complete_takeover(&mut self, client_id: &str) -> bool {
        if let Some(takeover) = self.pending_takevers.remove(client_id) {
            self.sessions
                .insert(client_id.to_string(), takeover.new_session);
            true
        } else {
            false
        }
    }

    /// Mark a session as disconnected.
    pub fn mark_disconnected(&mut self, client_id: &str) {
        if let Some(session) = self.sessions.get_mut(client_id) {
            session.is_connected = false;
        }

        // Check if there's a pending takeover
        self.complete_takeover(client_id);
    }

    /// Remove a session (clean session or expired).
    pub fn remove_session(&mut self, client_id: &str) -> Option<SessionInfo> {
        self.pending_takevers.remove(client_id);
        self.sessions.remove(client_id)
    }

    /// Get all active sessions.
    pub fn active_sessions(&self) -> impl Iterator<Item = &SessionInfo> {
        self.sessions.values().filter(|s| s.is_connected)
    }

    /// Get active session count.
    pub fn active_count(&self) -> usize {
        self.sessions.values().filter(|s| s.is_connected).count()
    }

    /// Get total session count (including disconnected persistent sessions).
    pub fn total_count(&self) -> usize {
        self.sessions.len()
    }

    /// Clean up expired disconnected sessions.
    pub fn cleanup_expired(&mut self) -> Vec<String> {
        let mut expired = Vec::new();

        self.sessions.retain(|client_id, session| {
            if !session.is_connected && !session.session_persists() {
                expired.push(client_id.clone());
                false
            } else {
                true
            }
        });

        expired
    }
}

// ---------------------------------------------------------------------------
// Keep-Alive Monitor
// ---------------------------------------------------------------------------

/// Monitors keep-alive timeouts for connected sessions.
#[derive(Debug, Clone, Default)]
pub struct KeepAliveMonitor {
    /// Last activity time for each client.
    last_activity: HashMap<String, Instant>,
    /// Keep-alive intervals for each client.
    keep_alive_intervals: HashMap<String, u16>,
    /// Default keep-alive multiplier (server uses 1.5x client's keep-alive).
    multiplier: f32,
}

impl KeepAliveMonitor {
    pub fn new() -> Self {
        Self {
            last_activity: HashMap::new(),
            keep_alive_intervals: HashMap::new(),
            multiplier: 1.5,
        }
    }

    pub fn with_multiplier(mut self, multiplier: f32) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Register a client with its keep-alive interval.
    pub fn register(&mut self, client_id: &str, keep_alive: u16) {
        self.last_activity
            .insert(client_id.to_string(), Instant::now());
        self.keep_alive_intervals
            .insert(client_id.to_string(), keep_alive);
    }

    /// Update last activity for a client.
    pub fn touch(&mut self, client_id: &str) {
        if let Some(activity) = self.last_activity.get_mut(client_id) {
            *activity = Instant::now();
        }
    }

    /// Remove a client from monitoring.
    pub fn unregister(&mut self, client_id: &str) {
        self.last_activity.remove(client_id);
        self.keep_alive_intervals.remove(client_id);
    }

    /// Check for timed-out clients and return their IDs.
    pub fn check_timeouts(&self) -> Vec<String> {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        for (client_id, last_activity) in &self.last_activity {
            if let Some(&keep_alive) = self.keep_alive_intervals.get(client_id) {
                if keep_alive == 0 {
                    continue; // No keep-alive
                }

                let timeout_secs = (keep_alive as f32 * self.multiplier) as u64;
                let elapsed = now.duration_since(*last_activity).as_secs();

                if elapsed > timeout_secs {
                    timed_out.push(client_id.clone());
                }
            }
        }

        timed_out
    }

    /// Get elapsed time since last activity for a client.
    pub fn elapsed_since_activity(&self, client_id: &str) -> Option<std::time::Duration> {
        self.last_activity.get(client_id).map(|last| last.elapsed())
    }

    /// Get monitored client count.
    pub fn client_count(&self) -> usize {
        self.last_activity.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disconnect_reason_codes() {
        assert!(DisconnectReasonCode::NormalDisconnection.is_success());
        assert!(!DisconnectReasonCode::NormalDisconnection.is_error());

        assert!(!DisconnectReasonCode::ProtocolError.is_success());
        assert!(DisconnectReasonCode::ProtocolError.is_error());
    }

    #[test]
    fn test_disconnect_request() {
        let req = DisconnectRequest::server_shutdown();
        assert_eq!(req.reason, DisconnectReasonCode::ServerShuttingDown);

        let req = DisconnectRequest::use_another_server("server2.example.com".to_string());
        assert_eq!(req.reason, DisconnectReasonCode::UseAnotherServer);
        assert_eq!(
            req.server_reference,
            Some("server2.example.com".to_string())
        );
    }

    #[test]
    fn test_client_id_generator() {
        let mut gen = ClientIdGenerator::new("test").with_node_id(1);

        let id1 = gen.generate();
        let id2 = gen.generate();

        assert!(id1.starts_with("test-0001-"));
        assert_ne!(id1, id2);
        assert!(gen.is_generated(&id1));
        assert!(!gen.is_generated("custom-client"));
    }

    #[test]
    fn test_client_id_validation() {
        assert!(ClientIdGenerator::validate("valid-client_123").is_valid());
        assert!(ClientIdGenerator::validate("").needs_assignment());
        assert!(matches!(
            ClientIdGenerator::validate("client with spaces"),
            ClientIdValidation::InvalidCharacters
        ));
    }

    #[test]
    fn test_session_info_persistence() {
        // MQTT 5.0 - persists based on session_expiry_interval
        let session = SessionInfo {
            client_id: "client1".to_string(),
            clean_start: true,
            session_expiry_interval: 300,
            connected_at: Instant::now(),
            keep_alive: 60,
            is_connected: true,
            protocol_version: 5,
        };
        assert!(session.session_persists());

        // MQTT 5.0 - doesn't persist if expiry is 0
        let session = SessionInfo {
            session_expiry_interval: 0,
            ..session
        };
        assert!(!session.session_persists());

        // MQTT 3.1.1 - persists based on clean_session
        let session = SessionInfo {
            clean_start: false,
            protocol_version: 4,
            ..session
        };
        assert!(session.session_persists());
    }

    #[test]
    fn test_session_takeover() {
        let mut manager = SessionTakeoverManager::new();

        // First session
        let session1 = SessionInfo::new("client1".to_string(), false, 300);
        let takeover = manager.register_session(session1);
        assert!(takeover.is_none());
        assert!(manager.has_session("client1"));

        // Second session with same client ID triggers takeover
        let session2 = SessionInfo::new("client1".to_string(), false, 300);
        let takeover = manager.register_session(session2);
        assert!(takeover.is_some());
        assert_eq!(
            takeover.unwrap().reason,
            DisconnectReasonCode::SessionTakenOver
        );

        // Complete takeover after old session disconnects
        manager.mark_disconnected("client1");
        assert!(manager.has_session("client1"));
    }

    #[test]
    fn test_keep_alive_monitor() {
        let mut monitor = KeepAliveMonitor::new().with_multiplier(1.5);

        monitor.register("client1", 10);
        monitor.touch("client1");

        // Should not timeout immediately
        let timeouts = monitor.check_timeouts();
        assert!(timeouts.is_empty());

        // Elapsed time should be small
        let elapsed = monitor.elapsed_since_activity("client1").unwrap();
        assert!(elapsed.as_secs() < 1);
    }
}
