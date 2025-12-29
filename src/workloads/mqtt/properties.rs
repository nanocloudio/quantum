//! MQTT 5.0 property handling.
//!
//! This module implements MQTT 5.0 properties as defined in the specification,
//! including request/response correlation, subscription identifiers, and
//! payload format indicators.
//!
//! Tasks 14-16, 21-22: Request/response, subscription identifiers, payload format.

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// MQTT 5.0 Property Types
// ---------------------------------------------------------------------------

/// MQTT 5.0 property identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PropertyId {
    PayloadFormatIndicator = 0x01,
    MessageExpiryInterval = 0x02,
    ContentType = 0x03,
    ResponseTopic = 0x08,
    CorrelationData = 0x09,
    SubscriptionIdentifier = 0x0B,
    SessionExpiryInterval = 0x11,
    AssignedClientIdentifier = 0x12,
    ServerKeepAlive = 0x13,
    AuthenticationMethod = 0x15,
    AuthenticationData = 0x16,
    RequestProblemInformation = 0x17,
    WillDelayInterval = 0x18,
    RequestResponseInformation = 0x19,
    ResponseInformation = 0x1A,
    ServerReference = 0x1C,
    ReasonString = 0x1F,
    ReceiveMaximum = 0x21,
    TopicAliasMaximum = 0x22,
    TopicAlias = 0x23,
    MaximumQoS = 0x24,
    RetainAvailable = 0x25,
    UserProperty = 0x26,
    MaximumPacketSize = 0x27,
    WildcardSubscriptionAvailable = 0x28,
    SubscriptionIdentifierAvailable = 0x29,
    SharedSubscriptionAvailable = 0x2A,
}

impl PropertyId {
    /// Try to parse a property ID from a byte.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x01 => Some(Self::PayloadFormatIndicator),
            0x02 => Some(Self::MessageExpiryInterval),
            0x03 => Some(Self::ContentType),
            0x08 => Some(Self::ResponseTopic),
            0x09 => Some(Self::CorrelationData),
            0x0B => Some(Self::SubscriptionIdentifier),
            0x11 => Some(Self::SessionExpiryInterval),
            0x12 => Some(Self::AssignedClientIdentifier),
            0x13 => Some(Self::ServerKeepAlive),
            0x15 => Some(Self::AuthenticationMethod),
            0x16 => Some(Self::AuthenticationData),
            0x17 => Some(Self::RequestProblemInformation),
            0x18 => Some(Self::WillDelayInterval),
            0x19 => Some(Self::RequestResponseInformation),
            0x1A => Some(Self::ResponseInformation),
            0x1C => Some(Self::ServerReference),
            0x1F => Some(Self::ReasonString),
            0x21 => Some(Self::ReceiveMaximum),
            0x22 => Some(Self::TopicAliasMaximum),
            0x23 => Some(Self::TopicAlias),
            0x24 => Some(Self::MaximumQoS),
            0x25 => Some(Self::RetainAvailable),
            0x26 => Some(Self::UserProperty),
            0x27 => Some(Self::MaximumPacketSize),
            0x28 => Some(Self::WildcardSubscriptionAvailable),
            0x29 => Some(Self::SubscriptionIdentifierAvailable),
            0x2A => Some(Self::SharedSubscriptionAvailable),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Request/Response Correlation (Task 14)
// ---------------------------------------------------------------------------

/// Request/response correlation data.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RequestResponseInfo {
    /// Response topic for request/response pattern.
    pub response_topic: Option<String>,
    /// Correlation data for request/response pattern.
    pub correlation_data: Option<Vec<u8>>,
}

impl RequestResponseInfo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_response_topic(mut self, topic: String) -> Self {
        self.response_topic = Some(topic);
        self
    }

    pub fn with_correlation_data(mut self, data: Vec<u8>) -> Self {
        self.correlation_data = Some(data);
        self
    }

    /// Check if this contains request/response information.
    pub fn is_request(&self) -> bool {
        self.response_topic.is_some()
    }

    /// Check if this contains correlation data.
    pub fn has_correlation(&self) -> bool {
        self.correlation_data.is_some()
    }
}

/// Pending request tracker for request/response pattern.
#[derive(Debug, Clone)]
pub struct RequestTracker {
    /// Pending requests keyed by correlation data.
    pending: HashMap<Vec<u8>, PendingRequest>,
    /// Maximum pending requests.
    max_pending: usize,
    /// Timeout for pending requests in seconds.
    timeout_secs: u64,
}

/// A pending request awaiting response.
#[derive(Debug, Clone)]
pub struct PendingRequest {
    /// Original request's response topic.
    pub response_topic: String,
    /// Correlation data.
    pub correlation_data: Vec<u8>,
    /// Request timestamp (Unix epoch seconds).
    pub created_at: u64,
    /// Client ID that made the request.
    pub client_id: String,
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestTracker {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
            max_pending: 1000,
            timeout_secs: 300, // 5 minutes default
        }
    }

    pub fn with_max_pending(mut self, max: usize) -> Self {
        self.max_pending = max;
        self
    }

    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    /// Track a new request.
    pub fn track(
        &mut self,
        correlation_data: Vec<u8>,
        response_topic: String,
        client_id: String,
    ) -> Result<(), RequestResponseError> {
        if self.pending.len() >= self.max_pending {
            return Err(RequestResponseError::TooManyPending {
                current: self.pending.len(),
                max: self.max_pending,
            });
        }

        let request = PendingRequest {
            response_topic,
            correlation_data: correlation_data.clone(),
            created_at: current_unix_timestamp(),
            client_id,
        };

        self.pending.insert(correlation_data, request);
        Ok(())
    }

    /// Get pending request by correlation data.
    pub fn get(&self, correlation_data: &[u8]) -> Option<&PendingRequest> {
        self.pending.get(correlation_data)
    }

    /// Remove and return a pending request.
    pub fn remove(&mut self, correlation_data: &[u8]) -> Option<PendingRequest> {
        self.pending.remove(correlation_data)
    }

    /// Clean up expired requests.
    pub fn cleanup_expired(&mut self) -> Vec<PendingRequest> {
        let now = current_unix_timestamp();
        let mut expired = Vec::new();

        self.pending.retain(|_, request| {
            let age = now.saturating_sub(request.created_at);
            if age >= self.timeout_secs {
                expired.push(request.clone());
                false
            } else {
                true
            }
        });

        expired
    }

    /// Get pending count.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Clear all pending requests.
    pub fn clear(&mut self) -> Vec<PendingRequest> {
        self.pending.drain().map(|(_, v)| v).collect()
    }
}

/// Errors for request/response operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestResponseError {
    /// Too many pending requests.
    TooManyPending { current: usize, max: usize },
    /// Request not found.
    NotFound,
    /// Request expired.
    Expired { correlation_data: Vec<u8> },
}

impl std::fmt::Display for RequestResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyPending { current, max } => {
                write!(f, "too many pending requests: {current}/{max}")
            }
            Self::NotFound => write!(f, "request not found"),
            Self::Expired { .. } => write!(f, "request expired"),
        }
    }
}

impl std::error::Error for RequestResponseError {}

// ---------------------------------------------------------------------------
// Subscription Identifiers (Task 15)
// ---------------------------------------------------------------------------

/// Subscription identifier for tracking which subscriptions matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SubscriptionId(pub u32);

impl SubscriptionId {
    /// Create a new subscription identifier.
    pub fn new(id: u32) -> Option<Self> {
        // Subscription identifiers must be 1 to 268,435,455 (variable byte integer)
        if id > 0 && id <= 268_435_455 {
            Some(Self(id))
        } else {
            None
        }
    }

    /// Get the raw identifier value.
    pub fn value(&self) -> u32 {
        self.0
    }
}

/// Tracks subscription identifiers for a session.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionIdTracker {
    /// Filter -> Subscription ID mapping.
    filter_to_id: HashMap<String, SubscriptionId>,
    /// ID -> Filter mapping for reverse lookup.
    id_to_filter: HashMap<SubscriptionId, String>,
}

impl SubscriptionIdTracker {
    pub fn new() -> Self {
        Self {
            filter_to_id: HashMap::new(),
            id_to_filter: HashMap::new(),
        }
    }

    /// Register a subscription identifier for a filter.
    pub fn register(&mut self, filter: String, id: SubscriptionId) {
        // Remove old mapping if exists
        if let Some(old_id) = self.filter_to_id.remove(&filter) {
            self.id_to_filter.remove(&old_id);
        }
        if let Some(old_filter) = self.id_to_filter.remove(&id) {
            self.filter_to_id.remove(&old_filter);
        }

        self.filter_to_id.insert(filter.clone(), id);
        self.id_to_filter.insert(id, filter);
    }

    /// Get subscription ID for a filter.
    pub fn get(&self, filter: &str) -> Option<SubscriptionId> {
        self.filter_to_id.get(filter).copied()
    }

    /// Get filter for a subscription ID.
    pub fn get_filter(&self, id: SubscriptionId) -> Option<&str> {
        self.id_to_filter.get(&id).map(|s| s.as_str())
    }

    /// Remove subscription ID for a filter.
    pub fn remove(&mut self, filter: &str) -> Option<SubscriptionId> {
        if let Some(id) = self.filter_to_id.remove(filter) {
            self.id_to_filter.remove(&id);
            Some(id)
        } else {
            None
        }
    }

    /// Get all subscription IDs for matching filters.
    ///
    /// Given a topic, find all subscriptions that match it and return their IDs.
    pub fn matching_ids<F>(&self, topic: &str, matcher: F) -> Vec<SubscriptionId>
    where
        F: Fn(&str, &str) -> bool,
    {
        self.filter_to_id
            .iter()
            .filter(|(filter, _)| matcher(filter, topic))
            .map(|(_, id)| *id)
            .collect()
    }

    /// Clear all subscriptions.
    pub fn clear(&mut self) {
        self.filter_to_id.clear();
        self.id_to_filter.clear();
    }

    /// Get count of tracked subscriptions.
    pub fn len(&self) -> usize {
        self.filter_to_id.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.filter_to_id.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Payload Format (Task 22)
// ---------------------------------------------------------------------------

/// Payload format indicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum PayloadFormat {
    /// Unspecified bytes (default).
    #[default]
    Unspecified,
    /// UTF-8 encoded string.
    Utf8,
}

impl PayloadFormat {
    /// Create from MQTT property byte value.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Utf8),
            _ => None,
        }
    }

    /// Convert to MQTT property byte value.
    pub fn to_byte(&self) -> u8 {
        match self {
            Self::Unspecified => 0,
            Self::Utf8 => 1,
        }
    }

    /// Check if the payload is valid for this format.
    pub fn validate(&self, payload: &[u8]) -> bool {
        match self {
            Self::Unspecified => true,
            Self::Utf8 => std::str::from_utf8(payload).is_ok(),
        }
    }
}

/// Content type for MQTT 5.0 messages.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ContentType(pub Option<String>);

impl ContentType {
    pub fn new(content_type: String) -> Self {
        Self(Some(content_type))
    }

    pub fn none() -> Self {
        Self(None)
    }

    pub fn is_json(&self) -> bool {
        self.0
            .as_ref()
            .map(|ct| ct.contains("application/json"))
            .unwrap_or(false)
    }

    pub fn is_text(&self) -> bool {
        self.0
            .as_ref()
            .map(|ct| ct.starts_with("text/"))
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// User Properties
// ---------------------------------------------------------------------------

/// User properties for MQTT 5.0.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct UserProperties {
    properties: Vec<(String, String)>,
}

impl UserProperties {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a user property.
    pub fn add(&mut self, key: String, value: String) {
        self.properties.push((key, value));
    }

    /// Get all values for a key (multiple values allowed).
    pub fn get_all(&self, key: &str) -> Vec<&str> {
        self.properties
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect()
    }

    /// Get first value for a key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Get all properties.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Get property count.
    pub fn len(&self) -> usize {
        self.properties.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }

    /// Clear all properties.
    pub fn clear(&mut self) {
        self.properties.clear();
    }
}

// ---------------------------------------------------------------------------
// Publish Properties Aggregate
// ---------------------------------------------------------------------------

/// All MQTT 5.0 publish properties aggregated.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PublishProperties {
    /// Payload format indicator.
    pub payload_format: PayloadFormat,
    /// Message expiry interval in seconds.
    pub message_expiry_interval: Option<u32>,
    /// Topic alias.
    pub topic_alias: Option<u16>,
    /// Response topic.
    pub response_topic: Option<String>,
    /// Correlation data.
    pub correlation_data: Option<Vec<u8>>,
    /// User properties.
    pub user_properties: UserProperties,
    /// Subscription identifiers (populated when delivering).
    pub subscription_identifiers: Vec<SubscriptionId>,
    /// Content type.
    pub content_type: ContentType,
}

impl PublishProperties {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from request/response info.
    pub fn from_request_response(info: &RequestResponseInfo) -> Self {
        let mut props = Self::new();
        props.response_topic = info.response_topic.clone();
        props.correlation_data = info.correlation_data.clone();
        props
    }

    /// Extract request/response info.
    pub fn to_request_response(&self) -> RequestResponseInfo {
        RequestResponseInfo {
            response_topic: self.response_topic.clone(),
            correlation_data: self.correlation_data.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn current_unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_id_from_byte() {
        assert_eq!(
            PropertyId::from_byte(0x01),
            Some(PropertyId::PayloadFormatIndicator)
        );
        assert_eq!(
            PropertyId::from_byte(0x09),
            Some(PropertyId::CorrelationData)
        );
        assert_eq!(PropertyId::from_byte(0xFF), None);
    }

    #[test]
    fn test_request_response_info() {
        let info = RequestResponseInfo::new()
            .with_response_topic("response/topic".to_string())
            .with_correlation_data(b"corr123".to_vec());

        assert!(info.is_request());
        assert!(info.has_correlation());
        assert_eq!(info.response_topic, Some("response/topic".to_string()));
    }

    #[test]
    fn test_request_tracker() {
        let mut tracker = RequestTracker::new().with_max_pending(10);

        tracker
            .track(
                b"corr1".to_vec(),
                "response/topic".to_string(),
                "client1".to_string(),
            )
            .unwrap();

        assert_eq!(tracker.pending_count(), 1);
        assert!(tracker.get(b"corr1").is_some());

        let request = tracker.remove(b"corr1").unwrap();
        assert_eq!(request.response_topic, "response/topic");
        assert_eq!(tracker.pending_count(), 0);
    }

    #[test]
    fn test_request_tracker_max_pending() {
        let mut tracker = RequestTracker::new().with_max_pending(2);

        tracker
            .track(b"c1".to_vec(), "r".to_string(), "client".to_string())
            .unwrap();
        tracker
            .track(b"c2".to_vec(), "r".to_string(), "client".to_string())
            .unwrap();

        let result = tracker.track(b"c3".to_vec(), "r".to_string(), "client".to_string());
        assert!(matches!(
            result,
            Err(RequestResponseError::TooManyPending { .. })
        ));
    }

    #[test]
    fn test_subscription_id() {
        assert!(SubscriptionId::new(0).is_none()); // 0 is invalid
        assert!(SubscriptionId::new(1).is_some());
        assert!(SubscriptionId::new(268_435_455).is_some());
        assert!(SubscriptionId::new(268_435_456).is_none()); // Too large
    }

    #[test]
    fn test_subscription_id_tracker() {
        let mut tracker = SubscriptionIdTracker::new();

        let id = SubscriptionId::new(1).unwrap();
        tracker.register("sensors/#".to_string(), id);

        assert_eq!(tracker.get("sensors/#"), Some(id));
        assert_eq!(tracker.get_filter(id), Some("sensors/#"));
        assert_eq!(tracker.len(), 1);

        tracker.remove("sensors/#");
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_payload_format() {
        assert_eq!(
            PayloadFormat::from_byte(0),
            Some(PayloadFormat::Unspecified)
        );
        assert_eq!(PayloadFormat::from_byte(1), Some(PayloadFormat::Utf8));
        assert_eq!(PayloadFormat::from_byte(2), None);

        assert!(PayloadFormat::Unspecified.validate(&[0xFF, 0xFE]));
        assert!(PayloadFormat::Utf8.validate(b"hello"));
        assert!(!PayloadFormat::Utf8.validate(&[0xFF, 0xFE])); // Invalid UTF-8
    }

    #[test]
    fn test_content_type() {
        let json = ContentType::new("application/json".to_string());
        assert!(json.is_json());
        assert!(!json.is_text());

        let text = ContentType::new("text/plain".to_string());
        assert!(!text.is_json());
        assert!(text.is_text());
    }

    #[test]
    fn test_user_properties() {
        let mut props = UserProperties::new();
        props.add("key1".to_string(), "value1".to_string());
        props.add("key1".to_string(), "value2".to_string());
        props.add("key2".to_string(), "value3".to_string());

        assert_eq!(props.get("key1"), Some("value1")); // First value
        assert_eq!(props.get_all("key1"), vec!["value1", "value2"]);
        assert_eq!(props.len(), 3);
    }

    #[test]
    fn test_publish_properties() {
        let props = PublishProperties {
            payload_format: PayloadFormat::Utf8,
            message_expiry_interval: Some(300),
            response_topic: Some("response/topic".to_string()),
            correlation_data: Some(b"corr123".to_vec()),
            ..Default::default()
        };

        let info = props.to_request_response();
        assert_eq!(info.response_topic, Some("response/topic".to_string()));

        let props2 = PublishProperties::from_request_response(&info);
        assert_eq!(props2.response_topic, props.response_topic);
    }
}
