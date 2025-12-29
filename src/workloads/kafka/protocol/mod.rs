//! Kafka wire protocol implementation.
//!
//! This module implements the Kafka wire protocol including:
//! - Request/response framing (4-byte length prefix)
//! - Header encoding/decoding
//! - Primitive type serialization (varint, compact strings, etc.)
//! - All supported API request/response types
//!
//! # Wire Format
//!
//! Kafka uses a simple framing protocol:
//! ```text
//! +----------------+------------------+
//! | Length (4B BE) | Payload          |
//! +----------------+------------------+
//! ```
//!
//! Request header (v0-v1):
//! ```text
//! +------------+-------------+----------------+-----------+
//! | api_key    | api_version | correlation_id | client_id |
//! | (2B)       | (2B)        | (4B)           | (string)  |
//! +------------+-------------+----------------+-----------+
//! ```
//!
//! Request header v2+ adds tagged fields for flexible versions.

pub mod codec;
pub mod request;
pub mod response;
pub mod types;

pub use codec::*;
pub use request::*;
pub use response::*;
pub use types::*;

use crate::workloads::kafka::ApiKey;

// ---------------------------------------------------------------------------
// Request Header
// ---------------------------------------------------------------------------

/// Kafka request header.
#[derive(Debug, Clone)]
pub struct RequestHeader {
    /// The API key for this request.
    pub api_key: ApiKey,
    /// The API version for this request.
    pub api_version: i16,
    /// The correlation ID for matching responses.
    pub correlation_id: i32,
    /// The client ID string (nullable).
    pub client_id: Option<String>,
    /// Tagged fields (flexible versions only).
    pub tagged_fields: TaggedFields,
}

impl RequestHeader {
    /// Create a new request header.
    pub fn new(api_key: ApiKey, api_version: i16, correlation_id: i32) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id: None,
            tagged_fields: TaggedFields::default(),
        }
    }

    /// Set the client ID.
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Check if this version uses flexible encoding.
    pub fn is_flexible(&self) -> bool {
        // Most APIs become flexible at version 2
        self.api_version >= 2
    }
}

// ---------------------------------------------------------------------------
// Response Header
// ---------------------------------------------------------------------------

/// Kafka response header.
#[derive(Debug, Clone, Default)]
pub struct ResponseHeader {
    /// The correlation ID matching the request.
    pub correlation_id: i32,
    /// Tagged fields (flexible versions only).
    pub tagged_fields: TaggedFields,
}

impl ResponseHeader {
    /// Create a new response header.
    pub fn new(correlation_id: i32) -> Self {
        Self {
            correlation_id,
            tagged_fields: TaggedFields::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tagged Fields
// ---------------------------------------------------------------------------

/// Tagged fields container for flexible protocol versions.
#[derive(Debug, Clone, Default)]
pub struct TaggedFields {
    /// Raw tagged field data.
    fields: Vec<TaggedField>,
}

impl TaggedFields {
    /// Create empty tagged fields.
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    /// Add a tagged field.
    pub fn push(&mut self, tag: u32, data: Vec<u8>) {
        self.fields.push(TaggedField { tag, data });
    }

    /// Get a tagged field by tag.
    pub fn get(&self, tag: u32) -> Option<&[u8]> {
        self.fields
            .iter()
            .find(|f| f.tag == tag)
            .map(|f| f.data.as_slice())
    }

    /// Get the number of tagged fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if there are no tagged fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Iterate over tagged fields.
    pub fn iter(&self) -> impl Iterator<Item = &TaggedField> {
        self.fields.iter()
    }
}

/// A single tagged field.
#[derive(Debug, Clone)]
pub struct TaggedField {
    /// The tag identifier.
    pub tag: u32,
    /// The field data.
    pub data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// API Version Range
// ---------------------------------------------------------------------------

/// Supported version range for an API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionRange {
    /// The API key.
    pub api_key: ApiKey,
    /// Minimum supported version.
    pub min_version: i16,
    /// Maximum supported version.
    pub max_version: i16,
}

impl ApiVersionRange {
    /// Create a new version range.
    pub const fn new(api_key: ApiKey, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key,
            min_version,
            max_version,
        }
    }

    /// Check if a version is supported.
    pub fn supports(&self, version: i16) -> bool {
        version >= self.min_version && version <= self.max_version
    }
}

/// Get supported API versions.
pub fn supported_api_versions() -> Vec<ApiVersionRange> {
    vec![
        ApiVersionRange::new(ApiKey::Produce, 0, 9),
        ApiVersionRange::new(ApiKey::Fetch, 0, 13),
        ApiVersionRange::new(ApiKey::ListOffsets, 0, 7),
        ApiVersionRange::new(ApiKey::Metadata, 0, 12),
        ApiVersionRange::new(ApiKey::OffsetCommit, 0, 8),
        ApiVersionRange::new(ApiKey::OffsetFetch, 0, 8),
        ApiVersionRange::new(ApiKey::FindCoordinator, 0, 4),
        ApiVersionRange::new(ApiKey::JoinGroup, 0, 9),
        ApiVersionRange::new(ApiKey::Heartbeat, 0, 4),
        ApiVersionRange::new(ApiKey::LeaveGroup, 0, 5),
        ApiVersionRange::new(ApiKey::SyncGroup, 0, 5),
        ApiVersionRange::new(ApiKey::DescribeGroups, 0, 5),
        ApiVersionRange::new(ApiKey::ListGroups, 0, 4),
        ApiVersionRange::new(ApiKey::SaslHandshake, 0, 1),
        ApiVersionRange::new(ApiKey::ApiVersions, 0, 3),
        ApiVersionRange::new(ApiKey::CreateTopics, 0, 7),
        ApiVersionRange::new(ApiKey::DeleteTopics, 0, 6),
        ApiVersionRange::new(ApiKey::InitProducerId, 0, 4),
        ApiVersionRange::new(ApiKey::AddPartitionsToTxn, 0, 4),
        ApiVersionRange::new(ApiKey::EndTxn, 0, 3),
        ApiVersionRange::new(ApiKey::SaslAuthenticate, 0, 2),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_header() {
        let header = RequestHeader::new(ApiKey::Metadata, 9, 12345).with_client_id("test-client");

        assert_eq!(header.api_key, ApiKey::Metadata);
        assert_eq!(header.api_version, 9);
        assert_eq!(header.correlation_id, 12345);
        assert_eq!(header.client_id, Some("test-client".to_string()));
    }

    #[test]
    fn test_tagged_fields() {
        let mut fields = TaggedFields::new();
        assert!(fields.is_empty());

        fields.push(0, vec![1, 2, 3]);
        fields.push(1, vec![4, 5]);

        assert_eq!(fields.len(), 2);
        assert_eq!(fields.get(0), Some([1, 2, 3].as_slice()));
        assert_eq!(fields.get(1), Some([4, 5].as_slice()));
        assert_eq!(fields.get(2), None);
    }

    #[test]
    fn test_api_version_range() {
        let range = ApiVersionRange::new(ApiKey::Metadata, 0, 12);

        assert!(range.supports(0));
        assert!(range.supports(6));
        assert!(range.supports(12));
        assert!(!range.supports(-1));
        assert!(!range.supports(13));
    }

    #[test]
    fn test_supported_api_versions() {
        let versions = supported_api_versions();
        assert!(!versions.is_empty());

        // Check that ApiVersions itself is supported
        let api_versions = versions.iter().find(|v| v.api_key == ApiKey::ApiVersions);
        assert!(api_versions.is_some());
    }
}
