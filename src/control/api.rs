//! Protocol management API types.
//!
//! Defines request/response types for the protocol management REST/gRPC API.

use super::capabilities::{
    ProtocolAssignment, ProtocolFeatureFlag, ProtocolLimits, ProtocolType, TenantCapabilities,
};
use serde::{Deserialize, Serialize};

// ============================================================================
// Protocol Assignment API
// ============================================================================

/// Request to assign a protocol to a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignProtocolRequest {
    pub tenant_id: String,
    pub protocol: String,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub features: Option<Vec<String>>,
    #[serde(default)]
    pub limits: Option<ProtocolLimitsRequest>,
    #[serde(default)]
    pub prg_affinity: Option<String>,
    #[serde(default)]
    pub priority: Option<u8>,
}

impl AssignProtocolRequest {
    pub fn to_assignment(&self) -> ProtocolAssignment {
        let protocol = ProtocolType::from(self.protocol.as_str());
        let mut assignment = ProtocolAssignment::new(protocol);

        if let Some(enabled) = self.enabled {
            assignment.enabled = enabled;
        }

        if let Some(priority) = self.priority {
            assignment.priority = priority;
        }

        assignment.prg_affinity = self.prg_affinity.clone();

        if let Some(features) = &self.features {
            assignment.features = features.iter().map(|f| parse_feature_flag(f)).collect();
        }

        if let Some(limits) = &self.limits {
            assignment.limits = limits.to_limits();
        }

        assignment
    }
}

/// Response for protocol assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignProtocolResponse {
    pub success: bool,
    pub tenant_id: String,
    pub protocol: String,
    pub capability_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to remove a protocol from a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveProtocolRequest {
    pub tenant_id: String,
    pub protocol: String,
}

/// Response for protocol removal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveProtocolResponse {
    pub success: bool,
    pub tenant_id: String,
    pub protocol: String,
    pub capability_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ============================================================================
// Tenant Management API
// ============================================================================

/// Request to create or update a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertTenantRequest {
    pub tenant_id: String,
    #[serde(default)]
    pub protocols: Option<Vec<ProtocolAssignmentRequest>>,
    #[serde(default)]
    pub global_features: Option<Vec<String>>,
    #[serde(default)]
    pub active: Option<bool>,
}

/// Protocol assignment within a tenant upsert request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolAssignmentRequest {
    pub protocol: String,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub features: Option<Vec<String>>,
    #[serde(default)]
    pub limits: Option<ProtocolLimitsRequest>,
    #[serde(default)]
    pub prg_affinity: Option<String>,
    #[serde(default)]
    pub priority: Option<u8>,
}

/// Protocol limits in API requests.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProtocolLimitsRequest {
    #[serde(default)]
    pub max_connections: Option<u32>,
    #[serde(default)]
    pub max_message_size: Option<u32>,
    #[serde(default)]
    pub max_messages_per_second: Option<u32>,
    #[serde(default)]
    pub max_bytes_per_second: Option<u64>,
    #[serde(default)]
    pub max_offline_queue_bytes: Option<u64>,
    #[serde(default)]
    pub max_retained_messages: Option<u32>,
    #[serde(default)]
    pub max_subscriptions_per_connection: Option<u32>,
}

impl ProtocolLimitsRequest {
    pub fn to_limits(&self) -> ProtocolLimits {
        let mut limits = ProtocolLimits::default();
        if let Some(v) = self.max_connections {
            limits.max_connections = v;
        }
        if let Some(v) = self.max_message_size {
            limits.max_message_size = v;
        }
        if let Some(v) = self.max_messages_per_second {
            limits.max_messages_per_second = v;
        }
        if let Some(v) = self.max_bytes_per_second {
            limits.max_bytes_per_second = v;
        }
        if let Some(v) = self.max_offline_queue_bytes {
            limits.max_offline_queue_bytes = v;
        }
        if let Some(v) = self.max_retained_messages {
            limits.max_retained_messages = v;
        }
        if let Some(v) = self.max_subscriptions_per_connection {
            limits.max_subscriptions_per_connection = v;
        }
        limits
    }
}

/// Response for tenant upsert.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertTenantResponse {
    pub success: bool,
    pub tenant_id: String,
    pub capability_epoch: u64,
    pub created: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to deactivate a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeactivateTenantRequest {
    pub tenant_id: String,
}

/// Response for tenant deactivation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeactivateTenantResponse {
    pub success: bool,
    pub tenant_id: String,
    pub capability_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to activate a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivateTenantRequest {
    pub tenant_id: String,
}

/// Response for tenant activation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivateTenantResponse {
    pub success: bool,
    pub tenant_id: String,
    pub capability_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ============================================================================
// Query API
// ============================================================================

/// Request to get tenant capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTenantRequest {
    pub tenant_id: String,
}

/// Response containing tenant capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetTenantResponse {
    pub found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<TenantCapabilitiesResponse>,
}

/// Tenant capabilities in API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantCapabilitiesResponse {
    pub tenant_id: String,
    pub epoch: u64,
    pub protocols: Vec<ProtocolAssignmentResponse>,
    pub global_features: Vec<String>,
    pub active: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

impl From<&TenantCapabilities> for TenantCapabilitiesResponse {
    fn from(caps: &TenantCapabilities) -> Self {
        Self {
            tenant_id: caps.tenant_id.clone(),
            epoch: caps.epoch,
            protocols: caps
                .protocols
                .values()
                .map(ProtocolAssignmentResponse::from)
                .collect(),
            global_features: caps
                .global_features
                .iter()
                .map(|f| f.as_str().to_string())
                .collect(),
            active: caps.active,
            created_at: caps.created_at,
            updated_at: caps.updated_at,
        }
    }
}

/// Protocol assignment in API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolAssignmentResponse {
    pub protocol: String,
    pub enabled: bool,
    pub features: Vec<String>,
    pub limits: ProtocolLimitsResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prg_affinity: Option<String>,
    pub priority: u8,
}

impl From<&ProtocolAssignment> for ProtocolAssignmentResponse {
    fn from(assignment: &ProtocolAssignment) -> Self {
        Self {
            protocol: assignment.protocol.as_str().to_string(),
            enabled: assignment.enabled,
            features: assignment
                .features
                .iter()
                .map(|f| f.as_str().to_string())
                .collect(),
            limits: ProtocolLimitsResponse::from(&assignment.limits),
            prg_affinity: assignment.prg_affinity.clone(),
            priority: assignment.priority,
        }
    }
}

/// Protocol limits in API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolLimitsResponse {
    pub max_connections: u32,
    pub max_message_size: u32,
    pub max_messages_per_second: u32,
    pub max_bytes_per_second: u64,
    pub max_offline_queue_bytes: u64,
    pub max_retained_messages: u32,
    pub max_subscriptions_per_connection: u32,
}

impl From<&ProtocolLimits> for ProtocolLimitsResponse {
    fn from(limits: &ProtocolLimits) -> Self {
        Self {
            max_connections: limits.max_connections,
            max_message_size: limits.max_message_size,
            max_messages_per_second: limits.max_messages_per_second,
            max_bytes_per_second: limits.max_bytes_per_second,
            max_offline_queue_bytes: limits.max_offline_queue_bytes,
            max_retained_messages: limits.max_retained_messages,
            max_subscriptions_per_connection: limits.max_subscriptions_per_connection,
        }
    }
}

/// Request to list tenants.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTenantsRequest {
    #[serde(default)]
    pub protocol_filter: Option<String>,
    #[serde(default)]
    pub active_only: Option<bool>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Response containing tenant list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTenantsResponse {
    pub tenants: Vec<TenantSummary>,
    pub total_count: usize,
    pub capability_epoch: u64,
}

/// Summary of a tenant for list responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantSummary {
    pub tenant_id: String,
    pub epoch: u64,
    pub protocols: Vec<String>,
    pub active: bool,
}

/// Request to list available protocols.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListProtocolsRequest {}

/// Response containing available protocols.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListProtocolsResponse {
    pub protocols: Vec<ProtocolInfo>,
}

/// Information about an available protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolInfo {
    pub name: String,
    pub default_features: Vec<String>,
    pub available_features: Vec<String>,
}

// ============================================================================
// Feature Flag API
// ============================================================================

/// Request to add a feature flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddFeatureRequest {
    pub tenant_id: String,
    pub protocol: Option<String>,
    pub feature: String,
}

/// Response for adding a feature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddFeatureResponse {
    pub success: bool,
    pub tenant_id: String,
    pub feature: String,
    pub capability_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request to remove a feature flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveFeatureRequest {
    pub tenant_id: String,
    pub protocol: Option<String>,
    pub feature: String,
}

/// Response for removing a feature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveFeatureResponse {
    pub success: bool,
    pub tenant_id: String,
    pub feature: String,
    pub capability_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ============================================================================
// Dedicated PRG API
// ============================================================================

/// Request for a dedicated PRG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedicatedPrgRequest {
    pub tenant_id: String,
    pub protocol: String,
    #[serde(default)]
    pub preferred_node: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Response for dedicated PRG request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedicatedPrgResponse {
    pub success: bool,
    pub tenant_id: String,
    pub protocol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prg_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    pub capability_epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ============================================================================
// Helper Functions
// ============================================================================

fn parse_feature_flag(s: &str) -> ProtocolFeatureFlag {
    match s {
        "mqtt5_features" => ProtocolFeatureFlag::Mqtt5Features,
        "retained_messages" => ProtocolFeatureFlag::RetainedMessages,
        "offline_queue" => ProtocolFeatureFlag::OfflineQueue,
        "deduplication" => ProtocolFeatureFlag::Deduplication,
        "exactly_once" => ProtocolFeatureFlag::ExactlyOnce,
        "topic_acl" => ProtocolFeatureFlag::TopicAcl,
        "message_encryption" => ProtocolFeatureFlag::MessageEncryption,
        "audit_logging" => ProtocolFeatureFlag::AuditLogging,
        other => ProtocolFeatureFlag::Custom(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_protocol_request() {
        let request = AssignProtocolRequest {
            tenant_id: "tenant-1".to_string(),
            protocol: "mqtt".to_string(),
            enabled: Some(true),
            features: Some(vec![
                "mqtt5_features".to_string(),
                "offline_queue".to_string(),
            ]),
            limits: Some(ProtocolLimitsRequest {
                max_connections: Some(1000),
                max_message_size: Some(65536),
                ..Default::default()
            }),
            prg_affinity: Some("prg-0".to_string()),
            priority: Some(100),
        };

        let assignment = request.to_assignment();
        assert_eq!(assignment.protocol, ProtocolType::Mqtt);
        assert!(assignment.enabled);
        assert_eq!(assignment.priority, 100);
        assert!(assignment.has_feature(&ProtocolFeatureFlag::Mqtt5Features));
        assert!(assignment.has_feature(&ProtocolFeatureFlag::OfflineQueue));
        assert_eq!(assignment.limits.max_connections, 1000);
        assert_eq!(assignment.limits.max_message_size, 65536);
    }

    #[test]
    fn test_tenant_capabilities_response() {
        let mut caps = TenantCapabilities::new("tenant-1");
        caps.set_protocol(ProtocolAssignment::new(ProtocolType::Mqtt));
        caps.add_global_feature(ProtocolFeatureFlag::AuditLogging);

        let response = TenantCapabilitiesResponse::from(&caps);
        assert_eq!(response.tenant_id, "tenant-1");
        assert_eq!(response.protocols.len(), 1);
        assert!(response
            .global_features
            .contains(&"audit_logging".to_string()));
    }

    #[test]
    fn test_protocol_limits_request_default() {
        let limits_req = ProtocolLimitsRequest {
            max_connections: Some(5000),
            ..Default::default()
        };

        let limits = limits_req.to_limits();
        assert_eq!(limits.max_connections, 5000);
        // Defaults should be applied for unspecified fields
        assert_eq!(limits.max_message_size, 256 * 1024);
    }
}
