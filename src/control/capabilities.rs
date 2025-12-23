//! Protocol capability registry for tenant-level protocol assignments.
//!
//! Manages which protocols/workloads are enabled per tenant, feature flags,
//! and capability epochs for tracking configuration changes.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Unique identifier for a protocol/workload type.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolType {
    Mqtt,
    Amqp,
    Kafka,
    Custom(String),
}

impl ProtocolType {
    pub fn as_str(&self) -> &str {
        match self {
            ProtocolType::Mqtt => "mqtt",
            ProtocolType::Amqp => "amqp",
            ProtocolType::Kafka => "kafka",
            ProtocolType::Custom(s) => s,
        }
    }
}

impl std::fmt::Display for ProtocolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for ProtocolType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "mqtt" => ProtocolType::Mqtt,
            "amqp" => ProtocolType::Amqp,
            "kafka" => ProtocolType::Kafka,
            other => ProtocolType::Custom(other.to_string()),
        }
    }
}

/// Feature flags that can be enabled per-protocol per-tenant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolFeatureFlag {
    /// MQTT 5.0 features (shared subscriptions, message expiry, etc.)
    Mqtt5Features,
    /// Retained message support.
    RetainedMessages,
    /// Offline/persistent session support.
    OfflineQueue,
    /// Message deduplication.
    Deduplication,
    /// Exactly-once semantics (QoS 2 or equivalent).
    ExactlyOnce,
    /// Topic-level access control.
    TopicAcl,
    /// Message-level encryption.
    MessageEncryption,
    /// Audit logging for this tenant.
    AuditLogging,
    /// Custom feature flag.
    Custom(String),
}

impl ProtocolFeatureFlag {
    pub fn as_str(&self) -> &str {
        match self {
            ProtocolFeatureFlag::Mqtt5Features => "mqtt5_features",
            ProtocolFeatureFlag::RetainedMessages => "retained_messages",
            ProtocolFeatureFlag::OfflineQueue => "offline_queue",
            ProtocolFeatureFlag::Deduplication => "deduplication",
            ProtocolFeatureFlag::ExactlyOnce => "exactly_once",
            ProtocolFeatureFlag::TopicAcl => "topic_acl",
            ProtocolFeatureFlag::MessageEncryption => "message_encryption",
            ProtocolFeatureFlag::AuditLogging => "audit_logging",
            ProtocolFeatureFlag::Custom(s) => s,
        }
    }

    /// Get default flags for a protocol type.
    pub fn defaults_for(protocol: &ProtocolType) -> HashSet<ProtocolFeatureFlag> {
        let mut flags = HashSet::new();
        match protocol {
            ProtocolType::Mqtt => {
                flags.insert(ProtocolFeatureFlag::Mqtt5Features);
                flags.insert(ProtocolFeatureFlag::RetainedMessages);
                flags.insert(ProtocolFeatureFlag::OfflineQueue);
            }
            ProtocolType::Amqp => {
                flags.insert(ProtocolFeatureFlag::ExactlyOnce);
                flags.insert(ProtocolFeatureFlag::Deduplication);
            }
            ProtocolType::Kafka => {
                flags.insert(ProtocolFeatureFlag::ExactlyOnce);
            }
            ProtocolType::Custom(_) => {}
        }
        flags
    }
}

impl std::fmt::Display for ProtocolFeatureFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Resource limits for a protocol assignment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtocolLimits {
    /// Maximum concurrent connections.
    pub max_connections: u32,
    /// Maximum message size in bytes.
    pub max_message_size: u32,
    /// Maximum messages per second (0 = unlimited).
    pub max_messages_per_second: u32,
    /// Maximum bytes per second (0 = unlimited).
    pub max_bytes_per_second: u64,
    /// Maximum offline queue size in bytes.
    pub max_offline_queue_bytes: u64,
    /// Maximum retained messages.
    pub max_retained_messages: u32,
    /// Maximum subscriptions per connection.
    pub max_subscriptions_per_connection: u32,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            max_message_size: 256 * 1024,               // 256 KB
            max_messages_per_second: 0,                 // Unlimited
            max_bytes_per_second: 0,                    // Unlimited
            max_offline_queue_bytes: 256 * 1024 * 1024, // 256 MB
            max_retained_messages: 10_000,
            max_subscriptions_per_connection: 100,
        }
    }
}

/// A protocol assignment for a tenant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtocolAssignment {
    /// The protocol type.
    pub protocol: ProtocolType,
    /// Whether this assignment is enabled.
    pub enabled: bool,
    /// Enabled feature flags.
    pub features: HashSet<ProtocolFeatureFlag>,
    /// Resource limits for this protocol.
    pub limits: ProtocolLimits,
    /// PRG affinity hint (optional specific PRG assignment).
    pub prg_affinity: Option<String>,
    /// Priority for PRG scheduling (higher = more priority).
    pub priority: u8,
}

impl ProtocolAssignment {
    pub fn new(protocol: ProtocolType) -> Self {
        let features = ProtocolFeatureFlag::defaults_for(&protocol);
        Self {
            protocol,
            enabled: true,
            features,
            limits: ProtocolLimits::default(),
            prg_affinity: None,
            priority: 50,
        }
    }

    pub fn with_feature(mut self, flag: ProtocolFeatureFlag) -> Self {
        self.features.insert(flag);
        self
    }

    pub fn without_feature(mut self, flag: &ProtocolFeatureFlag) -> Self {
        self.features.remove(flag);
        self
    }

    pub fn with_limits(mut self, limits: ProtocolLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_prg_affinity(mut self, prg: impl Into<String>) -> Self {
        self.prg_affinity = Some(prg.into());
        self
    }

    pub fn has_feature(&self, flag: &ProtocolFeatureFlag) -> bool {
        self.features.contains(flag)
    }
}

/// Tenant capabilities containing all protocol assignments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantCapabilities {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Capability epoch for change tracking.
    pub epoch: u64,
    /// Protocol assignments for this tenant.
    pub protocols: HashMap<ProtocolType, ProtocolAssignment>,
    /// Tenant-level feature flags (apply to all protocols).
    pub global_features: HashSet<ProtocolFeatureFlag>,
    /// Whether the tenant is active.
    pub active: bool,
    /// Creation timestamp (Unix epoch seconds).
    pub created_at: u64,
    /// Last modification timestamp.
    pub updated_at: u64,
}

impl TenantCapabilities {
    pub fn new(tenant_id: impl Into<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            tenant_id: tenant_id.into(),
            epoch: 1,
            protocols: HashMap::new(),
            global_features: HashSet::new(),
            active: true,
            created_at: now,
            updated_at: now,
        }
    }

    /// Add or update a protocol assignment.
    pub fn set_protocol(&mut self, assignment: ProtocolAssignment) {
        self.protocols
            .insert(assignment.protocol.clone(), assignment);
        self.bump_epoch();
    }

    /// Remove a protocol assignment.
    pub fn remove_protocol(&mut self, protocol: &ProtocolType) -> Option<ProtocolAssignment> {
        let removed = self.protocols.remove(protocol);
        if removed.is_some() {
            self.bump_epoch();
        }
        removed
    }

    /// Check if a protocol is enabled.
    pub fn has_protocol(&self, protocol: &ProtocolType) -> bool {
        self.protocols
            .get(protocol)
            .map(|a| a.enabled)
            .unwrap_or(false)
    }

    /// Get a protocol assignment.
    pub fn get_protocol(&self, protocol: &ProtocolType) -> Option<&ProtocolAssignment> {
        self.protocols.get(protocol)
    }

    /// Get a mutable protocol assignment.
    pub fn get_protocol_mut(&mut self, protocol: &ProtocolType) -> Option<&mut ProtocolAssignment> {
        self.protocols.get_mut(protocol)
    }

    /// Check if a feature is enabled (either global or protocol-specific).
    pub fn has_feature(&self, protocol: &ProtocolType, flag: &ProtocolFeatureFlag) -> bool {
        if self.global_features.contains(flag) {
            return true;
        }
        self.protocols
            .get(protocol)
            .map(|a| a.has_feature(flag))
            .unwrap_or(false)
    }

    /// Add a global feature flag.
    pub fn add_global_feature(&mut self, flag: ProtocolFeatureFlag) {
        self.global_features.insert(flag);
        self.bump_epoch();
    }

    /// Remove a global feature flag.
    pub fn remove_global_feature(&mut self, flag: &ProtocolFeatureFlag) {
        if self.global_features.remove(flag) {
            self.bump_epoch();
        }
    }

    /// Get all enabled protocols.
    pub fn enabled_protocols(&self) -> Vec<&ProtocolType> {
        self.protocols
            .iter()
            .filter(|(_, a)| a.enabled)
            .map(|(p, _)| p)
            .collect()
    }

    /// Bump the epoch and update timestamp.
    fn bump_epoch(&mut self) {
        self.epoch += 1;
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(self.updated_at);
    }

    /// Deactivate the tenant.
    pub fn deactivate(&mut self) {
        self.active = false;
        self.bump_epoch();
    }

    /// Reactivate the tenant.
    pub fn activate(&mut self) {
        self.active = true;
        self.bump_epoch();
    }
}

/// Capability registry managing all tenant capabilities.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapabilityRegistry {
    /// All tenant capabilities indexed by tenant ID.
    tenants: HashMap<String, TenantCapabilities>,
    /// Global registry epoch.
    epoch: u64,
    /// Protocol catalog (available protocols).
    available_protocols: HashSet<ProtocolType>,
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        let mut available_protocols = HashSet::new();
        available_protocols.insert(ProtocolType::Mqtt);
        Self {
            tenants: HashMap::new(),
            epoch: 1,
            available_protocols,
        }
    }

    /// Register an available protocol in the catalog.
    pub fn register_protocol(&mut self, protocol: ProtocolType) {
        self.available_protocols.insert(protocol);
        self.epoch += 1;
    }

    /// Check if a protocol is available in the catalog.
    pub fn is_protocol_available(&self, protocol: &ProtocolType) -> bool {
        self.available_protocols.contains(protocol)
    }

    /// Get all available protocols.
    pub fn available_protocols(&self) -> &HashSet<ProtocolType> {
        &self.available_protocols
    }

    /// Get or create tenant capabilities.
    pub fn get_or_create_tenant(&mut self, tenant_id: &str) -> &mut TenantCapabilities {
        if !self.tenants.contains_key(tenant_id) {
            self.tenants
                .insert(tenant_id.to_string(), TenantCapabilities::new(tenant_id));
            self.epoch += 1;
        }
        self.tenants.get_mut(tenant_id).unwrap()
    }

    /// Get tenant capabilities.
    pub fn get_tenant(&self, tenant_id: &str) -> Option<&TenantCapabilities> {
        self.tenants.get(tenant_id)
    }

    /// Get mutable tenant capabilities.
    pub fn get_tenant_mut(&mut self, tenant_id: &str) -> Option<&mut TenantCapabilities> {
        self.tenants.get_mut(tenant_id)
    }

    /// Remove a tenant.
    pub fn remove_tenant(&mut self, tenant_id: &str) -> Option<TenantCapabilities> {
        let removed = self.tenants.remove(tenant_id);
        if removed.is_some() {
            self.epoch += 1;
        }
        removed
    }

    /// List all tenant IDs.
    pub fn tenant_ids(&self) -> Vec<&String> {
        self.tenants.keys().collect()
    }

    /// Get the registry epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Count tenants.
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    /// Validate a protocol assignment for a tenant.
    pub fn validate_assignment(
        &self,
        tenant_id: &str,
        assignment: &ProtocolAssignment,
    ) -> Result<(), CapabilityError> {
        // Check protocol is available
        if !self.is_protocol_available(&assignment.protocol) {
            return Err(CapabilityError::ProtocolNotAvailable(
                assignment.protocol.to_string(),
            ));
        }

        // Check tenant exists (for updates)
        if !self.tenants.contains_key(tenant_id) {
            // Allow creating new tenants
        }

        // Validate limits
        if assignment.limits.max_message_size == 0 {
            return Err(CapabilityError::InvalidLimit(
                "max_message_size must be > 0".to_string(),
            ));
        }

        if assignment.limits.max_connections == 0 {
            return Err(CapabilityError::InvalidLimit(
                "max_connections must be > 0".to_string(),
            ));
        }

        Ok(())
    }

    /// Assign a protocol to a tenant with validation.
    pub fn assign_protocol(
        &mut self,
        tenant_id: &str,
        assignment: ProtocolAssignment,
    ) -> Result<(), CapabilityError> {
        self.validate_assignment(tenant_id, &assignment)?;

        let tenant = self.get_or_create_tenant(tenant_id);
        tenant.set_protocol(assignment);
        self.epoch += 1;
        Ok(())
    }

    /// Get tenants using a specific protocol.
    pub fn tenants_with_protocol(&self, protocol: &ProtocolType) -> Vec<&str> {
        self.tenants
            .iter()
            .filter(|(_, caps)| caps.has_protocol(protocol))
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Get all active tenants.
    pub fn active_tenants(&self) -> Vec<&TenantCapabilities> {
        self.tenants.values().filter(|t| t.active).collect()
    }
}

/// Errors related to capability management.
#[derive(Debug, Clone, PartialEq)]
pub enum CapabilityError {
    /// The requested protocol is not available in the catalog.
    ProtocolNotAvailable(String),
    /// Invalid resource limit.
    InvalidLimit(String),
    /// Tenant not found.
    TenantNotFound(String),
    /// Protocol not assigned to tenant.
    ProtocolNotAssigned(String),
    /// Feature not available.
    FeatureNotAvailable(String),
    /// Migration validation failed.
    MigrationValidationFailed(String),
}

impl std::fmt::Display for CapabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CapabilityError::ProtocolNotAvailable(p) => {
                write!(f, "protocol not available: {}", p)
            }
            CapabilityError::InvalidLimit(msg) => write!(f, "invalid limit: {}", msg),
            CapabilityError::TenantNotFound(t) => write!(f, "tenant not found: {}", t),
            CapabilityError::ProtocolNotAssigned(p) => {
                write!(f, "protocol not assigned: {}", p)
            }
            CapabilityError::FeatureNotAvailable(feat) => {
                write!(f, "feature not available: {}", feat)
            }
            CapabilityError::MigrationValidationFailed(msg) => {
                write!(f, "migration validation failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for CapabilityError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_type_conversion() {
        assert_eq!(ProtocolType::from("mqtt"), ProtocolType::Mqtt);
        assert_eq!(ProtocolType::from("MQTT"), ProtocolType::Mqtt);
        assert_eq!(ProtocolType::from("amqp"), ProtocolType::Amqp);
        assert_eq!(ProtocolType::from("kafka"), ProtocolType::Kafka);
        assert_eq!(
            ProtocolType::from("custom"),
            ProtocolType::Custom("custom".to_string())
        );
    }

    #[test]
    fn test_protocol_assignment() {
        let assignment = ProtocolAssignment::new(ProtocolType::Mqtt)
            .with_feature(ProtocolFeatureFlag::AuditLogging)
            .with_prg_affinity("prg-dedicated-1");

        assert!(assignment.has_feature(&ProtocolFeatureFlag::Mqtt5Features));
        assert!(assignment.has_feature(&ProtocolFeatureFlag::AuditLogging));
        assert!(!assignment.has_feature(&ProtocolFeatureFlag::ExactlyOnce));
        assert_eq!(assignment.prg_affinity, Some("prg-dedicated-1".to_string()));
    }

    #[test]
    fn test_tenant_capabilities() {
        let mut caps = TenantCapabilities::new("tenant-1");
        assert_eq!(caps.epoch, 1);
        assert!(caps.active);

        // Add MQTT protocol
        caps.set_protocol(ProtocolAssignment::new(ProtocolType::Mqtt));
        assert_eq!(caps.epoch, 2);
        assert!(caps.has_protocol(&ProtocolType::Mqtt));
        assert!(!caps.has_protocol(&ProtocolType::Amqp));

        // Check features
        assert!(caps.has_feature(&ProtocolType::Mqtt, &ProtocolFeatureFlag::Mqtt5Features));

        // Add global feature
        caps.add_global_feature(ProtocolFeatureFlag::AuditLogging);
        assert!(caps.has_feature(&ProtocolType::Mqtt, &ProtocolFeatureFlag::AuditLogging));

        // Deactivate
        caps.deactivate();
        assert!(!caps.active);
    }

    #[test]
    fn test_capability_registry() {
        let mut registry = CapabilityRegistry::new();
        assert!(registry.is_protocol_available(&ProtocolType::Mqtt));

        // Register additional protocol
        registry.register_protocol(ProtocolType::Amqp);
        assert!(registry.is_protocol_available(&ProtocolType::Amqp));

        // Assign protocol to tenant
        let assignment = ProtocolAssignment::new(ProtocolType::Mqtt);
        registry
            .assign_protocol("tenant-1", assignment)
            .expect("assignment should succeed");

        assert!(registry.get_tenant("tenant-1").is_some());
        let tenant = registry.get_tenant("tenant-1").unwrap();
        assert!(tenant.has_protocol(&ProtocolType::Mqtt));

        // List tenants with protocol
        let mqtt_tenants = registry.tenants_with_protocol(&ProtocolType::Mqtt);
        assert_eq!(mqtt_tenants, vec!["tenant-1"]);
    }

    #[test]
    fn test_validate_assignment() {
        let registry = CapabilityRegistry::new();

        // Valid assignment
        let assignment = ProtocolAssignment::new(ProtocolType::Mqtt);
        assert!(registry
            .validate_assignment("tenant-1", &assignment)
            .is_ok());

        // Invalid: protocol not available
        let assignment = ProtocolAssignment::new(ProtocolType::Kafka);
        assert!(matches!(
            registry.validate_assignment("tenant-1", &assignment),
            Err(CapabilityError::ProtocolNotAvailable(_))
        ));

        // Invalid: zero message size
        let mut assignment = ProtocolAssignment::new(ProtocolType::Mqtt);
        assignment.limits.max_message_size = 0;
        assert!(matches!(
            registry.validate_assignment("tenant-1", &assignment),
            Err(CapabilityError::InvalidLimit(_))
        ));
    }

    #[test]
    fn test_epoch_tracking() {
        let mut registry = CapabilityRegistry::new();
        let initial_epoch = registry.epoch();

        registry.register_protocol(ProtocolType::Amqp);
        assert!(registry.epoch() > initial_epoch);

        let epoch_before = registry.epoch();
        let assignment = ProtocolAssignment::new(ProtocolType::Mqtt);
        registry.assign_protocol("tenant-1", assignment).unwrap();
        assert!(registry.epoch() > epoch_before);
    }

    #[test]
    fn test_default_features() {
        let mqtt_defaults = ProtocolFeatureFlag::defaults_for(&ProtocolType::Mqtt);
        assert!(mqtt_defaults.contains(&ProtocolFeatureFlag::Mqtt5Features));
        assert!(mqtt_defaults.contains(&ProtocolFeatureFlag::RetainedMessages));
        assert!(mqtt_defaults.contains(&ProtocolFeatureFlag::OfflineQueue));

        let amqp_defaults = ProtocolFeatureFlag::defaults_for(&ProtocolType::Amqp);
        assert!(amqp_defaults.contains(&ProtocolFeatureFlag::ExactlyOnce));
        assert!(amqp_defaults.contains(&ProtocolFeatureFlag::Deduplication));
    }
}
