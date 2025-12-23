//! Workload migration validator.
//!
//! Validates that PRGs have proper workload mappings before migration,
//! ensuring no data loss during rebalance operations.

use super::capabilities::{CapabilityError, CapabilityRegistry, ProtocolType};
use crate::routing::{PrgId, PrgPlacement};
use std::collections::{HashMap, HashSet};

/// Result of a migration validation check.
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationValidation {
    /// Migration is safe to proceed.
    Valid,
    /// Migration blocked due to validation errors.
    Invalid(Vec<MigrationError>),
}

impl MigrationValidation {
    pub fn is_valid(&self) -> bool {
        matches!(self, MigrationValidation::Valid)
    }

    pub fn errors(&self) -> Vec<&MigrationError> {
        match self {
            MigrationValidation::Valid => vec![],
            MigrationValidation::Invalid(errors) => errors.iter().collect(),
        }
    }
}

/// Specific migration validation errors.
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationError {
    /// PRG has no workload mapping.
    NoWorkloadMapping { prg_id: PrgId },
    /// Workload type mismatch between source and destination.
    WorkloadTypeMismatch {
        prg_id: PrgId,
        source_type: String,
        dest_type: String,
    },
    /// Protocol not enabled for tenant on destination.
    ProtocolNotEnabled {
        tenant_id: String,
        protocol: ProtocolType,
    },
    /// Feature required by PRG not available on destination.
    FeatureNotAvailable { prg_id: PrgId, feature: String },
    /// Destination node capacity exceeded.
    CapacityExceeded {
        node_id: String,
        current: u64,
        limit: u64,
    },
    /// In-flight messages would be lost.
    InFlightMessagesAtRisk { prg_id: PrgId, count: u64 },
    /// Replication lag too high for safe migration.
    ReplicationLagTooHigh {
        prg_id: PrgId,
        lag_seconds: u64,
        threshold: u64,
    },
    /// Session state not fully synchronized.
    SessionStateNotSynced {
        prg_id: PrgId,
        pending_sessions: u32,
    },
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::NoWorkloadMapping { prg_id } => {
                write!(
                    f,
                    "PRG {}:{} has no workload mapping",
                    prg_id.tenant_id, prg_id.partition_index
                )
            }
            MigrationError::WorkloadTypeMismatch {
                prg_id,
                source_type,
                dest_type,
            } => {
                write!(
                    f,
                    "PRG {}:{} workload type mismatch: {} != {}",
                    prg_id.tenant_id, prg_id.partition_index, source_type, dest_type
                )
            }
            MigrationError::ProtocolNotEnabled {
                tenant_id,
                protocol,
            } => {
                write!(
                    f,
                    "protocol {} not enabled for tenant {}",
                    protocol, tenant_id
                )
            }
            MigrationError::FeatureNotAvailable { prg_id, feature } => {
                write!(
                    f,
                    "PRG {}:{} requires feature {} not available on destination",
                    prg_id.tenant_id, prg_id.partition_index, feature
                )
            }
            MigrationError::CapacityExceeded {
                node_id,
                current,
                limit,
            } => {
                write!(
                    f,
                    "node {} capacity exceeded: {} >= {}",
                    node_id, current, limit
                )
            }
            MigrationError::InFlightMessagesAtRisk { prg_id, count } => {
                write!(
                    f,
                    "PRG {}:{} has {} in-flight messages at risk",
                    prg_id.tenant_id, prg_id.partition_index, count
                )
            }
            MigrationError::ReplicationLagTooHigh {
                prg_id,
                lag_seconds,
                threshold,
            } => {
                write!(
                    f,
                    "PRG {}:{} replication lag {}s exceeds threshold {}s",
                    prg_id.tenant_id, prg_id.partition_index, lag_seconds, threshold
                )
            }
            MigrationError::SessionStateNotSynced {
                prg_id,
                pending_sessions,
            } => {
                write!(
                    f,
                    "PRG {}:{} has {} sessions not fully synced",
                    prg_id.tenant_id, prg_id.partition_index, pending_sessions
                )
            }
        }
    }
}

/// PRG state snapshot for migration validation.
#[derive(Debug, Clone, Default)]
pub struct PrgStateSnapshot {
    /// Workload type (e.g., "mqtt", "amqp").
    pub workload_type: Option<String>,
    /// Number of in-flight messages.
    pub in_flight_messages: u64,
    /// Replication lag in seconds.
    pub replication_lag_seconds: u64,
    /// Number of sessions pending sync.
    pub pending_sessions: u32,
    /// Required features for this PRG.
    pub required_features: HashSet<String>,
}

/// Migration plan entry.
#[derive(Debug, Clone)]
pub struct MigrationEntry {
    pub prg_id: PrgId,
    pub source_node: String,
    pub dest_node: String,
}

/// Configuration for migration validation.
#[derive(Debug, Clone)]
pub struct MigrationValidatorConfig {
    /// Maximum allowed replication lag in seconds.
    pub max_replication_lag_seconds: u64,
    /// Maximum in-flight messages before blocking migration.
    pub max_in_flight_messages: u64,
    /// Maximum pending sessions before blocking migration.
    pub max_pending_sessions: u32,
    /// Whether to allow migration of PRGs without workload mappings.
    pub allow_unmapped_prgs: bool,
}

impl Default for MigrationValidatorConfig {
    fn default() -> Self {
        Self {
            max_replication_lag_seconds: 5,
            max_in_flight_messages: 100,
            max_pending_sessions: 10,
            allow_unmapped_prgs: false,
        }
    }
}

/// Workload migration validator.
///
/// Validates that PRGs can be safely migrated between nodes without data loss.
#[derive(Debug, Clone)]
pub struct MigrationValidator {
    config: MigrationValidatorConfig,
}

impl MigrationValidator {
    pub fn new(config: MigrationValidatorConfig) -> Self {
        Self { config }
    }

    /// Validate a single PRG migration.
    pub fn validate_prg_migration(
        &self,
        entry: &MigrationEntry,
        state: &PrgStateSnapshot,
        capabilities: &CapabilityRegistry,
    ) -> MigrationValidation {
        let mut errors = Vec::new();

        // Check workload mapping
        if state.workload_type.is_none() && !self.config.allow_unmapped_prgs {
            errors.push(MigrationError::NoWorkloadMapping {
                prg_id: entry.prg_id.clone(),
            });
        }

        // Check protocol enabled for tenant
        if let Some(workload_type) = &state.workload_type {
            let protocol = ProtocolType::from(workload_type.as_str());
            if let Some(tenant) = capabilities.get_tenant(&entry.prg_id.tenant_id) {
                if !tenant.has_protocol(&protocol) {
                    errors.push(MigrationError::ProtocolNotEnabled {
                        tenant_id: entry.prg_id.tenant_id.clone(),
                        protocol,
                    });
                }
            }
        }

        // Check replication lag
        if state.replication_lag_seconds > self.config.max_replication_lag_seconds {
            errors.push(MigrationError::ReplicationLagTooHigh {
                prg_id: entry.prg_id.clone(),
                lag_seconds: state.replication_lag_seconds,
                threshold: self.config.max_replication_lag_seconds,
            });
        }

        // Check in-flight messages
        if state.in_flight_messages > self.config.max_in_flight_messages {
            errors.push(MigrationError::InFlightMessagesAtRisk {
                prg_id: entry.prg_id.clone(),
                count: state.in_flight_messages,
            });
        }

        // Check pending sessions
        if state.pending_sessions > self.config.max_pending_sessions {
            errors.push(MigrationError::SessionStateNotSynced {
                prg_id: entry.prg_id.clone(),
                pending_sessions: state.pending_sessions,
            });
        }

        if errors.is_empty() {
            MigrationValidation::Valid
        } else {
            MigrationValidation::Invalid(errors)
        }
    }

    /// Validate a batch of PRG migrations.
    pub fn validate_migration_batch(
        &self,
        entries: &[MigrationEntry],
        states: &HashMap<PrgId, PrgStateSnapshot>,
        capabilities: &CapabilityRegistry,
        node_capacity: &HashMap<String, (u64, u64)>, // (current, limit)
    ) -> MigrationValidation {
        let mut all_errors = Vec::new();

        // Track capacity changes during validation
        let mut projected_capacity: HashMap<String, u64> = node_capacity
            .iter()
            .map(|(k, (current, _))| (k.clone(), *current))
            .collect();

        for entry in entries {
            let state = states.get(&entry.prg_id).cloned().unwrap_or_default();

            // Validate individual PRG
            if let MigrationValidation::Invalid(errors) =
                self.validate_prg_migration(entry, &state, capabilities)
            {
                all_errors.extend(errors);
            }

            // Update projected capacity
            if let Some(current) = projected_capacity.get_mut(&entry.dest_node) {
                *current += 1;
            } else {
                projected_capacity.insert(entry.dest_node.clone(), 1);
            }

            // Check capacity limit
            if let Some((_, limit)) = node_capacity.get(&entry.dest_node) {
                if let Some(current) = projected_capacity.get(&entry.dest_node) {
                    if *current > *limit {
                        all_errors.push(MigrationError::CapacityExceeded {
                            node_id: entry.dest_node.clone(),
                            current: *current,
                            limit: *limit,
                        });
                    }
                }
            }
        }

        if all_errors.is_empty() {
            MigrationValidation::Valid
        } else {
            MigrationValidation::Invalid(all_errors)
        }
    }

    /// Validate that all PRGs in current placements have workload mappings.
    pub fn validate_placements_have_workloads(
        &self,
        placements: &HashMap<PrgId, PrgPlacement>,
        workload_types: &HashMap<PrgId, String>,
    ) -> MigrationValidation {
        if self.config.allow_unmapped_prgs {
            return MigrationValidation::Valid;
        }

        let mut errors = Vec::new();

        for prg_id in placements.keys() {
            if !workload_types.contains_key(prg_id) {
                errors.push(MigrationError::NoWorkloadMapping {
                    prg_id: prg_id.clone(),
                });
            }
        }

        if errors.is_empty() {
            MigrationValidation::Valid
        } else {
            MigrationValidation::Invalid(errors)
        }
    }
}

impl Default for MigrationValidator {
    fn default() -> Self {
        Self::new(MigrationValidatorConfig::default())
    }
}

/// Convert a migration error to a capability error.
impl From<MigrationError> for CapabilityError {
    fn from(error: MigrationError) -> Self {
        CapabilityError::MigrationValidationFailed(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::capabilities::ProtocolAssignment;

    fn make_prg_id(tenant: &str, partition: u64) -> PrgId {
        PrgId {
            tenant_id: tenant.to_string(),
            partition_index: partition,
        }
    }

    #[test]
    fn test_valid_migration() {
        let validator = MigrationValidator::default();
        let mut registry = CapabilityRegistry::new();

        // Set up tenant with MQTT
        registry
            .assign_protocol("tenant-1", ProtocolAssignment::new(ProtocolType::Mqtt))
            .unwrap();

        let entry = MigrationEntry {
            prg_id: make_prg_id("tenant-1", 0),
            source_node: "node-1".to_string(),
            dest_node: "node-2".to_string(),
        };

        let state = PrgStateSnapshot {
            workload_type: Some("mqtt".to_string()),
            in_flight_messages: 10,
            replication_lag_seconds: 1,
            pending_sessions: 0,
            required_features: HashSet::new(),
        };

        let result = validator.validate_prg_migration(&entry, &state, &registry);
        assert!(result.is_valid());
    }

    #[test]
    fn test_no_workload_mapping() {
        let validator = MigrationValidator::default();
        let registry = CapabilityRegistry::new();

        let entry = MigrationEntry {
            prg_id: make_prg_id("tenant-1", 0),
            source_node: "node-1".to_string(),
            dest_node: "node-2".to_string(),
        };

        let state = PrgStateSnapshot::default();

        let result = validator.validate_prg_migration(&entry, &state, &registry);
        assert!(!result.is_valid());
        assert!(matches!(
            result.errors()[0],
            MigrationError::NoWorkloadMapping { .. }
        ));
    }

    #[test]
    fn test_replication_lag_too_high() {
        let validator = MigrationValidator::default();
        let mut registry = CapabilityRegistry::new();
        registry
            .assign_protocol("tenant-1", ProtocolAssignment::new(ProtocolType::Mqtt))
            .unwrap();

        let entry = MigrationEntry {
            prg_id: make_prg_id("tenant-1", 0),
            source_node: "node-1".to_string(),
            dest_node: "node-2".to_string(),
        };

        let state = PrgStateSnapshot {
            workload_type: Some("mqtt".to_string()),
            replication_lag_seconds: 10, // Over threshold of 5
            ..Default::default()
        };

        let result = validator.validate_prg_migration(&entry, &state, &registry);
        assert!(!result.is_valid());
        assert!(matches!(
            result.errors()[0],
            MigrationError::ReplicationLagTooHigh { .. }
        ));
    }

    #[test]
    fn test_in_flight_messages_at_risk() {
        let validator = MigrationValidator::default();
        let mut registry = CapabilityRegistry::new();
        registry
            .assign_protocol("tenant-1", ProtocolAssignment::new(ProtocolType::Mqtt))
            .unwrap();

        let entry = MigrationEntry {
            prg_id: make_prg_id("tenant-1", 0),
            source_node: "node-1".to_string(),
            dest_node: "node-2".to_string(),
        };

        let state = PrgStateSnapshot {
            workload_type: Some("mqtt".to_string()),
            in_flight_messages: 200, // Over threshold of 100
            ..Default::default()
        };

        let result = validator.validate_prg_migration(&entry, &state, &registry);
        assert!(!result.is_valid());
        assert!(matches!(
            result.errors()[0],
            MigrationError::InFlightMessagesAtRisk { .. }
        ));
    }

    #[test]
    fn test_batch_validation_with_capacity() {
        let validator = MigrationValidator::default();
        let mut registry = CapabilityRegistry::new();
        registry
            .assign_protocol("tenant-1", ProtocolAssignment::new(ProtocolType::Mqtt))
            .unwrap();

        let entries = vec![
            MigrationEntry {
                prg_id: make_prg_id("tenant-1", 0),
                source_node: "node-1".to_string(),
                dest_node: "node-2".to_string(),
            },
            MigrationEntry {
                prg_id: make_prg_id("tenant-1", 1),
                source_node: "node-1".to_string(),
                dest_node: "node-2".to_string(),
            },
        ];

        let mut states = HashMap::new();
        states.insert(
            make_prg_id("tenant-1", 0),
            PrgStateSnapshot {
                workload_type: Some("mqtt".to_string()),
                ..Default::default()
            },
        );
        states.insert(
            make_prg_id("tenant-1", 1),
            PrgStateSnapshot {
                workload_type: Some("mqtt".to_string()),
                ..Default::default()
            },
        );

        // Node-2 can only hold 1 more PRG
        let mut capacity = HashMap::new();
        capacity.insert("node-2".to_string(), (0, 1));

        let result = validator.validate_migration_batch(&entries, &states, &registry, &capacity);
        assert!(!result.is_valid());
        assert!(result
            .errors()
            .iter()
            .any(|e| matches!(e, MigrationError::CapacityExceeded { .. })));
    }

    #[test]
    fn test_allow_unmapped_prgs() {
        let config = MigrationValidatorConfig {
            allow_unmapped_prgs: true,
            ..Default::default()
        };
        let validator = MigrationValidator::new(config);
        let registry = CapabilityRegistry::new();

        let entry = MigrationEntry {
            prg_id: make_prg_id("tenant-1", 0),
            source_node: "node-1".to_string(),
            dest_node: "node-2".to_string(),
        };

        let state = PrgStateSnapshot::default();

        let result = validator.validate_prg_migration(&entry, &state, &registry);
        assert!(result.is_valid());
    }

    #[test]
    fn test_validate_placements_have_workloads() {
        let validator = MigrationValidator::default();

        let mut placements = HashMap::new();
        placements.insert(
            make_prg_id("tenant-1", 0),
            PrgPlacement {
                node_id: "node-1".to_string(),
                replicas: vec!["node-1".to_string()],
            },
        );
        placements.insert(
            make_prg_id("tenant-1", 1),
            PrgPlacement {
                node_id: "node-1".to_string(),
                replicas: vec!["node-1".to_string()],
            },
        );

        // Only one PRG has a workload mapping
        let mut workload_types = HashMap::new();
        workload_types.insert(make_prg_id("tenant-1", 0), "mqtt".to_string());

        let result = validator.validate_placements_have_workloads(&placements, &workload_types);
        assert!(!result.is_valid());
        assert_eq!(result.errors().len(), 1);
    }
}
