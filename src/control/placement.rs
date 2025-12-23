//! Placement planner for mixed workloads.
//!
//! Handles resource-aware placement decisions, capacity modeling,
//! workload affinity, and hardware validation.

use super::capabilities::ProtocolType;
use crate::routing::{PrgId, PrgPlacement};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

// ============================================================================
// Resource Profiles and Capacity Models
// ============================================================================

/// IO profile for workload placement hints.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IoProfile {
    /// Low latency IO required (SSD, NVMe).
    LatencySensitive,
    /// High throughput IO (bulk operations).
    ThroughputHeavy,
    /// Balanced IO pattern.
    #[default]
    Balanced,
}

/// Disk tier for hardware requirements.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskTier {
    Nvme,
    Ssd,
    Hdd,
    #[default]
    Any,
}

/// Resource requirements for a workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadResources {
    /// CPU millicores per PRG.
    pub cpu_millicores: u32,
    /// Memory bytes per PRG.
    pub memory_bytes: u64,
    /// Network bandwidth bytes/sec per PRG.
    pub network_bytes_per_sec: u64,
    /// Disk IOPS per PRG.
    pub disk_iops: u32,
    /// Disk bytes per PRG (storage footprint).
    pub disk_bytes: u64,
}

impl Default for WorkloadResources {
    fn default() -> Self {
        Self {
            cpu_millicores: 100,                     // 0.1 CPU
            memory_bytes: 64 * 1024 * 1024,          // 64 MB
            network_bytes_per_sec: 10 * 1024 * 1024, // 10 MB/s
            disk_iops: 100,
            disk_bytes: 256 * 1024 * 1024, // 256 MB
        }
    }
}

/// Resource profile for a workload including feature modifiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadResourceProfile {
    /// Base resources per PRG.
    pub base: WorkloadResources,
    /// Additional resources when offline queue is enabled.
    pub offline_queue_modifier: Option<WorkloadResources>,
    /// Additional resources when retained messages are enabled.
    pub retained_modifier: Option<WorkloadResources>,
    /// Additional resources when deduplication is enabled.
    pub dedupe_modifier: Option<WorkloadResources>,
}

impl Default for WorkloadResourceProfile {
    fn default() -> Self {
        Self {
            base: WorkloadResources::default(),
            offline_queue_modifier: Some(WorkloadResources {
                cpu_millicores: 10,
                memory_bytes: 32 * 1024 * 1024,
                network_bytes_per_sec: 0,
                disk_iops: 50,
                disk_bytes: 128 * 1024 * 1024,
            }),
            retained_modifier: Some(WorkloadResources {
                cpu_millicores: 5,
                memory_bytes: 16 * 1024 * 1024,
                network_bytes_per_sec: 0,
                disk_iops: 20,
                disk_bytes: 64 * 1024 * 1024,
            }),
            dedupe_modifier: Some(WorkloadResources {
                cpu_millicores: 5,
                memory_bytes: 8 * 1024 * 1024,
                network_bytes_per_sec: 0,
                disk_iops: 10,
                disk_bytes: 32 * 1024 * 1024,
            }),
        }
    }
}

impl WorkloadResourceProfile {
    /// Calculate total resources for a PRG with given features enabled.
    pub fn calculate(
        &self,
        offline_queue: bool,
        retained: bool,
        dedupe: bool,
    ) -> WorkloadResources {
        let mut total = self.base.clone();

        if offline_queue {
            if let Some(modifier) = &self.offline_queue_modifier {
                total.cpu_millicores += modifier.cpu_millicores;
                total.memory_bytes += modifier.memory_bytes;
                total.network_bytes_per_sec += modifier.network_bytes_per_sec;
                total.disk_iops += modifier.disk_iops;
                total.disk_bytes += modifier.disk_bytes;
            }
        }

        if retained {
            if let Some(modifier) = &self.retained_modifier {
                total.cpu_millicores += modifier.cpu_millicores;
                total.memory_bytes += modifier.memory_bytes;
                total.network_bytes_per_sec += modifier.network_bytes_per_sec;
                total.disk_iops += modifier.disk_iops;
                total.disk_bytes += modifier.disk_bytes;
            }
        }

        if dedupe {
            if let Some(modifier) = &self.dedupe_modifier {
                total.cpu_millicores += modifier.cpu_millicores;
                total.memory_bytes += modifier.memory_bytes;
                total.network_bytes_per_sec += modifier.network_bytes_per_sec;
                total.disk_iops += modifier.disk_iops;
                total.disk_bytes += modifier.disk_bytes;
            }
        }

        total
    }
}

// ============================================================================
// Placement Hints and Affinity
// ============================================================================

/// Workload placement hints for scheduling decisions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkloadPlacementHints {
    /// Preferred node affinity (e.g., "ssd", "region-a").
    pub leader_affinity: Option<String>,
    /// IO profile for the workload.
    pub io_profile: IoProfile,
    /// Preferred regions for placement.
    pub preferred_regions: Vec<String>,
    /// Node labels that must be present.
    pub required_labels: HashSet<String>,
    /// Node labels to avoid.
    pub anti_affinity_labels: HashSet<String>,
}

impl WorkloadPlacementHints {
    pub fn with_leader_affinity(mut self, affinity: impl Into<String>) -> Self {
        self.leader_affinity = Some(affinity.into());
        self
    }

    pub fn with_io_profile(mut self, profile: IoProfile) -> Self {
        self.io_profile = profile;
        self
    }

    pub fn with_required_label(mut self, label: impl Into<String>) -> Self {
        self.required_labels.insert(label.into());
        self
    }
}

// ============================================================================
// Hardware Validation
// ============================================================================

/// Hardware requirements for a workload.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HardwareRequirements {
    /// Minimum disk tier required.
    pub min_disk_tier: DiskTier,
    /// Required CPU features (e.g., "avx2", "aes-ni").
    pub required_cpu_features: HashSet<String>,
    /// Minimum memory in bytes.
    pub min_memory_bytes: u64,
    /// Whether encryption at rest is required.
    pub encryption_required: bool,
    /// Required node labels.
    pub required_labels: HashSet<String>,
}

impl HardwareRequirements {
    pub fn mqtt_default() -> Self {
        Self {
            min_disk_tier: DiskTier::Any,
            required_cpu_features: HashSet::new(),
            min_memory_bytes: 128 * 1024 * 1024, // 128 MB
            encryption_required: false,
            required_labels: HashSet::new(),
        }
    }

    pub fn for_protocol(protocol: &ProtocolType) -> Self {
        match protocol {
            ProtocolType::Mqtt => Self::mqtt_default(),
            ProtocolType::Amqp => Self {
                min_disk_tier: DiskTier::Ssd,
                min_memory_bytes: 256 * 1024 * 1024,
                ..Self::mqtt_default()
            },
            ProtocolType::Kafka => Self {
                min_disk_tier: DiskTier::Ssd,
                min_memory_bytes: 512 * 1024 * 1024,
                ..Self::mqtt_default()
            },
            ProtocolType::Custom(_) => Self::mqtt_default(),
        }
    }
}

/// Node hardware capabilities.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeHardware {
    /// Node identifier.
    pub node_id: String,
    /// Disk tier available.
    pub disk_tier: DiskTier,
    /// Available CPU features.
    pub cpu_features: HashSet<String>,
    /// Total memory in bytes.
    pub total_memory_bytes: u64,
    /// Available memory in bytes.
    pub available_memory_bytes: u64,
    /// Whether encryption at rest is enabled.
    pub encryption_enabled: bool,
    /// Node labels.
    pub labels: HashSet<String>,
    /// Node region.
    pub region: Option<String>,
    /// Current PRG count on this node.
    pub prg_count: u32,
    /// Maximum PRG capacity.
    pub max_prgs: u32,
}

impl NodeHardware {
    /// Check if this node meets the hardware requirements.
    pub fn meets_requirements(&self, requirements: &HardwareRequirements) -> bool {
        // Check disk tier
        let disk_ok = match requirements.min_disk_tier {
            DiskTier::Any => true,
            DiskTier::Hdd => true, // Any disk works
            DiskTier::Ssd => matches!(self.disk_tier, DiskTier::Ssd | DiskTier::Nvme),
            DiskTier::Nvme => matches!(self.disk_tier, DiskTier::Nvme),
        };

        if !disk_ok {
            return false;
        }

        // Check CPU features
        for feature in &requirements.required_cpu_features {
            if !self.cpu_features.contains(feature) {
                return false;
            }
        }

        // Check memory
        if self.available_memory_bytes < requirements.min_memory_bytes {
            return false;
        }

        // Check encryption
        if requirements.encryption_required && !self.encryption_enabled {
            return false;
        }

        // Check required labels
        for label in &requirements.required_labels {
            if !self.labels.contains(label) {
                return false;
            }
        }

        true
    }

    /// Check if this node matches placement hints.
    pub fn matches_hints(&self, hints: &WorkloadPlacementHints) -> bool {
        // Check required labels
        for label in &hints.required_labels {
            if !self.labels.contains(label) {
                return false;
            }
        }

        // Check anti-affinity
        for label in &hints.anti_affinity_labels {
            if self.labels.contains(label) {
                return false;
            }
        }

        // Check region preference (soft constraint)
        if !hints.preferred_regions.is_empty() {
            if let Some(region) = &self.region {
                if !hints.preferred_regions.contains(region) {
                    // Prefer matching regions but don't fail
                }
            }
        }

        true
    }

    /// Check if node has capacity for more PRGs.
    pub fn has_capacity(&self) -> bool {
        self.prg_count < self.max_prgs
    }
}

/// Result of hardware validation.
#[derive(Debug, Clone)]
pub enum HardwareValidation {
    Valid,
    Invalid(Vec<HardwareValidationError>),
}

impl HardwareValidation {
    pub fn is_valid(&self) -> bool {
        matches!(self, HardwareValidation::Valid)
    }
}

/// Hardware validation error.
#[derive(Debug, Clone)]
pub enum HardwareValidationError {
    DiskTierMismatch {
        required: DiskTier,
        available: DiskTier,
    },
    MissingCpuFeature(String),
    InsufficientMemory {
        required: u64,
        available: u64,
    },
    EncryptionNotEnabled,
    MissingLabel(String),
    CapacityExceeded,
}

impl std::fmt::Display for HardwareValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HardwareValidationError::DiskTierMismatch {
                required,
                available,
            } => {
                write!(
                    f,
                    "disk tier mismatch: required {:?}, available {:?}",
                    required, available
                )
            }
            HardwareValidationError::MissingCpuFeature(feat) => {
                write!(f, "missing CPU feature: {}", feat)
            }
            HardwareValidationError::InsufficientMemory {
                required,
                available,
            } => {
                write!(
                    f,
                    "insufficient memory: required {}, available {}",
                    required, available
                )
            }
            HardwareValidationError::EncryptionNotEnabled => {
                write!(f, "encryption not enabled")
            }
            HardwareValidationError::MissingLabel(label) => {
                write!(f, "missing required label: {}", label)
            }
            HardwareValidationError::CapacityExceeded => {
                write!(f, "node capacity exceeded")
            }
        }
    }
}

// ============================================================================
// Placement Planner
// ============================================================================

/// Placement decision for a PRG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementDecision {
    pub prg_id: PrgId,
    pub node_id: String,
    pub replicas: Vec<String>,
    pub workload_label: String,
    pub workload_version: String,
    pub resource_profile: WorkloadResources,
    pub capability_epoch: u64,
}

/// Placement plan containing multiple decisions.
#[derive(Debug, Clone, Default)]
pub struct PlacementPlan {
    pub decisions: Vec<PlacementDecision>,
    pub capability_epoch: u64,
    pub errors: Vec<PlacementError>,
}

/// Placement error.
#[derive(Debug, Clone)]
pub enum PlacementError {
    NoCompatibleNodes {
        prg_id: PrgId,
        reason: String,
    },
    InsufficientCapacity {
        prg_id: PrgId,
        required: WorkloadResources,
    },
    HardwareValidationFailed {
        prg_id: PrgId,
        node_id: String,
        errors: Vec<HardwareValidationError>,
    },
}

impl std::fmt::Display for PlacementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlacementError::NoCompatibleNodes { prg_id, reason } => {
                write!(
                    f,
                    "no compatible nodes for {}:{}: {}",
                    prg_id.tenant_id, prg_id.partition_index, reason
                )
            }
            PlacementError::InsufficientCapacity { prg_id, required } => {
                write!(
                    f,
                    "insufficient capacity for {}:{}: requires {} CPU, {} memory",
                    prg_id.tenant_id,
                    prg_id.partition_index,
                    required.cpu_millicores,
                    required.memory_bytes
                )
            }
            PlacementError::HardwareValidationFailed {
                prg_id,
                node_id,
                errors,
            } => {
                write!(
                    f,
                    "hardware validation failed for {}:{} on {}: {:?}",
                    prg_id.tenant_id, prg_id.partition_index, node_id, errors
                )
            }
        }
    }
}

/// Placement request for a tenant's PRGs.
#[derive(Debug, Clone)]
pub struct PlacementRequest {
    pub tenant_id: String,
    pub prg_count: u64,
    pub workload_label: String,
    pub workload_version: String,
    pub resources: WorkloadResourceProfile,
    pub hints: WorkloadPlacementHints,
    pub requirements: HardwareRequirements,
    pub capability_epoch: u64,
    /// Feature flags affecting resources.
    pub offline_queue_enabled: bool,
    pub retained_enabled: bool,
    pub dedupe_enabled: bool,
}

/// Placement planner for mixed workloads.
#[derive(Debug, Clone, Default)]
pub struct PlacementPlanner {
    /// Node inventory.
    nodes: HashMap<String, NodeHardware>,
    /// Current placements.
    placements: HashMap<PrgId, PlacementDecision>,
}

impl PlacementPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node in the inventory.
    pub fn register_node(&mut self, node: NodeHardware) {
        self.nodes.insert(node.node_id.clone(), node);
    }

    /// Remove a node from the inventory.
    pub fn remove_node(&mut self, node_id: &str) -> Option<NodeHardware> {
        self.nodes.remove(node_id)
    }

    /// Get a node by ID.
    pub fn get_node(&self, node_id: &str) -> Option<&NodeHardware> {
        self.nodes.get(node_id)
    }

    /// Get all nodes.
    pub fn nodes(&self) -> &HashMap<String, NodeHardware> {
        &self.nodes
    }

    /// Plan placements for a request.
    pub fn plan(&mut self, request: &PlacementRequest) -> PlacementPlan {
        let mut plan = PlacementPlan {
            capability_epoch: request.capability_epoch,
            ..Default::default()
        };

        // Calculate resource requirements per PRG
        let resources = request.resources.calculate(
            request.offline_queue_enabled,
            request.retained_enabled,
            request.dedupe_enabled,
        );

        // Find compatible nodes
        let compatible_nodes: Vec<_> = self
            .nodes
            .values()
            .filter(|node| {
                node.meets_requirements(&request.requirements)
                    && node.matches_hints(&request.hints)
                    && node.has_capacity()
            })
            .collect();

        if compatible_nodes.is_empty() {
            for idx in 0..request.prg_count {
                plan.errors.push(PlacementError::NoCompatibleNodes {
                    prg_id: PrgId {
                        tenant_id: request.tenant_id.clone(),
                        partition_index: idx,
                    },
                    reason: "no nodes meet requirements and hints".to_string(),
                });
            }
            return plan;
        }

        // Round-robin placement across compatible nodes
        for idx in 0..request.prg_count {
            let prg_id = PrgId {
                tenant_id: request.tenant_id.clone(),
                partition_index: idx,
            };

            // Select node (simple round-robin for now)
            let node_idx = (idx as usize) % compatible_nodes.len();
            let node = compatible_nodes[node_idx];

            // Build replica list (leader + followers)
            let mut replicas = vec![node.node_id.clone()];
            // Add additional replicas from other nodes if available
            for (i, other_node) in compatible_nodes.iter().enumerate() {
                if i != node_idx && replicas.len() < 3 {
                    replicas.push(other_node.node_id.clone());
                }
            }

            let decision = PlacementDecision {
                prg_id: prg_id.clone(),
                node_id: node.node_id.clone(),
                replicas,
                workload_label: request.workload_label.clone(),
                workload_version: request.workload_version.clone(),
                resource_profile: resources.clone(),
                capability_epoch: request.capability_epoch,
            };

            self.placements.insert(prg_id, decision.clone());
            plan.decisions.push(decision);
        }

        plan
    }

    /// Validate existing placements against current node inventory.
    pub fn validate_placements(&self) -> Vec<PlacementError> {
        let mut errors = Vec::new();

        for (prg_id, decision) in &self.placements {
            if let Some(node) = self.nodes.get(&decision.node_id) {
                // Check capacity
                if !node.has_capacity() {
                    errors.push(PlacementError::InsufficientCapacity {
                        prg_id: prg_id.clone(),
                        required: decision.resource_profile.clone(),
                    });
                }
            } else {
                errors.push(PlacementError::NoCompatibleNodes {
                    prg_id: prg_id.clone(),
                    reason: format!("node {} not found in inventory", decision.node_id),
                });
            }
        }

        errors
    }

    /// Get placement for a PRG.
    pub fn get_placement(&self, prg_id: &PrgId) -> Option<&PlacementDecision> {
        self.placements.get(prg_id)
    }

    /// Convert placements to PrgPlacement map.
    pub fn to_prg_placements(&self) -> HashMap<PrgId, PrgPlacement> {
        self.placements
            .iter()
            .map(|(id, decision)| {
                (
                    id.clone(),
                    PrgPlacement {
                        node_id: decision.node_id.clone(),
                        replicas: decision.replicas.clone(),
                    },
                )
            })
            .collect()
    }

    /// Calculate total resources needed for a workload type.
    pub fn calculate_total_resources(&self, workload_label: &str) -> WorkloadResources {
        let mut total = WorkloadResources {
            cpu_millicores: 0,
            memory_bytes: 0,
            network_bytes_per_sec: 0,
            disk_iops: 0,
            disk_bytes: 0,
        };

        for decision in self.placements.values() {
            if decision.workload_label == workload_label {
                total.cpu_millicores += decision.resource_profile.cpu_millicores;
                total.memory_bytes += decision.resource_profile.memory_bytes;
                total.network_bytes_per_sec += decision.resource_profile.network_bytes_per_sec;
                total.disk_iops += decision.resource_profile.disk_iops;
                total.disk_bytes += decision.resource_profile.disk_bytes;
            }
        }

        total
    }
}

// ============================================================================
// Capacity Calculator
// ============================================================================

/// Capacity calculation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityReport {
    /// Total resources across all workloads.
    pub total_resources: WorkloadResources,
    /// Per-workload breakdown.
    pub by_workload: HashMap<String, WorkloadResources>,
    /// Per-tenant breakdown.
    pub by_tenant: HashMap<String, WorkloadResources>,
    /// Nodes with available capacity.
    pub nodes_with_capacity: Vec<String>,
    /// Nodes at capacity.
    pub nodes_at_capacity: Vec<String>,
}

/// Capacity calculator for workload planning.
#[derive(Debug)]
pub struct CapacityCalculator<'a> {
    planner: &'a PlacementPlanner,
}

impl<'a> CapacityCalculator<'a> {
    pub fn new(planner: &'a PlacementPlanner) -> Self {
        Self { planner }
    }

    /// Generate a capacity report.
    pub fn report(&self) -> CapacityReport {
        let mut total = WorkloadResources {
            cpu_millicores: 0,
            memory_bytes: 0,
            network_bytes_per_sec: 0,
            disk_iops: 0,
            disk_bytes: 0,
        };

        let mut by_workload: HashMap<String, WorkloadResources> = HashMap::new();
        let mut by_tenant: HashMap<String, WorkloadResources> = HashMap::new();

        for (prg_id, decision) in &self.planner.placements {
            // Update totals
            total.cpu_millicores += decision.resource_profile.cpu_millicores;
            total.memory_bytes += decision.resource_profile.memory_bytes;
            total.network_bytes_per_sec += decision.resource_profile.network_bytes_per_sec;
            total.disk_iops += decision.resource_profile.disk_iops;
            total.disk_bytes += decision.resource_profile.disk_bytes;

            // Update by workload
            let workload_entry = by_workload
                .entry(decision.workload_label.clone())
                .or_insert_with(|| WorkloadResources {
                    cpu_millicores: 0,
                    memory_bytes: 0,
                    network_bytes_per_sec: 0,
                    disk_iops: 0,
                    disk_bytes: 0,
                });
            workload_entry.cpu_millicores += decision.resource_profile.cpu_millicores;
            workload_entry.memory_bytes += decision.resource_profile.memory_bytes;
            workload_entry.network_bytes_per_sec += decision.resource_profile.network_bytes_per_sec;
            workload_entry.disk_iops += decision.resource_profile.disk_iops;
            workload_entry.disk_bytes += decision.resource_profile.disk_bytes;

            // Update by tenant
            let tenant_entry = by_tenant
                .entry(prg_id.tenant_id.clone())
                .or_insert_with(|| WorkloadResources {
                    cpu_millicores: 0,
                    memory_bytes: 0,
                    network_bytes_per_sec: 0,
                    disk_iops: 0,
                    disk_bytes: 0,
                });
            tenant_entry.cpu_millicores += decision.resource_profile.cpu_millicores;
            tenant_entry.memory_bytes += decision.resource_profile.memory_bytes;
            tenant_entry.network_bytes_per_sec += decision.resource_profile.network_bytes_per_sec;
            tenant_entry.disk_iops += decision.resource_profile.disk_iops;
            tenant_entry.disk_bytes += decision.resource_profile.disk_bytes;
        }

        // Categorize nodes by capacity
        let mut nodes_with_capacity = Vec::new();
        let mut nodes_at_capacity = Vec::new();

        for node in self.planner.nodes.values() {
            if node.has_capacity() {
                nodes_with_capacity.push(node.node_id.clone());
            } else {
                nodes_at_capacity.push(node.node_id.clone());
            }
        }

        CapacityReport {
            total_resources: total,
            by_workload,
            by_tenant,
            nodes_with_capacity,
            nodes_at_capacity,
        }
    }

    /// Estimate additional resources needed for enabling a feature.
    pub fn estimate_feature_impact(
        &self,
        tenant_id: &str,
        profile: &WorkloadResourceProfile,
        feature: &str,
    ) -> WorkloadResources {
        let prg_count = self
            .planner
            .placements
            .keys()
            .filter(|id| id.tenant_id == tenant_id)
            .count() as u32;

        let modifier = match feature {
            "offline_queue" => profile.offline_queue_modifier.clone(),
            "retained" => profile.retained_modifier.clone(),
            "dedupe" => profile.dedupe_modifier.clone(),
            _ => None,
        };

        if let Some(m) = modifier {
            WorkloadResources {
                cpu_millicores: m.cpu_millicores * prg_count,
                memory_bytes: m.memory_bytes * prg_count as u64,
                network_bytes_per_sec: m.network_bytes_per_sec * prg_count as u64,
                disk_iops: m.disk_iops * prg_count,
                disk_bytes: m.disk_bytes * prg_count as u64,
            }
        } else {
            WorkloadResources::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str, disk: DiskTier, memory: u64, prgs: u32, max: u32) -> NodeHardware {
        NodeHardware {
            node_id: id.to_string(),
            disk_tier: disk,
            cpu_features: HashSet::new(),
            total_memory_bytes: memory,
            available_memory_bytes: memory,
            encryption_enabled: false,
            labels: HashSet::new(),
            region: Some("us-east".to_string()),
            prg_count: prgs,
            max_prgs: max,
        }
    }

    #[test]
    fn test_hardware_requirements_check() {
        let node = make_node("node-1", DiskTier::Ssd, 512 * 1024 * 1024, 0, 10);

        let req = HardwareRequirements {
            min_disk_tier: DiskTier::Ssd,
            min_memory_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        assert!(node.meets_requirements(&req));

        let req = HardwareRequirements {
            min_disk_tier: DiskTier::Nvme,
            ..Default::default()
        };
        assert!(!node.meets_requirements(&req));
    }

    #[test]
    fn test_placement_planning() {
        let mut planner = PlacementPlanner::new();
        planner.register_node(make_node(
            "node-1",
            DiskTier::Ssd,
            1024 * 1024 * 1024,
            0,
            10,
        ));
        planner.register_node(make_node(
            "node-2",
            DiskTier::Ssd,
            1024 * 1024 * 1024,
            0,
            10,
        ));

        let request = PlacementRequest {
            tenant_id: "tenant-1".to_string(),
            prg_count: 4,
            workload_label: "mqtt".to_string(),
            workload_version: "1.0".to_string(),
            resources: WorkloadResourceProfile::default(),
            hints: WorkloadPlacementHints::default(),
            requirements: HardwareRequirements::mqtt_default(),
            capability_epoch: 1,
            offline_queue_enabled: true,
            retained_enabled: false,
            dedupe_enabled: false,
        };

        let plan = planner.plan(&request);
        assert!(plan.errors.is_empty());
        assert_eq!(plan.decisions.len(), 4);
    }

    #[test]
    fn test_resource_calculation() {
        let profile = WorkloadResourceProfile::default();

        let base = profile.calculate(false, false, false);
        let with_all = profile.calculate(true, true, true);

        assert!(with_all.cpu_millicores > base.cpu_millicores);
        assert!(with_all.memory_bytes > base.memory_bytes);
    }

    #[test]
    fn test_capacity_report() {
        let mut planner = PlacementPlanner::new();
        planner.register_node(make_node(
            "node-1",
            DiskTier::Ssd,
            1024 * 1024 * 1024,
            0,
            10,
        ));

        let request = PlacementRequest {
            tenant_id: "tenant-1".to_string(),
            prg_count: 2,
            workload_label: "mqtt".to_string(),
            workload_version: "1.0".to_string(),
            resources: WorkloadResourceProfile::default(),
            hints: WorkloadPlacementHints::default(),
            requirements: HardwareRequirements::mqtt_default(),
            capability_epoch: 1,
            offline_queue_enabled: false,
            retained_enabled: false,
            dedupe_enabled: false,
        };

        planner.plan(&request);

        let calc = CapacityCalculator::new(&planner);
        let report = calc.report();

        assert!(report.total_resources.cpu_millicores > 0);
        assert!(report.by_workload.contains_key("mqtt"));
        assert!(report.by_tenant.contains_key("tenant-1"));
    }
}
