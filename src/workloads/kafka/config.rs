//! Kafka workload configuration.
//!
//! This module provides configuration types for the Kafka protocol adapter,
//! including multi-PRG coordination and cluster metadata generation.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

// ---------------------------------------------------------------------------
// Kafka Configuration
// ---------------------------------------------------------------------------

/// Kafka workload configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaWorkloadConfig {
    /// Enable Kafka protocol support.
    pub enabled: bool,

    /// Bind address for Kafka listener.
    pub bind: SocketAddr,

    /// Advertised host for Kafka clients (if different from bind).
    pub advertised_host: Option<String>,

    /// Advertised port for Kafka clients (if different from bind).
    pub advertised_port: Option<u16>,

    /// Default number of partitions for auto-created topics.
    pub default_partitions: u32,

    /// Default replication factor for new topics.
    pub default_replication_factor: u16,

    /// Enable auto-creation of topics on produce/fetch.
    pub auto_create_topics: bool,

    /// Maximum message size in bytes.
    pub max_message_bytes: usize,

    /// Maximum request size in bytes.
    pub max_request_size: usize,

    /// Consumer group session timeout in milliseconds.
    pub session_timeout_ms: u32,

    /// Consumer group rebalance timeout in milliseconds.
    pub rebalance_timeout_ms: u32,

    /// Transaction timeout in milliseconds.
    pub transaction_timeout_ms: u32,

    /// SASL configuration.
    pub sasl: SaslConfig,

    /// TLS configuration (optional).
    pub tls: Option<TlsConfig>,

    /// Rate limiting configuration.
    pub rate_limit: RateLimitConfig,

    /// Multi-PRG coordination settings.
    pub coordination: CoordinationConfig,
}

impl Default for KafkaWorkloadConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "0.0.0.0:9092".parse().unwrap(),
            advertised_host: None,
            advertised_port: None,
            default_partitions: 1,
            default_replication_factor: 1,
            auto_create_topics: true,
            max_message_bytes: 1_048_576,        // 1MB
            max_request_size: 100 * 1024 * 1024, // 100MB
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 60_000,
            transaction_timeout_ms: 60_000,
            sasl: SaslConfig::default(),
            tls: None,
            rate_limit: RateLimitConfig::default(),
            coordination: CoordinationConfig::default(),
        }
    }
}

impl KafkaWorkloadConfig {
    /// Get the advertised host (or bind host if not specified).
    pub fn advertised_host(&self) -> &str {
        self.advertised_host.as_deref().unwrap_or("localhost")
    }

    /// Get the advertised port (or bind port if not specified).
    pub fn advertised_port(&self) -> u16 {
        self.advertised_port.unwrap_or(self.bind.port())
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.default_partitions == 0 {
            return Err(ConfigError::InvalidValue(
                "default_partitions must be > 0".to_string(),
            ));
        }
        if self.default_replication_factor == 0 {
            return Err(ConfigError::InvalidValue(
                "default_replication_factor must be > 0".to_string(),
            ));
        }
        if self.max_message_bytes == 0 {
            return Err(ConfigError::InvalidValue(
                "max_message_bytes must be > 0".to_string(),
            ));
        }
        if self.session_timeout_ms < 1000 {
            return Err(ConfigError::InvalidValue(
                "session_timeout_ms must be >= 1000".to_string(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SASL Configuration
// ---------------------------------------------------------------------------

/// SASL authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SaslConfig {
    /// Enable SASL authentication.
    pub enabled: bool,

    /// Enabled SASL mechanisms.
    pub mechanisms: Vec<String>,

    /// Allow anonymous authentication (no SASL).
    pub allow_anonymous: bool,
}

impl Default for SaslConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mechanisms: vec!["PLAIN".to_string()],
            allow_anonymous: false,
        }
    }
}

// ---------------------------------------------------------------------------
// TLS Configuration
// ---------------------------------------------------------------------------

/// TLS configuration for Kafka listener.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to certificate file (PEM).
    pub cert_file: String,

    /// Path to private key file (PEM).
    pub key_file: String,

    /// Path to CA certificate file (for client verification).
    pub ca_file: Option<String>,

    /// Require client certificate.
    pub require_client_cert: bool,
}

// ---------------------------------------------------------------------------
// Rate Limiting Configuration
// ---------------------------------------------------------------------------

/// Rate limiting configuration for Kafka.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    /// Enable rate limiting.
    pub enabled: bool,

    /// Maximum produce requests per second per connection.
    pub produce_rate: u32,

    /// Maximum fetch requests per second per connection.
    pub fetch_rate: u32,

    /// Maximum bytes per second for produce (per connection).
    pub produce_bytes_rate: u64,

    /// Maximum bytes per second for fetch (per connection).
    pub fetch_bytes_rate: u64,

    /// Request throttle time in milliseconds when rate exceeded.
    pub throttle_time_ms: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            produce_rate: 1000,
            fetch_rate: 1000,
            produce_bytes_rate: 50 * 1024 * 1024, // 50MB/s
            fetch_bytes_rate: 100 * 1024 * 1024,  // 100MB/s
            throttle_time_ms: 100,
        }
    }
}

// ---------------------------------------------------------------------------
// Multi-PRG Coordination Configuration
// ---------------------------------------------------------------------------

/// Configuration for multi-PRG coordination.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CoordinationConfig {
    /// Partition assignment strategy.
    pub partition_strategy: PartitionStrategy,

    /// Enable cross-PRG routing for produce requests.
    pub cross_prg_produce: bool,

    /// Enable cross-PRG routing for fetch requests.
    pub cross_prg_fetch: bool,

    /// Maximum partitions per PRG.
    pub max_partitions_per_prg: u32,

    /// Coordinator assignment timeout in milliseconds.
    pub coordinator_timeout_ms: u32,
}

impl Default for CoordinationConfig {
    fn default() -> Self {
        Self {
            partition_strategy: PartitionStrategy::JumpHash,
            cross_prg_produce: true,
            cross_prg_fetch: true,
            max_partitions_per_prg: 1000,
            coordinator_timeout_ms: 5000,
        }
    }
}

/// Partition assignment strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    /// Jump consistent hashing (default).
    JumpHash,
    /// Round-robin assignment.
    RoundRobin,
    /// Manual assignment via configuration.
    Manual,
}

impl Default for PartitionStrategy {
    fn default() -> Self {
        Self::JumpHash
    }
}

// ---------------------------------------------------------------------------
// Partition to PRG Mapping
// ---------------------------------------------------------------------------

/// Maps Kafka topic partitions to PRGs.
#[derive(Debug, Clone, Default)]
pub struct PartitionRouter {
    /// Number of PRGs in the cluster.
    num_prgs: u32,
    /// Manual partition assignments (topic -> partition -> prg_index).
    manual_assignments: HashMap<String, HashMap<i32, u32>>,
    /// Strategy for automatic assignment.
    strategy: PartitionStrategy,
}

impl PartitionRouter {
    /// Create a new partition router.
    pub fn new(num_prgs: u32, strategy: PartitionStrategy) -> Self {
        Self {
            num_prgs,
            manual_assignments: HashMap::new(),
            strategy,
        }
    }

    /// Get the PRG index for a topic-partition.
    pub fn get_prg(&self, topic: &str, partition: i32) -> u32 {
        // Check for manual assignment first
        if let Some(topic_map) = self.manual_assignments.get(topic) {
            if let Some(&prg) = topic_map.get(&partition) {
                return prg;
            }
        }

        // Use strategy for automatic assignment
        match self.strategy {
            PartitionStrategy::JumpHash => self.jump_hash(topic, partition),
            PartitionStrategy::RoundRobin => self.round_robin(topic, partition),
            PartitionStrategy::Manual => 0, // Fallback to PRG 0 if not assigned
        }
    }

    /// Set a manual assignment for a topic-partition.
    pub fn set_assignment(&mut self, topic: &str, partition: i32, prg_index: u32) {
        self.manual_assignments
            .entry(topic.to_string())
            .or_default()
            .insert(partition, prg_index);
    }

    /// Jump consistent hash for partition assignment.
    fn jump_hash(&self, topic: &str, partition: i32) -> u32 {
        // Combine topic and partition into a key
        let key = format!("{}:{}", topic, partition);
        let hash = self.fnv1a_hash(key.as_bytes());
        self.jump_consistent_hash(hash, self.num_prgs)
    }

    /// Round-robin assignment based on partition number.
    fn round_robin(&self, _topic: &str, partition: i32) -> u32 {
        (partition as u32) % self.num_prgs
    }

    /// FNV-1a hash function.
    fn fnv1a_hash(&self, data: &[u8]) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0100_0000_01b3;

        let mut hash = FNV_OFFSET;
        for byte in data {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    /// Jump consistent hash algorithm.
    fn jump_consistent_hash(&self, mut key: u64, num_buckets: u32) -> u32 {
        if num_buckets == 0 {
            return 0;
        }

        let mut b: i64 = -1;
        let mut j: i64 = 0;

        while j < num_buckets as i64 {
            b = j;
            key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
            j = ((b.wrapping_add(1) as f64) * (1i64 << 31) as f64
                / ((key >> 33).wrapping_add(1) as f64)) as i64;
        }

        b as u32
    }

    /// Get all partitions assigned to a specific PRG.
    pub fn partitions_for_prg(&self, prg_index: u32, topics: &[(&str, i32)]) -> Vec<(String, i32)> {
        topics
            .iter()
            .filter_map(|&(topic, partition)| {
                if self.get_prg(topic, partition) == prg_index {
                    Some((topic.to_string(), partition))
                } else {
                    None
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Cluster Metadata Generator
// ---------------------------------------------------------------------------

/// Generates Kafka cluster metadata for clients.
#[derive(Debug, Clone)]
pub struct ClusterMetadata {
    /// Cluster ID.
    pub cluster_id: String,
    /// Controller node ID.
    pub controller_id: i32,
    /// Broker information.
    pub brokers: Vec<BrokerInfo>,
    /// Topic metadata.
    pub topics: HashMap<String, TopicInfo>,
}

/// Broker information for metadata response.
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    /// Node ID.
    pub node_id: i32,
    /// Host address.
    pub host: String,
    /// Port number.
    pub port: i32,
    /// Rack identifier (optional).
    pub rack: Option<String>,
}

/// Topic information for metadata response.
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Topic is internal.
    pub is_internal: bool,
    /// Partition information.
    pub partitions: Vec<PartitionInfo>,
}

/// Partition information for metadata response.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition index.
    pub partition: i32,
    /// Leader node ID.
    pub leader: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Replica node IDs.
    pub replicas: Vec<i32>,
    /// In-sync replica node IDs.
    pub isr: Vec<i32>,
    /// Offline replica node IDs.
    pub offline_replicas: Vec<i32>,
}

impl ClusterMetadata {
    /// Create new cluster metadata.
    pub fn new(cluster_id: String, controller_id: i32) -> Self {
        Self {
            cluster_id,
            controller_id,
            brokers: Vec::new(),
            topics: HashMap::new(),
        }
    }

    /// Add a broker to the metadata.
    pub fn add_broker(&mut self, broker: BrokerInfo) {
        self.brokers.push(broker);
    }

    /// Add a topic to the metadata.
    pub fn add_topic(&mut self, topic: TopicInfo) {
        self.topics.insert(topic.name.clone(), topic);
    }

    /// Get broker by node ID.
    pub fn get_broker(&self, node_id: i32) -> Option<&BrokerInfo> {
        self.brokers.iter().find(|b| b.node_id == node_id)
    }
}

// ---------------------------------------------------------------------------
// Coordinator Discovery
// ---------------------------------------------------------------------------

/// Coordinator types for FindCoordinator requests (used in routing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorKind {
    /// Consumer group coordinator.
    Group,
    /// Transaction coordinator.
    Transaction,
}

/// Coordinator discovery service.
#[derive(Debug, Clone)]
pub struct CoordinatorDiscovery {
    /// Number of PRGs for coordinator assignment.
    num_prgs: u32,
    /// Broker info for each PRG.
    prg_brokers: HashMap<u32, BrokerInfo>,
}

impl CoordinatorDiscovery {
    /// Create a new coordinator discovery service.
    pub fn new(num_prgs: u32) -> Self {
        Self {
            num_prgs,
            prg_brokers: HashMap::new(),
        }
    }

    /// Register a broker for a PRG.
    pub fn register_broker(&mut self, prg_index: u32, broker: BrokerInfo) {
        self.prg_brokers.insert(prg_index, broker);
    }

    /// Find the coordinator for a group or transaction.
    pub fn find_coordinator(
        &self,
        key: &str,
        coordinator_type: CoordinatorKind,
    ) -> Option<&BrokerInfo> {
        let prg = self.coordinator_prg(key, coordinator_type);
        self.prg_brokers.get(&prg)
    }

    /// Get the PRG that coordinates a given key.
    fn coordinator_prg(&self, key: &str, _coordinator_type: CoordinatorKind) -> u32 {
        // Use hash-based assignment
        let hash = self.hash_key(key);
        hash % self.num_prgs
    }

    /// Simple hash function for coordinator assignment.
    fn hash_key(&self, key: &str) -> u32 {
        let mut hash: u32 = 0;
        for byte in key.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
        }
        hash
    }
}

// ---------------------------------------------------------------------------
// Configuration Errors
// ---------------------------------------------------------------------------

/// Configuration error type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// Invalid configuration value.
    InvalidValue(String),
    /// Missing required field.
    MissingField(String),
    /// IO error.
    IoError(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidValue(msg) => write!(f, "invalid configuration value: {}", msg),
            ConfigError::MissingField(field) => write!(f, "missing required field: {}", field),
            ConfigError::IoError(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = KafkaWorkloadConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.default_partitions, 1);
        assert!(config.auto_create_topics);
    }

    #[test]
    fn test_config_validation() {
        let mut config = KafkaWorkloadConfig::default();
        assert!(config.validate().is_ok());

        config.default_partitions = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_partition_router_jump_hash() {
        let router = PartitionRouter::new(10, PartitionStrategy::JumpHash);

        // Same topic-partition should always map to same PRG
        let prg1 = router.get_prg("test-topic", 0);
        let prg2 = router.get_prg("test-topic", 0);
        assert_eq!(prg1, prg2);

        // Should be within bounds
        for p in 0..100 {
            let prg = router.get_prg("test-topic", p);
            assert!(prg < 10);
        }
    }

    #[test]
    fn test_partition_router_round_robin() {
        let router = PartitionRouter::new(3, PartitionStrategy::RoundRobin);

        assert_eq!(router.get_prg("topic", 0), 0);
        assert_eq!(router.get_prg("topic", 1), 1);
        assert_eq!(router.get_prg("topic", 2), 2);
        assert_eq!(router.get_prg("topic", 3), 0);
    }

    #[test]
    fn test_partition_router_manual_assignment() {
        let mut router = PartitionRouter::new(10, PartitionStrategy::JumpHash);

        // Before manual assignment - partition 0 might hash to any PRG
        let auto_prg_p0 = router.get_prg("special-topic", 0);

        // Set manual assignment to a DIFFERENT value than what auto would give
        let manual_prg = (auto_prg_p0 + 5) % 10;
        router.set_assignment("special-topic", 0, manual_prg);

        // After manual assignment should return manual value
        assert_eq!(router.get_prg("special-topic", 0), manual_prg);

        // Partition 1 still uses automatic assignment (not affected by manual)
        let auto_prg_p1 =
            PartitionRouter::new(10, PartitionStrategy::JumpHash).get_prg("special-topic", 1);
        assert_eq!(router.get_prg("special-topic", 1), auto_prg_p1);
    }

    #[test]
    fn test_coordinator_discovery() {
        let mut discovery = CoordinatorDiscovery::new(3);

        discovery.register_broker(
            0,
            BrokerInfo {
                node_id: 1,
                host: "broker-0".to_string(),
                port: 9092,
                rack: None,
            },
        );
        discovery.register_broker(
            1,
            BrokerInfo {
                node_id: 2,
                host: "broker-1".to_string(),
                port: 9092,
                rack: None,
            },
        );

        // Same key should always return same coordinator
        let coord1 = discovery.find_coordinator("my-group", CoordinatorKind::Group);
        let coord2 = discovery.find_coordinator("my-group", CoordinatorKind::Group);

        if let (Some(b1), Some(b2)) = (coord1, coord2) {
            assert_eq!(b1.node_id, b2.node_id);
        }
    }

    #[test]
    fn test_cluster_metadata() {
        let mut metadata = ClusterMetadata::new("test-cluster".to_string(), 1);

        metadata.add_broker(BrokerInfo {
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            rack: None,
        });

        metadata.add_topic(TopicInfo {
            name: "test-topic".to_string(),
            is_internal: false,
            partitions: vec![PartitionInfo {
                partition: 0,
                leader: 1,
                leader_epoch: 0,
                replicas: vec![1],
                isr: vec![1],
                offline_replicas: vec![],
            }],
        });

        assert_eq!(metadata.cluster_id, "test-cluster");
        assert!(metadata.get_broker(1).is_some());
        assert!(metadata.topics.contains_key("test-topic"));
    }

    #[test]
    fn test_rate_limit_config_default() {
        let config = RateLimitConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.produce_rate, 1000);
    }
}
