use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

const EMBEDDED_HTTP_BIND_DEFAULT: &str = "127.0.0.1:19000";
const EMBEDDED_RAFT_BIND_DEFAULT: &str = "127.0.0.1:19001";

fn default_require_revocation_feeds() -> bool {
    false
}

/// Top-level configuration for the Quantum runtime.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub control_plane: ControlPlaneConfig,
    pub listeners: ListenerConfig,
    pub durability: DurabilityConfig,
    pub telemetry: TelemetryConfig,
    pub tenants: TenantConfig,
    #[serde(default)]
    pub paths: PathConfig,
    #[serde(default)]
    pub dr: DrConfig,
    #[serde(default)]
    pub forward_plane: ForwardPlaneConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ControlPlaneConfig {
    pub endpoints: Vec<String>,
    pub cache_ttl_seconds: u64,
    #[serde(default)]
    pub peer_versions: Option<HashMap<String, u8>>,
    /// Require OCSP/CRL revocation feeds to be healthy before accepting control-plane responses.
    /// Disabled by default to avoid external dependencies in single-node or dev environments.
    #[serde(default = "default_require_revocation_feeds")]
    pub require_revocation_feeds: bool,
    #[serde(default = "default_control_plane_mode")]
    pub mode: ControlPlaneMode,
    #[serde(default)]
    pub embedded_http_bind: Option<String>,
    #[serde(default)]
    pub embedded_raft_bind: Option<String>,
    #[serde(default)]
    pub embedded_tls: Option<EmbeddedTlsConfig>,
    #[serde(default)]
    pub bootstrap_tenants: Vec<BootstrapTenantConfig>,
    #[serde(default)]
    pub bootstrap_placements: Vec<BootstrapPlacementConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneMode {
    Embedded,
    External,
}

impl FromStr for ControlPlaneMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "embedded" => Ok(Self::Embedded),
            "external" => Ok(Self::External),
            other => bail!("invalid control_plane.mode {}", other),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmbeddedTlsConfig {
    pub tls_chain_path: PathBuf,
    pub tls_key_path: PathBuf,
    /// Optional CA bundle to override the default trust store in embedded mode.
    #[serde(default)]
    pub client_ca_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapTenantConfig {
    pub tenant_id: String,
    #[serde(default = "default_prg_count")]
    pub tenant_prg_count: u64,
    /// Protocol assignments for this tenant.
    #[serde(default)]
    pub protocols: Vec<BootstrapProtocolConfig>,
    /// Global feature flags for this tenant.
    #[serde(default)]
    pub global_features: Vec<String>,
    /// Whether the tenant is active at bootstrap.
    #[serde(default = "default_active")]
    pub active: bool,
}

/// Bootstrap configuration for a protocol assignment.
#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapProtocolConfig {
    /// Protocol type (mqtt, amqp, kafka, or custom).
    pub protocol: String,
    /// Whether this protocol is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Feature flags for this protocol.
    #[serde(default)]
    pub features: Vec<String>,
    /// Optional PRG affinity hint.
    #[serde(default)]
    pub prg_affinity: Option<String>,
    /// Protocol priority (0-255, higher = more priority).
    #[serde(default = "default_priority")]
    pub priority: u8,
    /// Optional limits override.
    #[serde(default)]
    pub limits: Option<BootstrapProtocolLimits>,
}

/// Bootstrap limits for a protocol.
#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapProtocolLimits {
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

fn default_active() -> bool {
    true
}

fn default_enabled() -> bool {
    true
}

fn default_priority() -> u8 {
    50
}

#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapPlacementConfig {
    pub tenant_id: String,
    pub partition: u64,
    pub replicas: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ControlPlaneBootstrap {
    pub tenants: Vec<BootstrapTenantConfig>,
    pub placements: Vec<BootstrapPlacementConfig>,
}

impl BootstrapProtocolConfig {
    /// Convert to a ProtocolAssignment for capability registry integration.
    pub fn to_protocol_assignment(&self) -> crate::control::ProtocolAssignment {
        use crate::control::{
            ProtocolAssignment, ProtocolFeatureFlag, ProtocolLimits, ProtocolType,
        };

        let protocol = ProtocolType::from(self.protocol.as_str());
        let mut assignment = ProtocolAssignment::new(protocol);
        assignment.enabled = self.enabled;
        assignment.priority = self.priority;
        assignment.prg_affinity = self.prg_affinity.clone();

        // Parse feature flags
        for feat in &self.features {
            let flag = match feat.as_str() {
                "mqtt5_features" => ProtocolFeatureFlag::Mqtt5Features,
                "retained_messages" => ProtocolFeatureFlag::RetainedMessages,
                "offline_queue" => ProtocolFeatureFlag::OfflineQueue,
                "deduplication" => ProtocolFeatureFlag::Deduplication,
                "exactly_once" => ProtocolFeatureFlag::ExactlyOnce,
                "topic_acl" => ProtocolFeatureFlag::TopicAcl,
                "message_encryption" => ProtocolFeatureFlag::MessageEncryption,
                "audit_logging" => ProtocolFeatureFlag::AuditLogging,
                other => ProtocolFeatureFlag::Custom(other.to_string()),
            };
            assignment.features.insert(flag);
        }

        // Apply limits if specified
        if let Some(limits) = &self.limits {
            let mut proto_limits = ProtocolLimits::default();
            if let Some(v) = limits.max_connections {
                proto_limits.max_connections = v;
            }
            if let Some(v) = limits.max_message_size {
                proto_limits.max_message_size = v;
            }
            if let Some(v) = limits.max_messages_per_second {
                proto_limits.max_messages_per_second = v;
            }
            if let Some(v) = limits.max_bytes_per_second {
                proto_limits.max_bytes_per_second = v;
            }
            if let Some(v) = limits.max_offline_queue_bytes {
                proto_limits.max_offline_queue_bytes = v;
            }
            if let Some(v) = limits.max_retained_messages {
                proto_limits.max_retained_messages = v;
            }
            if let Some(v) = limits.max_subscriptions_per_connection {
                proto_limits.max_subscriptions_per_connection = v;
            }
            assignment.limits = proto_limits;
        }

        assignment
    }
}

impl BootstrapTenantConfig {
    /// Convert global feature strings to ProtocolFeatureFlags.
    pub fn parse_global_features(&self) -> Vec<crate::control::ProtocolFeatureFlag> {
        use crate::control::ProtocolFeatureFlag;

        self.global_features
            .iter()
            .map(|feat| match feat.as_str() {
                "mqtt5_features" => ProtocolFeatureFlag::Mqtt5Features,
                "retained_messages" => ProtocolFeatureFlag::RetainedMessages,
                "offline_queue" => ProtocolFeatureFlag::OfflineQueue,
                "deduplication" => ProtocolFeatureFlag::Deduplication,
                "exactly_once" => ProtocolFeatureFlag::ExactlyOnce,
                "topic_acl" => ProtocolFeatureFlag::TopicAcl,
                "message_encryption" => ProtocolFeatureFlag::MessageEncryption,
                "audit_logging" => ProtocolFeatureFlag::AuditLogging,
                other => ProtocolFeatureFlag::Custom(other.to_string()),
            })
            .collect()
    }
}

impl ControlPlaneConfig {
    pub fn bootstrap_seeds(&self, replica_id: &str) -> ControlPlaneBootstrap {
        if !self.bootstrap_tenants.is_empty() || !self.bootstrap_placements.is_empty() {
            return ControlPlaneBootstrap {
                tenants: self.bootstrap_tenants.clone(),
                placements: self.bootstrap_placements.clone(),
            };
        }
        ControlPlaneBootstrap {
            tenants: vec![BootstrapTenantConfig {
                tenant_id: "local".into(),
                tenant_prg_count: default_prg_count(),
                protocols: vec![BootstrapProtocolConfig {
                    protocol: "mqtt".into(),
                    enabled: true,
                    features: Vec::new(),
                    prg_affinity: None,
                    priority: default_priority(),
                    limits: None,
                }],
                global_features: Vec::new(),
                active: true,
            }],
            placements: vec![BootstrapPlacementConfig {
                tenant_id: "local".into(),
                partition: 0,
                replicas: vec![replica_id.to_string()],
            }],
        }
    }

    pub fn embedded_http_bind_or_default(&self) -> String {
        self.embedded_http_bind
            .clone()
            .unwrap_or_else(|| EMBEDDED_HTTP_BIND_DEFAULT.to_string())
    }

    pub fn embedded_raft_bind_or_default(&self) -> String {
        self.embedded_raft_bind
            .clone()
            .unwrap_or_else(|| EMBEDDED_RAFT_BIND_DEFAULT.to_string())
    }
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            cache_ttl_seconds: 0,
            peer_versions: None,
            require_revocation_feeds: default_require_revocation_feeds(),
            mode: default_control_plane_mode(),
            embedded_http_bind: None,
            embedded_raft_bind: None,
            embedded_tls: None,
            bootstrap_tenants: Vec::new(),
            bootstrap_placements: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListenerConfig {
    pub tcp: TcpConfig,
    #[serde(default)]
    pub quic: Option<QuicConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ForwardPlaneConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "ForwardPlaneConfig::default_scheme")]
    pub scheme: String,
    #[serde(default = "ForwardPlaneConfig::default_port")]
    pub port: u16,
    #[serde(default = "ForwardPlaneConfig::default_path")]
    pub path: String,
    #[serde(default = "ForwardPlaneConfig::default_timeout_ms")]
    pub timeout_ms: u64,
}

impl ForwardPlaneConfig {
    const fn default_timeout_ms() -> u64 {
        5_000
    }

    fn default_scheme() -> String {
        "http".into()
    }

    const fn default_port() -> u16 {
        21_000
    }

    fn default_path() -> String {
        "/v1/workloads/forward".into()
    }
}

impl Default for ForwardPlaneConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            scheme: Self::default_scheme(),
            port: Self::default_port(),
            path: Self::default_path(),
            timeout_ms: Self::default_timeout_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TcpConfig {
    pub bind: String,
    pub tls_chain_path: PathBuf,
    pub tls_key_path: PathBuf,
    pub client_ca_path: PathBuf,
    pub alpn: Vec<String>,
    #[serde(default = "default_tcp_require_sni")]
    pub require_sni: bool,
    #[serde(default = "default_tcp_require_alpn")]
    pub require_alpn: bool,
    #[serde(default)]
    pub default_tenant: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuicConfig {
    pub bind: String,
    pub tls_chain_path: PathBuf,
    pub tls_key_path: PathBuf,
    pub client_ca_path: PathBuf,
    #[serde(default = "default_quic_alpn")]
    pub alpn: Vec<String>,
    #[serde(default = "default_quic_max_idle")]
    pub max_idle_timeout_seconds: u64,
    #[serde(default = "default_quic_max_ack_delay_ms")]
    pub max_ack_delay_ms: u64,
    #[serde(default = "default_quic_require_sni")]
    pub require_sni: bool,
    #[serde(default = "default_quic_require_alpn")]
    pub require_alpn: bool,
    #[serde(default)]
    pub default_tenant: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DurabilityConfig {
    #[serde(default = "default_durability_mode")]
    pub durability_mode: DurabilityMode,
    #[serde(default = "default_commit_visibility")]
    pub commit_visibility: CommitVisibility,
    /// Number of replicas participating in clustor quorum (including self).
    #[serde(default = "default_quorum_size")]
    pub quorum_size: usize,
    /// Local replica identity registered with clustor.
    #[serde(default = "default_replica_id")]
    pub replica_id: String,
    /// Additional replica IDs to register for quorum tracking.
    #[serde(default)]
    pub peer_replicas: Vec<String>,
    /// Initial term used for ledger proofs; may be bumped during elections.
    #[serde(default = "default_initial_term")]
    pub initial_term: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    Strict,
    GroupFsync,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitVisibility {
    DurableOnly,
    CommitAllowsPreDurable,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    pub metrics_bind: Option<String>,
    pub log_level: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DrConfig {
    /// Optional DR mirror backend configuration.
    #[serde(default)]
    pub mirror: Option<DrMirrorConfig>,
    /// Override checkpoint shipping cadence (seconds); defaults to BackupSchedule in DrManager.
    #[serde(default)]
    pub checkpoint_interval_seconds: Option<u64>,
    /// Override WAL archive cadence (seconds).
    #[serde(default)]
    pub wal_interval_seconds: Option<u64>,
    /// Region certificate expected during DR promotion/restore.
    #[serde(default)]
    pub region_certificate: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DrMirrorConfig {
    Filesystem {
        path: PathBuf,
    },
    ObjectStore {
        bucket: PathBuf,
        #[serde(default)]
        prefix: Option<String>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct TenantConfig {
    #[serde(default)]
    pub max_tenants: Option<u64>,
    #[serde(default)]
    pub quotas: Option<QuotaLimits>,
    #[serde(default = "default_prg_count")]
    pub tenant_prg_count: u64,
    #[serde(default)]
    pub thresholds: Option<ThresholdConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PathConfig {
    /// Optional path to a control-plane certificate bundle.
    pub cp_certs: Option<PathBuf>,
    /// Optional local override for the telemetry catalog.
    pub telemetry_catalog: Option<PathBuf>,
    /// Optional path for clustor-backed PRG storage snapshots.
    pub storage_dir: Option<PathBuf>,
}

/// Per-tenant quota limits.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct QuotaLimits {
    pub max_sessions_per_tenant: Option<u64>,
    pub max_publishes_per_second: Option<u64>,
    pub max_retained_bytes: Option<u64>,
    pub max_dedupe_entries: Option<u64>,
    /// Optional cap on offline queue entries per session.
    pub max_offline_entries: Option<u64>,
    /// Optional cap on offline payload bytes per session.
    pub max_offline_bytes: Option<u64>,
}

/// Backpressure thresholds for flow control surfaces.
#[derive(Debug, Clone, Deserialize)]
pub struct ThresholdConfig {
    #[serde(default = "default_commit_to_apply_pause_ack")]
    pub commit_to_apply_pause_ack: u64,
    #[serde(default = "default_apply_to_delivery_drop_qos0")]
    pub apply_to_delivery_drop_qos0: u64,
    #[serde(default = "default_max_apply_lag_ready")]
    pub max_apply_lag_seconds_ready: u64,
    #[serde(default = "default_max_replication_lag_ready")]
    pub max_replication_lag_seconds_ready: u64,
}

impl Default for ThresholdConfig {
    fn default() -> Self {
        Self {
            commit_to_apply_pause_ack: default_commit_to_apply_pause_ack(),
            apply_to_delivery_drop_qos0: default_apply_to_delivery_drop_qos0(),
            max_apply_lag_seconds_ready: default_max_apply_lag_ready(),
            max_replication_lag_seconds_ready: default_max_replication_lag_ready(),
        }
    }
}

fn default_prg_count() -> u64 {
    1
}

impl Config {
    /// Load configuration from a path resolved via QUANTUM_CONFIG or defaults to `config/quantum.toml`.
    /// Applies QUANTUM_CP_MODE to override `control_plane.mode` after parsing.
    pub fn load_from_env() -> Result<Self> {
        let path = env_config_path();
        let mut cfg = Self::load(&path)?;
        cfg.apply_env_overrides()?;
        Ok(cfg)
    }

    /// Load configuration from a specific file (TOML or JSON based on extension).
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path_ref = path.as_ref();
        let data = fs::read_to_string(path_ref)
            .with_context(|| format!("unable to read config {}", path_ref.display()))?;
        if is_json(path_ref) {
            Ok(serde_json::from_str(&data)
                .with_context(|| format!("invalid JSON config {}", path_ref.display()))?)
        } else {
            Ok(toml::from_str(&data)
                .with_context(|| format!("invalid TOML config {}", path_ref.display()))?)
        }
    }

    /// Lightweight validation to ensure optional filesystem paths exist before startup.
    pub fn validate_paths(&self) -> Result<()> {
        if let Some(dir) = &self.paths.storage_dir {
            if !dir.exists() {
                anyhow::bail!("storage_dir {} missing", dir.display());
            }
        }
        if let Some(bundle) = &self.paths.cp_certs {
            if !bundle.exists() {
                anyhow::bail!("cp_certs bundle {} missing", bundle.display());
            }
        }
        if let Some(tls) = &self.control_plane.embedded_tls {
            if !tls.tls_chain_path.exists() {
                anyhow::bail!(
                    "embedded tls_chain_path {} missing",
                    tls.tls_chain_path.display()
                );
            }
            if !tls.tls_key_path.exists() {
                anyhow::bail!(
                    "embedded tls_key_path {} missing",
                    tls.tls_key_path.display()
                );
            }
            if let Some(ca) = &tls.client_ca_path {
                if !ca.exists() {
                    anyhow::bail!("embedded client_ca_path {} missing", ca.display());
                }
            }
        }
        if let Some(catalog) = &self.paths.telemetry_catalog {
            if !catalog.exists() {
                anyhow::bail!("telemetry_catalog {} missing", catalog.display());
            }
        }
        Ok(())
    }

    /// Validate schema-level invariants for ops usage.
    pub fn validate(&self) -> Result<()> {
        self.validate_paths()?;
        if self.tenants.tenant_prg_count == 0 {
            bail!("tenant_prg_count must be > 0");
        }
        if self.durability.quorum_size == 0 {
            bail!("durability.quorum_size must be > 0");
        }
        if self.durability.replica_id.is_empty() {
            bail!("durability.replica_id must be non-empty");
        }
        match self.control_plane.mode {
            ControlPlaneMode::External => {
                bail!("control_plane.external mode is no longer supported; use embedded mode");
            }
            ControlPlaneMode::Embedded => {
                let raft_bind = self.control_plane.embedded_raft_bind_or_default();
                let http_bind = self.control_plane.embedded_http_bind_or_default();
                let raft_ephemeral = is_ephemeral(&raft_bind);
                let http_ephemeral = is_ephemeral(&http_bind);
                if (http_bind == raft_bind && !(raft_ephemeral || http_ephemeral))
                    || bind_conflicts(&raft_bind, &http_bind)
                {
                    bail!("embedded_raft_bind must differ from embedded_http_bind");
                }
                if bind_conflicts(&raft_bind, &self.listeners.tcp.bind) {
                    bail!("embedded_raft_bind conflicts with TCP listener bind");
                }
                if let Some(quic) = &self.listeners.quic {
                    if bind_conflicts(&raft_bind, &quic.bind) {
                        bail!("embedded_raft_bind conflicts with QUIC listener bind");
                    }
                }
                if let Some(metrics) = &self.telemetry.metrics_bind {
                    if bind_conflicts(&raft_bind, metrics) {
                        bail!("embedded_raft_bind conflicts with telemetry bind");
                    }
                }
            }
        }
        if let Some(th) = &self.tenants.thresholds {
            if th.commit_to_apply_pause_ack == 0 || th.apply_to_delivery_drop_qos0 == 0 {
                bail!("thresholds must be non-zero");
            }
        }
        Ok(())
    }

    fn apply_env_overrides(&mut self) -> Result<()> {
        if let Ok(mode) = std::env::var("QUANTUM_CP_MODE") {
            self.control_plane.mode = ControlPlaneMode::from_str(&mode)?;
        }
        if let Ok(http_bind) = std::env::var("QUANTUM_CP_HTTP_BIND") {
            self.control_plane.embedded_http_bind = Some(http_bind);
        }
        if let Ok(raft_bind) = std::env::var("QUANTUM_CP_RAFT_BIND") {
            self.control_plane.embedded_raft_bind = Some(raft_bind);
        }
        Ok(())
    }
}

fn env_config_path() -> PathBuf {
    if let Ok(path) = std::env::var("QUANTUM_CONFIG") {
        PathBuf::from(path)
    } else {
        PathBuf::from("config/quantum.toml")
    }
}

fn is_json(path: &Path) -> bool {
    matches!(path.extension().and_then(|s| s.to_str()), Some("json"))
}

fn default_durability_mode() -> DurabilityMode {
    DurabilityMode::Strict
}

fn default_control_plane_mode() -> ControlPlaneMode {
    ControlPlaneMode::Embedded
}

fn default_commit_visibility() -> CommitVisibility {
    CommitVisibility::DurableOnly
}

fn default_quorum_size() -> usize {
    1
}

fn default_replica_id() -> String {
    "local".into()
}

fn default_initial_term() -> u64 {
    1
}

fn default_quic_max_idle() -> u64 {
    360
}

fn default_quic_max_ack_delay_ms() -> u64 {
    25
}

fn default_quic_alpn() -> Vec<String> {
    vec!["mqtt-quic".into()]
}

fn default_quic_require_sni() -> bool {
    true
}

fn default_quic_require_alpn() -> bool {
    true
}

fn default_tcp_require_sni() -> bool {
    true
}

fn default_tcp_require_alpn() -> bool {
    true
}

fn default_commit_to_apply_pause_ack() -> u64 {
    10_000
}

fn default_apply_to_delivery_drop_qos0() -> u64 {
    5_000
}

fn bind_conflicts(a: &str, b: &str) -> bool {
    match (
        a.parse::<std::net::SocketAddr>(),
        b.parse::<std::net::SocketAddr>(),
    ) {
        (Ok(a_addr), Ok(b_addr)) => a_addr.port() != 0 && b_addr.port() != 0 && a_addr == b_addr,
        _ => a == b,
    }
}

fn is_ephemeral(bind: &str) -> bool {
    bind.parse::<std::net::SocketAddr>()
        .map(|addr| addr.port() == 0)
        .unwrap_or(false)
}

fn default_max_apply_lag_ready() -> u64 {
    10
}

fn default_max_replication_lag_ready() -> u64 {
    10
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            durability_mode: default_durability_mode(),
            commit_visibility: default_commit_visibility(),
            quorum_size: default_quorum_size(),
            replica_id: default_replica_id(),
            peer_replicas: Vec::new(),
            initial_term: default_initial_term(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_dummy(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, "dummy").unwrap();
        path
    }

    fn base_config(
        mode: &str,
        quorum_size: usize,
        chain: &Path,
        key: &Path,
        client_ca: &Path,
        cp_certs: Option<&Path>,
    ) -> Config {
        let cp_certs_line = cp_certs
            .map(|p| format!("cp_certs = \"{}\"", p.display()))
            .unwrap_or_default();
        let doc = format!(
            r#"
[control_plane]
endpoints = ["cp.local:19000"]
cache_ttl_seconds = 5
mode = "{mode}"

[listeners.tcp]
bind = "0.0.0.0:1883"
tls_chain_path = "{chain}"
tls_key_path = "{key}"
client_ca_path = "{client_ca}"
alpn = ["mqtt"]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = {quorum_size}
replica_id = "node-0"
initial_term = 1

[telemetry]
metrics_bind = "0.0.0.0:9090"
log_level = "info"

[tenants]
tenant_prg_count = 1

[paths]
{cp_certs_line}
"#,
            chain = chain.display(),
            key = key.display(),
            client_ca = client_ca.display(),
            quorum_size = quorum_size,
            mode = mode,
            cp_certs_line = cp_certs_line
        );
        toml::from_str(&doc).unwrap()
    }

    #[test]
    fn external_mode_rejected() {
        let dir = tempdir().unwrap();
        let chain = write_dummy(dir.path(), "server.pem");
        let key = write_dummy(dir.path(), "server.key");
        let ca = write_dummy(dir.path(), "ca.pem");
        let cp_bundle = write_dummy(dir.path(), "cp-ca.pem");
        let cfg = base_config("external", 2, &chain, &key, &ca, Some(&cp_bundle));
        let err = cfg.validate().unwrap_err();
        assert!(format!("{err:?}").contains("external mode is no longer supported"));
    }

    #[test]
    fn embedded_mode_allows_single_node_quorum() {
        let dir = tempdir().unwrap();
        let chain = write_dummy(dir.path(), "server.pem");
        let key = write_dummy(dir.path(), "server.key");
        let ca = write_dummy(dir.path(), "ca.pem");
        let cfg = base_config("embedded", 1, &chain, &key, &ca, None);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn env_override_rejects_external_mode() {
        let dir = tempdir().unwrap();
        let chain = write_dummy(dir.path(), "server.pem");
        let key = write_dummy(dir.path(), "server.key");
        let ca = write_dummy(dir.path(), "ca.pem");
        let cp_bundle = write_dummy(dir.path(), "cp-ca.pem");
        let mut cfg = base_config("embedded", 3, &chain, &key, &ca, Some(&cp_bundle));
        std::env::set_var("QUANTUM_CP_MODE", "external");
        cfg.apply_env_overrides().unwrap();
        std::env::remove_var("QUANTUM_CP_MODE");
        let err = cfg.validate().unwrap_err();
        assert!(format!("{err:?}").contains("external mode is no longer supported"));
    }
}
