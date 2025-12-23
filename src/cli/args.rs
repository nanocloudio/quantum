//! CLI argument definitions using clap.

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

/// Quantum - distributed Raft-based multi-tenant MQTT broker.
#[derive(Parser)]
#[command(name = "quantum")]
#[command(version)]
#[command(about = "Quantum MQTT broker and diagnostic tools")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the Quantum broker
    Start(StartArgs),

    /// Initialize/seed workload data from fixtures
    Init(InitArgs),

    /// Subscribe to MQTT topics and stream messages to stdout (kcat-style)
    Subscribe(SubscribeArgs),

    /// Publish MQTT messages from stdin or command line (kcat-style)
    Publish(PublishArgs),

    /// Inspect WAL segments and storage
    Inspect(InspectArgs),

    /// Snapshot inspection and listing
    Snapshot(SnapshotArgs),

    /// Workload schema tooling
    Workload(WorkloadArgs),

    /// Telemetry replay and fetching
    Telemetry(TelemetryArgs),

    /// Overload simulation
    Simulator(SimulatorArgs),

    /// Chaos scenario planning
    Chaos(ChaosArgs),

    /// Synthetic probe generation
    Synthetic(SyntheticArgs),
}

// -----------------------------------------------------------------------------
// Start command
// -----------------------------------------------------------------------------

#[derive(Args)]
pub struct StartArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "config/quantum.toml")]
    pub config: PathBuf,
}

// -----------------------------------------------------------------------------
// Init command (seed workload data)
// -----------------------------------------------------------------------------

#[derive(Args)]
pub struct InitArgs {
    /// Workload identifier to seed (matches manifest.workload)
    #[arg(long, default_value = "mqtt")]
    pub workload: String,

    /// Path to a manifest.json file or directory containing it
    #[arg(long, default_value = "data/fixtures/mqtt")]
    pub fixtures: PathBuf,

    /// Target data root that receives PRG/capability files
    #[arg(long, default_value = "data")]
    pub data_root: PathBuf,

    /// Limit seeding to specific tenants (can be repeated)
    #[arg(long, action = clap::ArgAction::Append)]
    pub tenant: Vec<String>,

    /// Validate fixtures but do not write any files
    #[arg(long)]
    pub dry_run: bool,
}

// -----------------------------------------------------------------------------
// Subscribe/Publish commands (kcat-style MQTT client)
// -----------------------------------------------------------------------------

/// Common mTLS connection arguments shared by subscribe/publish commands.
#[derive(Args, Clone)]
pub struct TlsArgs {
    /// Broker hostname or IP
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Broker TLS port
    #[arg(long, default_value_t = 8883)]
    pub port: u16,

    /// PEM file containing the client certificate chain
    #[arg(long, value_name = "PATH")]
    pub cert: PathBuf,

    /// PEM file containing the client private key
    #[arg(long, value_name = "PATH")]
    pub key: PathBuf,

    /// PEM file containing the broker CA certificate
    #[arg(long, value_name = "PATH")]
    pub ca: PathBuf,

    /// MQTT client identifier (auto-generated if not specified)
    #[arg(long)]
    pub client_id: Option<String>,
}

/// Output format for subscribe command.
#[derive(clap::ValueEnum, Clone, Default)]
pub enum OutputFormat {
    /// JSON objects, one per line
    #[default]
    Json,
    /// Raw payload only (no metadata)
    Raw,
}

/// QoS level for MQTT operations.
#[derive(clap::ValueEnum, Clone, Default)]
pub enum QosLevel {
    /// At most once (fire and forget)
    Qos0,
    /// At least once (acknowledged delivery)
    #[default]
    Qos1,
    /// Exactly once (assured delivery)
    Qos2,
}

#[derive(Args)]
pub struct SubscribeArgs {
    #[command(flatten)]
    pub tls: TlsArgs,

    /// Comma-separated list of topics to subscribe to
    #[arg(long, value_delimiter = ',', required = true)]
    pub topic: Vec<String>,

    /// QoS level for subscriptions
    #[arg(long, value_enum, default_value = "qos1")]
    pub qos: QosLevel,

    /// Output format
    #[arg(long, value_enum, default_value = "json")]
    pub format: OutputFormat,
}

/// Parse delimiter argument supporting escape sequences and hex notation.
///
/// Supported formats:
/// - `\t` - tab character
/// - `\n` - newline character
/// - `\r` - carriage return
/// - `0x1f` or `0X1F` - hex byte value (must be valid UTF-8)
/// - Any other string is used literally
///
/// Note: MQTT topics cannot contain wildcard characters `+` or `#`.
/// If your topics contain colons, use `--delimiter '\t'` or `--delimiter 0x1f`.
fn parse_delimiter(s: &str) -> Result<String, String> {
    match s {
        "\\t" => Ok("\t".to_string()),
        "\\n" => Ok("\n".to_string()),
        "\\r" => Ok("\r".to_string()),
        s if s.starts_with("0x") || s.starts_with("0X") => {
            let hex = &s[2..];
            let byte =
                u8::from_str_radix(hex, 16).map_err(|_| format!("invalid hex delimiter: {s}"))?;
            Ok(String::from_utf8(vec![byte])
                .map_err(|_| format!("delimiter byte {s} is not valid UTF-8"))?)
        }
        _ => Ok(s.to_string()),
    }
}

#[derive(Args)]
pub struct PublishArgs {
    #[command(flatten)]
    pub tls: TlsArgs,

    /// Topic to publish to (required for single-message mode)
    #[arg(long)]
    pub topic: Option<String>,

    /// Message payload (if omitted, reads from stdin)
    #[arg(long, requires = "topic")]
    pub message: Option<String>,

    /// Delimiter for stdin multi-topic mode (topic<delim>payload)
    #[arg(long, default_value = ":", value_parser = parse_delimiter)]
    pub delimiter: String,

    /// QoS level for publishes
    #[arg(long, value_enum, default_value = "qos1")]
    pub qos: QosLevel,

    /// Set retain flag on published messages
    #[arg(long)]
    pub retain: bool,

    /// Decode payload as base64 (for binary data in stdin multi-topic mode)
    #[arg(long)]
    pub binary: bool,

    /// Output format for acknowledgements/errors
    #[arg(long, value_enum, default_value = "json")]
    pub format: OutputFormat,
}

// -----------------------------------------------------------------------------
// Inspect command (WAL inspector)
// -----------------------------------------------------------------------------

#[derive(Args)]
pub struct InspectArgs {
    #[command(subcommand)]
    pub action: InspectAction,
}

#[derive(Subcommand)]
pub enum InspectAction {
    /// Inspect WAL segments
    Wal(WalInspectArgs),
}

#[derive(Args)]
pub struct WalInspectArgs {
    /// WAL segment files or directories to inspect
    #[arg(required = true)]
    pub paths: Vec<PathBuf>,
}

// -----------------------------------------------------------------------------
// Snapshot command
// -----------------------------------------------------------------------------

#[derive(Args)]
pub struct SnapshotArgs {
    #[command(subcommand)]
    pub action: SnapshotAction,
}

#[derive(Subcommand)]
pub enum SnapshotAction {
    /// Inspect a snapshot manifest
    Inspect(SnapshotInspectArgs),
    /// List snapshots in a directory
    List(SnapshotListArgs),
}

#[derive(Args)]
pub struct SnapshotInspectArgs {
    /// Manifest path or snapshot directory
    pub target: PathBuf,
    /// Emit JSON instead of table output
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct SnapshotListArgs {
    /// Base storage directory containing prg_*/snapshots
    pub base: PathBuf,
    /// Optional tenant filter
    #[arg(long)]
    pub tenant: Option<String>,
    /// Emit JSON instead of table output
    #[arg(long)]
    pub json: bool,
}

// -----------------------------------------------------------------------------
// Workload command
// -----------------------------------------------------------------------------

#[derive(Args)]
pub struct WorkloadArgs {
    #[command(subcommand)]
    pub action: WorkloadAction,
}

#[derive(Subcommand)]
pub enum WorkloadAction {
    /// Schema operations
    Schema(SchemaArgs),
}

#[derive(Args)]
pub struct SchemaArgs {
    #[command(subcommand)]
    pub action: SchemaAction,
}

#[derive(Subcommand)]
pub enum SchemaAction {
    /// Diff two schema files
    Diff(SchemaDiffArgs),
}

#[derive(Args)]
pub struct SchemaDiffArgs {
    #[arg(long)]
    pub old: PathBuf,
    #[arg(long)]
    pub new: PathBuf,
    #[arg(long)]
    pub json: bool,
}

// -----------------------------------------------------------------------------
// Telemetry command
// -----------------------------------------------------------------------------

#[derive(Args)]
pub struct TelemetryArgs {
    #[command(subcommand)]
    pub action: TelemetryAction,
}

#[derive(Subcommand)]
pub enum TelemetryAction {
    /// Replay telemetry samples
    Replay(TelemetryReplayArgs),
    /// Fetch telemetry data
    Fetch(TelemetryFetchArgs),
}

#[derive(Args)]
pub struct TelemetryReplayArgs {
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long)]
    pub workload: Option<String>,
    #[arg(long)]
    pub version: Option<String>,
    #[arg(long, value_name = "EPOCH")]
    pub capability_epoch: Option<u64>,
    #[arg(long, default_value_t = 1.0)]
    pub speed: f64,
    #[arg(long)]
    pub apply_delay: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::ValueEnum, Clone)]
pub enum TelemetryResourceArg {
    Workloads,
    ProtocolMetrics,
    WorkloadHealth,
    WorkloadPlacements,
}

#[derive(Args)]
pub struct TelemetryFetchArgs {
    #[arg(long, conflicts_with = "url", required_unless_present = "url")]
    pub file: Option<PathBuf>,
    #[arg(long, conflicts_with = "file", required_unless_present = "file")]
    pub url: Option<String>,
    #[arg(long, value_enum, default_value = "workloads")]
    pub resource: TelemetryResourceArg,
    #[arg(long)]
    pub tenant: Option<String>,
    #[arg(long)]
    pub workload: Option<String>,
    #[arg(long)]
    pub version: Option<String>,
    #[arg(long)]
    pub json: bool,
}

// -----------------------------------------------------------------------------
// Simulator command
// -----------------------------------------------------------------------------

#[derive(Args)]
pub struct SimulatorArgs {
    #[command(subcommand)]
    pub action: SimulatorAction,
}

#[derive(Subcommand)]
pub enum SimulatorAction {
    /// Run overload simulation
    Overload(OverloadSimArgs),
}

#[derive(Args)]
pub struct OverloadSimArgs {
    pub config: PathBuf,
    #[arg(long, default_value_t = 1_000)]
    pub baseline_rps: u64,
    #[arg(long)]
    pub dump_config: bool,
    #[arg(long)]
    pub json: bool,
}

// -----------------------------------------------------------------------------
// Chaos command
// -----------------------------------------------------------------------------

#[derive(Args)]
pub struct ChaosArgs {
    #[command(subcommand)]
    pub action: ChaosAction,
}

#[derive(Subcommand)]
pub enum ChaosAction {
    /// Run chaos scenario
    Run(ChaosRunArgs),
}

#[derive(Args)]
pub struct ChaosRunArgs {
    pub scenario: PathBuf,
    #[arg(long)]
    pub json: bool,
}

// -----------------------------------------------------------------------------
// Synthetic command
// -----------------------------------------------------------------------------

#[derive(Args)]
pub struct SyntheticArgs {
    #[command(subcommand)]
    pub action: SyntheticAction,
}

#[derive(Subcommand)]
pub enum SyntheticAction {
    /// Run synthetic probes
    Run(SyntheticRunArgs),
}

/// Parse a key=value argument into a tuple.
fn parse_feature_flag(s: &str) -> Result<(String, String), String> {
    let (key, value) = s
        .split_once('=')
        .ok_or_else(|| "expected KEY=VALUE".to_string())?;
    if key.is_empty() {
        return Err("feature flag key may not be empty".into());
    }
    Ok((key.to_string(), value.to_string()))
}

#[derive(Args)]
pub struct SyntheticRunArgs {
    pub protocol: String,
    #[arg(long)]
    pub workload_version: Option<String>,
    #[arg(long = "feature", value_name = "KEY=VALUE", value_parser = parse_feature_flag)]
    pub feature_flags: Vec<(String, String)>,
    #[arg(long, default_value_t = 100)]
    pub count: u32,
    #[arg(long)]
    pub json: bool,
}
