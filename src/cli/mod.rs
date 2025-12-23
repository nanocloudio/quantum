//! Quantum CLI - unified command-line interface.
//!
//! Provides a single binary entry point for:
//! - `quantum start` - Start the broker
//! - `quantum init` - Seed workload fixtures
//! - `quantum subscribe` - Stream MQTT messages (kcat -C style)
//! - `quantum publish` - Send MQTT messages (kcat -P style)
//! - `quantum inspect` - Inspect WAL segments and storage
//! - `quantum snapshot` - Snapshot inspection
//! - `quantum workload` - Workload tooling
//! - `quantum telemetry` - Telemetry operations
//! - `quantum simulator` - Simulation tools
//! - `quantum chaos` - Chaos testing
//! - `quantum synthetic` - Synthetic probes

mod args;
pub mod commands;

pub use args::{Cli, Commands, OutputFormat, PublishArgs, QosLevel, SubscribeArgs, TlsArgs};
