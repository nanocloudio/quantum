#![deny(unused, dead_code)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::similar_names)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::wildcard_imports)]

//! Quantum - unified CLI entrypoint.
//!
//! Usage:
//!   quantum start --config config/quantum.toml
//!   quantum inspect wal <wal-segment-or-directory>...
//!   quantum snapshot inspect <target>
//!   quantum snapshot list <base>

use anyhow::Result;
use clap::Parser;
use quantum::cli::commands::{
    run_chaos, run_init, run_inspect, run_publish, run_simulator, run_snapshot, run_start,
    run_subscribe, run_synthetic, run_telemetry, run_workload,
};
use quantum::cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start(args) => run_start(args).await,
        Commands::Init(args) => run_init(args),
        Commands::Subscribe(args) => run_subscribe(args).await,
        Commands::Publish(args) => run_publish(args).await,
        Commands::Inspect(args) => run_inspect(args),
        Commands::Snapshot(args) => run_snapshot(args),
        Commands::Workload(args) => run_workload(args),
        Commands::Telemetry(args) => run_telemetry(args),
        Commands::Simulator(args) => run_simulator(args),
        Commands::Chaos(args) => run_chaos(args),
        Commands::Synthetic(args) => run_synthetic(args),
    }
}
