//! Start command - launches the Quantum broker.

use crate::cli::args::StartArgs;
use crate::config::Config;
use crate::runtime::Runtime;
use crate::telemetry;
use crate::time::SystemClock;
use anyhow::Result;
use std::env;

pub async fn run_start(args: StartArgs) -> Result<()> {
    // Set config path via environment so Config::load_from_env picks it up
    env::set_var("QUANTUM_CONFIG", args.config.display().to_string());

    let config = Config::load_from_env()?;
    let log_handle = telemetry::init_tracing(config.telemetry.log_level.as_deref())?;
    let clock = SystemClock;
    let mut runtime = Runtime::new(config, clock, Some(log_handle))?;
    runtime.run().await
}
