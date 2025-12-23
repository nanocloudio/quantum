//! Telemetry commands.

use crate::cli::args::{TelemetryAction, TelemetryArgs, TelemetryResourceArg};
use crate::toolkit::telemetry::{self, ReplayOptions, TelemetryFilters, TelemetryResource};
use anyhow::{bail, Context, Result};
use reqwest::blocking;
use serde::Serialize;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

impl From<TelemetryResourceArg> for TelemetryResource {
    fn from(value: TelemetryResourceArg) -> Self {
        match value {
            TelemetryResourceArg::Workloads => TelemetryResource::Workloads,
            TelemetryResourceArg::ProtocolMetrics => TelemetryResource::ProtocolMetrics,
            TelemetryResourceArg::WorkloadHealth => TelemetryResource::WorkloadHealth,
            TelemetryResourceArg::WorkloadPlacements => TelemetryResource::WorkloadPlacements,
        }
    }
}

pub fn run_telemetry(args: TelemetryArgs) -> Result<()> {
    match args.action {
        TelemetryAction::Replay(replay_args) => {
            let samples = telemetry::load_samples(&replay_args.input)?;
            let opts = ReplayOptions {
                workload: replay_args.workload.clone(),
                version: replay_args.version.clone(),
                capability_epoch: replay_args.capability_epoch,
                speed: replay_args.speed,
                apply_delay: replay_args.apply_delay,
            };
            let replayed = telemetry::replay_samples(samples, &opts);
            if replay_args.json {
                print_json(&replayed)?;
            } else {
                print_replay_summary(&replayed);
            }
            Ok(())
        }
        TelemetryAction::Fetch(fetch_args) => {
            if fetch_args.file.is_none() && fetch_args.url.is_none() {
                bail!("provide either --file or --url for telemetry fetch");
            }
            let content = load_source(fetch_args.file, fetch_args.url)?;
            let json: Value = serde_json::from_str(&content).context("decode telemetry payload")?;
            let filters = TelemetryFilters {
                tenant: fetch_args.tenant.clone(),
                workload: fetch_args.workload.clone(),
                version: fetch_args.version.clone(),
            };
            let filtered =
                telemetry::filter_endpoint_data(&json, fetch_args.resource.into(), &filters);
            if fetch_args.json {
                print_json(&filtered)?;
            } else {
                print_telemetry_preview(&filtered);
            }
            Ok(())
        }
    }
}

fn print_replay_summary(replayed: &[telemetry::ReplayedSample]) {
    println!("replayed {} samples", replayed.len());
    println!(
        "{:<24} {:<10} {:<10} {:<12}",
        "metric", "value", "delay_ms", "timestamp"
    );
    for sample in replayed.iter().take(25) {
        println!(
            "{:<24} {:<10.2} {:<10} {:<12}",
            sample.metric, sample.value, sample.delay_ms, sample.timestamp_ms,
        );
    }
    if replayed.len() > 25 {
        println!("... {} additional samples omitted", replayed.len() - 25);
    }
}

fn print_telemetry_preview(value: &Value) {
    match value {
        Value::Array(items) => {
            println!("{} entries", items.len());
            for item in items.iter().take(10) {
                println!("{}", serde_json::to_string_pretty(item).unwrap_or_default());
            }
            if items.len() > 10 {
                println!("... {} additional entries omitted", items.len() - 10);
            }
        }
        other => println!(
            "{}",
            serde_json::to_string_pretty(other).unwrap_or_default()
        ),
    }
}

fn load_source(file: Option<PathBuf>, url: Option<String>) -> Result<String> {
    if let Some(path) = file {
        return fs::read_to_string(&path).with_context(|| format!("read {}", path.display()));
    }
    if let Some(url) = url {
        let response = blocking::get(&url)
            .with_context(|| format!("fetch {}", url))?
            .error_for_status()
            .context("unexpected status")?;
        return response.text().context("read response body");
    }
    bail!("no source provided");
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
