//! Toolkit commands - chaos, simulator, synthetic, workload.

use crate::cli::args::{
    ChaosAction, ChaosArgs, SchemaAction, SimulatorAction, SimulatorArgs, SyntheticAction,
    SyntheticArgs, WorkloadAction, WorkloadArgs,
};
use crate::toolkit::{chaos, schema, simulator, synthetic};
use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;

pub fn run_workload(args: WorkloadArgs) -> Result<()> {
    match args.action {
        WorkloadAction::Schema(schema_args) => match schema_args.action {
            SchemaAction::Diff(diff_args) => {
                let changes = schema::diff_schemas(&diff_args.old, &diff_args.new)?;
                if diff_args.json {
                    print_json(&changes)?;
                } else {
                    print_schema_table(&changes);
                }
                Ok(())
            }
        },
    }
}

pub fn run_simulator(args: SimulatorArgs) -> Result<()> {
    match args.action {
        SimulatorAction::Overload(overload_args) => {
            let cfg = simulator::load_overload_config(&overload_args.config)?;
            let report = simulator::simulate_overload(cfg.clone(), overload_args.baseline_rps);
            if overload_args.json {
                print_json(&report)?;
            } else {
                print_overload_report(&report);
                if overload_args.dump_config {
                    println!("\n# config");
                    println!("{}", serde_json::to_string_pretty(&cfg)?);
                }
            }
            Ok(())
        }
    }
}

pub fn run_chaos(args: ChaosArgs) -> Result<()> {
    match args.action {
        ChaosAction::Run(run_args) => {
            let scenario = chaos::load_scenario(&run_args.scenario)?;
            let plan = chaos::plan_chaos(scenario);
            if run_args.json {
                print_json(&plan)?;
            } else {
                print_chaos_plan(&plan);
            }
            Ok(())
        }
    }
}

pub fn run_synthetic(args: SyntheticArgs) -> Result<()> {
    match args.action {
        SyntheticAction::Run(run_args) => {
            let mut flags = HashMap::new();
            for (key, value) in run_args.feature_flags {
                flags.insert(key, value);
            }
            let opts = synthetic::SyntheticOptions {
                protocol: run_args.protocol,
                workload_version: run_args.workload_version,
                feature_flags: flags,
                count: run_args.count,
            };
            let result = synthetic::run_probes(opts);
            if run_args.json {
                print_json(&result)?;
            } else {
                print_synthetic_summary(&result);
            }
            Ok(())
        }
    }
}

fn print_schema_table(changes: &[schema::SchemaChange]) {
    if changes.is_empty() {
        println!("no schema changes detected");
        return;
    }
    println!(
        "{:<8} {:<8} {:<18} {:<18} {:<8}",
        "workload", "kind", "name", "change", "needs-note"
    );
    for change in changes {
        println!(
            "{:<8} {:<8} {:<18} {:<18} {:<8}",
            change.workload,
            change.kind,
            change.name,
            format!("{} ({})", change.change, change.details),
            if change.requires_migration_note {
                "yes"
            } else {
                "no"
            },
        );
    }
}

fn print_overload_report(report: &simulator::OverloadReport) {
    println!(
        "pattern: {:?} duration: {}s total_peak_rps: {}",
        report.pattern, report.duration_seconds, report.total_peak_rps
    );
    println!(
        "{:<8} {:<10} {:<12} {:<12}",
        "workload", "version", "avg_rps", "peak_rps"
    );
    for entry in &report.workloads {
        println!(
            "{:<8} {:<10} {:<12} {:<12}",
            entry.workload,
            entry.version.as_deref().unwrap_or("-"),
            entry.average_rps,
            entry.peak_rps,
        );
    }
}

fn print_chaos_plan(plan: &chaos::ChaosPlan) {
    println!(
        "scenario '{}' protocol {} (epoch {:?})",
        plan.name, plan.protocol, plan.capability_epoch
    );
    for (idx, step) in plan.steps.iter().enumerate() {
        println!("{}. {}", idx + 1, step.description);
        println!("   validate: {}", step.validation);
    }
}

fn print_synthetic_summary(result: &synthetic::SyntheticResult) {
    println!(
        "protocol {} version {:?} probes {} success {} failed {} avg_latency {}ms",
        result.protocol,
        result.workload_version,
        result.probes,
        result.success,
        result.failed,
        result.average_latency_ms,
    );
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
