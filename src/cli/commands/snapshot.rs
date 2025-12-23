//! Snapshot inspection commands.

use crate::cli::args::{SnapshotAction, SnapshotArgs};
use crate::toolkit::snapshot;
use anyhow::Result;
use serde::Serialize;

pub fn run_snapshot(args: SnapshotArgs) -> Result<()> {
    match args.action {
        SnapshotAction::Inspect(inspect_args) => {
            let report = snapshot::inspect_snapshot(&inspect_args.target)?;
            if inspect_args.json {
                print_json(&report)?;
            } else {
                print_snapshot_report(&report);
            }
            Ok(())
        }
        SnapshotAction::List(list_args) => {
            let reports = snapshot::list_snapshots(&list_args.base, list_args.tenant.as_deref())?;
            if list_args.json {
                print_json(&reports)?;
            } else {
                print_snapshot_table(&reports);
            }
            Ok(())
        }
    }
}

fn print_snapshot_report(report: &snapshot::SnapshotReport) {
    println!("PRG: {}", report.prg_id());
    println!("Tenant: {}", report.tenant_id);
    println!(
        "Workload: {} v{}",
        report.workload_label, report.workload_version
    );
    println!("Schema revision: {}", report.schema_revision);
    println!("Capability epoch: {}", report.capability_epoch);
    println!("Effective floor: {}", report.effective_floor);
    println!("Sessions: {}", report.sessions);
    println!("Retained topics: {}", report.retained_topics);
    println!(
        "Offline entries: {} ({} bytes)",
        report.offline_entries, report.offline_bytes
    );
    println!("Capability digest: {}", report.capability_digest);
    println!("Manifest: {}", report.manifest_path.display());
}

fn print_snapshot_table(reports: &[snapshot::SnapshotReport]) {
    if reports.is_empty() {
        println!("no snapshots found");
        return;
    }
    println!(
        "{:<18} {:<8} {:<6} {:<6} {:<12} {:<8} {:<8} {:<10}",
        "prg", "workload", "ver", "schema", "cap_epoch", "sessions", "retained", "offline"
    );
    for report in reports {
        println!(
            "{:<18} {:<8} {:<6} {:<6} {:<12} {:<8} {:<8} {:<10}",
            report.prg_id(),
            report.workload_label,
            report.workload_version,
            report.schema_revision,
            report.capability_epoch,
            report.sessions,
            report.retained_topics,
            report.offline_entries,
        );
    }
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
