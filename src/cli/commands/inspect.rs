//! WAL inspection commands.

use crate::cli::args::{InspectAction, InspectArgs};
use crate::prg::PersistedPrgState;
use crate::routing::PrgId;
use anyhow::{anyhow, Context, Result};
use clustor::storage::layout::WalSegmentRef;
use clustor::storage::replay::WalReplayScanner;
use serde::Deserialize;
use std::fs;
use std::path::Path;

pub fn run_inspect(args: InspectArgs) -> Result<()> {
    match args.action {
        InspectAction::Wal(wal_args) => {
            if wal_args.paths.is_empty() {
                return Err(anyhow!(
                    "usage: quantum inspect wal <wal-segment-or-directory> [more ...]"
                ));
            }
            for path in &wal_args.paths {
                inspect_path(path)?;
            }
            Ok(())
        }
    }
}

fn inspect_path(path: &Path) -> Result<()> {
    if path.is_dir() {
        let mut segments = collect_segments(path)?;
        if segments.is_empty() {
            println!("no WAL segments found under {}", path.display());
        }
        segments.sort();
        for segment in segments {
            inspect_segment(&segment)?;
        }
    } else {
        inspect_segment(path)?;
    }
    Ok(())
}

fn inspect_segment(path: &Path) -> Result<()> {
    let seq = parse_segment_seq(path).unwrap_or(0);
    let segment = WalSegmentRef {
        seq,
        log_path: path.to_path_buf(),
        index_path: None,
    };
    println!(
        "\n=== Inspecting segment {} (seq={}) ===",
        segment.log_path.display(),
        seq
    );
    let result = WalReplayScanner::scan(std::slice::from_ref(&segment))
        .with_context(|| format!("unable to scan {}", segment.log_path.display()))?;
    if let Some(trunc) = &result.truncation {
        println!(
            "warning: truncated segment {} bytes={} error={:?}",
            trunc.segment_seq, trunc.truncated_bytes, trunc.error
        );
    }
    if result.frames.is_empty() {
        println!("(no frames)");
        return Ok(());
    }
    for frame in result.frames {
        let prg: PrgId = serde_json::from_slice(&frame.metadata).context("decode prg id")?;
        let record: WalRecord =
            serde_json::from_slice(&frame.payload).context("decode wal payload")?;
        let summary = WalSummary::from_state(&record.state);
        println!(
            "#{:>6} term={:<3} prg={}:{} sessions={:<4} topics={:<4} retained={:<3} offline_entries={:<5} offline_bytes={:<6} committed_index={}",
            frame.header.index,
            frame.header.term,
            prg.tenant_id,
            prg.partition_index,
            summary.sessions,
            summary.topics,
            summary.retained,
            summary.offline_entries,
            summary.offline_bytes,
            record.state.committed_index
        );
    }
    Ok(())
}

fn collect_segments(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && is_segment_file(&path) {
            files.push(path);
        }
    }
    Ok(files)
}

fn is_segment_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.starts_with("segment-") && name.ends_with(".log"))
        .unwrap_or(false)
}

fn parse_segment_seq(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let start = name.find('-')? + 1;
    let end = name.find('.')?;
    name[start..end].parse().ok()
}

#[derive(Deserialize)]
struct WalRecord {
    state: PersistedPrgState,
}

struct WalSummary {
    sessions: usize,
    topics: usize,
    retained: usize,
    offline_entries: usize,
    offline_bytes: usize,
}

impl WalSummary {
    fn from_state(state: &PersistedPrgState) -> Self {
        let mut offline_entries = 0usize;
        let mut offline_bytes = 0usize;
        for entries in state.offline.values() {
            offline_entries += entries.len();
            offline_bytes += entries
                .iter()
                .map(|entry| entry.payload.len())
                .sum::<usize>();
        }
        Self {
            sessions: state.sessions.len(),
            topics: state.topics.len(),
            retained: state.retained.len(),
            offline_entries,
            offline_bytes,
        }
    }
}
