use crate::prg::PersistedPrgState;
use crate::routing::PrgId;
use crate::workloads::mqtt::MqttWorkload;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fs;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use twox_hash::XxHash64;

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotReport {
    pub tenant_id: String,
    pub partition_index: u64,
    pub manifest_path: PathBuf,
    pub workload_label: String,
    pub workload_version: u16,
    pub schema_revision: u16,
    pub capability_epoch: u64,
    pub effective_floor: u64,
    pub sessions: usize,
    pub retained_topics: usize,
    pub offline_entries: usize,
    pub offline_bytes: usize,
    pub capability_digest: String,
}

impl SnapshotReport {
    pub fn prg_id(&self) -> String {
        format!("{}:{}", self.tenant_id, self.partition_index)
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "prg": self.prg_id(),
            "workload": self.workload_label,
            "workload_version": self.workload_version,
            "schema_revision": self.schema_revision,
            "capability_epoch": self.capability_epoch,
            "effective_floor": self.effective_floor,
            "sessions": self.sessions,
            "retained_topics": self.retained_topics,
            "offline_entries": self.offline_entries,
            "offline_bytes": self.offline_bytes,
            "capability_digest": self.capability_digest,
            "manifest": self.manifest_path,
        })
    }
}

#[derive(Debug, Deserialize)]
struct SnapshotManifest {
    #[allow(dead_code)]
    term: u64,
    #[allow(dead_code)]
    index: u64,
    state: PersistedPrgState,
    #[serde(default)]
    effective_floor: u64,
}

/// Inspect a snapshot manifest or directory containing manifests.
pub fn inspect_snapshot(target: &Path) -> Result<SnapshotReport> {
    let manifest_path = resolve_manifest_path(target)?;
    let bytes = fs::read(&manifest_path)
        .with_context(|| format!("read snapshot manifest {}", manifest_path.display()))?;
    let manifest: SnapshotManifest =
        serde_json::from_slice(&bytes).context("decode snapshot manifest")?;
    let prg = infer_prg_id(&manifest_path).ok_or_else(|| {
        anyhow!(
            "unable to infer PRG id from manifest path {}",
            manifest_path.display()
        )
    })?;
    Ok(build_report(&manifest, prg, manifest_path))
}

/// List the most recent snapshot for each PRG beneath the provided storage directory.
pub fn list_snapshots(base: &Path, tenant_filter: Option<&str>) -> Result<Vec<SnapshotReport>> {
    if !base.is_dir() {
        anyhow::bail!("{} is not a directory", base.display());
    }
    let mut reports = Vec::new();
    for entry in fs::read_dir(base).with_context(|| format!("read {}", base.display()))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        let Some(prg_id) = parse_prg_component(&name) else {
            continue;
        };
        if let Some(filter) = tenant_filter {
            if prg_id.tenant_id != filter {
                continue;
            }
        }
        let prg_dir = entry.path();
        let Ok(manifest_path) = resolve_latest_manifest_for_prg(&prg_dir) else {
            continue;
        };
        let bytes = match fs::read(&manifest_path) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        if let Ok(manifest) = serde_json::from_slice::<SnapshotManifest>(&bytes) {
            reports.push(build_report(&manifest, prg_id, manifest_path));
        }
    }
    reports.sort_by_key(|r| (r.tenant_id.clone(), r.partition_index));
    Ok(reports)
}

fn build_report(manifest: &SnapshotManifest, prg: PrgId, manifest_path: PathBuf) -> SnapshotReport {
    let descriptor = MqttWorkload::descriptor_static();
    let sessions = manifest.state.sessions.len();
    let retained_topics = manifest.state.retained.len();
    let (offline_entries, offline_bytes) = offline_stats(&manifest.state);
    let capability_epoch = manifest
        .state
        .sessions
        .values()
        .map(|session| session.session_epoch)
        .max()
        .unwrap_or(0);
    let digest = capability_digest(&manifest.state);
    SnapshotReport {
        tenant_id: prg.tenant_id,
        partition_index: prg.partition_index,
        manifest_path,
        workload_label: descriptor.label.into(),
        workload_version: descriptor.version,
        schema_revision: descriptor.version,
        capability_epoch,
        effective_floor: manifest.effective_floor.max(manifest.state.committed_index),
        sessions,
        retained_topics,
        offline_entries,
        offline_bytes,
        capability_digest: digest,
    }
}

fn offline_stats(state: &PersistedPrgState) -> (usize, usize) {
    let mut total_entries = 0usize;
    let mut total_bytes = 0usize;
    for entries in state.offline.values() {
        total_entries += entries.len();
        total_bytes += entries
            .iter()
            .map(|entry| entry.payload.len())
            .sum::<usize>();
    }
    (total_entries, total_bytes)
}

fn capability_digest(state: &PersistedPrgState) -> String {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write_u64(state.sessions.len() as u64);
    hasher.write_u64(state.retained.len() as u64);
    hasher.write_u64(state.offline.len() as u64);
    for key in state.sessions.keys().collect::<Vec<_>>() {
        hasher.write(key.as_bytes());
    }
    format!("{:016x}", hasher.finish())
}

fn resolve_manifest_path(target: &Path) -> Result<PathBuf> {
    if target.is_file() {
        return Ok(target.to_path_buf());
    }
    if !target.is_dir() {
        anyhow::bail!("{} is not a file or directory", target.display());
    }
    if target.join("manifest.json").is_file() {
        return Ok(target.join("manifest.json"));
    }
    if target
        .file_name()
        .and_then(OsStr::to_str)
        .map(|name| name.starts_with("snap-"))
        .unwrap_or(false)
    {
        let candidate = target.join("manifest.json");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    resolve_latest_manifest_for_prg(target)
}

fn resolve_latest_manifest_for_prg(prg_dir: &Path) -> Result<PathBuf> {
    let snapshots_dir = prg_dir.join("snapshots");
    if !snapshots_dir.is_dir() {
        anyhow::bail!("directory {} has no snapshots", snapshots_dir.display());
    }
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in
        fs::read_dir(&snapshots_dir).with_context(|| format!("read {}", snapshots_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        let Some((_term, index)) = parse_snapshot_dir(&name) else {
            continue;
        };
        let manifest = entry.path().join("manifest.json");
        if !manifest.is_file() {
            continue;
        }
        match &best {
            Some((best_index, _)) if *best_index >= index => continue,
            _ => best = Some((index, manifest)),
        }
    }
    best.map(|(_, path)| path)
        .ok_or_else(|| anyhow!("no manifests found under {}", snapshots_dir.display()))
}

fn infer_prg_id(manifest_path: &Path) -> Option<PrgId> {
    for ancestor in manifest_path.ancestors() {
        if let Some(name) = ancestor.file_name().and_then(OsStr::to_str) {
            if let Some(prg) = parse_prg_component(name) {
                return Some(prg);
            }
        }
    }
    None
}

fn parse_prg_component(name: &str) -> Option<PrgId> {
    let stripped = name.strip_prefix("prg_")?;
    let (tenant, partition) = stripped.rsplit_once('_')?;
    let partition_index = partition.parse().ok()?;
    Some(PrgId {
        tenant_id: tenant.to_string(),
        partition_index,
    })
}

fn parse_snapshot_dir(name: &str) -> Option<(u64, u64)> {
    let parts: Vec<_> = name.strip_prefix("snap-")?.split('-').collect();
    if parts.len() != 2 {
        return None;
    }
    let term = parts[0].parse().ok()?;
    let index = parts[1].parse().ok()?;
    Some((term, index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn inspect_manifest_extracts_stats() {
        let dir = TempDir::new().unwrap();
        let manifest = dir.path().join("manifest.json");
        let prg_dir = dir
            .path()
            .join("prg_tenantA_0")
            .join("snapshots")
            .join("snap-1-10");
        fs::create_dir_all(&prg_dir).unwrap();
        let manifest_path = prg_dir.join("manifest.json");
        let mut state = PersistedPrgState::default();
        state.sessions.insert("client-a".into(), Default::default());
        fs::write(
            &manifest_path,
            serde_json::to_vec(&json!({
                "term": 1,
                "index": 10,
                "state": state,
                "effective_floor": 10
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &manifest,
            serde_json::to_vec(&json!({
                "term": 1,
                "index": 10,
                "state": PersistedPrgState::default(),
                "effective_floor": 10
            }))
            .unwrap(),
        )
        .unwrap();
        let report = inspect_snapshot(&manifest_path).unwrap();
        assert_eq!(report.prg_id(), "tenantA:0");
        assert_eq!(report.sessions, 1);
        let listing = list_snapshots(dir.path(), None).unwrap();
        assert_eq!(listing.len(), 1);
    }
}
