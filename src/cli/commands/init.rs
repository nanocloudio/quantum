//! Init command - seed workload data from fixtures.

use crate::cli::args::InitArgs;
use crate::control::{
    CapabilityRegistry, ProtocolAssignment, ProtocolFeatureFlag, ProtocolLimits, ProtocolType,
    TenantCapabilities,
};
use crate::prg::PersistedPrgState;
use crate::routing::PrgId;
use anyhow::{bail, Context, Result};
use clustor::storage::{EntryFrameBuilder, WalWriter};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use twox_hash::XxHash64;

const DEFAULT_BLOCK_SIZE: u64 = 4096;
const DEFAULT_SEGMENT_SEQ: u64 = 1;

pub fn run_init(args: InitArgs) -> Result<()> {
    let manifest_path = resolve_manifest_path(&args.fixtures)?;
    let manifest_dir = manifest_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let manifest: FixtureManifest = read_json(&manifest_path)?;
    if manifest.workload != args.workload {
        bail!(
            "fixture workload {} does not match requested workload {}",
            manifest.workload,
            args.workload
        );
    }
    let tenant_filter: HashSet<String> = args.tenant.iter().cloned().collect();
    let mut registry = CapabilityRegistry::new();
    let mut seeded = Vec::new();

    for tenant in manifest.tenants.iter() {
        if !tenant_filter.is_empty() && !tenant_filter.contains(&tenant.tenant_id) {
            continue;
        }
        let caps_path = manifest_dir.join(&tenant.capabilities);
        let caps_fixture: TenantCapabilitiesSeed = read_json(&caps_path)
            .with_context(|| format!("load tenant capabilities {}", caps_path.display()))?;
        let tenant_caps = build_tenant_capabilities(&caps_fixture)?;
        validate_listener_files(&tenant.listener_files, &manifest_dir)?;
        seed_prgs_for_tenant(
            tenant,
            &manifest_dir,
            &args.data_root,
            &tenant_caps.tenant_id,
            args.dry_run,
        )?;
        let slot = registry.get_or_create_tenant(&tenant_caps.tenant_id);
        *slot = tenant_caps;
        seeded.push(SeededTenantReport {
            tenant_id: tenant.tenant_id.clone(),
            partitions: tenant.prg.iter().map(|p| p.partition).collect(),
        });
    }

    if seeded.is_empty() {
        bail!("no tenants matched the current selection");
    }

    let digest = compute_capability_digest(&registry);
    let epoch = registry.epoch();
    if !args.dry_run {
        persist_capability_registry(&registry, &args.data_root)?;
        write_seed_report(
            &SeedReport {
                workload: manifest.workload.clone(),
                manifest: manifest_path.display().to_string(),
                data_root: args.data_root.display().to_string(),
                tenants: seeded.clone(),
                capability_epoch: epoch,
                capability_digest: digest,
            },
            &args.data_root,
        )?;
    }

    println!(
        "seeded {} tenant(s) for workload {} (epoch={}, digest=0x{digest:016x})",
        seeded.len(),
        manifest.workload,
        epoch
    );
    for tenant in seeded {
        println!(
            "  - {} partitions {:?}",
            tenant.tenant_id, tenant.partitions
        );
    }
    if args.dry_run {
        println!("dry-run complete; no files were written");
    }
    Ok(())
}

fn resolve_manifest_path(path: &Path) -> Result<PathBuf> {
    if path.is_file() {
        return Ok(path.to_path_buf());
    }
    let candidate = path.join("manifest.json");
    if candidate.is_file() {
        Ok(candidate)
    } else {
        bail!(
            "fixture manifest not found at {} (pass a manifest.json or directory)",
            path.display()
        )
    }
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("read json fixture {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse json {}", path.display()))
}

fn build_tenant_capabilities(seed: &TenantCapabilitiesSeed) -> Result<TenantCapabilities> {
    let mut tenant = TenantCapabilities::new(&seed.tenant_id);
    tenant.tenant_id = seed.tenant_id.clone();
    tenant.epoch = seed.epoch;
    tenant.active = seed.active;
    tenant.created_at = seed.created_at;
    tenant.updated_at = seed.updated_at;
    tenant.global_features = seed
        .global_features
        .iter()
        .map(|feat| parse_feature_flag(feat))
        .collect::<HashSet<_>>();

    tenant.protocols.clear();
    for assignment in &seed.protocols {
        let protocol = ProtocolType::from(assignment.protocol.as_str());
        let mut proto = ProtocolAssignment::new(protocol.clone());
        proto.enabled = assignment.enabled;
        proto.priority = assignment.priority;
        proto.prg_affinity = assignment.prg_affinity.clone();
        proto.features.clear();
        for feat in &assignment.features {
            proto.features.insert(parse_feature_flag(feat));
        }
        if let Some(limits) = &assignment.limits {
            proto.limits = ProtocolLimits {
                max_connections: limits
                    .max_connections
                    .unwrap_or(proto.limits.max_connections),
                max_message_size: limits
                    .max_message_size
                    .unwrap_or(proto.limits.max_message_size),
                max_messages_per_second: limits
                    .max_messages_per_second
                    .unwrap_or(proto.limits.max_messages_per_second),
                max_bytes_per_second: limits
                    .max_bytes_per_second
                    .unwrap_or(proto.limits.max_bytes_per_second),
                max_offline_queue_bytes: limits
                    .max_offline_queue_bytes
                    .unwrap_or(proto.limits.max_offline_queue_bytes),
                max_retained_messages: limits
                    .max_retained_messages
                    .unwrap_or(proto.limits.max_retained_messages),
                max_subscriptions_per_connection: limits
                    .max_subscriptions_per_connection
                    .unwrap_or(proto.limits.max_subscriptions_per_connection),
            };
        }
        tenant.protocols.insert(protocol, proto);
    }
    Ok(tenant)
}

fn parse_feature_flag(flag: &str) -> ProtocolFeatureFlag {
    match flag {
        "mqtt5_features" => ProtocolFeatureFlag::Mqtt5Features,
        "retained_messages" => ProtocolFeatureFlag::RetainedMessages,
        "offline_queue" => ProtocolFeatureFlag::OfflineQueue,
        "deduplication" => ProtocolFeatureFlag::Deduplication,
        "exactly_once" => ProtocolFeatureFlag::ExactlyOnce,
        "topic_acl" => ProtocolFeatureFlag::TopicAcl,
        "message_encryption" => ProtocolFeatureFlag::MessageEncryption,
        "audit_logging" => ProtocolFeatureFlag::AuditLogging,
        other => ProtocolFeatureFlag::Custom(other.to_string()),
    }
}

fn validate_listener_files(files: &[PathBuf], root: &Path) -> Result<()> {
    for rel in files {
        let path = root.join(rel);
        if !path.is_file() {
            bail!("listener file {} does not exist", path.display());
        }
    }
    Ok(())
}

fn seed_prgs_for_tenant(
    tenant: &TenantFixture,
    fixtures_root: &Path,
    data_root: &Path,
    tenant_id: &str,
    dry_run: bool,
) -> Result<()> {
    for prg in &tenant.prg {
        seed_single_prg(prg, fixtures_root, data_root, tenant_id, dry_run)?;
    }
    Ok(())
}

fn seed_single_prg(
    prg: &PrgFixture,
    fixtures_root: &Path,
    data_root: &Path,
    tenant_id: &str,
    dry_run: bool,
) -> Result<()> {
    let snapshot_path = fixtures_root.join(&prg.snapshot);
    let wal_seed_path = fixtures_root.join(&prg.wal_seed);
    let metadata_path = fixtures_root.join(&prg.metadata);
    if !snapshot_path.is_file() {
        bail!("snapshot manifest missing at {}", snapshot_path.display());
    }
    if !wal_seed_path.is_file() {
        bail!("wal seed missing at {}", wal_seed_path.display());
    }
    if !metadata_path.is_file() {
        bail!("metadata missing at {}", metadata_path.display());
    }
    let snapshot_bytes = fs::read(&snapshot_path)
        .with_context(|| format!("read snapshot {}", snapshot_path.display()))?;
    let snapshot: SnapshotSeed =
        serde_json::from_slice(&snapshot_bytes).context("parse snapshot manifest")?;
    let wal_seed: WalSeed = read_json(&wal_seed_path)?;
    let frames = build_wal_frames(
        &wal_seed,
        fixtures_root,
        tenant_id,
        prg.partition,
        snapshot.state.clone(),
    )?;

    if dry_run {
        return Ok(());
    }

    let target_dir = data_root.join(format!("prg_{}_{}", tenant_id, prg.partition));
    if target_dir.exists() {
        fs::remove_dir_all(&target_dir)
            .with_context(|| format!("clear {}", target_dir.display()))?;
    }
    fs::create_dir_all(target_dir.join("definitions"))?;
    fs::create_dir_all(target_dir.join("snapshot"))?;
    fs::create_dir_all(target_dir.join("wal"))?;
    fs::copy(&metadata_path, target_dir.join("metadata.json"))
        .with_context(|| format!("copy metadata {}", metadata_path.display()))?;
    let snapshot_dir = target_dir
        .join("snapshot")
        .join(format!("snap-{}-{}", snapshot.term, snapshot.index));
    fs::create_dir_all(&snapshot_dir)?;
    fs::write(snapshot_dir.join("manifest.json"), snapshot_bytes)
        .context("write snapshot manifest")?;
    let wal_path = target_dir
        .join("wal")
        .join(format!("segment-{DEFAULT_SEGMENT_SEQ:010}.log"));
    let mut writer = WalWriter::open(&wal_path, DEFAULT_BLOCK_SIZE)
        .with_context(|| format!("open wal {}", wal_path.display()))?;
    for frame in frames {
        writer
            .append_frame(&frame)
            .with_context(|| format!("append WAL frame for {}", target_dir.display()))?;
    }
    Ok(())
}

fn build_wal_frames(
    wal_seed: &WalSeed,
    fixtures_root: &Path,
    tenant_id: &str,
    partition: u64,
    fallback_state: PersistedPrgState,
) -> Result<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    let mut entries = wal_seed.entries.clone();
    entries.sort_by_key(|e| e.index);
    for entry in entries {
        let state_path = fixtures_root.join(&entry.state_path);
        let state = if state_path.is_file() {
            let manifest: SnapshotSeed = read_json(&state_path)?;
            manifest.state
        } else {
            fallback_state.clone()
        };
        let mut wal_state = state;
        wal_state.committed_index = entry.index;
        wal_state.recompute_effective_product_floors(entry.index);
        let payload = serde_json::to_vec(&WalEntry {
            state: wal_state.clone(),
        })
        .context("encode wal")?;
        let metadata = serde_json::to_vec(&PrgId {
            tenant_id: tenant_id.to_string(),
            partition_index: partition,
        })
        .context("encode wal metadata")?;
        let term = entry.term.unwrap_or(wal_seed.term);
        let frame = EntryFrameBuilder::new(term, entry.index)
            .metadata(metadata)
            .payload(payload)
            .build();
        frames.push(frame.encode());
    }
    Ok(frames)
}

fn persist_capability_registry(registry: &CapabilityRegistry, data_root: &Path) -> Result<()> {
    let cp_dir = data_root.join("cp");
    fs::create_dir_all(&cp_dir)?;
    let path = cp_dir.join("capabilities.json");
    let bytes = serde_json::to_vec_pretty(registry)?;
    fs::write(&path, bytes).with_context(|| format!("write {}", path.display()))
}

fn write_seed_report(report: &SeedReport, data_root: &Path) -> Result<()> {
    let path = data_root.join("seed_report.json");
    let bytes = serde_json::to_vec_pretty(report)?;
    fs::write(&path, bytes).with_context(|| format!("write {}", path.display()))
}

fn compute_capability_digest(registry: &CapabilityRegistry) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    let mut tenant_ids: Vec<_> = registry.tenant_ids().into_iter().cloned().collect();
    tenant_ids.sort();
    for tenant_id in tenant_ids {
        tenant_id.hash(&mut hasher);
        if let Some(tenant) = registry.get_tenant(&tenant_id) {
            tenant.epoch.hash(&mut hasher);
            tenant.active.hash(&mut hasher);
            let mut protocols: Vec<_> = tenant
                .protocols
                .keys()
                .map(|p| p.as_str().to_string())
                .collect();
            protocols.sort();
            for proto in protocols {
                proto.hash(&mut hasher);
                if let Some(assignment) = tenant.protocols.get(&ProtocolType::from(proto.as_str()))
                {
                    assignment.enabled.hash(&mut hasher);
                    assignment.priority.hash(&mut hasher);
                }
            }
        }
    }
    hasher.finish()
}

#[derive(Debug, Deserialize)]
struct FixtureManifest {
    #[serde(default)]
    _schema_version: u32,
    #[allow(dead_code)]
    fixture_version: Option<String>,
    workload: String,
    tenants: Vec<TenantFixture>,
}

#[derive(Debug, Deserialize)]
struct TenantFixture {
    tenant_id: String,
    capabilities: PathBuf,
    #[serde(default)]
    listener_files: Vec<PathBuf>,
    prg: Vec<PrgFixture>,
}

#[derive(Debug, Deserialize)]
struct PrgFixture {
    partition: u64,
    snapshot: PathBuf,
    wal_seed: PathBuf,
    metadata: PathBuf,
}

#[derive(Debug, Deserialize)]
struct TenantCapabilitiesSeed {
    tenant_id: String,
    epoch: u64,
    active: bool,
    created_at: u64,
    updated_at: u64,
    #[serde(default)]
    global_features: Vec<String>,
    protocols: Vec<TenantProtocolSeed>,
}

#[derive(Debug, Deserialize)]
struct TenantProtocolSeed {
    protocol: String,
    enabled: bool,
    priority: u8,
    prg_affinity: Option<String>,
    #[serde(default)]
    features: Vec<String>,
    limits: Option<TenantProtocolLimitsSeed>,
}

#[derive(Debug, Deserialize)]
struct TenantProtocolLimitsSeed {
    max_connections: Option<u32>,
    max_message_size: Option<u32>,
    max_messages_per_second: Option<u32>,
    max_bytes_per_second: Option<u64>,
    max_offline_queue_bytes: Option<u64>,
    max_retained_messages: Option<u32>,
    max_subscriptions_per_connection: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct SnapshotSeed {
    term: u64,
    index: u64,
    #[serde(default)]
    _effective_floor: Option<u64>,
    state: PersistedPrgState,
}

#[derive(Debug, Deserialize, Clone)]
struct WalSeed {
    term: u64,
    entries: Vec<WalSeedEntry>,
}

#[derive(Debug, Deserialize, Clone)]
struct WalSeedEntry {
    index: u64,
    state_path: PathBuf,
    #[serde(default)]
    term: Option<u64>,
}

#[derive(Debug, Serialize)]
struct WalEntry {
    state: PersistedPrgState,
}

#[derive(Debug, Serialize, Clone)]
struct SeedReport {
    workload: String,
    manifest: String,
    data_root: String,
    tenants: Vec<SeededTenantReport>,
    capability_epoch: u64,
    capability_digest: u64,
}

#[derive(Debug, Serialize, Clone)]
struct SeededTenantReport {
    tenant_id: String,
    partitions: Vec<u64>,
}
