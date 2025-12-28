use crate::prg::PersistedPrgState;
use crate::routing::PrgId;
use anyhow::{Context, Result};
use clustor::consensus::DurabilityProof;
use clustor::storage::layout::WalSegmentRef;
use clustor::storage::{EntryFrame, EntryFrameBuilder, StorageLayout, WalReplayScanner, WalWriter};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tracing::{error, warn};

const DEFAULT_TERM: u64 = 1;
const DEFAULT_BLOCK_SIZE: u64 = 4096;
const DEFAULT_SEGMENT_SEQ: u64 = 1;
const SNAPSHOT_FILE: &str = "manifest.json";

/// Clustor-backed storage facade using WAL frames + snapshot manifests per PRG.
#[derive(Clone)]
pub struct ClustorStorage {
    base: PathBuf,
    writers: Arc<Mutex<HashMap<PrgId, WalWriter>>>,
    next_index: Arc<Mutex<HashMap<PrgId, u64>>>,
    floors: Arc<Mutex<HashMap<PrgId, u64>>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PrgWalEntry {
    state: PersistedPrgState,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotManifest {
    term: u64,
    index: u64,
    state: PersistedPrgState,
    #[serde(default)]
    effective_floor: u64,
    #[serde(default)]
    proof: Option<ProofMetadata>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ProofMetadata {
    term: u64,
    index: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct SnapshotSummary {
    pub index: u64,
    pub path: PathBuf,
    pub effective_floor: u64,
    pub proof: Option<DurabilityProof>,
}

impl From<DurabilityProof> for ProofMetadata {
    fn from(proof: DurabilityProof) -> Self {
        Self {
            term: proof.term,
            index: proof.index,
        }
    }
}

impl From<&ProofMetadata> for DurabilityProof {
    fn from(meta: &ProofMetadata) -> Self {
        DurabilityProof::new(meta.term, meta.index)
    }
}

#[derive(Debug, Clone)]
pub struct RecoveredState {
    pub state: PersistedPrgState,
    pub proof: Option<DurabilityProof>,
    pub effective_floor: u64,
}

#[derive(Debug)]
struct LoadResult {
    state: Option<PersistedPrgState>,
    last_index: u64,
    effective_floor: u64,
    proof: Option<DurabilityProof>,
}

impl ClustorStorage {
    pub fn new(base: impl AsRef<Path>) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
            writers: Arc::new(Mutex::new(HashMap::new())),
            next_index: Arc::new(Mutex::new(HashMap::new())),
            floors: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn layout_for(&self, prg: &PrgId) -> StorageLayout {
        let dir = self
            .base
            .join(format!("prg_{}_{}", prg.tenant_id, prg.partition_index));
        StorageLayout::new(dir)
    }

    fn wal_path(layout: &StorageLayout) -> PathBuf {
        layout
            .paths()
            .wal_dir
            .join(format!("segment-{DEFAULT_SEGMENT_SEQ:010}.log"))
    }

    fn partition_key(prg: &PrgId) -> String {
        format!("{}:{}", prg.tenant_id, prg.partition_index)
    }

    async fn ensure_next_index_seeded(&self, prg: &PrgId) -> Result<()> {
        if self.next_index.lock().await.contains_key(prg) {
            return Ok(());
        }
        // Seed from on-disk state if present.
        let _ = self.load(prg).await?;
        Ok(())
    }

    async fn persist_snapshot(
        &self,
        layout: &StorageLayout,
        index: u64,
        state: &PersistedPrgState,
        effective_floor: u64,
        proof: Option<&DurabilityProof>,
    ) -> Result<()> {
        let term = proof.map(|p| p.term).unwrap_or(DEFAULT_TERM);
        let manifest = SnapshotManifest {
            term,
            index,
            state: state.clone(),
            effective_floor,
            proof: proof.cloned().map(ProofMetadata::from),
        };
        let dir = layout
            .paths()
            .snapshot_dir
            .join(format!("snap-{term}-{index}"));
        fs::create_dir_all(&dir)
            .with_context(|| format!("create snapshot dir {}", dir.display()))?;
        let path = dir.join(SNAPSHOT_FILE);
        let bytes = serde_json::to_vec_pretty(&manifest)?;
        fs::write(&path, bytes).with_context(|| format!("write snapshot {}", path.display()))?;
        Ok(())
    }

    async fn next_log_index(&self, prg: &PrgId) -> u64 {
        let mut guard = self.next_index.lock().await;
        let entry = guard.entry(prg.clone()).or_insert(1);
        let idx = *entry;
        *entry = entry.saturating_add(1);
        idx
    }

    fn ensure_metadata(layout: &StorageLayout, prg: &PrgId) -> Result<()> {
        let mut metadata = layout.metadata_store().load_or_default()?;
        if metadata.partition_id.is_empty() {
            metadata.partition_id = Self::partition_key(prg);
            layout.metadata_store().persist(&metadata)?;
        }
        Ok(())
    }

    fn compute_effective_floor(state: &PersistedPrgState, fallback: u64) -> u64 {
        state.sessions.values().fold(fallback, |acc, session| {
            let offline_floor = session.earliest_offline.max(session.offline_floor);
            let session_floor = session
                .effective_product_floor
                .max(session.dedupe_floor)
                .max(offline_floor)
                .max(session.forward_chain_floor);
            acc.max(session_floor).max(fallback)
        })
    }

    async fn prune(&self, prg: &PrgId, floor: u64) -> Result<()> {
        if floor == 0 {
            return Ok(());
        }
        let layout = self.layout_for(prg);
        let prg_key = prg.clone();
        let floor = floor.max(self.effective_floor(prg).await);
        {
            let mut writers = self.writers.lock().await;
            writers.remove(prg);
        }
        let work = move || -> Result<()> {
            layout.ensure().context("ensure layout during prune")?;
            let state = layout
                .load_state()
                .context("load storage state during prune")?;
            for snap in state.snapshots {
                if snap.index < floor {
                    if let Some(dir) = snap.manifest_path.parent() {
                        let _ = fs::remove_dir_all(dir);
                    }
                }
            }
            for seg in state.wal_segments {
                if !seg.log_path.exists() {
                    continue;
                }
                let scan = WalReplayScanner::scan(std::slice::from_ref(&seg))
                    .context("scan wal for prune")?;
                if let Some(trunc) = &scan.truncation {
                    scan.enforce_truncation()
                        .context("truncate corrupt wal segment during prune")?;
                    tracing::warn!(
                        "wal truncation observed while pruning {} bytes={} segment={}",
                        Self::partition_key(&prg_key),
                        trunc.truncated_bytes,
                        trunc.segment_seq
                    );
                }
                let total_frames = scan.frames.len();
                let kept: Vec<EntryFrame> = scan
                    .frames
                    .into_iter()
                    .filter(|frame| frame.header.index >= floor)
                    .collect();
                if kept.is_empty() {
                    let _ = fs::remove_file(&seg.log_path);
                    if let Some(idx) = seg.index_path {
                        let _ = fs::remove_file(idx);
                    }
                    continue;
                }
                let dropped = kept.len() < total_frames || scan.truncation.is_some();
                if !dropped {
                    continue;
                }
                let tmp = seg.log_path.with_extension("pruned");
                {
                    let mut writer =
                        WalWriter::open(&tmp, DEFAULT_BLOCK_SIZE).context("open pruned wal")?;
                    for frame in kept {
                        writer
                            .append_frame(&frame.encode())
                            .context("rewrite wal frame")?;
                    }
                }
                fs::rename(&tmp, &seg.log_path)
                    .with_context(|| format!("replace wal {}", seg.log_path.display()))?;
                if let Some(idx) = seg.index_path {
                    let _ = fs::remove_file(idx);
                }
            }
            Ok(())
        };
        if Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(work)
                .await
                .context("join wal prune task")??;
        } else {
            work()?;
        }
        Ok(())
    }

    /// Append a WAL frame and snapshot for the PRG state; returns the committed index.
    pub async fn persist(&self, prg: &PrgId, state: &PersistedPrgState) -> Result<u64> {
        self.ensure_next_index_seeded(prg).await?;
        let index = self.next_log_index(prg).await;
        self.persist_at_with_proof(prg, state, index, None).await
    }

    /// Append a WAL frame at an explicit clustor-committed index.
    pub async fn persist_at(
        &self,
        prg: &PrgId,
        state: &PersistedPrgState,
        index: u64,
    ) -> Result<u64> {
        self.persist_at_with_proof(prg, state, index, None).await
    }

    /// Append a WAL frame at an explicit clustor-committed index, embedding an optional durability proof.
    pub async fn persist_at_with_proof(
        &self,
        prg: &PrgId,
        state: &PersistedPrgState,
        index: u64,
        proof: Option<DurabilityProof>,
    ) -> Result<u64> {
        self.ensure_next_index_seeded(prg).await?;
        {
            let mut guard = self.next_index.lock().await;
            guard.insert(prg.clone(), index.saturating_add(1));
        }
        let layout = self.layout_for(prg);
        let mut wal_state = state.clone();
        wal_state.committed_index = index;
        wal_state.recompute_effective_product_floors(index);
        let wal_entry = serde_json::to_vec(&PrgWalEntry {
            state: wal_state.clone(),
        })?;
        let metadata = serde_json::to_vec(prg)?;
        let term = proof.as_ref().map(|p| p.term).unwrap_or(DEFAULT_TERM);
        let effective_floor = Self::compute_effective_floor(state, index).min(index);
        let frame = EntryFrameBuilder::new(term, index)
            .metadata(metadata)
            .payload(wal_entry)
            .build();

        {
            let mut writers = self.writers.lock().await;
            if !writers.contains_key(prg) {
                Self::ensure_metadata(&layout, prg)?;
                let writer = WalWriter::open(Self::wal_path(&layout), DEFAULT_BLOCK_SIZE)
                    .with_context(|| "open WAL writer for PRG state")?;
                writers.insert(prg.clone(), writer);
            }
            if let Some(writer) = writers.get_mut(prg) {
                writer.append_frame(&frame.encode()).with_context(|| {
                    format!("append wal frame for {}", Self::partition_key(prg))
                })?;
            }
        }
        {
            let mut floors = self.floors.lock().await;
            floors.insert(prg.clone(), effective_floor);
        }
        self.persist_snapshot(&layout, index, &wal_state, effective_floor, proof.as_ref())
            .await?;
        self.prune(prg, effective_floor).await?;
        Ok(index)
    }

    /// Load the latest PRG snapshot + WAL replay, returning the recovered state (if any).
    pub async fn load(&self, prg: &PrgId) -> Result<Option<PersistedPrgState>> {
        self.load_at(prg, None).await
    }

    /// Load state and associated durability proof metadata up to `committed_floor`.
    pub async fn load_with_proof(
        &self,
        prg: &PrgId,
        committed_floor: Option<u64>,
    ) -> Result<Option<RecoveredState>> {
        let result = self.load_inner(prg, committed_floor).await?;
        self.update_cached_indices(prg, &result).await;
        Ok(result.state.map(|state| RecoveredState {
            state,
            proof: result.proof,
            effective_floor: result.effective_floor,
        }))
    }

    /// Load state capped at `committed_floor` to enforce ledger proofs during DR restore.
    pub async fn load_at(
        &self,
        prg: &PrgId,
        committed_floor: Option<u64>,
    ) -> Result<Option<PersistedPrgState>> {
        let result = self.load_inner(prg, committed_floor).await?;
        self.update_cached_indices(prg, &result).await;
        Ok(result.state)
    }

    async fn update_cached_indices(&self, prg: &PrgId, result: &LoadResult) {
        let mut guard = self.next_index.lock().await;
        guard.insert(prg.clone(), result.last_index.saturating_add(1));
        let mut floors = self.floors.lock().await;
        if let Some(ref recovered) = result.state {
            let capped =
                Self::compute_effective_floor(recovered, result.last_index).min(result.last_index);
            let effective = result.effective_floor.min(result.last_index).max(capped);
            floors.insert(prg.clone(), effective);
        }
    }

    async fn load_inner(&self, prg: &PrgId, committed_floor: Option<u64>) -> Result<LoadResult> {
        let layout = self.layout_for(prg);
        let prg_key = prg.clone();
        let prg_label = Self::partition_key(&prg_key);
        let base = self.base.clone();
        let work = move || -> Result<LoadResult> {
            layout
                .ensure()
                .with_context(|| format!("ensure layout {}", base.display()))?;
            Self::ensure_metadata(&layout, &prg_key)?;
            let mut state = layout
                .load_state()
                .with_context(|| format!("load layout {}", base.display()))?;
            state.wal_segments.sort_by_key(|seg| seg.seq);
            let snapshot_state = state.snapshots.last().and_then(|snap| {
                std::fs::read(&snap.manifest_path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<SnapshotManifest>(&bytes).ok())
            });
            let snapshot_term = snapshot_state
                .as_ref()
                .map(|snap| snap.term)
                .unwrap_or(DEFAULT_TERM);
            let mut proof = snapshot_state
                .as_ref()
                .and_then(|snap| snap.proof.as_ref().map(DurabilityProof::from))
                .or_else(|| {
                    snapshot_state
                        .as_ref()
                        .map(|snap| DurabilityProof::new(snap.term, snap.index))
                });
            let mut recovered = snapshot_state
                .clone()
                .filter(|snap| committed_floor.map(|f| snap.index <= f).unwrap_or(true))
                .map(|mut snap| {
                    snap.state.committed_index = snap.index;
                    snap.state
                });
            let mut last_index = snapshot_state
                .as_ref()
                .filter(|snap| committed_floor.map(|f| snap.index <= f).unwrap_or(true))
                .map(|snap| snap.index)
                .unwrap_or(0);
            let mut effective_floor = snapshot_state
                .as_ref()
                .map(|snap| snap.effective_floor)
                .unwrap_or(0);
            let replay =
                WalReplayScanner::scan(&state.wal_segments).context("replay wal for PRG state")?;
            if let Some(trunc) = &replay.truncation {
                replay
                    .enforce_truncation()
                    .context("truncate corrupt WAL segment")?;
                tracing::warn!(
                    "wal truncation detected for {} bytes={} segment={}",
                    Self::partition_key(&prg_key),
                    trunc.truncated_bytes,
                    trunc.segment_seq
                );
            }
            for frame in replay.frames {
                if frame.header.index <= last_index {
                    continue;
                }
                if let Some(floor) = committed_floor {
                    if frame.header.index > floor {
                        break;
                    }
                }
                let entry: PrgWalEntry =
                    serde_json::from_slice(&frame.payload).context("decode wal entry")?;
                let mut state = entry.state;
                state.committed_index = frame.header.index;
                recovered = Some(state);
                last_index = frame.header.index;
                proof = Some(DurabilityProof::new(frame.header.term, frame.header.index));
            }
            if proof.is_none() && last_index > 0 {
                proof = Some(DurabilityProof::new(snapshot_term, last_index));
            }
            if let Some(ref state) = recovered {
                effective_floor =
                    effective_floor.max(Self::compute_effective_floor(state, last_index));
            }
            Ok(LoadResult {
                state: recovered,
                last_index,
                effective_floor,
                proof,
            })
        };
        if replay_inline() || Handle::try_current().is_err() {
            work()
        } else {
            match tokio::task::spawn_blocking(work).await {
                Ok(result) => result,
                Err(join_err) => {
                    if join_err.is_cancelled() {
                        warn!(
                            "wal replay task cancelled for {} (set QUANTUM_INLINE_WAL_REPLAY=1 to run inline)",
                            prg_label
                        );
                    } else if join_err.is_panic() {
                        error!(
                            "wal replay task panicked for {} (enable RUST_BACKTRACE=1 to inspect)",
                            prg_label
                        );
                    }
                    Err(join_err.into())
                }
            }
        }
    }

    /// List PRGs present on disk using the `prg_<tenant>_<partition>` layout.
    pub async fn list_prgs(&self) -> Result<Vec<PrgId>> {
        let base = self.base.clone();
        let work = move || -> Result<Vec<PrgId>> {
            if !base.exists() {
                return Ok(Vec::new());
            }
            let mut prgs = Vec::new();
            for entry in fs::read_dir(base.clone())? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some(prg) = Self::parse_prg_dir(&name) {
                    prgs.push(prg);
                }
            }
            Ok(prgs)
        };
        if Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(work)
                .await
                .context("join prg discovery")?
        } else {
            work()
        }
    }

    fn parse_prg_dir(name: &str) -> Option<PrgId> {
        let stripped = name.strip_prefix("prg_")?;
        let (tenant, partition) = stripped.rsplit_once('_')?;
        let partition_index = partition.parse().ok()?;
        Some(PrgId {
            tenant_id: tenant.to_string(),
            partition_index,
        })
    }

    /// Return WAL segment references for the PRG if present.
    pub async fn wal_segments(&self, prg: &PrgId) -> Result<Vec<WalSegmentRef>> {
        let layout = self.layout_for(prg);
        let base = self.base.clone();
        let work = move || -> Result<Vec<WalSegmentRef>> {
            layout
                .ensure()
                .with_context(|| format!("ensure layout {}", base.display()))?;
            let mut state = layout
                .load_state()
                .with_context(|| format!("load layout {}", base.display()))?;
            state.wal_segments.sort_by_key(|seg| seg.seq);
            Ok(state.wal_segments)
        };
        if Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(work)
                .await
                .context("join wal segment discovery")?
        } else {
            work()
        }
    }

    /// Return the latest snapshot manifest metadata for a PRG if present.
    pub(crate) async fn latest_snapshot_summary(
        &self,
        prg: &PrgId,
    ) -> Result<Option<SnapshotSummary>> {
        let layout = self.layout_for(prg);
        let base = self.base.clone();
        let work = move || -> Result<Option<SnapshotSummary>> {
            layout
                .ensure()
                .with_context(|| format!("ensure layout {}", base.display()))?;
            let mut state = layout
                .load_state()
                .with_context(|| format!("load layout {}", base.display()))?;
            state.snapshots.sort_by_key(|snap| (snap.term, snap.index));
            let Some(snapshot) = state.snapshots.last() else {
                return Ok(None);
            };
            let bytes = fs::read(&snapshot.manifest_path)
                .with_context(|| format!("read manifest {}", snapshot.manifest_path.display()))?;
            let manifest: SnapshotManifest = serde_json::from_slice(&bytes)?;
            let proof = manifest
                .proof
                .as_ref()
                .map(DurabilityProof::from)
                .or_else(|| Some(DurabilityProof::new(manifest.term, manifest.index)));
            Ok(Some(SnapshotSummary {
                index: manifest.index,
                path: snapshot.manifest_path.clone(),
                effective_floor: manifest.effective_floor,
                proof,
            }))
        };
        if Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(work)
                .await
                .context("join snapshot manifest read")?
        } else {
            work()
        }
    }

    /// Last committed index observed for a PRG based on next_index cache.
    pub async fn last_index(&self, prg: &PrgId) -> Result<u64> {
        self.ensure_next_index_seeded(prg).await?;
        let guard = self.next_index.lock().await;
        Ok(guard.get(prg).cloned().unwrap_or(1).saturating_sub(1))
    }

    /// Effective product floor derived from the most recent snapshot/WAL for a PRG.
    pub async fn effective_floor(&self, prg: &PrgId) -> u64 {
        self.floors.lock().await.get(prg).cloned().unwrap_or(0)
    }
}

fn replay_inline() -> bool {
    matches!(
        env::var("QUANTUM_INLINE_WAL_REPLAY")
            .ok()
            .as_deref()
            .map(str::to_lowercase)
            .as_deref(),
        Some("1" | "true" | "yes")
    )
}
