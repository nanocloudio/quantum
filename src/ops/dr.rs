use crate::control::{CacheState, ControlPlaneClient};
use crate::flow::BackpressureState;
use crate::prg::PersistedPrgState;
use crate::replication::consensus::AckContract;
use crate::routing::PrgId;
use crate::storage::ClustorStorage;
use crate::time::Clock;
use clustor::consensus::DurabilityProof;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub struct CheckpointExport {
    pub prg: PrgId,
    pub index: u64,
    pub path: PathBuf,
    pub proof: Option<DurabilityProof>,
    pub effective_floor: u64,
}

#[derive(Debug, Clone)]
pub struct WalExport {
    pub prg: PrgId,
    pub segment_seq: u64,
    pub path: PathBuf,
    pub committed_index: u64,
    pub proof: Option<DurabilityProof>,
    pub floor: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DrShippingStatus {
    pub last_index: u64,
    pub last_proof: Option<DurabilityProof>,
}

#[derive(Debug, Clone, Copy)]
pub struct BackupSchedule {
    pub cp_snapshot_secs: u64,
    pub checkpoint_secs: u64,
    pub wal_archive_secs: u64,
}

pub trait DrMirror: Send + Sync {
    fn ship_checkpoint(&self, export: CheckpointExport);
    fn ship_wal(&self, export: WalExport);
    fn restore(&self, prg: &PrgId, committed_index: u64);
    fn promotion(&self, mode: DrPromotion, certificate: Option<String>);
}

#[derive(Debug, Clone, Copy)]
pub enum DrPromotion {
    Controlled,
    Uncontrolled,
}

#[derive(Debug, Default, Clone)]
struct NoopDrMirror;

impl DrMirror for NoopDrMirror {
    fn ship_checkpoint(&self, _export: CheckpointExport) {}
    fn ship_wal(&self, _export: WalExport) {}
    fn restore(&self, _prg: &PrgId, _committed_index: u64) {}
    fn promotion(&self, _mode: DrPromotion, _certificate: Option<String>) {}
}

#[derive(Debug, Clone)]
pub struct FilesystemDrMirror {
    root: PathBuf,
}

impl FilesystemDrMirror {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn partition_dir(&self, prg: &PrgId) -> PathBuf {
        self.root
            .join(format!("prg_{}_{}", prg.tenant_id, prg.partition_index))
    }

    fn persist_metadata(
        &self,
        path: &Path,
        index: u64,
        floor: u64,
        proof: &Option<DurabilityProof>,
    ) {
        let meta_path = path.with_extension("meta");
        let meta = json!({
            "index": index,
            "effective_floor": floor,
            "proof_term": proof.as_ref().map(|p| p.term),
            "proof_index": proof.as_ref().map(|p| p.index),
        });
        if let Some(parent) = meta_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::write(meta_path, serde_json::to_vec(&meta).unwrap_or_default());
    }
}

impl DrMirror for FilesystemDrMirror {
    fn ship_checkpoint(&self, export: CheckpointExport) {
        let dir = self.partition_dir(&export.prg).join("checkpoint");
        if let Err(err) = fs::create_dir_all(&dir) {
            tracing::warn!("dr filesystem mirror: create checkpoint dir error: {err:?}");
            return;
        }
        let dest = dir.join(format!("snap-{:020}.json", export.index));
        if let Err(err) = fs::copy(&export.path, &dest) {
            tracing::warn!("dr filesystem mirror: checkpoint copy failed: {err:?}");
            return;
        }
        self.persist_metadata(&dest, export.index, export.effective_floor, &export.proof);
    }

    fn ship_wal(&self, export: WalExport) {
        let dir = self.partition_dir(&export.prg).join("wal");
        if let Err(err) = fs::create_dir_all(&dir) {
            tracing::warn!("dr filesystem mirror: create wal dir error: {err:?}");
            return;
        }
        let dest = dir.join(format!("segment-{:010}.log", export.segment_seq));
        if let Err(err) = fs::copy(&export.path, &dest) {
            tracing::warn!("dr filesystem mirror: wal copy failed: {err:?}");
            return;
        }
        self.persist_metadata(&dest, export.committed_index, export.floor, &export.proof);
    }

    fn restore(&self, prg: &PrgId, committed_index: u64) {
        tracing::info!(
            "dr filesystem mirror restore {:?} to {}",
            prg,
            committed_index
        );
    }

    fn promotion(&self, mode: DrPromotion, certificate: Option<String>) {
        tracing::warn!(
            "dr filesystem mirror promotion {:?} cert={}",
            mode,
            certificate.unwrap_or_default()
        );
    }
}

#[derive(Debug, Clone)]
pub struct ObjectStoreDrMirror {
    fs: FilesystemDrMirror,
}

impl ObjectStoreDrMirror {
    pub fn new(bucket: impl Into<PathBuf>, prefix: Option<String>) -> Self {
        let mut root = bucket.into();
        if let Some(prefix) = prefix {
            root = root.join(prefix);
        }
        Self {
            fs: FilesystemDrMirror::new(root),
        }
    }
}

impl DrMirror for ObjectStoreDrMirror {
    fn ship_checkpoint(&self, export: CheckpointExport) {
        self.fs.ship_checkpoint(export);
    }

    fn ship_wal(&self, export: WalExport) {
        self.fs.ship_wal(export);
    }

    fn restore(&self, prg: &PrgId, committed_index: u64) {
        self.fs.restore(prg, committed_index);
    }

    fn promotion(&self, mode: DrPromotion, certificate: Option<String>) {
        self.fs.promotion(mode, certificate);
    }
}

/// Tracks DR shipping state, promotion flows, and backup schedules.
#[derive(Clone)]
pub struct DrManager<C: Clock> {
    clock: C,
    ack_contract: AckContract,
    storage: ClustorStorage,
    last_ship: Arc<std::sync::Mutex<Option<Instant>>>,
    last_checkpoint_ship: Arc<std::sync::Mutex<Option<Instant>>>,
    last_wal_ship: Arc<std::sync::Mutex<Option<Instant>>>,
    lag_seconds: Arc<std::sync::Mutex<u64>>,
    promoted: Arc<std::sync::Mutex<bool>>,
    uncontrolled: Arc<std::sync::Mutex<bool>>,
    backups: Arc<std::sync::Mutex<BackupSchedule>>,
    metrics: Arc<BackpressureState>,
    region_certificate: Arc<std::sync::Mutex<Option<String>>>,
    expected_region_certificate: Arc<std::sync::Mutex<Option<String>>>,
    certificate_mismatch: Arc<std::sync::Mutex<bool>>,
    mirror: Arc<std::sync::Mutex<Arc<dyn DrMirror + Send + Sync>>>,
    control_plane: Arc<std::sync::Mutex<Option<ControlPlaneClient<C>>>>,
    shipped: Arc<std::sync::Mutex<HashMap<PrgId, DrShippingStatus>>>,
}

impl Default for BackupSchedule {
    fn default() -> Self {
        Self {
            cp_snapshot_secs: 60,
            checkpoint_secs: 300,
            wal_archive_secs: 1800,
        }
    }
}

impl<C: Clock> std::fmt::Debug for DrManager<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DrManager")
            .field("lag_seconds", &self.lag_seconds.lock().ok())
            .field("promoted", &self.promoted.lock().ok())
            .field("uncontrolled", &self.uncontrolled.lock().ok())
            .finish()
    }
}

impl<C: Clock> DrManager<C> {
    pub fn new(
        clock: C,
        ack_contract: AckContract,
        metrics: Arc<BackpressureState>,
        storage: ClustorStorage,
    ) -> Self {
        Self {
            clock,
            ack_contract,
            storage,
            last_ship: Arc::new(std::sync::Mutex::new(None)),
            last_checkpoint_ship: Arc::new(std::sync::Mutex::new(None)),
            last_wal_ship: Arc::new(std::sync::Mutex::new(None)),
            lag_seconds: Arc::new(std::sync::Mutex::new(0)),
            promoted: Arc::new(std::sync::Mutex::new(false)),
            uncontrolled: Arc::new(std::sync::Mutex::new(false)),
            backups: Arc::new(std::sync::Mutex::new(BackupSchedule::default())),
            metrics,
            region_certificate: Arc::new(std::sync::Mutex::new(None)),
            expected_region_certificate: Arc::new(std::sync::Mutex::new(None)),
            certificate_mismatch: Arc::new(std::sync::Mutex::new(false)),
            mirror: Arc::new(std::sync::Mutex::new(Arc::new(NoopDrMirror))),
            control_plane: Arc::new(std::sync::Mutex::new(None)),
            shipped: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Install a mirror to ship DR artifacts to an external backend.
    pub fn install_mirror(&self, mirror: Arc<dyn DrMirror + Send + Sync>) {
        if let Ok(mut guard) = self.mirror.lock() {
            *guard = mirror;
        }
    }

    /// Attach a control-plane handle so we can validate observer catch-up.
    pub fn set_control_plane(&self, control_plane: ControlPlaneClient<C>) {
        if let Ok(mut guard) = self.control_plane.lock() {
            *guard = Some(control_plane);
        }
    }

    /// Expected certificate for DR promotions.
    pub fn set_expected_region_certificate(&self, cert: Option<String>) {
        if let Ok(mut guard) = self.expected_region_certificate.lock() {
            *guard = cert;
        }
    }

    pub fn shipping_snapshot(&self) -> HashMap<String, DrShippingStatus> {
        self.shipped
            .lock()
            .map(|map| {
                map.iter()
                    .map(|(prg, status)| {
                        (
                            format!("{}:{}", prg.tenant_id, prg.partition_index),
                            status.clone(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Start WAL/checkpoint shipping; records last ship time and zeroes lag.
    pub async fn start_shipping(&self) {
        if self.ack_contract.committed_index() == 0 {
            let _ = self.ack_contract.wait_for_commit().await;
        }
        if let Err(err) = self.ship_once().await {
            tracing::warn!("initial DR ship failed: {err:?}");
        }
        self.record_ship_time();
        if Handle::try_current().is_ok() {
            let this = self.clone();
            tokio::spawn(async move { this.ship_loop().await });
        } else {
            tracing::info!("skipping DR ship loop; no tokio runtime detected");
        }
        tracing::info!("DR shipping started (checkpoint + WAL)");
    }

    pub async fn ship_once(&self) -> anyhow::Result<()> {
        self.ship_exports(true, true).await
    }

    pub fn update_lag(&self, seconds: u64) {
        if let Ok(mut lag) = self.lag_seconds.lock() {
            *lag = seconds;
        }
        self.ack_contract.set_replication_lag(seconds);
        self.metrics.set_replication_lag(seconds);
        self.metrics.set_apply_lag(seconds);
    }

    pub fn lag_seconds(&self) -> u64 {
        self.lag_seconds.lock().map(|v| *v).unwrap_or_default()
    }

    /// Seconds since last successful ship (checkpoint or WAL).
    pub fn last_ship_age_seconds(&self) -> u64 {
        if let Ok(last) = self.last_ship.lock() {
            if let Some(at) = *last {
                return self.clock.now().saturating_duration_since(at).as_secs();
            }
        }
        0
    }

    /// Controlled promotion: fence durability then mark promoted.
    pub async fn controlled_promotion(&self) {
        if let Err(err) = self.verify_promotion_certificate() {
            tracing::warn!("controlled promotion blocked: {err:?}");
            return;
        }
        self.ack_contract.set_durability_fence(true);
        let proof = self.await_durability_proof().await;
        if proof.is_none() {
            tracing::warn!("controlled promotion aborted: durability proof unavailable");
            self.ack_contract.set_durability_fence(false);
            return;
        }
        if !self.cp_observer_ready() {
            tracing::warn!("controlled promotion paused: control-plane observers not caught up");
            self.ack_contract.set_durability_fence(false);
            return;
        }
        self.set_uncontrolled(false);
        if let Ok(mut promoted) = self.promoted.lock() {
            *promoted = true;
        }
        self.ack_contract.set_durability_fence(false);
        crate::audit::emit("dr_promotion", "system", "dr", "controlled");
        self.mirror(|m| m.promotion(DrPromotion::Controlled, self.region_certificate()));
        tracing::warn!("DR controlled promotion executed");
    }

    /// Uncontrolled promotion: mark unsafe path.
    pub fn uncontrolled_promotion(&self) {
        if let Err(err) = self.verify_promotion_certificate() {
            tracing::warn!("uncontrolled promotion blocked: {err:?}");
            return;
        }
        self.set_uncontrolled(true);
        if let Ok(mut promoted) = self.promoted.lock() {
            *promoted = false;
        }
        self.ack_contract.set_durability_fence(true);
        self.metrics.add_forward_backpressure(1);
        crate::audit::emit("dr_promotion", "system", "dr", "uncontrolled");
        self.mirror(|m| m.promotion(DrPromotion::Uncontrolled, self.region_certificate()));
        tracing::warn!("DR uncontrolled promotion activated");
    }

    pub fn promoted(&self) -> bool {
        self.promoted.lock().map(|v| *v).unwrap_or(false)
    }

    pub fn uncontrolled(&self) -> bool {
        self.uncontrolled.lock().map(|v| *v).unwrap_or(false)
    }

    /// Roll back an uncontrolled promotion once the region is healthy again.
    pub fn rollback_uncontrolled(&self) {
        self.set_uncontrolled(false);
        self.ack_contract.set_durability_fence(false);
        crate::audit::emit("dr_uncontrolled_rollback", "system", "dr", "rollback");
    }

    pub fn schedule_backups(&self, schedule: BackupSchedule) {
        if let Ok(mut s) = self.backups.lock() {
            *s = schedule;
        }
    }

    pub fn backup_schedule(&self) -> BackupSchedule {
        self.backups
            .lock()
            .map(|v| *v)
            .unwrap_or_else(|_| BackupSchedule::default())
    }

    /// Placeholder restore hook.
    pub async fn restore_from_backup(
        &self,
        region_certificate: Option<&str>,
    ) -> anyhow::Result<()> {
        self.ack_contract.set_durability_fence(true);
        self.validate_region_certificate(region_certificate)?;
        let prgs = self.storage.list_prgs().await?;
        let committed = self.ack_contract.committed_index();
        let certificate = region_certificate
            .map(|c| c.to_string())
            .or_else(|| self.region_certificate());
        let mut restored = 0usize;
        for prg in prgs {
            let target = if committed > 0 {
                committed
            } else {
                self.storage.last_index(&prg).await.unwrap_or(0)
            };
            let _ = self
                .restore_with_proof(&self.storage, &prg, target, certificate.as_deref())
                .await?;
            restored += 1;
        }
        if let Ok(mut promoted) = self.promoted.lock() {
            *promoted = false;
        }
        self.set_certificate_mismatch(false);
        self.set_uncontrolled(false);
        self.update_lag(0);
        self.ack_contract.set_durability_fence(false);
        crate::audit::emit(
            "dr_restore",
            "system",
            "dr",
            &format!("restored {} partitions", restored),
        );
        tracing::info!("restore completed for {} partitions", restored);
        Ok(())
    }

    /// Restore a PRG state using ledger proofs and enforce region certificate fencing.
    pub async fn restore_with_proof(
        &self,
        storage: &ClustorStorage,
        prg: &PrgId,
        committed_index: u64,
        region_certificate: Option<&str>,
    ) -> anyhow::Result<Option<PersistedPrgState>> {
        self.validate_region_certificate(region_certificate)?;
        let recovered = storage.load_with_proof(prg, Some(committed_index)).await?;
        if let Some(ref snapshot) = recovered {
            let effective_floor = storage
                .effective_floor(prg)
                .await
                .max(snapshot.effective_floor);
            if effective_floor > committed_index {
                anyhow::bail!(
                    "state floor {} exceeds committed {}",
                    effective_floor,
                    committed_index
                );
            }
            if let Some(proof) = snapshot.proof {
                self.ack_contract.set_term(proof.term);
                self.ack_contract.update_clustor_floor(proof.index);
            } else {
                self.ack_contract.update_clustor_floor(committed_index);
            }
            self.update_lag(0);
            self.metrics.set_apply_lag(0);
            self.mirror(|m| m.restore(prg, committed_index));
            tracing::info!(
                "restored {:?} to committed index {} (effective floor {})",
                prg,
                committed_index,
                effective_floor
            );
        }
        Ok(recovered.map(|r| r.state))
    }

    pub fn region_certificate(&self) -> Option<String> {
        self.region_certificate.lock().ok().and_then(|c| c.clone())
    }

    pub fn region_mismatch(&self) -> bool {
        self.certificate_mismatch
            .lock()
            .map(|v| *v)
            .unwrap_or(false)
    }

    pub fn cp_observer_ready(&self) -> bool {
        if self.uncontrolled() {
            return false;
        }
        if self.region_mismatch() {
            return false;
        }
        let cp = self.control_plane.lock().ok().and_then(|cp| cp.clone());
        let local_proof = self.ack_contract.latest_proof();
        if let Some(cp) = cp {
            let snapshot = cp.snapshot();
            if !matches!(snapshot.cache_state, CacheState::Fresh) {
                return false;
            }
            let observed = snapshot.last_published_proof.or(snapshot.last_local_proof);
            return match (local_proof, observed) {
                (Some(local), Some(obs)) => obs.index >= local.index && obs.term >= local.term,
                (Some(_), None) => false,
                _ => true,
            };
        }
        local_proof.is_some() || self.ack_contract.committed_index() == 0
    }

    async fn ship_loop(self) {
        loop {
            let schedule = self.backups.lock().map(|b| *b).unwrap_or_default();
            let now = self.clock.now();
            let checkpoint_due = self
                .last_checkpoint_ship
                .lock()
                .map(|last| {
                    last.map(|t| now.duration_since(t).as_secs() >= schedule.checkpoint_secs)
                        .unwrap_or(true)
                })
                .unwrap_or(true);
            let wal_due = self
                .last_wal_ship
                .lock()
                .map(|last| {
                    last.map(|t| now.duration_since(t).as_secs() >= schedule.wal_archive_secs)
                        .unwrap_or(true)
                })
                .unwrap_or(true);
            if checkpoint_due || wal_due {
                let _ = self.ack_contract.wait_for_commit().await;
                if let Err(err) = self.ship_exports(checkpoint_due, wal_due).await {
                    tracing::warn!("dr ship failed: {err:?}");
                }
            }
            let sleep_dur = std::time::Duration::from_secs(
                schedule
                    .checkpoint_secs
                    .min(schedule.wal_archive_secs)
                    .max(1),
            );
            sleep(sleep_dur).await;
            self.bump_lag();
        }
    }

    async fn ship_exports(&self, ship_checkpoint: bool, ship_wal: bool) -> anyhow::Result<()> {
        let prgs = self.storage.list_prgs().await?;
        let latest_proof = self.ack_contract.latest_proof();
        for prg in prgs {
            let floor = self.storage.effective_floor(&prg).await;
            if ship_checkpoint {
                if let Some(snapshot) = self.storage.latest_snapshot_summary(&prg).await? {
                    if self.should_ship(&prg, snapshot.index) {
                        let export = CheckpointExport {
                            prg: prg.clone(),
                            index: snapshot.index,
                            path: snapshot.path.clone(),
                            proof: snapshot.proof.or(latest_proof),
                            effective_floor: snapshot.effective_floor.max(floor),
                        };
                        self.mirror(|m| m.ship_checkpoint(export.clone()));
                        self.record_shipped(&prg, export.index, export.proof);
                        self.touch_checkpoint_ship();
                    }
                }
            }
            if ship_wal {
                let last_index = self.storage.last_index(&prg).await.unwrap_or(0);
                if last_index == 0 || !self.should_ship(&prg, last_index) {
                    continue;
                }
                let segments = self.storage.wal_segments(&prg).await.unwrap_or_default();
                for segment in segments {
                    let export = WalExport {
                        prg: prg.clone(),
                        segment_seq: segment.seq,
                        path: segment.log_path.clone(),
                        committed_index: last_index,
                        proof: latest_proof,
                        floor,
                    };
                    self.mirror(|m| m.ship_wal(export));
                }
                self.record_shipped(&prg, last_index, latest_proof);
                self.touch_wal_ship();
            }
        }
        if ship_checkpoint || ship_wal {
            self.record_ship_time();
        }
        Ok(())
    }

    fn should_ship(&self, prg: &PrgId, index: u64) -> bool {
        if index == 0 {
            return false;
        }
        self.shipped
            .lock()
            .map(|map| map.get(prg).map(|s| s.last_index < index).unwrap_or(true))
            .unwrap_or(true)
    }

    fn record_shipped(&self, prg: &PrgId, index: u64, proof: Option<DurabilityProof>) {
        if index == 0 {
            return;
        }
        if let Ok(mut guard) = self.shipped.lock() {
            guard.insert(
                prg.clone(),
                DrShippingStatus {
                    last_index: index,
                    last_proof: proof,
                },
            );
        }
    }

    fn touch_checkpoint_ship(&self) {
        let now = self.clock.now();
        if let Ok(mut guard) = self.last_checkpoint_ship.lock() {
            *guard = Some(now);
        }
    }

    fn touch_wal_ship(&self) {
        let now = self.clock.now();
        if let Ok(mut guard) = self.last_wal_ship.lock() {
            *guard = Some(now);
        }
    }

    fn record_ship_time(&self) {
        let now = self.clock.now();
        if let Ok(mut last) = self.last_ship.lock() {
            *last = Some(now);
        }
        self.update_lag(0);
    }

    fn bump_lag(&self) {
        if let Ok(last) = self.last_ship.lock() {
            if let Some(at) = *last {
                let lag = self.clock.now().saturating_duration_since(at).as_secs();
                self.update_lag(lag);
            }
        }
    }

    fn mirror(&self, f: impl FnOnce(&dyn DrMirror)) {
        if let Ok(guard) = self.mirror.lock() {
            let mirror = guard.clone();
            f(mirror.as_ref());
        }
    }

    async fn await_durability_proof(&self) -> Option<DurabilityProof> {
        if let Some(proof) = self.ack_contract.latest_proof() {
            return Some(proof);
        }
        let _ = self.ack_contract.wait_for_commit().await;
        self.ack_contract.latest_proof()
    }

    fn verify_promotion_certificate(&self) -> anyhow::Result<()> {
        let expected = self
            .expected_region_certificate
            .lock()
            .ok()
            .and_then(|v| v.clone());
        if let Some(expected) = expected {
            let observed = self.region_certificate();
            if observed.as_deref() != Some(expected.as_str()) {
                self.set_certificate_mismatch(true);
                self.ack_contract.set_durability_fence(true);
                crate::audit::emit(
                    "dr_region_certificate_mismatch",
                    "system",
                    "dr",
                    "promotion",
                );
                anyhow::bail!("region certificate mismatch for promotion");
            }
        } else {
            self.set_certificate_mismatch(false);
        }
        Ok(())
    }

    fn validate_region_certificate(&self, presented: Option<&str>) -> anyhow::Result<()> {
        if let Some(presented) = presented {
            if let Ok(mut cert) = self.region_certificate.lock() {
                match cert.as_ref() {
                    Some(existing) if existing == presented => {}
                    Some(_) => {
                        self.set_certificate_mismatch(true);
                        self.ack_contract.set_durability_fence(true);
                        crate::audit::emit(
                            "dr_region_certificate_mismatch",
                            "system",
                            "dr",
                            "restore",
                        );
                        anyhow::bail!("region certificate mismatch; fencing durability");
                    }
                    None => *cert = Some(presented.to_string()),
                }
            }
        }
        if let Some(expected) = self
            .expected_region_certificate
            .lock()
            .ok()
            .and_then(|v| v.clone())
        {
            if Some(expected.as_str()) != presented {
                self.set_certificate_mismatch(true);
                self.ack_contract.set_durability_fence(true);
                crate::audit::emit("dr_region_certificate_mismatch", "system", "dr", "expected");
                anyhow::bail!("expected region certificate not satisfied");
            }
        } else {
            self.set_certificate_mismatch(false);
        }
        Ok(())
    }

    fn set_certificate_mismatch(&self, mismatch: bool) {
        if let Ok(mut guard) = self.certificate_mismatch.lock() {
            *guard = mismatch;
        }
    }

    fn set_uncontrolled(&self, value: bool) {
        if let Ok(mut guard) = self.uncontrolled.lock() {
            let prev = *guard;
            *guard = value;
            if prev != value {
                let state = if value {
                    "uncontrolled_on"
                } else {
                    "uncontrolled_off"
                };
                crate::audit::emit("dr_state", "system", "dr", state);
            }
        }
    }
}
