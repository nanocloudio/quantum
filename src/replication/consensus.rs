use super::replica::RaftReplica;
use crate::clustor_client::ClustorClient;
use crate::config::{CommitVisibility, DurabilityConfig, DurabilityMode};
use crate::ops::FaultInjector;
use crate::prg::PersistedPrgState;
use crate::routing::PrgId;
use crate::workloads::mqtt::{MqttApply, Qos};
use anyhow::Result;
use clustor::control_plane::core::CommitVisibility as CpCommitVisibility;
use clustor::raft::ReplicaId;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;

/// Replicated Raft operation payloads.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", content = "data")]
pub enum RaftOp {
    Noop,
    PersistSnapshot(PersistSnapshotOp),
    Publish(PublishOp),
    Forward(ForwardOp),
    WorkloadApply(WorkloadApplyOp),
}

impl RaftOp {
    pub fn serialize(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| anyhow::anyhow!("serialize raft op: {e}"))
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow::anyhow!("deserialize raft op: {e}"))
    }
}

/// Snapshot persistence payload (includes full state).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistSnapshotOp {
    pub prg: PrgId,
    pub state: PersistedPrgState,
}

/// Publish payload replicated via Raft.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PublishOp {
    pub prg: PrgId,
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: Qos,
    #[serde(default)]
    pub retain: bool,
}

/// Cross-PRG forward payload replicated via Raft.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ForwardOp {
    pub session_prg: PrgId,
    pub topic_prg: PrgId,
    pub seq: u64,
    pub routing_epoch: u64,
}

/// Workload-specific apply payload replicated via Raft.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkloadApplyOp {
    pub prg: PrgId,
    pub entry: MqttApply,
}

/// Wrapper around the Clustor durability ledger for ACK gating.
/// Per-partition ACK contract; in the per-PRG path we instantiate one per PRG.
#[derive(Clone, Debug)]
pub struct AckContract {
    durability_mode: DurabilityMode,
    commit_visibility: CommitVisibility,
    durability_fence: Arc<AtomicBool>,
    clustor: ClustorClient,
    durability_cfg: crate::config::DurabilityConfig,
    next_index: Arc<AtomicU64>,
    term: Arc<AtomicU64>,
    fault_injector: Option<FaultInjector>,
    local_label: String,
    raft: Arc<Mutex<Option<Arc<RaftReplica>>>>,
}

impl AckContract {
    pub fn new(cfg: &DurabilityConfig) -> Self {
        let clustor =
            ClustorClient::new(cfg.quorum_size, cfg.replica_id.clone(), &cfg.peer_replicas);
        let commit_visibility = match cfg.durability_mode {
            DurabilityMode::GroupFsync => CommitVisibility::CommitAllowsPreDurable,
            DurabilityMode::Strict => cfg.commit_visibility.clone(),
        };
        Self {
            durability_mode: cfg.durability_mode.clone(),
            commit_visibility,
            durability_fence: Arc::new(AtomicBool::new(false)),
            clustor,
            durability_cfg: cfg.clone(),
            next_index: Arc::new(AtomicU64::new(0)),
            term: Arc::new(AtomicU64::new(cfg.initial_term)),
            fault_injector: None,
            local_label: cfg.replica_id.clone(),
            raft: Arc::new(Mutex::new(None)),
        }
    }

    /// Attach a fault injector to emulate clustor-layer delays/failures.
    pub fn with_fault_injector(mut self, fault_injector: FaultInjector) -> Self {
        self.fault_injector = Some(fault_injector);
        self
    }

    /// Await clustor quorum fsync + ledger proof for the next index.
    pub async fn wait_for_commit(&self) -> Result<u64> {
        self.commit_with(|_| async { RaftOp::Noop.serialize() })
            .await
    }

    /// Wait for the clustor floor to reach an existing committed index without recording a new entry.
    pub async fn wait_for_floor(&self, index: u64) -> Result<()> {
        if index == 0 {
            return Ok(());
        }
        if let Some(replica) = self.raft.lock().ok().and_then(|r| r.clone()) {
            let mut rx = replica.apply_stream();
            loop {
                if replica.committed_index() >= index {
                    return Ok(());
                }
                match rx.recv().await {
                    Ok(evt) if evt.index >= index => return Ok(()),
                    Ok(_) => continue,
                    Err(_) => anyhow::bail!("raft apply stream closed"),
                }
            }
        } else {
            self.clustor
                .wait_for_ack(index)
                .await
                .map_err(|e| anyhow::anyhow!("ledger wait error: {e}"))
        }
    }

    /// Reserve the next index, serialize the Raft op, then wait for quorum commit.
    pub async fn commit_with<F, Fut>(&self, op_builder: F) -> Result<u64>
    where
        F: FnOnce(u64) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>, anyhow::Error>> + Send,
    {
        let idx = self.next_index.fetch_add(1, Ordering::SeqCst) + 1;
        let term = self.term.load(Ordering::SeqCst);
        self.maybe_inject_faults().await?;
        let payload = op_builder(idx).await?;
        if let Some(replica) = self.raft.lock().ok().and_then(|r| r.clone()) {
            let _proof = replica
                .propose_index(idx, payload)
                .await
                .map_err(|e| anyhow::anyhow!("raft proposal failed: {e}"))?;
            replica
                .wait_for_apply(idx)
                .await
                .map_err(|e| anyhow::anyhow!("raft apply wait failed: {e}"))?;
            Ok(idx)
        } else {
            self.record_and_wait(term, idx, Some(payload)).await
        }
    }

    pub fn visibility(&self) -> &CommitVisibility {
        &self.commit_visibility
    }

    pub fn durability_mode(&self) -> &DurabilityMode {
        &self.durability_mode
    }

    pub fn durability_config(&self) -> crate::config::DurabilityConfig {
        self.durability_cfg.clone()
    }

    pub fn io_mode(&self) -> clustor::durability::IoMode {
        match self.durability_mode {
            DurabilityMode::Strict => clustor::durability::IoMode::Strict,
            DurabilityMode::GroupFsync => clustor::durability::IoMode::Group,
        }
    }

    /// Expose a clone of the underlying clustor client for Raft networking.
    pub fn clustor_client(&self) -> ClustorClient {
        self.clustor.clone()
    }

    pub fn has_raft(&self) -> bool {
        self.raft.lock().ok().and_then(|r| r.clone()).is_some()
    }

    pub fn cp_commit_visibility(&self) -> CpCommitVisibility {
        match self.commit_visibility {
            CommitVisibility::DurableOnly => CpCommitVisibility::DurableOnly,
            CommitVisibility::CommitAllowsPreDurable => CpCommitVisibility::CommitAllowsPreDurable,
        }
    }

    pub fn durability_fence_active(&self) -> bool {
        self.durability_fence.load(Ordering::Relaxed) || self.clustor.durability_fence_active()
    }

    pub fn set_durability_fence(&self, active: bool) {
        self.durability_fence.store(active, Ordering::Relaxed);
        self.clustor.set_durability_fence(active);
    }

    pub fn clustor_floor(&self) -> u64 {
        if let Some(replica) = self.raft.lock().ok().and_then(|r| r.clone()) {
            replica.committed_index()
        } else {
            self.clustor.floor()
        }
    }

    pub fn update_clustor_floor(&self, floor: u64) {
        if let Some(_replica) = self.raft.lock().ok().and_then(|r| r.clone()) {
            // When a Raft replica is attached, avoid re-seeding commits from observers
            // (e.g. WAL replay) to prevent re-entering apply hooks; just align the next
            // index with the observed floor.
            let _ = self.next_index.fetch_max(floor, Ordering::SeqCst);
            return;
        } else {
            let current = self.clustor.floor();
            if floor <= current {
                // Ignore regressions to avoid quorum mismatch warnings.
                let _ = self.next_index.fetch_max(current, Ordering::SeqCst);
                return;
            }
            let _ = self.clustor.record_local_commit(
                self.term.load(Ordering::SeqCst),
                floor,
                self.io_mode(),
                None,
            );
        }
        let _ = self.next_index.fetch_max(floor, Ordering::SeqCst);
    }

    pub fn set_replication_lag(&self, seconds: u64) {
        self.clustor.set_replication_lag(seconds);
    }

    pub fn replication_lag_seconds(&self) -> u64 {
        if let Some(replica) = self.raft.lock().ok().and_then(|r| r.clone()) {
            replica.status().pending_entries
        } else {
            self.clustor.replication_lag_seconds()
        }
    }

    pub fn latest_proof(&self) -> Option<clustor::consensus::DurabilityProof> {
        if let Some(replica) = self.raft.lock().ok().and_then(|r| r.clone()) {
            replica.latest_proof()
        } else {
            self.clustor.latest_proof()
        }
    }

    pub fn committed_index(&self) -> u64 {
        if let Some(replica) = self.raft.lock().ok().and_then(|r| r.clone()) {
            replica.committed_index()
        } else {
            self.clustor.committed_index()
        }
    }

    /// Demote Raft leadership (if attached) and flush to current commit index.
    pub async fn step_down_for_upgrade(&self) -> Result<()> {
        if let Some(replica) = self.raft.lock().ok().and_then(|r| r.clone()) {
            replica
                .step_down()
                .await
                .map_err(|e| anyhow::anyhow!("raft step-down failed: {e}"))?;
        }
        Ok(())
    }

    pub fn register_replica(&self, replica: clustor::raft::ReplicaId) {
        let _ = self.clustor.register_replica(replica);
    }

    /// Reconfigure quorum/replica set from control-plane placements (safe before first commit).
    pub fn reconfigure_from_placements(
        &self,
        placements: &std::collections::HashMap<crate::routing::PrgId, crate::routing::PrgPlacement>,
    ) {
        let mut labels: std::collections::HashSet<String> = placements
            .values()
            .flat_map(|p| {
                std::iter::once(p.node_id.clone())
                    .chain(p.replicas.iter().cloned())
                    .collect::<Vec<_>>()
            })
            .collect();
        labels.insert(self.local_replica_label());
        let replicas: Vec<ReplicaId> = labels.into_iter().map(ReplicaId::new).collect();
        let voters = replicas.len().max(1);
        self.clustor.reconfigure(voters, &replicas);
    }

    fn local_replica_label(&self) -> String {
        self.local_label.clone()
    }

    pub fn set_term(&self, term: u64) {
        self.term.store(term, Ordering::SeqCst);
    }

    pub fn term(&self) -> u64 {
        self.term.load(Ordering::SeqCst)
    }

    pub fn apply_stream(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::clustor_client::ApplyEvent> {
        if let Some(replica) = self.raft.lock().ok().and_then(|r| r.clone()) {
            replica.apply_stream()
        } else {
            self.clustor.apply_stream()
        }
    }

    /// Attach a Raft replica to drive networked commits.
    pub fn attach_raft(&self, replica: Arc<RaftReplica>) {
        if let Ok(mut guard) = self.raft.lock() {
            let committed = replica.committed_index();
            let _ = self.next_index.fetch_max(committed, Ordering::SeqCst);
            *guard = Some(replica);
        }
    }

    async fn delay(&self, duration: Duration) {
        if duration.is_zero() {
            return;
        }
        if Handle::try_current().is_ok() {
            tokio::time::sleep(duration).await;
        } else {
            std::thread::sleep(duration);
        }
    }

    async fn maybe_inject_faults(&self) -> Result<()> {
        if let Some(injector) = &self.fault_injector {
            if injector.kms_outage() {
                anyhow::bail!("kms outage injected");
            }
            if injector.election_delay() || injector.fsync_slow() {
                let delay_ms = injector.latency_ms().max(1) as u64;
                self.delay(Duration::from_millis(delay_ms)).await;
            }
        }
        Ok(())
    }

    async fn record_and_wait(&self, term: u64, idx: u64, payload: Option<Vec<u8>>) -> Result<u64> {
        self.clustor
            .record_local_commit(term, idx, self.io_mode(), payload)
            .map_err(|e| anyhow::anyhow!("ledger record error: {e}"))?;
        self.clustor
            .wait_for_ack(idx)
            .await
            .map_err(|e| anyhow::anyhow!("ledger wait error: {e}"))?;
        Ok(idx)
    }
}
