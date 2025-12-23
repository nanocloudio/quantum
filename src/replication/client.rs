use clustor::consensus::DurabilityProof;
use clustor::durability::{AckRecord, DurabilityAckMessage, DurabilityLedger, IoMode, LedgerError};
use clustor::raft::{PartitionQuorumConfig, ReplicaId};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{broadcast, Notify};

/// Broadcastable ACK event representing a quorum commit.
#[derive(Debug, Clone)]
pub struct AckEvent {
    pub term: u64,
    pub index: u64,
}

/// Broadcastable apply event used to bootstrap partitions and enforce product floors.
#[derive(Debug, Clone)]
pub struct ApplyEvent {
    pub term: u64,
    pub index: u64,
    pub floor: u64,
    pub payload: Option<Vec<u8>>,
}

/// Minimal facade over clustor durability ledger and replication lag state.
#[derive(Clone, Debug)]
pub struct ClustorClient {
    ledger: Arc<Mutex<DurabilityLedger>>,
    floor: Arc<AtomicU64>,
    replication_lag_seconds: Arc<AtomicU64>,
    durability_fence: Arc<AtomicBool>,
    notify: Arc<Notify>,
    term: Arc<AtomicU64>,
    replica_id: ReplicaId,
    ack_tx: broadcast::Sender<AckEvent>,
    apply_tx: broadcast::Sender<ApplyEvent>,
    replicas: Arc<Mutex<Vec<ReplicaId>>>,
}

impl ClustorClient {
    /// Create a new clustor client with a local replica and quorum config.
    pub fn new(quorum: usize, replica_id: impl Into<String>, extra_replicas: &[String]) -> Self {
        let mut ledger = DurabilityLedger::new(PartitionQuorumConfig::new(quorum));
        let replica = ReplicaId::new(replica_id.into());
        ledger.register_replica(replica.clone());
        for peer in extra_replicas {
            let id = ReplicaId::new(peer.clone());
            ledger.register_replica(id.clone());
        }
        let (ack_tx, _) = broadcast::channel(32);
        let (apply_tx, _) = broadcast::channel(32);
        let replicas = {
            let mut all = Vec::new();
            all.push(replica.clone());
            for peer in extra_replicas {
                all.push(ReplicaId::new(peer.clone()));
            }
            Arc::new(Mutex::new(all))
        };
        Self {
            ledger: Arc::new(Mutex::new(ledger)),
            floor: Arc::new(AtomicU64::new(0)),
            replication_lag_seconds: Arc::new(AtomicU64::new(0)),
            durability_fence: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            term: Arc::new(AtomicU64::new(0)),
            replica_id: replica,
            ack_tx,
            apply_tx,
            replicas,
        }
    }

    /// Reconfigure quorum/replica set (only safe before first commit).
    pub fn reconfigure(&self, voters: usize, replicas: &[ReplicaId]) {
        if self.floor() > 0 {
            tracing::debug!(
                "clustor client reconfigure skipped because floor already set to {}",
                self.floor()
            );
            return;
        }
        let mut ledger = DurabilityLedger::new(PartitionQuorumConfig::new(voters.max(1)));
        for replica in replicas {
            ledger.register_replica(replica.clone());
        }
        if let Ok(mut guard) = self.ledger.lock() {
            *guard = ledger;
        }
        if let Ok(mut guard) = self.replicas.lock() {
            guard.clear();
            guard.extend(replicas.iter().cloned());
        }
    }

    pub fn register_replica(&self, replica_id: ReplicaId) -> Result<(), LedgerError> {
        self.ledger
            .lock()
            .unwrap()
            .register_replica(replica_id.clone());
        if let Ok(mut replicas) = self.replicas.lock() {
            if !replicas.iter().any(|r| r == &replica_id) {
                replicas.push(replica_id);
            }
        }
        Ok(())
    }

    pub fn ack_stream(&self) -> broadcast::Receiver<AckEvent> {
        self.ack_tx.subscribe()
    }

    pub fn apply_stream(&self) -> broadcast::Receiver<ApplyEvent> {
        self.apply_tx.subscribe()
    }

    /// Seed a partition with a committed index (e.g., after bootstrap) and surface an apply event.
    pub fn bootstrap_partition(&self, committed_index: u64) {
        self.floor.store(committed_index, Ordering::Relaxed);
        let _ = self.apply_tx.send(ApplyEvent {
            term: self.term(),
            index: committed_index,
            floor: committed_index,
            payload: None,
        });
        self.notify.notify_waiters();
    }

    /// Record a local commit/ack; returns the committed quorum index.
    pub fn record_local_commit(
        &self,
        term: u64,
        index: u64,
        io_mode: IoMode,
        payload: Option<Vec<u8>>,
    ) -> Result<u64, LedgerError> {
        let record = AckRecord {
            replica: self.replica_id.clone(),
            term,
            index,
            segment_seq: index,
            io_mode,
        };
        let update = self.ledger.lock().unwrap().record_ack(record)?;
        self.term.store(term, Ordering::Relaxed);
        self.floor
            .store(update.quorum_index.max(index), Ordering::Relaxed);
        let _ = self.ack_tx.send(AckEvent {
            term,
            index: update.quorum_index,
        });
        let _ = self.apply_tx.send(ApplyEvent {
            term,
            index: update.quorum_index,
            floor: self.floor(),
            payload,
        });
        self.notify.notify_waiters();
        Ok(update.quorum_index)
    }

    /// Ingest an ACK from a remote replica.
    pub fn ingest_ack(&self, ack: DurabilityAckMessage) -> Result<u64, LedgerError> {
        let update = self.ledger.lock().unwrap().ingest_ack(ack)?;
        self.term.store(update.record.term, Ordering::Relaxed);
        self.floor.store(
            update.quorum_index.max(update.record.index),
            Ordering::Relaxed,
        );
        let _ = self.ack_tx.send(AckEvent {
            term: update.record.term,
            index: update.quorum_index,
        });
        let _ = self.apply_tx.send(ApplyEvent {
            term: update.record.term,
            index: update.quorum_index,
            floor: self.floor(),
            payload: None,
        });
        self.notify.notify_waiters();
        Ok(update.quorum_index)
    }

    /// Block until the quorum commit index is ≥ target_index.
    pub async fn wait_for_ack(&self, target_index: u64) -> Result<(), LedgerError> {
        let mut apply_rx = self.apply_tx.subscribe();
        let has_rt = Handle::try_current().is_ok();
        loop {
            if self.floor.load(Ordering::Relaxed) >= target_index {
                return Ok(());
            }
            tokio::select! {
                _ = self.notify.notified() => {},
                recv = apply_rx.recv() => {
                    if let Ok(evt) = recv {
                        if evt.index >= target_index {
                            return Ok(());
                        }
                    }
                }
                _ = async {
                    if has_rt {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    } else {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                } => {},
            }
        }
    }

    pub fn floor(&self) -> u64 {
        self.floor.load(Ordering::Relaxed)
    }

    pub fn set_replication_lag(&self, seconds: u64) {
        self.replication_lag_seconds
            .store(seconds, Ordering::Relaxed);
    }

    pub fn replication_lag_seconds(&self) -> u64 {
        self.replication_lag_seconds.load(Ordering::Relaxed)
    }

    pub fn durability_fence_active(&self) -> bool {
        self.durability_fence.load(Ordering::Relaxed)
    }

    pub fn set_durability_fence(&self, active: bool) {
        self.durability_fence.store(active, Ordering::Relaxed);
    }

    pub fn latest_proof(&self) -> Option<DurabilityProof> {
        self.ledger
            .lock()
            .ok()
            .and_then(|ledger| ledger.latest_proof())
    }

    pub fn committed_index(&self) -> u64 {
        self.ledger
            .lock()
            .map(|ledger| ledger.status().committed_index)
            .unwrap_or(0)
    }

    fn term(&self) -> u64 {
        self.term.load(Ordering::Relaxed)
    }
}
