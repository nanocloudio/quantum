//! Raft replica management and networking.
//!
//! The `RaftReplica::start` function uses blocking `std::fs` calls for
//! directory creation and log initialization. This is intentional as it
//! runs during synchronous startup before the async runtime processes
//! network traffic.

use crate::clustor_client::{AckEvent, ApplyEvent};
use crate::config::{CommitVisibility, DurabilityMode};
use crate::routing::{PrgId, PrgPlacement};
use anyhow::{anyhow, Context, Result};
use clustor::consensus::{ConsensusCore, ConsensusCoreConfig, DurabilityProof};
use clustor::durability::{AckRecord, DurabilityLedger, IoMode, LedgerError};
use clustor::net::tls::{load_identity_from_pem, load_trust_store_from_pem};
use clustor::net::{
    AsyncRaftNetworkClient, AsyncRaftNetworkServer, AsyncRaftNetworkServerHandle,
    AsyncRaftTransportClientConfig, AsyncRaftTransportClientOptions,
    AsyncRaftTransportServerConfig,
};
use clustor::profile::PartitionProfile;
use clustor::replication::consensus::{RaftLogEntry, RaftLogStore};
use clustor::replication::raft::runtime_scaffold::{
    PinFuture, RaftNodeCallbacks, RaftNodeHandle, RaftNodeScaffold,
};
use clustor::replication::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ElectionController, PartitionQuorumConfig,
    RaftRouting, ReplicaId, RequestVoteRejectReason, RequestVoteRequest, RequestVoteResponse,
};
use clustor::replication::transport::raft::{RaftRpcHandler, RaftRpcServer};
use clustor::security::MtlsIdentityManager;
use parking_lot::Mutex as ParkingMutex;
use std::fmt;
use std::future::Future;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::broadcast;

const DEFAULT_ELECTION_SEED: u64 = 7;
const HEARTBEAT_BACKOFF: Duration = Duration::from_millis(50);

/// Configuration for spinning up a Raft replica derived from CP placement.
#[derive(Debug, Clone)]
pub struct RaftReplicaConfig {
    pub prg: PrgId,
    pub local_id: String,
    pub peers: Vec<(String, String)>,
    pub quorum: usize,
    pub routing_epoch: u64,
    pub durability_mode: DurabilityMode,
    pub commit_visibility: CommitVisibility,
    pub tls_chain: PathBuf,
    pub tls_key: PathBuf,
    pub trust_bundle: PathBuf,
    pub trust_domain: String,
    pub log_dir: PathBuf,
    pub bind_override: Option<String>,
}

impl RaftReplicaConfig {
    /// Build a config from CP placement data and durability profile settings.
    pub fn from_placement(
        prg: PrgId,
        placement: &PrgPlacement,
        durability: &crate::config::DurabilityConfig,
        paths: RaftReplicaPaths,
        routing_epoch: u64,
    ) -> Self {
        let mut seen = std::collections::HashSet::new();
        let mut peers: Vec<(String, String)> = Vec::new();
        for peer in
            std::iter::once(placement.node_id.clone()).chain(placement.replicas.iter().cloned())
        {
            if seen.insert(peer.clone()) {
                peers.push((peer.clone(), peer));
            }
        }
        if !seen.contains(&durability.replica_id) {
            peers.insert(
                0,
                (durability.replica_id.clone(), durability.replica_id.clone()),
            );
        }
        let quorum = peers.len().max(durability.quorum_size);
        let log_dir: PathBuf = paths
            .log_root
            .join(format!("prg_{}_{}", prg.tenant_id, prg.partition_index));
        Self {
            prg,
            local_id: durability.replica_id.clone(),
            peers,
            quorum,
            routing_epoch,
            durability_mode: durability.durability_mode.clone(),
            commit_visibility: durability.commit_visibility.clone(),
            tls_chain: paths.tls_chain,
            tls_key: paths.tls_key,
            trust_bundle: paths.trust_bundle,
            trust_domain: paths.trust_domain,
            log_dir,
            bind_override: paths.bind_override,
        }
    }
}

/// Paths/TLS material used to build a Raft replica.
#[derive(Debug, Clone)]
pub struct RaftReplicaPaths {
    pub tls_chain: PathBuf,
    pub tls_key: PathBuf,
    pub trust_bundle: PathBuf,
    pub trust_domain: String,
    pub log_root: PathBuf,
    pub bind_override: Option<String>,
}

/// Raft replica that owns durability ledger, consensus core, WAL, network server/clients, and timers.
pub struct RaftReplica {
    shared: Arc<ReplicaShared>,
    clients: Arc<Vec<PeerClient>>,
    _server: Option<RaftServerHandle>,
    _timers: Option<RaftNodeHandle>,
}

/// Minimal status surface for replication/apply lag and commit tracking.
#[derive(Debug, Clone, Copy)]
pub struct RaftReplicaStatus {
    pub commit_index: u64,
    pub pending_entries: u64,
}

struct ReplicaShared {
    id: ReplicaId,
    id_label: String,
    ledger: Mutex<DurabilityLedger>,
    core: Mutex<ConsensusCore>,
    log: Mutex<RaftLogStore>,
    routing: RaftRouting,
    commit_index: AtomicU64,
    term: AtomicU64,
    is_leader: AtomicBool,
    election_deadline: Mutex<Instant>,
    ack_tx: broadcast::Sender<AckEvent>,
    apply_tx: broadcast::Sender<ApplyEvent>,
    io_mode: IoMode,
    _visibility: CommitVisibility,
    apply_hook: Mutex<Option<ApplyHook>>,
}

impl fmt::Debug for ReplicaShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicaShared")
            .field("id", &self.id_label)
            .field("commit_index", &self.commit_index.load(Ordering::SeqCst))
            .field("is_leader", &self.is_leader.load(Ordering::SeqCst))
            .finish()
    }
}

impl ReplicaShared {
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: ReplicaId,
        ledger: DurabilityLedger,
        id_label: String,
        core: ConsensusCore,
        log: RaftLogStore,
        routing: RaftRouting,
        visibility: CommitVisibility,
        durability_mode: DurabilityMode,
    ) -> Self {
        let io_mode = match durability_mode {
            DurabilityMode::Strict => IoMode::Strict,
            DurabilityMode::GroupFsync => IoMode::Group,
        };
        let (ack_tx, _) = broadcast::channel(64);
        let (apply_tx, _) = broadcast::channel(64);
        Self {
            id,
            id_label,
            ledger: Mutex::new(ledger),
            core: Mutex::new(core),
            log: Mutex::new(log),
            routing,
            commit_index: AtomicU64::new(0),
            term: AtomicU64::new(1),
            is_leader: AtomicBool::new(true),
            election_deadline: Mutex::new(Instant::now()),
            ack_tx,
            apply_tx,
            io_mode,
            _visibility: visibility,
            apply_hook: Mutex::new(None),
        }
    }

    fn record_local_ack(&self, term: u64, index: u64) -> Result<u64, LedgerError> {
        let record = AckRecord {
            replica: self.id.clone(),
            term,
            index,
            segment_seq: index,
            io_mode: self.io_mode,
        };
        self.record_ack(record)
    }

    fn record_remote_ack(
        &self,
        replica: impl Into<ReplicaId>,
        term: u64,
        index: u64,
    ) -> Result<u64, LedgerError> {
        let record = AckRecord {
            replica: replica.into(),
            term,
            index,
            segment_seq: index,
            io_mode: self.io_mode,
        };
        self.record_ack(record)
    }

    fn record_ack(&self, record: AckRecord) -> Result<u64, LedgerError> {
        let update = self.ledger.lock().unwrap().record_ack(record)?;
        self.publish_commit(update.record.term, update.quorum_index);
        Ok(update.quorum_index)
    }

    fn publish_commit(&self, term: u64, index: u64) {
        if index == 0 {
            return;
        }
        let payload = self
            .log
            .lock()
            .ok()
            .and_then(|log| log.entry(index).ok().flatten().map(|e| e.payload));
        self.run_apply_hook(term, index, payload.clone());
        self.commit_index.store(index, Ordering::SeqCst);
        let _ = self.ack_tx.send(AckEvent { term, index });
        let _ = self.apply_tx.send(ApplyEvent {
            term,
            index,
            floor: index,
            payload,
        });
        if let Ok(mut core) = self.core.lock() {
            core.mark_proof_published(DurabilityProof::new(term, index));
        }
    }

    fn run_apply_hook(&self, term: u64, index: u64, payload: Option<Vec<u8>>) {
        let hook = self.apply_hook.lock().ok().and_then(|h| h.clone());
        if let Some(hook) = hook {
            let fut = (hook)(term, index, payload);
            if let Ok(handle) = Handle::try_current() {
                tokio::task::block_in_place(|| handle.block_on(fut));
            } else {
                futures::executor::block_on(fut);
            }
        }
    }

    fn latest_proof(&self) -> Option<DurabilityProof> {
        self.ledger
            .lock()
            .ok()
            .and_then(|ledger| ledger.latest_proof())
    }

    fn last_term_index(&self) -> (u64, u64) {
        let log = self.log.lock().unwrap();
        let last_index = log.last_index();
        let term = log
            .term_at(last_index)
            .unwrap_or(self.term.load(Ordering::SeqCst));
        (term, last_index)
    }
}

struct PeerClient {
    id: ReplicaId,
    client: AsyncRaftNetworkClient,
}

struct RaftServerHandle {
    _handle: AsyncRaftNetworkServerHandle,
}

type ApplyHook = Arc<
    dyn Fn(u64, u64, Option<Vec<u8>>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
>;

impl fmt::Debug for RaftReplica {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftReplica")
            .field("id", &self.shared.id_label)
            .field("clients", &self.clients.len())
            .finish()
    }
}

impl fmt::Debug for PeerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerClient")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for RaftServerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftServerHandle").finish_non_exhaustive()
    }
}

impl RaftReplica {
    /// Register an apply hook invoked before apply events are broadcast.
    pub fn set_apply_hook<F, Fut>(&self, hook: F)
    where
        F: Fn(u64, u64, Option<Vec<u8>>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let hook: ApplyHook =
            Arc::new(move |term, index, payload| Box::pin(hook(term, index, payload)));
        if let Ok(mut guard) = self.shared.apply_hook.lock() {
            *guard = Some(hook);
        }
    }

    /// Start a Raft replica with networking, ledger, WAL, and election timers.
    pub fn start(cfg: RaftReplicaConfig) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&cfg.log_dir)
            .with_context(|| format!("create raft log dir {}", cfg.log_dir.display()))?;
        let log_path = cfg.log_dir.join("raft.log");
        let log = RaftLogStore::open(&log_path).context("open raft log")?;

        let mut ledger = DurabilityLedger::new(PartitionQuorumConfig::new(cfg.quorum.max(1)));
        let local_id = ReplicaId::new(cfg.local_id.clone());
        ledger.register_replica(local_id.clone());
        for (peer_id, _) in &cfg.peers {
            ledger.register_replica(ReplicaId::new(peer_id.clone()));
        }
        let core = ConsensusCore::new(ConsensusCoreConfig::for_profile(PartitionProfile::Latency));
        let routing = RaftRouting::alias(
            format!("{}:{}", cfg.prg.tenant_id, cfg.prg.partition_index),
            cfg.routing_epoch.max(1),
        );

        let shared = Arc::new(ReplicaShared::new(
            local_id.clone(),
            ledger,
            cfg.local_id.clone(),
            core,
            log,
            routing.clone(),
            cfg.commit_visibility.clone(),
            cfg.durability_mode.clone(),
        ));

        let network = ReplicaNetwork::start(&cfg, &routing, shared.clone())?;

        let callbacks = Arc::new(ReplicaCallbacks::new(
            shared.clone(),
            network.clients.clone(),
        ));
        let timers = if Handle::try_current().is_ok() {
            let controller = ElectionController::for_partition_profile(
                PartitionProfile::Latency,
                DEFAULT_ELECTION_SEED,
            );
            let heartbeat_interval = controller.heartbeat_interval().max(HEARTBEAT_BACKOFF);
            Some(
                RaftNodeScaffold::new(
                    callbacks,
                    controller,
                    heartbeat_interval,
                    cfg.local_id.clone(),
                )
                .spawn(),
            )
        } else {
            None
        };

        Ok(Arc::new(Self {
            shared,
            clients: network.clients,
            _server: network.server,
            _timers: timers,
        }))
    }

    /// Propose a log entry at a fixed index; used when caller manages indexing.
    pub async fn propose_index(&self, idx: u64, payload: Vec<u8>) -> Result<DurabilityProof> {
        let term = self.shared.term.load(Ordering::SeqCst);
        self.propose_internal(term, idx, payload).await
    }

    /// Append a Raft log entry and replicate to peers; returns the committed proof.
    pub async fn propose(&self, payload: Vec<u8>) -> Result<DurabilityProof> {
        let term = self.shared.term.load(Ordering::SeqCst);
        let next_index = {
            let log = self.shared.log.lock().unwrap();
            log.last_index().saturating_add(1)
        };
        self.propose_internal(term, next_index, payload).await
    }

    async fn propose_internal(
        &self,
        term: u64,
        idx: u64,
        payload: Vec<u8>,
    ) -> Result<DurabilityProof> {
        let (prev_term, prev_index) = self.shared.last_term_index();
        {
            let mut log = self.shared.log.lock().unwrap();
            if idx != log.last_index().saturating_add(1) {
                anyhow::bail!(
                    "non-sequential proposal idx={} expected={}",
                    idx,
                    log.last_index().saturating_add(1)
                );
            }
            let entry = RaftLogEntry::new(term, idx, payload);
            log.append(entry.clone())
                .map_err(|e| anyhow!("append entry failed: {e}"))?;
        }
        self.shared
            .record_local_ack(term, idx)
            .map_err(|e| anyhow!("local ack failed: {e}"))?;
        self.send_append_entries(prev_index, prev_term, term).await;
        Ok(self.shared.latest_proof().unwrap_or_else(|| {
            DurabilityProof::new(term, self.shared.commit_index.load(Ordering::SeqCst))
        }))
    }

    async fn send_append_entries(&self, prev_index: u64, prev_term: u64, term: u64) {
        let leader_commit = self.shared.commit_index.load(Ordering::SeqCst);
        let entries = {
            let log = self.shared.log.lock().unwrap();
            let next = log.entry(prev_index.saturating_add(1)).ok().flatten();
            next.into_iter().collect::<Vec<_>>()
        };
        let request = AppendEntriesRequest {
            term,
            leader_id: self.shared.id_label.clone(),
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            leader_commit,
            entries,
            routing: self.shared.routing.clone(),
        };
        for peer in self.clients.iter() {
            if peer.id == self.shared.id {
                continue;
            }
            match peer
                .client
                .append_entries(request.clone(), Instant::now())
                .await
            {
                Ok(AppendEntriesResponse {
                    success: true,
                    match_index,
                    term: peer_term,
                    ..
                }) => {
                    let _ = self.shared.record_remote_ack(
                        peer.id.clone(),
                        peer_term.max(term),
                        match_index.max(leader_commit),
                    );
                }
                Ok(resp) => {
                    tracing::warn!(
                        "append_entries to {:?} failed success={} term={}",
                        peer.id,
                        resp.success,
                        resp.term
                    );
                }
                Err(err) => {
                    tracing::warn!("append_entries to {:?} error: {err:?}", peer.id);
                }
            }
        }
    }

    pub fn ack_stream(&self) -> broadcast::Receiver<AckEvent> {
        self.shared.ack_tx.subscribe()
    }

    pub fn apply_stream(&self) -> broadcast::Receiver<ApplyEvent> {
        self.shared.apply_tx.subscribe()
    }

    pub fn latest_proof(&self) -> Option<DurabilityProof> {
        self.shared.latest_proof()
    }

    pub fn committed_index(&self) -> u64 {
        self.shared.commit_index.load(Ordering::SeqCst)
    }

    pub fn status(&self) -> RaftReplicaStatus {
        let pending = self
            .shared
            .ledger
            .lock()
            .map(|l| l.pending_entries())
            .unwrap_or(0);
        RaftReplicaStatus {
            commit_index: self.committed_index(),
            pending_entries: pending,
        }
    }

    pub fn has_leader(&self) -> bool {
        self.shared.is_leader.load(Ordering::SeqCst)
    }

    /// Wait for apply/commit index to reach target.
    pub async fn wait_for_apply(&self, target: u64) -> Result<()> {
        if target == 0 {
            return Ok(());
        }
        let mut rx = self.apply_stream();
        loop {
            if self.committed_index() >= target {
                return Ok(());
            }
            match rx.recv().await {
                Ok(evt) if evt.index >= target => return Ok(()),
                Ok(_) => {}
                Err(_) => anyhow::bail!("apply stream closed"),
            }
        }
    }

    /// Seed commit/term from recovered proof without emitting network traffic.
    pub fn seed_commit(&self, term: u64, index: u64) {
        self.shared.term.store(term, Ordering::SeqCst);
        self.shared.commit_index.store(index, Ordering::SeqCst);
        self.shared.publish_commit(term, index);
    }

    /// Demote local leadership and flush to current commit index.
    pub async fn step_down(&self) -> Result<()> {
        self.shared.is_leader.store(false, Ordering::SeqCst);
        let idx = self.committed_index();
        self.wait_for_apply(idx).await
    }
}

#[derive(Debug)]
struct ReplicaNetwork {
    server: Option<RaftServerHandle>,
    clients: Arc<Vec<PeerClient>>,
}

impl ReplicaNetwork {
    fn start(
        cfg: &RaftReplicaConfig,
        routing: &RaftRouting,
        shared: Arc<ReplicaShared>,
    ) -> Result<Self> {
        let now = Instant::now();
        let identity = load_identity_from_pem(&cfg.tls_chain, &cfg.tls_key, now)
            .context("load raft tls identity")?;
        let trust_store =
            load_trust_store_from_pem(&cfg.trust_bundle).context("load raft trust bundle")?;
        let mut server_mtls = MtlsIdentityManager::new(
            identity.certificate.clone(),
            &cfg.trust_domain,
            std::time::Duration::from_secs(600),
            now,
        );
        let _ = server_mtls.rotate(now);

        let bind_addr = resolve_bind(cfg, &cfg.local_id);
        let server = if let Some(bind) = bind_addr {
            let server = RaftRpcServer::new(
                server_mtls,
                ReplicaRpcHandler::new(shared.clone()),
                routing.clone(),
            );
            let spawn = AsyncRaftNetworkServer::spawn(
                AsyncRaftTransportServerConfig {
                    bind,
                    identity: identity.clone(),
                    trust_store: trust_store.clone(),
                },
                server,
            );
            let handle = if let Ok(handle) = Handle::try_current() {
                tokio::task::block_in_place(|| handle.block_on(spawn))
                    .context("spawn raft server")?
            } else {
                tokio::runtime::Runtime::new()
                    .context("create runtime for raft server spawn")?
                    .block_on(spawn)
                    .context("spawn raft server")?
            };
            Some(RaftServerHandle { _handle: handle })
        } else {
            tracing::warn!(
                "raft replica {:?} has no resolvable bind address; server not started",
                shared.id
            );
            None
        };

        let mut clients = Vec::new();
        for (peer_id, peer_addr) in &cfg.peers {
            let peer_id = ReplicaId::new(peer_id.clone());
            if peer_id == shared.id {
                continue;
            }
            let Some(addr) = peer_addr
                .to_socket_addrs()
                .ok()
                .and_then(|mut it| it.next())
            else {
                tracing::warn!("peer address {} could not be resolved", peer_addr);
                continue;
            };
            let client_mtls = Arc::new(ParkingMutex::new(MtlsIdentityManager::new(
                identity.certificate.clone(),
                &cfg.trust_domain,
                std::time::Duration::from_secs(600),
                now,
            )));
            {
                let mut guard = client_mtls.lock();
                let _ = guard.rotate(now);
            }
            let cfg = AsyncRaftTransportClientConfig {
                host: addr.ip().to_string(),
                port: addr.port(),
                identity: identity.clone(),
                trust_store: trust_store.clone(),
                mtls: client_mtls,
            };
            let options = AsyncRaftTransportClientOptions::default()
                // Tag the peer node to allow transport pooling/multiplexing across PRGs.
                .peer_node_id(peer_addr.clone());
            match AsyncRaftNetworkClient::with_options(cfg, options) {
                Ok(client) => clients.push(PeerClient {
                    id: peer_id,
                    client,
                }),
                Err(err) => {
                    tracing::warn!("failed to create raft client to {}: {err:?}", peer_addr)
                }
            }
        }
        Ok(Self {
            server,
            clients: Arc::new(clients),
        })
    }
}

struct ReplicaCallbacks {
    shared: Arc<ReplicaShared>,
    clients: Arc<Vec<PeerClient>>,
}

impl ReplicaCallbacks {
    fn new(shared: Arc<ReplicaShared>, clients: Arc<Vec<PeerClient>>) -> Self {
        let now = Instant::now();
        if let Ok(mut deadline) = shared.election_deadline.lock() {
            *deadline = now + HEARTBEAT_BACKOFF;
        }
        Self { shared, clients }
    }

    async fn heartbeat(&self) {
        let term = self.shared.term.load(Ordering::SeqCst);
        let (prev_term, prev_index) = self.shared.last_term_index();
        let leader_commit = self.shared.commit_index.load(Ordering::SeqCst);
        let request = AppendEntriesRequest {
            term,
            leader_id: self.shared.id_label.clone(),
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            leader_commit,
            entries: Vec::new(),
            routing: self.shared.routing.clone(),
        };
        for peer in self.clients.iter() {
            if peer.id == self.shared.id {
                continue;
            }
            let _ = peer
                .client
                .append_entries(request.clone(), Instant::now())
                .await;
        }
    }
}

impl RaftNodeCallbacks for ReplicaCallbacks {
    fn on_leader_heartbeat(&self) -> PinFuture<()> {
        let this = self.clone();
        Box::pin(async move {
            this.heartbeat().await;
        })
    }

    fn on_start_election(&self) -> PinFuture<()> {
        let this = self.clone();
        Box::pin(async move {
            this.shared.is_leader.store(true, Ordering::SeqCst);
            this.shared.term.fetch_add(1, Ordering::SeqCst);
            this.heartbeat().await;
        })
    }

    fn is_leader(&self) -> bool {
        self.shared.is_leader.load(Ordering::SeqCst)
    }

    fn schedule_deadline(&self, now: Instant, timeout: Duration) {
        if let Ok(mut deadline) = self.shared.election_deadline.lock() {
            *deadline = now + timeout;
        }
    }

    fn election_deadline(&self) -> Instant {
        *self.shared.election_deadline.lock().unwrap()
    }
}

impl Clone for ReplicaCallbacks {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            clients: self.clients.clone(),
        }
    }
}

struct ReplicaRpcHandler {
    shared: Arc<ReplicaShared>,
}

impl ReplicaRpcHandler {
    fn new(shared: Arc<ReplicaShared>) -> Self {
        Self { shared }
    }

    fn append_entries(&self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        let mut log = self.shared.log.lock().unwrap();
        let current_term = self.shared.term.load(Ordering::SeqCst);
        if request.term > current_term {
            self.shared.term.store(request.term, Ordering::SeqCst);
            self.shared.is_leader.store(false, Ordering::SeqCst);
        }
        let last_index = log.last_index();
        if request.prev_log_index > last_index {
            return AppendEntriesResponse {
                term: request.term.max(current_term),
                success: false,
                match_index: last_index,
                conflict_index: Some(last_index.saturating_add(1)),
                conflict_term: None,
            };
        }
        if request.prev_log_index > 0 {
            if let Some(term) = log.term_at(request.prev_log_index) {
                if term != request.prev_log_term {
                    return AppendEntriesResponse {
                        term: request.term.max(current_term),
                        success: false,
                        match_index: last_index,
                        conflict_index: Some(request.prev_log_index),
                        conflict_term: Some(term),
                    };
                }
            }
        }
        if let Some(first) = request.entries.first() {
            if first.index <= log.last_index() {
                let _ = log.truncate_from(first.index);
            }
        }
        let mut appended = last_index;
        for entry in &request.entries {
            match log.append(entry.clone()) {
                Ok(_) => appended = entry.index,
                Err(err) => {
                    tracing::warn!(
                        "append_entries append failure at {}: {:?}",
                        entry.index,
                        err
                    );
                    return AppendEntriesResponse {
                        term: request.term.max(current_term),
                        success: false,
                        match_index: appended,
                        conflict_index: Some(entry.index),
                        conflict_term: None,
                    };
                }
            }
        }
        drop(log);
        let ack_index = appended.max(request.leader_commit);
        let _ = self.shared.record_remote_ack(
            ReplicaId::new(request.leader_id.clone()),
            request.term.max(current_term),
            ack_index,
        );
        AppendEntriesResponse {
            term: request.term.max(current_term),
            success: true,
            match_index: ack_index,
            conflict_index: None,
            conflict_term: None,
        }
    }
}

impl RaftRpcHandler for ReplicaRpcHandler {
    fn on_request_vote(&mut self, request: RequestVoteRequest) -> RequestVoteResponse {
        let current_term = self.shared.term.load(Ordering::SeqCst);
        if request.term < current_term {
            return RequestVoteResponse {
                term: current_term,
                granted: false,
                reject_reason: Some(RequestVoteRejectReason::TermOutOfDate),
            };
        }
        self.shared.term.store(request.term, Ordering::SeqCst);
        self.shared.is_leader.store(false, Ordering::SeqCst);
        RequestVoteResponse {
            term: request.term,
            granted: true,
            reject_reason: None,
        }
    }

    fn on_append_entries(&mut self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        self.append_entries(request)
    }
}

fn resolve_bind(cfg: &RaftReplicaConfig, local_id: &str) -> Option<std::net::SocketAddr> {
    let mut candidates = Vec::new();
    for (peer_id, addr) in &cfg.peers {
        if peer_id == local_id {
            candidates.push(addr.clone());
        }
    }
    for candidate in &candidates {
        if let Ok(mut iter) = candidate.to_socket_addrs() {
            if let Some(addr) = iter.next() {
                return Some(addr);
            }
        }
    }
    if let Some(bind) = cfg.bind_override.clone() {
        if let Ok(mut iter) = bind.to_socket_addrs() {
            if let Some(addr) = iter.next() {
                tracing::warn!(
                    "raft bind address falling back to override {} because CP address was not resolvable for local id {}",
                    bind,
                    local_id
                );
                return Some(addr);
            }
        }
    }
    None
}
