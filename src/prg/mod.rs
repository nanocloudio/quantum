//! Partition Replica Group (PRG) management.
//!
//! This module provides PRG lifecycle and workload abstraction:
//! - Core PRG state machine and manager
//! - `workload` - Workload trait and helpers
//! - `workload_rpc` - Remote workload RPC client
//!
//! The file is large (~2400 lines) because PRG management is the
//! central coordination point for all replica group operations:
//! host registration, apply loops, forwarding, snapshotting, and
//! state machine transitions. Splitting would require threading
//! complex state across many boundaries.

pub mod workload;
pub mod workload_rpc;

pub use workload::*;
pub use workload_rpc::*;

use crate::config::{DurabilityConfig, ForwardPlaneConfig};
use crate::control::{ControlPlaneClient, ControlPlaneError};
use crate::dedupe::PersistedDedupeEntry;
use crate::flow::{BackpressureState, QueueDepthKind};
use crate::forwarding::{ForwardReceipt, ForwardRequest, ForwardingEngine};
use crate::offline::OfflineEntry;
use crate::ops::VersionGate;
use crate::replication::consensus::{AckContract, PersistSnapshotOp, RaftOp, WorkloadApplyOp};
use crate::replication::network::RaftReplica;
use crate::routing::{
    ensure_routing_epoch, ForwardSeqEntry, PrgId, PrgPlacement, RoutingEpoch, RoutingTable,
};
use crate::storage::compaction::ProductFloors;
use crate::storage::ClustorStorage;
use crate::time::Clock;
use crate::workloads::mqtt::session::{PersistedInflight, PersistedOutbound};
use crate::workloads::mqtt::{ForwardPublishPayload, MqttApply, MqttWorkload};
use anyhow::{anyhow, Result};
use clustor::consensus::DurabilityProof;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::{Builder as TokioRuntimeBuilder, Handle};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;

type SharedPrgState = Arc<Mutex<PersistedPrgState>>;
type DynWorkload = dyn PrgWorkload<Snapshot = PersistedPrgState, LogEntry = MqttApply>;
type SharedWorkload = Arc<Mutex<Box<DynWorkload>>>;

const FORWARD_INTENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct ForwardIntentMsg {
    intents: Vec<ForwardIntent>,
}

type ForwardWaiter = oneshot::Sender<Result<ForwardReceipt, ForwardError>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ForwardIntentKey {
    session_prg: PrgId,
    topic_prg: PrgId,
    routing_epoch: u64,
    seq: u64,
}

impl ForwardIntentKey {
    fn new(req: &ForwardRequest) -> Self {
        Self {
            session_prg: req.session_prg.clone(),
            topic_prg: req.topic_prg.clone(),
            routing_epoch: req.routing_epoch.0,
            seq: req.seq,
        }
    }
}

#[derive(Debug)]
struct CommitProgress {
    last_index: u64,
    last_seen: Instant,
}

#[derive(Debug)]
pub enum ForwardError {
    Control(ControlPlaneError),
    Routing(crate::routing::RoutingError),
}

impl From<ControlPlaneError> for ForwardError {
    fn from(err: ControlPlaneError) -> Self {
        ForwardError::Control(err)
    }
}

impl From<crate::routing::RoutingError> for ForwardError {
    fn from(err: crate::routing::RoutingError) -> Self {
        ForwardError::Routing(err)
    }
}

/// Manages PRG lifecycle and placement; clustor-backed wiring to follow.
#[derive(Clone)]
pub struct PrgManager<C: Clock> {
    routing: Arc<RwLock<RoutingTable>>,
    clock: C,
    control_plane: ControlPlaneClient<C>,
    ack_contract: Arc<AckContract>,
    durability_cfg: DurabilityConfig,
    raft_net: Option<crate::raft_net::RaftNetResources>,
    metrics: Arc<BackpressureState>,
    ready: Arc<AtomicBool>,
    version_gate: VersionGate,
    effective_floors: Arc<Mutex<HashMap<PrgId, u64>>>,
    persisted: Arc<Mutex<HashMap<PrgId, SharedPrgState>>>,
    storage_backend: ClustorStorage,
    forwarding: ForwardingEngine,
    forward_intent_tx: UnboundedSender<ForwardIntentMsg>,
    forward_intent_rx: Arc<Mutex<Option<UnboundedReceiver<ForwardIntentMsg>>>>,
    pending_forwards: Arc<Mutex<HashMap<ForwardIntentKey, ForwardWaiter>>>,
    forward_response_timeout: Duration,
    remote_workload: RemoteWorkloadClient,
    schema_registry: SchemaRegistryHandle,
    workload_features: WorkloadFeatures,
    active_hosts: Arc<Mutex<HashMap<PrgId, String>>>,
    hosts: Arc<Mutex<HashMap<PrgId, Arc<PrgHost<C>>>>>,
    cross_prg_total: Arc<AtomicU64>,
    cross_prg_cross: Arc<AtomicU64>,
    commit_progress: Arc<Mutex<CommitProgress>>,
}

/// Aggregated inputs to reduce constructor argument count.
#[derive(Clone)]
pub struct PrgManagerInputs<C: Clock> {
    pub control_plane: ControlPlaneClient<C>,
    pub ack_contract: Arc<AckContract>,
    pub durability_cfg: crate::config::DurabilityConfig,
    pub raft_net: Option<crate::raft_net::RaftNetResources>,
    pub metrics: Arc<BackpressureState>,
    pub version_gate: VersionGate,
    pub storage_backend: ClustorStorage,
    pub forward_plane: ForwardPlaneConfig,
}

impl<C: Clock> PrgManager<C> {
    pub fn new(routing: RoutingTable, clock: C, inputs: PrgManagerInputs<C>) -> Self {
        let now = clock.now();
        let forwarding = ForwardingEngine::new(
            (*inputs.ack_contract.clone()).clone(),
            inputs.metrics.clone(),
            Duration::from_secs(600),
        );
        let remote_workload = RemoteWorkloadClient::from_config(&inputs.forward_plane);
        let schema_registry = SchemaRegistryHandle::default();
        schema_registry.register_workload(MqttWorkload::descriptor_static());
        let workload_features = WorkloadFeatures::default();
        let (intent_tx, intent_rx) = unbounded_channel();
        let manager = Self {
            routing: Arc::new(RwLock::new(routing)),
            clock,
            control_plane: inputs.control_plane.clone(),
            ack_contract: inputs.ack_contract.clone(),
            durability_cfg: inputs.durability_cfg.clone(),
            raft_net: inputs.raft_net.clone(),
            metrics: inputs.metrics.clone(),
            ready: Arc::new(AtomicBool::new(false)),
            version_gate: inputs.version_gate,
            effective_floors: Arc::new(Mutex::new(HashMap::new())),
            persisted: Arc::new(Mutex::new(HashMap::new())),
            storage_backend: inputs.storage_backend.clone(),
            forwarding,
            forward_intent_tx: intent_tx,
            forward_intent_rx: Arc::new(Mutex::new(Some(intent_rx))),
            pending_forwards: Arc::new(Mutex::new(HashMap::new())),
            forward_response_timeout: FORWARD_INTENT_TIMEOUT,
            remote_workload,
            schema_registry,
            workload_features,
            active_hosts: Arc::new(Mutex::new(HashMap::new())),
            hosts: Arc::new(Mutex::new(HashMap::new())),
            cross_prg_total: Arc::new(AtomicU64::new(0)),
            cross_prg_cross: Arc::new(AtomicU64::new(0)),
            commit_progress: Arc::new(Mutex::new(CommitProgress {
                last_index: 0,
                last_seen: now,
            })),
        };
        manager.spawn_forward_intent_worker();
        manager
    }

    /// Placeholder start hook; will spin up PRG hosts based on routing table.
    pub async fn start(&self) -> Result<()> {
        let epoch = self.refresh_routing_table().await?;
        let _started_at = self.clock.now();
        tracing::info!(
            "prg manager starting with epoch {} ({} placements)",
            epoch.0,
            self.routing.read().await.placements.len()
        );
        self.spawn_cp_watch();
        self.spawn_apply_watch();
        self.spawn_forward_intent_worker();
        Ok(())
    }

    fn spawn_cp_watch(&self) {
        if Handle::try_current().is_err() {
            return;
        }
        let mut rx = self.control_plane.routing_updates();
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                if let Err(err) = this.refresh_routing_table().await {
                    tracing::warn!("routing refresh from cp watch failed: {err:?}");
                }
            }
        });
    }

    fn spawn_apply_watch(&self) {
        if Handle::try_current().is_err() {
            return;
        }
        let mut rx = self.ack_contract.apply_stream();
        let cp = self.control_plane.clone();
        let visibility = self.ack_contract.cp_commit_visibility();
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(evt) => {
                        cp.ingest_apply(evt.term, evt.index, visibility);
                        this.publish_latest_proof();
                        this.on_apply(evt).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        });
    }

    fn spawn_forward_intent_worker(&self) {
        let rx_guard = self.forward_intent_rx.clone();
        let manager = self.clone();
        if Handle::try_current().is_ok() {
            tokio::spawn(async move {
                let mut rx_opt = rx_guard.lock().await;
                let mut rx = match rx_opt.take() {
                    Some(receiver) => receiver,
                    None => return,
                };
                drop(rx_opt);
                while let Some(msg) = rx.recv().await {
                    manager.handle_forward_intents(msg).await;
                }
            });
        } else {
            thread::spawn(move || {
                let runtime = TokioRuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("forward intent runtime");
                runtime.block_on(async move {
                    let mut rx_opt = rx_guard.lock().await;
                    let mut rx = match rx_opt.take() {
                        Some(receiver) => receiver,
                        None => return,
                    };
                    drop(rx_opt);
                    while let Some(msg) = rx.recv().await {
                        manager.handle_forward_intents(msg).await;
                    }
                });
            });
        }
    }

    async fn handle_forward_intents(&self, msg: ForwardIntentMsg) {
        for intent in msg.intents {
            let key = ForwardIntentKey::new(&intent.request);
            let result = self.dispatch_forward_intent(intent).await;
            self.finish_pending_forward(&key, result).await;
        }
    }

    async fn finish_pending_forward(
        &self,
        key: &ForwardIntentKey,
        result: Result<ForwardReceipt, ForwardError>,
    ) {
        let waiter = self.pending_forwards.lock().await.remove(key);
        match waiter {
            Some(tx) => {
                let _ = tx.send(result);
            }
            None => {
                if let Err(err) = &result {
                    tracing::warn!("forward intent {:?} failed without waiter: {err:?}", key);
                }
            }
        }
    }

    async fn dispatch_forward_intent(
        &self,
        intent: ForwardIntent,
    ) -> Result<ForwardReceipt, ForwardError> {
        let payload = self.decode_forward_payload(&intent)?;
        let is_cross = intent.request.session_prg != intent.request.topic_prg;
        let mut target_local = self.has_local_host(&intent.request.topic_prg).await;
        if !target_local {
            target_local = self
                .ensure_local_host(&intent.request.topic_prg)
                .await
                .is_some();
        }
        if target_local {
            let manager = self.clone();
            let prg_target = intent.request.topic_prg.clone();
            let metrics = self.metrics.clone();
            let cross = is_cross;
            let topic_owned = payload.topic.clone();
            let body_owned = payload.payload.clone();
            let qos = payload.qos;
            let retain = payload.retain;
            self.forwarding
                .forward_and_commit(intent.request, move || {
                    let mgr = manager.clone();
                    let prg = prg_target.clone();
                    let topic = topic_owned.clone();
                    let body = body_owned.clone();
                    let metrics = metrics.clone();
                    async move {
                        if let Err(err) = mgr.commit_publish(&prg, &topic, &body, qos, retain).await
                        {
                            if cross {
                                metrics.record_forward_failure();
                            }
                            tracing::warn!(
                                "forwarded publish dropped due to stale epoch {:?}: {err:?}",
                                prg
                            );
                        }
                    }
                })
                .await
                .map_err(ForwardError::Routing)
        } else {
            let entry = payload.into_entry();
            let request = intent.request;
            let target = request.topic_prg.clone();
            self.forward_remote_publish(request, target, entry, is_cross)
                .await
        }
    }

    fn decode_forward_payload(
        &self,
        intent: &ForwardIntent,
    ) -> Result<ForwardPublishPayload, ForwardError> {
        match intent.workload {
            "mqtt" => ForwardPublishPayload::from_envelope(&intent.envelope).map_err(|err| {
                tracing::warn!(
                    "failed to decode MQTT forward payload for {:?}: {err:?}",
                    intent.request.topic_prg
                );
                ForwardError::Control(ControlPlaneError::StrictFallback)
            }),
            other => {
                tracing::warn!("unsupported workload {} for forward intent", other);
                Err(ForwardError::Control(ControlPlaneError::StrictFallback))
            }
        }
    }

    /// Refresh routing table from control-plane snapshot.
    pub async fn refresh_routing_table(&self) -> Result<RoutingEpoch> {
        let mut snapshot = self.control_plane.routing_snapshot();
        self.enforce_version_skew(&snapshot)?;
        let mut guard = self.routing.write().await;
        let previous = guard.clone();
        if self.control_plane.is_embedded()
            && snapshot.placements.is_empty()
            && !previous.placements.is_empty()
        {
            tracing::warn!(
                "embedded control-plane returned no placements; retaining previous snapshot (epoch {})",
                previous.epoch.0
            );
            snapshot = previous.clone();
        }
        if snapshot == previous {
            return Ok(previous.epoch);
        }
        *guard = snapshot.clone();
        drop(guard);
        self.reconcile_hosts(&previous, &snapshot).await?;
        for prg in snapshot.placements.keys() {
            if let Err(err) = self
                .control_plane
                .validate_placement_epoch(prg, snapshot.epoch)
            {
                tracing::warn!(
                    "placement epoch validation failed for {:?} at epoch {}: {:?}",
                    prg,
                    snapshot.epoch.0,
                    err
                );
            }
        }
        self.control_plane.update_routing_epoch(snapshot.epoch);
        self.ack_contract
            .reconfigure_from_placements(&snapshot.placements);
        tracing::info!(
            "routing table refreshed to epoch {} placements={}",
            snapshot.epoch.0,
            snapshot.placements.len()
        );
        self.update_ready_state(!snapshot.placements.is_empty())
            .await;
        self.hydrate_hosts_from_storage().await?;
        Ok(snapshot.epoch)
    }

    async fn update_ready_state(&self, has_placements: bool) {
        let cache_fresh = self.control_plane.cache_is_fresh();
        let strict_fallback = self.control_plane.strict_fallback();
        let mut fence_active = self.ack_contract.durability_fence_active();
        if self.control_plane.is_embedded() && has_placements {
            fence_active = false;
            self.ack_contract.set_durability_fence(false);
            self.control_plane.set_strict_fallback(false);
        }
        let read_gate_ok = self.control_plane.guard_read_gate(true).is_ok();
        let (leader_ok, leaderless) = self.raft_leader_health().await;
        self.metrics.record_raft_leaderless(leaderless);
        let commit_ok = self.commit_advancing(self.readiness_commit_timeout()).await;
        let ready = cache_fresh
            && !strict_fallback
            && has_placements
            && read_gate_ok
            && leader_ok
            && commit_ok
            && !fence_active;
        self.ready.store(ready, Ordering::Relaxed);
        let fence_active = !cache_fresh || strict_fallback || !has_placements || !read_gate_ok;
        self.ack_contract.set_durability_fence(fence_active);
        self.metrics.set_durability_fence(fence_active);
        self.set_host_fences(fence_active).await;
        self.control_plane.ingest_read_gate(!fence_active);
    }

    async fn reconcile_hosts(&self, old: &RoutingTable, new: &RoutingTable) -> Result<()> {
        let local_id = self.durability_cfg.replica_id.clone();
        let cp_configured = self.control_plane.configured();
        let rebalance_grace = self.control_plane.rebalance_grace_active();
        let mut departing: Vec<(PrgId, Arc<PrgHost<C>>)> = Vec::new();
        {
            let hosts = self.hosts.lock().await;
            for prg in old.placements.keys() {
                let remove = match new.placements.get(prg) {
                    None => true,
                    Some(placement) => {
                        let is_local = placement.node_id == local_id || !cp_configured;
                        !is_local
                    }
                };
                if remove {
                    if let Some(host) = hosts.get(prg) {
                        departing.push((prg.clone(), host.clone()));
                    }
                }
            }
        }
        for (prg, host) in &departing {
            host.set_fence(true);
            if let Err(err) = host.checkpoint_and_step_down().await {
                tracing::warn!(
                    "failed to flush host {:?} before leaving placement: {err:?}",
                    prg
                );
            }
            if let Err(err) = host.ack().step_down_for_upgrade().await {
                tracing::warn!("failed to hand off leadership for {:?}: {err:?}", prg);
            }
            if rebalance_grace && !new.placements.contains_key(prg) {
                tracing::info!("rebalance grace active while leaving {:?}", prg);
            }
        }
        {
            let mut active_hosts = self.active_hosts.lock().await;
            let mut hosts = self.hosts.lock().await;
            for (prg, _) in &departing {
                active_hosts.remove(prg);
                hosts.remove(prg);
            }
        }
        {
            let mut floors = self.effective_floors.lock().await;
            for (prg, _) in &departing {
                floors.remove(prg);
            }
        }
        let mut pending_start: Vec<(PrgId, Arc<PrgHost<C>>)> = Vec::new();
        {
            let mut active_hosts = self.active_hosts.lock().await;
            let mut hosts = self.hosts.lock().await;
            for (prg, placement) in &new.placements {
                let is_local = placement.node_id == local_id || !cp_configured;
                if !is_local {
                    continue;
                }
                active_hosts
                    .entry(prg.clone())
                    .or_insert_with(|| placement.node_id.clone());
                if let Some(host) = hosts.get(prg) {
                    host.update_placement(placement.clone());
                    continue;
                }
                let state = self.ensure_state_handle(prg).await;
                let use_raft = self.placement_requires_raft(placement);
                let ack = if use_raft {
                    Arc::new(AckContract::new(&self.durability_cfg))
                } else {
                    self.ack_contract.clone()
                };
                let raft = if use_raft {
                    self.raft_net
                        .as_ref()
                        .and_then(|net| net.start_replica(prg, placement, ack.clone(), new.epoch.0))
                } else {
                    None
                };
                let workload = {
                    let snapshot = state.lock().await.clone();
                    Arc::new(Mutex::new(
                        Box::new(MqttWorkload::new(snapshot)) as Box<DynWorkload>
                    ))
                };
                let host_ctx = PrgHostContext {
                    storage: self.storage_backend.clone(),
                    ack: ack.clone(),
                    raft,
                    metrics: self.metrics.clone(),
                    clock: self.clock.clone(),
                    schema_registry: self.schema_registry.clone(),
                    features: self.workload_features.clone(),
                    forwarding: self.forwarding.clone(),
                    forward_intent_tx: self.forward_intent_tx.clone(),
                    force_inline_apply: false,
                };
                let host = Arc::new(PrgHost::new(
                    prg.clone(),
                    placement.clone(),
                    state,
                    workload,
                    host_ctx,
                ));
                self.spawn_host_apply_watch(prg.clone(), ack.clone());
                host.install_apply_hook();
                hosts.insert(prg.clone(), host.clone());
                pending_start.push((prg.clone(), host));
            }
        }
        for (prg, host) in pending_start {
            if let Err(err) = host.start().await {
                tracing::warn!("failed to start PRG host {:?}: {err:?}", prg);
            }
        }
        self.refresh_lag_metrics().await;
        Ok(())
    }

    fn spawn_host_apply_watch(&self, prg: PrgId, ack: Arc<AckContract>) {
        let cp = self.control_plane.clone();
        let visibility = ack.cp_commit_visibility();
        let manager = self.clone();
        let prg_id = prg;
        let fut = async move {
            let mut rx = ack.apply_stream();
            loop {
                match rx.recv().await {
                    Ok(evt) => {
                        if let Some(host) = manager.host_for(&prg_id).await {
                            if !host.uses_raft() && !host.force_inline_apply() {
                                if let Err(err) = host
                                    .apply_payload(evt.term, evt.index, evt.payload.clone())
                                    .await
                                {
                                    tracing::warn!(
                                        "apply handling failed for {:?} at {}: {err:?}",
                                        prg_id,
                                        evt.index
                                    );
                                }
                            }
                        }
                        cp.ingest_apply(evt.term, evt.index, visibility);
                        cp.publish_proof(clustor::replication::consensus::DurabilityProof::new(
                            evt.term, evt.index,
                        ));
                        manager
                            .on_host_apply(prg_id.clone(), ack.clone(), evt)
                            .await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        };
        if Handle::try_current().is_ok() {
            tokio::spawn(fut);
        } else {
            thread::spawn(move || {
                let runtime = TokioRuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("host apply runtime");
                runtime.block_on(fut);
            });
        }
    }

    fn enforce_version_skew(&self, snapshot: &RoutingTable) -> Result<()> {
        let cp = self.control_plane.snapshot();
        for placement in snapshot.placements.values() {
            for node in std::iter::once(&placement.node_id).chain(placement.replicas.iter()) {
                if let Some(peer_minor) = cp.peer_versions.get(node) {
                    if !self.version_gate.compatible(*peer_minor) {
                        return Err(anyhow!(
                            "peer {} minor {} incompatible with local {}",
                            node,
                            peer_minor,
                            self.version_gate.current_minor()
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    async fn guard_read_gate(&self) -> Result<(), ControlPlaneError> {
        if self.control_plane.strict_fallback() {
            self.fence_from_control_plane().await;
            return Err(ControlPlaneError::StrictFallback);
        }
        if !self.control_plane.configured() {
            return Ok(());
        }
        let read_gate_ok = !self.ack_contract.durability_fence_active();
        match self.control_plane.guard_read_gate(read_gate_ok) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.fence_from_control_plane().await;
                Err(err)
            }
        }
    }

    async fn fence_from_control_plane(&self) {
        self.ready.store(false, Ordering::Relaxed);
        self.ack_contract.set_durability_fence(true);
        self.set_host_fences(true).await;
    }

    /// Verify routing epoch before serving a request.
    pub async fn check_epoch(
        &self,
        expected: RoutingEpoch,
    ) -> Result<(), crate::routing::RoutingError> {
        let observed = self.control_plane.routing_epoch();
        match ensure_routing_epoch(observed, expected) {
            Ok(_) => Ok(()),
            Err(err) => {
                let refreshed = self.refresh_routing_table().await.ok();
                if let Some(epoch) = refreshed {
                    if self.control_plane.rebalance_grace_active()
                        && expected.0.saturating_sub(epoch.0) <= 1
                    {
                        tracing::warn!(
                            "rebalance grace active; tolerating epoch drift local={} expected={}",
                            epoch.0,
                            expected.0
                        );
                        return Ok(());
                    }
                    ensure_routing_epoch(epoch, expected)
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Current routing epoch from control-plane cache.
    pub fn routing_epoch(&self) -> RoutingEpoch {
        self.control_plane.routing_epoch()
    }

    /// Compute session PRG for a tenant/client using jump-consistent hash ring.
    pub fn session_prg(&self, tenant_id: &str, client_id: &str) -> PrgId {
        let prg_index = crate::routing::session_partition(
            tenant_id,
            client_id,
            self.control_plane.tenant_prg_count(tenant_id),
        );
        PrgId {
            tenant_id: tenant_id.to_string(),
            partition_index: prg_index,
        }
    }

    /// Compute topic PRG for a given normalized topic.
    pub fn topic_prg(&self, tenant_id: &str, topic: &str) -> PrgId {
        let prg_index = crate::routing::topic_partition(
            tenant_id,
            topic,
            self.control_plane.tenant_prg_count(tenant_id),
        );
        PrgId {
            tenant_id: tenant_id.to_string(),
            partition_index: prg_index,
        }
    }

    /// Clear forward sequence tracking for a session PRG.
    ///
    /// This should be called when a session is reset (clean_start=true) to allow
    /// new forward sequences starting from 1 to be accepted.
    pub fn clear_session_forward_state(&self, session_prg: &PrgId) {
        self.forwarding.clear_session(session_prg);
    }

    /// Enforce single-tenant identity for PRG operations.
    pub async fn validate_tenant(
        &self,
        prg_id: &PrgId,
        tenant_id: &str,
    ) -> Result<(), crate::routing::RoutingError> {
        if prg_id.tenant_id != tenant_id {
            return Err(crate::routing::RoutingError::TenantMismatch);
        }
        if self.control_plane.ensure_tenant_single_prg(prg_id).is_err() {
            return Err(crate::routing::RoutingError::StaleEpoch {
                local: self.control_plane.routing_epoch().0,
                expected: self.control_plane.routing_epoch().0,
            });
        }
        Ok(())
    }

    /// Mark a PRG migrating and log intent; migration mechanics will integrate with clustor.
    pub async fn mark_migrating(
        &self,
        source: &PrgId,
        destination: &PrgId,
        epoch: RoutingEpoch,
    ) -> Result<()> {
        self.control_plane.update_rebalance_epoch(epoch.0);
        tracing::info!(
            "marking PRG {:?} migrating to {:?} at epoch {}",
            source,
            destination,
            epoch.0
        );
        if let Some(host) = self.host_for(source).await {
            host.set_fence(true);
            let _ = host.ack().step_down_for_upgrade().await;
            self.ack_contract.set_durability_fence(true);
        }
        Ok(())
    }

    /// Prepare PRG for rolling upgrade: stub to transfer leadership and checkpoint.
    pub async fn prepare_upgrade(&self) -> Result<()> {
        let snapshot = self.control_plane.snapshot();
        for (peer, minor) in snapshot.peer_versions {
            if !self.version_gate.compatible(minor) {
                return Err(anyhow!(
                    "version skew too large: peer {} minor {} vs local {}",
                    peer,
                    minor,
                    self.version_gate.current_minor()
                ));
            }
        }
        self.ready.store(false, Ordering::Relaxed);
        let hosts: Vec<Arc<PrgHost<C>>> = self.hosts.lock().await.values().cloned().collect();
        let host_count = hosts.len();
        for host in hosts {
            if let Err(err) = host.ack().step_down_for_upgrade().await {
                tracing::warn!(
                    "failed to demote raft leadership for {:?} during upgrade: {err:?}",
                    host.id()
                );
            }
            let committed = host.ack().committed_index();
            if committed > 0 {
                if let Err(err) = host.ack().wait_for_floor(committed).await {
                    tracing::warn!(
                        "failed to await committed floor {} for {:?}: {err:?}",
                        committed,
                        host.id()
                    );
                }
            }
            if let Err(err) = host.checkpoint_and_step_down().await {
                tracing::warn!(
                    "failed to checkpoint host {:?} during upgrade: {err:?}",
                    host.id()
                );
            }
            host.set_fence(true);
        }
        self.ack_contract.set_durability_fence(true);
        self.set_host_fences(true).await;
        self.control_plane.set_strict_fallback(true);
        tracing::info!(
            "prepared {} PRG hosts for rolling upgrade (fenced + checkpointed)",
            host_count
        );
        Ok(())
    }

    /// Track cross-PRG ratio derived from forwarding requests.
    pub fn cross_prg_ratio(&self) -> f64 {
        let total = self.cross_prg_total.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let cross = self.cross_prg_cross.load(Ordering::Relaxed);
        cross as f64 / total as f64
    }

    /// Persist dedupe/offline floors for a session PRG; backed by per-PRG hosts.
    pub async fn persist_session_state(
        &self,
        prg: &PrgId,
        mut session: PersistedSessionState,
    ) -> Result<u64, ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(prg).await?;
        let clustor_floor = self.clustor_floor_for(prg).await;
        let floors = ProductFloors {
            clustor_floor,
            local_dedup_floor: session.dedupe_floor,
            earliest_offline_queue_index: session.earliest_offline.max(session.offline_floor),
            forward_chain_floor: session.forward_chain_floor,
        };
        let effective = floors.effective_product_floor();
        session.effective_product_floor = effective;
        self.apply_mqtt_entry(
            prg,
            MqttApply::PersistSession {
                session: session.clone(),
            },
        )
        .await?;
        Ok(effective)
    }

    pub async fn persisted_state(&self, prg: &PrgId) -> Option<PersistedPrgState> {
        if let Some(state) = self.state_handle(prg).await {
            return Some(state.lock().await.clone());
        }
        None
    }

    /// Restore persisted session metadata and offline queue for hydration.
    pub async fn session_snapshot(
        &self,
        prg: &PrgId,
        client_id: &str,
    ) -> Result<Option<(PersistedSessionState, Vec<OfflineEntry>)>, ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(prg).await?;
        if let Some(state) = self.state_handle(prg).await {
            let guard = state.lock().await;
            return Ok(guard
                .sessions
                .get(client_id)
                .cloned()
                .map(|s| (s, guard.offline.get(client_id).cloned().unwrap_or_default())));
        }
        Ok(None)
    }

    /// Purge persisted state for a session (e.g., after clean start or expiry).
    pub async fn purge_session(
        &self,
        prg: &PrgId,
        client_id: &str,
    ) -> Result<(), ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(prg).await?;
        if let Some(state) = self.state_handle(prg).await {
            let snapshot = {
                let guard = state.lock().await;
                let mut updated = guard.clone();
                updated.sessions.remove(client_id);
                updated.offline.remove(client_id);
                updated
            };
            self.persist_snapshot_with_commit(prg, snapshot.clone())
                .await;
        }
        Ok(())
    }

    /// Commit a publish to the owning topic PRG and update retained store if requested.
    pub async fn commit_publish(
        &self,
        topic_prg: &PrgId,
        topic: &str,
        payload: &[u8],
        qos: crate::workloads::mqtt::Qos,
        retain: bool,
    ) -> Result<(), ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(topic_prg).await?;
        self.apply_mqtt_entry(
            topic_prg,
            MqttApply::CommitPublish {
                topic: topic.to_string(),
                payload: payload.to_vec(),
                qos,
                retain,
            },
        )
        .await?;
        Ok(())
    }

    /// Forward a publish to a topic PRG, enforcing duplicate suppression per routing epoch.
    #[allow(clippy::too_many_arguments)]
    pub async fn forward_topic_publish(
        &self,
        session_prg: &PrgId,
        topic_prg: &PrgId,
        seq: u64,
        routing_epoch: RoutingEpoch,
        topic: &str,
        payload: &[u8],
        qos: crate::workloads::mqtt::Qos,
        retain: bool,
    ) -> Result<ForwardReceipt, ForwardError> {
        self.guard_read_gate().await?;
        self.record_forwarding_stats(session_prg, topic_prg);
        let local_epoch = self.control_plane.routing_epoch();
        let allow_grace = self.control_plane.rebalance_grace_active();
        if let Err(err) = self.validate_prg_epoch(topic_prg).await {
            self.ready.store(false, Ordering::Relaxed);
            return Err(ForwardError::Control(err));
        }
        let mut target_local = self.has_local_host(topic_prg).await;
        if !target_local {
            target_local = self.ensure_local_host(topic_prg).await.is_some();
        }
        if target_local && allow_grace && local_epoch.0 > routing_epoch.0 {
            let floor = self.clustor_floor_for(topic_prg).await;
            return Ok(ForwardReceipt {
                ack_index: floor,
                duplicate: true,
                applied: false,
                latency_ms: 0,
            });
        }
        let payload = ForwardPublishPayload {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos,
            retain,
        };
        let entry = MqttApply::ForwardIntent {
            target_prg: topic_prg.clone(),
            seq,
            routing_epoch: routing_epoch.0,
            local_epoch: local_epoch.0,
            allow_grace,
            publish: payload,
        };
        let key = ForwardIntentKey {
            session_prg: session_prg.clone(),
            topic_prg: topic_prg.clone(),
            routing_epoch: routing_epoch.0,
            seq,
        };
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.pending_forwards.lock().await;
            if guard.insert(key.clone(), tx).is_some() {
                tracing::warn!("replacing existing pending forward {:?}", key);
            }
        }
        if let Err(err) = self.apply_mqtt_entry(session_prg, entry).await {
            self.pending_forwards.lock().await.remove(&key);
            return Err(ForwardError::Control(err));
        }
        if Handle::try_current().is_ok() {
            match timeout(self.forward_response_timeout, rx).await {
                Ok(Ok(Ok(result))) => Ok(result),
                Ok(Ok(Err(err))) => Err(err),
                Ok(Err(_recv_err)) => {
                    self.pending_forwards.lock().await.remove(&key);
                    Err(ForwardError::Control(ControlPlaneError::StrictFallback))
                }
                Err(_) => {
                    self.pending_forwards.lock().await.remove(&key);
                    Err(ForwardError::Control(ControlPlaneError::StrictFallback))
                }
            }
        } else {
            let duration = self.forward_response_timeout;
            let runtime = TokioRuntimeBuilder::new_current_thread()
                .enable_all()
                .build()
                .expect("forward intent response runtime");
            let result = runtime.block_on(async { timeout(duration, rx).await });
            drop(runtime);
            match result {
                Ok(Ok(Ok(result))) => Ok(result),
                Ok(Ok(Err(err))) => Err(err),
                Ok(Err(_recv_err)) => {
                    self.pending_forwards.lock().await.remove(&key);
                    Err(ForwardError::Control(ControlPlaneError::StrictFallback))
                }
                Err(_) => {
                    self.pending_forwards.lock().await.remove(&key);
                    Err(ForwardError::Control(ControlPlaneError::StrictFallback))
                }
            }
        }
    }

    /// Record an offline enqueue for a subscriber and persist it to WAL/snapshot.
    pub async fn record_offline_entry(
        &self,
        prg: &PrgId,
        client_id: &str,
        entry: OfflineEntry,
    ) -> Result<(), ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(prg).await?;
        self.apply_mqtt_entry(
            prg,
            MqttApply::RecordOffline {
                client_id: client_id.to_string(),
                entry,
            },
        )
        .await?;
        Ok(())
    }

    /// Trim offline entries up to a delivered enqueue index for a subscriber.
    pub async fn clear_offline_entries(
        &self,
        prg: &PrgId,
        client_id: &str,
        upto_index: u64,
    ) -> Result<(), ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(prg).await?;
        self.apply_mqtt_entry(
            prg,
            MqttApply::ClearOffline {
                client_id: client_id.to_string(),
                upto_index,
            },
        )
        .await?;
        Ok(())
    }

    /// Add a subscription to the topic PRG index.
    pub async fn add_subscription(
        &self,
        topic_prg: &PrgId,
        filter: &str,
        qos: crate::workloads::mqtt::Qos,
        client_id: &str,
        shared_group: Option<String>,
    ) -> Result<(), ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(topic_prg).await?;
        self.apply_mqtt_entry(
            topic_prg,
            MqttApply::AddSubscription {
                filter: filter.to_string(),
                subscription: Subscription {
                    client_id: client_id.to_string(),
                    qos,
                    shared_group: shared_group.clone(),
                },
            },
        )
        .await?;
        Ok(())
    }

    pub async fn remove_subscription(
        &self,
        topic_prg: &PrgId,
        filter: &str,
        client_id: &str,
        shared_group: Option<String>,
    ) -> Result<(), ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(topic_prg).await?;
        self.apply_mqtt_entry(
            topic_prg,
            MqttApply::RemoveSubscription {
                filter: filter.to_string(),
                client_id: client_id.to_string(),
                shared_group,
            },
        )
        .await?;
        Ok(())
    }

    pub async fn subscribers_for_topic(
        &self,
        topic_prg: &PrgId,
        topic: &str,
    ) -> Result<Vec<Subscription>, ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(topic_prg).await?;
        if let Some(state) = self.state_handle(topic_prg).await {
            let guard = state.lock().await;
            let mut result = Vec::new();
            // Check all subscription filters against the topic
            for (filter, subs) in &guard.subscriptions {
                if topic_matches_filter(topic, filter) {
                    result.extend(subs.clone());
                }
            }
            return Ok(result);
        }
        Ok(Vec::new())
    }

    pub async fn retained(
        &self,
        topic_prg: &PrgId,
        topic: &str,
    ) -> Result<Option<Vec<u8>>, ControlPlaneError> {
        self.guard_read_gate().await?;
        self.validate_prg_epoch(topic_prg).await?;
        if let Some(state) = self.state_handle(topic_prg).await {
            return Ok(state.lock().await.retained.get(topic).cloned());
        }
        Ok(None)
    }

    /// Wait for durability floor for a specific PRG, falling back to the global contract.
    pub async fn wait_for_prg_floor(&self, prg: &PrgId, index: u64) -> Result<(), anyhow::Error> {
        if let Some(ack) = self.ack_for(prg).await {
            ack.wait_for_floor(index)
                .await
                .map_err(|e| anyhow::anyhow!("durability wait failed: {e}"))
        } else {
            self.ack_contract
                .wait_for_floor(index)
                .await
                .map_err(|e| anyhow::anyhow!("durability wait failed: {e}"))
        }
    }

    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
            && self.control_plane.cache_is_fresh()
            && !self.control_plane.strict_fallback()
            && !self.ack_contract.durability_fence_active()
    }

    pub async fn effective_product_floor(&self, prg: &PrgId) -> u64 {
        self.effective_floors
            .lock()
            .await
            .get(prg)
            .cloned()
            .unwrap_or(0)
    }

    pub async fn validate_prg_epoch(&self, prg: &PrgId) -> Result<RoutingEpoch, ControlPlaneError> {
        let epoch = self.control_plane.routing_epoch();
        if !self.control_plane.configured() {
            self.metrics.record_routing_skew(0);
            return Ok(epoch);
        }
        match self.control_plane.validate_placement_epoch(prg, epoch) {
            Ok(()) => {
                self.metrics.record_routing_skew(0);
                Ok(epoch)
            }
            Err(ControlPlaneError::StaleEpoch) => {
                self.metrics.record_routing_skew(1);
                Err(ControlPlaneError::StaleEpoch)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn active_hosts_len(&self) -> usize {
        self.active_hosts.lock().await.len()
    }

    async fn persist_snapshot_with_commit(&self, prg: &PrgId, mut snapshot: PersistedPrgState) {
        let _ = self.ensure_state_handle(prg).await;
        let clustor_floor = self.clustor_floor_for(prg).await;
        snapshot.recompute_effective_product_floors(clustor_floor);
        let snapshot_clone = snapshot.clone();
        if let Some(host) = self.host_for(prg).await {
            match host.persist(snapshot_clone.clone()).await {
                Ok(idx) => {
                    self.ack_contract.set_term(host.ack().term());
                    self.ack_contract.update_clustor_floor(idx);
                }
                Err(err) => {
                    tracing::warn!(
                        "failed to satisfy clustor ack contract for {:?}: {err:?}",
                        prg
                    );
                    let _ = self
                        .storage_backend
                        .persist_at(prg, &snapshot_clone, clustor_floor.saturating_add(1))
                        .await;
                    self.ack_contract
                        .update_clustor_floor(clustor_floor.saturating_add(1));
                }
            }
        } else {
            tracing::warn!(
                "persist snapshot requested for {:?} but no local host is active",
                prg
            );
            let _ = self
                .storage_backend
                .persist_at(prg, &snapshot_clone, clustor_floor.saturating_add(1))
                .await;
            self.ack_contract
                .update_clustor_floor(clustor_floor.saturating_add(1));
        }
        // Keep the in-memory handle aligned with the persisted snapshot.
        if let Some(handle) = self.state_handle(prg).await {
            let mut guard = handle.lock().await;
            *guard = snapshot_clone;
        }
    }

    async fn apply_mqtt_entry(
        &self,
        prg: &PrgId,
        entry: MqttApply,
    ) -> Result<u64, ControlPlaneError> {
        let label = entry.label();
        if let Some(host) = self.host_for(prg).await {
            return Self::replicate_entry_with_host(host, entry, prg, label).await;
        }
        if let Some(host) = self.ensure_local_host(prg).await {
            return Self::replicate_entry_with_host(host, entry, prg, label).await;
        }
        eprintln!("apply requested but no local host: {:?}", prg);
        tracing::warn!(
            "apply requested for {:?} but no local PRG host is active",
            prg
        );
        Err(ControlPlaneError::StrictFallback)
    }

    async fn replicate_entry_with_host(
        host: Arc<PrgHost<C>>,
        entry: MqttApply,
        prg: &PrgId,
        label: &'static str,
    ) -> Result<u64, ControlPlaneError> {
        host.replicate_workload_entry(entry).await.map_err(|err| {
            tracing::warn!(
                "workload entry apply failed for {:?} op={} err={:?}",
                prg,
                label,
                err
            );
            ControlPlaneError::StrictFallback
        })
    }

    async fn forward_remote_publish(
        &self,
        req: ForwardRequest,
        topic_prg: PrgId,
        entry: MqttApply,
        is_cross: bool,
    ) -> Result<ForwardReceipt, ForwardError> {
        let duplicate = self.forwarding.prepare_forward(&req)?;
        if duplicate {
            let floor = self.clustor_floor_for(&topic_prg).await;
            return Ok(ForwardReceipt {
                ack_index: floor,
                duplicate: true,
                applied: false,
                latency_ms: 0,
            });
        }
        let started = Instant::now();
        let host = match self.remote_host_for(&topic_prg).await {
            Some(h) => h,
            None => {
                tracing::warn!("no remote placement for {:?}", topic_prg);
                return Err(ForwardError::Control(ControlPlaneError::StrictFallback));
            }
        };
        match self
            .remote_workload
            .forward_mqtt_entry(
                &host,
                &topic_prg,
                MqttWorkload::descriptor_static().label,
                &req,
                &entry,
            )
            .await
        {
            Ok(resp) => {
                let latency = if resp.latency_ms > 0 {
                    resp.latency_ms
                } else {
                    started.elapsed().as_millis() as u64
                };
                if is_cross {
                    self.metrics.record_forward_latency(latency);
                }
                Ok(ForwardReceipt {
                    ack_index: resp.ack_index,
                    duplicate: false,
                    applied: resp.applied,
                    latency_ms: latency,
                })
            }
            Err(err) => {
                if is_cross {
                    self.metrics.record_forward_failure();
                }
                tracing::warn!(
                    "remote workload forward failed for {:?}: {err:?}",
                    topic_prg
                );
                Err(self.map_remote_error(err))
            }
        }
    }

    fn map_remote_error(&self, err: RemoteWorkloadError) -> ForwardError {
        match err {
            RemoteWorkloadError::Disabled => {
                ForwardError::Control(ControlPlaneError::StrictFallback)
            }
            RemoteWorkloadError::Unavailable { .. } => {
                ForwardError::Control(ControlPlaneError::StrictFallback)
            }
            RemoteWorkloadError::UnsupportedWorkload(_) => {
                ForwardError::Control(ControlPlaneError::StrictFallback)
            }
            RemoteWorkloadError::CapabilityDisabled(_) => {
                ForwardError::Control(ControlPlaneError::StrictFallback)
            }
        }
    }

    async fn on_apply(&self, evt: crate::clustor_client::ApplyEvent) {
        self.ack_contract.update_clustor_floor(evt.floor);
        self.mark_commit_progress(evt.index).await;
        self.refresh_lag_metrics().await;
        let has_placements = !self.routing.read().await.placements.is_empty();
        self.update_ready_state(has_placements).await;
    }

    async fn replay_committed(&self, committed: u64) {
        let hosts: Vec<Arc<PrgHost<C>>> = self.hosts.lock().await.values().cloned().collect();
        for host in hosts {
            let applied_before = host.committed_index().await;
            if applied_before < committed {
                match host.replay(committed).await {
                    Ok(Some(floor)) => {
                        self.effective_floors
                            .lock()
                            .await
                            .insert(host.id().clone(), floor);
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(
                            "failed to replay committed index {} for {:?}: {err:?}",
                            committed,
                            host.id()
                        );
                    }
                }
            }
        }
        self.refresh_lag_metrics().await;
    }

    fn publish_latest_proof(&self) {
        let proof = self
            .hosts
            .try_lock()
            .ok()
            .and_then(|guard| {
                guard
                    .values()
                    .filter_map(|h| h.ack().latest_proof())
                    .max_by_key(|p| p.index)
            })
            .or_else(|| self.ack_contract.latest_proof());
        if let Some(proof) = proof {
            let cp_proof =
                clustor::replication::consensus::DurabilityProof::new(proof.term, proof.index);
            self.control_plane.publish_proof(cp_proof);
        }
    }

    async fn hydrate_hosts_from_storage(&self) -> Result<()> {
        let hosts: Vec<Arc<PrgHost<C>>> = self.hosts.lock().await.values().cloned().collect();
        for host in hosts.iter() {
            match host.start().await {
                Ok(floor) => {
                    if floor > 0 {
                        self.effective_floors
                            .lock()
                            .await
                            .insert(host.id().clone(), floor);
                        self.ack_contract.update_clustor_floor(floor);
                    }
                    self.metrics.record_replication_lag_for_prg(host.id(), 0);
                }
                Err(err) => match err.downcast::<tokio::task::JoinError>() {
                    Ok(join_err) => {
                        if join_err.is_cancelled() {
                            tracing::warn!(
                                "wal replay cancelled while hydrating {:?} (try QUANTUM_INLINE_WAL_REPLAY=1 to run inline)",
                                host.id()
                            );
                        } else if join_err.is_panic() {
                            tracing::error!(
                                "wal replay panicked while hydrating {:?}: {:?}",
                                host.id(),
                                join_err
                            );
                        } else {
                            tracing::warn!(
                                "failed to hydrate PRG host {:?} from storage (join error): {join_err:?}",
                                host.id()
                            );
                        }
                    }
                    Err(other) => tracing::warn!(
                        "failed to hydrate PRG host {:?} from storage: {other:?}",
                        host.id()
                    ),
                },
            }
        }
        let floor = hosts
            .iter()
            .map(|h| h.clustor_floor())
            .max()
            .unwrap_or_else(|| self.ack_contract.clustor_floor());
        if floor > 0 {
            self.mark_commit_progress(floor).await;
            self.replay_committed(floor).await;
        }
        self.refresh_lag_metrics().await;
        self.publish_latest_proof();
        Ok(())
    }

    async fn ensure_state_handle(&self, prg: &PrgId) -> SharedPrgState {
        let mut guard = self.persisted.lock().await;
        guard
            .entry(prg.clone())
            .or_insert_with(|| Arc::new(Mutex::new(PersistedPrgState::default())))
            .clone()
    }

    async fn state_handle(&self, prg: &PrgId) -> Option<SharedPrgState> {
        self.persisted.lock().await.get(prg).cloned()
    }

    async fn host_for(&self, prg: &PrgId) -> Option<Arc<PrgHost<C>>> {
        self.hosts.lock().await.get(prg).cloned()
    }

    async fn has_local_host(&self, prg: &PrgId) -> bool {
        self.host_for(prg).await.is_some()
    }

    async fn ensure_local_host(&self, prg: &PrgId) -> Option<Arc<PrgHost<C>>> {
        if let Some(host) = self.host_for(prg).await {
            return Some(host);
        }
        let (placement, has_placement) = {
            let routing = self.routing.read().await;
            match routing.placements.get(prg) {
                Some(p) => (p.clone(), true),
                None => (
                    PrgPlacement {
                        node_id: self.durability_cfg.replica_id.clone(),
                        replicas: vec![self.durability_cfg.replica_id.clone()],
                    },
                    false,
                ),
            }
        };
        let state = self.ensure_state_handle(prg).await;
        let workload = {
            let snapshot = state.lock().await.clone();
            Arc::new(Mutex::new(
                Box::new(MqttWorkload::new(snapshot)) as Box<DynWorkload>
            ))
        };
        let host_ctx = PrgHostContext {
            storage: self.storage_backend.clone(),
            ack: self.ack_contract.clone(),
            raft: None,
            metrics: self.metrics.clone(),
            clock: self.clock.clone(),
            schema_registry: self.schema_registry.clone(),
            features: self.workload_features.clone(),
            forwarding: self.forwarding.clone(),
            forward_intent_tx: self.forward_intent_tx.clone(),
            force_inline_apply: !has_placement,
        };
        let node_id = placement.node_id.clone();
        let host = Arc::new(PrgHost::new(
            prg.clone(),
            placement.clone(),
            state,
            workload,
            host_ctx,
        ));
        self.spawn_host_apply_watch(prg.clone(), self.ack_contract.clone());
        host.install_apply_hook();
        if let Err(err) = host.start().await {
            tracing::warn!("failed to start implicit host {:?}: {err:?}", prg);
            return None;
        }
        {
            let mut active = self.active_hosts.lock().await;
            active.insert(prg.clone(), node_id);
        }
        {
            let mut hosts = self.hosts.lock().await;
            hosts.insert(prg.clone(), host.clone());
        }
        Some(host)
    }

    async fn remote_host_for(&self, prg: &PrgId) -> Option<String> {
        let routing = self.routing.read().await;
        routing.placements.get(prg).map(|p| p.node_id.clone())
    }

    fn placement_requires_raft(&self, placement: &PrgPlacement) -> bool {
        if self.raft_net.is_none() {
            return false;
        }
        if placement
            .replicas
            .iter()
            .any(|replica| replica != &self.durability_cfg.replica_id)
        {
            return true;
        }
        self.durability_cfg.quorum_size > 1 || !self.durability_cfg.peer_replicas.is_empty()
    }

    async fn ack_for(&self, prg: &PrgId) -> Option<AckContract> {
        self.host_for(prg).await.map(|h| h.ack())
    }

    async fn clustor_floor_for(&self, prg: &PrgId) -> u64 {
        self.ack_for(prg)
            .await
            .map(|ack| ack.clustor_floor())
            .unwrap_or_else(|| self.ack_contract.clustor_floor())
    }

    async fn refresh_lag_metrics(&self) {
        let hosts: Vec<Arc<PrgHost<C>>> = self.hosts.lock().await.values().cloned().collect();
        let mut max_apply_gap = 0;
        let mut max_replication = self.ack_contract.replication_lag_seconds();
        for host in hosts {
            let committed = host.ack().committed_index();
            let applied = host.committed_index().await;
            let apply_gap = committed.saturating_sub(applied);
            let replication = host.replication_lag().max(apply_gap);
            self.metrics
                .record_replication_lag_for_prg(host.id(), replication);
            max_apply_gap = max_apply_gap.max(apply_gap);
            max_replication = max_replication.max(replication);
        }
        self.metrics.set_apply_lag(max_apply_gap);
        self.metrics.set_replication_lag(max_replication);
    }

    async fn set_host_fences(&self, active: bool) {
        let hosts: Vec<Arc<PrgHost<C>>> = self.hosts.lock().await.values().cloned().collect();
        for host in hosts {
            host.set_fence(active);
        }
    }

    async fn on_host_apply(
        &self,
        prg: PrgId,
        ack: Arc<AckContract>,
        evt: crate::clustor_client::ApplyEvent,
    ) {
        ack.set_term(evt.term);
        ack.update_clustor_floor(evt.floor);
        self.ack_contract.set_term(evt.term);
        self.ack_contract.update_clustor_floor(evt.floor);
        let floor = if let Some(host) = self.host_for(&prg).await {
            host.effective_floor().await
        } else {
            evt.floor
        };
        self.effective_floors.lock().await.insert(prg, floor);
        self.mark_commit_progress(evt.index).await;
        self.refresh_lag_metrics().await;
        let has_placements = !self.routing.read().await.placements.is_empty();
        self.update_ready_state(has_placements).await;
    }

    fn record_forwarding_stats(&self, session_prg: &PrgId, topic_prg: &PrgId) {
        self.cross_prg_total.fetch_add(1, Ordering::Relaxed);
        if session_prg != topic_prg {
            self.cross_prg_cross.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn mark_commit_progress(&self, index: u64) {
        let now = self.clock.now();
        let mut guard = self.commit_progress.lock().await;
        if index > guard.last_index {
            guard.last_index = index;
            guard.last_seen = now;
        }
    }

    async fn commit_advancing(&self, timeout: Duration) -> bool {
        let now = self.clock.now();
        let latest = self.ack_contract.committed_index();
        let mut guard = self.commit_progress.lock().await;
        if latest > guard.last_index {
            guard.last_index = latest;
            guard.last_seen = now;
            return true;
        }
        now.duration_since(guard.last_seen) <= timeout
    }

    async fn raft_leader_health(&self) -> (bool, u64) {
        let hosts = self.hosts.lock().await;
        let mut leaderless: u64 = 0;
        for host in hosts.values() {
            if host.uses_raft() && !host.has_leader() {
                leaderless = leaderless.saturating_add(1);
            }
        }
        (leaderless == 0, leaderless)
    }

    fn readiness_commit_timeout(&self) -> Duration {
        let ttl = self.control_plane.snapshot().cache_ttl;
        if ttl.is_zero() {
            Duration::from_secs(1)
        } else {
            ttl
        }
    }
}

struct PrgHost<C: Clock> {
    prg: PrgId,
    placement: Arc<StdMutex<PrgPlacement>>,
    state: SharedPrgState,
    workload: SharedWorkload,
    storage: ClustorStorage,
    ack: Arc<AckContract>,
    raft: Option<Arc<RaftReplica>>,
    metrics: Arc<BackpressureState>,
    schema_registry: SchemaRegistryHandle,
    workload_features: WorkloadFeatures,
    forwarding: ForwardingEngine,
    forward_intent_tx: UnboundedSender<ForwardIntentMsg>,
    leadership: LeadershipSignal,
    ready_notified: AtomicBool,
    initialized: AtomicBool,
    _clock: C,
    force_inline_apply: bool,
}

#[derive(Clone)]
struct PrgHostContext<C: Clock> {
    storage: ClustorStorage,
    ack: Arc<AckContract>,
    raft: Option<Arc<RaftReplica>>,
    metrics: Arc<BackpressureState>,
    clock: C,
    schema_registry: SchemaRegistryHandle,
    features: WorkloadFeatures,
    forwarding: ForwardingEngine,
    forward_intent_tx: UnboundedSender<ForwardIntentMsg>,
    force_inline_apply: bool,
}

impl<C: Clock> PrgHost<C> {
    fn new(
        prg: PrgId,
        placement: PrgPlacement,
        state: SharedPrgState,
        workload: SharedWorkload,
        ctx: PrgHostContext<C>,
    ) -> Self {
        Self {
            prg,
            placement: Arc::new(StdMutex::new(placement)),
            state,
            workload,
            storage: ctx.storage,
            ack: ctx.ack,
            raft: ctx.raft,
            metrics: ctx.metrics,
            schema_registry: ctx.schema_registry,
            workload_features: ctx.features,
            forwarding: ctx.forwarding,
            forward_intent_tx: ctx.forward_intent_tx,
            leadership: LeadershipSignal::new(),
            ready_notified: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
            _clock: ctx.clock,
            force_inline_apply: ctx.force_inline_apply,
        }
    }

    async fn init_workload(&self) -> Result<()> {
        let span = tracing::info_span!(
            "workload_init",
            tenant = %self.prg.tenant_id,
            partition = self.prg.partition_index
        );
        let init = WorkloadInit::new(
            &self.prg,
            self.ack.clone(),
            self.schema_registry.clone(),
            &self.workload_features,
            self.leadership.clone(),
            span,
        );
        let mut workload = self.workload.lock().await;
        workload
            .init(init)
            .into_result()
            .map_err(|err| Self::workload_err("init", &self.prg, err))
    }

    async fn hydrate_workload(&self, snapshot: &PersistedPrgState) -> Result<()> {
        let mut workload = self.workload.lock().await;
        workload
            .hydrate(snapshot)
            .into_result()
            .map_err(|err| Self::workload_err("hydrate", &self.prg, err))
    }

    async fn store_state_snapshot(
        &self,
        snapshot: PersistedPrgState,
        prev: Option<PersistedPrgState>,
        hydrate: bool,
    ) -> Result<()> {
        self.record_state_metrics(prev.as_ref(), &snapshot);
        {
            let mut guard = self.state.lock().await;
            *guard = snapshot.clone();
        }
        if hydrate {
            self.hydrate_workload(&snapshot).await?;
        }
        Ok(())
    }

    async fn persist_workload_snapshot(
        &self,
        snapshot: &PersistedPrgState,
        term: u64,
        index: u64,
    ) -> Result<()> {
        let proof = DurabilityProof::new(term, index);
        self.storage
            .persist_at_with_proof(&self.prg, snapshot, index, Some(proof))
            .await?;
        Ok(())
    }

    async fn notify_workload_ready(&self) -> Result<()> {
        if self.ready_notified.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        let mut workload = self.workload.lock().await;
        let mut ctx = ReadyContext::new(
            self.prg.clone(),
            WorkloadMetrics::new(self.metrics.clone()),
            self.leadership.clone(),
            tracing::info_span!(
                "workload_ready",
                tenant = %self.prg.tenant_id,
                partition = self.prg.partition_index
            ),
        );
        workload
            .on_ready(&mut ctx)
            .into_result()
            .map_err(|err| Self::workload_err("ready", &self.prg, err))
    }

    async fn compaction_handle_from_state(&self) -> CompactionHandle {
        let state = self.state.lock().await.clone();
        let floors = Self::derive_product_floors(&state, self.ack.clustor_floor());
        CompactionHandle::new(floors)
    }

    fn derive_product_floors(state: &PersistedPrgState, clustor_floor: u64) -> ProductFloors {
        let mut local_dedup = 0;
        let mut earliest_offline = 0;
        let mut forward_chain = 0;
        for session in state.sessions.values() {
            local_dedup = local_dedup.max(session.dedupe_floor);
            earliest_offline =
                earliest_offline.max(session.earliest_offline.max(session.offline_floor));
            forward_chain = forward_chain.max(session.forward_chain_floor);
        }
        ProductFloors {
            clustor_floor,
            local_dedup_floor: local_dedup,
            earliest_offline_queue_index: earliest_offline,
            forward_chain_floor: forward_chain,
        }
    }

    fn workload_err(phase: &str, prg: &PrgId, err: WorkloadError) -> anyhow::Error {
        anyhow::anyhow!("workload {} failed for {:?}: {err}", phase, prg)
    }

    async fn apply_workload_entry(&self, term: u64, index: u64, entry: MqttApply) -> Result<u64> {
        let compaction = self.compaction_handle_from_state().await;
        let mut ctx = ApplyContext::new(
            self.ack.clone(),
            WorkloadMetrics::new(self.metrics.clone()),
            compaction,
            self.schema_registry.clone(),
            self.prg.clone(),
            ApplyMeta::new(term, index, self.ack.clustor_floor()),
            tracing::info_span!(
                "workload_apply",
                tenant = %self.prg.tenant_id,
                partition = self.prg.partition_index,
                kind = %entry.label()
            ),
            self.forwarding.clone(),
        );
        let prev = { self.state.lock().await.clone() };
        {
            let mut workload = self.workload.lock().await;
            workload
                .apply(entry, &mut ctx)
                .into_result()
                .map_err(|err| Self::workload_err("apply", &self.prg, err))?;
            let snapshot = workload
                .build_snapshot()
                .into_result()
                .map_err(|err| Self::workload_err("snapshot", &self.prg, err))?;
            drop(workload);
            let persisted = snapshot.clone();
            self.store_state_snapshot(snapshot, Some(prev), false)
                .await?;
            self.persist_workload_snapshot(&persisted, term, index)
                .await?;
        }
        let intents = ctx.take_forward_intents();
        if !intents.is_empty() {
            let msg = ForwardIntentMsg { intents };
            let intent_count = msg.intents.len();
            if self.forward_intent_tx.send(msg).is_err() {
                tracing::warn!(
                    "forward intent channel closed for {:?}; dropping {} intents",
                    self.prg,
                    intent_count
                );
            }
        }
        let _ = ctx.take_offline_entries();
        self.notify_workload_ready().await.ok();
        Ok(self.effective_floor().await.max(index))
    }

    async fn replicate_workload_entry(&self, entry: MqttApply) -> Result<u64> {
        let prg = self.prg.clone();
        let entry_clone = entry.clone();
        let ack = self.ack.clone();
        let result = self
            .ack
            .commit_with(move |_| {
                let op = RaftOp::WorkloadApply(WorkloadApplyOp {
                    prg: prg.clone(),
                    entry: entry_clone.clone(),
                });
                async move { op.serialize() }
            })
            .await?;
        if self.force_inline_apply || tokio::runtime::Handle::try_current().is_err() {
            let _ = self.apply_workload_entry(ack.term(), result, entry).await?;
        }
        Ok(result)
    }

    fn id(&self) -> &PrgId {
        &self.prg
    }

    fn ack(&self) -> AckContract {
        (*self.ack).clone()
    }

    fn clustor_floor(&self) -> u64 {
        self.ack.clustor_floor()
    }

    fn replication_lag(&self) -> u64 {
        self.ack.replication_lag_seconds()
    }

    fn uses_raft(&self) -> bool {
        self.raft.is_some() && self.ack.has_raft()
    }

    fn force_inline_apply(&self) -> bool {
        self.force_inline_apply
    }

    fn has_leader(&self) -> bool {
        let has = if let Some(raft) = &self.raft {
            raft.has_leader()
        } else {
            true
        };
        self.leadership.set(has);
        has
    }

    fn offline_stats(state: &PersistedPrgState) -> (u64, u64) {
        state.offline.values().fold((0_u64, 0_u64), |acc, entries| {
            let bytes = entries.iter().map(|e| e.payload.len() as u64).sum::<u64>();
            (
                acc.0.saturating_add(entries.len() as u64),
                acc.1.saturating_add(bytes),
            )
        })
    }

    fn record_state_metrics(
        &self,
        prev: Option<&PersistedPrgState>,
        new_state: &PersistedPrgState,
    ) {
        self.metrics.record_queue_depth(
            QueueDepthKind::CommitToApply,
            new_state.sessions.len() as u64,
        );
        self.metrics.record_queue_depth(
            QueueDepthKind::ApplyToDelivery,
            new_state.topics.len() as u64,
        );
        let (prev_entries, prev_bytes) = prev.map(Self::offline_stats).unwrap_or_else(|| (0, 0));
        let (new_entries, new_bytes) = Self::offline_stats(new_state);
        self.metrics.record_offline_delta(
            &self.prg.tenant_id,
            new_entries as i64 - prev_entries as i64,
            new_bytes as i64 - prev_bytes as i64,
        );
    }

    fn set_fence(&self, active: bool) {
        self.ack.set_durability_fence(active);
    }

    fn update_placement(&self, placement: PrgPlacement) {
        if let Ok(mut guard) = self.placement.lock() {
            *guard = placement;
        }
    }

    fn install_apply_hook(self: &Arc<Self>) {
        if let Some(raft) = &self.raft {
            let host = self.clone();
            raft.set_apply_hook(move |term, index, payload| {
                let host = host.clone();
                async move {
                    if let Err(err) = host.apply_payload(term, index, payload.clone()).await {
                        tracing::warn!(
                            "apply hook failed for {:?} at {}: {err:?}",
                            host.id(),
                            index
                        );
                    }
                }
            });
        }
    }

    async fn start(&self) -> Result<u64> {
        if let Some(r) = &self.raft {
            self.ack.attach_raft(r.clone());
        }
        if !self.initialized.swap(true, Ordering::Relaxed) {
            self.init_workload().await?;
        }
        let mut floor = 0;
        if let Some(mut recovered) = self.storage.load_with_proof(&self.prg, None).await? {
            let inferred = recovered
                .effective_floor
                .max(Self::state_floor(&recovered.state))
                .max(self.storage.effective_floor(&self.prg).await);
            recovered.state.recompute_effective_product_floors(inferred);
            self.store_state_snapshot(recovered.state.clone(), None, true)
                .await?;
            floor = inferred;
            if let Some(proof) = recovered.proof {
                self.ack.set_term(proof.term);
                self.ack.update_clustor_floor(proof.index);
            } else {
                self.ack.update_clustor_floor(inferred);
            }
        } else {
            let snapshot = { self.state.lock().await.clone() };
            self.store_state_snapshot(snapshot, None, true).await?;
        }
        self.notify_workload_ready().await?;
        Ok(floor)
    }

    async fn persist(&self, snapshot: PersistedPrgState) -> Result<u64> {
        self.hydrate_workload(&snapshot).await?;
        let prg = self.prg.clone();
        let ack = self.ack.clone();
        let closure_snapshot = snapshot.clone();
        let result = self
            .ack
            .commit_with(move |idx| {
                let mut snapshot = closure_snapshot.clone();
                snapshot.committed_index = idx;
                let prg = prg.clone();
                async move {
                    let op = RaftOp::PersistSnapshot(PersistSnapshotOp {
                        prg,
                        state: snapshot,
                    });
                    op.serialize()
                }
            })
            .await?;
        // When no async runtime is driving apply hooks, install state eagerly.
        if tokio::runtime::Handle::try_current().is_err() {
            let mut snap = snapshot.clone();
            snap.committed_index = result;
            let _ = self.install_state(snap, ack.term(), result, None).await?;
        }
        Ok(result)
    }

    async fn flush(&self) -> Result<u64> {
        let snapshot = self.state.lock().await.clone();
        self.persist(snapshot).await
    }

    async fn checkpoint_and_step_down(&self) -> Result<u64> {
        let committed = self.flush().await?;
        if committed > 0 {
            let _ = self.ack.wait_for_floor(committed).await;
        }
        Ok(committed)
    }

    fn state_floor(state: &PersistedPrgState) -> u64 {
        state.sessions.values().fold(0, |acc, session| {
            let offline_floor = session.earliest_offline.max(session.offline_floor);
            let session_floor = session
                .effective_product_floor
                .max(session.dedupe_floor)
                .max(offline_floor)
                .max(session.forward_chain_floor);
            acc.max(session_floor)
        })
    }

    async fn committed_index(&self) -> u64 {
        self.state.lock().await.committed_index
    }

    async fn effective_floor(&self) -> u64 {
        let state = { self.state.lock().await.clone() };
        let storage_floor = self.storage.effective_floor(&self.prg).await;
        Self::state_floor(&state)
            .max(state.committed_index)
            .max(self.ack.clustor_floor())
            .max(storage_floor)
    }

    async fn apply_payload(&self, term: u64, index: u64, payload: Option<Vec<u8>>) -> Result<u64> {
        if let Some(bytes) = payload {
            match RaftOp::deserialize(&bytes) {
                Ok(RaftOp::PersistSnapshot(op)) => {
                    return self
                        .install_state(
                            op.state,
                            term,
                            index,
                            Some(DurabilityProof::new(term, index)),
                        )
                        .await;
                }
                Ok(RaftOp::WorkloadApply(op)) => {
                    if op.prg != self.prg {
                        tracing::warn!(
                            "workload apply routed to wrong host {:?} expected {:?}",
                            op.prg,
                            self.prg
                        );
                        return Ok(index);
                    }
                    return self.apply_workload_entry(term, index, op.entry).await;
                }
                Ok(RaftOp::Noop) => return Ok(index),
                Ok(other) => {
                    tracing::warn!(
                        "unsupported raft op {:?} for {:?} at index {}",
                        other,
                        self.prg,
                        index
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "failed to decode raft op for {:?} at {}: {err:?}",
                        self.prg,
                        index
                    );
                }
            }
        }
        match self.replay(index).await? {
            Some(floor) => Ok(floor),
            None => Ok(index),
        }
    }

    async fn install_state(
        &self,
        mut new_state: PersistedPrgState,
        term: u64,
        index: u64,
        proof: Option<DurabilityProof>,
    ) -> Result<u64> {
        let prev = { self.state.lock().await.clone() };
        new_state.committed_index = index.max(new_state.committed_index);
        new_state.recompute_effective_product_floors(index);
        let proof = proof.unwrap_or_else(|| DurabilityProof::new(term, index));
        self.storage
            .persist_at_with_proof(&self.prg, &new_state, index, Some(proof))
            .await?;
        self.store_state_snapshot(new_state.clone(), Some(prev), true)
            .await?;
        let storage_floor = self.storage.effective_floor(&self.prg).await;
        Ok(Self::state_floor(&new_state)
            .max(new_state.committed_index)
            .max(storage_floor))
    }

    async fn replay(&self, committed: u64) -> Result<Option<u64>> {
        let prev = { self.state.lock().await.clone() };
        if let Some(recovered) = self
            .storage
            .load_with_proof(&self.prg, Some(committed))
            .await?
        {
            let mut state = recovered.state;
            if state.committed_index == 0 {
                state.committed_index = committed;
            }
            let floor = recovered
                .effective_floor
                .max(Self::state_floor(&state))
                .max(self.storage.effective_floor(&self.prg).await);
            state.recompute_effective_product_floors(floor);
            self.store_state_snapshot(state.clone(), Some(prev), true)
                .await?;
            if let Some(proof) = recovered.proof {
                self.ack.set_term(proof.term);
                self.ack.update_clustor_floor(proof.index);
            } else if floor > 0 {
                self.ack.update_clustor_floor(floor);
            }
            return Ok(Some(floor));
        }
        Ok(None)
    }
}

/// Persisted subset of session state for compaction bookkeeping.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PersistedSessionState {
    pub client_id: String,
    pub dedupe_floor: u64,
    pub offline_floor: u64,
    pub earliest_offline: u64,
    pub forward_chain_floor: u64,
    pub effective_product_floor: u64,
    #[serde(default)]
    pub keep_alive: u16,
    #[serde(default)]
    pub session_expiry_interval: Option<u32>,
    #[serde(default)]
    pub session_epoch: u64,
    #[serde(default)]
    pub inflight: Vec<PersistedInflight>,
    #[serde(default)]
    pub outbound: Vec<PersistedOutbound>,
    #[serde(default)]
    pub dedupe: Vec<PersistedDedupeEntry>,
    #[serde(default)]
    pub forward_seq: Vec<ForwardSeqEntry>,
    #[serde(default)]
    pub last_packet_at: Option<u64>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PersistedPrgState {
    pub sessions: HashMap<String, PersistedSessionState>,
    pub topics: Vec<StoredMessage>,
    pub retained: HashMap<String, Vec<u8>>,
    pub subscriptions: HashMap<String, Vec<Subscription>>,
    pub offline: HashMap<String, Vec<crate::offline::OfflineEntry>>,
    #[serde(default)]
    pub committed_index: u64,
}

impl PersistedPrgState {
    pub fn recompute_effective_product_floors(&mut self, clustor_floor: u64) {
        for session in self.sessions.values_mut() {
            let floors = ProductFloors {
                clustor_floor,
                local_dedup_floor: session.dedupe_floor,
                earliest_offline_queue_index: session.earliest_offline.max(session.offline_floor),
                forward_chain_floor: session.forward_chain_floor,
            };
            session.effective_product_floor = floors.effective_product_floor();
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: crate::workloads::mqtt::Qos,
    pub retained: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Subscription {
    pub client_id: String,
    pub qos: crate::workloads::mqtt::Qos,
    #[serde(default)]
    pub shared_group: Option<String>,
}

/// Check if an MQTT topic matches a subscription filter.
///
/// MQTT topic filter wildcards:
/// - `#` matches any number of levels (must be at end, after `/` or alone)
/// - `+` matches exactly one level
///
/// Examples:
/// - `test` matches filter `test`
/// - `test/foo` matches filter `test/+`
/// - `test/foo/bar` matches filter `test/#`
/// - `test/foo/bar` matches filter `#`
pub fn topic_matches_filter(topic: &str, filter: &str) -> bool {
    // Handle exact match first (most common case)
    if topic == filter {
        return true;
    }

    // Handle multi-level wildcard at filter root
    if filter == "#" {
        return true;
    }

    let topic_parts: Vec<&str> = topic.split('/').collect();
    let filter_parts: Vec<&str> = filter.split('/').collect();

    let mut ti = 0;
    let mut fi = 0;

    while fi < filter_parts.len() {
        let fp = filter_parts[fi];

        if fp == "#" {
            // Multi-level wildcard matches everything remaining
            return true;
        }

        if ti >= topic_parts.len() {
            // Topic exhausted but filter still has parts (and it's not #)
            return false;
        }

        if fp == "+" {
            // Single-level wildcard matches exactly one level
            ti += 1;
            fi += 1;
        } else if fp == topic_parts[ti] {
            // Exact match for this level
            ti += 1;
            fi += 1;
        } else {
            // Mismatch
            return false;
        }
    }

    // Both must be exhausted for a match (unless filter ended with #)
    ti == topic_parts.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_matches_filter_exact() {
        assert!(topic_matches_filter("test", "test"));
        assert!(topic_matches_filter("test/foo", "test/foo"));
        assert!(topic_matches_filter("test/foo/bar", "test/foo/bar"));
        assert!(!topic_matches_filter("test", "test2"));
        assert!(!topic_matches_filter("test/foo", "test/bar"));
    }

    #[test]
    fn test_topic_matches_filter_multi_level_wildcard() {
        // # at root matches everything
        assert!(topic_matches_filter("test", "#"));
        assert!(topic_matches_filter("test/foo", "#"));
        assert!(topic_matches_filter("test/foo/bar", "#"));

        // # at end of filter
        assert!(topic_matches_filter("test", "test/#"));
        assert!(topic_matches_filter("test/foo", "test/#"));
        assert!(topic_matches_filter("test/foo/bar", "test/#"));
        assert!(!topic_matches_filter("other", "test/#"));
        assert!(!topic_matches_filter("other/foo", "test/#"));
    }

    #[test]
    fn test_topic_matches_filter_single_level_wildcard() {
        assert!(topic_matches_filter("test/foo", "test/+"));
        assert!(topic_matches_filter("test/bar", "test/+"));
        assert!(!topic_matches_filter("test", "test/+"));
        assert!(!topic_matches_filter("test/foo/bar", "test/+"));

        // Multiple + wildcards
        assert!(topic_matches_filter("a/b/c", "+/+/+"));
        assert!(topic_matches_filter("test/foo/bar", "+/foo/+"));
        assert!(!topic_matches_filter("test/baz/bar", "+/foo/+"));
    }

    #[test]
    fn test_topic_matches_filter_mixed_wildcards() {
        assert!(topic_matches_filter("test/foo/bar/baz", "test/+/#"));
        assert!(topic_matches_filter("test/foo", "test/+/#"));
        assert!(topic_matches_filter("a/b/c/d/e", "+/b/#"));
        assert!(!topic_matches_filter("a/x/c/d/e", "+/b/#"));
    }
}
