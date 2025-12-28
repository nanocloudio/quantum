//! Control plane client and cache coordination.
//!
//! This module provides the primary `ControlPlaneClient` which caches
//! routing epochs, tenant metadata, and placement information from
//! the embedded CP-Raft. The file is large (~1450 lines) because it
//! consolidates all CP interaction logic, cache refresh, proof
//! coordination, and telemetry export in one cohesive unit. Submodules
//! handle specialized concerns (api, capabilities, cli, embedded,
//! migration, placement, routing_cache, telemetry).

use anyhow::{anyhow, Result};
use clustor::control_plane::capabilities::FeatureManifest;
use clustor::control_plane::core::client::{
    CpApiTransport, CpClientError, CpControlPlaneClient, TransportResponse,
};
use clustor::control_plane::core::{
    CommitVisibility as CpCommitVisibility, CpCachePolicy, CpCacheState, CpProofCoordinator,
    CpUnavailableReason,
};
use clustor::cp_raft::{CpPlacementClient, PlacementRecord, RoutingEpochError};
use clustor::replication::consensus::DurabilityProof;
use clustor::replication::consensus::{ConsensusCore, ConsensusCoreConfig};
use clustor::security::{MtlsIdentityManager, SecurityError};
use clustor::telemetry::SharedMetricsRegistry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::audit;
use crate::config::QuotaLimits;
use crate::config::{ControlPlaneBootstrap, ControlPlaneConfig, ControlPlaneMode};
use crate::routing::{PrgId, PrgPlacement, RoutingEpoch, RoutingTable};
use crate::time::Clock;
pub mod api;
pub mod capabilities;
pub mod cli;
pub mod embedded;
pub mod migration;
pub mod placement;
pub mod routing_cache;
pub mod telemetry;
pub use api::{
    AssignProtocolRequest, AssignProtocolResponse, GetTenantRequest, GetTenantResponse,
    ListTenantsRequest, ListTenantsResponse, ProtocolAssignmentResponse,
    TenantCapabilitiesResponse, TenantSummary, UpsertTenantRequest, UpsertTenantResponse,
};
pub use capabilities::{
    CapabilityError, CapabilityRegistry, ProtocolAssignment, ProtocolFeatureFlag, ProtocolLimits,
    ProtocolType, TenantCapabilities,
};
pub use cli::{CapabilityInspector, InspectFormat, TenantInspectOutput};
pub use migration::{
    MigrationEntry, MigrationError, MigrationValidation, MigrationValidator,
    MigrationValidatorConfig, PrgStateSnapshot,
};
pub use placement::{
    CapacityCalculator, CapacityReport, DiskTier, HardwareRequirements, HardwareValidation,
    HardwareValidationError, IoProfile, NodeHardware, PlacementDecision, PlacementError,
    PlacementPlan, PlacementPlanner, PlacementRequest, WorkloadPlacementHints,
    WorkloadResourceProfile, WorkloadResources,
};
pub use routing_cache::{
    CacheInvalidationCoordinator, InvalidationReason, RoutingCache, RoutingCacheEntry,
    RoutingCacheKey, RoutingCacheStats, RoutingHint,
};
use serde::Deserialize;
pub use telemetry::{
    AlertSeverity, CpAlert, CpAlertState, CpAlertThresholds, CpWorkloadTelemetry,
    WorkloadPlacementMetrics,
};
use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio::time::interval;

/// Thin wrapper so we can store heterogeneous transports behind a single CpControlPlaneClient.
#[derive(Clone)]
pub struct BoxTransport {
    inner: Arc<dyn CpApiTransport + Send + Sync>,
}

impl BoxTransport {
    pub fn new(inner: impl CpApiTransport + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl CpApiTransport for BoxTransport {
    fn get(&self, path: &str) -> Result<TransportResponse, CpClientError> {
        self.inner.get(path)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheState {
    Fresh,
    Stale,
    Expired,
}

#[derive(Debug, Clone, Copy)]
pub enum ControlPlaneError {
    CacheExpired,
    NeededForReadIndex,
    StrictFallback,
    StaleEpoch,
}

/// Snapshot of control-plane state; will grow to include routing, tenant policies, and cert bundles.
#[derive(Debug, Clone)]
pub struct ControlPlaneState {
    pub endpoints: Vec<String>,
    pub cache_ttl: Duration,
    pub tenant_prg_count: u64,
    pub routing_epoch: RoutingEpoch,
    pub last_refresh: Instant,
    pub cache_state: CacheState,
    pub rebalance_epoch: u64,
    pub rebalance_started_at: Option<Instant>,
    pub strict_fallback: bool,
    pub tenant_overrides: HashMap<String, u64>,
    pub peer_versions: HashMap<String, u8>,
    pub placements: HashMap<PrgId, PrgPlacement>,
    pub tenant_tokens: HashMap<String, Vec<String>>,
    pub tenant_certs: HashMap<String, Vec<String>>,
    pub tenant_acl_prefixes: HashMap<String, Vec<String>>,
    pub tenant_quotas: HashMap<String, QuotaLimits>,
    pub last_read_gate: Option<bool>,
    pub commit_visibility: CpCommitVisibility,
    pub wal_committed_index: u64,
    pub raft_commit_index: u64,
    pub last_local_proof: Option<DurabilityProof>,
    pub last_published_proof: Option<DurabilityProof>,
    pub metrics: SharedMetricsRegistry,
    /// Protocol capability registry for tenant workload assignments.
    pub capabilities: CapabilityRegistry,
    /// Capability epoch for tracking configuration changes.
    pub capability_epoch: u64,
    /// Capability digest for cache invalidation (hash of tenant protocol assignments).
    pub capability_digest: u64,
    /// Protocol revision for schema versioning.
    pub protocol_revision: u64,
}

/// Control-plane client facade. Actual client wiring to clustor will be added as the project evolves.
#[derive(Clone)]
pub struct ControlPlaneClient<C: Clock> {
    cfg: ControlPlaneConfig,
    clock: C,
    state: Arc<Mutex<ControlPlaneState>>,
    placement: Arc<Mutex<CpPlacementClient>>,
    cp_client: Arc<Mutex<Option<CpControlPlaneClient<BoxTransport>>>>,
    routing_watch: watch::Sender<RoutingEpoch>,
    guard: Arc<Mutex<CpProofCoordinator>>,
}

impl<C: Clock> ControlPlaneClient<C> {
    pub fn new(cfg: ControlPlaneConfig, tenant_prg_count: u64, clock: C) -> Result<Self> {
        let cache_ttl = Duration::from_secs(cfg.cache_ttl_seconds);
        let peer_versions = cfg.peer_versions.clone().unwrap_or_default();
        let endpoints = cfg.endpoints.clone();
        let metrics = SharedMetricsRegistry::new("quantum_cp");
        let _ = metrics.set_gauge("cache_state", cache_state_gauge(CacheState::Fresh));
        let _ = metrics.set_gauge("routing_epoch", 0);
        let _ = metrics.set_gauge("placement_count", 0);
        let mut guard = CpProofCoordinator::new(ConsensusCore::new(ConsensusCoreConfig::default()));
        let ttl_ms = cache_ttl.as_millis() as u64;
        if ttl_ms > 0 {
            let policy = CpCachePolicy::new(ttl_ms).with_cache_windows(ttl_ms, ttl_ms);
            guard = guard.with_cache_policy(policy);
        }
        let state = ControlPlaneState {
            endpoints,
            cache_ttl,
            tenant_prg_count,
            routing_epoch: RoutingEpoch(0),
            last_refresh: clock.now(),
            cache_state: CacheState::Fresh,
            rebalance_epoch: 0,
            rebalance_started_at: None,
            strict_fallback: false,
            tenant_overrides: HashMap::new(),
            peer_versions,
            placements: HashMap::new(),
            tenant_tokens: HashMap::new(),
            tenant_certs: HashMap::new(),
            tenant_acl_prefixes: HashMap::new(),
            tenant_quotas: HashMap::new(),
            last_read_gate: None,
            commit_visibility: CpCommitVisibility::DurableOnly,
            wal_committed_index: 0,
            raft_commit_index: 0,
            last_local_proof: None,
            last_published_proof: None,
            metrics,
            capabilities: CapabilityRegistry::new(),
            capability_epoch: 0,
            capability_digest: 0,
            protocol_revision: 0,
        };
        let (routing_watch, _) = watch::channel(RoutingEpoch(0));
        Ok(Self {
            cfg: cfg.clone(),
            clock,
            state: Arc::new(Mutex::new(state)),
            placement: Arc::new(Mutex::new(CpPlacementClient::new(cache_ttl))),
            cp_client: Arc::new(Mutex::new(None)),
            routing_watch,
            guard: Arc::new(Mutex::new(guard)),
        })
    }

    /// Placeholder for fetching initial state and starting watches.
    pub async fn start(&self) -> Result<()> {
        let _started_at = self.clock.now();
        let endpoints_configured = self
            .state
            .lock()
            .map(|s| !s.endpoints.is_empty())
            .unwrap_or(false);
        let cp_configured = self.cp_configured();
        if endpoints_configured
            && !cp_configured
            && matches!(self.cfg.mode, ControlPlaneMode::External)
        {
            anyhow::bail!("control-plane endpoints configured but transport not installed");
        }
        if !cp_configured {
            tracing::warn!("control-plane transport not configured; using static endpoints only");
            self.mark_refreshed();
            return Ok(());
        }
        match self.refresh_from_cp() {
            Ok(_) if self.cache_is_fresh() => self.mark_refreshed(),
            Ok(_) => self.mark_stale(),
            Err(err) => {
                tracing::warn!("control-plane initial refresh failed: {err:?}");
                self.mark_stale();
            }
        }
        if let Ok(mut state) = self.state.lock() {
            let endpoints = state.endpoints.clone();
            for endpoint in endpoints {
                state.peer_versions.entry(endpoint).or_insert(0);
            }
        }
        let state = self.state.lock().unwrap().clone();
        if cp_configured {
            self.spawn_watcher();
        }
        tracing::info!(
            "control-plane client started with endpoints={:?} cache_ttl={:?} prg_count={} epoch={} rebalance_epoch={}",
            state.endpoints,
            state.cache_ttl,
            state.tenant_prg_count,
            state.routing_epoch.0,
            state.rebalance_epoch
        );
        Ok(())
    }

    fn spawn_watcher(&self) {
        if Handle::try_current().is_err() {
            return;
        }
        let mut period = {
            let state = self.state.lock().unwrap();
            if state.cache_ttl.is_zero() {
                Duration::from_millis(50)
            } else {
                state.cache_ttl / 2
            }
        };
        if period.is_zero() {
            period = Duration::from_millis(50);
        }
        let cp = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(period);
            loop {
                ticker.tick().await;
                match cp.refresh_from_cp() {
                    Ok(_) => cp.mark_refreshed(),
                    Err(err) => {
                        tracing::warn!("control-plane watcher refresh failed: {err:?}");
                        cp.mark_stale();
                    }
                }
            }
        });
    }

    /// Surface commit visibility and quorum position from the clustor data plane.
    pub fn set_commit_visibility(&self, visibility: CpCommitVisibility) {
        if let Ok(mut state) = self.state.lock() {
            state.commit_visibility = visibility;
        }
    }

    /// Publish the latest durability proof observed on the data-plane path.
    pub fn publish_proof(&self, proof: DurabilityProof) {
        if let Ok(mut state) = self.state.lock() {
            let current = state.last_published_proof.map(|p| p.index).unwrap_or(0);
            if proof.index > current {
                state.last_published_proof = Some(proof);
                state.last_refresh = self.clock.now();
                state.cache_state = CacheState::Fresh;
                record_publish_metrics(&state.metrics, &proof);
                update_cache_metric(&state.metrics, state.cache_state);
            }
        }
        if let Ok(mut guard) = self.guard.lock() {
            guard.publish_cp_proof_at(proof, self.clock.now());
        }
    }

    /// Record the latest apply/ack position from the clustor client.
    pub fn ingest_apply(&self, term: u64, index: u64, visibility: CpCommitVisibility) {
        if let Ok(mut state) = self.state.lock() {
            state.commit_visibility = visibility;
            state.wal_committed_index = index;
            state.raft_commit_index = index;
            state.last_local_proof = Some(DurabilityProof::new(term, index));
            state.last_refresh = self.clock.now();
            state.cache_state = CacheState::Fresh;
            if state.last_published_proof.is_none() {
                state.last_published_proof = state.last_local_proof;
            }
            record_commit_metrics(&state.metrics, index);
            update_cache_metric(&state.metrics, state.cache_state);
        }
        if let Ok(mut guard) = self.guard.lock() {
            guard.publish_cp_proof_at(DurabilityProof::new(term, index), self.clock.now());
        }
    }

    /// Install a clustor control-plane client for live routing/placement updates.
    pub fn configure_cp_client(
        &self,
        transport: impl CpApiTransport + Send + Sync + 'static,
        mtls: MtlsIdentityManager,
    ) {
        let client =
            CpControlPlaneClient::new(BoxTransport::new(transport), mtls, "/routing", "/features");
        if let Ok(mut guard) = self.cp_client.lock() {
            *guard = Some(client);
        }
    }

    /// Fetch routing/placement data from the control plane and update caches.
    pub fn refresh_from_cp(&self) -> Result<()> {
        let attempts = 3;
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 0..attempts {
            let now = self.clock.now();
            match self.try_refresh_once(now) {
                Ok(_) => return Ok(()),
                Err(err) => {
                    last_err = Some(err);
                    if attempt + 1 < attempts {
                        let backoff = Duration::from_millis(50 * (attempt as u64 + 1));
                        sleep(backoff);
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("control-plane refresh failed")))
    }

    fn try_refresh_once(&self, now: Instant) -> Result<()> {
        let mut guard = self
            .cp_client
            .lock()
            .map_err(|e| anyhow!("cp_client poisoned: {e}"))?;
        let Some(client) = guard.as_mut() else {
            self.mark_stale();
            return Err(anyhow!("control-plane client not configured"));
        };
        let mut placement = self
            .placement
            .lock()
            .map_err(|e| anyhow!("placement client poisoned: {e}"))?;
        if let Err(err) = client.fetch_routing_bundle(&mut placement, now) {
            if self.should_fail_open_revocation(&err) {
                tracing::warn!(
                    "control-plane routing fetch blocked by {:?}; applying embedded revocation waiver",
                    err
                );
                client.apply_revocation_waiver("embedded revocation fail-open", now);
                if let Err(retry_err) = client.fetch_routing_bundle(&mut placement, now) {
                    self.mark_stale();
                    return Err(retry_err.into());
                }
            } else {
                self.mark_stale();
                return Err(err.into());
            }
        }
        let records = placement.records();
        drop(placement);
        self.apply_placement_records(records, now);
        match client.fetch_feature_manifest(now) {
            Ok(manifest) => self.apply_feature_manifest(&manifest),
            Err(err) => {
                if self.should_fail_open_revocation(&err) {
                    tracing::warn!(
                        "feature manifest fetch blocked by {:?}; applying embedded revocation waiver",
                        err
                    );
                    client.apply_revocation_waiver("embedded revocation fail-open", now);
                    match client.fetch_feature_manifest(now) {
                        Ok(manifest) => self.apply_feature_manifest(&manifest),
                        Err(retry_err) => tracing::warn!(
                            "feature manifest fetch failed after waiver: {retry_err:?}"
                        ),
                    }
                } else {
                    tracing::warn!("feature manifest fetch failed: {err:?}");
                }
            }
        }
        Ok(())
    }

    fn should_fail_open_revocation(&self, err: &CpClientError) -> bool {
        if !self.is_embedded() {
            return false;
        }
        match err {
            CpClientError::Security(security_err) => {
                matches!(
                    security_err,
                    SecurityError::RevocationDataStale | SecurityError::RevocationFailClosed
                )
            }
            _ => false,
        }
    }

    /// Placeholder freshness check; will be backed by clustor read-gate once client is wired.
    pub fn cache_is_fresh(&self) -> bool {
        let now = self.clock.now();
        let cp_configured = self.cp_configured();
        let embedded = self.is_embedded();
        let mut state = self.state.lock().unwrap();
        let cache_ttl_ms = state.cache_ttl.as_millis() as u64;
        let fail_open_embedded = embedded && !state.placements.is_empty();
        if embedded && !cp_configured {
            // Local-only embedded mode: empty endpoint lists are treated as stale but not strict.
            // Preserve explicit strict-fallback fences.
            if state.strict_fallback {
                state.cache_state = CacheState::Expired;
            } else if state.endpoints.is_empty() {
                state.cache_state = CacheState::Stale;
            } else {
                let age = now.duration_since(state.last_refresh);
                if state.cache_ttl.is_zero() || age <= state.cache_ttl {
                    state.cache_state = CacheState::Fresh;
                } else {
                    state.cache_state = CacheState::Expired;
                    state.strict_fallback = true;
                }
            }
            update_cache_metric(&state.metrics, state.cache_state);
        } else {
            if embedded && !state.placements.is_empty() {
                state.cache_state = CacheState::Fresh;
                update_cache_metric(&state.metrics, state.cache_state);
            }
            if !cp_configured {
                state.cache_state = CacheState::Stale;
                update_cache_metric(&state.metrics, state.cache_state);
                if !embedded {
                    state.cache_state = CacheState::Expired;
                    state.strict_fallback = true;
                    update_cache_metric(&state.metrics, state.cache_state);
                }
            }
            if state.strict_fallback {
                state.cache_state = CacheState::Expired;
                update_cache_metric(&state.metrics, state.cache_state);
            }
            if cp_configured && state.placements.is_empty() {
                if embedded {
                    state.cache_state = CacheState::Stale;
                    update_cache_metric(&state.metrics, state.cache_state);
                } else {
                    state.cache_state = CacheState::Expired;
                    state.strict_fallback = true;
                    update_cache_metric(&state.metrics, state.cache_state);
                }
            }
            if let Some(start) = state.rebalance_started_at {
                let grace = state.cache_ttl.max(Duration::from_millis(50));
                if now.duration_since(start) > grace {
                    state.cache_state = CacheState::Expired;
                    state.strict_fallback = true;
                    update_cache_metric(&state.metrics, state.cache_state);
                }
            }
            if !state.cache_ttl.is_zero() {
                let age = now.duration_since(state.last_refresh);
                if age > state.cache_ttl {
                    state.cache_state = CacheState::Expired;
                    state.strict_fallback = true;
                    update_cache_metric(&state.metrics, state.cache_state);
                }
            }
        }
        if fail_open_embedded {
            if state.cache_state != CacheState::Fresh {
                tracing::debug!(
                    "embedded fail-open overriding cache_state={:?} placements={} epoch={}",
                    state.cache_state,
                    state.placements.len(),
                    state.routing_epoch.0
                );
            }
            state.cache_state = CacheState::Fresh;
            state.last_refresh = now;
            state.strict_fallback = false;
            update_cache_metric(&state.metrics, state.cache_state);
        }
        let placements = state.placements.clone();
        let cache_state = state.cache_state;
        drop(state);
        if cp_configured && matches!(cache_state, CacheState::Fresh) && !placements.is_empty() {
            if let Ok(mut placement) = self.placement.lock() {
                for prg in placements.keys() {
                    let key = format!("{}:{}", prg.tenant_id, prg.partition_index);
                    match placement.validate_routing_epoch(&key, self.routing_epoch().0, now) {
                        Ok(_) | Err(RoutingEpochError::UnknownPartition { .. }) => {}
                        Err(_) => {
                            self.set_strict_fallback(true);
                            return false;
                        }
                    }
                }
            }
        }
        let state = self.state.lock().unwrap();
        let cp_state = match state.cache_state {
            CacheState::Fresh => CpCacheState::Fresh,
            CacheState::Stale => CpCacheState::Stale {
                age_ms: cache_ttl_ms,
            },
            CacheState::Expired => CpCacheState::Expired {
                age_ms: cache_ttl_ms,
            },
        };
        drop(state);
        if let Ok(mut guard) = self.guard.lock() {
            guard.set_cache_state(cp_state);
        }
        matches!(cp_state, CpCacheState::Fresh)
    }

    pub fn require_fresh_read(&self) -> Result<(), ControlPlaneError> {
        self.guard_read_gate(true)
    }

    pub fn guard_read_gate(&self, read_gate_ok: bool) -> Result<(), ControlPlaneError> {
        if self.strict_fallback() {
            let placements = self
                .state
                .lock()
                .map(|s| (s.placements.len(), s.routing_epoch))
                .unwrap_or((0, RoutingEpoch(0)));
            let embedded_fail_open = self.is_embedded() && placements.0 > 0;
            if embedded_fail_open {
                tracing::warn!(
                    "allowing embedded control-plane strict fallback override placements={} epoch={}",
                    placements.0,
                    placements.1 .0
                );
                self.set_strict_fallback(false);
                self.mark_refreshed();
            } else {
                if let Ok(mut state) = self.state.lock() {
                    state.last_read_gate = Some(false);
                }
                return Err(ControlPlaneError::StrictFallback);
            }
        }
        // If no control-plane transport is configured (e.g., test/local single-node),
        // allow healthy signals but still honor explicit fences.
        if !self.configured() {
            if let Ok(mut state) = self.state.lock() {
                state.last_read_gate = Some(read_gate_ok);
            }
            if !read_gate_ok {
                // Only set strict_fallback if endpoints are configured, indicating
                // this is a real control plane that failed, not just a test/local setup.
                if !self.cfg.endpoints.is_empty() {
                    self.set_strict_fallback(true);
                }
                return Err(ControlPlaneError::NeededForReadIndex);
            }
            return Ok(());
        }
        self.cache_is_fresh();
        if !read_gate_ok {
            self.set_strict_fallback(true);
            if let Ok(mut state) = self.state.lock() {
                state.last_read_gate = Some(false);
            }
            return Err(ControlPlaneError::NeededForReadIndex);
        }
        let now = self.clock.now();
        let decision = {
            let mut guard = self.guard.lock().unwrap();
            guard.guard_read_index(now)
        };
        match decision {
            Ok(_) => {
                if let Ok(mut state) = self.state.lock() {
                    state.last_read_gate = Some(true);
                }
                Ok(())
            }
            Err(err) => {
                if let Ok(mut state) = self.state.lock() {
                    state.last_read_gate = Some(false);
                }
                match err.response().reason {
                    CpUnavailableReason::CacheExpired => Err(ControlPlaneError::CacheExpired),
                    CpUnavailableReason::NeededForReadIndex => {
                        Err(ControlPlaneError::NeededForReadIndex)
                    }
                    CpUnavailableReason::CircuitBreakerOpen => {
                        Err(ControlPlaneError::StrictFallback)
                    }
                }
            }
        }
    }

    /// Ingest a clustor read-gate signal (true = healthy, false = fenced).
    pub fn ingest_read_gate(&self, ok: bool) {
        if self.guard_read_gate(ok).is_ok() {
            self.mark_refreshed();
        }
    }

    pub fn routing_epoch(&self) -> RoutingEpoch {
        self.state.lock().unwrap().routing_epoch
    }

    pub fn routing_updates(&self) -> watch::Receiver<RoutingEpoch> {
        self.routing_watch.subscribe()
    }

    pub fn configured(&self) -> bool {
        self.cp_configured()
    }

    pub fn is_embedded(&self) -> bool {
        matches!(self.cfg.mode, ControlPlaneMode::Embedded)
    }

    /// Provide a full snapshot for routing table construction and health surfaces.
    pub fn snapshot(&self) -> ControlPlaneState {
        self.state.lock().unwrap().clone()
    }

    pub fn validate_routing_epoch(
        &self,
        expected: RoutingEpoch,
    ) -> Result<(), crate::routing::RoutingError> {
        match crate::routing::ensure_routing_epoch(self.routing_epoch(), expected) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.set_strict_fallback(true);
                Err(err)
            }
        }
    }

    pub fn tenant_prg_count(&self, tenant_id: &str) -> u64 {
        let state = self.state.lock().unwrap();
        state
            .tenant_overrides
            .get(tenant_id)
            .cloned()
            .unwrap_or(state.tenant_prg_count)
    }

    pub fn update_routing_epoch(&self, epoch: RoutingEpoch) {
        if let Ok(mut state) = self.state.lock() {
            state.routing_epoch = epoch;
            state.last_refresh = self.clock.now();
            state.cache_state = CacheState::Fresh;
            update_routing_metrics(&state.metrics, epoch, state.placements.len());
            update_cache_metric(&state.metrics, state.cache_state);
        }
        let _ = self.routing_watch.send(epoch);
    }

    /// Build a routing table snapshot by fanning endpoints across the tenant-local ring.
    pub fn routing_snapshot(&self) -> RoutingTable {
        let cp_configured = self.cp_configured();
        let state = self.state.lock().unwrap().clone();
        let embedded = matches!(self.cfg.mode, ControlPlaneMode::Embedded);
        if embedded && state.placements.is_empty() && !cp_configured {
            // fall through to synthetic placements when embedded CP has not bootstrapped yet
        } else if !state.placements.is_empty() || (cp_configured && embedded) {
            return RoutingTable {
                epoch: state.routing_epoch,
                placements: state.placements.clone(),
            };
        }
        let endpoints = if state.endpoints.is_empty() {
            vec!["localhost".to_string()]
        } else {
            state.endpoints.clone()
        };
        let mut placements = HashMap::new();
        let tenants: Vec<String> = if state.tenant_overrides.is_empty() {
            vec!["default".to_string()]
        } else {
            state.tenant_overrides.keys().cloned().collect()
        };
        for tenant in tenants {
            let count = self.tenant_prg_count(&tenant);
            for idx in 0..count {
                let node_idx = (idx as usize) % endpoints.len();
                placements.insert(
                    PrgId {
                        tenant_id: tenant.clone(),
                        partition_index: idx,
                    },
                    PrgPlacement {
                        node_id: format!("{}-prg-{}", endpoints[node_idx], idx),
                        replicas: vec![format!("{}-prg-{}", endpoints[node_idx], idx)],
                    },
                );
            }
        }
        RoutingTable {
            epoch: state.routing_epoch,
            placements,
        }
    }

    fn cp_configured(&self) -> bool {
        if let Ok(guard) = self.cp_client.lock() {
            if guard.is_some() {
                return true;
            }
        }
        if matches!(self.cfg.mode, ControlPlaneMode::Embedded) {
            return self
                .state
                .lock()
                .map(|s| !s.placements.is_empty())
                .unwrap_or(false);
        }
        false
    }

    pub fn mark_refreshed(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.last_refresh = self.clock.now();
            state.cache_state = CacheState::Fresh;
            state.strict_fallback = false;
            update_cache_metric(&state.metrics, state.cache_state);
        }
        if let Ok(mut guard) = self.guard.lock() {
            guard.record_cache_refresh(self.clock.now());
        }
    }

    pub fn mark_stale(&self) {
        let mut age_ms = 0;
        if let Ok(mut state) = self.state.lock() {
            state.cache_state = CacheState::Expired;
            state.strict_fallback = true;
            update_cache_metric(&state.metrics, state.cache_state);
            age_ms = self
                .clock
                .now()
                .saturating_duration_since(state.last_refresh)
                .as_millis() as u64;
        }
        if let Ok(mut guard) = self.guard.lock() {
            guard.set_cache_state(CpCacheState::Expired { age_ms });
        }
    }

    pub fn set_strict_fallback(&self, value: bool) {
        let mut age_ms = 0;
        if let Ok(mut state) = self.state.lock() {
            state.strict_fallback = value;
            if value {
                state.cache_state = CacheState::Expired;
                age_ms = self
                    .clock
                    .now()
                    .saturating_duration_since(state.last_refresh)
                    .as_millis() as u64;
            }
            update_cache_metric(&state.metrics, state.cache_state);
        }
        if value {
            if let Ok(mut guard) = self.guard.lock() {
                guard.set_cache_state(CpCacheState::Expired { age_ms });
            }
        }
    }

    pub fn strict_fallback(&self) -> bool {
        self.state
            .lock()
            .map(|s| s.strict_fallback)
            .unwrap_or(false)
    }

    pub fn update_rebalance_epoch(&self, epoch: u64) {
        if let Ok(mut state) = self.state.lock() {
            let changed = epoch > state.rebalance_epoch;
            state.rebalance_epoch = epoch;
            state.rebalance_started_at = Some(self.clock.now());
            if changed {
                audit::emit(
                    "rebalance_epoch",
                    "system",
                    "control-plane",
                    &format!("epoch {}", epoch),
                );
            }
        }
    }

    pub fn rebalance_grace_active(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.rebalance_epoch == 0 || state.rebalance_epoch <= state.routing_epoch.0 {
            state.rebalance_started_at = None;
            return false;
        }
        if let Some(start) = state.rebalance_started_at {
            if self.clock.now().duration_since(start) <= Duration::from_secs(600) {
                return true;
            }
            state.rebalance_started_at = None;
        }
        false
    }

    pub fn update_tenant_prg(&self, tenant_id: &str, prg_count: u64, epoch: RoutingEpoch) {
        if let Ok(mut state) = self.state.lock() {
            state
                .tenant_overrides
                .insert(tenant_id.to_string(), prg_count);
            state.routing_epoch = epoch;
            state.last_refresh = self.clock.now();
            state.cache_state = CacheState::Fresh;
        }
    }

    pub fn validate_placement_epoch(
        &self,
        prg: &PrgId,
        observed: RoutingEpoch,
    ) -> Result<(), ControlPlaneError> {
        if self.strict_fallback() {
            return Err(ControlPlaneError::StrictFallback);
        }
        if let Ok(mut placement) = self.placement.lock() {
            let key = format!("{}:{}", prg.tenant_id, prg.partition_index);
            match placement.validate_routing_epoch(&key, observed.0, self.clock.now()) {
                Ok(_) | Err(RoutingEpochError::UnknownPartition { .. }) => return Ok(()),
                Err(_) => {
                    self.set_strict_fallback(true);
                    return Err(ControlPlaneError::StaleEpoch);
                }
            }
        }
        Err(ControlPlaneError::StaleEpoch)
    }

    pub fn ensure_tenant_single_prg(&self, prg: &PrgId) -> Result<(), ControlPlaneError> {
        let state = self.state.lock().unwrap();
        let allowed = state
            .tenant_overrides
            .get(&prg.tenant_id)
            .cloned()
            .unwrap_or(state.tenant_prg_count);
        if prg.partition_index >= allowed {
            return Err(ControlPlaneError::StaleEpoch);
        }
        Ok(())
    }

    pub fn update_peer_version(&self, node_id: &str, minor: u8) {
        if let Ok(mut state) = self.state.lock() {
            state.peer_versions.insert(node_id.to_string(), minor);
            state.last_refresh = self.clock.now();
        }
    }

    pub fn apply_peer_versions(&self, peers: &HashMap<String, u8>) {
        if let Ok(mut state) = self.state.lock() {
            for (node, minor) in peers {
                state.peer_versions.insert(node.clone(), *minor);
            }
            state.last_refresh = self.clock.now();
        }
    }

    pub fn apply_placement_records(&self, records: HashMap<String, PlacementRecord>, now: Instant) {
        let mut placements = HashMap::new();
        let mut tenant_counts: HashMap<String, u64> = HashMap::new();
        let mut epoch = 0u64;
        let mut rebalance_epoch = 0u64;
        for record in records.values() {
            if let Some(prg) = parse_prg_id(&record.partition_id) {
                epoch = epoch.max(record.routing_epoch);
                rebalance_epoch = rebalance_epoch.max(record.lease_epoch);
                let node = record.members.first().cloned().unwrap_or_default();
                let replicas = if record.members.is_empty() {
                    vec![node.clone()]
                } else {
                    record.members.clone()
                };
                placements.insert(
                    prg.clone(),
                    PrgPlacement {
                        node_id: node,
                        replicas,
                    },
                );
                let entry = tenant_counts.entry(prg.tenant_id.clone()).or_insert(0);
                *entry = (*entry).max(prg.partition_index.saturating_add(1));
            }
        }
        if let Ok(mut state) = self.state.lock() {
            if rebalance_epoch > state.rebalance_epoch {
                state.rebalance_started_at = Some(now);
                audit::emit(
                    "rebalance_epoch",
                    "system",
                    "control-plane",
                    &format!("epoch {}", rebalance_epoch),
                );
            }
            state.rebalance_epoch = rebalance_epoch;
            if !placements.is_empty() {
                state.placements = placements;
            }
            for (tenant, count) in tenant_counts {
                state.tenant_overrides.insert(tenant, count);
            }
            if epoch > 0 {
                state.routing_epoch = RoutingEpoch(epoch);
            }
            state.cache_state = CacheState::Fresh;
            state.last_refresh = now;
            update_routing_metrics(&state.metrics, state.routing_epoch, state.placements.len());
            update_cache_metric(&state.metrics, state.cache_state);
        }
        let _ = self
            .routing_watch
            .send(self.state.lock().unwrap().routing_epoch);
    }

    /// Install auth material and ACLs from the control-plane snapshot.
    pub fn install_auth_bundle(
        &self,
        tenant_id: &str,
        certs: Vec<String>,
        tokens: Vec<String>,
        acl_prefixes: Vec<String>,
    ) {
        if let Ok(mut state) = self.state.lock() {
            state.tenant_certs.insert(tenant_id.to_string(), certs);
            state.tenant_tokens.insert(tenant_id.to_string(), tokens);
            state
                .tenant_acl_prefixes
                .insert(tenant_id.to_string(), acl_prefixes);
            state.last_refresh = self.clock.now();
        }
    }

    pub fn apply_quota_override(&self, tenant_id: &str, quotas: QuotaLimits) {
        if let Ok(mut state) = self.state.lock() {
            state.tenant_quotas.insert(tenant_id.to_string(), quotas);
            state.last_refresh = self.clock.now();
        }
    }

    // =========================================================================
    // Capability Registry Management
    // =========================================================================

    /// Get the current capability epoch.
    pub fn capability_epoch(&self) -> u64 {
        self.state.lock().map(|s| s.capability_epoch).unwrap_or(0)
    }

    /// Assign a protocol to a tenant with validation.
    pub fn assign_protocol(
        &self,
        tenant_id: &str,
        assignment: ProtocolAssignment,
    ) -> Result<(), CapabilityError> {
        if let Ok(mut state) = self.state.lock() {
            state.capabilities.assign_protocol(tenant_id, assignment)?;
            state.capability_epoch = state.capabilities.epoch();
            state.last_refresh = self.clock.now();
            audit::emit(
                "protocol_assigned",
                tenant_id,
                "control-plane",
                &format!("epoch {}", state.capability_epoch),
            );
        }
        Ok(())
    }

    /// Remove a protocol assignment from a tenant.
    pub fn remove_protocol(
        &self,
        tenant_id: &str,
        protocol: &ProtocolType,
    ) -> Result<(), CapabilityError> {
        if let Ok(mut state) = self.state.lock() {
            if let Some(tenant) = state.capabilities.get_tenant_mut(tenant_id) {
                tenant.remove_protocol(protocol);
                state.capability_epoch = state.capabilities.epoch();
                state.last_refresh = self.clock.now();
                audit::emit(
                    "protocol_removed",
                    tenant_id,
                    "control-plane",
                    &format!("protocol {} epoch {}", protocol, state.capability_epoch),
                );
                Ok(())
            } else {
                Err(CapabilityError::TenantNotFound(tenant_id.to_string()))
            }
        } else {
            Err(CapabilityError::TenantNotFound(tenant_id.to_string()))
        }
    }

    /// Check if a tenant has a specific protocol enabled.
    pub fn has_protocol(&self, tenant_id: &str, protocol: &ProtocolType) -> bool {
        self.state
            .lock()
            .ok()
            .and_then(|s| {
                s.capabilities
                    .get_tenant(tenant_id)
                    .map(|t| t.has_protocol(protocol))
            })
            .unwrap_or(false)
    }

    /// Check if a tenant has a specific feature enabled.
    pub fn has_feature(
        &self,
        tenant_id: &str,
        protocol: &ProtocolType,
        flag: &ProtocolFeatureFlag,
    ) -> bool {
        self.state
            .lock()
            .ok()
            .and_then(|s| {
                s.capabilities
                    .get_tenant(tenant_id)
                    .map(|t| t.has_feature(protocol, flag))
            })
            .unwrap_or(false)
    }

    /// Get tenant capabilities.
    pub fn get_tenant_capabilities(&self, tenant_id: &str) -> Option<TenantCapabilities> {
        self.state
            .lock()
            .ok()
            .and_then(|s| s.capabilities.get_tenant(tenant_id).cloned())
    }

    /// Get all tenants with a specific protocol.
    pub fn tenants_with_protocol(&self, protocol: &ProtocolType) -> Vec<String> {
        self.state
            .lock()
            .ok()
            .map(|s| {
                s.capabilities
                    .tenants_with_protocol(protocol)
                    .iter()
                    .map(|s| s.to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Register an available protocol in the catalog.
    pub fn register_protocol(&self, protocol: ProtocolType) {
        if let Ok(mut state) = self.state.lock() {
            state.capabilities.register_protocol(protocol);
            state.capability_epoch = state.capabilities.epoch();
        }
    }

    /// Check if a protocol is available in the catalog.
    pub fn is_protocol_available(&self, protocol: &ProtocolType) -> bool {
        self.state
            .lock()
            .ok()
            .map(|s| s.capabilities.is_protocol_available(protocol))
            .unwrap_or(false)
    }

    /// Apply bootstrap configuration to set up initial tenant protocols.
    pub fn apply_bootstrap(&self, bootstrap: &ControlPlaneBootstrap) {
        if let Ok(mut state) = self.state.lock() {
            // Register MQTT as an available protocol by default
            state.capabilities.register_protocol(ProtocolType::Mqtt);

            for tenant_cfg in &bootstrap.tenants {
                // Ensure tenant exists
                let _ = state
                    .capabilities
                    .get_or_create_tenant(&tenant_cfg.tenant_id);

                // Apply protocol assignments
                for proto_cfg in &tenant_cfg.protocols {
                    let assignment = proto_cfg.to_protocol_assignment();
                    if let Err(e) = state
                        .capabilities
                        .assign_protocol(&tenant_cfg.tenant_id, assignment)
                    {
                        tracing::warn!(
                            "failed to assign protocol {} to tenant {}: {:?}",
                            proto_cfg.protocol,
                            tenant_cfg.tenant_id,
                            e
                        );
                    }
                }

                // Apply global features
                for flag in tenant_cfg.parse_global_features() {
                    if let Some(tenant) = state.capabilities.get_tenant_mut(&tenant_cfg.tenant_id) {
                        tenant.add_global_feature(flag);
                    }
                }
            }

            state.capability_epoch = state.capabilities.epoch();
            tracing::info!(
                "applied bootstrap configuration: {} tenants, capability_epoch={}",
                bootstrap.tenants.len(),
                state.capability_epoch
            );
        }
    }

    /// Get protocol limits for a tenant.
    pub fn get_protocol_limits(
        &self,
        tenant_id: &str,
        protocol: &ProtocolType,
    ) -> Option<ProtocolLimits> {
        self.state.lock().ok().and_then(|s| {
            s.capabilities
                .get_tenant(tenant_id)
                .and_then(|t| t.get_protocol(protocol))
                .map(|a| a.limits.clone())
        })
    }

    /// Deactivate a tenant.
    pub fn deactivate_tenant(&self, tenant_id: &str) -> Result<(), CapabilityError> {
        if let Ok(mut state) = self.state.lock() {
            if let Some(tenant) = state.capabilities.get_tenant_mut(tenant_id) {
                tenant.deactivate();
                state.capability_epoch = state.capabilities.epoch();
                audit::emit("tenant_deactivated", tenant_id, "control-plane", "");
                Ok(())
            } else {
                Err(CapabilityError::TenantNotFound(tenant_id.to_string()))
            }
        } else {
            Err(CapabilityError::TenantNotFound(tenant_id.to_string()))
        }
    }

    /// Activate a tenant.
    pub fn activate_tenant(&self, tenant_id: &str) -> Result<(), CapabilityError> {
        if let Ok(mut state) = self.state.lock() {
            if let Some(tenant) = state.capabilities.get_tenant_mut(tenant_id) {
                tenant.activate();
                state.capability_epoch = state.capabilities.epoch();
                audit::emit("tenant_activated", tenant_id, "control-plane", "");
                Ok(())
            } else {
                Err(CapabilityError::TenantNotFound(tenant_id.to_string()))
            }
        } else {
            Err(CapabilityError::TenantNotFound(tenant_id.to_string()))
        }
    }

    // =========================================================================
    // Cache Invalidation and Digest Management
    // =========================================================================

    /// Get the current capability digest.
    pub fn capability_digest(&self) -> u64 {
        self.state.lock().map(|s| s.capability_digest).unwrap_or(0)
    }

    /// Get the current protocol revision.
    pub fn protocol_revision(&self) -> u64 {
        self.state.lock().map(|s| s.protocol_revision).unwrap_or(0)
    }

    /// Compute capability digest from current state.
    fn compute_capability_digest(capabilities: &CapabilityRegistry) -> u64 {
        use std::hash::{Hash, Hasher};
        use twox_hash::XxHash64;

        let mut hasher = XxHash64::with_seed(0);

        // Hash each tenant's protocol assignments
        let mut tenant_ids: Vec<_> = capabilities
            .tenant_ids()
            .iter()
            .map(|s| s.as_str())
            .collect();
        tenant_ids.sort();

        for tenant_id in tenant_ids {
            tenant_id.hash(&mut hasher);
            if let Some(tenant) = capabilities.get_tenant(tenant_id) {
                tenant.epoch.hash(&mut hasher);
                tenant.active.hash(&mut hasher);

                // Hash protocols
                let mut protocols: Vec<_> = tenant.protocols.keys().map(|p| p.as_str()).collect();
                protocols.sort();
                for protocol in protocols {
                    protocol.hash(&mut hasher);
                    if let Some(assignment) = tenant.protocols.get(&ProtocolType::from(protocol)) {
                        assignment.enabled.hash(&mut hasher);
                        assignment.priority.hash(&mut hasher);
                    }
                }
            }
        }

        hasher.finish()
    }

    /// Recompute and update the capability digest.
    pub fn update_capability_digest(&self) {
        if let Ok(mut state) = self.state.lock() {
            let old_digest = state.capability_digest;
            state.capability_digest = Self::compute_capability_digest(&state.capabilities);

            if old_digest != state.capability_digest && old_digest != 0 {
                // Digest changed - emit telemetry
                let _ = state
                    .metrics
                    .inc_counter("capability_digest_change_total", 1);
                audit::emit(
                    "capability_digest_changed",
                    "system",
                    "control-plane",
                    &format!("old={} new={}", old_digest, state.capability_digest),
                );
            }
        }
    }

    /// Check if caches need invalidation based on epoch/digest changes.
    pub fn check_cache_invalidation(
        &self,
        expected_capability_epoch: u64,
        expected_routing_epoch: u64,
        expected_digest: u64,
    ) -> Vec<routing_cache::InvalidationReason> {
        let mut reasons = Vec::new();

        if let Ok(state) = self.state.lock() {
            if expected_capability_epoch > 0 && state.capability_epoch != expected_capability_epoch
            {
                reasons.push(routing_cache::InvalidationReason::CapabilityEpochChanged);
            }

            if expected_routing_epoch > 0 && state.routing_epoch.0 != expected_routing_epoch {
                reasons.push(routing_cache::InvalidationReason::RoutingEpochChanged);
            }

            if expected_digest > 0 && state.capability_digest != expected_digest {
                reasons.push(routing_cache::InvalidationReason::DigestMismatch);
            }
        }

        reasons
    }

    /// Update epochs from external source (e.g., CP snapshot).
    pub fn update_epochs(
        &self,
        capability_epoch: Option<u64>,
        routing_epoch: Option<RoutingEpoch>,
        protocol_revision: Option<u64>,
    ) {
        if let Ok(mut state) = self.state.lock() {
            let mut changed = false;

            if let Some(epoch) = capability_epoch {
                if epoch > state.capability_epoch {
                    state.capability_epoch = epoch;
                    changed = true;
                }
            }

            if let Some(epoch) = routing_epoch {
                if epoch.0 > state.routing_epoch.0 {
                    state.routing_epoch = epoch;
                    changed = true;
                }
            }

            if let Some(revision) = protocol_revision {
                if revision > state.protocol_revision {
                    state.protocol_revision = revision;
                    changed = true;
                }
            }

            if changed {
                state.last_refresh = self.clock.now();
                // Recompute digest
                state.capability_digest = Self::compute_capability_digest(&state.capabilities);
            }
        }
    }

    /// Get all epochs for cache validation.
    pub fn get_epochs(&self) -> (u64, RoutingEpoch, u64, u64) {
        self.state
            .lock()
            .map(|s| {
                (
                    s.capability_epoch,
                    s.routing_epoch,
                    s.capability_digest,
                    s.protocol_revision,
                )
            })
            .unwrap_or((0, RoutingEpoch(0), 0, 0))
    }

    fn apply_feature_manifest(&self, manifest: &FeatureManifest) {
        for feature in &manifest.features {
            // Only attempt to parse structured tenant data if the gate payload looks like JSON.
            if feature.cp_object != "tenant_auth" && feature.cp_object != "tenant_quota" {
                continue;
            }
            let gate = feature.gate.trim();
            if !gate.starts_with('{') {
                continue;
            }
            match serde_json::from_str::<TenantFeature>(gate) {
                Ok(tf) => {
                    if tf.tenant_id.is_empty() {
                        tracing::debug!(
                            "skipping feature manifest entry with empty tenant_id cp_object={}",
                            feature.cp_object
                        );
                        continue;
                    }
                    if let Some(tokens) = tf.tokens.clone().filter(|t| !t.is_empty()) {
                        let certs = tf.certs.clone().unwrap_or_default();
                        let prefixes = tf.acl_prefixes.clone().unwrap_or_default();
                        self.install_auth_bundle(&tf.tenant_id, certs, tokens, prefixes);
                    }
                    if let Some(quotas) = tf.quotas.clone() {
                        self.apply_quota_override(&tf.tenant_id, quotas);
                    }
                }
                Err(err) => {
                    tracing::debug!(
                        "feature manifest entry cp_object={} ignored (parse error): {err:?}",
                        feature.cp_object
                    );
                }
            }
        }
    }
}

impl<C: Clock> std::fmt::Debug for ControlPlaneClient<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let endpoints = self
            .state
            .lock()
            .ok()
            .map(|s| s.endpoints.clone())
            .unwrap_or_default();
        f.debug_struct("ControlPlaneClient")
            .field("endpoints", &endpoints)
            .finish()
    }
}

fn record_publish_metrics(metrics: &SharedMetricsRegistry, proof: &DurabilityProof) {
    let _ = metrics.inc_counter("publish_total", 1);
    let _ = metrics.set_gauge("publish_term", proof.term);
    let _ = metrics.set_gauge("publish_index", proof.index);
}

fn record_commit_metrics(metrics: &SharedMetricsRegistry, index: u64) {
    let _ = metrics.inc_counter("commit_total", 1);
    let _ = metrics.set_gauge("commit_index", index);
}

fn update_cache_metric(metrics: &SharedMetricsRegistry, cache_state: CacheState) {
    let _ = metrics.set_gauge("cache_state", cache_state_gauge(cache_state));
}

fn cache_state_gauge(cache_state: CacheState) -> u64 {
    match cache_state {
        CacheState::Fresh => 0,
        CacheState::Stale => 1,
        CacheState::Expired => 2,
    }
}

fn update_routing_metrics(
    metrics: &SharedMetricsRegistry,
    epoch: RoutingEpoch,
    placement_count: usize,
) {
    let _ = metrics.set_gauge("routing_epoch", epoch.0);
    let _ = metrics.set_gauge("placement_count", placement_count as u64);
}

fn parse_prg_id(partition_id: &str) -> Option<PrgId> {
    let mut parts = partition_id.split(':');
    let tenant = parts.next()?;
    let idx = parts.next()?.parse().ok()?;
    Some(PrgId {
        tenant_id: tenant.to_string(),
        partition_index: idx,
    })
}

#[derive(Debug, Clone, Deserialize)]
struct TenantFeature {
    tenant_id: String,
    #[serde(default)]
    tokens: Option<Vec<String>>,
    #[serde(default)]
    certs: Option<Vec<String>>,
    #[serde(default)]
    acl_prefixes: Option<Vec<String>>,
    #[serde(default)]
    quotas: Option<QuotaLimits>,
}
