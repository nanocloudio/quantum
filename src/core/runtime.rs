use crate::audit;
use crate::config::{Config, ControlPlaneBootstrap, ControlPlaneMode, DrMirrorConfig};
use crate::control::embedded::EmbeddedCpState;
use crate::control::ControlPlaneClient;
use crate::dr::{BackupSchedule, DrManager, FilesystemDrMirror, ObjectStoreDrMirror};
use crate::flow::BackpressureState;
use crate::listeners::Listeners;
use crate::net::security::SecurityManager;
use crate::ops::{FaultInjector, VersionGate};
use crate::prg::{PrgManager, PrgManagerInputs};
use crate::replication::consensus::AckContract;
use crate::replication::network::{RaftNetConfig, RaftNetResources};
use crate::routing::{RoutingEpoch, RoutingTable};
use crate::storage::ClustorStorage;
use crate::telemetry;
use crate::telemetry::LogHandle;
use crate::time::Clock;
use anyhow::{anyhow, Context, Result};
use clustor::control_plane::HttpCpTransportBuilder;
use clustor::net::tls::{load_identity_from_pem, load_trust_store_from_pem};
use clustor::security::MtlsIdentityManager;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::sync::Mutex;

/// Unified runtime scaffold: wires control-plane client, PRG hosts, listeners, metrics, and shutdown.
pub struct Runtime<C: Clock> {
    pub(crate) config: Config,
    pub(crate) clock: C,
    control_plane: ControlPlaneClient<C>,
    prg_manager: PrgManager<C>,
    ack_contract: AckContract,
    dr_manager: DrManager<C>,
    metrics: Arc<BackpressureState>,
    security: SecurityManager<C>,
    cp_bootstrap: ControlPlaneBootstrap,
    cp_storage_root: PathBuf,
    cp_server_addr: Option<SocketAddr>,
    cp_server: Option<tokio::task::JoinHandle<()>>,
    fault_injector: FaultInjector,
    version_gate: VersionGate,
    sessions: Arc<Mutex<HashMap<String, crate::workloads::mqtt::session::SessionState>>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    log_handle: Option<LogHandle>,
}

impl<C: Clock> Runtime<C> {
    pub fn new(config: Config, clock: C, log_handle: Option<LogHandle>) -> Result<Self> {
        config.validate()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        audit::install_sink(audit::AuditSink::default());
        let metrics = Arc::new(BackpressureState::default());
        let fault_injector = FaultInjector::default();
        let ack_contract =
            AckContract::new(&config.durability).with_fault_injector(fault_injector.clone());
        let version_gate = VersionGate::new(0);
        let control_plane = ControlPlaneClient::new(
            config.control_plane.clone(),
            config.tenants.tenant_prg_count,
            clock.clone(),
        )?;
        control_plane.set_commit_visibility(ack_contract.cp_commit_visibility());
        if let Some(peers) = &config.control_plane.peer_versions {
            control_plane.apply_peer_versions(peers);
        }
        let routing_table = RoutingTable::empty(control_plane.routing_epoch());
        let storage_dir = config
            .paths
            .storage_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("data"));
        let cp_storage_root = storage_dir.join("cp");
        let storage_backend = ClustorStorage::new(storage_dir.clone());
        let raft_net = build_raft_net_config(&config, &storage_dir);
        let prg_manager = PrgManager::new(
            routing_table,
            clock.clone(),
            PrgManagerInputs {
                control_plane: control_plane.clone(),
                ack_contract: Arc::new(ack_contract.clone()),
                durability_cfg: config.durability.clone(),
                raft_net,
                metrics: metrics.clone(),
                version_gate,
                storage_backend: storage_backend.clone(),
                forward_plane: config.forward_plane.clone(),
            },
        );
        let quotas = config.tenants.quotas.clone().unwrap_or_default();
        let security = SecurityManager::new(control_plane.clone(), quotas);
        let dr_manager = DrManager::new(
            clock.clone(),
            ack_contract.clone(),
            metrics.clone(),
            storage_backend.clone(),
        );
        dr_manager.set_control_plane(control_plane.clone());
        let mut schedule = BackupSchedule::default();
        if let Some(cp) = config.dr.checkpoint_interval_seconds {
            schedule.checkpoint_secs = cp;
        }
        if let Some(wal) = config.dr.wal_interval_seconds {
            schedule.wal_archive_secs = wal;
        }
        dr_manager.schedule_backups(schedule);
        if let Some(cert) = config.dr.region_certificate.clone() {
            dr_manager.set_expected_region_certificate(Some(cert));
        }
        if let Some(mirror) = &config.dr.mirror {
            match mirror {
                DrMirrorConfig::Filesystem { path } => {
                    dr_manager.install_mirror(Arc::new(FilesystemDrMirror::new(path.clone())));
                }
                DrMirrorConfig::ObjectStore { bucket, prefix } => {
                    dr_manager.install_mirror(Arc::new(ObjectStoreDrMirror::new(
                        bucket.clone(),
                        prefix.clone(),
                    )));
                }
            }
        }
        let cp_bootstrap = config
            .control_plane
            .bootstrap_seeds(&config.durability.replica_id);
        Ok(Self {
            config,
            clock,
            control_plane,
            prg_manager,
            ack_contract,
            dr_manager,
            metrics,
            security,
            cp_bootstrap,
            cp_storage_root,
            cp_server_addr: None,
            cp_server: None,
            fault_injector,
            version_gate,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            shutdown_tx,
            shutdown_rx,
            log_handle,
        })
    }

    /// Start the runtime: initialize control-plane cache, start listeners, and wait for shutdown.
    pub async fn run(&mut self) -> Result<()> {
        let _started_at = self.clock.now();
        self.init_control_plane().await?;
        self.prg_manager.start().await?;
        self.dr_manager.start_shipping().await;
        self.start_telemetry().await?;
        self.start_listeners().await?;
        self.handle_shutdown().await
    }

    async fn init_control_plane(&mut self) -> Result<()> {
        // Apply bootstrap configuration to set up initial tenant protocols
        self.control_plane.apply_bootstrap(&self.cp_bootstrap);
        self.configure_control_plane_transport().await?;
        self.control_plane.start().await
    }

    async fn configure_control_plane_transport(&mut self) -> Result<()> {
        match self.config.control_plane.mode {
            ControlPlaneMode::External => self.configure_external_control_plane(),
            ControlPlaneMode::Embedded => self.configure_embedded_control_plane().await,
        }
    }

    pub fn control_plane_cache_fresh(&self) -> bool {
        self.control_plane.cache_is_fresh()
    }

    /// Surface control-plane read-gate signals.
    pub fn ingest_read_gate(&self, ok: bool) {
        self.control_plane.ingest_read_gate(ok);
    }

    /// Ingest peer version data from the control plane watcher.
    pub fn ingest_peer_versions(&self, peers: std::collections::HashMap<String, u8>) {
        self.control_plane.apply_peer_versions(&peers);
    }

    pub fn security(&self) -> &SecurityManager<C> {
        &self.security
    }

    pub fn control_plane_bootstrap(&self) -> ControlPlaneBootstrap {
        self.cp_bootstrap.clone()
    }

    pub fn metrics(&self) -> Arc<BackpressureState> {
        self.metrics.clone()
    }

    pub fn ack_contract(&self) -> &AckContract {
        &self.ack_contract
    }

    /// Ingest clustor-provided backpressure metrics.
    pub fn ingest_clustor_metrics(&self, metrics: crate::flow::ClustorBackpressure) {
        self.metrics.ingest_clustor_metrics(metrics);
        self.ack_contract
            .set_replication_lag(metrics.replication_lag_seconds);
    }

    pub fn dr_manager(&self) -> &DrManager<C> {
        &self.dr_manager
    }

    pub fn fault_injector(&self) -> &FaultInjector {
        &self.fault_injector
    }

    pub fn version_gate(&self) -> &VersionGate {
        &self.version_gate
    }
    pub fn prg_manager(&self) -> PrgManager<C> {
        self.prg_manager.clone()
    }

    pub fn prg_ready(&self) -> bool {
        self.prg_manager.ready()
    }

    /// Composite readiness across CP cache freshness, PRG readiness, and durability fence.
    pub fn ready(&self) -> bool {
        let thresholds = self.config.tenants.thresholds.clone().unwrap_or_default();
        let backpressure = self.metrics.snapshot();
        let lag_ok = backpressure.apply_lag_seconds < thresholds.max_apply_lag_seconds_ready
            && backpressure.replication_lag_seconds < thresholds.max_replication_lag_seconds_ready;
        let queue_ok = backpressure.queue_depth_apply < thresholds.commit_to_apply_pause_ack;
        let cp_server_ok = self.config.control_plane.mode == ControlPlaneMode::External
            || self.config.control_plane.mode == ControlPlaneMode::Embedded
            || self.cp_server_addr.is_some();
        self.control_plane_cache_fresh()
            && self.prg_ready()
            && !self.ack_contract.durability_fence_active()
            && !self.dr_manager.uncontrolled()
            && self.dr_manager.cp_observer_ready()
            && lag_ok
            && queue_ok
            && cp_server_ok
    }

    pub fn control_plane(&self) -> ControlPlaneClient<C> {
        self.control_plane.clone()
    }

    pub fn clock(&self) -> C {
        self.clock.clone()
    }

    pub fn sessions(
        &self,
    ) -> Arc<Mutex<HashMap<String, crate::workloads::mqtt::session::SessionState>>> {
        self.sessions.clone()
    }

    pub fn strict_fallback(&self) -> bool {
        self.control_plane.strict_fallback()
    }

    pub async fn sessions_len(&self) -> usize {
        self.sessions.lock().await.len()
    }

    pub async fn register_session(
        &self,
        tenant_id: &str,
        client_id: &str,
        clean_start: bool,
    ) -> (bool, crate::workloads::mqtt::session::SessionState) {
        let key = format!("{tenant_id}:{client_id}");
        let mut guard = self.sessions.lock().await;
        let existed = guard.contains_key(&key);
        let entry =
            guard
                .entry(key)
                .or_insert_with(|| crate::workloads::mqtt::session::SessionState {
                    client_id: client_id.to_string(),
                    tenant_id: tenant_id.to_string(),
                    session_epoch: 0,
                    inflight: HashMap::new(),
                    dedupe_floor_index: 0,
                    offline_floor_index: 0,
                    status: crate::workloads::mqtt::session::SessionStatus::Connected,
                    earliest_offline_queue_index: 0,
                    offline: crate::offline::OfflineQueue::default(),
                    keep_alive: 0,
                    session_expiry_interval: None,
                    last_packet_at: Some(self.clock.now()),
                    last_packet_at_wall: Some(SystemTime::now()),
                    will: None,
                    outbound: HashMap::new(),
                    outbound_tx: None,
                    dedupe_entries: Vec::new(),
                    forward_progress: Vec::new(),
                });
        if clean_start && existed {
            entry.session_epoch = entry.session_epoch.saturating_add(1);
            entry.inflight.clear();
            entry.offline = crate::offline::OfflineQueue::default();
            entry.earliest_offline_queue_index = 0;
            entry.dedupe_floor_index = 0;
            entry.offline_floor_index = 0;
            entry.last_packet_at = Some(self.clock.now());
            entry.last_packet_at_wall = Some(SystemTime::now());
            entry.dedupe_entries.clear();
            entry.forward_progress.clear();
        }
        (existed, entry.clone())
    }

    /// Expose routing epoch checker for edge/listener usage.
    pub async fn ensure_routing_epoch(
        &self,
        expected: RoutingEpoch,
    ) -> Result<(), crate::routing::RoutingError> {
        self.prg_manager.check_epoch(expected).await
    }

    pub fn log_handle(&self) -> Option<LogHandle> {
        self.log_handle.clone()
    }

    pub fn backup_schedule(&self) -> BackupSchedule {
        self.dr_manager.backup_schedule()
    }

    /// Await the clustor ACK contract for a durability proof.
    pub async fn wait_for_ack(&self) -> Result<u64> {
        self.ack_contract.wait_for_commit().await
    }

    async fn start_listeners(&self) -> Result<()> {
        tracing::info!("initializing listeners");
        Listeners::start(self).await
    }

    pub async fn start_telemetry(&self) -> Result<()> {
        if let Some(bind) = &self.config.telemetry.metrics_bind {
            let runtime = Arc::new(self.clone_runtime());
            telemetry::start_http(bind, runtime, self.log_handle()).await?;
        }
        Ok(())
    }

    fn clone_runtime(&self) -> Self {
        Self {
            config: self.config.clone(),
            clock: self.clock.clone(),
            control_plane: self.control_plane.clone(),
            prg_manager: self.prg_manager.clone(),
            ack_contract: self.ack_contract.clone(),
            dr_manager: self.dr_manager.clone(),
            metrics: self.metrics.clone(),
            security: self.security.clone(),
            cp_bootstrap: self.cp_bootstrap.clone(),
            cp_storage_root: self.cp_storage_root.clone(),
            cp_server_addr: self.cp_server_addr,
            cp_server: None,
            fault_injector: self.fault_injector.clone(),
            version_gate: self.version_gate,
            sessions: self.sessions.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            log_handle: self.log_handle.clone(),
        }
    }

    async fn handle_shutdown(&mut self) -> Result<()> {
        // Graceful shutdown hook: wait for CTRL+C then notify subsystems to drain.
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::warn!("shutdown signal received");
            }
            _ = self.shutdown_rx.changed() => {
                tracing::info!("shutdown requested by component");
            }
        }
        self.shutdown_tx
            .send(true)
            .context("failed to broadcast shutdown")?;
        self.drain().await?;
        Ok(())
    }

    /// Test helper to start control-plane and PRGs without listeners/telemetry.
    pub async fn start_for_tests(&mut self) -> Result<()> {
        self.init_control_plane().await?;
        self.prg_manager.start().await?;
        self.dr_manager.start_shipping().await;
        Ok(())
    }

    /// Test helper to stop background tasks without waiting for SIGINT.
    pub async fn shutdown_for_tests(&mut self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.cp_server.take() {
            handle.abort();
        }
        Ok(())
    }

    async fn drain(&mut self) -> Result<()> {
        tracing::info!("draining runtime components for upgrade/shutdown");
        self.prg_manager.prepare_upgrade().await?;
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.cp_server.take() {
            if let Err(err) = handle.await {
                tracing::warn!("embedded control-plane server shutdown failed: {err:?}");
            }
        }
        Ok(())
    }

    /// Hot-reload configuration only when CP cache is fresh.
    pub fn hot_reload(&mut self, new_config: Config) -> Result<()> {
        if self.control_plane.require_fresh_read().is_err() {
            anyhow::bail!("control-plane cache stale; hot-reload blocked");
        }
        if !self
            .version_gate
            .compatible(self.version_gate.current_minor())
        {
            anyhow::bail!("version skew exceeds 1 minor");
        }
        // Shallow replace supported fields.
        self.config.telemetry = new_config.telemetry;
        Ok(())
    }

    fn configure_external_control_plane(&self) -> Result<()> {
        if self.config.control_plane.endpoints.is_empty() {
            anyhow::bail!("control_plane endpoints missing");
        }
        let (chain_path, key_path) = control_plane_identity_paths(&self.config);
        let trust_path = control_plane_trust_path(&self.config)
            .ok_or_else(|| anyhow!("control-plane trust bundle not configured"))?;
        let now = self.clock.now();
        let identity = load_identity_from_pem(chain_path, key_path, now)
            .context("load control-plane identity material")?;
        let trust_store =
            load_trust_store_from_pem(&trust_path).context("load control-plane trust bundle")?;
        let base_url = cp_base_url(
            self.config
                .control_plane
                .endpoints
                .first()
                .expect("control-plane endpoints checked"),
        );
        let transport = HttpCpTransportBuilder::new(&base_url)
            .context("build control-plane transport")?
            .identity(identity.clone())
            .trust_store(trust_store)
            .connection_pool(4)
            .build()
            .context("construct control-plane transport")?;
        let trust_domain = identity.certificate.spiffe_id.trust_domain.clone();
        let mut mtls = MtlsIdentityManager::new(
            identity.certificate.clone(),
            trust_domain.clone(),
            Duration::from_secs(self.config.control_plane.cache_ttl_seconds.max(1)),
            now,
        );
        mtls.set_revocation_enforcement(self.config.control_plane.require_revocation_feeds);
        self.control_plane.configure_cp_client(transport, mtls);
        Ok(())
    }

    async fn configure_embedded_control_plane(&mut self) -> Result<()> {
        let bind = self.config.control_plane.embedded_http_bind_or_default();
        let (chain_path, key_path) = control_plane_identity_paths(&self.config);
        let trust_path = control_plane_trust_path(&self.config)
            .ok_or_else(|| anyhow!("no TLS trust bundle available for embedded control-plane"))?;
        let now = self.clock.now();
        let identity = load_identity_from_pem(chain_path, key_path, now)
            .context("load embedded control-plane identity material")?;
        let trust_store = load_trust_store_from_pem(&trust_path)
            .context("load embedded control-plane trust bundle")?;
        let trust_domain = identity.certificate.spiffe_id.trust_domain.clone();
        let state = EmbeddedCpState::load_or_bootstrap(
            &self.cp_storage_root,
            self.cp_bootstrap.clone(),
            self.config.durability.replica_id.clone(),
            self.config.control_plane.cache_ttl_seconds,
        )?;
        let mut mtls = MtlsIdentityManager::new(
            identity.certificate.clone(),
            trust_domain.clone(),
            Duration::from_secs(self.config.control_plane.cache_ttl_seconds.max(1)),
            now,
        );
        mtls.set_revocation_enforcement(self.config.control_plane.require_revocation_feeds);
        let shutdown = self.shutdown_rx.clone();
        let (handle, addr) = crate::control::embedded::start_embedded_cp_server(
            &bind,
            chain_path.clone(),
            key_path.clone(),
            trust_path,
            state,
            shutdown,
        )
        .await?;
        self.cp_server = Some(handle);
        self.cp_server_addr = Some(addr);
        let base_url = format!("https://{}", addr);
        let transport = HttpCpTransportBuilder::new(&base_url)
            .context("build embedded control-plane transport")?
            .identity(identity.clone())
            .trust_store(trust_store)
            .connection_pool(2)
            .build()
            .context("construct embedded control-plane transport")?;
        self.control_plane.configure_cp_client(transport, mtls);
        Ok(())
    }
}

fn build_raft_net_config(cfg: &Config, storage_dir: &Path) -> Option<RaftNetResources> {
    let trust_bundle = control_plane_trust_path(cfg)?;
    let trust_domain = std::env::var("RAFT_TRUST_DOMAIN").unwrap_or_else(|_| "local".to_string());
    let log_dir = storage_dir.join("raft").join("prg");
    let bind = std::env::var("RAFT_BIND").ok().or_else(|| {
        if cfg.control_plane.mode == ControlPlaneMode::Embedded {
            Some(cfg.control_plane.embedded_raft_bind_or_default())
        } else {
            None
        }
    });
    Some(RaftNetResources::new(RaftNetConfig {
        local_id: cfg.durability.replica_id.clone(),
        bind,
        tls_chain_path: cfg.listeners.tcp.tls_chain_path.clone(),
        tls_key_path: cfg.listeners.tcp.tls_key_path.clone(),
        trust_bundle: trust_bundle.clone(),
        trust_domain,
        log_dir,
    }))
}

fn control_plane_identity_paths(cfg: &Config) -> (&PathBuf, &PathBuf) {
    if let Some(tls) = &cfg.control_plane.embedded_tls {
        (&tls.tls_chain_path, &tls.tls_key_path)
    } else {
        (
            &cfg.listeners.tcp.tls_chain_path,
            &cfg.listeners.tcp.tls_key_path,
        )
    }
}

fn control_plane_trust_path(cfg: &Config) -> Option<PathBuf> {
    if let Some(tls) = &cfg.control_plane.embedded_tls {
        if let Some(ca) = &tls.client_ca_path {
            return Some(ca.clone());
        }
    }
    if let Some(bundle) = &cfg.paths.cp_certs {
        Some(bundle.clone())
    } else {
        Some(cfg.listeners.tcp.client_ca_path.clone())
    }
}

fn cp_base_url(endpoint: &str) -> String {
    if endpoint.starts_with("https://") || endpoint.starts_with("http://") {
        endpoint.to_string()
    } else {
        format!("https://{endpoint}")
    }
}
