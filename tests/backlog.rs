use clustor::consensus::DurabilityProof;
use clustor::control_plane::core::client::{
    CpApiTransport, CpClientError, TransportError, TransportResponse,
};
use clustor::cp_raft::PlacementRecord;
use clustor::durability::IoMode;
use clustor::raft::ReplicaId;
use clustor::replication::consensus::DurabilityProof as CpDurabilityProof;
use clustor::security::{Certificate, MtlsIdentityManager, SerialNumber, SpiffeId};
use futures::executor::block_on;
use futures_util::FutureExt;
use quantum::clustor_client::ClustorClient;
use quantum::config::{
    CommitVisibility, Config, ControlPlaneConfig, DrConfig, DurabilityConfig, DurabilityMode,
    ForwardPlaneConfig, ListenerConfig, PathConfig, QuicConfig, TcpConfig, TelemetryConfig,
    TenantConfig,
};
use quantum::control::ControlPlaneClient;
use quantum::dr::{CheckpointExport, DrMirror, DrPromotion, WalExport};
use quantum::flow::{BackpressureState, ClustorBackpressure};
use quantum::forwarding::{ForwardRequest, ForwardingEngine};
use quantum::mqtt::{read_connect, read_packet, ControlPacket, InternalCode, ProtocolVersion};
use quantum::offline::{OfflineEntry, OfflineQueue};
use quantum::ops::{FaultInjector, VersionGate};
use quantum::prg::{PersistedPrgState, PersistedSessionState, PrgManager, PrgManagerInputs};
use quantum::raft::AckContract;
use quantum::routing::{ForwardSeqTracker, PrgId, RoutingEpoch};
use quantum::runtime::Runtime;
use quantum::security::{AuthContext, SecurityError, SecurityManager};
use quantum::session::{
    OutboundMessage, OutboundStage, OutboundTracking, SessionProcessor, SessionState, SessionStatus,
};
use quantum::subscriptions::SharedSubscriptionBalancer;
use quantum::time::{Clock, SystemClock};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use tempfile::tempdir;

fn test_config() -> Config {
    Config {
        control_plane: ControlPlaneConfig {
            endpoints: vec!["localhost:9000".into()],
            cache_ttl_seconds: 0,
            peer_versions: None,
            ..Default::default()
        },
        listeners: ListenerConfig {
            tcp: TcpConfig {
                bind: "127.0.0.1:0".into(),
                tls_chain_path: PathBuf::from("chain.pem"),
                tls_key_path: PathBuf::from("key.pem"),
                client_ca_path: PathBuf::from("ca.pem"),
                alpn: vec!["mqtt".into()],
                require_sni: true,
                require_alpn: true,
                default_tenant: None,
            },
            quic: Some(QuicConfig {
                bind: "127.0.0.1:0".into(),
                tls_chain_path: PathBuf::from("chain.pem"),
                tls_key_path: PathBuf::from("key.pem"),
                client_ca_path: PathBuf::from("ca.pem"),
                alpn: vec!["mqtt-quic".into()],
                max_idle_timeout_seconds: 1,
                max_ack_delay_ms: 1,
                require_sni: true,
                require_alpn: true,
                default_tenant: None,
            }),
        },
        durability: DurabilityConfig {
            durability_mode: DurabilityMode::Strict,
            commit_visibility: CommitVisibility::DurableOnly,
            ..Default::default()
        },
        telemetry: TelemetryConfig {
            metrics_bind: None,
            log_level: None,
        },
        tenants: TenantConfig {
            max_tenants: None,
            quotas: None,
            tenant_prg_count: 1,
            thresholds: None,
        },
        paths: PathConfig {
            cp_certs: None,
            telemetry_catalog: None,
            storage_dir: None,
        },
        dr: DrConfig::default(),
        forward_plane: ForwardPlaneConfig::default(),
    }
}

fn new_prg_manager(
    table: quantum::routing::RoutingTable,
    clock: SystemClock,
    cp: ControlPlaneClient<SystemClock>,
    ack: Arc<AckContract>,
    durability: DurabilityConfig,
    metrics: Arc<BackpressureState>,
    storage: quantum::storage::ClustorStorage,
) -> PrgManager<SystemClock> {
    PrgManager::new(
        table,
        clock,
        PrgManagerInputs {
            control_plane: cp,
            ack_contract: ack,
            durability_cfg: durability,
            raft_net: None,
            metrics,
            version_gate: VersionGate::new(0),
            storage_backend: storage,
            forward_plane: ForwardPlaneConfig::default(),
        },
    )
}

fn durability_for_cp(
    mut durability: DurabilityConfig,
    cp: &ControlPlaneClient<SystemClock>,
) -> DurabilityConfig {
    if durability.replica_id == "local" {
        if let Some(id) = cp
            .routing_snapshot()
            .placements
            .values()
            .next()
            .map(|p| p.node_id.clone())
        {
            durability.replica_id = id;
        }
    }
    durability
}

fn cp_certificate(spiffe: &str, now: Instant) -> Certificate {
    Certificate {
        spiffe_id: SpiffeId::parse(spiffe).unwrap(),
        serial: SerialNumber::from_u64(1),
        valid_from: now - Duration::from_secs(5),
        valid_until: now + Duration::from_secs(60),
    }
}

struct TestCpTransport {
    responses: HashMap<String, TransportResponse>,
}

impl TestCpTransport {
    fn with_routing(records: Vec<PlacementRecord>, now: Instant) -> Self {
        let body = serde_json::to_vec(&records).unwrap();
        let mut responses = HashMap::new();
        responses.insert(
            "/routing".into(),
            TransportResponse {
                body,
                server_certificate: cp_certificate("spiffe://cp.internal/nodes/1", now),
            },
        );
        Self { responses }
    }
}

impl CpApiTransport for TestCpTransport {
    fn get(&self, path: &str) -> Result<TransportResponse, CpClientError> {
        self.responses.get(path).cloned().ok_or_else(|| {
            CpClientError::Transport(TransportError::NoRoute {
                path: path.to_string(),
            })
        })
    }
}

#[test]
fn deterministic_replay_floor_matches() {
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let clock = SystemClock;
    let mut proc_a = SessionProcessor::new(clock.clone(), ack.clone());
    let mut proc_b = SessionProcessor::new(clock, ack);
    let mut state_a = SessionState {
        client_id: "c".into(),
        tenant_id: "t".into(),
        session_epoch: 1,
        inflight: HashMap::new(),
        dedupe_floor_index: 0,
        offline_floor_index: 0,
        status: SessionStatus::Connected,
        earliest_offline_queue_index: 0,
        offline: OfflineQueue::default(),
        keep_alive: 0,
        session_expiry_interval: None,
        last_packet_at: Some(Instant::now()),
        last_packet_at_wall: Some(SystemTime::now()),
        will: None,
        outbound: HashMap::new(),
        outbound_tx: None,
        dedupe_entries: Vec::new(),
        forward_progress: Vec::new(),
    };
    let mut state_b = state_a.clone();
    proc_a.record_dedupe(&mut state_a, 1, quantum::dedupe::Direction::Publish, 7);
    proc_b.record_dedupe(&mut state_b, 1, quantum::dedupe::Direction::Publish, 7);
    let floor_a = proc_a.product_floors(&state_a, 5).effective_product_floor();
    let floor_b = proc_b.product_floors(&state_b, 5).effective_product_floor();
    assert_eq!(floor_a, floor_b);
}

#[test]
fn forward_seq_monotonic_and_stale() {
    let mut tracker = ForwardSeqTracker::default();
    let epoch = RoutingEpoch(1);
    let first = tracker.next_seq("s", "t", epoch);
    let second = tracker.next_seq("s", "t", epoch);
    assert!(second > first);
    assert!(tracker.is_stale("s", "t", epoch, first));
}

#[test]
fn security_blocks_when_cp_stale() {
    let cfg = ControlPlaneConfig {
        endpoints: vec!["localhost:9000".into()],
        cache_ttl_seconds: 0,
        peer_versions: None,
        ..Default::default()
    };
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    cp.mark_stale();
    let sec = SecurityManager::new(cp, Default::default());
    assert!(sec.authorize().is_err());
}

#[test]
fn session_clean_start_fences_epoch() {
    let cfg = test_config();
    let clock = SystemClock;
    let runtime = Runtime::new(cfg, clock, None).unwrap();
    let (_, s1) = runtime
        .register_session("t", "c", false)
        .now_or_never()
        .unwrap();
    assert_eq!(s1.session_epoch, 0);
    let (_, s2) = runtime
        .register_session("t", "c", true)
        .now_or_never()
        .unwrap();
    assert_eq!(s2.session_epoch, 1);
}

#[test]
fn receive_max_hint_reuses_window() {
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let mut processor = SessionProcessor::new(SystemClock, ack);
    let first = processor.receive_max_hint(0.5);
    let second = processor.receive_max_hint(0.1);
    assert_eq!(first, second);
}

#[test]
fn version_gate_enforces_minor_skew() {
    let gate = VersionGate::new(3);
    assert!(gate.compatible(4));
    assert!(!gate.compatible(6));
}

#[test]
fn fault_injector_toggles() {
    let injector = FaultInjector::default();
    injector.enable_election_delay(true);
    injector.enable_fsync_slow(true);
    injector.enable_kms_outage(true);
    injector.set_latency_ms(10);
    assert!(injector.election_delay());
    assert!(injector.fsync_slow());
    assert!(injector.kms_outage());
    assert_eq!(injector.latency_ms(), 10);
}

#[test]
fn prg_prepare_upgrade_stub() {
    let clock = SystemClock;
    let cfg = ControlPlaneConfig {
        endpoints: vec!["localhost:9000".into()],
        cache_ttl_seconds: 1,
        peer_versions: None,
        ..Default::default()
    };
    let cp = ControlPlaneClient::new(cfg, 1, clock.clone()).unwrap();
    let table = quantum::routing::RoutingTable::empty(RoutingEpoch(0));
    let durability = durability_for_cp(
        DurabilityConfig {
            durability_mode: DurabilityMode::Strict,
            commit_visibility: CommitVisibility::DurableOnly,
            ..Default::default()
        },
        &cp,
    );
    let ack = Arc::new(AckContract::new(&durability));
    let metrics = Arc::new(BackpressureState::default());
    let storage_dir = tempdir().unwrap();
    let storage = quantum::storage::ClustorStorage::new(storage_dir.path());
    let storage_handle = storage.clone();
    let prg = new_prg_manager(
        table,
        clock.clone(),
        cp,
        ack.clone(),
        durability,
        metrics,
        storage,
    );
    block_on(prg.refresh_routing_table()).unwrap();
    let topic_prg = prg.topic_prg("tenant-a", "tenant-a/topic");
    block_on(prg.commit_publish(
        &topic_prg,
        "tenant-a/topic",
        b"payload",
        quantum::mqtt::Qos::AtLeastOnce,
        false,
    ))
    .unwrap();
    block_on(prg.prepare_upgrade()).unwrap();
    assert!(ack.durability_fence_active());
    assert!(block_on(storage_handle.last_index(&topic_prg)).unwrap() > 0);
}

#[test]
fn dr_manager_flags() {
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let metrics = Arc::new(BackpressureState::default());
    let dir = tempdir().unwrap();
    let storage = quantum::storage::ClustorStorage::new(dir.path());
    let dr = quantum::dr::DrManager::new(SystemClock, ack, metrics, storage);
    block_on(dr.start_shipping());
    dr.update_lag(3);
    assert_eq!(dr.lag_seconds(), 3);
    dr.uncontrolled_promotion();
    assert!(dr.uncontrolled());
}

#[test]
fn dr_shipping_records_last_index() {
    #[derive(Clone, Default)]
    struct TestMirror {
        checkpoints: Arc<Mutex<Vec<CheckpointExport>>>,
        wals: Arc<Mutex<Vec<WalExport>>>,
    }
    impl DrMirror for TestMirror {
        fn ship_checkpoint(&self, export: CheckpointExport) {
            self.checkpoints.lock().unwrap().push(export);
        }
        fn ship_wal(&self, export: WalExport) {
            self.wals.lock().unwrap().push(export);
        }
        fn restore(&self, _prg: &PrgId, _committed_index: u64) {}
        fn promotion(&self, _mode: DrPromotion, _certificate: Option<String>) {}
    }
    let dir = tempdir().unwrap();
    let storage = quantum::storage::ClustorStorage::new(dir.path());
    let prg = PrgId {
        tenant_id: "tenant".into(),
        partition_index: 0,
    };
    block_on(storage.persist(&prg, &PersistedPrgState::default())).unwrap();
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let metrics = Arc::new(BackpressureState::default());
    let dr = quantum::dr::DrManager::new(SystemClock, ack, metrics, storage.clone());
    let mirror = TestMirror::default();
    dr.install_mirror(Arc::new(mirror.clone()));
    block_on(dr.ship_once()).unwrap();
    let shipped = dr.shipping_snapshot();
    assert_eq!(shipped.get("tenant:0").map(|s| s.last_index), Some(1));
    assert!(!mirror.checkpoints.lock().unwrap().is_empty());
}

#[test]
fn control_plane_seeds_peer_versions() {
    let mut peers = HashMap::new();
    peers.insert("node-a".to_string(), 2u8);
    let cfg = ControlPlaneConfig {
        endpoints: vec!["node-a".into()],
        cache_ttl_seconds: 0,
        peer_versions: Some(peers.clone()),
        ..Default::default()
    };
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    let snapshot = cp.snapshot();
    assert_eq!(snapshot.peer_versions.get("node-a"), peers.get("node-a"));
}

#[test]
fn clustor_wal_replay_restores_prg_state() {
    let dir = tempdir().unwrap();
    let cfg = test_config();
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg.control_plane.clone(), 1, clock.clone()).unwrap();
    cp.update_tenant_prg("tenant-a", 1, RoutingEpoch(1));
    let durability = durability_for_cp(cfg.durability.clone(), &cp);
    let ack = Arc::new(AckContract::new(&durability));
    let metrics = Arc::new(BackpressureState::default());
    let storage = quantum::storage::ClustorStorage::new(dir.path());
    let table = quantum::routing::RoutingTable::empty(RoutingEpoch(0));
    let prg_id = PrgId {
        tenant_id: "default".into(),
        partition_index: 0,
    };

    let prg = new_prg_manager(
        table.clone(),
        clock.clone(),
        cp.clone(),
        ack.clone(),
        durability.clone(),
        metrics.clone(),
        storage.clone(),
    );
    block_on(prg.refresh_routing_table()).unwrap();
    let session_state = PersistedSessionState {
        client_id: "client-a".into(),
        dedupe_floor: 5,
        offline_floor: 3,
        earliest_offline: 2,
        forward_chain_floor: 9,
        effective_product_floor: 0,
        keep_alive: 0,
        session_expiry_interval: Some(30),
        session_epoch: 0,
        inflight: Vec::new(),
        outbound: Vec::new(),
        dedupe: Vec::new(),
        forward_seq: Vec::new(),
        last_packet_at: None,
    };
    block_on(prg.persist_session_state(&prg_id, session_state)).unwrap();
    block_on(prg.add_subscription(
        &prg_id,
        "t/+",
        quantum::mqtt::Qos::AtLeastOnce,
        "client-a",
        None,
    ))
    .unwrap();
    block_on(prg.commit_publish(
        &prg_id,
        "tenant/topic",
        b"payload",
        quantum::mqtt::Qos::AtLeastOnce,
        true,
    ))
    .unwrap();
    block_on(prg.record_offline_entry(
        &prg_id,
        "client-a",
        OfflineEntry {
            client_id: "client-a".into(),
            topic: "tenant/topic".into(),
            qos: quantum::mqtt::Qos::AtLeastOnce,
            retain: false,
            enqueue_index: 11,
            payload: b"offline".to_vec(),
        },
    ))
    .unwrap();

    assert!(ack.clustor_floor() > 0);

    let stored = block_on(storage.load(&prg_id))
        .unwrap()
        .expect("persisted state on disk");
    assert!(stored.sessions.contains_key("client-a"));

    let cp2 = ControlPlaneClient::new(cfg.control_plane, 1, clock.clone()).unwrap();
    let prg2 = new_prg_manager(table, clock, cp2, ack, durability, metrics, storage);
    block_on(prg2.refresh_routing_table()).unwrap();
    let persisted = block_on(prg2.persisted_state(&prg_id)).expect("persisted state");
    assert!(persisted.sessions.contains_key("client-a"));
    assert!(
        persisted
            .retained
            .get("tenant/topic")
            .map(|v| v.as_slice())
            .unwrap_or_default()
            == b"payload"
    );
    assert!(persisted.subscriptions.contains_key("t/+"));
    assert!(!persisted.topics.is_empty());
    assert!(persisted
        .offline
        .get("client-a")
        .map(|entries| !entries.is_empty())
        .unwrap_or(false));
}

#[test]
fn cp_watcher_updates_routing_from_clustor() {
    let now = Instant::now();
    let records = vec![PlacementRecord {
        partition_id: "tenant-a:0".into(),
        routing_epoch: 7,
        lease_epoch: 3,
        members: vec!["node-a".into()],
    }];
    let transport = TestCpTransport::with_routing(records, now);
    let mtls = MtlsIdentityManager::new(
        cp_certificate("spiffe://cp.internal/clients/tester", now),
        "cp.internal",
        Duration::from_secs(60),
        now,
    );
    let cfg = ControlPlaneConfig {
        endpoints: vec!["node-a".into()],
        cache_ttl_seconds: 5,
        peer_versions: None,
        ..Default::default()
    };
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    cp.configure_cp_client(transport, mtls);
    cp.refresh_from_cp().unwrap();
    let snapshot = cp.snapshot();
    assert_eq!(snapshot.routing_epoch.0, 7);
    assert_eq!(snapshot.placements.len(), 1);
    let routing = cp.routing_snapshot();
    assert_eq!(routing.placements.len(), 1);
    assert!(cp.cache_is_fresh());
}

#[test]
fn routing_update_moves_prg_off_local_node() {
    let now = Instant::now();
    let records = vec![PlacementRecord {
        partition_id: "tenant-a:0".into(),
        routing_epoch: 1,
        lease_epoch: 1,
        members: vec!["node-a".into()],
    }];
    let transport = TestCpTransport::with_routing(records, now);
    let mtls = MtlsIdentityManager::new(
        cp_certificate("spiffe://cp.internal/clients/tester", now),
        "cp.internal",
        Duration::from_secs(60),
        now,
    );
    let mut cfg = test_config();
    cfg.control_plane.cache_ttl_seconds = 1;
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg.control_plane.clone(), 1, clock.clone()).unwrap();
    cp.configure_cp_client(transport, mtls);
    cp.refresh_from_cp().unwrap();
    let durability = durability_for_cp(cfg.durability.clone(), &cp);
    let ack = Arc::new(AckContract::new(&durability));
    let metrics = Arc::new(BackpressureState::default());
    let storage = quantum::storage::ClustorStorage::new(tempdir().unwrap().path());
    let table = quantum::routing::RoutingTable::empty(cp.routing_epoch());
    let prg = new_prg_manager(
        table,
        clock.clone(),
        cp.clone(),
        ack,
        durability,
        metrics,
        storage,
    );
    block_on(prg.refresh_routing_table()).unwrap();
    assert_eq!(block_on(prg.active_hosts_len()), 1);

    let updated = vec![PlacementRecord {
        partition_id: "tenant-a:0".into(),
        routing_epoch: 2,
        lease_epoch: 2,
        members: vec!["node-b".into()],
    }];
    let transport_move = TestCpTransport::with_routing(updated, now);
    let mtls_move = MtlsIdentityManager::new(
        cp_certificate("spiffe://cp.internal/clients/tester", now),
        "cp.internal",
        Duration::from_secs(60),
        now,
    );
    cp.configure_cp_client(transport_move, mtls_move);
    cp.refresh_from_cp().unwrap();
    block_on(prg.refresh_routing_table()).unwrap();
    assert_eq!(block_on(prg.active_hosts_len()), 0);
}

#[test]
fn prg_ready_respects_strict_fallback() {
    let cfg = test_config();
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg.control_plane.clone(), 1, clock.clone()).unwrap();
    let table = quantum::routing::RoutingTable::empty(RoutingEpoch(0));
    let durability = durability_for_cp(cfg.durability.clone(), &cp);
    let ack = Arc::new(AckContract::new(&durability));
    let metrics = Arc::new(BackpressureState::default());
    let dir = tempdir().unwrap();
    let storage = quantum::storage::ClustorStorage::new(dir.path());
    let prg = new_prg_manager(table, clock, cp.clone(), ack, durability, metrics, storage);
    block_on(cp.start()).unwrap();
    block_on(prg.start()).unwrap();
    assert!(prg.ready());
    cp.set_strict_fallback(true);
    assert!(!prg.ready());
    cp.set_strict_fallback(false);
    cp.mark_refreshed();
    block_on(prg.refresh_routing_table()).unwrap();
    assert!(prg.ready());
}

#[test]
fn readiness_respects_durability_and_cp_freshness() {
    let mut cfg = test_config();
    cfg.control_plane.cache_ttl_seconds = 1;
    let clock = SystemClock;
    let runtime = Runtime::new(cfg.clone(), clock.clone(), None).unwrap();
    let cp = runtime.control_plane();
    block_on(cp.start()).unwrap();
    let prg = runtime.prg_manager();
    block_on(prg.start()).unwrap();
    block_on(runtime.ack_contract().wait_for_commit()).unwrap();
    if let Some(proof) = runtime.ack_contract().latest_proof() {
        cp.ingest_apply(
            proof.term,
            proof.index,
            runtime.ack_contract().cp_commit_visibility(),
        );
        cp.publish_proof(CpDurabilityProof::new(proof.term, proof.index));
    }
    assert!(runtime.ack_contract().clustor_floor() >= 1);
    assert!(runtime.ready());
    runtime.ack_contract().set_durability_fence(true);
    assert!(!runtime.ready());
    runtime.ack_contract().set_durability_fence(false);
    thread::sleep(Duration::from_secs(2));
    assert!(!runtime.ready());
}

#[test]
fn ingest_clustor_metrics_updates_state() {
    let cfg = test_config();
    let clock = SystemClock;
    let runtime = Runtime::new(cfg, clock, None).unwrap();
    runtime.ingest_clustor_metrics(ClustorBackpressure {
        commit_to_apply_depth: 5,
        apply_to_delivery_depth: 3,
        apply_lag_seconds: 2,
        replication_lag_seconds: 4,
        rejected_overload: 1,
        tenant_throttle_events: 2,
        leader_credit_hint: 7,
        forward_backpressure_seconds: 9,
        ..Default::default()
    });
    let snap = runtime.metrics().snapshot();
    assert_eq!(snap.queue_depth_apply, 5);
    assert_eq!(snap.queue_depth_delivery, 3);
    assert_eq!(snap.apply_lag_seconds, 2);
    assert_eq!(snap.replication_lag_seconds, 4);
    assert_eq!(snap.reject_overload_total, 1);
    assert_eq!(snap.tenant_throttle_events_total, 2);
    assert_eq!(snap.leader_credit_hint, 7);
    assert_eq!(snap.forward_backpressure_seconds, 9);
    assert_eq!(runtime.ack_contract().replication_lag_seconds(), 4);
}

#[test]
fn runtime_ingests_peer_versions() {
    let cfg = test_config();
    let clock = SystemClock;
    let runtime = Runtime::new(cfg, clock, None).unwrap();
    let mut peers = HashMap::new();
    peers.insert("node-b".to_string(), 1u8);
    runtime.ingest_peer_versions(peers.clone());
    let snapshot = runtime.control_plane().snapshot();
    assert_eq!(snapshot.peer_versions.get("node-b"), peers.get("node-b"));
}

#[test]
fn ack_contract_tracks_clustor_floor() {
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let before = ack.clustor_floor();
    block_on(ack.wait_for_commit()).unwrap();
    let after = ack.clustor_floor();
    assert!(after > before);
}

#[test]
fn ack_contract_respects_fsync_delay() {
    let injector = FaultInjector::default();
    injector.enable_fsync_slow(true);
    injector.set_latency_ms(25);
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    })
    .with_fault_injector(injector);
    let start = Instant::now();
    block_on(ack.wait_for_commit()).unwrap();
    assert!(start.elapsed() >= Duration::from_millis(25));
}

#[test]
fn hot_reload_blocks_on_stale() {
    let cfg = test_config();
    let clock = SystemClock;
    let mut runtime = Runtime::new(cfg.clone(), clock, None).unwrap();
    runtime.control_plane().mark_stale();
    assert!(runtime.hot_reload(cfg).is_err());
}

#[test]
fn clustor_apply_and_ack_streams() {
    let client = ClustorClient::new(3, "local", &[]);
    client.register_replica(ReplicaId::new("peer-1")).unwrap();
    let mut apply_rx = client.apply_stream();
    let commit_idx = client
        .record_local_commit(1, 5, IoMode::Strict, None)
        .expect("commit");
    let evt = block_on(apply_rx.recv()).expect("apply event");
    assert_eq!(evt.index, commit_idx);
    client.bootstrap_partition(10);
    let evt2 = block_on(apply_rx.recv()).expect("bootstrap apply");
    assert_eq!(evt2.index, 10);
    assert!(evt2.floor >= 10);
}

#[test]
fn dr_restore_fences_region_mismatch() {
    let dir = tempdir().unwrap();
    let prg = PrgId {
        tenant_id: "tenant-a".into(),
        partition_index: 0,
    };
    let storage = quantum::storage::ClustorStorage::new(dir.path());
    let state = PersistedPrgState::default();
    block_on(storage.persist(&prg, &state)).unwrap();
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let metrics = Arc::new(BackpressureState::default());
    let dr = quantum::dr::DrManager::new(SystemClock, ack, metrics, storage.clone());
    let restored = block_on(dr.restore_with_proof(&storage, &prg, 1, Some("region-a"))).unwrap();
    assert!(restored.is_some());
    assert_eq!(dr.region_certificate().as_deref(), Some("region-a"));
    let err = block_on(dr.restore_with_proof(&storage, &prg, 1, Some("region-b")));
    assert!(err.is_err());
}

#[test]
fn controlled_promotion_requires_cp_observer() {
    let dir = tempdir().unwrap();
    let prg = PrgId {
        tenant_id: "tenant-a".into(),
        partition_index: 0,
    };
    let storage = quantum::storage::ClustorStorage::new(dir.path());
    let state = PersistedPrgState::default();
    block_on(storage.persist(&prg, &state)).unwrap();
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(
        ControlPlaneConfig {
            endpoints: vec!["cp.local".into()],
            cache_ttl_seconds: 0,
            peer_versions: None,
            ..Default::default()
        },
        1,
        clock.clone(),
    )
    .unwrap();
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let metrics = Arc::new(BackpressureState::default());
    let dr = quantum::dr::DrManager::new(clock, ack.clone(), metrics, storage);
    dr.set_control_plane(cp.clone());
    // First attempt should fence but not promote because the CP observer has not seen a proof.
    block_on(dr.controlled_promotion());
    assert!(!dr.promoted());
    // Publish the proof into the control-plane cache to satisfy the observer gate.
    if let Some(proof) = ack.latest_proof() {
        cp.ingest_apply(proof.term, proof.index, ack.cp_commit_visibility());
        cp.publish_proof(CpDurabilityProof::new(proof.term, proof.index));
    }
    block_on(dr.controlled_promotion());
    assert!(dr.promoted());
    assert!(!dr.uncontrolled());
}

#[test]
fn restore_from_backup_replays_and_clears_uncontrolled() {
    let dir = tempdir().unwrap();
    let prg = PrgId {
        tenant_id: "tenant-a".into(),
        partition_index: 0,
    };
    let storage = quantum::storage::ClustorStorage::new(dir.path());
    let state = PersistedPrgState {
        committed_index: 2,
        ..Default::default()
    };
    block_on(storage.persist_at_with_proof(&prg, &state, 2, Some(DurabilityProof::new(1, 2))))
        .unwrap();
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let metrics = Arc::new(BackpressureState::default());
    let dr = quantum::dr::DrManager::new(SystemClock, ack.clone(), metrics, storage);
    dr.uncontrolled_promotion();
    assert!(dr.uncontrolled());
    block_on(dr.restore_from_backup(Some("region-a"))).unwrap();
    assert!(!dr.uncontrolled());
    assert!(!dr.promoted());
    assert_eq!(ack.clustor_floor(), 2);
    assert!(!ack.durability_fence_active());
}

#[test]
fn forwarding_engine_duplicate_and_grace() {
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let metrics = Arc::new(BackpressureState::default());
    let engine = ForwardingEngine::new(ack, metrics.clone(), Duration::from_secs(60));
    let session_prg = PrgId {
        tenant_id: "tenant".into(),
        partition_index: 0,
    };
    let topic_prg = session_prg.clone();
    let req = ForwardRequest {
        session_prg: session_prg.clone(),
        topic_prg: topic_prg.clone(),
        seq: 1,
        routing_epoch: RoutingEpoch(1),
        local_epoch: RoutingEpoch(1),
        allow_grace: false,
    };
    let first = block_on(engine.forward_and_commit(req.clone(), || async {})).unwrap();
    assert!(first.applied);
    let dup = block_on(engine.forward_and_commit(req.clone(), || async {})).unwrap();
    assert!(dup.duplicate);
    let stale_req = ForwardRequest {
        routing_epoch: RoutingEpoch(3),
        local_epoch: RoutingEpoch(1),
        allow_grace: false,
        ..req.clone()
    };
    assert!(block_on(engine.forward_and_commit(stale_req, || async {})).is_err());
    let grace_req = ForwardRequest {
        routing_epoch: RoutingEpoch(2),
        local_epoch: RoutingEpoch(3),
        allow_grace: true,
        ..req
    };
    assert!(block_on(engine.forward_and_commit(grace_req, || async {})).is_ok());
}

#[test]
fn shared_subscription_balances_and_dedupes() {
    let mut balancer = SharedSubscriptionBalancer::default();
    let members = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let shuffled = vec![members[2].clone(), members[0].clone(), members[1].clone()];
    let mut counts: HashMap<String, u64> = HashMap::new();
    for packet in 0..60u64 {
        let assigned = balancer
            .assign("tenant/$share/group/topic", &members, packet)
            .expect("assignment");
        *counts.entry(assigned.clone()).or_insert(0) += 1;
        let again = balancer
            .assign("tenant/$share/group/topic", &shuffled, packet)
            .expect("repeat assignment");
        assert_eq!(assigned, again);
        balancer.release("tenant/$share/group/topic", packet);
    }
    let min = counts.values().copied().min().unwrap();
    let max = counts.values().copied().max().unwrap();
    assert!(max.saturating_sub(min) <= 1);
}

#[test]
fn cross_prg_forwarding_stale_epoch_dedupes() {
    let mut cfg = test_config();
    cfg.tenants.tenant_prg_count = 2;
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg.control_plane.clone(), 2, clock.clone()).unwrap();
    let durability = durability_for_cp(cfg.durability.clone(), &cp);
    let ack = Arc::new(AckContract::new(&durability));
    let metrics = Arc::new(BackpressureState::default());
    let storage_dir = tempdir().unwrap();
    let storage = quantum::storage::ClustorStorage::new(storage_dir.path());
    let storage_handle = storage.clone();
    let prg = new_prg_manager(
        quantum::routing::RoutingTable::empty(cp.routing_epoch()),
        clock.clone(),
        cp.clone(),
        ack.clone(),
        durability.clone(),
        metrics,
        storage,
    );
    cp.update_routing_epoch(RoutingEpoch(1));
    cp.update_rebalance_epoch(2);
    block_on(prg.refresh_routing_table()).unwrap();
    let session_prg = prg.session_prg("tenant-a", "client-a");
    let mut topic_prg = prg.topic_prg("tenant-a", "tenant-a/topic-a");
    if session_prg == topic_prg {
        topic_prg = prg.topic_prg("tenant-a", "tenant-a/topic-b");
    }
    assert_ne!(session_prg, topic_prg);
    let stale = block_on(prg.forward_topic_publish(
        &session_prg,
        &topic_prg,
        1,
        RoutingEpoch(0),
        "tenant-a/topic-a",
        b"payload",
        quantum::mqtt::Qos::AtLeastOnce,
        false,
    ))
    .unwrap();
    assert!(stale.duplicate);
    assert!(!stale.applied);
    let current = block_on(prg.forward_topic_publish(
        &session_prg,
        &topic_prg,
        2,
        cp.routing_epoch(),
        "tenant-a/topic-a",
        b"payload",
        quantum::mqtt::Qos::AtLeastOnce,
        false,
    ))
    .unwrap();
    assert!(!current.duplicate);
    assert!(current.applied);
    assert!(block_on(storage_handle.last_index(&topic_prg)).unwrap() > 0);
    assert!(ack.clustor_floor() > 0);
}

#[test]
fn restart_restores_qos2_state_from_storage() {
    let dir = tempdir().unwrap();
    let cfg = test_config();
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg.control_plane.clone(), 1, clock.clone()).unwrap();
    let durability = durability_for_cp(cfg.durability.clone(), &cp);
    let ack = Arc::new(AckContract::new(&durability));
    let metrics = Arc::new(BackpressureState::default());
    let storage = quantum::storage::ClustorStorage::new(dir.path());
    let prg = new_prg_manager(
        quantum::routing::RoutingTable::empty(cp.routing_epoch()),
        clock.clone(),
        cp.clone(),
        ack.clone(),
        durability.clone(),
        metrics.clone(),
        storage.clone(),
    );
    block_on(prg.refresh_routing_table()).unwrap();
    let session_prg = prg.session_prg("tenant-a", "client-a");
    let mut processor = SessionProcessor::new(clock.clone(), (*ack).clone());
    let mut state = SessionState {
        client_id: "client-a".into(),
        tenant_id: "tenant-a".into(),
        session_epoch: 2,
        inflight: HashMap::new(),
        dedupe_floor_index: 0,
        offline_floor_index: 0,
        status: SessionStatus::Connected,
        earliest_offline_queue_index: 0,
        offline: OfflineQueue::default(),
        keep_alive: 5,
        session_expiry_interval: Some(30),
        last_packet_at: Some(clock.now()),
        last_packet_at_wall: Some(std::time::SystemTime::now()),
        will: None,
        outbound: HashMap::new(),
        outbound_tx: None,
        dedupe_entries: Vec::new(),
        forward_progress: Vec::new(),
    };
    let mid = 42;
    processor.record_dedupe(&mut state, mid, quantum::dedupe::Direction::Publish, 3);
    processor.record_ack_progress(&mut state, mid, 7);
    processor.record_inflight(&mut state, mid);
    let offline_entry = OfflineEntry {
        client_id: "client-a".into(),
        topic: "tenant-a/topic".into(),
        qos: quantum::mqtt::Qos::ExactlyOnce,
        retain: false,
        enqueue_index: 11,
        payload: b"offline-payload".to_vec(),
    };
    state.offline.enqueue(offline_entry.clone());
    state.earliest_offline_queue_index = state.offline.earliest_offline_queue_index();
    state.offline_floor_index = offline_entry.enqueue_index;
    let outbound = OutboundMessage {
        client_id: "client-a".into(),
        topic: "tenant-a/topic".into(),
        payload: b"payload".to_vec(),
        qos: quantum::mqtt::Qos::ExactlyOnce,
        retain: false,
        enqueue_index: 5,
    };
    state.outbound.insert(
        mid,
        OutboundTracking {
            msg: outbound.clone(),
            stage: OutboundStage::WaitPubRec,
            mid,
            retry_at: clock.now(),
            retry_delay: Duration::from_secs(1),
        },
    );
    state.dedupe_entries = processor.dedupe_snapshot();
    state.forward_progress = processor.forward_seq_snapshot();
    let last_packet_at = state
        .last_packet_at_wall
        .and_then(|ts| ts.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs());
    let persisted = PersistedSessionState {
        client_id: state.client_id.clone(),
        dedupe_floor: state.dedupe_floor_index,
        offline_floor: state.offline_floor_index,
        earliest_offline: state.earliest_offline_queue_index,
        forward_chain_floor: state.offline_floor_index,
        effective_product_floor: 0,
        keep_alive: state.keep_alive,
        session_expiry_interval: state.session_expiry_interval,
        session_epoch: state.session_epoch,
        inflight: state
            .inflight
            .keys()
            .map(|mid| quantum::session::PersistedInflight { message_id: *mid })
            .collect(),
        outbound: state
            .outbound
            .values()
            .map(|track| quantum::session::PersistedOutbound {
                mid: track.mid,
                stage: track.stage,
                msg: track.msg.clone(),
                retry_delay_ms: track.retry_delay.as_millis() as u64,
            })
            .collect(),
        dedupe: state.dedupe_entries.clone(),
        forward_seq: state.forward_progress.clone(),
        last_packet_at,
    };
    block_on(prg.record_offline_entry(&session_prg, "client-a", offline_entry)).unwrap();
    block_on(prg.persist_session_state(&session_prg, persisted)).unwrap();
    assert!(ack.clustor_floor() > 0);
    let cp2 = ControlPlaneClient::new(cfg.control_plane, 1, clock.clone()).unwrap();
    cp2.update_tenant_prg("tenant-a", 1, RoutingEpoch(1));
    let prg2 = new_prg_manager(
        quantum::routing::RoutingTable::empty(cp2.routing_epoch()),
        clock.clone(),
        cp2,
        ack.clone(),
        durability,
        metrics,
        storage,
    );
    block_on(prg2.refresh_routing_table()).unwrap();
    let (restored, offline) = block_on(prg2.session_snapshot(&session_prg, "client-a"))
        .unwrap()
        .expect("restored session");
    assert_eq!(restored.session_epoch, 2);
    assert_eq!(restored.inflight.len(), 1);
    assert_eq!(restored.outbound.len(), 1);
    assert_eq!(restored.dedupe_floor, 3);
    assert_eq!(offline.len(), 1);
    assert_eq!(offline[0].payload, b"offline-payload");
    let mut resumed = SessionState {
        client_id: restored.client_id.clone(),
        tenant_id: "tenant-a".into(),
        session_epoch: restored.session_epoch,
        inflight: restored
            .inflight
            .iter()
            .map(|p| {
                (
                    p.message_id,
                    quantum::session::InflightRecord {
                        message_id: p.message_id,
                        created_at: Duration::from_secs(0),
                    },
                )
            })
            .collect(),
        dedupe_floor_index: restored.dedupe_floor,
        offline_floor_index: restored.offline_floor,
        status: SessionStatus::Connected,
        earliest_offline_queue_index: restored.earliest_offline,
        offline: {
            let mut queue = OfflineQueue::default();
            for entry in offline {
                queue.enqueue(entry);
            }
            queue
        },
        keep_alive: restored.keep_alive,
        session_expiry_interval: restored.session_expiry_interval,
        last_packet_at: None,
        last_packet_at_wall: None,
        will: None,
        outbound: restored
            .outbound
            .iter()
            .map(|p| {
                (
                    p.mid,
                    OutboundTracking {
                        msg: p.msg.clone(),
                        stage: p.stage,
                        mid: p.mid,
                        retry_at: clock.now(),
                        retry_delay: Duration::from_millis(p.retry_delay_ms),
                    },
                )
            })
            .collect(),
        outbound_tx: None,
        dedupe_entries: restored.dedupe.clone(),
        forward_progress: restored.forward_seq.clone(),
    };
    let mut resumed_processor = SessionProcessor::new(clock, (*ack).clone());
    resumed_processor.hydrate(&resumed);
    assert_eq!(
        resumed_processor.dedupe_wal_index(&resumed, mid, quantum::dedupe::Direction::Ack),
        Some(7)
    );
    assert!(resumed_processor.record_dedupe(
        &mut resumed,
        mid,
        quantum::dedupe::Direction::Publish,
        9
    ));
}

#[test]
fn read_gate_failure_maps_to_permanent_durability() {
    let cfg = ControlPlaneConfig {
        endpoints: vec!["localhost:9000".into()],
        cache_ttl_seconds: 0,
        peer_versions: None,
        ..Default::default()
    };
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    let err = cp.guard_read_gate(false).expect_err("read gate fenced");
    assert!(cp.strict_fallback());
    assert_eq!(cp.snapshot().last_read_gate, Some(false));
    let code: InternalCode = (&err).into();
    assert_eq!(code, InternalCode::PermanentDurability);
}

#[test]
fn forward_publish_epoch_mismatch_does_not_advance_seq() {
    let cfg = test_config();
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg.control_plane.clone(), 1, clock.clone()).unwrap();
    let ack = Arc::new(AckContract::new(&cfg.durability));
    let metrics = Arc::new(BackpressureState::default());
    let storage = quantum::storage::ClustorStorage::new(tempdir().unwrap().path());
    let prg = new_prg_manager(
        quantum::routing::RoutingTable::empty(cp.routing_epoch()),
        clock.clone(),
        cp.clone(),
        ack.clone(),
        cfg.durability.clone(),
        metrics,
        storage,
    );
    block_on(prg.refresh_routing_table()).unwrap();
    let session_prg = prg.session_prg("tenant-a", "client-a");
    let topic_prg = prg.topic_prg("tenant-a", "tenant-a/topic");
    let mut processor = SessionProcessor::new(clock, (*ack).clone());
    let routing_epoch = RoutingEpoch(cp.routing_epoch().0 + 2);
    let res = block_on(processor.forward_publish(
        &prg,
        &session_prg,
        &topic_prg,
        routing_epoch,
        "tenant-a/topic",
        b"payload",
        quantum::mqtt::Qos::AtLeastOnce,
        false,
    ));
    assert!(res.is_err());
    assert!(processor.forward_seq_snapshot().is_empty());
}

fn encode_remaining(mut len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
    out
}

#[test]
fn mqtt_v5_parses_properties_and_will() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&4u16.to_be_bytes());
    payload.extend_from_slice(b"MQTT");
    payload.push(5); // protocol level
    let connect_flags = 0b1100_1110; // user+pass+will qos1 + will flag + clean start
    payload.push(connect_flags);
    payload.extend_from_slice(&10u16.to_be_bytes()); // keep alive
    let mut props = Vec::new();
    props.push(0x11);
    props.extend_from_slice(&60u32.to_be_bytes());
    props.push(0x21);
    props.extend_from_slice(&5u16.to_be_bytes());
    payload.push(props.len() as u8);
    payload.extend(props);
    payload.extend_from_slice(&3u16.to_be_bytes());
    payload.extend_from_slice(b"cid");
    // will properties
    let mut will_props = Vec::new();
    will_props.push(0x18);
    will_props.extend_from_slice(&30u32.to_be_bytes());
    payload.push(will_props.len() as u8);
    payload.extend(will_props);
    payload.extend_from_slice(&5u16.to_be_bytes());
    payload.extend_from_slice(b"topic");
    payload.extend_from_slice(&7u16.to_be_bytes());
    payload.extend_from_slice(b"payload");
    payload.extend_from_slice(&4u16.to_be_bytes());
    payload.extend_from_slice(b"user");
    payload.extend_from_slice(&4u16.to_be_bytes());
    payload.extend_from_slice(b"pass");
    let mut frame = vec![0x10];
    frame.extend(encode_remaining(payload.len()));
    frame.extend(payload);
    let mut cursor = Cursor::new(frame);
    let pkt = block_on(read_connect(&mut cursor)).expect("connect parse");
    assert!(pkt.clean_start);
    assert_eq!(pkt.protocol, ProtocolVersion::V5);
    assert_eq!(pkt.properties.session_expiry_interval, Some(60));
    assert_eq!(pkt.properties.receive_maximum, Some(5));
    assert_eq!(pkt.keep_alive, 10);
    let will = pkt.will.expect("will present");
    assert_eq!(will.qos, quantum::mqtt::Qos::AtLeastOnce);
    assert_eq!(will.properties.delay_interval, Some(30));
}

#[test]
fn subscribe_option_validation_catches_reserved_bits() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&10u16.to_be_bytes()); // packet id
    payload.push(0); // properties len
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.extend_from_slice(b"a");
    payload.push(0b1111_0001); // invalid retain handling/reserved bits
    let mut frame = vec![0x82];
    frame.extend(encode_remaining(payload.len()));
    frame.extend(payload);
    let mut cursor = Cursor::new(frame);
    let result = block_on(read_packet(&mut cursor, ProtocolVersion::V5));
    assert!(result.is_err());
}

#[test]
fn mqtt_v5_parses_auth_packet() {
    // AUTH with reason code success, auth method "SCRAM"
    let mut body = vec![0x00]; // reason code
    body.push(0x08); // properties length
    body.push(0x15); // auth method id
    body.extend_from_slice(&5u16.to_be_bytes());
    body.extend_from_slice(b"SCRAM");
    let mut frame = vec![0xF0];
    frame.push(10); // remaining length
    frame.extend(body);
    let mut cursor = Cursor::new(frame);
    let pkt = block_on(read_packet(&mut cursor, ProtocolVersion::V5)).expect("auth parsed");
    match pkt {
        ControlPacket::Auth(auth) => {
            assert_eq!(auth.reason_code, 0);
            assert_eq!(auth.auth_method.as_deref(), Some("SCRAM"));
            assert!(auth.auth_data.is_none());
        }
        other => panic!("expected auth packet, got {other:?}"),
    }
}

#[test]
fn auth_context_enforces_token_and_sni() {
    let cfg = ControlPlaneConfig {
        endpoints: vec!["auth.local".into()],
        cache_ttl_seconds: 0,
        peer_versions: None,
        ..Default::default()
    };
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    cp.install_auth_bundle(
        "tenant-a",
        vec!["spiffe://tenant/mtls".into()],
        vec!["token-1".into()],
        vec!["tenant/tenant-a/".into()],
    );
    let sec = SecurityManager::new(cp, Default::default());
    let ctx = AuthContext {
        tenant_id: "tenant-a",
        sni: Some("tenant-a"),
        presented_spiffe: Some("spiffe://tenant/mtls"),
        bearer_token: Some("token-1"),
        topic: Some("tenant/tenant-a/topic"),
    };
    assert!(sec.authorize_connect(ctx).is_ok());
    let bad_token = AuthContext {
        tenant_id: "tenant-a",
        sni: Some("tenant-a"),
        presented_spiffe: Some("spiffe://tenant/mtls"),
        topic: Some("tenant/tenant-a/topic"),
        bearer_token: Some("bad"),
    };
    assert!(matches!(
        sec.authorize_connect(bad_token),
        Err(SecurityError::InvalidToken)
    ));
}

#[test]
fn read_gate_sets_strict_fallback() {
    let cfg = ControlPlaneConfig {
        endpoints: vec!["node-a".into()],
        cache_ttl_seconds: 0,
        peer_versions: None,
        ..Default::default()
    };
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    cp.ingest_read_gate(false);
    assert!(cp.strict_fallback());
    assert!(!cp.cache_is_fresh());
}

#[test]
fn strict_fallback_blocks_prg_writes() {
    let cfg = test_config();
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg.control_plane.clone(), 1, clock.clone()).unwrap();
    cp.set_strict_fallback(true);
    let durability = durability_for_cp(cfg.durability.clone(), &cp);
    let ack = Arc::new(AckContract::new(&durability));
    let metrics = Arc::new(BackpressureState::default());
    let storage = quantum::storage::ClustorStorage::new(tempdir().unwrap().path());
    let table = quantum::routing::RoutingTable::empty(RoutingEpoch(0));
    let prg = new_prg_manager(
        table,
        clock,
        cp.clone(),
        ack.clone(),
        durability,
        metrics,
        storage,
    );
    let topic_prg = prg.topic_prg("tenant-a", "tenant-a/topic");
    let err = block_on(prg.commit_publish(
        &topic_prg,
        "tenant-a/topic",
        b"payload",
        quantum::mqtt::Qos::AtLeastOnce,
        false,
    ));
    assert!(err.is_err());
    assert!(ack.durability_fence_active());
    assert!(!prg.ready());
}

#[test]
fn active_hosts_reflect_routing() {
    let cfg = test_config();
    let clock = SystemClock;
    let cp = ControlPlaneClient::new(cfg.control_plane.clone(), 1, clock.clone()).unwrap();
    let durability = durability_for_cp(cfg.durability.clone(), &cp);
    let ack = Arc::new(AckContract::new(&durability));
    let metrics = Arc::new(BackpressureState::default());
    let storage = quantum::storage::ClustorStorage::new(tempdir().unwrap().path());
    let table = quantum::routing::RoutingTable::empty(RoutingEpoch(0));
    let prg = new_prg_manager(table, clock, cp, ack, durability, metrics, storage);
    block_on(prg.refresh_routing_table()).unwrap();
    assert!(block_on(prg.active_hosts_len()) > 0);
}

#[test]
fn uncontrolled_promotion_sets_fence() {
    let cfg = test_config();
    let clock = SystemClock;
    let runtime = Runtime::new(cfg, clock, None).unwrap();
    assert!(!runtime.ack_contract().durability_fence_active());
    runtime.dr_manager().uncontrolled_promotion();
    assert!(runtime.dr_manager().uncontrolled());
    assert!(runtime.ack_contract().durability_fence_active());
}

#[test]
fn config_validation_rejects_zero_prg() {
    let mut cfg = test_config();
    cfg.tenants.tenant_prg_count = 0;
    assert!(cfg.validate().is_err());
}

#[test]
fn audit_snapshot_mirrors_events() {
    quantum::audit::emit("audit_test", "tenant-x", "client-y", "mirror");
    let events = quantum::audit::snapshot();
    assert!(events.iter().any(|e| e.event_type == "audit_test"));
}

#[test]
fn zero_rtt_and_retry_policy() {
    assert!(quantum::listeners::Listeners::validate_zero_rtt(true));
    assert!(!quantum::listeners::Listeners::validate_zero_rtt(false));
    assert!(quantum::listeners::Listeners::retry_token_valid(
        Duration::from_secs(10)
    ));
    assert!(!quantum::listeners::Listeners::retry_token_valid(
        Duration::from_secs(61)
    ));
}
