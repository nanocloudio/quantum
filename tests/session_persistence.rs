use quantum::config::{
    CommitVisibility, Config, ControlPlaneConfig, DurabilityConfig, DurabilityMode,
    ForwardPlaneConfig, ListenerConfig, PathConfig, TcpConfig, TelemetryConfig, TenantConfig,
};
use quantum::flow::BackpressureState;
use quantum::prg::{PersistedPrgState, PersistedSessionState, PrgManager, PrgManagerInputs};
use quantum::raft::AckContract;
use quantum::routing::{PrgId, RoutingEpoch, RoutingTable};
use quantum::time::SystemClock;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::tempdir;

fn minimal_config() -> Config {
    let tmp = tempdir().unwrap();
    let storage_path = tmp.path().to_path_buf();
    std::mem::forget(tmp); // keep around for test lifetime
    Config {
        control_plane: ControlPlaneConfig {
            endpoints: vec![],
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
            quic: None,
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
            storage_dir: Some(storage_path),
            ..Default::default()
        },
        dr: Default::default(),
        forward_plane: ForwardPlaneConfig::default(),
    }
}

#[test]
fn recompute_effective_product_floor_respects_offline_and_forward() {
    let mut prg_state = PersistedPrgState::default();
    let session = PersistedSessionState {
        client_id: "c1".into(),
        dedupe_floor: 5,
        offline_floor: 7,
        earliest_offline: 6,
        forward_chain_floor: 9,
        effective_product_floor: 0,
        ..Default::default()
    };
    prg_state.sessions.insert("c1".into(), session.clone());
    prg_state.recompute_effective_product_floors(3);
    let recomputed = prg_state.sessions.get("c1").unwrap();
    assert_eq!(recomputed.effective_product_floor, 9);
}

#[tokio::test]
async fn purge_session_clears_persisted_state() {
    let cfg = minimal_config();
    let routing = RoutingTable::empty(RoutingEpoch(0));
    let clock = SystemClock;
    let cp = quantum::control::ControlPlaneClient::new(
        cfg.control_plane.clone(),
        cfg.tenants.tenant_prg_count,
        clock.clone(),
    )
    .unwrap();
    let ack = Arc::new(AckContract::new(&cfg.durability));
    let prg = PrgManager::new(
        routing,
        clock,
        PrgManagerInputs {
            control_plane: cp.clone(),
            ack_contract: ack.clone(),
            durability_cfg: cfg.durability.clone(),
            raft_net: None,
            metrics: Arc::new(BackpressureState::default()),
            version_gate: quantum::ops::VersionGate::new(0),
            storage_backend: quantum::storage::ClustorStorage::new(
                cfg.paths
                    .storage_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("data")),
            ),
            forward_plane: ForwardPlaneConfig::default(),
        },
    );
    let prg_id = PrgId {
        tenant_id: "tenant".into(),
        partition_index: 0,
    };
    let session_state = PersistedSessionState {
        client_id: "client-a".into(),
        offline_floor: 1,
        earliest_offline: 1,
        dedupe_floor: 2,
        forward_chain_floor: 3,
        effective_product_floor: 0,
        ..Default::default()
    };
    prg.persist_session_state(&prg_id, session_state)
        .await
        .unwrap();
    prg.purge_session(&prg_id, "client-a").await.unwrap();
    let state = prg.persisted_state(&prg_id).await.unwrap();
    assert!(state.sessions.is_empty());
    assert!(state.offline.is_empty());
}
