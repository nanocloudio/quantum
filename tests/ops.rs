use futures::executor::block_on;
use quantum::config::{
    CommitVisibility, Config, ControlPlaneConfig, DrConfig, DurabilityConfig, DurabilityMode,
    ForwardPlaneConfig, ListenerConfig, PathConfig, QuicConfig, QuotaLimits, TcpConfig,
    TelemetryConfig, TenantConfig,
};
use quantum::raft::AckContract;
use quantum::runtime::Runtime;
use quantum::security::QuotaEnforcer;
use quantum::telemetry;
use quantum::time::SystemClock;
use quantum::tls::ReloadableTlsConfig;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

fn base_config() -> Config {
    Config {
        control_plane: ControlPlaneConfig {
            endpoints: vec!["localhost:9000".into()],
            cache_ttl_seconds: 1,
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

#[test]
fn config_validation_honors_optional_paths() {
    let dir = tempdir().unwrap();
    let mut cfg = base_config();
    cfg.paths.storage_dir = Some(dir.path().join("storage"));
    fs::create_dir_all(cfg.paths.storage_dir.as_ref().unwrap()).unwrap();
    assert!(cfg.validate_paths().is_ok());

    let mut bad_cfg = base_config();
    bad_cfg.paths.cp_certs = Some(dir.path().join("missing-ca.pem"));
    assert!(bad_cfg.validate_paths().is_err());
}

#[test]
fn reloadable_tls_detects_mtime_changes() {
    let dir = tempdir().unwrap();
    let chain = dir.path().join("chain.pem");
    let key = dir.path().join("key.pem");
    let ca = dir.path().join("ca.pem");
    fs::write(&chain, b"cert").unwrap();
    fs::write(&key, b"key").unwrap();
    fs::write(&ca, b"ca").unwrap();
    let mut reloadable = ReloadableTlsConfig::new(&chain, &key, &ca, vec!["mqtt".into()]);
    assert!(reloadable.changed().unwrap());
    assert!(!reloadable.changed().unwrap());
    std::thread::sleep(Duration::from_millis(5));
    fs::write(&chain, b"cert-updated").unwrap();
    assert!(reloadable.changed().unwrap());
}

#[test]
fn ack_contract_handles_burst_without_durability_fence() {
    let ack = AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    for _ in 0..50 {
        block_on(ack.wait_for_commit()).unwrap();
    }
    assert!(ack.clustor_floor() >= 50);
}

#[test]
fn offline_quota_enforced() {
    let limits = QuotaLimits {
        max_offline_entries: Some(1),
        max_offline_bytes: Some(8),
        ..Default::default()
    };
    let enforcer = QuotaEnforcer::new(Some(limits));
    assert!(enforcer.record_offline_enqueue("tenant", 1, 6).is_ok());
    assert!(enforcer.record_offline_enqueue("tenant", 1, 4).is_err());
    assert!(enforcer.record_offline_enqueue("tenant", -1, -6).is_ok());
}

#[tokio::test]
async fn readyz_reflects_backpressure_and_dr_lag() {
    let mut cfg = base_config();
    cfg.tenants.thresholds = Some(quantum::config::ThresholdConfig {
        commit_to_apply_pause_ack: 1,
        apply_to_delivery_drop_qos0: 1,
        max_apply_lag_seconds_ready: 1,
        max_replication_lag_seconds_ready: 1,
    });
    let clock = SystemClock;
    let runtime = Arc::new(Runtime::new(cfg, clock, None).unwrap());
    let (code, body, _) = telemetry::readyz(&runtime).await;
    assert_eq!(code, 503);
    assert!(body.contains("\"apply_lag\""));
}
