use quantum::config::{Config, ControlPlaneConfig, ControlPlaneMode};
use quantum::control::CacheState;
use quantum::control::ControlPlaneClient;
use quantum::runtime::Runtime;
use quantum::time::SystemClock;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, Ia5String, IsCa, KeyPair, SanType,
};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn write_tls_material(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec!["ca.local".into()]).unwrap();
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    server_params.is_ca = IsCa::NoCa;
    server_params.subject_alt_names.push(SanType::URI(
        Ia5String::try_from("spiffe://local/server").unwrap(),
    ));
    server_params.distinguished_name = DistinguishedName::new();
    let server = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();

    let ca_path = dir.join("ca.pem");
    let server_cert_path = dir.join("server.pem");
    let server_key_path = dir.join("server.key");
    fs::write(&ca_path, ca.pem()).unwrap();
    fs::write(&server_cert_path, server.pem()).unwrap();
    fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
    (server_cert_path, server_key_path, ca_path)
}

fn embedded_config(
    storage: &Path,
    tls_chain: &Path,
    tls_key: &Path,
    ca: &Path,
    seeded: bool,
) -> Config {
    let seeds = if seeded {
        r#"
bootstrap_tenants = [{ tenant_id = "t", tenant_prg_count = 1 }]
bootstrap_placements = [{ tenant_id = "t", partition = 0, replicas = ["n0"] }]
"#
    } else {
        ""
    };
    let doc = format!(
        r#"
[control_plane]
cache_ttl_seconds = 1
mode = "embedded"
endpoints = []
embedded_http_bind = "127.0.0.1:0"
embedded_raft_bind = "127.0.0.1:0"
{seeds}

[listeners.tcp]
bind = "127.0.0.1:0"
tls_chain_path = "{chain}"
tls_key_path = "{key}"
client_ca_path = "{ca}"
alpn = ["mqtt"]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "n0"
initial_term = 1

[telemetry]
metrics_bind = "127.0.0.1:0"
log_level = "warn"

[tenants]
tenant_prg_count = 1

[paths]
storage_dir = "{storage}"
"#,
        chain = tls_chain.display(),
        key = tls_key.display(),
        ca = ca.display(),
        storage = storage.display(),
        seeds = seeds
    );
    let cfg: Config = toml::from_str(&doc).unwrap();
    cfg.validate().unwrap();
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_runtime_bootstraps_and_becomes_ready() {
    // rustls 0.21 uses ring by default; no explicit provider install needed.
    let dir = tempdir().unwrap();
    let storage = dir.path().join("data");
    fs::create_dir_all(&storage).unwrap();
    let (chain, key, ca) = write_tls_material(dir.path());
    let cfg = embedded_config(&storage, &chain, &key, &ca, true);
    let clock = SystemClock;
    let mut runtime = Runtime::new(cfg, clock, None).unwrap();
    runtime.start_for_tests().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let fresh = runtime.control_plane().cache_is_fresh();
    let snapshot = runtime.control_plane().snapshot();
    println!(
        "ready={} prg_ready={} cache_state={:?} placements={} fence={} cache_fresh={}",
        runtime.ready(),
        runtime.prg_ready(),
        snapshot.cache_state,
        snapshot.placements.len(),
        runtime.ack_contract().durability_fence_active(),
        fresh
    );
    assert!(
        !snapshot.placements.is_empty(),
        "placements should be present"
    );
    assert!(
        matches!(snapshot.cache_state, CacheState::Fresh),
        "cache should be fresh"
    );
    assert!(runtime.prg_ready(), "prg manager should be ready");
    assert!(runtime.ready(), "runtime ready gate should flip");
    runtime.shutdown_for_tests().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_runtime_bootstraps_default_tenant_when_empty() {
    // rustls 0.21 uses ring by default; no explicit provider install needed.
    let dir = tempdir().unwrap();
    let storage = dir.path().join("data");
    fs::create_dir_all(&storage).unwrap();
    let (chain, key, ca) = write_tls_material(dir.path());
    let cfg = embedded_config(&storage, &chain, &key, &ca, false);
    let clock = SystemClock;
    let mut runtime = Runtime::new(cfg, clock, None).unwrap();
    runtime.start_for_tests().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let fresh = runtime.control_plane().cache_is_fresh();
    let snapshot = runtime.control_plane().snapshot();
    assert!(
        snapshot.placements.keys().any(|p| p.tenant_id == "local"),
        "default tenant placement should be created"
    );
    println!(
        "ready={} prg_ready={} cache_state={:?} placements={} fence={} cache_fresh={}",
        runtime.ready(),
        runtime.prg_ready(),
        snapshot.cache_state,
        snapshot.placements.len(),
        runtime.ack_contract().durability_fence_active(),
        fresh
    );
    assert!(runtime.prg_ready(), "prg manager should be ready");
    runtime.shutdown_for_tests().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_cp_downtime_marks_runtime_not_ready() {
    let cfg = ControlPlaneConfig {
        endpoints: vec![],
        cache_ttl_seconds: 1,
        mode: ControlPlaneMode::Embedded,
        ..Default::default()
    };
    let clock = SystemClock;
    let client = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    client.apply_placement_records(
        [(
            "t:0".to_string(),
            clustor::control_plane::core::placement::PlacementRecord {
                partition_id: "t:0".into(),
                routing_epoch: 1,
                lease_epoch: 1,
                members: vec!["n0".into()],
            },
        )]
        .into_iter()
        .collect(),
        std::time::Instant::now(),
    );
    assert!(client.cache_is_fresh());
    client.mark_stale();
    assert!(client.strict_fallback());
}
