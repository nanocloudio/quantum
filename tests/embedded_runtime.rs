mod common;

use common::{embedded_config_toml, write_tls_materials};
use quantum::config::{Config, ControlPlaneConfig, ControlPlaneMode};
use quantum::control::CacheState;
use quantum::control::ControlPlaneClient;
use quantum::runtime::Runtime;
use quantum::time::SystemClock;
use std::fs;
use tempfile::tempdir;

fn embedded_config(storage: &std::path::Path, tls: &common::TlsPaths, seeded: bool) -> Config {
    let toml = embedded_config_toml(storage, tls, seeded, "n0");
    let cfg: Config = toml::from_str(&toml).unwrap();
    cfg.validate().unwrap();
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_runtime_bootstraps_and_becomes_ready() {
    let dir = tempdir().unwrap();
    let storage = dir.path().join("data");
    fs::create_dir_all(&storage).unwrap();
    let tls = write_tls_materials(dir.path(), "test.local");
    let cfg = embedded_config(&storage, &tls, true);
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
    let dir = tempdir().unwrap();
    let storage = dir.path().join("data");
    fs::create_dir_all(&storage).unwrap();
    let tls = write_tls_materials(dir.path(), "test.local");
    let cfg = embedded_config(&storage, &tls, false);
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
