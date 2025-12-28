//! Public surface integration tests validating README.md promises.
//!
//! These tests verify the core broker functionality:
//! - Runtime starts and becomes ready
//! - Readiness gate flips when PRGs are initialized
//! - Clean shutdown without hangs
//!
//! Tests use bounded timeouts and polling rather than wall-clock sleeps.

mod common;

use common::{embedded_config_toml, write_tls_materials};
use quantum::config::Config;
use quantum::control::CacheState;
use quantum::runtime::Runtime;
use quantum::time::SystemClock;
use std::fs;
use std::time::Duration;
use tempfile::tempdir;

fn make_config(storage: &std::path::Path, tls: &common::TlsPaths) -> Config {
    let toml = embedded_config_toml(storage, tls, true, "n0");
    let cfg: Config = toml::from_str(&toml).unwrap();
    cfg.validate().unwrap();
    cfg
}

/// Wait for a condition with bounded timeout, polling at intervals.
async fn wait_for<F>(timeout: Duration, interval: Duration, mut condition: F) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        tokio::time::sleep(interval).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_starts_and_readiness_flips() {
    let dir = tempdir().unwrap();
    let storage = dir.path().join("data");
    fs::create_dir_all(&storage).unwrap();
    let tls = write_tls_materials(dir.path(), "test.local");
    let cfg = make_config(&storage, &tls);
    let clock = SystemClock;

    let mut runtime = Runtime::new(cfg, clock, None).unwrap();
    runtime.start_for_tests().await.unwrap();

    // Wait for readiness with bounded timeout (not a sleep for delay)
    let ready = wait_for(Duration::from_secs(5), Duration::from_millis(50), || {
        runtime.ready()
    })
    .await;

    assert!(ready, "runtime should become ready within timeout");
    assert!(runtime.prg_ready(), "PRG manager should be ready");

    let snapshot = runtime.control_plane().snapshot();
    assert!(
        matches!(snapshot.cache_state, CacheState::Fresh),
        "cache should be fresh"
    );
    assert!(
        !snapshot.placements.is_empty(),
        "placements should be present"
    );

    runtime.shutdown_for_tests().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_shutdown_completes_cleanly() {
    let dir = tempdir().unwrap();
    let storage = dir.path().join("data");
    fs::create_dir_all(&storage).unwrap();
    let tls = write_tls_materials(dir.path(), "test.local");
    let cfg = make_config(&storage, &tls);
    let clock = SystemClock;

    let mut runtime = Runtime::new(cfg, clock, None).unwrap();
    runtime.start_for_tests().await.unwrap();

    // Wait for ready state
    let _ = wait_for(Duration::from_secs(5), Duration::from_millis(50), || {
        runtime.ready()
    })
    .await;

    // Shutdown should complete within bounded time
    let shutdown_result =
        tokio::time::timeout(Duration::from_secs(10), runtime.shutdown_for_tests()).await;

    assert!(
        shutdown_result.is_ok(),
        "shutdown should complete within timeout"
    );
    assert!(shutdown_result.unwrap().is_ok(), "shutdown should succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn durability_fence_is_inactive_on_healthy_start() {
    let dir = tempdir().unwrap();
    let storage = dir.path().join("data");
    fs::create_dir_all(&storage).unwrap();
    let tls = write_tls_materials(dir.path(), "test.local");
    let cfg = make_config(&storage, &tls);
    let clock = SystemClock;

    let mut runtime = Runtime::new(cfg, clock, None).unwrap();
    runtime.start_for_tests().await.unwrap();

    let _ = wait_for(Duration::from_secs(5), Duration::from_millis(50), || {
        runtime.ready()
    })
    .await;

    // Durability fence should not be active on healthy startup
    assert!(
        !runtime.ack_contract().durability_fence_active(),
        "durability fence should be inactive"
    );

    runtime.shutdown_for_tests().await.unwrap();
}
