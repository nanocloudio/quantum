use clustor::control_plane::core::client::CpApiTransport;
use clustor::control_plane::HttpCpTransportBuilder;
use clustor::net::tls::{load_identity_from_pem, load_trust_store_from_pem};
use clustor::security::MtlsIdentityManager;
use quantum::config::ControlPlaneConfig;
use quantum::config::ControlPlaneMode;
use quantum::config::{BootstrapPlacementConfig, ControlPlaneBootstrap};
use quantum::control::embedded::start_embedded_cp_server;
use quantum::control::embedded::EmbeddedCpState;
use quantum::control::BoxTransport;
use quantum::control::{ControlPlaneClient, ControlPlaneError};
use quantum::routing::{PrgId, RoutingEpoch};
use quantum::time::Clock;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, Ia5String, IsCa, KeyPair, SanType,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::sync::watch;

#[derive(Clone)]
struct TestClock {
    now: Arc<Mutex<Instant>>,
}

impl TestClock {
    fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(Instant::now())),
        }
    }

    fn advance(&self, delta: Duration) {
        if let Ok(mut guard) = self.now.lock() {
            *guard += delta;
        }
    }
}

impl Clock for TestClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }

    fn sleep(&self, duration: Duration) -> tokio::time::Sleep {
        tokio::time::sleep(duration)
    }
}

fn default_cfg() -> ControlPlaneConfig {
    ControlPlaneConfig {
        endpoints: vec!["localhost:9000".into()],
        cache_ttl_seconds: 1,
        peer_versions: None,
        mode: ControlPlaneMode::Embedded,
        ..Default::default()
    }
}

#[test]
fn cache_expiry_sets_strict_fallback() {
    let clock = TestClock::new();
    let client = ControlPlaneClient::new(default_cfg(), 1, clock.clone()).unwrap();
    assert!(client.cache_is_fresh());
    clock.advance(Duration::from_secs(2));
    assert!(!client.cache_is_fresh());
    assert!(client.strict_fallback());
}

#[test]
fn read_gate_failure_marks_fallback() {
    let client = ControlPlaneClient::new(default_cfg(), 1, TestClock::new()).unwrap();
    let res = client.guard_read_gate(false);
    assert!(matches!(res, Err(ControlPlaneError::NeededForReadIndex)));
    assert!(client.strict_fallback());
}

#[test]
fn placement_epoch_validation_respects_fence() {
    let client = ControlPlaneClient::new(default_cfg(), 1, TestClock::new()).unwrap();
    client.set_strict_fallback(true);
    let prg = PrgId {
        tenant_id: "tenant".into(),
        partition_index: 0,
    };
    let res = client.validate_placement_epoch(&prg, RoutingEpoch(1));
    assert!(matches!(res, Err(ControlPlaneError::StrictFallback)));
}

#[tokio::test]
async fn embedded_http_exposes_routing_and_health() {
    let dir = tempdir().unwrap();
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
    let server = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut client_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    client_params.is_ca = IsCa::NoCa;
    client_params.subject_alt_names.push(SanType::URI(
        Ia5String::try_from("spiffe://local/client").unwrap(),
    ));
    let client_cert = client_params.signed_by(&client_key, &ca, &ca_key).unwrap();

    let ca_pem = ca.pem();
    let server_pem = server.pem();
    let server_key_pem = server_key.serialize_pem();
    let client_pem = client_cert.pem();
    let client_key_pem = client_key.serialize_pem();
    let ca_path = dir.path().join("ca.pem");
    let server_cert_path = dir.path().join("server.pem");
    let server_key_path = dir.path().join("server.key");
    let client_cert_path = dir.path().join("client.pem");
    let client_key_path = dir.path().join("client.key");
    std::fs::write(&ca_path, ca_pem).unwrap();
    std::fs::write(&server_cert_path, server_pem).unwrap();
    std::fs::write(&server_key_path, server_key_pem).unwrap();
    std::fs::write(&client_cert_path, client_pem).unwrap();
    std::fs::write(&client_key_path, client_key_pem).unwrap();

    let seeds = ControlPlaneBootstrap {
        tenants: vec![quantum::config::BootstrapTenantConfig {
            tenant_id: "t".into(),
            tenant_prg_count: 1,
            protocols: Vec::new(),
            global_features: Vec::new(),
            active: true,
        }],
        placements: vec![BootstrapPlacementConfig {
            tenant_id: "t".into(),
            partition: 0,
            replicas: vec!["n0".into()],
        }],
    };
    let state =
        EmbeddedCpState::load_or_bootstrap(&dir.path().join("cp"), seeds, "n0".into(), 1).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (handle, addr) = start_embedded_cp_server(
        "127.0.0.1:0",
        server_cert_path.clone(),
        server_key_path.clone(),
        ca_path.clone(),
        state.clone(),
        shutdown_rx,
    )
    .await
    .unwrap();
    let base_url = format!("https://{}", addr);
    let identity =
        load_identity_from_pem(&client_cert_path, &client_key_path, Instant::now()).unwrap();
    let trust = load_trust_store_from_pem(&ca_path).unwrap();
    let transport = HttpCpTransportBuilder::new(&base_url)
        .unwrap()
        .identity(identity.clone())
        .trust_store(trust)
        .build()
        .unwrap();
    let mtls = MtlsIdentityManager::new(
        identity.certificate.clone(),
        identity.certificate.spiffe_id.trust_domain.clone(),
        Duration::from_secs(1),
        Instant::now(),
    );
    let mut client = clustor::control_plane::core::client::CpControlPlaneClient::new(
        transport,
        mtls,
        "/routing",
        "/features",
    );
    let mut placement_client =
        clustor::control_plane::core::placement::CpPlacementClient::new(Duration::from_secs(5));
    client
        .fetch_routing_bundle(&mut placement_client, Instant::now())
        .unwrap();
    assert!(!placement_client.records().is_empty());
    shutdown_tx.send(true).unwrap();
    let _ = handle.await;
}

#[test]
fn embedded_transport_short_circuits_routing_and_features() {
    let seeds = ControlPlaneBootstrap {
        tenants: vec![],
        placements: vec![BootstrapPlacementConfig {
            tenant_id: "t".into(),
            partition: 0,
            replicas: vec!["n0".into()],
        }],
    };
    let state = EmbeddedCpState::new(seeds, "n0".into(), 1);
    let cert = clustor::security::Certificate {
        spiffe_id: clustor::security::SpiffeId::parse("spiffe://local/server").unwrap(),
        serial: clustor::security::SerialNumber::from_u64(1),
        valid_from: Instant::now(),
        valid_until: Instant::now() + Duration::from_secs(60),
    };
    let transport = BoxTransport::new(quantum::control::embedded::EmbeddedCpTransport::new(
        state.clone(),
        cert.clone(),
    ));
    let res = transport.get("/routing").unwrap();
    let routes: Vec<clustor::control_plane::core::placement::PlacementRecord> =
        serde_json::from_slice(&res.body).unwrap();
    assert_eq!(routes.len(), 1);
    let res = transport.get("/features").unwrap();
    let manifest: clustor::control_plane::capabilities::FeatureManifest =
        serde_json::from_slice(&res.body).unwrap();
    assert_eq!(manifest.schema_version, 1);
    assert_eq!(res.server_certificate.serial, cert.serial);
}

#[test]
fn routing_snapshot_uses_placements_in_embedded_mode() {
    let cfg = ControlPlaneConfig {
        endpoints: vec![],
        cache_ttl_seconds: 1,
        mode: ControlPlaneMode::Embedded,
        ..Default::default()
    };
    let clock = TestClock::new();
    let client = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    client.apply_placement_records(
        [(
            "t:0".to_string(),
            clustor::control_plane::core::placement::PlacementRecord {
                partition_id: "t:0".into(),
                routing_epoch: 2,
                lease_epoch: 1,
                members: vec!["n0".into()],
            },
        )]
        .into_iter()
        .collect(),
        Instant::now(),
    );
    let snapshot = client.routing_snapshot();
    assert_eq!(snapshot.epoch.0, 2);
    assert_eq!(snapshot.placements.len(), 1);
}

#[test]
fn routing_snapshot_falls_back_in_external_mode() {
    let cfg = ControlPlaneConfig {
        endpoints: vec!["cp.local:19000".into()],
        cache_ttl_seconds: 1,
        mode: ControlPlaneMode::External,
        ..Default::default()
    };
    let clock = TestClock::new();
    let client = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    let snapshot = client.routing_snapshot();
    assert!(!snapshot.placements.is_empty());
}

#[test]
fn embedded_cache_fresh_with_placements_passes_read_gate() {
    let cfg = ControlPlaneConfig {
        endpoints: vec![],
        cache_ttl_seconds: 1,
        mode: ControlPlaneMode::Embedded,
        ..Default::default()
    };
    let clock = TestClock::new();
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
        Instant::now(),
    );
    assert!(client.cache_is_fresh());
    assert!(client.guard_read_gate(true).is_ok());
}

#[test]
fn embedded_cache_with_empty_placements_is_stale_not_strict() {
    let cfg = ControlPlaneConfig {
        endpoints: vec![],
        cache_ttl_seconds: 1,
        mode: ControlPlaneMode::Embedded,
        ..Default::default()
    };
    let clock = TestClock::new();
    let client = ControlPlaneClient::new(cfg, 1, clock).unwrap();
    assert!(!client.cache_is_fresh());
    assert!(!client.strict_fallback());
}

#[test]
fn external_cache_ttl_expiry_sets_strict_fallback() {
    let cfg = ControlPlaneConfig {
        endpoints: vec!["cp.local:19000".into()],
        cache_ttl_seconds: 0,
        mode: ControlPlaneMode::External,
        ..Default::default()
    };
    let clock = TestClock::new();
    let client = ControlPlaneClient::new(cfg, 1, clock.clone()).unwrap();
    clock.advance(Duration::from_millis(10));
    assert!(!client.cache_is_fresh());
    assert!(client.strict_fallback());
}

// ============================================================================
// CP Failover Tests for Mixed Protocol Configurations (Task 11)
// ============================================================================

use quantum::control::{
    CacheInvalidationCoordinator, InvalidationReason, ProtocolAssignment, ProtocolFeatureFlag,
    ProtocolType, RoutingCache, RoutingHint,
};

#[test]
fn capability_epoch_increments_on_protocol_assignment() {
    let client = ControlPlaneClient::new(default_cfg(), 1, TestClock::new()).unwrap();
    let initial_epoch = client.capability_epoch();

    // Assign a protocol to a tenant
    let assignment = ProtocolAssignment::new(ProtocolType::Mqtt);
    client.assign_protocol("tenant-1", assignment).unwrap();

    // Epoch should increment
    assert!(client.capability_epoch() > initial_epoch);
}

#[test]
fn capability_digest_changes_on_protocol_assignment() {
    let client = ControlPlaneClient::new(default_cfg(), 1, TestClock::new()).unwrap();

    // Assign first protocol
    client
        .assign_protocol("tenant-1", ProtocolAssignment::new(ProtocolType::Mqtt))
        .unwrap();
    client.update_capability_digest();
    let digest1 = client.capability_digest();

    // Assign second protocol to different tenant
    client
        .assign_protocol("tenant-2", ProtocolAssignment::new(ProtocolType::Mqtt))
        .unwrap();
    client.update_capability_digest();
    let digest2 = client.capability_digest();

    // Digest should change
    assert_ne!(digest1, digest2);
}

#[test]
fn cache_invalidation_detects_epoch_changes() {
    let client = ControlPlaneClient::new(default_cfg(), 1, TestClock::new()).unwrap();

    // First assignment to get non-zero epochs
    client
        .assign_protocol("tenant-1", ProtocolAssignment::new(ProtocolType::Mqtt))
        .unwrap();
    client.update_capability_digest();

    // Record epochs after first assignment (now non-zero)
    let (initial_cap_epoch, initial_routing_epoch, initial_digest, _) = client.get_epochs();
    assert!(
        initial_cap_epoch > 0,
        "capability epoch should be > 0 after first assignment"
    );

    // Assign another protocol to bump capability epoch again
    client
        .assign_protocol("tenant-2", ProtocolAssignment::new(ProtocolType::Mqtt))
        .unwrap();
    client.update_capability_digest();

    // Check for invalidation with old epochs
    let reasons =
        client.check_cache_invalidation(initial_cap_epoch, initial_routing_epoch.0, initial_digest);

    // Should detect capability epoch change
    assert!(reasons
        .iter()
        .any(|r| matches!(r, InvalidationReason::CapabilityEpochChanged)));
}

#[test]
fn routing_cache_invalidates_on_capability_epoch_update() {
    let mut cache = RoutingCache::new(Duration::from_secs(60));

    // Cache a placement at epoch 1
    cache.update_capability_epoch(1, 12345);
    let prg_id = PrgId {
        tenant_id: "tenant-1".to_string(),
        partition_index: 0,
    };
    cache.cache_placement(
        &prg_id,
        quantum::routing::PrgPlacement {
            node_id: "node-1".to_string(),
            replicas: vec!["node-1".to_string()],
        },
        "mqtt".to_string(),
        "1.0".to_string(),
        vec![],
    );

    assert_eq!(cache.len(), 1);

    // Update to epoch 2 - should invalidate stale entries
    let changed = cache.update_capability_epoch(2, 54321);
    assert!(changed);

    // Cache should be empty (stale entries removed)
    assert!(cache.is_empty());
}

#[test]
fn cache_invalidation_coordinator_tracks_changes() {
    let mut coord = CacheInvalidationCoordinator::new();

    // Initial state - no changes
    let reasons = coord.check_for_changes(0, 0, RoutingEpoch(0), 0);
    assert!(reasons.is_empty());

    // Bump capability epoch
    let reasons = coord.check_for_changes(1, 12345, RoutingEpoch(0), 0);
    assert!(reasons.contains(&InvalidationReason::CapabilityEpochChanged));

    // Bump routing epoch
    let reasons = coord.check_for_changes(1, 12345, RoutingEpoch(1), 0);
    assert!(reasons.contains(&InvalidationReason::RoutingEpochChanged));

    // Change digest
    let reasons = coord.check_for_changes(1, 99999, RoutingEpoch(1), 0);
    assert!(reasons.contains(&InvalidationReason::DigestMismatch));

    // Bump protocol revision
    let reasons = coord.check_for_changes(1, 99999, RoutingEpoch(1), 1);
    assert!(reasons.contains(&InvalidationReason::ProtocolRevisionChanged));
}

#[test]
fn protocol_assignment_preserves_through_client_refresh() {
    let client = ControlPlaneClient::new(default_cfg(), 1, TestClock::new()).unwrap();

    // Assign protocol
    let assignment =
        ProtocolAssignment::new(ProtocolType::Mqtt).with_feature(ProtocolFeatureFlag::OfflineQueue);
    client.assign_protocol("tenant-1", assignment).unwrap();

    // Verify assignment persists
    assert!(client.has_protocol("tenant-1", &ProtocolType::Mqtt));
    assert!(client.has_feature(
        "tenant-1",
        &ProtocolType::Mqtt,
        &ProtocolFeatureFlag::OfflineQueue
    ));

    // Mark cache refreshed (simulating CP reconnection)
    client.mark_refreshed();

    // Assignment should still be there
    assert!(client.has_protocol("tenant-1", &ProtocolType::Mqtt));
}

#[test]
fn multiple_tenant_protocols_tracked_correctly() {
    let client = ControlPlaneClient::new(default_cfg(), 1, TestClock::new()).unwrap();

    // Assign different protocols to different tenants
    client
        .assign_protocol("tenant-mqtt", ProtocolAssignment::new(ProtocolType::Mqtt))
        .unwrap();

    // Register AMQP as available first
    client.register_protocol(ProtocolType::Amqp);
    client
        .assign_protocol("tenant-amqp", ProtocolAssignment::new(ProtocolType::Amqp))
        .unwrap();

    // Verify each tenant has correct protocol
    assert!(client.has_protocol("tenant-mqtt", &ProtocolType::Mqtt));
    assert!(!client.has_protocol("tenant-mqtt", &ProtocolType::Amqp));

    assert!(client.has_protocol("tenant-amqp", &ProtocolType::Amqp));
    assert!(!client.has_protocol("tenant-amqp", &ProtocolType::Mqtt));

    // Query tenants by protocol
    let mqtt_tenants = client.tenants_with_protocol(&ProtocolType::Mqtt);
    assert!(mqtt_tenants.contains(&"tenant-mqtt".to_string()));
    assert!(!mqtt_tenants.contains(&"tenant-amqp".to_string()));
}

#[test]
fn get_epochs_returns_consistent_state() {
    let client = ControlPlaneClient::new(default_cfg(), 1, TestClock::new()).unwrap();

    // Assign protocol
    client
        .assign_protocol("tenant-1", ProtocolAssignment::new(ProtocolType::Mqtt))
        .unwrap();
    client.update_capability_digest();

    // Update routing epoch
    client.update_routing_epoch(RoutingEpoch(5));

    // Get all epochs
    let (cap_epoch, routing_epoch, digest, proto_rev) = client.get_epochs();

    assert!(cap_epoch > 0);
    assert_eq!(routing_epoch.0, 5);
    assert!(digest > 0);
    assert_eq!(proto_rev, 0); // Not explicitly set
}

#[test]
fn routing_hint_includes_capability_epoch() {
    let hint = RoutingHint::new("tenant-1", "mqtt")
        .with_capability_epoch(42)
        .with_hash_key("client-123");

    assert_eq!(hint.tenant_id, "tenant-1");
    assert_eq!(hint.workload_label, "mqtt");
    assert_eq!(hint.capability_epoch, 42);
    assert_eq!(hint.hash_key, Some("client-123".to_string()));
}
