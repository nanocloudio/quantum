//! Common test harness utilities for integration tests.
//!
//! This module provides helpers for:
//! - Allocating ephemeral ports
//! - Creating temporary directories
//! - Generating ephemeral TLS materials
//! - Building test configurations
//!
//! All helpers use only `std` and existing dev-dependencies.

// Not all test files use all helpers; silence dead_code warnings for unused exports.
#![allow(dead_code)]

use rcgen::{BasicConstraints, CertificateParams, DnType, Ia5String, IsCa, KeyPair, SanType};
use std::convert::TryFrom;
use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};

/// Allocate an ephemeral loopback port. Returns the address with assigned port.
pub fn ephemeral_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("ephemeral addr")
}

/// TLS material paths returned by [`write_tls_materials`].
#[derive(Debug, Clone)]
pub struct TlsPaths {
    pub chain: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

/// Generate self-signed CA and leaf certificate for testing.
///
/// Creates:
/// - A self-signed CA certificate
/// - A leaf certificate signed by the CA with localhost SAN and SPIFFE URI
/// - Writes all materials to the given directory
///
/// Returns paths to (chain.pem, key.pem, ca.pem).
pub fn write_tls_materials(dir: &Path, trust_domain: &str) -> TlsPaths {
    // Generate CA
    let ca_key = KeyPair::generate().expect("generate CA key");
    let mut ca_params = CertificateParams::default();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "test-ca");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign CA");
    let ca_pem = ca_cert.pem();

    // Generate leaf certificate
    let leaf_key = KeyPair::generate().expect("generate leaf key");
    let mut leaf_params =
        CertificateParams::new(vec!["localhost".into()]).expect("leaf cert params");
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    leaf_params.subject_alt_names.push(SanType::URI(
        Ia5String::try_from(format!("spiffe://{trust_domain}/node")).expect("valid SPIFFE URI"),
    ));
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &ca_cert, &ca_key)
        .expect("sign leaf cert");

    // Build full chain (leaf + CA)
    let mut chain = leaf_cert.pem();
    chain.push_str(&ca_pem);

    // Write files
    let ca_path = dir.join("ca.pem");
    let chain_path = dir.join("chain.pem");
    let key_path = dir.join("key.pem");
    fs::write(&ca_path, &ca_pem).expect("write CA");
    fs::write(&chain_path, &chain).expect("write chain");
    fs::write(&key_path, leaf_key.serialize_pem()).expect("write key");

    TlsPaths {
        chain: chain_path,
        key: key_path,
        ca: ca_path,
    }
}

/// Build an embedded control-plane test configuration TOML string.
///
/// - `storage`: path to storage directory
/// - `tls`: TLS material paths from [`write_tls_materials`]
/// - `seeded`: whether to include bootstrap tenants/placements
/// - `replica_id`: replica identifier (defaults to "n0")
pub fn embedded_config_toml(
    storage: &Path,
    tls: &TlsPaths,
    seeded: bool,
    replica_id: &str,
) -> String {
    let seeds = if seeded {
        format!(
            r#"
bootstrap_tenants = [{{ tenant_id = "t", tenant_prg_count = 1 }}]
bootstrap_placements = [{{ tenant_id = "t", partition = 0, replicas = ["{replica_id}"] }}]
"#
        )
    } else {
        String::new()
    };

    format!(
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
replica_id = "{replica_id}"
initial_term = 1

[telemetry]
metrics_bind = "127.0.0.1:0"
log_level = "warn"

[tenants]
tenant_prg_count = 1

[paths]
storage_dir = "{storage}"
"#,
        chain = tls.chain.display(),
        key = tls.key.display(),
        ca = tls.ca.display(),
        storage = storage.display(),
        seeds = seeds,
        replica_id = replica_id
    )
}
