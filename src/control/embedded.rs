use crate::config::{BootstrapPlacementConfig, ControlPlaneBootstrap};
use anyhow::Result;
use clustor::control_plane::capabilities::FeatureManifest;
use clustor::control_plane::core::client::{
    CpApiTransport, CpClientError, TransportError, TransportResponse,
};
use clustor::control_plane::core::placement::PlacementRecord;
use clustor::net::tls::{load_identity_from_pem, load_trust_store_from_pem, TlsTrustStore};
use clustor::security::Certificate;
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use serde_json::to_vec;
use std::io::{BufRead, BufReader as StdBufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tokio::task::spawn_blocking;
use tokio::task::JoinHandle;

/// Shared embedded control-plane state for transport and HTTP server.
#[derive(Clone)]
pub struct EmbeddedCpState {
    routing: Arc<Vec<PlacementRecord>>,
    manifest: Arc<FeatureManifest>,
}

impl EmbeddedCpState {
    pub fn new(
        seeds: ControlPlaneBootstrap,
        default_replica: String,
        cache_ttl_seconds: u64,
    ) -> Self {
        let routing = seeds
            .placements
            .into_iter()
            .map(|p| placement_record(&p, cache_ttl_seconds))
            .collect::<Vec<_>>();
        let manifest = FeatureManifest {
            schema_version: 1,
            generated_at_ms: now_ms(),
            features: Vec::new(),
            signature: "local".into(),
        };
        let routing = if routing.is_empty() {
            vec![PlacementRecord {
                partition_id: "local:0".into(),
                routing_epoch: 1,
                lease_epoch: 1,
                members: vec![default_replica],
            }]
        } else {
            routing
        };
        Self {
            routing: Arc::new(routing),
            manifest: Arc::new(manifest),
        }
    }

    pub fn load_or_bootstrap(
        root: &std::path::Path,
        seeds: ControlPlaneBootstrap,
        default_replica: String,
        cache_ttl_seconds: u64,
    ) -> Result<Self> {
        let routing_path = root.join("routing.json");
        let manifest_path = root.join("feature_manifest.json");
        if routing_path.exists() && manifest_path.exists() {
            let routing: Vec<PlacementRecord> =
                serde_json::from_slice(&std::fs::read(&routing_path)?)?;
            let manifest: FeatureManifest =
                serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
            return Ok(Self {
                routing: Arc::new(routing),
                manifest: Arc::new(manifest),
            });
        }
        let state = Self::new(seeds, default_replica, cache_ttl_seconds);
        std::fs::create_dir_all(root)?;
        serde_json::to_writer(&mut std::fs::File::create(&routing_path)?, &*state.routing)?;
        serde_json::to_writer(
            &mut std::fs::File::create(&manifest_path)?,
            &*state.manifest,
        )?;
        Ok(state)
    }
}

/// Minimal in-process CP transport that serves routing/feature data from bootstrap seeds.
#[derive(Clone)]
pub struct EmbeddedCpTransport {
    state: EmbeddedCpState,
    server_certificate: Certificate,
}

impl EmbeddedCpTransport {
    pub fn new(state: EmbeddedCpState, server_certificate: Certificate) -> Self {
        Self {
            state,
            server_certificate,
        }
    }
}

impl CpApiTransport for EmbeddedCpTransport {
    fn get(&self, path: &str) -> Result<TransportResponse, CpClientError> {
        let body = match path {
            "/routing" => to_vec(&*self.state.routing)?,
            "/features" => to_vec(&*self.state.manifest)?,
            other => {
                return Err(CpClientError::Transport(TransportError::NoRoute {
                    path: other.to_string(),
                }))
            }
        };
        Ok(TransportResponse {
            body,
            server_certificate: self.server_certificate.clone(),
        })
    }
}

pub async fn start_embedded_cp_server(
    bind: &str,
    chain_path: PathBuf,
    key_path: PathBuf,
    client_ca_path: PathBuf,
    state: EmbeddedCpState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(JoinHandle<()>, SocketAddr)> {
    let addr: SocketAddr = bind.parse()?;
    let server_config = server_config(chain_path, key_path, client_ca_path)?;
    let state = Arc::new(state);
    tracing::info!("starting embedded control-plane server on {addr}");
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = spawn_blocking(move || {
        if let Err(err) = run_embedded_server(addr, server_config, state, &mut shutdown, Some(tx)) {
            tracing::warn!("embedded cp server exited with error: {err:?}");
        }
    });
    let bound = rx
        .recv()
        .map_err(|e| anyhow::anyhow!("failed to receive embedded cp bind: {e}"))?;
    tracing::info!("embedded control-plane listening on {bound}");
    Ok((handle, bound))
}

fn placement_record(seed: &BootstrapPlacementConfig, cache_ttl_seconds: u64) -> PlacementRecord {
    PlacementRecord {
        partition_id: format!("{}:{}", seed.tenant_id, seed.partition),
        routing_epoch: 1,
        lease_epoch: cache_ttl_seconds.max(1),
        members: if seed.replicas.is_empty() {
            vec![seed.tenant_id.clone()]
        } else {
            seed.replicas.clone()
        },
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as u64
}

fn run_embedded_server(
    addr: SocketAddr,
    server_config: ServerConfig,
    state: Arc<EmbeddedCpState>,
    shutdown: &mut watch::Receiver<bool>,
    announce: Option<std::sync::mpsc::Sender<SocketAddr>>,
) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    let bound = listener.local_addr()?;
    if let Some(tx) = announce {
        let _ = tx.send(bound);
    }
    tracing::info!("embedded cp server bound to {bound}");
    listener.set_nonblocking(true)?;
    let config = Arc::new(server_config);
    loop {
        if *shutdown.borrow() {
            tracing::info!("embedded control-plane server shutting down");
            break;
        }
        match listener.accept() {
            Ok((stream, _addr)) => {
                let config = config.clone();
                let state = state.clone();
                std::thread::spawn(move || {
                    if let Err(err) = handle_cp_request(stream, config, state) {
                        tracing::warn!("embedded cp request failed: {err:?}");
                    }
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(10));
                continue;
            }
            Err(err) => {
                tracing::warn!("embedded cp listener error: {err:?}");
                break;
            }
        }
    }
    Ok(())
}

fn handle_cp_request(
    mut stream: TcpStream,
    config: Arc<ServerConfig>,
    state: Arc<EmbeddedCpState>,
) -> Result<()> {
    let mut conn = ServerConnection::new(config).map_err(|e| anyhow::anyhow!(e))?;
    while conn.is_handshaking() {
        conn.complete_io(&mut stream)?;
    }
    let tls = StreamOwned::new(conn, stream);
    let mut reader = StdBufReader::new(tls);
    let mut first_line = String::new();
    reader.read_line(&mut first_line)?;
    let path = first_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_string();
    tracing::debug!("embedded cp request path={}", path);
    // Drain headers until blank line.
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 || line == "\r\n" {
            break;
        }
    }
    let (status, body) = match path.as_str() {
        "/routing" => (200, to_vec(&*state.routing)?),
        "/features" => (200, to_vec(&*state.manifest)?),
        "/healthz" => (200, b"{\"status\":\"ok\"}".to_vec()),
        _ => (404, b"{\"error\":\"not found\"}".to_vec()),
    };
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    let mut tls = reader.into_inner();
    tls.write_all(response.as_bytes())?;
    tls.write_all(&body)?;
    tls.flush()?;
    Ok(())
}

fn server_config(
    chain_path: PathBuf,
    key_path: PathBuf,
    client_ca_path: PathBuf,
) -> Result<ServerConfig> {
    let now = Instant::now();
    let identity = load_identity_from_pem(&chain_path, &key_path, now)?;
    let trust: TlsTrustStore = load_trust_store_from_pem(&client_ca_path)?;
    let cfg = identity.server_config(&trust)?;
    Ok(cfg)
}
