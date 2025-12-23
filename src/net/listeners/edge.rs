#[cfg(feature = "quic")]
use crate::config::QuicConfig;
use crate::config::TcpConfig;
use anyhow::{Context, Result};
use clustor::net::tls::{load_identity_from_pem, load_trust_store_from_pem};
use clustor::net::NetError;
use rustls::ServerConfig;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_rustls::TlsAcceptor;

#[derive(Debug, Clone)]
pub(crate) struct EdgeIdentity {
    pub tenant_id: String,
    pub sni: Option<String>,
    pub alpn: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct EdgePolicy {
    alpn: Vec<String>,
    require_sni: bool,
    require_alpn: bool,
    default_tenant: Option<String>,
}

impl EdgePolicy {
    pub fn from_tcp_config(cfg: &TcpConfig) -> Self {
        Self {
            alpn: cfg.alpn.clone(),
            require_sni: cfg.require_sni,
            require_alpn: cfg.require_alpn,
            default_tenant: cfg.default_tenant.clone(),
        }
    }

    #[cfg(feature = "quic")]
    pub fn from_quic_config(cfg: &QuicConfig) -> Self {
        Self {
            alpn: cfg.alpn.clone(),
            require_sni: cfg.require_sni,
            require_alpn: cfg.require_alpn,
            default_tenant: cfg.default_tenant.clone(),
        }
    }

    pub fn resolve(
        &self,
        sni: Option<String>,
        mut alpn: Option<String>,
    ) -> Result<EdgeIdentity, EdgeError> {
        if let Some(proto) = &alpn {
            if !self.alpn.is_empty() && !self.alpn.iter().any(|allowed| allowed == proto) {
                return Err(EdgeError::UnsupportedAlpn(proto.clone()));
            }
        } else if self.require_alpn {
            return Err(EdgeError::MissingAlpn);
        } else if let Some(fallback) = self.alpn.first() {
            alpn = Some(fallback.clone());
        }
        let tenant_id = match sni.clone() {
            Some(name) => name,
            None if !self.require_sni => self
                .default_tenant
                .clone()
                .ok_or(EdgeError::MissingTenant)?,
            None => return Err(EdgeError::MissingSni),
        };
        Ok(EdgeIdentity {
            tenant_id,
            sni,
            alpn,
        })
    }
}

pub(crate) struct EdgeTlsEndpoint {
    acceptor: Arc<RwLock<TlsAcceptor>>,
    policy: EdgePolicy,
}

impl EdgeTlsEndpoint {
    pub fn new(cfg: &TcpConfig) -> Result<Self> {
        let mut reload = ListenerTlsReloader::new(
            cfg.tls_chain_path.clone(),
            cfg.tls_key_path.clone(),
            cfg.client_ca_path.clone(),
            cfg.alpn.clone(),
        );
        let tls_config = reload
            .reload_if_changed()?
            .ok_or_else(|| anyhow::anyhow!("invalid initial TLS listener config"))?;
        let acceptor = Arc::new(RwLock::new(TlsAcceptor::from(Arc::new(tls_config))));
        spawn_tls_hot_reload(acceptor.clone(), reload);
        Ok(Self {
            acceptor,
            policy: EdgePolicy::from_tcp_config(cfg),
        })
    }

    pub async fn accept(
        &self,
        stream: TcpStream,
        peer: std::net::SocketAddr,
    ) -> Result<EdgeAcceptedStream, EdgeError> {
        let tls_stream = self
            .acceptor
            .read()
            .await
            .accept(stream)
            .await
            .map_err(|err| EdgeError::Tls(format!("{err:?}")))?;
        let (_, server_conn) = tls_stream.get_ref();
        let raw_sni = server_conn.server_name().map(|s| s.to_string());
        let raw_alpn = server_conn
            .alpn_protocol()
            .map(|p| String::from_utf8_lossy(p).to_string());
        let missing_alpn = raw_alpn.is_none();
        let missing_sni = raw_sni.is_none();
        let identity = self.policy.resolve(raw_sni, raw_alpn)?;
        if missing_alpn {
            if let Some(alpn) = &identity.alpn {
                tracing::warn!(
                    "missing ALPN from {peer}; falling back to {alpn} per configuration"
                );
            }
        }
        if missing_sni {
            tracing::warn!(
                "missing SNI from {peer}; defaulting to tenant {} per configuration",
                identity.tenant_id
            );
        }
        Ok(EdgeAcceptedStream {
            stream: tls_stream,
            peer_addr: peer,
            identity,
        })
    }
}

pub(crate) struct EdgeAcceptedStream {
    pub stream: tokio_rustls::server::TlsStream<TcpStream>,
    pub peer_addr: std::net::SocketAddr,
    pub identity: EdgeIdentity,
}

#[derive(Debug, Error)]
pub enum EdgeError {
    #[error("missing ALPN from peer")]
    MissingAlpn,
    #[error("unsupported ALPN {0}")]
    UnsupportedAlpn(String),
    #[error("missing SNI")]
    MissingSni,
    #[error("missing tenant and no default configured")]
    MissingTenant,
    #[error("TLS handshake failed: {0}")]
    Tls(String),
}

#[derive(Debug, Clone)]
pub(crate) struct ListenerTlsReloader {
    chain_path: PathBuf,
    key_path: PathBuf,
    client_ca_path: PathBuf,
    alpn: Vec<String>,
    last_mtime: Option<SystemTime>,
}

impl ListenerTlsReloader {
    pub fn new(
        chain_path: PathBuf,
        key_path: PathBuf,
        client_ca_path: PathBuf,
        alpn: Vec<String>,
    ) -> Self {
        Self {
            chain_path,
            key_path,
            client_ca_path,
            alpn,
            last_mtime: None,
        }
    }

    fn changed(&mut self) -> Result<bool> {
        let mtime = latest_mtime(&self.chain_path, &self.key_path, &self.client_ca_path)?;
        let changed = self.last_mtime.map(|prev| prev != mtime).unwrap_or(true);
        self.last_mtime = Some(mtime);
        Ok(changed)
    }

    pub fn reload_if_changed(&mut self) -> Result<Option<rustls::ServerConfig>> {
        if self.changed()? {
            let cfg = build_server_config(
                &self.chain_path,
                &self.key_path,
                &self.client_ca_path,
                &self.alpn,
            )?;
            return Ok(Some(cfg));
        }
        Ok(None)
    }

    #[cfg(feature = "quic")]
    pub fn reload_quic_if_changed(
        &mut self,
        quic_cfg: &crate::config::QuicConfig,
    ) -> Result<Option<quinn::ServerConfig>> {
        if self.changed()? {
            let cfg = super::build_quic_server_config(
                &self.chain_path,
                &self.key_path,
                &self.client_ca_path,
                quic_cfg,
            )?;
            return Ok(Some(cfg));
        }
        Ok(None)
    }
}

fn build_server_config(
    chain_path: &PathBuf,
    key_path: &PathBuf,
    client_ca_path: &PathBuf,
    alpn: &[String],
) -> Result<ServerConfig> {
    let now = Instant::now();
    let identity = load_identity_from_pem(chain_path, key_path, now)
        .map_err(|e| anyhow::anyhow!("load listener identity: {e}"))?;
    let trust = load_trust_store_from_pem(client_ca_path)
        .map_err(|e| anyhow::anyhow!("load listener trust store: {e}"))?;
    let mut cfg = identity
        .server_config(&trust)
        .map_err(|e| map_net_error("build listener server config", e))?;
    cfg.alpn_protocols = alpn.iter().map(|p| p.as_bytes().to_vec()).collect();
    cfg.max_early_data_size = 0;
    Ok(cfg)
}

pub(crate) fn spawn_tls_hot_reload(
    acceptor: Arc<RwLock<TlsAcceptor>>,
    mut reloadable: ListenerTlsReloader,
) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            match reloadable.reload_if_changed() {
                Ok(Some(cfg)) => {
                    let mut guard = acceptor.write().await;
                    *guard = TlsAcceptor::from(Arc::new(cfg));
                    tracing::info!("reloaded TLS listener certificates");
                }
                Ok(None) => {}
                Err(err) => tracing::warn!("tls reload failed: {err:?}"),
            }
        }
    });
}

fn latest_mtime(chain: &PathBuf, key: &PathBuf, ca: &PathBuf) -> Result<SystemTime> {
    let chain_mt = fs::metadata(chain)
        .with_context(|| format!("stat chain {}", chain.display()))?
        .modified()
        .with_context(|| format!("mtime chain {}", chain.display()))?;
    let key_mt = fs::metadata(key)
        .with_context(|| format!("stat key {}", key.display()))?
        .modified()
        .with_context(|| format!("mtime key {}", key.display()))?;
    let ca_mt = fs::metadata(ca)
        .with_context(|| format!("stat ca {}", ca.display()))?
        .modified()
        .with_context(|| format!("mtime ca {}", ca.display()))?;
    Ok(*[chain_mt, key_mt, ca_mt].iter().max().unwrap_or(&chain_mt))
}

fn map_net_error(ctx: &str, err: NetError) -> anyhow::Error {
    anyhow::anyhow!("{}: {err}", ctx)
}
