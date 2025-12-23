use anyhow::{Context, Result};
use clustor::net::tls::{load_identity_from_pem, load_trust_store_from_pem};
use rustls::ServerConfig;
use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;

/// Simple TLS config watcher used in tests to detect on-disk cert/key changes.
#[derive(Debug, Clone)]
pub struct ReloadableTlsConfig {
    chain_path: PathBuf,
    key_path: PathBuf,
    client_ca_path: PathBuf,
    alpn: Vec<String>,
    last_mtime: Option<SystemTime>,
}

impl ReloadableTlsConfig {
    pub fn new(
        chain_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
        client_ca_path: impl Into<PathBuf>,
        alpn: Vec<String>,
    ) -> Self {
        Self {
            chain_path: chain_path.into(),
            key_path: key_path.into(),
            client_ca_path: client_ca_path.into(),
            alpn,
            last_mtime: None,
        }
    }

    /// Returns true if the underlying files changed since the last check.
    pub fn changed(&mut self) -> Result<bool> {
        let mtime = latest_mtime(&self.chain_path, &self.key_path, &self.client_ca_path)?;
        let changed = self.last_mtime.map(|prev| prev != mtime).unwrap_or(true);
        self.last_mtime = Some(mtime);
        Ok(changed)
    }

    /// Rebuilds a rustls `ServerConfig` if any TLS material changed.
    pub fn reload_if_changed(&mut self) -> Result<Option<ServerConfig>> {
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

fn build_server_config(
    chain_path: &PathBuf,
    key_path: &PathBuf,
    client_ca_path: &PathBuf,
    alpn: &[String],
) -> Result<ServerConfig> {
    let now = std::time::Instant::now();
    let identity = load_identity_from_pem(chain_path, key_path, now)
        .map_err(|e| anyhow::anyhow!("load listener identity: {e}"))?;
    let trust = load_trust_store_from_pem(client_ca_path)
        .map_err(|e| anyhow::anyhow!("load listener trust store: {e}"))?;
    let mut cfg = identity
        .server_config(&trust)
        .map_err(|e| anyhow::anyhow!("build listener server config: {e}"))?;
    cfg.alpn_protocols = alpn.iter().map(|p| p.as_bytes().to_vec()).collect();
    cfg.max_early_data_size = 0;
    Ok(cfg)
}
