use super::consensus::AckContract;
pub use super::replica::RaftReplica;
use super::replica::{RaftReplicaConfig, RaftReplicaPaths};
use crate::routing::{PrgId, PrgPlacement};
use std::path::PathBuf;
use std::sync::Arc;

/// Compatibility wrapper to build Raft replicas from placement data.
#[derive(Debug, Clone)]
pub struct RaftNetConfig {
    pub local_id: String,
    pub bind: Option<String>,
    pub tls_chain_path: PathBuf,
    pub tls_key_path: PathBuf,
    pub trust_bundle: PathBuf,
    pub trust_domain: String,
    pub log_dir: PathBuf,
}

/// Prepared Raft network resources to avoid re-parsing paths for each placement.
#[derive(Debug, Clone)]
pub struct RaftNetResources {
    inner: Arc<RaftNetConfig>,
}

impl RaftNetResources {
    pub fn new(cfg: RaftNetConfig) -> Self {
        Self {
            inner: Arc::new(cfg),
        }
    }

    pub fn paths(&self) -> RaftReplicaPaths {
        crate::raft_replica::RaftReplicaPaths {
            tls_chain: self.inner.tls_chain_path.clone(),
            tls_key: self.inner.tls_key_path.clone(),
            trust_bundle: self.inner.trust_bundle.clone(),
            trust_domain: self.inner.trust_domain.clone(),
            log_root: self.inner.log_dir.clone(),
            bind_override: self.inner.bind.clone(),
        }
    }

    pub fn start_replica(
        &self,
        prg: &PrgId,
        placement: &PrgPlacement,
        ack: Arc<AckContract>,
        routing_epoch: u64,
    ) -> Option<Arc<RaftReplica>> {
        let paths = self.paths();
        let cfg = RaftReplicaConfig::from_placement(
            prg.clone(),
            placement,
            &ack.durability_config(),
            paths,
            routing_epoch,
        );
        match RaftReplica::start(cfg) {
            Ok(replica) => Some(replica),
            Err(err) => {
                tracing::warn!("failed to start raft replica for {:?}: {err:?}", prg);
                None
            }
        }
    }
}
