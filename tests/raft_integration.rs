//! End-to-end Raft wiring across three replicas using the built-in network stack.
use anyhow::Result;
use quantum::config::{CommitVisibility, DurabilityConfig, DurabilityMode};
use quantum::prg::PersistedPrgState;
use quantum::raft::{AckContract, PersistSnapshotOp, RaftOp};
use quantum::raft_replica::{RaftReplica, RaftReplicaConfig, RaftReplicaPaths};
use quantum::routing::{PrgId, PrgPlacement};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType};
use std::convert::TryInto;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_raft_cluster_end_to_end() -> Result<()> {
    let trust_domain = "test.local";
    let temp = TempDir::new()?;
    let (chain, key, ca) = write_tls_materials(&temp, trust_domain)?;
    let prg = PrgId {
        tenant_id: "tenant-a".into(),
        partition_index: 0,
    };
    let binds: Vec<SocketAddr> = (0..3).map(|_| next_loopback()).collect();
    let placement = PrgPlacement {
        node_id: binds[0].to_string(),
        replicas: vec![binds[1].to_string(), binds[2].to_string()],
    };
    let mut nodes = Vec::new();
    for (idx, bind) in binds.iter().enumerate() {
        let mut durability = DurabilityConfig {
            durability_mode: DurabilityMode::Strict,
            commit_visibility: CommitVisibility::DurableOnly,
            quorum_size: 3,
            ..Default::default()
        };
        durability.replica_id = bind.to_string();
        let ack = Arc::new(AckContract::new(&durability));
        let paths = RaftReplicaPaths {
            tls_chain: chain.clone(),
            tls_key: key.clone(),
            trust_bundle: ca.clone(),
            trust_domain: trust_domain.to_string(),
            log_root: temp.path().join(format!("node_{idx}")),
            bind_override: None,
        };
        let cfg = RaftReplicaConfig::from_placement(prg.clone(), &placement, &durability, paths, 1);
        let replica = RaftReplica::start(cfg)?;
        ack.attach_raft(replica.clone());
        let applied: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let applied_hook = applied.clone();
        replica.set_apply_hook(move |_term, _idx, payload| {
            let applied_hook = applied_hook.clone();
            async move {
                if let Some(bytes) = payload {
                    applied_hook.lock().unwrap().push(bytes);
                }
            }
        });
        nodes.push((ack, replica, applied));
    }

    // Allow the network stack to settle before proposing.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Commit an initial snapshot from node 0 and wait for quorum apply.
    let first_payload = RaftOp::PersistSnapshot(PersistSnapshotOp {
        prg: prg.clone(),
        state: PersistedPrgState::default(),
    })
    .serialize()?;
    nodes[0]
        .0
        .commit_with(|_| async { Ok(first_payload.clone()) })
        .await?;
    for (ack, _, _) in &nodes {
        ack.wait_for_floor(1).await?;
        ack.update_clustor_floor(ack.committed_index());
    }

    // Demote the original leader and commit from a different replica to exercise leader change.
    nodes[0].0.step_down_for_upgrade().await?;
    let second_payload = RaftOp::PersistSnapshot(PersistSnapshotOp {
        prg: prg.clone(),
        state: PersistedPrgState::default(),
    })
    .serialize()?;
    nodes[1]
        .0
        .commit_with(|_| async { Ok(second_payload.clone()) })
        .await?;
    for (ack, _, _) in &nodes {
        ack.wait_for_floor(2).await?;
        if let Some(proof) = ack.latest_proof() {
            assert!(proof.index >= 2, "expected proof to reach index 2");
        }
        assert_eq!(ack.replication_lag_seconds(), 0);
    }

    // All replicas should have applied both payloads via the apply hooks.
    for (_ack, _replica, applied) in &nodes {
        let guard = applied.lock().unwrap();
        assert!(
            guard.len() >= 2,
            "expected two applied entries per replica, got {}",
            guard.len()
        );
    }

    // Fencing signals are still propagated after leadership transfer.
    for (ack, _, _) in &nodes {
        if let Some(proof) = ack.latest_proof() {
            ack.wait_for_floor(proof.index).await?;
        }
        ack.step_down_for_upgrade().await?;
        ack.set_durability_fence(true);
        assert!(ack.durability_fence_active());
    }

    Ok(())
}

fn next_loopback() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("ephemeral addr")
}

fn write_tls_materials(temp: &TempDir, trust_domain: &str) -> Result<(PathBuf, PathBuf, PathBuf)> {
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::default();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "test-ca");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let ca_pem = ca_cert.pem();
    let ca_path = temp.path().join("ca.pem");
    std::fs::write(&ca_path, &ca_pem)?;

    let leaf_key = KeyPair::generate()?;
    let mut leaf_params = CertificateParams::new(vec!["localhost".to_string()])?;
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    leaf_params.subject_alt_names.push(SanType::URI(
        format!("spiffe://{trust_domain}/node").try_into()?,
    ));
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key)?;
    let mut chain = leaf_cert.pem();
    chain.push_str(&ca_pem);
    let chain_path = temp.path().join("chain.pem");
    let key_path = temp.path().join("leaf.key");
    std::fs::write(&chain_path, &chain)?;
    std::fs::write(&key_path, leaf_key.serialize_pem())?;

    Ok((chain_path, key_path, ca_path))
}
