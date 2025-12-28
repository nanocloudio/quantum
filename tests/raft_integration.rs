//! End-to-end Raft wiring across three replicas using the built-in network stack.

mod common;

use anyhow::Result;
use common::{ephemeral_port, write_tls_materials};
use quantum::config::{CommitVisibility, DurabilityConfig, DurabilityMode};
use quantum::prg::PersistedPrgState;
use quantum::raft::{AckContract, PersistSnapshotOp, RaftOp};
use quantum::raft_replica::{RaftReplica, RaftReplicaConfig, RaftReplicaPaths};
use quantum::routing::{PrgId, PrgPlacement};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_raft_cluster_end_to_end() -> Result<()> {
    let trust_domain = "test.local";
    let temp = TempDir::new()?;
    let tls = write_tls_materials(temp.path(), trust_domain);
    let prg = PrgId {
        tenant_id: "tenant-a".into(),
        partition_index: 0,
    };
    let binds: Vec<_> = (0..3).map(|_| ephemeral_port()).collect();
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
            tls_chain: tls.chain.clone(),
            tls_key: tls.key.clone(),
            trust_bundle: tls.ca.clone(),
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
