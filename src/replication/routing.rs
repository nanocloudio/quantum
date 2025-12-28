use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hasher;

use thiserror::Error;
use twox_hash::XxHash64;

/// Routing epoch wrapper for clarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoutingEpoch(pub u64);

impl RoutingEpoch {
    pub fn incremented(self) -> Self {
        RoutingEpoch(self.0.saturating_add(1))
    }
}

/// PRG identifier: single-tenant partition index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PrgId {
    pub tenant_id: String,
    pub partition_index: u64,
}

/// Placement information for a PRG (host node identity placeholder).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrgPlacement {
    pub node_id: String,
    pub replicas: Vec<String>,
}

/// Routing table snapshot for a tenant ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingTable {
    pub epoch: RoutingEpoch,
    pub placements: HashMap<PrgId, PrgPlacement>,
}

impl RoutingTable {
    pub fn empty(epoch: RoutingEpoch) -> Self {
        Self {
            epoch,
            placements: HashMap::new(),
        }
    }

    pub fn update_epoch(&mut self, epoch: RoutingEpoch) {
        self.epoch = epoch;
    }
}

/// Forward sequence tracker to enforce monotonic forward_seq per (session_PRG, topic_PRG, routing_epoch).
#[derive(Debug, Default)]
pub struct ForwardSeqTracker {
    last_seq: HashMap<(String, String, u64), u64>,
}

/// Persisted forward sequence entry for checkpoint/WAL storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardSeqEntry {
    pub session_prg: String,
    pub topic_prg: String,
    pub routing_epoch: u64,
    pub last_seq: u64,
}

impl ForwardSeqTracker {
    pub fn next_seq(
        &mut self,
        session_prg: &str,
        topic_prg: &str,
        routing_epoch: RoutingEpoch,
    ) -> u64 {
        let key = (
            session_prg.to_string(),
            topic_prg.to_string(),
            routing_epoch.0,
        );
        let next = self
            .last_seq
            .get(&key)
            .cloned()
            .unwrap_or(0)
            .saturating_add(1);
        self.last_seq.insert(key, next);
        next
    }

    pub fn is_stale(
        &self,
        session_prg: &str,
        topic_prg: &str,
        routing_epoch: RoutingEpoch,
        seq: u64,
    ) -> bool {
        let key = (
            session_prg.to_string(),
            topic_prg.to_string(),
            routing_epoch.0,
        );
        if let Some(latest) = self.last_seq.get(&key) {
            seq < *latest
        } else {
            false
        }
    }

    pub fn snapshot(&self) -> Vec<ForwardSeqEntry> {
        self.last_seq
            .iter()
            .map(|((session, topic, epoch), last)| ForwardSeqEntry {
                session_prg: session.clone(),
                topic_prg: topic.clone(),
                routing_epoch: *epoch,
                last_seq: *last,
            })
            .collect()
    }

    pub fn hydrate(&mut self, entries: &[ForwardSeqEntry]) {
        self.last_seq.clear();
        for entry in entries {
            self.last_seq.insert(
                (
                    entry.session_prg.clone(),
                    entry.topic_prg.clone(),
                    entry.routing_epoch,
                ),
                entry.last_seq,
            );
        }
    }
}

#[derive(Debug, Error)]
pub enum RoutingError {
    #[error("routing epoch stale (local={local}, expected={expected})")]
    StaleEpoch { local: u64, expected: u64 },
    #[error("tenant mismatch for PRG operation")]
    TenantMismatch,
}

/// Compute a stable 64-bit hash for routing.
fn hash64(bytes: impl AsRef<[u8]>) -> u64 {
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(bytes.as_ref());
    hasher.finish()
}

/// Jump-consistent hash for tenant-local rings.
pub fn jump_consistent_hash(key: u64, buckets: u64) -> u64 {
    if buckets == 0 {
        return 0;
    }
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    let mut k = key;
    while j < buckets as i64 {
        b = j;
        k = k.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        j = (((b + 1) as f64) * (1u64 << 31) as f64 / (((k >> 33) + 1) as f64)).floor() as i64;
    }
    b as u64
}

/// Session partition = hash(tenant_id, client_id) % tenant_prg_count.
pub fn session_partition(tenant_id: &str, client_id: &str, tenant_prg_count: u64) -> u64 {
    let mut buf = Vec::new();
    buf.extend_from_slice(tenant_id.as_bytes());
    buf.push(0);
    buf.extend_from_slice(client_id.as_bytes());
    jump_consistent_hash(hash64(buf), tenant_prg_count)
}

/// Topic partition = hash(tenant_id, topic_normalized) % tenant_prg_count.
pub fn topic_partition(tenant_id: &str, topic_normalized: &str, tenant_prg_count: u64) -> u64 {
    let mut buf = Vec::new();
    buf.extend_from_slice(tenant_id.as_bytes());
    buf.push(0);
    buf.extend_from_slice(topic_normalized.as_bytes());
    jump_consistent_hash(hash64(buf), tenant_prg_count)
}

/// Ensure local routing epoch matches expected; stale epoch maps to PERMANENT_EPOCH.
pub fn ensure_routing_epoch(
    local: RoutingEpoch,
    expected: RoutingEpoch,
) -> Result<(), RoutingError> {
    if local == expected {
        Ok(())
    } else {
        Err(RoutingError::StaleEpoch {
            local: local.0,
            expected: expected.0,
        })
    }
}
