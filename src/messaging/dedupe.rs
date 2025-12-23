use std::collections::HashMap;
use std::time::{Duration, Instant};

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Direction {
    Publish,
    Ack,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DedupeKey {
    pub tenant_id: String,
    pub client_id: String,
    pub session_epoch: u64,
    pub message_id: u16,
    pub direction: Direction,
}

#[derive(Debug, Clone)]
struct DedupeEntry {
    recorded_at: Instant,
    wal_index: u64,
}

/// Persisted view of a dedupe entry to allow WAL/snapshot storage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedDedupeEntry {
    pub tenant_id: String,
    pub client_id: String,
    pub session_epoch: u64,
    pub message_id: u16,
    pub direction: Direction,
    pub wal_index: u64,
}

/// Dedupe table enforcing per-session and per-tenant bounds.
pub struct DedupeTable {
    entries: HashMap<DedupeKey, DedupeEntry>,
    per_session_cap: usize,
    per_tenant_cap: usize,
    horizon: Duration,
}

#[derive(Debug, Error)]
pub enum DedupeError {
    #[error("dedupe cap exceeded for session")]
    SessionCap,
    #[error("dedupe cap exceeded for tenant")]
    TenantCap,
}

impl DedupeTable {
    pub fn new(per_session_cap: usize, per_tenant_cap: usize, horizon: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            per_session_cap,
            per_tenant_cap,
            horizon,
        }
    }

    pub fn is_duplicate(&self, key: &DedupeKey) -> bool {
        self.entries.contains_key(key)
    }

    pub fn record(&mut self, key: DedupeKey, wal_index: u64) -> Result<(), DedupeError> {
        self.evict_expired();
        if self.session_size(&key) >= self.per_session_cap {
            return Err(DedupeError::SessionCap);
        }
        if self.entries.len() >= self.per_tenant_cap {
            return Err(DedupeError::TenantCap);
        }
        self.entries.insert(
            key,
            DedupeEntry {
                recorded_at: Instant::now(),
                wal_index,
            },
        );
        Ok(())
    }

    pub fn record_or_update(&mut self, key: DedupeKey, wal_index: u64) -> Result<(), DedupeError> {
        self.evict_expired();
        if !self.entries.contains_key(&key) {
            if self.session_size(&key) >= self.per_session_cap {
                return Err(DedupeError::SessionCap);
            }
            if self.entries.len() >= self.per_tenant_cap {
                return Err(DedupeError::TenantCap);
            }
        }
        self.entries.insert(
            key,
            DedupeEntry {
                recorded_at: Instant::now(),
                wal_index,
            },
        );
        Ok(())
    }

    pub fn earliest_index(&self) -> u64 {
        self.entries
            .values()
            .map(|entry| entry.wal_index)
            .min()
            .unwrap_or(0)
    }

    pub fn update_wal_index(&mut self, key: &DedupeKey, wal_index: u64) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.wal_index = wal_index;
        }
    }

    pub fn wal_index(&self, key: &DedupeKey) -> Option<u64> {
        self.entries.get(key).map(|entry| entry.wal_index)
    }

    fn session_size(&self, key: &DedupeKey) -> usize {
        self.entries
            .keys()
            .filter(|k| {
                k.tenant_id == key.tenant_id
                    && k.client_id == key.client_id
                    && k.session_epoch == key.session_epoch
            })
            .count()
    }

    fn evict_expired(&mut self) {
        let now = Instant::now();
        self.entries
            .retain(|_, entry| now.duration_since(entry.recorded_at) <= self.horizon);
    }

    pub fn snapshot(&self) -> Vec<PersistedDedupeEntry> {
        self.entries
            .iter()
            .map(|(key, entry)| PersistedDedupeEntry {
                tenant_id: key.tenant_id.clone(),
                client_id: key.client_id.clone(),
                session_epoch: key.session_epoch,
                message_id: key.message_id,
                direction: key.direction.clone(),
                wal_index: entry.wal_index,
            })
            .collect()
    }

    pub fn hydrate(&mut self, entries: &[PersistedDedupeEntry], now: Instant) {
        self.entries.clear();
        for entry in entries {
            let key = DedupeKey {
                tenant_id: entry.tenant_id.clone(),
                client_id: entry.client_id.clone(),
                session_epoch: entry.session_epoch,
                message_id: entry.message_id,
                direction: entry.direction.clone(),
            };
            self.entries.insert(
                key,
                DedupeEntry {
                    recorded_at: now,
                    wal_index: entry.wal_index,
                },
            );
        }
    }
}
