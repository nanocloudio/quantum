//! Protocol-aware routing cache with cache invalidation.
//!
//! Provides routing cache entries that include workload metadata,
//! capability epochs, and protocol-specific routing hints.

use crate::routing::{PrgId, PrgPlacement, RoutingEpoch};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Routing Hints
// ============================================================================

/// Protocol-agnostic routing hint for placement decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingHint {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Workload label (e.g., "mqtt", "amqp").
    pub workload_label: String,
    /// Workload version.
    pub workload_version: String,
    /// Capability epoch for this hint.
    pub capability_epoch: u64,
    /// Optional hash key for consistent hashing (e.g., client_id, topic).
    pub hash_key: Option<String>,
    /// Optional sticky token for session affinity.
    pub sticky_token: Option<u64>,
    /// Allowed transports for this routing.
    pub allowed_transports: Vec<String>,
    /// Required features for routing validation.
    pub required_features: Vec<String>,
}

impl RoutingHint {
    pub fn new(tenant_id: impl Into<String>, workload_label: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            workload_label: workload_label.into(),
            workload_version: "1.0".to_string(),
            capability_epoch: 0,
            hash_key: None,
            sticky_token: None,
            allowed_transports: vec!["tcp".to_string(), "quic".to_string()],
            required_features: Vec::new(),
        }
    }

    pub fn with_hash_key(mut self, key: impl Into<String>) -> Self {
        self.hash_key = Some(key.into());
        self
    }

    pub fn with_sticky_token(mut self, token: u64) -> Self {
        self.sticky_token = Some(token);
        self
    }

    pub fn with_capability_epoch(mut self, epoch: u64) -> Self {
        self.capability_epoch = epoch;
        self
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.workload_version = version.into();
        self
    }
}

// ============================================================================
// Routing Cache Entry
// ============================================================================

/// Cached routing entry with protocol metadata.
#[derive(Debug, Clone)]
pub struct RoutingCacheEntry {
    /// PRG placement information.
    pub placement: PrgPlacement,
    /// Workload label.
    pub workload_label: String,
    /// Workload version.
    pub workload_version: String,
    /// Protocol revision (schema version).
    pub protocol_revision: u64,
    /// Capability epoch when cached.
    pub capability_epoch: u64,
    /// Capability digest for validation.
    pub capability_digest: u64,
    /// Routing epoch when cached.
    pub routing_epoch: RoutingEpoch,
    /// Feature flags enabled for this placement.
    pub feature_flags: Vec<String>,
    /// When this entry was cached.
    pub cached_at: Instant,
    /// TTL for this entry.
    pub ttl: Duration,
}

impl RoutingCacheEntry {
    pub fn is_expired(&self) -> bool {
        self.cached_at.elapsed() > self.ttl
    }

    pub fn is_stale(
        &self,
        current_capability_epoch: u64,
        current_routing_epoch: RoutingEpoch,
    ) -> bool {
        self.capability_epoch < current_capability_epoch
            || self.routing_epoch.0 < current_routing_epoch.0
    }

    pub fn matches_hint(&self, hint: &RoutingHint) -> bool {
        self.workload_label == hint.workload_label
            && self.workload_version == hint.workload_version
            && self.capability_epoch >= hint.capability_epoch
    }
}

// ============================================================================
// Routing Cache
// ============================================================================

/// Cache key for routing lookups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoutingCacheKey {
    pub tenant_id: String,
    pub workload_label: String,
    pub partition_index: u64,
}

impl From<&PrgId> for RoutingCacheKey {
    fn from(prg_id: &PrgId) -> Self {
        Self {
            tenant_id: prg_id.tenant_id.clone(),
            workload_label: "mqtt".to_string(), // Default, should be set from context
            partition_index: prg_id.partition_index,
        }
    }
}

/// Routing cache statistics.
#[derive(Debug, Clone, Default)]
pub struct RoutingCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub stale_hits: u64,
    pub invalidations: u64,
}

impl RoutingCacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Reason for cache invalidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationReason {
    CapabilityEpochChanged,
    RoutingEpochChanged,
    ProtocolRevisionChanged,
    DigestMismatch,
    TtlExpired,
    Manual,
}

/// Protocol-aware routing cache.
#[derive(Debug)]
pub struct RoutingCache {
    /// Cache entries by key.
    entries: HashMap<RoutingCacheKey, RoutingCacheEntry>,
    /// Current capability epoch.
    capability_epoch: u64,
    /// Current capability digest.
    capability_digest: u64,
    /// Current routing epoch.
    routing_epoch: RoutingEpoch,
    /// Current protocol revision.
    protocol_revision: u64,
    /// Default TTL for entries.
    default_ttl: Duration,
    /// Statistics.
    stats: RoutingCacheStats,
    /// Invalidation listeners.
    invalidation_count: u64,
}

impl RoutingCache {
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            capability_epoch: 0,
            capability_digest: 0,
            routing_epoch: RoutingEpoch(0),
            protocol_revision: 0,
            default_ttl,
            stats: RoutingCacheStats::default(),
            invalidation_count: 0,
        }
    }

    /// Get a cache entry, tracking hit/miss stats.
    pub fn get(&mut self, key: &RoutingCacheKey) -> Option<&RoutingCacheEntry> {
        if let Some(entry) = self.entries.get(key) {
            if entry.is_expired() {
                self.stats.misses += 1;
                return None;
            }
            if entry.is_stale(self.capability_epoch, self.routing_epoch) {
                self.stats.stale_hits += 1;
            } else {
                self.stats.hits += 1;
            }
            Some(entry)
        } else {
            self.stats.misses += 1;
            None
        }
    }

    /// Get a cache entry without tracking stats.
    pub fn peek(&self, key: &RoutingCacheKey) -> Option<&RoutingCacheEntry> {
        self.entries.get(key).filter(|e| !e.is_expired())
    }

    /// Insert a cache entry.
    pub fn insert(&mut self, key: RoutingCacheKey, entry: RoutingCacheEntry) {
        self.entries.insert(key, entry);
    }

    /// Insert a placement with metadata.
    pub fn cache_placement(
        &mut self,
        prg_id: &PrgId,
        placement: PrgPlacement,
        workload_label: String,
        workload_version: String,
        feature_flags: Vec<String>,
    ) {
        let key = RoutingCacheKey {
            tenant_id: prg_id.tenant_id.clone(),
            workload_label: workload_label.clone(),
            partition_index: prg_id.partition_index,
        };

        let entry = RoutingCacheEntry {
            placement,
            workload_label,
            workload_version,
            protocol_revision: self.protocol_revision,
            capability_epoch: self.capability_epoch,
            capability_digest: self.capability_digest,
            routing_epoch: self.routing_epoch,
            feature_flags,
            cached_at: Instant::now(),
            ttl: self.default_ttl,
        };

        self.entries.insert(key, entry);
    }

    /// Remove a cache entry.
    pub fn remove(&mut self, key: &RoutingCacheKey) -> Option<RoutingCacheEntry> {
        self.entries.remove(key)
    }

    /// Invalidate entries for a specific tenant.
    pub fn invalidate_tenant(&mut self, tenant_id: &str, _reason: InvalidationReason) -> usize {
        let keys_to_remove: Vec<_> = self
            .entries
            .keys()
            .filter(|k| k.tenant_id == tenant_id)
            .cloned()
            .collect();

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.entries.remove(&key);
        }

        self.stats.invalidations += count as u64;
        self.invalidation_count += 1;
        count
    }

    /// Invalidate entries for a specific workload.
    pub fn invalidate_workload(
        &mut self,
        workload_label: &str,
        _reason: InvalidationReason,
    ) -> usize {
        let keys_to_remove: Vec<_> = self
            .entries
            .keys()
            .filter(|k| k.workload_label == workload_label)
            .cloned()
            .collect();

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.entries.remove(&key);
        }

        self.stats.invalidations += count as u64;
        self.invalidation_count += 1;
        count
    }

    /// Invalidate all stale entries.
    pub fn invalidate_stale(&mut self) -> usize {
        let keys_to_remove: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, e)| {
                e.is_stale(self.capability_epoch, self.routing_epoch) || e.is_expired()
            })
            .map(|(k, _)| k.clone())
            .collect();

        let count = keys_to_remove.len();
        for key in keys_to_remove {
            self.entries.remove(&key);
        }

        self.stats.invalidations += count as u64;
        count
    }

    /// Clear the entire cache.
    pub fn clear(&mut self) {
        let count = self.entries.len();
        self.entries.clear();
        self.stats.invalidations += count as u64;
        self.invalidation_count += 1;
    }

    /// Update the capability epoch, potentially invalidating entries.
    pub fn update_capability_epoch(&mut self, epoch: u64, digest: u64) -> bool {
        if epoch > self.capability_epoch || digest != self.capability_digest {
            let old_epoch = self.capability_epoch;
            self.capability_epoch = epoch;
            self.capability_digest = digest;

            // Invalidate entries with older epochs
            self.invalidate_stale();

            epoch > old_epoch
        } else {
            false
        }
    }

    /// Update the routing epoch.
    pub fn update_routing_epoch(&mut self, epoch: RoutingEpoch) -> bool {
        if epoch.0 > self.routing_epoch.0 {
            self.routing_epoch = epoch;
            self.invalidate_stale();
            true
        } else {
            false
        }
    }

    /// Update the protocol revision.
    pub fn update_protocol_revision(&mut self, revision: u64) -> bool {
        if revision > self.protocol_revision {
            self.protocol_revision = revision;
            // Protocol revision changes invalidate all entries
            self.clear();
            true
        } else {
            false
        }
    }

    /// Get current epochs.
    pub fn epochs(&self) -> (u64, RoutingEpoch, u64) {
        (
            self.capability_epoch,
            self.routing_epoch,
            self.protocol_revision,
        )
    }

    /// Get cache statistics.
    pub fn stats(&self) -> &RoutingCacheStats {
        &self.stats
    }

    /// Get entry count.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get invalidation count (for telemetry).
    pub fn invalidation_count(&self) -> u64 {
        self.invalidation_count
    }
}

// ============================================================================
// Cache Invalidation Coordinator
// ============================================================================

/// Coordinates cache invalidation across components.
#[derive(Debug)]
pub struct CacheInvalidationCoordinator {
    /// Last known capability epoch.
    last_capability_epoch: u64,
    /// Last known capability digest.
    last_capability_digest: u64,
    /// Last known routing epoch.
    last_routing_epoch: RoutingEpoch,
    /// Last known protocol revision.
    last_protocol_revision: u64,
    /// Total invalidation events.
    invalidation_events: u64,
}

impl Default for CacheInvalidationCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheInvalidationCoordinator {
    pub fn new() -> Self {
        Self {
            last_capability_epoch: 0,
            last_capability_digest: 0,
            last_routing_epoch: RoutingEpoch(0),
            last_protocol_revision: 0,
            invalidation_events: 0,
        }
    }

    /// Check if any epochs have changed and return invalidation reasons.
    pub fn check_for_changes(
        &mut self,
        capability_epoch: u64,
        capability_digest: u64,
        routing_epoch: RoutingEpoch,
        protocol_revision: u64,
    ) -> Vec<InvalidationReason> {
        let mut reasons = Vec::new();

        if capability_epoch > self.last_capability_epoch {
            reasons.push(InvalidationReason::CapabilityEpochChanged);
            self.last_capability_epoch = capability_epoch;
            self.invalidation_events += 1;
        }

        if capability_digest != self.last_capability_digest && self.last_capability_digest != 0 {
            reasons.push(InvalidationReason::DigestMismatch);
            self.last_capability_digest = capability_digest;
            self.invalidation_events += 1;
        } else {
            self.last_capability_digest = capability_digest;
        }

        if routing_epoch.0 > self.last_routing_epoch.0 {
            reasons.push(InvalidationReason::RoutingEpochChanged);
            self.last_routing_epoch = routing_epoch;
            self.invalidation_events += 1;
        }

        if protocol_revision > self.last_protocol_revision {
            reasons.push(InvalidationReason::ProtocolRevisionChanged);
            self.last_protocol_revision = protocol_revision;
            self.invalidation_events += 1;
        }

        reasons
    }

    /// Get total invalidation events.
    pub fn invalidation_events(&self) -> u64 {
        self.invalidation_events
    }

    /// Get current state.
    pub fn state(&self) -> (u64, u64, RoutingEpoch, u64) {
        (
            self.last_capability_epoch,
            self.last_capability_digest,
            self.last_routing_epoch,
            self.last_protocol_revision,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routing_hint() {
        let hint = RoutingHint::new("tenant-1", "mqtt")
            .with_hash_key("client-123")
            .with_sticky_token(42)
            .with_capability_epoch(5);

        assert_eq!(hint.tenant_id, "tenant-1");
        assert_eq!(hint.workload_label, "mqtt");
        assert_eq!(hint.hash_key, Some("client-123".to_string()));
        assert_eq!(hint.sticky_token, Some(42));
        assert_eq!(hint.capability_epoch, 5);
    }

    #[test]
    fn test_routing_cache_hit_miss() {
        let mut cache = RoutingCache::new(Duration::from_secs(60));

        let key = RoutingCacheKey {
            tenant_id: "tenant-1".to_string(),
            workload_label: "mqtt".to_string(),
            partition_index: 0,
        };

        // Miss on empty cache
        assert!(cache.get(&key).is_none());
        assert_eq!(cache.stats().misses, 1);

        // Insert and hit
        let prg_id = PrgId {
            tenant_id: "tenant-1".to_string(),
            partition_index: 0,
        };
        cache.cache_placement(
            &prg_id,
            PrgPlacement {
                node_id: "node-1".to_string(),
                replicas: vec!["node-1".to_string()],
            },
            "mqtt".to_string(),
            "1.0".to_string(),
            vec![],
        );

        assert!(cache.get(&key).is_some());
        assert_eq!(cache.stats().hits, 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let mut cache = RoutingCache::new(Duration::from_secs(60));

        let prg_id = PrgId {
            tenant_id: "tenant-1".to_string(),
            partition_index: 0,
        };
        cache.cache_placement(
            &prg_id,
            PrgPlacement {
                node_id: "node-1".to_string(),
                replicas: vec!["node-1".to_string()],
            },
            "mqtt".to_string(),
            "1.0".to_string(),
            vec![],
        );

        assert_eq!(cache.len(), 1);

        // Invalidate by tenant
        let count = cache.invalidate_tenant("tenant-1", InvalidationReason::Manual);
        assert_eq!(count, 1);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_capability_epoch_update() {
        let mut cache = RoutingCache::new(Duration::from_secs(60));

        // Initial epoch
        cache.update_capability_epoch(1, 12345);

        let prg_id = PrgId {
            tenant_id: "tenant-1".to_string(),
            partition_index: 0,
        };
        cache.cache_placement(
            &prg_id,
            PrgPlacement {
                node_id: "node-1".to_string(),
                replicas: vec!["node-1".to_string()],
            },
            "mqtt".to_string(),
            "1.0".to_string(),
            vec![],
        );

        // Update epoch - should invalidate stale entries
        let changed = cache.update_capability_epoch(2, 54321);
        assert!(changed);

        // Entry should be gone (stale)
        assert!(cache.is_empty());
    }

    #[test]
    fn test_invalidation_coordinator() {
        let mut coord = CacheInvalidationCoordinator::new();

        // No changes initially
        let reasons = coord.check_for_changes(0, 0, RoutingEpoch(0), 0);
        assert!(reasons.is_empty());

        // Capability epoch change
        let reasons = coord.check_for_changes(1, 12345, RoutingEpoch(0), 0);
        assert!(reasons.contains(&InvalidationReason::CapabilityEpochChanged));

        // Routing epoch change
        let reasons = coord.check_for_changes(1, 12345, RoutingEpoch(1), 0);
        assert!(reasons.contains(&InvalidationReason::RoutingEpochChanged));

        // Protocol revision change
        let reasons = coord.check_for_changes(1, 12345, RoutingEpoch(1), 1);
        assert!(reasons.contains(&InvalidationReason::ProtocolRevisionChanged));

        // Digest mismatch
        let reasons = coord.check_for_changes(1, 99999, RoutingEpoch(1), 1);
        assert!(reasons.contains(&InvalidationReason::DigestMismatch));
    }
}
