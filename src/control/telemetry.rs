//! Control-plane telemetry for workload counts and capability metrics.
//!
//! Provides metrics for tracking workload distribution, capability epochs,
//! cache invalidations, and protocol assignments.

use super::capabilities::CapabilityRegistry;
use clustor::telemetry::SharedMetricsRegistry;
use std::collections::HashMap;

/// Telemetry collector for control-plane workload metrics.
#[derive(Debug)]
pub struct CpWorkloadTelemetry {
    metrics: SharedMetricsRegistry,
}

impl CpWorkloadTelemetry {
    pub fn new(prefix: &str) -> Self {
        Self {
            metrics: SharedMetricsRegistry::new(prefix),
        }
    }

    pub fn with_registry(metrics: SharedMetricsRegistry) -> Self {
        Self { metrics }
    }

    /// Update workload tenant counts from capability registry.
    pub fn update_workload_counts(&self, registry: &CapabilityRegistry) {
        // Count tenants per protocol
        let mut protocol_tenant_counts: HashMap<String, u64> = HashMap::new();

        for tenant in registry.active_tenants() {
            for (protocol, assignment) in &tenant.protocols {
                if assignment.enabled {
                    let key = protocol.as_str().to_string();
                    *protocol_tenant_counts.entry(key.clone()).or_insert(0) += 1;
                }
            }
        }

        // Update metrics
        for (protocol, count) in protocol_tenant_counts {
            let _ = self
                .metrics
                .set_gauge(format!("workload_tenants_total_{}", protocol), count);
        }

        // Update total protocol assignments
        let _ = self.metrics.set_gauge(
            "protocol_assignments_total",
            registry
                .active_tenants()
                .iter()
                .map(|t| t.protocols.len() as u64)
                .sum(),
        );
    }

    /// Update capability epoch metrics.
    pub fn update_capability_epoch(&self, epoch: u64, digest: u64) {
        let _ = self.metrics.set_gauge("capability_epoch", epoch);
        let _ = self.metrics.set_gauge("capability_digest", digest);
    }

    /// Record a cache invalidation event.
    pub fn record_cache_invalidation(&self, reason: &str) {
        let _ = self
            .metrics
            .inc_counter(format!("cache_invalidations_total_{}", reason), 1);
    }

    /// Record a capability refresh.
    pub fn record_capability_refresh(&self, duration_ms: u64) {
        let _ = self.metrics.inc_counter("capability_refresh_total", 1);
        let _ = self
            .metrics
            .set_gauge("capability_refresh_duration_ms", duration_ms);
    }

    /// Record a digest mismatch.
    pub fn record_digest_mismatch(&self) {
        let _ = self
            .metrics
            .inc_counter("capability_digest_mismatch_total", 1);
    }

    /// Update PRG counts per workload.
    pub fn update_prg_counts(&self, workload_prg_counts: &HashMap<String, u64>) {
        for (workload, count) in workload_prg_counts {
            let _ = self
                .metrics
                .set_gauge(format!("workload_prgs_total_{}", workload), *count);
        }
    }

    /// Update routing cache metrics.
    pub fn update_routing_cache_metrics(&self, hits: u64, misses: u64, stale: u64) {
        let _ = self.metrics.set_gauge("routing_cache_hits", hits);
        let _ = self.metrics.set_gauge("routing_cache_misses", misses);
        let _ = self.metrics.set_gauge("routing_cache_stale", stale);
    }

    /// Record a hardware mismatch.
    pub fn record_hardware_mismatch(&self, node_id: &str, workload: &str) {
        let _ = self.metrics.inc_counter(
            format!("workload_hardware_mismatch_total_{}_{}", workload, node_id),
            1,
        );
    }

    /// Get the underlying metrics registry.
    pub fn metrics(&self) -> &SharedMetricsRegistry {
        &self.metrics
    }
}

/// Workload placement metrics.
#[derive(Debug, Clone, Default)]
pub struct WorkloadPlacementMetrics {
    /// Tenants per workload.
    pub tenants_by_workload: HashMap<String, u64>,
    /// PRGs per workload.
    pub prgs_by_workload: HashMap<String, u64>,
    /// PRGs per node.
    pub prgs_by_node: HashMap<String, u64>,
    /// Tenants using dedicated PRGs.
    pub dedicated_prg_tenants: u64,
    /// Current capability epoch.
    pub capability_epoch: u64,
    /// Current routing epoch.
    pub routing_epoch: u64,
    /// Current protocol revision.
    pub protocol_revision: u64,
}

impl WorkloadPlacementMetrics {
    /// Create from capability registry and placements.
    pub fn from_registry(
        registry: &CapabilityRegistry,
        placements: &HashMap<crate::routing::PrgId, crate::routing::PrgPlacement>,
    ) -> Self {
        let mut metrics = Self {
            capability_epoch: registry.epoch(),
            ..Default::default()
        };

        // Count tenants and PRGs by workload
        for tenant in registry.active_tenants() {
            for (protocol, assignment) in &tenant.protocols {
                if assignment.enabled {
                    let key = protocol.as_str().to_string();
                    *metrics.tenants_by_workload.entry(key.clone()).or_insert(0) += 1;

                    // Count dedicated PRG tenants
                    if assignment.prg_affinity.is_some() {
                        metrics.dedicated_prg_tenants += 1;
                    }
                }
            }
        }

        // Count PRGs by node
        for placement in placements.values() {
            *metrics
                .prgs_by_node
                .entry(placement.node_id.clone())
                .or_insert(0) += 1;
        }

        metrics
    }

    /// Export to telemetry collector.
    pub fn export(&self, telemetry: &CpWorkloadTelemetry) {
        telemetry.update_capability_epoch(self.capability_epoch, 0);
        telemetry.update_prg_counts(&self.prgs_by_workload);

        for (workload, count) in &self.tenants_by_workload {
            let _ = telemetry
                .metrics()
                .set_gauge(format!("workload_tenants_{}", workload), *count);
        }

        for (node, count) in &self.prgs_by_node {
            let _ = telemetry
                .metrics()
                .set_gauge(format!("prgs_on_node_{}", node), *count);
        }

        let _ = telemetry
            .metrics()
            .set_gauge("dedicated_prg_tenants", self.dedicated_prg_tenants);
    }
}

/// Alert thresholds for control-plane metrics.
#[derive(Debug, Clone)]
pub struct CpAlertThresholds {
    /// Alert if PRG count drops by more than this percentage.
    pub prg_drop_threshold_pct: f64,
    /// Alert if capability epoch hasn't changed in this many seconds.
    pub stale_epoch_seconds: u64,
    /// Alert if digest mismatches exceed this count.
    pub max_digest_mismatches: u64,
    /// Alert if cache invalidation rate exceeds this per minute.
    pub max_invalidations_per_minute: u64,
}

impl Default for CpAlertThresholds {
    fn default() -> Self {
        Self {
            prg_drop_threshold_pct: 10.0,
            stale_epoch_seconds: 3600, // 1 hour
            max_digest_mismatches: 0,
            max_invalidations_per_minute: 100,
        }
    }
}

/// Alert state for control-plane metrics.
#[derive(Debug, Clone, Default)]
pub struct CpAlertState {
    /// Previous PRG count for drop detection.
    pub prev_prg_count: u64,
    /// Last capability epoch change time (Unix seconds).
    pub last_epoch_change: u64,
    /// Current digest mismatch count.
    pub digest_mismatch_count: u64,
    /// Invalidations in current minute.
    pub invalidations_this_minute: u64,
    /// Active alerts.
    pub active_alerts: Vec<CpAlert>,
}

/// Control-plane alert.
#[derive(Debug, Clone)]
pub struct CpAlert {
    pub name: String,
    pub severity: AlertSeverity,
    pub message: String,
    pub fired_at: u64,
}

/// Alert severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Warning,
    Critical,
}

impl CpAlertState {
    /// Check thresholds and update alert state.
    pub fn check_thresholds(
        &mut self,
        metrics: &WorkloadPlacementMetrics,
        thresholds: &CpAlertThresholds,
        now_unix: u64,
    ) {
        self.active_alerts.clear();

        // Check PRG drop
        let total_prgs: u64 = metrics.prgs_by_workload.values().sum();
        if self.prev_prg_count > 0 {
            let drop_pct = (self.prev_prg_count as f64 - total_prgs as f64)
                / self.prev_prg_count as f64
                * 100.0;
            if drop_pct > thresholds.prg_drop_threshold_pct {
                self.active_alerts.push(CpAlert {
                    name: "prg_count_drop".to_string(),
                    severity: AlertSeverity::Warning,
                    message: format!(
                        "PRG count dropped by {:.1}% (from {} to {})",
                        drop_pct, self.prev_prg_count, total_prgs
                    ),
                    fired_at: now_unix,
                });
            }
        }
        self.prev_prg_count = total_prgs;

        // Check stale epoch
        if self.last_epoch_change > 0 {
            let age = now_unix.saturating_sub(self.last_epoch_change);
            if age > thresholds.stale_epoch_seconds {
                self.active_alerts.push(CpAlert {
                    name: "stale_capability_epoch".to_string(),
                    severity: AlertSeverity::Warning,
                    message: format!("Capability epoch hasn't changed in {} seconds", age),
                    fired_at: now_unix,
                });
            }
        }

        // Check digest mismatches
        if self.digest_mismatch_count > thresholds.max_digest_mismatches {
            self.active_alerts.push(CpAlert {
                name: "digest_mismatch".to_string(),
                severity: AlertSeverity::Critical,
                message: format!(
                    "Capability digest mismatches detected: {}",
                    self.digest_mismatch_count
                ),
                fired_at: now_unix,
            });
        }

        // Check invalidation rate
        if self.invalidations_this_minute > thresholds.max_invalidations_per_minute {
            self.active_alerts.push(CpAlert {
                name: "high_invalidation_rate".to_string(),
                severity: AlertSeverity::Warning,
                message: format!(
                    "Cache invalidation rate too high: {} per minute",
                    self.invalidations_this_minute
                ),
                fired_at: now_unix,
            });
        }
    }

    /// Record a capability epoch change.
    pub fn record_epoch_change(&mut self, now_unix: u64) {
        self.last_epoch_change = now_unix;
    }

    /// Record a digest mismatch.
    pub fn record_digest_mismatch(&mut self) {
        self.digest_mismatch_count += 1;
    }

    /// Record an invalidation.
    pub fn record_invalidation(&mut self) {
        self.invalidations_this_minute += 1;
    }

    /// Reset minute counters (call every minute).
    pub fn reset_minute_counters(&mut self) {
        self.invalidations_this_minute = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::capabilities::{CapabilityRegistry, ProtocolAssignment, ProtocolType};

    #[test]
    fn test_workload_placement_metrics() {
        let mut registry = CapabilityRegistry::new();
        registry
            .assign_protocol("tenant-1", ProtocolAssignment::new(ProtocolType::Mqtt))
            .unwrap();
        registry
            .assign_protocol("tenant-2", ProtocolAssignment::new(ProtocolType::Mqtt))
            .unwrap();

        let placements = HashMap::new();
        let metrics = WorkloadPlacementMetrics::from_registry(&registry, &placements);

        assert_eq!(metrics.tenants_by_workload.get("mqtt"), Some(&2));
    }

    #[test]
    fn test_alert_thresholds() {
        let mut state = CpAlertState::default();
        let thresholds = CpAlertThresholds::default();

        // Record initial PRG count
        state.prev_prg_count = 100;

        // Simulate significant drop
        let mut metrics = WorkloadPlacementMetrics::default();
        metrics.prgs_by_workload.insert("mqtt".to_string(), 80);

        state.check_thresholds(&metrics, &thresholds, 1000);

        // Should have PRG drop alert
        assert!(state
            .active_alerts
            .iter()
            .any(|a| a.name == "prg_count_drop"));
    }

    #[test]
    fn test_telemetry_collector() {
        let telemetry = CpWorkloadTelemetry::new("test_cp");

        telemetry.update_capability_epoch(5, 12345);
        telemetry.record_cache_invalidation("epoch_change");
        telemetry.record_digest_mismatch();

        // Metrics should be updated (can't easily verify without more infrastructure)
    }
}
