//! Observability infrastructure for workload-aware telemetry.
//!
//! This module provides:
//! - Standard metric labels for workloads and protocols
//! - Workload metrics handles with automatic labeling
//! - Tracing span helpers for workload operations
//! - Audit event types with protocol/workload context
//! - Reason code maps for workload errors

use crate::prg::workload::WorkloadDescriptor;
use crate::routing::PrgId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Standard Metric Labels
// =============================================================================

/// Standard labels required for all workload metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkloadLabels {
    /// Tenant identifier.
    pub tenant: String,
    /// Workload label (e.g., "mqtt").
    pub workload: String,
    /// Workload version from descriptor.
    pub workload_version: u16,
    /// Protocol module name (may match workload).
    pub protocol: String,
    /// PRG identifier (tenant:partition).
    pub prg: String,
    /// Transport type (tcp, quic).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
    /// Capability epoch when metric was recorded.
    #[serde(default)]
    pub capability_epoch: u64,
}

impl WorkloadLabels {
    /// Create labels for a workload.
    pub fn new(tenant: impl Into<String>, workload: impl Into<String>, version: u16) -> Self {
        let workload_str = workload.into();
        Self {
            tenant: tenant.into(),
            workload: workload_str.clone(),
            workload_version: version,
            protocol: workload_str,
            prg: String::new(),
            transport: None,
            capability_epoch: 0,
        }
    }

    /// Set PRG identifier.
    pub fn with_prg(mut self, prg: &PrgId) -> Self {
        self.prg = format!("{}:{}", prg.tenant_id, prg.partition_index);
        self.tenant = prg.tenant_id.clone();
        self
    }

    /// Set transport type.
    pub fn with_transport(mut self, transport: impl Into<String>) -> Self {
        self.transport = Some(transport.into());
        self
    }

    /// Set capability epoch.
    pub fn with_capability_epoch(mut self, epoch: u64) -> Self {
        self.capability_epoch = epoch;
        self
    }

    /// Format as Prometheus label string.
    pub fn to_prometheus_labels(&self) -> String {
        let mut parts = vec![
            format!("tenant=\"{}\"", self.tenant),
            format!("workload=\"{}\"", self.workload),
            format!("workload_version=\"{}\"", self.workload_version),
        ];
        if !self.prg.is_empty() {
            parts.push(format!("prg=\"{}\"", self.prg));
        }
        if let Some(ref transport) = self.transport {
            parts.push(format!("transport=\"{}\"", transport));
        }
        parts.join(",")
    }
}

/// Labels for listener/protocol metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ListenerLabels {
    /// Protocol name.
    pub protocol: String,
    /// Transport type (tcp, quic).
    pub transport: String,
    /// Workload label if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload: Option<String>,
}

impl ListenerLabels {
    /// Create listener labels.
    pub fn new(protocol: impl Into<String>, transport: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            transport: transport.into(),
            workload: None,
        }
    }

    /// Set workload label.
    pub fn with_workload(mut self, workload: impl Into<String>) -> Self {
        self.workload = Some(workload.into());
        self
    }

    /// Format as Prometheus label string.
    pub fn to_prometheus_labels(&self) -> String {
        let mut parts = vec![
            format!("protocol=\"{}\"", self.protocol),
            format!("transport=\"{}\"", self.transport),
        ];
        if let Some(ref workload) = self.workload {
            parts.push(format!("workload=\"{}\"", workload));
        }
        parts.join(",")
    }
}

// =============================================================================
// Workload Metrics Handle
// =============================================================================

/// Handle for recording workload-specific metrics with automatic labeling.
///
/// Workloads receive this handle during `init()` and use it to record metrics.
/// Labels are automatically applied based on the workload descriptor.
#[derive(Clone)]
pub struct WorkloadMetricsHandle {
    labels: WorkloadLabels,
    counters: Arc<WorkloadCounters>,
    histograms: Arc<WorkloadHistograms>,
}

/// Atomic counters for workload metrics.
#[derive(Default)]
pub struct WorkloadCounters {
    /// Active sessions.
    pub sessions_total: AtomicU64,
    /// Sessions ready (passed readiness gate).
    pub sessions_ready: AtomicU64,
    /// Apply operations.
    pub apply_total: AtomicU64,
    /// Apply failures.
    pub apply_failures: AtomicU64,
    /// Forward operations.
    pub forward_total: AtomicU64,
    /// Forward duplicates.
    pub forward_duplicates: AtomicU64,
    /// Backpressure events.
    pub backpressure_events: AtomicU64,
    /// Slow consumer detections.
    pub slow_consumer_detected: AtomicU64,
    /// Registration failures.
    pub registration_failures: AtomicU64,
    /// Offline queue entries.
    pub offline_entries: AtomicU64,
    /// Offline queue bytes.
    pub offline_bytes: AtomicU64,
}

/// Histogram accumulators for workload metrics.
#[derive(Default)]
pub struct WorkloadHistograms {
    /// Apply latency samples.
    apply_latency: parking_lot::Mutex<Vec<u64>>,
    /// Forward latency samples.
    forward_latency: parking_lot::Mutex<Vec<u64>>,
    /// Snapshot size samples.
    snapshot_bytes: parking_lot::Mutex<Vec<u64>>,
}

impl WorkloadMetricsHandle {
    /// Create a new metrics handle for a workload.
    pub fn new(descriptor: &WorkloadDescriptor, tenant: impl Into<String>) -> Self {
        Self {
            labels: WorkloadLabels::new(tenant, descriptor.label, descriptor.version),
            counters: Arc::new(WorkloadCounters::default()),
            histograms: Arc::new(WorkloadHistograms::default()),
        }
    }

    /// Create with PRG context.
    pub fn with_prg(mut self, prg: &PrgId) -> Self {
        self.labels = self.labels.with_prg(prg);
        self
    }

    /// Get the workload label.
    pub fn workload(&self) -> &str {
        &self.labels.workload
    }

    /// Get the labels.
    pub fn labels(&self) -> &WorkloadLabels {
        &self.labels
    }

    // Counter operations

    /// Increment active sessions.
    pub fn inc_sessions(&self) {
        self.counters.sessions_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active sessions.
    pub fn dec_sessions(&self) {
        self.counters.sessions_total.fetch_sub(1, Ordering::Relaxed);
    }

    /// Set sessions ready count.
    pub fn set_sessions_ready(&self, count: u64) {
        self.counters.sessions_ready.store(count, Ordering::Relaxed);
    }

    /// Record an apply operation.
    pub fn record_apply(&self, success: bool, latency_ms: u64) {
        self.counters.apply_total.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.counters.apply_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.histograms.apply_latency.lock().push(latency_ms);
    }

    /// Record a forward operation.
    pub fn record_forward(&self, duplicate: bool, latency_ms: u64) {
        self.counters.forward_total.fetch_add(1, Ordering::Relaxed);
        if duplicate {
            self.counters
                .forward_duplicates
                .fetch_add(1, Ordering::Relaxed);
        }
        self.histograms.forward_latency.lock().push(latency_ms);
    }

    /// Record a backpressure event.
    pub fn record_backpressure(&self) {
        self.counters
            .backpressure_events
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record slow consumer detection.
    pub fn record_slow_consumer(&self) {
        self.counters
            .slow_consumer_detected
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a registration failure.
    pub fn record_registration_failure(&self) {
        self.counters
            .registration_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Update offline queue stats.
    pub fn update_offline_stats(&self, entries: u64, bytes: u64) {
        self.counters
            .offline_entries
            .store(entries, Ordering::Relaxed);
        self.counters.offline_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Record snapshot size.
    pub fn record_snapshot_size(&self, bytes: u64) {
        self.histograms.snapshot_bytes.lock().push(bytes);
    }

    /// Get counter snapshot for metrics export.
    pub fn counter_snapshot(&self) -> WorkloadCounterSnapshot {
        WorkloadCounterSnapshot {
            sessions_total: self.counters.sessions_total.load(Ordering::Relaxed),
            sessions_ready: self.counters.sessions_ready.load(Ordering::Relaxed),
            apply_total: self.counters.apply_total.load(Ordering::Relaxed),
            apply_failures: self.counters.apply_failures.load(Ordering::Relaxed),
            forward_total: self.counters.forward_total.load(Ordering::Relaxed),
            forward_duplicates: self.counters.forward_duplicates.load(Ordering::Relaxed),
            backpressure_events: self.counters.backpressure_events.load(Ordering::Relaxed),
            slow_consumer_detected: self.counters.slow_consumer_detected.load(Ordering::Relaxed),
            registration_failures: self.counters.registration_failures.load(Ordering::Relaxed),
            offline_entries: self.counters.offline_entries.load(Ordering::Relaxed),
            offline_bytes: self.counters.offline_bytes.load(Ordering::Relaxed),
        }
    }

    /// Format metrics as Prometheus exposition format.
    pub fn to_prometheus(&self) -> String {
        let labels = self.labels.to_prometheus_labels();
        let snapshot = self.counter_snapshot();
        format!(
            concat!(
                "workload_sessions_total{{{labels}}} {sessions}\n",
                "workload_sessions_ready{{{labels}}} {ready}\n",
                "workload_apply_total{{{labels}}} {apply}\n",
                "workload_apply_failures_total{{{labels}}} {apply_fail}\n",
                "workload_forward_total{{{labels}}} {forward}\n",
                "workload_forward_duplicates_total{{{labels}}} {forward_dup}\n",
                "workload_backpressure_events_total{{{labels}}} {backpressure}\n",
                "workload_slow_consumer_detected_total{{{labels}}} {slow}\n",
                "workload_registration_failures_total{{{labels}}} {reg_fail}\n",
                "workload_offline_entries{{{labels}}} {offline_entries}\n",
                "workload_offline_bytes{{{labels}}} {offline_bytes}\n",
            ),
            labels = labels,
            sessions = snapshot.sessions_total,
            ready = snapshot.sessions_ready,
            apply = snapshot.apply_total,
            apply_fail = snapshot.apply_failures,
            forward = snapshot.forward_total,
            forward_dup = snapshot.forward_duplicates,
            backpressure = snapshot.backpressure_events,
            slow = snapshot.slow_consumer_detected,
            reg_fail = snapshot.registration_failures,
            offline_entries = snapshot.offline_entries,
            offline_bytes = snapshot.offline_bytes,
        )
    }
}

/// Snapshot of workload counter values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkloadCounterSnapshot {
    pub sessions_total: u64,
    pub sessions_ready: u64,
    pub apply_total: u64,
    pub apply_failures: u64,
    pub forward_total: u64,
    pub forward_duplicates: u64,
    pub backpressure_events: u64,
    pub slow_consumer_detected: u64,
    pub registration_failures: u64,
    pub offline_entries: u64,
    pub offline_bytes: u64,
}

// =============================================================================
// Workload Metrics Registry
// =============================================================================

/// Registry of workload metrics handles.
#[derive(Default)]
pub struct WorkloadMetricsRegistry {
    handles: parking_lot::RwLock<HashMap<String, WorkloadMetricsHandle>>,
}

impl WorkloadMetricsRegistry {
    /// Create a new registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a workload metrics handle.
    pub fn register(&self, handle: WorkloadMetricsHandle) {
        let key = format!("{}:{}", handle.labels.tenant, handle.labels.workload);
        self.handles.write().insert(key, handle);
    }

    /// Get handle for a workload.
    pub fn get(&self, tenant: &str, workload: &str) -> Option<WorkloadMetricsHandle> {
        let key = format!("{tenant}:{workload}");
        self.handles.read().get(&key).cloned()
    }

    /// Remove a workload handle.
    pub fn remove(&self, tenant: &str, workload: &str) {
        let key = format!("{tenant}:{workload}");
        self.handles.write().remove(&key);
    }

    /// Export all metrics in Prometheus format.
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        for handle in self.handles.read().values() {
            output.push_str(&handle.to_prometheus());
        }
        output
    }

    /// Get all registered workloads.
    pub fn workloads(&self) -> Vec<(String, String)> {
        self.handles
            .read()
            .values()
            .map(|h| (h.labels.tenant.clone(), h.labels.workload.clone()))
            .collect()
    }
}

// =============================================================================
// Tracing Spans
// =============================================================================

/// Span tags for workload operations.
#[derive(Debug, Clone, Default)]
pub struct WorkloadSpanTags {
    /// Workload label.
    pub workload: String,
    /// Workload version.
    pub workload_version: u16,
    /// Tenant ID.
    pub tenant: String,
    /// PRG identifier.
    pub prg: String,
    /// Routing epoch.
    pub routing_epoch: u64,
    /// Schema ID if applicable.
    pub schema_id: Option<String>,
}

impl WorkloadSpanTags {
    /// Create span tags from labels.
    pub fn from_labels(labels: &WorkloadLabels) -> Self {
        Self {
            workload: labels.workload.clone(),
            workload_version: labels.workload_version,
            tenant: labels.tenant.clone(),
            prg: labels.prg.clone(),
            routing_epoch: labels.capability_epoch,
            schema_id: None,
        }
    }

    /// Set schema ID.
    pub fn with_schema_id(mut self, id: impl Into<String>) -> Self {
        self.schema_id = Some(id.into());
        self
    }
}

/// Create a workload apply span.
#[macro_export]
macro_rules! workload_span {
    ($name:expr, $tags:expr) => {
        tracing::info_span!(
            $name,
            workload = %$tags.workload,
            workload_version = $tags.workload_version,
            tenant = %$tags.tenant,
            prg = %$tags.prg,
        )
    };
    ($name:expr, $tags:expr, $($field:tt)*) => {
        tracing::info_span!(
            $name,
            workload = %$tags.workload,
            workload_version = $tags.workload_version,
            tenant = %$tags.tenant,
            prg = %$tags.prg,
            $($field)*
        )
    };
}

/// Create a forward span.
#[macro_export]
macro_rules! forward_span {
    ($source:expr, $target:expr, $routing_epoch:expr) => {
        tracing::info_span!(
            "workload.forward",
            source_workload = %$source,
            target_workload = %$target,
            routing_epoch = $routing_epoch,
        )
    };
}

// =============================================================================
// Audit Events
// =============================================================================

/// Audit event types for workload operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum WorkloadAuditEvent {
    /// Workload registration event.
    Registration {
        workload: String,
        version: u16,
        tenant: String,
        capabilities: u32,
        success: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Workload lifecycle event.
    Lifecycle {
        workload: String,
        tenant: String,
        prg: String,
        stage: WorkloadLifecycleStage,
        duration_ms: u64,
        success: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Forward event.
    Forward {
        source_workload: String,
        target_workload: String,
        source_prg: String,
        target_prg: String,
        routing_epoch: u64,
        duplicate: bool,
        latency_ms: u64,
    },
    /// Session event.
    Session {
        workload: String,
        protocol: String,
        tenant: String,
        client_id: String,
        action: SessionAction,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Capability toggle event.
    CapabilityToggle {
        workload: String,
        tenant: String,
        capability: String,
        enabled: bool,
        epoch: u64,
    },
    /// Error event with reason code.
    Error {
        workload: String,
        tenant: String,
        prg: String,
        error_class: WorkloadErrorClass,
        reason_code: u16,
        message: String,
    },
}

/// Workload lifecycle stages.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadLifecycleStage {
    Init,
    Hydrate,
    Ready,
    Apply,
    Snapshot,
    Unload,
}

/// Session actions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionAction {
    Connect,
    Disconnect,
    AuthFailure,
    Throttle,
    Offline,
}

/// Workload error classes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadErrorClass {
    Permanent,
    Transient,
    Input,
    Backpressure,
}

impl WorkloadAuditEvent {
    /// Create a registration event.
    pub fn registration(
        descriptor: &WorkloadDescriptor,
        tenant: impl Into<String>,
        success: bool,
        error: Option<String>,
    ) -> Self {
        Self::Registration {
            workload: descriptor.label.to_string(),
            version: descriptor.version,
            tenant: tenant.into(),
            capabilities: descriptor.capabilities.bits(),
            success,
            error,
        }
    }

    /// Create a lifecycle event.
    pub fn lifecycle(
        workload: impl Into<String>,
        tenant: impl Into<String>,
        prg: &PrgId,
        stage: WorkloadLifecycleStage,
        duration: Duration,
        success: bool,
        error: Option<String>,
    ) -> Self {
        Self::Lifecycle {
            workload: workload.into(),
            tenant: tenant.into(),
            prg: format!("{}:{}", prg.tenant_id, prg.partition_index),
            stage,
            duration_ms: duration.as_millis() as u64,
            success,
            error,
        }
    }

    /// Create a forward event.
    pub fn forward(
        source_workload: impl Into<String>,
        target_workload: impl Into<String>,
        source_prg: &PrgId,
        target_prg: &PrgId,
        routing_epoch: u64,
        duplicate: bool,
        latency_ms: u64,
    ) -> Self {
        Self::Forward {
            source_workload: source_workload.into(),
            target_workload: target_workload.into(),
            source_prg: format!("{}:{}", source_prg.tenant_id, source_prg.partition_index),
            target_prg: format!("{}:{}", target_prg.tenant_id, target_prg.partition_index),
            routing_epoch,
            duplicate,
            latency_ms,
        }
    }

    /// Create a session event.
    pub fn session(
        workload: impl Into<String>,
        protocol: impl Into<String>,
        tenant: impl Into<String>,
        client_id: impl Into<String>,
        action: SessionAction,
        reason: Option<String>,
    ) -> Self {
        Self::Session {
            workload: workload.into(),
            protocol: protocol.into(),
            tenant: tenant.into(),
            client_id: client_id.into(),
            action,
            reason,
        }
    }

    /// Create an error event.
    pub fn error(
        workload: impl Into<String>,
        tenant: impl Into<String>,
        prg: &PrgId,
        error_class: WorkloadErrorClass,
        reason_code: u16,
        message: impl Into<String>,
    ) -> Self {
        Self::Error {
            workload: workload.into(),
            tenant: tenant.into(),
            prg: format!("{}:{}", prg.tenant_id, prg.partition_index),
            error_class,
            reason_code,
            message: message.into(),
        }
    }

    /// Get the workload label.
    pub fn workload(&self) -> &str {
        match self {
            Self::Registration { workload, .. } => workload,
            Self::Lifecycle { workload, .. } => workload,
            Self::Forward {
                source_workload, ..
            } => source_workload,
            Self::Session { workload, .. } => workload,
            Self::CapabilityToggle { workload, .. } => workload,
            Self::Error { workload, .. } => workload,
        }
    }

    /// Get the tenant ID.
    pub fn tenant(&self) -> &str {
        match self {
            Self::Registration { tenant, .. } => tenant,
            Self::Lifecycle { tenant, .. } => tenant,
            Self::Forward { source_prg, .. } => source_prg.split(':').next().unwrap_or(""),
            Self::Session { tenant, .. } => tenant,
            Self::CapabilityToggle { tenant, .. } => tenant,
            Self::Error { tenant, .. } => tenant,
        }
    }
}

// =============================================================================
// Reason Code Maps
// =============================================================================

/// Standard reason codes for workload errors.
pub mod reason_codes {
    // Permanent errors (0x1000-0x1FFF)
    pub const SCHEMA_MISMATCH: u16 = 0x1001;
    pub const CAPABILITY_DISABLED: u16 = 0x1002;
    pub const UNSUPPORTED_VERSION: u16 = 0x1003;
    pub const INVALID_PAYLOAD: u16 = 0x1004;
    pub const STORAGE_FAILURE: u16 = 0x1005;

    // Transient errors (0x2000-0x2FFF)
    pub const REPLICATION_LAG: u16 = 0x2001;
    pub const LEADERSHIP_LOST: u16 = 0x2002;
    pub const ROUTING_STALE: u16 = 0x2003;
    pub const FORWARD_TIMEOUT: u16 = 0x2004;
    pub const CLUSTOR_UNAVAILABLE: u16 = 0x2005;

    // Input errors (0x3000-0x3FFF)
    pub const INVALID_TOPIC: u16 = 0x3001;
    pub const INVALID_CLIENT_ID: u16 = 0x3002;
    pub const DUPLICATE_MESSAGE: u16 = 0x3003;
    pub const QUOTA_EXCEEDED: u16 = 0x3004;

    // Backpressure (0x4000-0x4FFF)
    pub const QUEUE_FULL: u16 = 0x4001;
    pub const APPLY_LAG: u16 = 0x4002;
    pub const SLOW_CONSUMER: u16 = 0x4003;
    pub const OFFLINE_LIMIT: u16 = 0x4004;

    /// Get description for a reason code.
    pub fn description(code: u16) -> &'static str {
        match code {
            SCHEMA_MISMATCH => "schema version mismatch",
            CAPABILITY_DISABLED => "capability not enabled for tenant",
            UNSUPPORTED_VERSION => "workload version not supported",
            INVALID_PAYLOAD => "invalid payload encoding",
            STORAGE_FAILURE => "storage operation failed",
            REPLICATION_LAG => "replication lag too high",
            LEADERSHIP_LOST => "raft leadership lost",
            ROUTING_STALE => "routing epoch stale",
            FORWARD_TIMEOUT => "forward request timed out",
            CLUSTOR_UNAVAILABLE => "clustor backend unavailable",
            INVALID_TOPIC => "invalid topic format",
            INVALID_CLIENT_ID => "invalid client ID",
            DUPLICATE_MESSAGE => "duplicate message detected",
            QUOTA_EXCEEDED => "tenant quota exceeded",
            QUEUE_FULL => "queue full, backpressure applied",
            APPLY_LAG => "apply lag threshold exceeded",
            SLOW_CONSUMER => "slow consumer detected",
            OFFLINE_LIMIT => "offline queue limit reached",
            _ => "unknown error",
        }
    }

    /// Get error class for a reason code.
    pub fn error_class(code: u16) -> super::WorkloadErrorClass {
        match code >> 12 {
            1 => super::WorkloadErrorClass::Permanent,
            2 => super::WorkloadErrorClass::Transient,
            3 => super::WorkloadErrorClass::Input,
            4 => super::WorkloadErrorClass::Backpressure,
            _ => super::WorkloadErrorClass::Permanent,
        }
    }
}

// =============================================================================
// Workload State for Readiness
// =============================================================================

/// Workload state for readiness reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadState {
    /// Workload label.
    pub label: String,
    /// Workload version.
    pub version: u16,
    /// Whether the workload is ready.
    pub ready: bool,
    /// Whether durability fence is active.
    pub durability_fence: bool,
    /// Capability flags.
    pub capabilities: u32,
    /// Last error if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl WorkloadState {
    /// Create a new workload state.
    pub fn new(descriptor: &WorkloadDescriptor) -> Self {
        Self {
            label: descriptor.label.to_string(),
            version: descriptor.version,
            ready: false,
            durability_fence: false,
            capabilities: descriptor.capabilities.bits(),
            last_error: None,
        }
    }

    /// Mark as ready.
    pub fn set_ready(&mut self, ready: bool) {
        self.ready = ready;
    }

    /// Set durability fence.
    pub fn set_fence(&mut self, fence: bool) {
        self.durability_fence = fence;
    }

    /// Set last error.
    pub fn set_error(&mut self, error: Option<String>) {
        self.last_error = error;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prg::workload::CapabilityFlags;

    #[test]
    fn test_workload_labels_prometheus() {
        let labels = WorkloadLabels::new("tenant1", "mqtt", 1)
            .with_prg(&PrgId {
                tenant_id: "tenant1".to_string(),
                partition_index: 0,
            })
            .with_transport("tcp");

        let prometheus = labels.to_prometheus_labels();
        assert!(prometheus.contains("tenant=\"tenant1\""));
        assert!(prometheus.contains("workload=\"mqtt\""));
        assert!(prometheus.contains("workload_version=\"1\""));
        assert!(prometheus.contains("prg=\"tenant1:0\""));
        assert!(prometheus.contains("transport=\"tcp\""));
    }

    #[test]
    fn test_workload_metrics_handle() {
        let descriptor = WorkloadDescriptor::new("test", 1, CapabilityFlags::empty());
        let handle = WorkloadMetricsHandle::new(&descriptor, "tenant1");

        handle.inc_sessions();
        handle.inc_sessions();
        handle.record_apply(true, 10);
        handle.record_forward(false, 5);

        let snapshot = handle.counter_snapshot();
        assert_eq!(snapshot.sessions_total, 2);
        assert_eq!(snapshot.apply_total, 1);
        assert_eq!(snapshot.forward_total, 1);
    }

    #[test]
    fn test_reason_codes() {
        assert_eq!(
            reason_codes::description(reason_codes::SCHEMA_MISMATCH),
            "schema version mismatch"
        );
        assert!(matches!(
            reason_codes::error_class(reason_codes::REPLICATION_LAG),
            WorkloadErrorClass::Transient
        ));
        assert!(matches!(
            reason_codes::error_class(reason_codes::QUEUE_FULL),
            WorkloadErrorClass::Backpressure
        ));
    }

    #[test]
    fn test_audit_event_serialization() {
        let event = WorkloadAuditEvent::session(
            "mqtt",
            "mqtt",
            "tenant1",
            "client1",
            SessionAction::Connect,
            None,
        );

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"event_type\":\"Session\""));
        assert!(json.contains("\"workload\":\"mqtt\""));
    }
}
