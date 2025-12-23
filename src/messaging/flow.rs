use crate::audit;
use crate::routing::PrgId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Queue thresholds aligning with §13.5.
#[derive(Debug, Clone, Copy)]
pub struct QueueThresholds {
    pub commit_to_apply_pause_ack: u64,
    pub apply_to_delivery_drop_qos0: u64,
    pub retained_write_buffer_bytes: u64,
    pub wal_dirty_bytes_cap: u64,
}

impl Default for QueueThresholds {
    fn default() -> Self {
        Self {
            commit_to_apply_pause_ack: 10_000,
            apply_to_delivery_drop_qos0: 5_000,
            retained_write_buffer_bytes: 16 * 1024 * 1024,
            wal_dirty_bytes_cap: 0,
        }
    }
}

/// Flow control parameters used to compute MQTT Receive Maximum hints.
#[derive(Debug, Clone)]
pub struct CreditParams {
    pub base_window: u32,
    pub follower_factor: f32,
}

impl Default for CreditParams {
    fn default() -> Self {
        Self {
            base_window: 10,
            follower_factor: 1.0,
        }
    }
}

/// Calculate credit hint per §13.3: base_window * (0.5 + 0.5 * headroom) * follower_factor.
pub fn credit_hint(base_window: u32, headroom: f32, follower_factor: f32) -> u32 {
    let headroom = headroom.clamp(0.0, 1.0);
    let hint = (base_window as f32) * (0.5 + 0.5 * headroom) * follower_factor;
    hint.max(1.0) as u32
}

/// Backpressure signals mapped to MQTT reason codes.
#[derive(Debug, Clone, Copy)]
pub enum BackpressureReason {
    TransientBackpressure,
    PermanentDurability,
    PermanentEpoch,
}

/// Tracks queue depths and backpressure signals for telemetry.
#[derive(Clone, Default, Debug)]
pub struct BackpressureState {
    queue_depth_apply: Arc<AtomicU64>,
    queue_depth_delivery: Arc<AtomicU64>,
    apply_lag_seconds: Arc<AtomicU64>,
    replication_lag_seconds: Arc<AtomicU64>,
    raft_leaderless: Arc<AtomicU64>,
    durability_fence: Arc<AtomicU64>,
    reject_overload_total: Arc<AtomicU64>,
    tenant_throttle_events_total: Arc<AtomicU64>,
    leader_credit_hint: Arc<AtomicU64>,
    forward_backpressure_seconds: Arc<AtomicU64>,
    forward_backpressure_events: Arc<AtomicU64>,
    prg_replication_lag: Arc<Mutex<HashMap<String, u64>>>,
    forward_latency_ms: Arc<AtomicU64>,
    forward_failures_total: Arc<AtomicU64>,
    last_forward_failure_epoch: Arc<AtomicU64>,
    offline_usage: Arc<Mutex<HashMap<String, OfflineStats>>>,
    routing_epoch_skew: Arc<AtomicU64>,
    routing_epoch_skew_events: Arc<AtomicU64>,
}

/// Clustor-provided backpressure snapshot to sync with runtime telemetry.
#[derive(Clone, Copy, Debug, Default)]
pub struct ClustorBackpressure {
    pub commit_to_apply_depth: u64,
    pub apply_to_delivery_depth: u64,
    pub apply_lag_seconds: u64,
    pub replication_lag_seconds: u64,
    pub rejected_overload: u64,
    pub tenant_throttle_events: u64,
    pub leader_credit_hint: u64,
    pub forward_backpressure_seconds: u64,
    pub forward_latency_ms: u64,
    pub forward_failures_total: u64,
    pub last_forward_failure_epoch: u64,
}

#[derive(Clone, Copy)]
pub enum QueueDepthKind {
    CommitToApply,
    ApplyToDelivery,
}

#[derive(Debug, Clone, Copy)]
pub struct BackpressureSnapshot {
    pub queue_depth_apply: u64,
    pub queue_depth_delivery: u64,
    pub apply_lag_seconds: u64,
    pub replication_lag_seconds: u64,
    pub raft_leaderless: u64,
    pub durability_fence_active: bool,
    pub routing_epoch_skew: u64,
    pub routing_epoch_skew_events: u64,
    pub reject_overload_total: u64,
    pub tenant_throttle_events_total: u64,
    pub leader_credit_hint: u64,
    pub forward_backpressure_seconds: u64,
    pub forward_backpressure_events: u64,
    pub forward_latency_ms: u64,
    pub forward_failures_total: u64,
    pub last_forward_failure_epoch: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OfflineSnapshot {
    pub entries: u64,
    pub bytes: u64,
    pub dropped: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct OfflineStats {
    entries: u64,
    bytes: u64,
    dropped: u64,
}

impl BackpressureState {
    pub fn record_queue_depth(&self, kind: QueueDepthKind, depth: u64) {
        match kind {
            QueueDepthKind::CommitToApply => {
                self.queue_depth_apply.store(depth, Ordering::Relaxed);
            }
            QueueDepthKind::ApplyToDelivery => {
                self.queue_depth_delivery.store(depth, Ordering::Relaxed);
            }
        }
    }

    pub fn set_apply_lag(&self, seconds: u64) {
        self.apply_lag_seconds.store(seconds, Ordering::Relaxed);
    }

    pub fn set_replication_lag(&self, seconds: u64) {
        self.replication_lag_seconds
            .store(seconds, Ordering::Relaxed);
    }

    pub fn inc_reject_overload(&self) {
        let _ = self.reject_overload_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_tenant_throttle(&self) {
        let _ = self
            .tenant_throttle_events_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_leader_credit_hint(&self, hint: u64) {
        self.leader_credit_hint.store(hint, Ordering::Relaxed);
    }

    pub fn add_forward_backpressure(&self, seconds: u64) {
        if seconds == 0 {
            return;
        }
        let prev = self
            .forward_backpressure_seconds
            .fetch_add(seconds, Ordering::Relaxed);
        self.forward_backpressure_events
            .fetch_add(1, Ordering::Relaxed);
        if prev == 0 {
            audit::emit(
                "forward_backpressure",
                "system",
                "prg",
                &format!("{seconds}s"),
            );
        }
    }

    pub fn record_forward_latency(&self, millis: u64) {
        self.forward_latency_ms.store(millis, Ordering::Relaxed);
    }

    pub fn record_forward_failure(&self) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_forward_failure_epoch
            .store(now, Ordering::Relaxed);
        let _ = self.forward_failures_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_replication_lag_for_prg(&self, prg: &PrgId, seconds: u64) {
        if let Ok(mut guard) = self.prg_replication_lag.lock() {
            guard.insert(
                format!("{}:{}", prg.tenant_id, prg.partition_index),
                seconds,
            );
            let max = guard.values().copied().max().unwrap_or(0);
            self.replication_lag_seconds.store(max, Ordering::Relaxed);
        }
    }

    pub fn record_raft_leaderless(&self, count: u64) {
        self.raft_leaderless.store(count, Ordering::Relaxed);
    }

    pub fn set_durability_fence(&self, active: bool) {
        self.durability_fence
            .store(if active { 1 } else { 0 }, Ordering::Relaxed);
    }

    pub fn per_prg_replication_lag(&self) -> HashMap<String, u64> {
        self.prg_replication_lag
            .lock()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    pub fn record_offline_delta(&self, tenant: &str, entries_delta: i64, bytes_delta: i64) {
        if let Ok(mut guard) = self.offline_usage.lock() {
            let entry = guard.entry(tenant.to_string()).or_default();
            if entries_delta.is_negative() {
                let abs = entries_delta.checked_abs().unwrap_or(i64::MAX) as u64;
                entry.entries = entry.entries.saturating_sub(abs);
            } else {
                entry.entries = entry.entries.saturating_add(entries_delta as u64);
            }
            if bytes_delta.is_negative() {
                let abs = bytes_delta.checked_abs().unwrap_or(i64::MAX) as u64;
                entry.bytes = entry.bytes.saturating_sub(abs);
            } else {
                entry.bytes = entry.bytes.saturating_add(bytes_delta as u64);
            }
        }
    }

    pub fn record_offline_drop(&self, tenant: &str) {
        if let Ok(mut guard) = self.offline_usage.lock() {
            let entry = guard.entry(tenant.to_string()).or_default();
            entry.dropped = entry.dropped.saturating_add(1);
        }
    }

    pub fn record_routing_skew(&self, skew: u64) {
        if skew > 0 {
            self.routing_epoch_skew.store(skew, Ordering::Relaxed);
            let _ = self
                .routing_epoch_skew_events
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.routing_epoch_skew.store(0, Ordering::Relaxed);
        }
    }

    pub fn offline_snapshot(&self) -> HashMap<String, OfflineSnapshot> {
        self.offline_usage
            .lock()
            .map(|map| {
                map.iter()
                    .map(|(tenant, stats)| {
                        (
                            tenant.clone(),
                            OfflineSnapshot {
                                entries: stats.entries,
                                bytes: stats.bytes,
                                dropped: stats.dropped,
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn snapshot(&self) -> BackpressureSnapshot {
        BackpressureSnapshot {
            queue_depth_apply: self.queue_depth_apply.load(Ordering::Relaxed),
            queue_depth_delivery: self.queue_depth_delivery.load(Ordering::Relaxed),
            apply_lag_seconds: self.apply_lag_seconds.load(Ordering::Relaxed),
            replication_lag_seconds: self.replication_lag_seconds.load(Ordering::Relaxed),
            raft_leaderless: self.raft_leaderless.load(Ordering::Relaxed),
            durability_fence_active: self.durability_fence.load(Ordering::Relaxed) > 0,
            routing_epoch_skew: self.routing_epoch_skew.load(Ordering::Relaxed),
            routing_epoch_skew_events: self.routing_epoch_skew_events.load(Ordering::Relaxed),
            reject_overload_total: self.reject_overload_total.load(Ordering::Relaxed),
            tenant_throttle_events_total: self.tenant_throttle_events_total.load(Ordering::Relaxed),
            leader_credit_hint: self.leader_credit_hint.load(Ordering::Relaxed),
            forward_backpressure_seconds: self.forward_backpressure_seconds.load(Ordering::Relaxed),
            forward_backpressure_events: self.forward_backpressure_events.load(Ordering::Relaxed),
            forward_latency_ms: self.forward_latency_ms.load(Ordering::Relaxed),
            forward_failures_total: self.forward_failures_total.load(Ordering::Relaxed),
            last_forward_failure_epoch: self.last_forward_failure_epoch.load(Ordering::Relaxed),
        }
    }

    pub fn ingest_clustor_metrics(&self, metrics: ClustorBackpressure) {
        self.record_queue_depth(QueueDepthKind::CommitToApply, metrics.commit_to_apply_depth);
        self.record_queue_depth(
            QueueDepthKind::ApplyToDelivery,
            metrics.apply_to_delivery_depth,
        );
        self.set_apply_lag(metrics.apply_lag_seconds);
        self.set_replication_lag(metrics.replication_lag_seconds);
        if metrics.rejected_overload > 0 {
            let _ = self
                .reject_overload_total
                .fetch_add(metrics.rejected_overload, Ordering::Relaxed);
        }
        if metrics.tenant_throttle_events > 0 {
            let _ = self
                .tenant_throttle_events_total
                .fetch_add(metrics.tenant_throttle_events, Ordering::Relaxed);
        }
        if metrics.leader_credit_hint > 0 {
            self.record_leader_credit_hint(metrics.leader_credit_hint);
        }
        if metrics.forward_backpressure_seconds > 0 {
            self.add_forward_backpressure(metrics.forward_backpressure_seconds);
        }
        if metrics.forward_latency_ms > 0 {
            self.record_forward_latency(metrics.forward_latency_ms);
        }
        if metrics.forward_failures_total > 0 {
            let _ = self
                .forward_failures_total
                .fetch_add(metrics.forward_failures_total, Ordering::Relaxed);
        }
        if metrics.last_forward_failure_epoch > 0 {
            self.last_forward_failure_epoch
                .store(metrics.last_forward_failure_epoch, Ordering::Relaxed);
        }
    }

    pub fn forward_backpressure_events(&self) -> u64 {
        self.forward_backpressure_events.load(Ordering::Relaxed)
    }
}

/// Receive Maximum recomputation cadence (every 200 ms).
pub const RECEIVE_MAX_RECALC_INTERVAL: Duration = Duration::from_millis(200);

// ---------------------------------------------------------------------------
// Workload-aware backpressure policies (Task 9)
// ---------------------------------------------------------------------------

/// Backpressure decision from policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureDecision {
    /// No backpressure - proceed normally.
    Normal,
    /// Throttle ingress - reduce credits/receive max.
    Throttle {
        /// Optional protocol-specific reason code hint.
        reason_code: Option<u16>,
    },
    /// Hard fence - reject new messages until resolved.
    Fence {
        /// Protocol-specific reason code for rejection.
        reason_code: u16,
    },
}

impl BackpressureDecision {
    pub fn is_normal(&self) -> bool {
        matches!(self, Self::Normal)
    }

    pub fn is_throttle(&self) -> bool {
        matches!(self, Self::Throttle { .. })
    }

    pub fn is_fence(&self) -> bool {
        matches!(self, Self::Fence { .. })
    }

    pub fn reason_code(&self) -> Option<u16> {
        match self {
            Self::Normal => None,
            Self::Throttle { reason_code } => *reason_code,
            Self::Fence { reason_code } => Some(*reason_code),
        }
    }
}

/// Context for policy evaluation.
#[derive(Debug, Clone, Default)]
pub struct PolicyContext {
    /// Workload identifier.
    pub workload: String,
    /// Tenant identifier.
    pub tenant: String,
    /// Whether offline queue is enabled for this workload.
    pub offline_queue_enabled: bool,
    /// Current dedupe floor index.
    pub dedupe_floor: u64,
    /// Current offline queue depth (entries).
    pub offline_queue_depth: u64,
    /// Current offline queue size (bytes).
    pub offline_queue_bytes: u64,
    /// Routing epoch for consistency checks.
    pub routing_epoch: u64,
    /// Capability overrides from tenant config.
    pub capability_overrides: CapabilityOverrides,
}

/// Tenant-specific capability overrides for backpressure tuning.
#[derive(Debug, Clone, Default)]
pub struct CapabilityOverrides {
    /// Override receive max base window.
    pub receive_max_base: Option<u16>,
    /// Override QoS0 drop threshold.
    pub drop_qos0_depth: Option<u64>,
    /// Override offline queue bytes cap.
    pub offline_queue_bytes_cap: Option<u64>,
    /// Custom policy name if different from default.
    pub policy_name: Option<String>,
}

/// Workload metrics for policy decisions.
#[derive(Debug, Clone, Default)]
pub struct WorkloadMetrics {
    /// Commit-to-apply queue depth.
    pub commit_to_apply_depth: u64,
    /// Apply-to-delivery queue depth.
    pub apply_to_delivery_depth: u64,
    /// Apply lag in seconds.
    pub apply_lag_seconds: u64,
    /// Replication lag in seconds.
    pub replication_lag_seconds: u64,
    /// Whether Raft is currently leaderless.
    pub raft_leaderless: bool,
    /// Current dedupe floor.
    pub dedupe_floor: u64,
    /// Offline queue depth (entries).
    pub offline_queue_depth: u64,
    /// Offline queue size (bytes).
    pub offline_queue_bytes: u64,
    /// Session count for this workload.
    pub session_count: u64,
    /// Forward backpressure duration (seconds).
    pub forward_backpressure_seconds: u64,
}

impl WorkloadMetrics {
    /// Build from a backpressure snapshot plus workload-specific data.
    pub fn from_snapshot(snapshot: &BackpressureSnapshot, ctx: &PolicyContext) -> Self {
        Self {
            commit_to_apply_depth: snapshot.queue_depth_apply,
            apply_to_delivery_depth: snapshot.queue_depth_delivery,
            apply_lag_seconds: snapshot.apply_lag_seconds,
            replication_lag_seconds: snapshot.replication_lag_seconds,
            raft_leaderless: snapshot.raft_leaderless > 0,
            dedupe_floor: ctx.dedupe_floor,
            offline_queue_depth: ctx.offline_queue_depth,
            offline_queue_bytes: ctx.offline_queue_bytes,
            session_count: 0,
            forward_backpressure_seconds: snapshot.forward_backpressure_seconds,
        }
    }
}

/// Flow control hooks for workload-specific scheduling callbacks.
#[derive(Debug, Clone, Default)]
pub struct FlowControlHooks {
    /// Receive Maximum override (if any).
    pub receive_max_override: Option<u16>,
    /// Credit hint override (if any).
    pub credit_hint_override: Option<u32>,
    /// Whether to pause acknowledgements.
    pub pause_acks: bool,
    /// Whether to drop QoS 0 messages.
    pub drop_qos0: bool,
    /// Custom throttle hint in milliseconds.
    pub throttle_hint_ms: Option<u64>,
}

impl FlowControlHooks {
    pub fn with_receive_max(mut self, receive_max: u16) -> Self {
        self.receive_max_override = Some(receive_max);
        self
    }

    pub fn with_credit_hint(mut self, credit_hint: u32) -> Self {
        self.credit_hint_override = Some(credit_hint);
        self
    }

    pub fn with_pause_acks(mut self) -> Self {
        self.pause_acks = true;
        self
    }

    pub fn with_drop_qos0(mut self) -> Self {
        self.drop_qos0 = true;
        self
    }

    pub fn with_throttle_hint(mut self, ms: u64) -> Self {
        self.throttle_hint_ms = Some(ms);
        self
    }
}

/// Backpressure policy trait for workload-specific flow control.
pub trait BackpressurePolicy: Send + Sync {
    /// Classify current state into a backpressure decision.
    fn classify(
        &self,
        snapshot: &BackpressureSnapshot,
        ctx: &PolicyContext,
    ) -> BackpressureDecision;

    /// Compute the Receive Maximum (or equivalent) for flow control.
    fn compute_receive_max(&self, metrics: &WorkloadMetrics) -> u16;

    /// Determine if QoS 0 (or equivalent best-effort) messages should be dropped.
    fn should_drop_qos0(&self, metrics: &WorkloadMetrics) -> bool;

    /// Populate flow control hooks based on current policy state.
    fn advertise_flow_control(&self, metrics: &WorkloadMetrics, hooks: &mut FlowControlHooks);

    /// Policy name for telemetry.
    fn name(&self) -> &'static str;
}

/// Flow context for hook callbacks.
#[derive(Debug, Clone)]
pub struct FlowContext {
    /// Workload identifier.
    pub workload: String,
    /// Tenant identifier.
    pub tenant: String,
    /// Current workload metrics.
    pub metrics: WorkloadMetrics,
    /// Current policy decision.
    pub decision: BackpressureDecision,
    /// Routing epoch.
    pub routing_epoch: u64,
    /// Capability flags.
    pub capability_flags: u32,
}

/// Flow decision from hook callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowDecision {
    /// Allow the operation to proceed.
    Allow,
    /// Deny the operation.
    Deny {
        /// Reason code for denial.
        reason_code: u16,
    },
    /// Throttle - reschedule after delay.
    Reschedule {
        /// Delay in milliseconds before retry.
        delay_ms: u64,
    },
}

impl FlowDecision {
    pub fn is_allow(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// Flow control hook trait for workload-specific scheduling callbacks.
pub trait FlowControlHook: Send + Sync {
    /// Called before scheduling an operation.
    fn before_schedule(&self, ctx: &FlowContext) -> FlowDecision;

    /// Called after an entry is applied.
    fn after_apply(&self, ctx: &FlowContext);

    /// Called when backpressure decision changes.
    fn on_backpressure(&self, decision: &BackpressureDecision);

    /// Hook name for telemetry.
    fn name(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// Slow consumer detection (Task 141)
// ---------------------------------------------------------------------------

/// Slow consumer health classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowConsumerHealth {
    /// Consumer is keeping up.
    Healthy,
    /// Consumer is falling behind but not critically.
    Warning,
    /// Consumer is critically behind and may be disconnected.
    Critical,
}

impl SlowConsumerHealth {
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub fn is_critical(&self) -> bool {
        matches!(self, Self::Critical)
    }
}

/// Statistics for slow consumer detection.
#[derive(Debug, Clone, Default)]
pub struct SlowConsumerStats {
    /// Outstanding acknowledgements (inflight messages).
    pub outstanding_acks: u64,
    /// Oldest outbound message age in milliseconds.
    pub oldest_message_age_ms: u64,
    /// Dedupe floor delta from expected.
    pub dedupe_floor_delta: i64,
    /// Offline queue backlog (entries).
    pub offline_queue_backlog: u64,
    /// Offline queue backlog (bytes).
    pub offline_queue_bytes: u64,
    /// Time since last ack in milliseconds.
    pub time_since_last_ack_ms: u64,
    /// Average ack latency in milliseconds.
    pub avg_ack_latency_ms: u64,
    /// Session identifier.
    pub session_id: String,
    /// Client identifier.
    pub client_id: String,
}

impl SlowConsumerStats {
    /// Check if any metric exceeds warning thresholds.
    pub fn is_warning(&self, thresholds: &SlowConsumerThresholds) -> bool {
        self.outstanding_acks > thresholds.outstanding_acks_warning
            || self.oldest_message_age_ms > thresholds.oldest_message_age_warning_ms
            || self.offline_queue_backlog > thresholds.offline_queue_warning
    }

    /// Check if any metric exceeds critical thresholds.
    pub fn is_critical(&self, thresholds: &SlowConsumerThresholds) -> bool {
        self.outstanding_acks > thresholds.outstanding_acks_critical
            || self.oldest_message_age_ms > thresholds.oldest_message_age_critical_ms
            || self.offline_queue_backlog > thresholds.offline_queue_critical
    }

    /// Classify health based on thresholds.
    pub fn classify(&self, thresholds: &SlowConsumerThresholds) -> SlowConsumerHealth {
        if self.is_critical(thresholds) {
            SlowConsumerHealth::Critical
        } else if self.is_warning(thresholds) {
            SlowConsumerHealth::Warning
        } else {
            SlowConsumerHealth::Healthy
        }
    }
}

/// Thresholds for slow consumer classification.
#[derive(Debug, Clone)]
pub struct SlowConsumerThresholds {
    /// Outstanding acks for warning level.
    pub outstanding_acks_warning: u64,
    /// Outstanding acks for critical level.
    pub outstanding_acks_critical: u64,
    /// Oldest message age (ms) for warning.
    pub oldest_message_age_warning_ms: u64,
    /// Oldest message age (ms) for critical.
    pub oldest_message_age_critical_ms: u64,
    /// Offline queue depth for warning.
    pub offline_queue_warning: u64,
    /// Offline queue depth for critical.
    pub offline_queue_critical: u64,
}

impl Default for SlowConsumerThresholds {
    fn default() -> Self {
        Self {
            outstanding_acks_warning: 100,
            outstanding_acks_critical: 500,
            oldest_message_age_warning_ms: 30_000,
            oldest_message_age_critical_ms: 120_000,
            offline_queue_warning: 10_000,
            offline_queue_critical: 50_000,
        }
    }
}

/// Slow consumer policy trait for workload-specific detection.
pub trait SlowConsumerPolicy: Send + Sync {
    /// Collect slow consumer stats for this workload.
    fn collect_stats(&self, session_id: &str) -> Option<SlowConsumerStats>;

    /// Classify a consumer's health based on stats.
    fn classify(&self, stats: &SlowConsumerStats) -> SlowConsumerHealth;

    /// Action to take for critical consumers.
    fn critical_action(&self) -> SlowConsumerAction;

    /// Policy name for telemetry.
    fn name(&self) -> &'static str;
}

/// Action to take for slow consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowConsumerAction {
    /// Continue monitoring, no action.
    Monitor,
    /// Reduce credits / receive max.
    ReduceCredits,
    /// Trigger backpressure throttle.
    Throttle,
    /// Disconnect the client.
    Disconnect,
}

// ---------------------------------------------------------------------------
// Backpressure configuration (Task 142)
// ---------------------------------------------------------------------------

/// Per-workload backpressure configuration.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Policy name (e.g., "receive_max", "credit_flow").
    pub policy: String,
    /// Base receive max window.
    pub receive_max_base: u16,
    /// Queue depth at which to drop QoS 0 messages.
    pub drop_qos0_depth: u64,
    /// Offline queue bytes cap.
    pub offline_queue_bytes_cap: u64,
    /// Prefetch window (for AMQP-style flow control).
    pub prefetch_window: u32,
    /// Whether to enable automatic throttling.
    pub auto_throttle: bool,
    /// Throttle ramp-up interval in milliseconds.
    pub throttle_ramp_ms: u64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            policy: "receive_max".into(),
            receive_max_base: 10,
            drop_qos0_depth: 5_000,
            offline_queue_bytes_cap: 256 * 1024 * 1024,
            prefetch_window: 2048,
            auto_throttle: true,
            throttle_ramp_ms: 100,
        }
    }
}

impl BackpressureConfig {
    /// Create MQTT default config.
    pub fn mqtt_default() -> Self {
        Self {
            policy: "receive_max".into(),
            receive_max_base: 10,
            drop_qos0_depth: 5_000,
            offline_queue_bytes_cap: 256 * 1024 * 1024,
            prefetch_window: 0,
            auto_throttle: true,
            throttle_ramp_ms: 100,
        }
    }

    /// Create AMQP default config.
    pub fn amqp_default() -> Self {
        Self {
            policy: "credit_flow".into(),
            receive_max_base: 0,
            drop_qos0_depth: 0,
            offline_queue_bytes_cap: 256 * 1024 * 1024,
            prefetch_window: 2048,
            auto_throttle: true,
            throttle_ramp_ms: 50,
        }
    }

    /// Apply tenant overrides.
    pub fn with_overrides(mut self, overrides: &CapabilityOverrides) -> Self {
        if let Some(base) = overrides.receive_max_base {
            self.receive_max_base = base;
        }
        if let Some(depth) = overrides.drop_qos0_depth {
            self.drop_qos0_depth = depth;
        }
        if let Some(cap) = overrides.offline_queue_bytes_cap {
            self.offline_queue_bytes_cap = cap;
        }
        if let Some(ref name) = overrides.policy_name {
            self.policy = name.clone();
        }
        self
    }
}

// ---------------------------------------------------------------------------
// Workload backpressure controller (Task 9)
// ---------------------------------------------------------------------------

/// Controller that coordinates backpressure policies and hooks per workload.
pub struct WorkloadBackpressureController {
    /// Per-workload policies.
    policies: HashMap<String, Arc<dyn BackpressurePolicy>>,
    /// Per-workload hooks.
    hooks: HashMap<String, Vec<Arc<dyn FlowControlHook>>>,
    /// Slow consumer policies.
    slow_consumer_policies: HashMap<String, Arc<dyn SlowConsumerPolicy>>,
    /// Shared backpressure state.
    state: Arc<BackpressureState>,
    /// Current decisions per workload.
    decisions: Mutex<HashMap<String, BackpressureDecision>>,
}

impl WorkloadBackpressureController {
    pub fn new(state: Arc<BackpressureState>) -> Self {
        Self {
            policies: HashMap::new(),
            hooks: HashMap::new(),
            slow_consumer_policies: HashMap::new(),
            state,
            decisions: Mutex::new(HashMap::new()),
        }
    }

    /// Register a backpressure policy for a workload.
    pub fn register_policy(&mut self, workload: &str, policy: Arc<dyn BackpressurePolicy>) {
        self.policies.insert(workload.to_string(), policy);
    }

    /// Register a flow control hook for a workload.
    pub fn register_hook(&mut self, workload: &str, hook: Arc<dyn FlowControlHook>) {
        self.hooks
            .entry(workload.to_string())
            .or_default()
            .push(hook);
    }

    /// Register a slow consumer policy for a workload.
    pub fn register_slow_consumer_policy(
        &mut self,
        workload: &str,
        policy: Arc<dyn SlowConsumerPolicy>,
    ) {
        self.slow_consumer_policies
            .insert(workload.to_string(), policy);
    }

    /// Evaluate backpressure for a workload.
    pub fn evaluate(&self, workload: &str, ctx: &PolicyContext) -> BackpressureDecision {
        let snapshot = self.state.snapshot();

        let decision = if let Some(policy) = self.policies.get(workload) {
            policy.classify(&snapshot, ctx)
        } else {
            // Default policy: no backpressure
            BackpressureDecision::Normal
        };

        // Cache decision
        if let Ok(mut guard) = self.decisions.lock() {
            let prev = guard.insert(workload.to_string(), decision);

            // Notify hooks if decision changed
            if prev != Some(decision) {
                if let Some(hooks) = self.hooks.get(workload) {
                    for hook in hooks {
                        hook.on_backpressure(&decision);
                    }
                }
            }
        }

        decision
    }

    /// Compute receive max for a workload.
    pub fn compute_receive_max(&self, workload: &str, metrics: &WorkloadMetrics) -> u16 {
        if let Some(policy) = self.policies.get(workload) {
            policy.compute_receive_max(metrics)
        } else {
            65535 // No limit
        }
    }

    /// Check if QoS 0 should be dropped for a workload.
    pub fn should_drop_qos0(&self, workload: &str, metrics: &WorkloadMetrics) -> bool {
        if let Some(policy) = self.policies.get(workload) {
            policy.should_drop_qos0(metrics)
        } else {
            false
        }
    }

    /// Get flow control hooks for a workload.
    pub fn get_hooks(&self, workload: &str, metrics: &WorkloadMetrics) -> FlowControlHooks {
        let mut hooks = FlowControlHooks::default();

        if let Some(policy) = self.policies.get(workload) {
            policy.advertise_flow_control(metrics, &mut hooks);
        }

        hooks
    }

    /// Run before_schedule hooks for a workload.
    pub fn before_schedule(&self, ctx: &FlowContext) -> FlowDecision {
        if let Some(hooks) = self.hooks.get(&ctx.workload) {
            for hook in hooks {
                let decision = hook.before_schedule(ctx);
                if !decision.is_allow() {
                    return decision;
                }
            }
        }
        FlowDecision::Allow
    }

    /// Run after_apply hooks for a workload.
    pub fn after_apply(&self, ctx: &FlowContext) {
        if let Some(hooks) = self.hooks.get(&ctx.workload) {
            for hook in hooks {
                hook.after_apply(ctx);
            }
        }
    }

    /// Check slow consumer health for a session.
    pub fn check_slow_consumer(
        &self,
        workload: &str,
        session_id: &str,
    ) -> Option<SlowConsumerHealth> {
        let policy = self.slow_consumer_policies.get(workload)?;
        let stats = policy.collect_stats(session_id)?;
        Some(policy.classify(&stats))
    }

    /// Get current decision for a workload.
    pub fn current_decision(&self, workload: &str) -> Option<BackpressureDecision> {
        self.decisions
            .lock()
            .ok()
            .and_then(|guard| guard.get(workload).copied())
    }

    /// Get policy name for a workload.
    pub fn policy_name(&self, workload: &str) -> Option<&'static str> {
        self.policies.get(workload).map(|p| p.name())
    }

    /// Get all registered workloads.
    pub fn workloads(&self) -> Vec<String> {
        self.policies.keys().cloned().collect()
    }
}

/// Snapshot of backpressure controller state for telemetry.
#[derive(Debug, Clone)]
pub struct BackpressureControllerSnapshot {
    pub workloads: Vec<WorkloadBackpressureStatus>,
}

/// Per-workload backpressure status.
#[derive(Debug, Clone)]
pub struct WorkloadBackpressureStatus {
    pub workload: String,
    pub policy: String,
    pub decision: BackpressureDecision,
    pub receive_max: u16,
    pub drop_qos0: bool,
}

impl WorkloadBackpressureController {
    /// Take a snapshot of current state for telemetry.
    pub fn snapshot(&self) -> BackpressureControllerSnapshot {
        let decisions = self.decisions.lock().ok();
        let mut workloads = Vec::new();

        for (workload, policy) in &self.policies {
            let decision = decisions
                .as_ref()
                .and_then(|d| d.get(workload).copied())
                .unwrap_or(BackpressureDecision::Normal);

            let metrics = WorkloadMetrics::default();
            let receive_max = policy.compute_receive_max(&metrics);
            let drop_qos0 = policy.should_drop_qos0(&metrics);

            workloads.push(WorkloadBackpressureStatus {
                workload: workload.clone(),
                policy: policy.name().to_string(),
                decision,
                receive_max,
                drop_qos0,
            });
        }

        BackpressureControllerSnapshot { workloads }
    }
}
