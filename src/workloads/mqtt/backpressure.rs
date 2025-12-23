//! MQTT-specific backpressure policy implementation.
//!
//! Implements the `BackpressurePolicy` and `SlowConsumerPolicy` traits
//! with MQTT-specific semantics including Receive Maximum flow control,
//! QoS 0 drop thresholds, and offline queue management.

use crate::flow::{
    BackpressureConfig, BackpressureDecision, BackpressurePolicy, BackpressureSnapshot,
    CapabilityOverrides, FlowContext, FlowControlHook, FlowControlHooks, FlowDecision,
    PolicyContext, SlowConsumerAction, SlowConsumerHealth, SlowConsumerPolicy, SlowConsumerStats,
    SlowConsumerThresholds, WorkloadMetrics,
};

/// MQTT backpressure policy implementing Receive Maximum flow control.
#[derive(Debug, Clone)]
pub struct MqttBackpressurePolicy {
    config: BackpressureConfig,
    thresholds: SlowConsumerThresholds,
}

impl MqttBackpressurePolicy {
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            thresholds: SlowConsumerThresholds::default(),
        }
    }

    pub fn with_thresholds(mut self, thresholds: SlowConsumerThresholds) -> Self {
        self.thresholds = thresholds;
        self
    }

    /// Create with tenant overrides applied.
    pub fn with_overrides(config: BackpressureConfig, overrides: &CapabilityOverrides) -> Self {
        Self::new(config.with_overrides(overrides))
    }

    /// Get the configured receive max base.
    pub fn receive_max_base(&self) -> u16 {
        self.config.receive_max_base
    }

    /// Get the configured QoS 0 drop depth.
    pub fn drop_qos0_depth(&self) -> u64 {
        self.config.drop_qos0_depth
    }

    /// Get the configured offline queue bytes cap.
    pub fn offline_queue_bytes_cap(&self) -> u64 {
        self.config.offline_queue_bytes_cap
    }

    /// Compute headroom factor (0.0 - 1.0) from queue depth.
    fn compute_headroom(&self, metrics: &WorkloadMetrics) -> f32 {
        // Use apply-to-delivery depth as primary signal
        let depth = metrics.apply_to_delivery_depth;
        let threshold = self.config.drop_qos0_depth;

        if threshold == 0 {
            return 1.0;
        }

        let ratio = depth as f32 / threshold as f32;
        (1.0 - ratio).clamp(0.0, 1.0)
    }
}

impl Default for MqttBackpressurePolicy {
    fn default() -> Self {
        Self::new(BackpressureConfig::mqtt_default())
    }
}

impl BackpressurePolicy for MqttBackpressurePolicy {
    fn classify(
        &self,
        snapshot: &BackpressureSnapshot,
        ctx: &PolicyContext,
    ) -> BackpressureDecision {
        // Check for hard fences first
        if snapshot.durability_fence_active {
            return BackpressureDecision::Fence {
                reason_code: 0x97, // MQTT Quota exceeded
            };
        }

        // Check for routing epoch skew
        if snapshot.routing_epoch_skew > 0 {
            return BackpressureDecision::Throttle {
                reason_code: Some(0x97),
            };
        }

        // Check offline queue size
        if ctx.offline_queue_enabled {
            let bytes_cap = self.config.offline_queue_bytes_cap;
            if ctx.offline_queue_bytes > bytes_cap {
                return BackpressureDecision::Fence {
                    reason_code: 0x97, // Quota exceeded
                };
            }
            // Throttle at 80% of cap
            if ctx.offline_queue_bytes > (bytes_cap * 80 / 100) {
                return BackpressureDecision::Throttle {
                    reason_code: Some(0x97),
                };
            }
        }

        // Check commit-to-apply depth for pause
        let pause_threshold = 10_000u64; // TODO: make configurable
        if snapshot.queue_depth_apply > pause_threshold {
            return BackpressureDecision::Throttle {
                reason_code: Some(0x97),
            };
        }

        // Check apply-to-delivery for QoS 0 drop
        if snapshot.queue_depth_delivery > self.config.drop_qos0_depth {
            return BackpressureDecision::Throttle { reason_code: None };
        }

        BackpressureDecision::Normal
    }

    fn compute_receive_max(&self, metrics: &WorkloadMetrics) -> u16 {
        let base = self.config.receive_max_base;
        if base == 0 {
            return 65535; // No limit
        }

        let headroom = self.compute_headroom(metrics);

        // Apply follower factor for replication lag
        let follower_factor = if metrics.replication_lag_seconds > 0 {
            0.5 + 0.5 / (1.0 + metrics.replication_lag_seconds as f32 / 10.0)
        } else {
            1.0
        };

        // credit_hint formula: base * (0.5 + 0.5 * headroom) * follower_factor
        let hint = (base as f32) * (0.5 + 0.5 * headroom) * follower_factor;
        hint.max(1.0) as u16
    }

    fn should_drop_qos0(&self, metrics: &WorkloadMetrics) -> bool {
        if self.config.drop_qos0_depth == 0 {
            return false;
        }
        metrics.apply_to_delivery_depth > self.config.drop_qos0_depth
    }

    fn advertise_flow_control(&self, metrics: &WorkloadMetrics, hooks: &mut FlowControlHooks) {
        let receive_max = self.compute_receive_max(metrics);
        hooks.receive_max_override = Some(receive_max);

        if self.should_drop_qos0(metrics) {
            hooks.drop_qos0 = true;
        }

        // Set throttle hint based on queue depth
        if metrics.apply_to_delivery_depth > self.config.drop_qos0_depth / 2 {
            hooks.throttle_hint_ms = Some(self.config.throttle_ramp_ms);
        }
    }

    fn name(&self) -> &'static str {
        "mqtt-receive-max"
    }
}

impl SlowConsumerPolicy for MqttBackpressurePolicy {
    fn collect_stats(&self, _session_id: &str) -> Option<SlowConsumerStats> {
        // This would typically query session state - returning None as placeholder
        // Real implementation would be provided by the runtime
        None
    }

    fn classify(&self, stats: &SlowConsumerStats) -> SlowConsumerHealth {
        stats.classify(&self.thresholds)
    }

    fn critical_action(&self) -> SlowConsumerAction {
        SlowConsumerAction::Disconnect
    }

    fn name(&self) -> &'static str {
        "mqtt-slow-consumer"
    }
}

/// MQTT-specific flow control hook for offline queue awareness.
#[derive(Debug, Clone)]
pub struct MqttOfflineQueueHook {
    bytes_cap: u64,
}

impl MqttOfflineQueueHook {
    pub fn new(bytes_cap: u64) -> Self {
        Self { bytes_cap }
    }
}

impl Default for MqttOfflineQueueHook {
    fn default() -> Self {
        Self {
            bytes_cap: 256 * 1024 * 1024,
        }
    }
}

impl FlowControlHook for MqttOfflineQueueHook {
    fn before_schedule(&self, ctx: &FlowContext) -> FlowDecision {
        // Check offline queue size
        if ctx.metrics.offline_queue_bytes > self.bytes_cap {
            return FlowDecision::Deny {
                reason_code: 0x97, // Quota exceeded
            };
        }

        // Throttle at 90% of cap
        if ctx.metrics.offline_queue_bytes > (self.bytes_cap * 90 / 100) {
            return FlowDecision::Reschedule { delay_ms: 100 };
        }

        FlowDecision::Allow
    }

    fn after_apply(&self, _ctx: &FlowContext) {
        // No-op for offline queue hook
    }

    fn on_backpressure(&self, _decision: &BackpressureDecision) {
        // Could emit telemetry here
    }

    fn name(&self) -> &'static str {
        "mqtt-offline-queue"
    }
}

/// MQTT dedupe-aware flow control hook.
#[derive(Debug, Clone, Default)]
pub struct MqttDedupeHook;

impl FlowControlHook for MqttDedupeHook {
    fn before_schedule(&self, ctx: &FlowContext) -> FlowDecision {
        // Allow scheduling if dedupe floor is healthy
        // Dedupe floor delta indicates how far behind we are
        let metrics = &ctx.metrics;

        // If we have significant forward backpressure, throttle
        if metrics.forward_backpressure_seconds > 5 {
            return FlowDecision::Reschedule { delay_ms: 200 };
        }

        FlowDecision::Allow
    }

    fn after_apply(&self, _ctx: &FlowContext) {
        // Could track ack latency for slow consumer detection
    }

    fn on_backpressure(&self, _decision: &BackpressureDecision) {
        // No-op
    }

    fn name(&self) -> &'static str {
        "mqtt-dedupe"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_policy_normal() {
        let policy = MqttBackpressurePolicy::default();
        let snapshot = BackpressureSnapshot {
            queue_depth_apply: 100,
            queue_depth_delivery: 100,
            apply_lag_seconds: 0,
            replication_lag_seconds: 0,
            raft_leaderless: 0,
            durability_fence_active: false,
            routing_epoch_skew: 0,
            routing_epoch_skew_events: 0,
            reject_overload_total: 0,
            tenant_throttle_events_total: 0,
            leader_credit_hint: 0,
            forward_backpressure_seconds: 0,
            forward_backpressure_events: 0,
            forward_latency_ms: 0,
            forward_failures_total: 0,
            last_forward_failure_epoch: 0,
        };
        let ctx = PolicyContext::default();

        let decision = BackpressurePolicy::classify(&policy, &snapshot, &ctx);
        assert!(decision.is_normal());
    }

    #[test]
    fn test_mqtt_policy_fence_on_durability() {
        let policy = MqttBackpressurePolicy::default();
        let snapshot = BackpressureSnapshot {
            queue_depth_apply: 100,
            queue_depth_delivery: 100,
            apply_lag_seconds: 0,
            replication_lag_seconds: 0,
            raft_leaderless: 0,
            durability_fence_active: true,
            routing_epoch_skew: 0,
            routing_epoch_skew_events: 0,
            reject_overload_total: 0,
            tenant_throttle_events_total: 0,
            leader_credit_hint: 0,
            forward_backpressure_seconds: 0,
            forward_backpressure_events: 0,
            forward_latency_ms: 0,
            forward_failures_total: 0,
            last_forward_failure_epoch: 0,
        };
        let ctx = PolicyContext::default();

        let decision = BackpressurePolicy::classify(&policy, &snapshot, &ctx);
        assert!(decision.is_fence());
    }

    #[test]
    fn test_mqtt_policy_throttle_on_queue_depth() {
        let policy = MqttBackpressurePolicy::default();
        let snapshot = BackpressureSnapshot {
            queue_depth_apply: 100,
            queue_depth_delivery: 6000, // Over drop_qos0_depth of 5000
            apply_lag_seconds: 0,
            replication_lag_seconds: 0,
            raft_leaderless: 0,
            durability_fence_active: false,
            routing_epoch_skew: 0,
            routing_epoch_skew_events: 0,
            reject_overload_total: 0,
            tenant_throttle_events_total: 0,
            leader_credit_hint: 0,
            forward_backpressure_seconds: 0,
            forward_backpressure_events: 0,
            forward_latency_ms: 0,
            forward_failures_total: 0,
            last_forward_failure_epoch: 0,
        };
        let ctx = PolicyContext::default();

        let decision = BackpressurePolicy::classify(&policy, &snapshot, &ctx);
        assert!(decision.is_throttle());
    }

    #[test]
    fn test_mqtt_compute_receive_max() {
        let policy = MqttBackpressurePolicy::default();

        // Normal conditions
        let metrics = WorkloadMetrics {
            apply_to_delivery_depth: 0,
            replication_lag_seconds: 0,
            ..Default::default()
        };
        let receive_max = policy.compute_receive_max(&metrics);
        assert_eq!(receive_max, 10); // base window

        // High queue depth reduces receive max
        let metrics = WorkloadMetrics {
            apply_to_delivery_depth: 5000,
            replication_lag_seconds: 0,
            ..Default::default()
        };
        let receive_max = policy.compute_receive_max(&metrics);
        assert!(receive_max < 10);
    }

    #[test]
    fn test_mqtt_should_drop_qos0() {
        let policy = MqttBackpressurePolicy::default();

        let metrics = WorkloadMetrics {
            apply_to_delivery_depth: 4000,
            ..Default::default()
        };
        assert!(!policy.should_drop_qos0(&metrics));

        let metrics = WorkloadMetrics {
            apply_to_delivery_depth: 6000,
            ..Default::default()
        };
        assert!(policy.should_drop_qos0(&metrics));
    }

    #[test]
    fn test_slow_consumer_classification() {
        let policy = MqttBackpressurePolicy::default();

        let stats = SlowConsumerStats {
            outstanding_acks: 50,
            ..Default::default()
        };
        assert_eq!(
            SlowConsumerPolicy::classify(&policy, &stats),
            SlowConsumerHealth::Healthy
        );

        let stats = SlowConsumerStats {
            outstanding_acks: 200,
            ..Default::default()
        };
        assert_eq!(
            SlowConsumerPolicy::classify(&policy, &stats),
            SlowConsumerHealth::Warning
        );

        let stats = SlowConsumerStats {
            outstanding_acks: 600,
            ..Default::default()
        };
        assert_eq!(
            SlowConsumerPolicy::classify(&policy, &stats),
            SlowConsumerHealth::Critical
        );
    }

    #[test]
    fn test_offline_queue_hook() {
        let hook = MqttOfflineQueueHook::new(1000);

        // Under cap - allow
        let ctx = FlowContext {
            workload: "mqtt".into(),
            tenant: "test".into(),
            metrics: WorkloadMetrics {
                offline_queue_bytes: 500,
                ..Default::default()
            },
            decision: BackpressureDecision::Normal,
            routing_epoch: 0,
            capability_flags: 0,
        };
        assert!(hook.before_schedule(&ctx).is_allow());

        // Over 90% - reschedule
        let ctx = FlowContext {
            metrics: WorkloadMetrics {
                offline_queue_bytes: 950,
                ..Default::default()
            },
            ..ctx.clone()
        };
        assert!(matches!(
            hook.before_schedule(&ctx),
            FlowDecision::Reschedule { .. }
        ));

        // Over cap - deny
        let ctx = FlowContext {
            metrics: WorkloadMetrics {
                offline_queue_bytes: 1100,
                ..Default::default()
            },
            ..ctx
        };
        assert!(matches!(
            hook.before_schedule(&ctx),
            FlowDecision::Deny { .. }
        ));
    }
}
