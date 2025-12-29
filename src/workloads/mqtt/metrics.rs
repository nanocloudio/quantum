//! MQTT adapter metrics and monitoring.
//!
//! This module provides metrics collection and export for MQTT protocol
//! operations, connection statistics, and performance monitoring.
//!
//! Tasks 23-24: MQTT adapter metrics and protocol conformance.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Counter Metrics
// ---------------------------------------------------------------------------

/// Atomic counter for thread-safe metric updates.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn reset(&self) -> u64 {
        self.0.swap(0, Ordering::Relaxed)
    }
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        Self(AtomicU64::new(self.get()))
    }
}

// ---------------------------------------------------------------------------
// Gauge Metrics
// ---------------------------------------------------------------------------

/// Atomic gauge for current values.
#[derive(Debug, Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn set(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Clone for Gauge {
    fn clone(&self) -> Self {
        Self(AtomicU64::new(self.get()))
    }
}

// ---------------------------------------------------------------------------
// Histogram (simple bucket-based)
// ---------------------------------------------------------------------------

/// Simple histogram for latency tracking.
#[derive(Debug)]
pub struct Histogram {
    /// Bucket boundaries in microseconds.
    buckets: Vec<u64>,
    /// Counts per bucket (last bucket is for values >= last boundary).
    counts: Vec<AtomicU64>,
    /// Sum of all values.
    sum: AtomicU64,
    /// Total count.
    count: AtomicU64,
}

impl Clone for Histogram {
    fn clone(&self) -> Self {
        Self {
            buckets: self.buckets.clone(),
            counts: self
                .counts
                .iter()
                .map(|c| AtomicU64::new(c.load(Ordering::Relaxed)))
                .collect(),
            sum: AtomicU64::new(self.sum.load(Ordering::Relaxed)),
            count: AtomicU64::new(self.count.load(Ordering::Relaxed)),
        }
    }
}

impl Histogram {
    /// Create a histogram with the given bucket boundaries (in microseconds).
    pub fn new(buckets: Vec<u64>) -> Self {
        let counts = (0..=buckets.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            buckets,
            counts,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Create default latency histogram (in microseconds).
    pub fn latency_histogram() -> Self {
        Self::new(vec![
            100,       // 100us
            500,       // 500us
            1000,      // 1ms
            5000,      // 5ms
            10_000,    // 10ms
            50_000,    // 50ms
            100_000,   // 100ms
            500_000,   // 500ms
            1_000_000, // 1s
        ])
    }

    /// Record a value.
    pub fn observe(&self, value_us: u64) {
        self.sum.fetch_add(value_us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        let bucket_idx = self
            .buckets
            .iter()
            .position(|&b| value_us < b)
            .unwrap_or(self.buckets.len());
        self.counts[bucket_idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Record a duration.
    pub fn observe_duration(&self, duration: Duration) {
        self.observe(duration.as_micros() as u64);
    }

    /// Get count.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get sum.
    pub fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    /// Get mean value (in microseconds).
    pub fn mean(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            0.0
        } else {
            self.sum() as f64 / count as f64
        }
    }

    /// Get bucket counts.
    pub fn bucket_counts(&self) -> Vec<u64> {
        self.counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect()
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::latency_histogram()
    }
}

// ---------------------------------------------------------------------------
// MQTT Protocol Metrics
// ---------------------------------------------------------------------------

/// MQTT protocol-level metrics.
#[derive(Debug, Default, Clone)]
pub struct MqttProtocolMetrics {
    // Packet counters
    pub connect_packets: Counter,
    pub connack_packets: Counter,
    pub publish_packets: Counter,
    pub puback_packets: Counter,
    pub pubrec_packets: Counter,
    pub pubrel_packets: Counter,
    pub pubcomp_packets: Counter,
    pub subscribe_packets: Counter,
    pub suback_packets: Counter,
    pub unsubscribe_packets: Counter,
    pub unsuback_packets: Counter,
    pub pingreq_packets: Counter,
    pub pingresp_packets: Counter,
    pub disconnect_packets: Counter,
    pub auth_packets: Counter,

    // Message counters by QoS
    pub messages_qos0: Counter,
    pub messages_qos1: Counter,
    pub messages_qos2: Counter,

    // Error counters
    pub malformed_packets: Counter,
    pub protocol_errors: Counter,
    pub auth_failures: Counter,
    pub quota_exceeded: Counter,

    // Byte counters
    pub bytes_received: Counter,
    pub bytes_sent: Counter,
}

impl MqttProtocolMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a received packet.
    pub fn record_received_packet(&self, packet_type: u8, size: u64) {
        self.bytes_received.add(size);
        match packet_type {
            1 => self.connect_packets.inc(),
            3 => self.publish_packets.inc(),
            4 => self.puback_packets.inc(),
            5 => self.pubrec_packets.inc(),
            6 => self.pubrel_packets.inc(),
            7 => self.pubcomp_packets.inc(),
            8 => self.subscribe_packets.inc(),
            10 => self.unsubscribe_packets.inc(),
            12 => self.pingreq_packets.inc(),
            14 => self.disconnect_packets.inc(),
            15 => self.auth_packets.inc(),
            _ => {}
        }
    }

    /// Record a sent packet.
    pub fn record_sent_packet(&self, packet_type: u8, size: u64) {
        self.bytes_sent.add(size);
        match packet_type {
            2 => self.connack_packets.inc(),
            3 => self.publish_packets.inc(),
            4 => self.puback_packets.inc(),
            5 => self.pubrec_packets.inc(),
            6 => self.pubrel_packets.inc(),
            7 => self.pubcomp_packets.inc(),
            9 => self.suback_packets.inc(),
            11 => self.unsuback_packets.inc(),
            13 => self.pingresp_packets.inc(),
            14 => self.disconnect_packets.inc(),
            15 => self.auth_packets.inc(),
            _ => {}
        }
    }

    /// Record a message by QoS.
    pub fn record_message(&self, qos: u8) {
        match qos {
            0 => self.messages_qos0.inc(),
            1 => self.messages_qos1.inc(),
            2 => self.messages_qos2.inc(),
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Connection Metrics
// ---------------------------------------------------------------------------

/// Connection-level metrics.
#[derive(Debug, Default, Clone)]
pub struct MqttConnectionMetrics {
    // Gauges
    pub active_connections: Gauge,
    pub active_sessions: Gauge,
    pub persistent_sessions: Gauge,

    // Counters
    pub total_connections: Counter,
    pub connection_errors: Counter,
    pub clean_disconnects: Counter,
    pub abnormal_disconnects: Counter,
    pub session_takevers: Counter,

    // Histograms
    pub connection_duration: Histogram,
    pub connect_latency: Histogram,
}

impl MqttConnectionMetrics {
    pub fn new() -> Self {
        Self {
            connection_duration: Histogram::new(vec![
                1_000_000,     // 1s
                10_000_000,    // 10s
                60_000_000,    // 1min
                300_000_000,   // 5min
                600_000_000,   // 10min
                3_600_000_000, // 1hour
            ]),
            connect_latency: Histogram::latency_histogram(),
            ..Default::default()
        }
    }

    /// Record a new connection.
    pub fn connection_started(&self) {
        self.active_connections.inc();
        self.total_connections.inc();
    }

    /// Record a connection ended.
    pub fn connection_ended(&self, duration: Duration, clean: bool) {
        self.active_connections.dec();
        self.connection_duration.observe_duration(duration);
        if clean {
            self.clean_disconnects.inc();
        } else {
            self.abnormal_disconnects.inc();
        }
    }
}

// ---------------------------------------------------------------------------
// Subscription Metrics
// ---------------------------------------------------------------------------

/// Subscription-level metrics.
#[derive(Debug, Default, Clone)]
pub struct MqttSubscriptionMetrics {
    // Gauges
    pub total_subscriptions: Gauge,
    pub shared_subscriptions: Gauge,
    pub wildcard_subscriptions: Gauge,

    // Counters
    pub subscribe_operations: Counter,
    pub unsubscribe_operations: Counter,
    pub subscription_matches: Counter,

    // Per-topic metrics (sampled)
    pub topics_with_subscribers: Gauge,
}

impl MqttSubscriptionMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a subscribe operation.
    pub fn record_subscribe(&self, is_shared: bool, is_wildcard: bool) {
        self.subscribe_operations.inc();
        self.total_subscriptions.inc();
        if is_shared {
            self.shared_subscriptions.inc();
        }
        if is_wildcard {
            self.wildcard_subscriptions.inc();
        }
    }

    /// Record an unsubscribe operation.
    pub fn record_unsubscribe(&self, is_shared: bool, is_wildcard: bool) {
        self.unsubscribe_operations.inc();
        self.total_subscriptions.dec();
        if is_shared {
            self.shared_subscriptions.dec();
        }
        if is_wildcard {
            self.wildcard_subscriptions.dec();
        }
    }
}

// ---------------------------------------------------------------------------
// Message Flow Metrics
// ---------------------------------------------------------------------------

/// Message flow metrics.
#[derive(Debug, Default, Clone)]
pub struct MqttMessageMetrics {
    // Counters
    pub messages_received: Counter,
    pub messages_delivered: Counter,
    pub messages_dropped: Counter,
    pub messages_expired: Counter,
    pub messages_retained: Counter,

    // Gauges
    pub inflight_messages: Gauge,
    pub queued_messages: Gauge,
    pub retained_messages: Gauge,

    // Histograms
    pub message_latency: Histogram,
    pub message_size: Histogram,
}

impl MqttMessageMetrics {
    pub fn new() -> Self {
        Self {
            message_size: Histogram::new(vec![
                64,        // 64 bytes
                256,       // 256 bytes
                1024,      // 1KB
                4096,      // 4KB
                16_384,    // 16KB
                65_536,    // 64KB
                262_144,   // 256KB
                1_048_576, // 1MB
            ]),
            ..Default::default()
        }
    }

    /// Record a received message.
    pub fn record_received(&self, size: u64) {
        self.messages_received.inc();
        self.message_size.observe(size);
    }

    /// Record a delivered message.
    pub fn record_delivered(&self, latency: Duration) {
        self.messages_delivered.inc();
        self.message_latency.observe_duration(latency);
    }
}

// ---------------------------------------------------------------------------
// Aggregate MQTT Metrics
// ---------------------------------------------------------------------------

/// All MQTT metrics aggregated.
#[derive(Debug, Clone)]
pub struct MqttMetrics {
    pub protocol: MqttProtocolMetrics,
    pub connections: MqttConnectionMetrics,
    pub subscriptions: MqttSubscriptionMetrics,
    pub messages: MqttMessageMetrics,
}

impl Default for MqttMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MqttMetrics {
    pub fn new() -> Self {
        Self {
            protocol: MqttProtocolMetrics::new(),
            connections: MqttConnectionMetrics::new(),
            subscriptions: MqttSubscriptionMetrics::new(),
            messages: MqttMessageMetrics::new(),
        }
    }

    /// Export metrics as a map of name -> value.
    pub fn export(&self) -> HashMap<String, f64> {
        let mut metrics = HashMap::new();

        // Protocol metrics
        metrics.insert(
            "mqtt_connect_packets_total".into(),
            self.protocol.connect_packets.get() as f64,
        );
        metrics.insert(
            "mqtt_publish_packets_total".into(),
            self.protocol.publish_packets.get() as f64,
        );
        metrics.insert(
            "mqtt_subscribe_packets_total".into(),
            self.protocol.subscribe_packets.get() as f64,
        );
        metrics.insert(
            "mqtt_bytes_received_total".into(),
            self.protocol.bytes_received.get() as f64,
        );
        metrics.insert(
            "mqtt_bytes_sent_total".into(),
            self.protocol.bytes_sent.get() as f64,
        );
        metrics.insert(
            "mqtt_protocol_errors_total".into(),
            self.protocol.protocol_errors.get() as f64,
        );

        // Connection metrics
        metrics.insert(
            "mqtt_active_connections".into(),
            self.connections.active_connections.get() as f64,
        );
        metrics.insert(
            "mqtt_total_connections".into(),
            self.connections.total_connections.get() as f64,
        );
        metrics.insert(
            "mqtt_connect_latency_mean_us".into(),
            self.connections.connect_latency.mean(),
        );

        // Subscription metrics
        metrics.insert(
            "mqtt_total_subscriptions".into(),
            self.subscriptions.total_subscriptions.get() as f64,
        );
        metrics.insert(
            "mqtt_shared_subscriptions".into(),
            self.subscriptions.shared_subscriptions.get() as f64,
        );

        // Message metrics
        metrics.insert(
            "mqtt_messages_received_total".into(),
            self.messages.messages_received.get() as f64,
        );
        metrics.insert(
            "mqtt_messages_delivered_total".into(),
            self.messages.messages_delivered.get() as f64,
        );
        metrics.insert(
            "mqtt_messages_dropped_total".into(),
            self.messages.messages_dropped.get() as f64,
        );
        metrics.insert(
            "mqtt_message_latency_mean_us".into(),
            self.messages.message_latency.mean(),
        );

        metrics
    }
}

// ---------------------------------------------------------------------------
// Per-Client Metrics
// ---------------------------------------------------------------------------

/// Per-client statistics.
#[derive(Debug, Clone)]
pub struct ClientStats {
    pub client_id: String,
    pub connected_at: Instant,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub last_activity: Instant,
}

impl ClientStats {
    pub fn new(client_id: String) -> Self {
        let now = Instant::now();
        Self {
            client_id,
            connected_at: now,
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            last_activity: now,
        }
    }

    pub fn record_sent(&mut self, messages: u64, bytes: u64) {
        self.messages_sent += messages;
        self.bytes_sent += bytes;
        self.last_activity = Instant::now();
    }

    pub fn record_received(&mut self, messages: u64, bytes: u64) {
        self.messages_received += messages;
        self.bytes_received += bytes;
        self.last_activity = Instant::now();
    }

    pub fn connection_duration(&self) -> Duration {
        self.connected_at.elapsed()
    }

    pub fn idle_time(&self) -> Duration {
        self.last_activity.elapsed()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::new();
        assert_eq!(counter.get(), 0);

        counter.inc();
        assert_eq!(counter.get(), 1);

        counter.add(5);
        assert_eq!(counter.get(), 6);

        let value = counter.reset();
        assert_eq!(value, 6);
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_gauge() {
        let gauge = Gauge::new();
        assert_eq!(gauge.get(), 0);

        gauge.set(10);
        assert_eq!(gauge.get(), 10);

        gauge.inc();
        assert_eq!(gauge.get(), 11);

        gauge.dec();
        assert_eq!(gauge.get(), 10);
    }

    #[test]
    fn test_histogram() {
        let histogram = Histogram::new(vec![100, 500, 1000]);

        histogram.observe(50); // bucket 0 (<100)
        histogram.observe(200); // bucket 1 (<500)
        histogram.observe(800); // bucket 2 (<1000)
        histogram.observe(2000); // bucket 3 (>=1000)

        assert_eq!(histogram.count(), 4);
        assert_eq!(histogram.sum(), 50 + 200 + 800 + 2000);

        let buckets = histogram.bucket_counts();
        assert_eq!(buckets, vec![1, 1, 1, 1]);
    }

    #[test]
    fn test_mqtt_protocol_metrics() {
        let metrics = MqttProtocolMetrics::new();

        metrics.record_received_packet(1, 100); // CONNECT
        metrics.record_received_packet(3, 500); // PUBLISH
        metrics.record_message(1);

        assert_eq!(metrics.connect_packets.get(), 1);
        assert_eq!(metrics.publish_packets.get(), 1);
        assert_eq!(metrics.messages_qos1.get(), 1);
        assert_eq!(metrics.bytes_received.get(), 600);
    }

    #[test]
    fn test_mqtt_connection_metrics() {
        let metrics = MqttConnectionMetrics::new();

        metrics.connection_started();
        assert_eq!(metrics.active_connections.get(), 1);
        assert_eq!(metrics.total_connections.get(), 1);

        metrics.connection_ended(Duration::from_secs(10), true);
        assert_eq!(metrics.active_connections.get(), 0);
        assert_eq!(metrics.clean_disconnects.get(), 1);
    }

    #[test]
    fn test_mqtt_metrics_export() {
        let metrics = MqttMetrics::new();

        metrics.protocol.connect_packets.inc();
        metrics.connections.connection_started();

        let exported = metrics.export();
        assert_eq!(exported.get("mqtt_connect_packets_total"), Some(&1.0));
        assert_eq!(exported.get("mqtt_active_connections"), Some(&1.0));
    }

    #[test]
    fn test_client_stats() {
        let mut stats = ClientStats::new("client1".to_string());

        stats.record_sent(5, 1000);
        stats.record_received(3, 500);

        assert_eq!(stats.messages_sent, 5);
        assert_eq!(stats.messages_received, 3);
        assert_eq!(stats.bytes_sent, 1000);
        assert_eq!(stats.bytes_received, 500);
    }
}
