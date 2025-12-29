//! Concrete workload implementations registered with the runtime.
//!
//! This module contains protocol-specific workload implementations that
//! implement the `PrgWorkload` trait. Each workload manages its own state,
//! snapshot format, and log entry types.
//!
//! Currently supported workloads:
//! - MQTT: Full MQTT 5.0 broker implementation with session management,
//!   dedupe, offline queues, retained messages, and subscription index.
//! - Kafka: Kafka protocol adapter supporting producer/consumer APIs,
//!   consumer groups, transactions, and idempotent producers.

#[cfg(feature = "mqtt")]
pub mod mqtt;

#[cfg(feature = "kafka")]
pub mod kafka;
