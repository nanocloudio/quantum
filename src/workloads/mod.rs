//! Concrete workload implementations registered with the runtime.
//!
//! This module contains protocol-specific workload implementations that
//! implement the `PrgWorkload` trait. Each workload manages its own state,
//! snapshot format, and log entry types.
//!
//! Currently supported workloads:
//! - MQTT: Full MQTT 5.0 broker implementation with session management,
//!   dedupe, offline queues, retained messages, and subscription index.

pub mod mqtt;
