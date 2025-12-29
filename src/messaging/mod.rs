//! Message handling infrastructure.
//!
//! This module provides message flow control and persistence:
//! - `flow` - Backpressure and credit-based flow control
//! - `dedupe` - Message deduplication
//! - `offline` - Offline queue management for disconnected clients
//! - `retained` - Retained message storage
//! - `topics` - Generic topic routing and matching
//! - `consumer_groups` - Consumer group coordination (Kafka-style)
//! - `transactions` - Transaction coordination with two-phase commit
//! - `acks` - Acknowledgment tracking and redelivery
//! - `prefetch` - Prefetch and flow control for consumers

pub mod acks;
pub mod consumer_groups;
pub mod dedupe;
pub mod flow;
pub mod offline;
pub mod prefetch;
pub mod retained;
pub mod topics;
pub mod transactions;

pub use acks::*;
pub use consumer_groups::*;
pub use dedupe::*;
pub use flow::*;
pub use offline::*;
pub use prefetch::*;
pub use retained::*;
pub use topics::*;
pub use transactions::*;
