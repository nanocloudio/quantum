//! Message handling infrastructure.
//!
//! This module provides message flow control and persistence:
//! - `flow` - Backpressure and credit-based flow control
//! - `dedupe` - Message deduplication
//! - `offline` - Offline queue management for disconnected clients
//! - `retained` - Retained message storage

pub mod dedupe;
pub mod flow;
pub mod offline;
pub mod retained;

pub use dedupe::*;
pub use flow::*;
pub use offline::*;
pub use retained::*;
