//! Replication and cluster coordination.
//!
//! This module provides distributed coordination:
//! - `consensus` - Raft consensus protocol
//! - `network` - Raft network transport
//! - `replica` - Replica management
//! - `routing` - PRG routing and placement
//! - `forwarding` - Cross-PRG message forwarding
//! - `client` - Cluster client for durability ledger

pub mod client;
pub mod consensus;
pub mod forwarding;
pub mod network;
pub mod replica;
pub mod routing;

pub use client::*;
pub use consensus::*;
pub use forwarding::*;
pub use network::*;
pub use replica::*;
pub use routing::*;
