//! Quantum - Multi-protocol message broker with Raft-based replication.
//!
//! # Module Organization
//!
//! ## Core
//! - `core::config` - Configuration parsing and validation
//! - `core::runtime` - Main runtime orchestration
//! - `core::time` - Deterministic time utilities
//!
//! ## Control Plane
//! - `control` - Tenant capabilities, routing, placement
//!
//! ## Replication
//! - `replication::consensus` - Raft consensus protocol
//! - `replication::network` - Raft network transport
//! - `replication::replica` - Replica management
//! - `replication::routing` - PRG routing and placement
//! - `replication::forwarding` - Cross-PRG message forwarding
//! - `replication::client` - Cluster client for durability ledger
//!
//! ## Storage
//! - `storage::wal` - Write-ahead log, snapshots, durable storage
//! - `storage::compaction` - Log compaction and cleanup
//!
//! ## PRG
//! - `prg` - Partition Replica Group management
//! - `prg::workload` - Workload trait and helpers
//! - `prg::workload_rpc` - Remote workload RPC
//!
//! ## Messaging
//! - `messaging::flow` - Backpressure and credit-based flow control
//! - `messaging::dedupe` - Message deduplication
//! - `messaging::offline` - Offline queue management
//! - `messaging::retained` - Retained message storage
//!
//! ## Networking
//! - `net::tls` - TLS and mTLS configuration
//! - `net::security` - Security manager and credentials
//! - `net::listeners` - Edge listeners and protocol routing
//!
//! ## Workloads
//! - `workloads::mqtt` - MQTT protocol workload
//!
//! ## Operations
//! - `ops::version` - Version compatibility and fault injection
//! - `ops::observability` - Metrics and health checks
//! - `ops::telemetry` - Telemetry collection
//! - `ops::audit` - Audit logging
//! - `ops::dr` - Disaster recovery
//!
//! ## Tools
//! - `toolkit` - Development and debugging tools

// Core infrastructure
pub mod core;

// Control plane
pub mod control;

// Replication
pub mod replication;

// Storage
pub mod storage;

// PRG & Workloads
pub mod prg;
pub mod workloads;

// Messaging
pub mod messaging;

// Networking
pub mod net;

// Operations
pub mod ops;

// Tools
pub mod toolkit;

// CLI
pub mod cli;

// Re-exports for convenience and backward compatibility
pub use self::core::{config, runtime, time};
pub use messaging::{dedupe, flow, offline, retained};
pub use net::{listeners, security, tls};
pub use ops::version::{FaultInjector, VersionGate};
pub use ops::{audit, dr, observability, telemetry};
pub use replication::{client as clustor_client, forwarding, routing};
pub use replication::{consensus as raft, network as raft_net, replica as raft_replica};
pub use workloads::mqtt;
pub use workloads::mqtt::session;
pub use workloads::mqtt::subscriptions;

// Backward compatibility: expose ClustorStorage at crate root
pub use storage::wal::ClustorStorage;
