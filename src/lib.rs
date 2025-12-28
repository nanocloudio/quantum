#![deny(unused, dead_code)]
#![deny(clippy::all, clippy::pedantic)]
// Module naming: common pattern in domain-driven code
#![allow(clippy::module_name_repetitions)]
// Function complexity: some functions are inherently complex
#![allow(clippy::too_many_lines)]
#![allow(clippy::too_many_arguments)]
// Variable naming: domain terms often similar
#![allow(clippy::similar_names)]
// Documentation style: many terms don't need backticks
#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
// API ergonomics: prefer simplicity over must_use annotations
#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]
// Format strings: allow non-inlined for readability
#![allow(clippy::uninlined_format_args)]
// Import style
#![allow(clippy::wildcard_imports)]
// Struct field patterns
#![allow(clippy::struct_excessive_bools)]
#![allow(clippy::struct_field_names)]
// Numeric casts: intentional in protocol code
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_lossless)]
// Control flow style
#![allow(clippy::if_not_else)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::single_match_else)]
#![allow(clippy::match_wildcard_for_single_variants)]
#![allow(clippy::manual_let_else)]
// Passing style
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::trivially_copy_pass_by_ref)]
// Self usage
#![allow(clippy::unused_self)]
#![allow(clippy::used_underscore_binding)]
// Clone/assign patterns
#![allow(clippy::assigning_clones)]
// Option/Result patterns
#![allow(clippy::unnecessary_wraps)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::map_unwrap_or)]
// Type defaults
#![allow(clippy::default_trait_access)]
#![allow(clippy::implicit_hasher)]
// Inlining
#![allow(clippy::inline_always)]
// Iterator patterns
#![allow(clippy::iter_without_into_iter)]
// Reference patterns
#![allow(clippy::ref_option)]
// Closure style
#![allow(clippy::redundant_closure_for_method_calls)]
// Unit patterns
#![allow(clippy::ignored_unit_patterns)]
// Large types
#![allow(clippy::large_futures)]
#![allow(clippy::large_enum_variant)]
// Explicit type bounds
#![allow(clippy::significant_drop_tightening)]
// Copy vs clone style
#![allow(clippy::cloned_instead_of_copied)]
// String conversion efficiency
#![allow(clippy::inefficient_to_string)]
// Sort stability
#![allow(clippy::stable_sort_primitive)]
// Debug impl completeness
#![allow(clippy::missing_fields_in_debug)]
// Error handling style
#![allow(clippy::result_large_err)]
#![allow(clippy::unnecessary_box_returns)]
// Boolean ops
#![allow(clippy::nonminimal_bool)]
// Explicit returns
#![allow(clippy::needless_return)]
#![allow(clippy::semicolon_if_nothing_returned)]
// Cast wrapping
#![allow(clippy::cast_possible_wrap)]
// Iteration style
#![allow(clippy::explicit_iter_loop)]
#![allow(clippy::explicit_into_iter_loop)]
// Bool conversion
#![allow(clippy::bool_to_int_with_if)]
// String allocation efficiency
#![allow(clippy::format_push_string)]
// File extension comparison
#![allow(clippy::case_sensitive_file_extension_comparisons)]
// Pointer casts
#![allow(clippy::ptr_as_ptr)]
#![allow(clippy::ptr_cast_constness)]
// Async functions that may not await yet
#![allow(clippy::unused_async)]

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
