//! Durable storage subsystem.
//!
//! This module provides persistent storage capabilities:
//! - `wal` - Write-ahead log, snapshots, and durable storage
//! - `compaction` - Log compaction and cleanup

pub mod compaction;
pub mod wal;

pub use compaction::*;
pub use wal::*;
