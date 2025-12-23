//! Operations and observability.
//!
//! This module provides operational tooling:
//! - `version` - Version compatibility checks and fault injection
//! - `observability` - Metrics and health checks
//! - `telemetry` - Telemetry collection and export
//! - `audit` - Audit logging
//! - `dr` - Disaster recovery

pub mod audit;
pub mod dr;
pub mod observability;
pub mod telemetry;
pub mod version;

pub use audit::*;
pub use dr::*;
pub use observability::*;
pub use telemetry::*;
pub use version::*;
