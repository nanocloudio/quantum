//! Host test bed for `modules/common/cores/*`.
//!
//! The cores are bounded, `no_std`, no-alloc logic shared by the PIC
//! modules. They are written to be `include!`d by a host crate as well
//! as by the modules, so their invariants can be tested without booting
//! a graph — see `kafka_core.rs`'s header. This is that crate.

/// Kafka partition-log semantics: segment framing, the sparse
/// offset/time indexes, watermarks, retention and roll boundaries.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package — mirrors \
              clustor's host tests over the same kind of source"
)]
pub mod kafka_log {
    include!("../../../modules/common/cores/kafka_log_core.rs");
}

/// Kafka group-coordinator semantics: membership, monotone generations,
/// and broker-owned session expiry.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
pub mod kafka_group {
    include!("../../../modules/common/cores/kafka_group_core.rs");
}

/// Edge-plane routing: owner resolution, epoch disposition, and the
/// per-protocol redirect style.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
pub mod edge_routing {
    include!("../../../modules/common/cores/edge_routing_core.rs");
}

/// Kafka idempotent-producer sequence bookkeeping.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
pub mod kafka_idem {
    include!("../../../modules/common/cores/kafka_idem_core.rs");
}

/// What a Kafka Metadata response should say about the cluster.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
pub mod kafka_metadata {
    include!("../../../modules/common/cores/kafka_metadata_core.rs");
}

#[cfg(test)]
mod edge_routing_tests;
#[cfg(test)]
mod kafka_group_tests;
#[cfg(test)]
mod kafka_idem_tests;
#[cfg(test)]
mod kafka_log_tests;
#[cfg(test)]
mod kafka_metadata_tests;
