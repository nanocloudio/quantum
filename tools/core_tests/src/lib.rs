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

/// Session identity: Fluxor `session_id` minting and the rule that keeps
/// Quantum's `session_generation` and Fluxor's `session_epoch` from
/// diverging.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
pub mod session_identity {
    include!("../../../modules/common/cores/session_identity_core.rs");
}

/// The MQTT anchor: delivery envelopes and their admission, liveness-only
/// mode, read-pause backpressure and the framed hold/replay FIFO.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
pub mod mqtt_anchor {
    include!("../../../modules/common/cores/mqtt_anchor_core.rs");
}

/// Session replay across a worker handoff: the QSR2 tail, the import
/// cursor rule, and the outbound delivery ledger the log rebuilds.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
pub mod session_replay {
    include!("../../../modules/common/cores/session_replay_core.rs");
}

/// Anchor continuity across an ingress takeover: the production envelope,
/// the output-horizon gate, the worker's retained productions, the
/// anchor's `QAC1` record and the re-anchor exchange. Mounted beside the
/// anchor core it names by bare name, as the modules mount it.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
pub mod anchor_continuity {
    include!("../../../modules/common/cores/mqtt_anchor_core.rs");
    include!("../../../modules/common/cores/anchor_continuity_core.rs");
    include!("../../../modules/common/cores/production_seat_core.rs");
}

/// A session exported to a surviving site and imported there (`QSS1`):
/// subscriptions, in-flight and owed deliveries, the dedupe and apply
/// cursors, and the import rule. Mounted beside the replay and identity
/// cores it names by bare name.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC core verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
pub mod session_site {
    include!("../../../modules/common/cores/session_replay_core.rs");
    include!("../../../modules/common/cores/session_identity_core.rs");
    include!("../../../modules/common/cores/session_site_core.rs");
}

/// The dial authority the seven outbound connectors compose their
/// `CMD_CONNECT_TO` records from. `#[path]`-mounted rather than
/// `include!`d so its own mount of the SDK contract resolves against
/// `modules/common/`, exactly as it does in a module build.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC helper verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
#[path = "../../../modules/common/authority.rs"]
pub mod authority;

/// The provider half of a PUBLISH exchange: request collection and
/// judgement, the in-flight window, the owed-record queue and LINK, as
/// every broker sink answers through it. `#[path]`-mounted for the same
/// reason as `authority`: its own mount of the SDK exchange contract.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC helper verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
#[path = "../../../modules/common/publish_exchange.rs"]
pub mod publish_exchange;

/// The held-exchange provider of the broker's subscription front:
/// request judgement, subscription phases, credit, held messages and the
/// records owed. `#[path]`-mounted for its own mount of the SDK contract.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC helper verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
#[path = "../../../modules/common/subscribe_exchange.rs"]
pub mod subscribe_exchange;

/// The broker front's side of the codec envelope: the session envelopes it
/// writes to `session_processor` and the answers it reads back.
#[allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles a no_std PIC helper verbatim into a host test crate; \
              `fluxor ci` lints `modules/**` against the module build, not \
              against this test-shaped package"
)]
#[path = "../../../modules/common/front_session.rs"]
pub mod front_session;

#[cfg(test)]
mod anchor_continuity_tests;
#[cfg(test)]
mod authority_tests;
#[cfg(test)]
mod edge_routing_tests;
#[cfg(test)]
mod front_session_tests;
#[cfg(test)]
mod kafka_group_tests;
#[cfg(test)]
mod kafka_idem_tests;
#[cfg(test)]
mod kafka_log_tests;
#[cfg(test)]
mod kafka_metadata_tests;
#[cfg(test)]
mod mqtt_anchor_tests;
#[cfg(test)]
mod production_seat_tests;
#[cfg(test)]
mod publish_exchange_tests;
#[cfg(test)]
mod session_identity_tests;
#[cfg(test)]
mod session_replay_tests;
#[cfg(test)]
mod session_site_tests;
#[cfg(test)]
mod subscribe_exchange_tests;
