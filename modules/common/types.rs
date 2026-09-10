//! Shared Quantum types for all fluxor modules.

#![allow(
    dead_code,
    reason = "shared types library; individual consumers each use a different subset"
)]

pub type TenantId = u32;
pub type StreamHash = u64;
/// Quantum's session fence: advances on every committed CONNECT and keys
/// the dedupe table and every in-flight QoS completion. Deliberately NOT
/// called an epoch: Fluxor's SessionCtrlV1 `session_epoch` fences the
/// placement of a client attachment and advances on every rebind, and a
/// worker move must leave this counter untouched. The rule relating the
/// two lives in `cores/session_identity_core.rs`.
pub type SessionGeneration = u32;
pub type MessageId = u32;
pub type PrgId = u16;
pub type RoutingEpoch = u32;

/// Protocol identifiers.
pub const PROTO_MQTT: u8 = 0;
pub const PROTO_KAFKA: u8 = 1;
pub const PROTO_AMQP: u8 = 2;
pub const PROTO_UNKNOWN: u8 = 0xFF;

/// MQTT QoS levels.
pub const QOS_0: u8 = 0;
pub const QOS_1: u8 = 1;
pub const QOS_2: u8 = 2;

/// QoS 2 four-phase states.
pub const QOS2_PUBLISH: u8 = 0;
pub const QOS2_PUBREC: u8 = 1;
pub const QOS2_PUBREL: u8 = 2;
pub const QOS2_PUBCOMP: u8 = 3;

/// Backpressure reasons.
pub const BP_TRANSIENT: u8 = 0;
pub const BP_PERMANENT_DURABILITY: u8 = 1;
pub const BP_PERMANENT_EPOCH: u8 = 2;

/// Audit event categories.
pub const AUDIT_CONNECT: u8 = 0;
pub const AUDIT_DISCONNECT: u8 = 1;
pub const AUDIT_AUTH_FAIL: u8 = 2;
pub const AUDIT_ACL_DENY: u8 = 3;
pub const AUDIT_THROTTLE: u8 = 4;
pub const AUDIT_DR_EVENT: u8 = 5;
pub const AUDIT_ADMIN: u8 = 6;

/// Consumer group states.
pub const GROUP_EMPTY: u8 = 0;
pub const GROUP_PREPARING: u8 = 1;
pub const GROUP_STABLE: u8 = 2;
pub const GROUP_REBALANCING: u8 = 3;

/// Transaction states.
pub const TXN_BEGIN: u8 = 0;
pub const TXN_PREPARE: u8 = 1;
pub const TXN_COMMIT: u8 = 2;
pub const TXN_ABORT: u8 = 3;

/// Default timers (ms).
pub const SESSION_TTL_DEFAULT_MS: u64 = 259_200_000;
pub const DEDUPE_TTL_DEFAULT_MS: u64 = 259_200_000;
pub const OFFLINE_TTL_DEFAULT_MS: u64 = 259_200_000;
