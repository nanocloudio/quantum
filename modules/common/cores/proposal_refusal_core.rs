// Bounded, no_std, no-alloc PROPOSAL REFUSAL core — what Clustor's refusal
// of a tagged proposal means for the operation waiting on it.
// `include!`d by `session_processor` and by the host test crate; it names
// nothing outside this file.
//
// Clustor answers every tagged proposal on `proposal_assigned`: with
// MSG_PROPOSAL_ASSIGNED once it is in a log, or with MSG_PROPOSAL_REFUSED
// `[correlation_id:u64 LE][status:u8][leader_id:u8]` when it is not — a
// follower that forwarded it heard no leader for its hold, the leader
// pushed back, was frozen, or found it too large. No assignment follows a
// refusal, so the correlation it names is freed at once and the operation
// waiting on it is failed now, in its protocol's terms, rather than left to
// a timeout that may never come.
//
// Every refusal is "not appended anywhere" except `OUTCOME_UNKNOWN`: the
// forward reached a leader that may have appended it and none could say.
// Each answer below is one a client may retry — and a retry is exact for
// MQTT (the client resends under the same packet id, which the broker
// deduplicates) — so that case is answered like the others rather than
// claimed committed or claimed lost.

/// One MSG_PROPOSAL_REFUSED payload, decoded.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Refusal {
    pub correlation_id: u64,
    /// A `REFUSED_*` code.
    pub status: u8,
    /// The leader the refusing node knows; 0xFF none.
    pub leader_id: u8,
}

/// Decode a MSG_PROPOSAL_REFUSED payload
/// `[correlation_id:u64 LE][status:u8][leader_id:u8]`; `None` when short.
pub fn decode_refused(payload: &[u8]) -> Option<Refusal> {
    let b = payload.get(..10)?;
    Some(Refusal {
        correlation_id: u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
        status: b[8],
        leader_id: b[9],
    })
}

/// Clustor's `CLIENT_REJECT_*` status codes, as a refusal carries them.
pub const REFUSED_THROTTLED: u8 = 0x01;
pub const REFUSED_NOT_LEADER: u8 = 0x02;
pub const REFUSED_FALLBACK: u8 = 0x04;
pub const REFUSED_TOO_LARGE: u8 = 0x06;
pub const REFUSED_OUTCOME_UNKNOWN: u8 = 0x07;

/// MQTT 5 reason codes a refused publish disconnects with.
pub const MQTT_REASON_SERVER_BUSY: u8 = 0x89;
pub const MQTT_REASON_PACKET_TOO_LARGE: u8 = 0x95;
pub const MQTT_REASON_QUOTA_EXCEEDED: u8 = 0x97;

/// Kafka error codes a refused produce or offset commit answers with.
pub const KERR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const KERR_REQUEST_TIMED_OUT: i16 = 7;
pub const KERR_MESSAGE_TOO_LARGE: i16 = 10;

/// The DISCONNECT reason for an MQTT PUBLISH or PUBREL whose proposal was
/// refused. MQTT has no negative acknowledgement that keeps a packet id for
/// a retry (an MQTT 5 PUBACK reason code ends the flow; MQTT 3.1.1 has none),
/// so the connection is closed — sent as DISCONNECT to an MQTT 5 client,
/// closed for an MQTT 3.1.1 one — and the client resends its unacknowledged
/// packets on reconnect, as both versions require. The reason says why:
/// backpressure is a quota, an oversize entry is too large, and anything
/// else (no leader, frozen, outcome unknown) is the server being busy.
pub fn mqtt_disconnect_reason(status: u8) -> u8 {
    match status {
        REFUSED_THROTTLED => MQTT_REASON_QUOTA_EXCEEDED,
        REFUSED_TOO_LARGE => MQTT_REASON_PACKET_TOO_LARGE,
        _ => MQTT_REASON_SERVER_BUSY,
    }
}

/// The per-partition error for a Kafka Produce partition whose proposal was
/// refused: no leader is `NOT_LEADER_OR_FOLLOWER` (the producer refreshes
/// metadata and retries), an oversize entry `MESSAGE_TOO_LARGE` (it splits
/// the batch), and backpressure, a frozen leader or an unknown outcome
/// `REQUEST_TIMED_OUT` — Kafka's own "may or may not have been written",
/// which an idempotent producer retries exactly.
pub fn kafka_produce_error(status: u8) -> i16 {
    match status {
        REFUSED_NOT_LEADER => KERR_NOT_LEADER_OR_FOLLOWER,
        REFUSED_TOO_LARGE => KERR_MESSAGE_TOO_LARGE,
        _ => KERR_REQUEST_TIMED_OUT,
    }
}

/// The error a Kafka OffsetCommit answers every partition with when one of
/// its offset proposals was refused: `REQUEST_TIMED_OUT`, which a consumer
/// retries (`NOT_LEADER_OR_FOLLOWER` and `MESSAGE_TOO_LARGE` are not
/// OffsetCommit errors).
pub fn kafka_offset_error(_status: u8) -> i16 {
    KERR_REQUEST_TIMED_OUT
}

/// Overwrite every partition's error code in a parked OffsetCommit
/// response body — `[topics:i32 BE]` then per topic `[name_len:i16 BE]
/// [name][partitions:i32 BE]` and per partition `[partition:i32 BE]
/// [error_code:i16 BE]` — with `err`. Returns false, leaving the body
/// unchanged, when it does not walk to its exact end: a body that is not
/// that shape is never half-rewritten.
pub fn offset_commit_fail(body: &mut [u8], err: i16) -> bool {
    // Walk once to prove the shape, then again writing the code.
    offset_commit_walk(body, None) && offset_commit_walk(body, Some(err.to_be_bytes()))
}

/// One walk of an OffsetCommit response body; with `code`, each partition's
/// error code is overwritten as it is passed. True when the body ends
/// exactly where its counts say.
fn offset_commit_walk(b: &mut [u8], code: Option<[u8; 2]>) -> bool {
    let be32 = |b: &[u8], at: usize| -> Option<i32> {
        let x = b.get(at..at.checked_add(4)?)?;
        Some(i32::from_be_bytes([x[0], x[1], x[2], x[3]]))
    };
    let be16 = |b: &[u8], at: usize| -> Option<i16> {
        let x = b.get(at..at.checked_add(2)?)?;
        Some(i16::from_be_bytes([x[0], x[1]]))
    };
    let Some(topics) = be32(b, 0).filter(|n| *n >= 0) else {
        return false;
    };
    let mut p = 4usize;
    for _ in 0..topics {
        let Some(name) = be16(b, p).filter(|n| *n >= 0) else {
            return false;
        };
        p += 2 + name as usize;
        let Some(parts) = be32(b, p).filter(|n| *n >= 0) else {
            return false;
        };
        p += 4;
        for _ in 0..parts {
            let Some(slot) = b.get_mut(p + 4..p + 6) else {
                return false;
            };
            if let Some(c) = code {
                slot.copy_from_slice(&c);
            }
            p += 6;
        }
    }
    p == b.len()
}
