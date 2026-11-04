//! What Clustor's refusal of a tagged proposal means for the operation
//! waiting on it (`proposal_refusal_core.rs`).
//!
//! The failures prevented: a refusal misread as another message or read
//! past its end; a refused publish answered as committed or left to hang;
//! a Kafka partition answered with an error its client does not retry;
//! a parked `OffsetCommit` answer that still says "committed", or one
//! half-rewritten when it is not the shape the broker built.

use super::proposal_refusal::*;

fn payload(cid: u64, status: u8, leader: u8) -> Vec<u8> {
    let mut p = cid.to_le_bytes().to_vec();
    p.extend_from_slice(&[status, leader]);
    p
}

#[test]
fn a_refusal_decodes_whole_or_not_at_all() {
    assert_eq!(
        decode_refused(&payload(0x0102_0304_0506_0708, REFUSED_NOT_LEADER, 0xFF)),
        Some(Refusal {
            correlation_id: 0x0102_0304_0506_0708,
            status: REFUSED_NOT_LEADER,
            leader_id: 0xFF,
        })
    );
    // A longer payload decodes its first ten bytes; a shorter one is none.
    let mut long = payload(9, REFUSED_THROTTLED, 2);
    long.push(0xAA);
    assert_eq!(decode_refused(&long).map(|r| r.leader_id), Some(2));
    assert_eq!(decode_refused(&payload(9, 1, 2)[..9]), None);
    assert_eq!(decode_refused(&[]), None);
}

#[test]
fn a_refused_mqtt_publish_disconnects_with_the_reason() {
    assert_eq!(
        mqtt_disconnect_reason(REFUSED_THROTTLED),
        MQTT_REASON_QUOTA_EXCEEDED
    );
    assert_eq!(
        mqtt_disconnect_reason(REFUSED_TOO_LARGE),
        MQTT_REASON_PACKET_TOO_LARGE
    );
    for st in [
        REFUSED_NOT_LEADER,
        REFUSED_FALLBACK,
        REFUSED_OUTCOME_UNKNOWN,
        0x55,
    ] {
        assert_eq!(
            mqtt_disconnect_reason(st),
            MQTT_REASON_SERVER_BUSY,
            "{st:#x}"
        );
    }
}

#[test]
fn a_refused_kafka_partition_answers_an_error_its_client_retries() {
    assert_eq!(
        kafka_produce_error(REFUSED_NOT_LEADER),
        KERR_NOT_LEADER_OR_FOLLOWER
    );
    assert_eq!(
        kafka_produce_error(REFUSED_TOO_LARGE),
        KERR_MESSAGE_TOO_LARGE
    );
    for st in [REFUSED_THROTTLED, REFUSED_FALLBACK, REFUSED_OUTCOME_UNKNOWN] {
        assert_eq!(kafka_produce_error(st), KERR_REQUEST_TIMED_OUT, "{st:#x}");
        assert_eq!(kafka_offset_error(st), KERR_REQUEST_TIMED_OUT, "{st:#x}");
    }
    // An OffsetCommit has no NOT_LEADER or TOO_LARGE answer.
    assert_eq!(
        kafka_offset_error(REFUSED_NOT_LEADER),
        KERR_REQUEST_TIMED_OUT
    );
    assert_eq!(
        kafka_offset_error(REFUSED_TOO_LARGE),
        KERR_REQUEST_TIMED_OUT
    );
}

/// An `OffsetCommit` response body: `topics` of `(name, partitions)`.
fn offset_body(topics: &[(&str, &[i32])]) -> Vec<u8> {
    let count = |n: usize| i32::try_from(n).expect("a test-sized count");
    let mut b = count(topics.len()).to_be_bytes().to_vec();
    for (name, parts) in topics {
        let len = i16::try_from(name.len()).expect("a test-sized name");
        b.extend_from_slice(&len.to_be_bytes());
        b.extend_from_slice(name.as_bytes());
        b.extend_from_slice(&count(parts.len()).to_be_bytes());
        for p in *parts {
            b.extend_from_slice(&p.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
        }
    }
    b
}

/// Every partition's error code, in order.
fn errors(b: &[u8], topics: &[(&str, &[i32])]) -> Vec<i16> {
    let mut out = Vec::new();
    let mut p = 4;
    for (name, parts) in topics {
        p += 2 + name.len() + 4;
        for _ in *parts {
            out.push(i16::from_be_bytes([b[p + 4], b[p + 5]]));
            p += 6;
        }
    }
    out
}

#[test]
fn a_parked_offset_commit_answer_fails_every_partition() {
    let shape: &[(&str, &[i32])] = &[("orders", &[0, 1, 2]), ("t", &[7]), ("empty", &[])];
    let mut b = offset_body(shape);
    let partitions = b.clone();
    assert!(offset_commit_fail(&mut b, KERR_REQUEST_TIMED_OUT));
    assert_eq!(errors(&b, shape), vec![KERR_REQUEST_TIMED_OUT; 4]);
    // Only the error codes changed: names and partition ids are intact.
    let changed: Vec<usize> = (0..b.len()).filter(|&i| b[i] != partitions[i]).collect();
    assert_eq!(changed.len(), 4, "one byte of each code (7 = 0x0007)");
    // No topics at all is a valid, empty answer.
    let mut none = offset_body(&[]);
    assert!(offset_commit_fail(&mut none, KERR_REQUEST_TIMED_OUT));
}

#[test]
fn a_body_not_of_that_shape_is_never_half_rewritten() {
    let shape: &[(&str, &[i32])] = &[("orders", &[0, 1])];
    let good = offset_body(shape);
    // Cut short anywhere, or with bytes past its end, or a negative count.
    for cut in 0..good.len() {
        let mut b = good[..cut].to_vec();
        let before = b.clone();
        assert!(
            !offset_commit_fail(&mut b, KERR_REQUEST_TIMED_OUT),
            "cut {cut}"
        );
        assert_eq!(b, before, "cut {cut} left untouched");
    }
    let mut long = good.clone();
    long.push(0);
    let before = long.clone();
    assert!(!offset_commit_fail(&mut long, KERR_REQUEST_TIMED_OUT));
    assert_eq!(long, before);
    let mut neg = good.clone();
    neg[..4].copy_from_slice(&(-1i32).to_be_bytes());
    assert!(!offset_commit_fail(&mut neg, KERR_REQUEST_TIMED_OUT));
    // A count far past the body is refused, not looped over.
    let mut huge = good;
    huge[..4].copy_from_slice(&i32::MAX.to_be_bytes());
    assert!(!offset_commit_fail(&mut huge, KERR_REQUEST_TIMED_OUT));
}
