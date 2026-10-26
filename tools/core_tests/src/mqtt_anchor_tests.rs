//! MQTT anchor: the delivery envelope, the worker's admission rule for
//! it, liveness-only mode, the read-pause backpressure, and the framed
//! FIFO the hold and the replay window are built on.
//!
//! The failures these rules prevent are the ones that make a worker
//! handoff observable to an MQTT client: a frame consumed twice after a
//! retry, a stale worker acting on a frame meant for its replacement, a
//! PINGRESP that let a client believe application work was taken, a
//! hold that overflowed before anything pushed back.

use super::mqtt_anchor::*;

const SID: [u8; SESSION_ID_LEN] = *b"QANCHOR0\x00\x07\x00\x00\x00\x01\x00\x00";

/// A session envelope `[conn:2][proto][pkt][flags][body]`.
fn env(conn: u16, pkt: u8, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut v = conn.to_le_bytes().to_vec();
    v.extend_from_slice(&[ENV_PROTO_MQTT, pkt, flags]);
    v.extend_from_slice(body);
    v
}

// ── Delivery envelope ───────────────────────────────────────────────────────

#[test]
fn a_delivery_envelope_round_trips_and_unwraps_to_the_original() {
    let inner = env(7, MQTT_PUBLISH, 0x02, b"topic-and-payload");
    let mut out = [0u8; 128];
    let n = encode_delivery(&mut out, 0x98, &inner, &SID, 3, 42);
    assert_eq!(n, inner.len() + DELIVERY_INSERT);
    let h = decode_delivery(&out[..n]).expect("header");
    assert_eq!(
        h,
        DeliveryHeader {
            conn: 7,
            session_id: SID,
            epoch: 3,
            delivery_id: 42,
            inner_msg_type: 0x98,
        }
    );
    let (h2, m) = unwrap_delivery_in_place(&mut out, n).expect("unwrap");
    assert_eq!(h2, h);
    assert_eq!(&out[..m], &inner[..], "the worker sees exactly the frame");
}

#[test]
fn a_delivery_envelope_is_never_truncated() {
    let inner = env(1, MQTT_PUBLISH, 0, &[0xAB; 64]);
    let mut short = [0u8; 64];
    assert_eq!(encode_delivery(&mut short, 0x98, &inner, &SID, 1, 1), 0);
    assert_eq!(encode_delivery(&mut [0u8; 64], 0x98, &[1], &SID, 1, 1), 0);
    assert!(decode_delivery(&[0u8; CONN_ID_LEN + DELIVERY_INSERT - 1]).is_none());
}

// ── The worker's admission rule ─────────────────────────────────────────────

fn hdr(epoch: u32, id: u64) -> DeliveryHeader {
    DeliveryHeader {
        conn: 7,
        session_id: SID,
        epoch,
        delivery_id: id,
        inner_msg_type: 0x98,
    }
}

#[test]
fn new_work_at_the_bound_epoch_is_accepted() {
    assert_eq!(
        admit_delivery(&SID, 2, 9, &hdr(2, 10)),
        DeliveryVerdict::Accept
    );
}

/// A replacement worker rejects old epochs: a frame decided under a
/// binding the session has left must not be acted on.
#[test]
fn an_old_epoch_is_rejected() {
    assert_eq!(
        admit_delivery(&SID, 3, 0, &hdr(2, 1)),
        DeliveryVerdict::StaleEpoch
    );
}

#[test]
fn an_epoch_the_worker_was_not_resumed_at_is_refused() {
    assert_eq!(
        admit_delivery(&SID, 2, 0, &hdr(3, 1)),
        DeliveryVerdict::FutureEpoch
    );
}

/// A retried delivery id is a duplicate whatever epoch it carries now:
/// the id was assigned when the client sent the frame.
#[test]
fn a_retried_delivery_id_is_a_duplicate() {
    assert_eq!(
        admit_delivery(&SID, 2, 10, &hdr(2, 10)),
        DeliveryVerdict::Duplicate
    );
    assert_eq!(
        admit_delivery(&SID, 2, 10, &hdr(2, 3)),
        DeliveryVerdict::Duplicate
    );
    assert_eq!(
        admit_delivery(&SID, 2, 0, &hdr(2, 0)),
        DeliveryVerdict::Duplicate
    );
}

#[test]
fn another_sessions_envelope_is_unknown() {
    let mut other = SID;
    other[15] = 9;
    assert_eq!(
        admit_delivery(&other, 2, 0, &hdr(2, 1)),
        DeliveryVerdict::UnknownSession
    );
    assert_eq!(
        admit_delivery(&SID, 0, 0, &hdr(0, 1)),
        DeliveryVerdict::UnknownSession
    );
}

// ── Delivery and production ids ─────────────────────────────────────────────

/// A refused write retried later keeps its id: the id is only consumed
/// once the frame is written or held.
#[test]
fn a_delivery_id_is_stable_until_committed() {
    let mut c = AnchorConn::zero();
    assert_eq!(c.peek_delivery(), 1);
    assert_eq!(c.peek_delivery(), 1, "a refusal does not consume it");
    c.commit_delivery();
    assert_eq!(c.peek_delivery(), 2);
}

#[test]
fn a_production_id_advances_only_on_transport_acceptance() {
    let mut c = AnchorConn::zero();
    let id = c.next_production;
    assert_eq!(c.next_production, id, "a refused response keeps its id");
    c.accept_production(55);
    assert_eq!(c.next_production, id + 1);
    assert_eq!(c.last_out_ms, 55);
}

#[test]
fn connect_records_the_identity_the_worker_keys_by() {
    let mut c = AnchorConn::zero();
    c.on_connect(5, 30, b"client-a");
    assert_eq!(c.mqtt_version, 5);
    assert_eq!(c.keepalive_s, 30);
    assert_eq!(c.client_hash, fnv1a_64(b"client-a"));
    assert!(c.has(FLAG_CONNECT_ADMITTED));
    // `session_processor` keys the session by the same hash.
    assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
}

// ── Liveness ────────────────────────────────────────────────────────────────

#[test]
fn a_live_worker_gets_the_ping_as_a_liveness_mark() {
    let mut c = AnchorConn::zero();
    c.phase = Phase::Active;
    assert_eq!(ping_action(&c, false), PingAction::AnswerAndMark);
    assert_eq!(ping_action(&c, true), PingAction::AnswerAndMark);
}

/// With the worker absent the anchor answers PINGREQ only when the
/// route admits liveness-only mode; otherwise the PINGRESP is owed.
#[test]
fn an_absent_worker_answers_pings_only_under_liveness_only_policy() {
    for phase in [
        Phase::WaitAttached,
        Phase::DrainWait,
        Phase::ImportWait,
        Phase::ResumeWait,
        Phase::RefuseWait,
        Phase::AdoptWait,
    ] {
        let mut c = AnchorConn::zero();
        c.phase = phase;
        assert_eq!(ping_action(&c, true), PingAction::Answer, "{phase:?}");
        assert_eq!(ping_action(&c, false), PingAction::Defer, "{phase:?}");
    }
}

/// PUBLISH, SUBSCRIBE, UNSUBSCRIBE and AUTH are never the anchor's to
/// acknowledge; a PINGREQ is not application work.
#[test]
fn application_work_is_classified() {
    for pkt in [MQTT_PUBLISH, MQTT_SUBSCRIBE, MQTT_UNSUBSCRIBE, MQTT_AUTH] {
        assert!(is_application_work(&env(1, pkt, 0, b"x")), "pkt {pkt}");
    }
    assert!(!is_application_work(&env(1, MQTT_PINGREQ, 0, b"")));
}

#[test]
fn keepalive_is_one_and_a_half_periods() {
    let mut c = AnchorConn::zero();
    c.keepalive_s = 10;
    c.last_in_ms = 1_000;
    assert!(!keepalive_expired(&c, 16_000));
    assert!(keepalive_expired(&c, 16_001));
    c.keepalive_s = 0;
    assert!(!keepalive_expired(&c, u64::MAX), "0 = no keep-alive");
}

// ── Replay retention ────────────────────────────────────────────────────────

#[test]
fn only_work_whose_loss_the_client_would_see_is_retained_for_replay() {
    assert!(needs_replay(&env(1, MQTT_PUBLISH, 0x02, b"q1")));
    assert!(needs_replay(&env(1, MQTT_PUBLISH, 0x04, b"q2")));
    assert!(
        !needs_replay(&env(1, MQTT_PUBLISH, 0x00, b"q0")),
        "at-most-once"
    );
    assert!(needs_replay(&env(1, MQTT_SUBSCRIBE, 0x02, b"s")));
    assert!(needs_replay(&env(1, 6, 0x02, b"\x00\x01")), "PUBREL");
    assert!(!needs_replay(&env(1, MQTT_PINGREQ, 0, b"")));
    assert!(!needs_replay(&env(1, MQTT_CONNECT, 0, b"")));
    let mut kafka = env(1, 0, 0, b"k");
    kafka[2] = 1;
    assert!(!needs_replay(&kafka), "not an MQTT envelope");
}

// ── Backpressure ────────────────────────────────────────────────────────────

/// Reading stops before the hold is full, so overflow is never the
/// first signal the client's transport gets.
#[test]
fn reads_pause_at_the_high_water_mark_before_the_hold_fills() {
    assert!(!read_pause(74, 100, 75));
    assert!(read_pause(75, 100, 75));
    assert!(read_pause(100, 100, 75));
    assert!(!read_pause(100, 100, 0), "0 disables it");
}

// ── The framed FIFO ─────────────────────────────────────────────────────────

fn ids(buf: &[u8], len: usize) -> Vec<(u16, u64)> {
    let mut at = 0;
    let mut v = Vec::new();
    while let Some(e) = fifo_entry(buf, len, at) {
        v.push((e.conn, e.delivery_id));
        at = e.next;
    }
    v
}

#[test]
fn the_fifo_keeps_order_and_ids_and_refuses_what_does_not_fit() {
    let mut buf = [0u8; 64];
    let mut len = 0usize;
    assert!(fifo_push(&mut buf, &mut len, 64, 1, 0x98, 10, b"aaaa"));
    assert!(fifo_push(&mut buf, &mut len, 64, 2, 0x98, 20, b"bbbbbb"));
    assert_eq!(ids(&buf, len), vec![(1, 10), (2, 20)]);
    let before = len;
    assert!(!fifo_push(&mut buf, &mut len, 64, 3, 0x98, 30, &[0u8; 40]));
    assert_eq!(len, before, "a refused push writes nothing");
    let e = fifo_entry(&buf, len, 0).unwrap();
    assert_eq!(&buf[e.payload_at..e.payload_at + e.payload_len], b"aaaa");
}

/// A stalled release leaves the stalled entry AND everything after it:
/// releasing a later frame first would reorder the session.
#[test]
fn a_stall_keeps_everything_after_it_in_order() {
    let mut buf = [0u8; 256];
    let mut len = 0usize;
    for (conn, id) in [(1u16, 1u64), (2, 1), (1, 2), (2, 2)] {
        assert!(fifo_push(&mut buf, &mut len, 256, conn, 0x98, id, b"p"));
    }
    let released = fifo_drain(&mut buf, &mut len, |e, _| {
        if e.conn == 1 && e.delivery_id == 2 {
            Visit::Stop
        } else if e.conn == 1 {
            Visit::Release
        } else {
            Visit::Keep
        }
    });
    assert_eq!(released, 1);
    assert_eq!(ids(&buf, len), vec![(2, 1), (1, 2), (2, 2)]);
}

#[test]
fn trimming_a_connection_up_to_an_id_keeps_the_rest() {
    let mut buf = [0u8; 256];
    let mut len = 0usize;
    for (conn, id) in [(1u16, 1u64), (2, 1), (1, 2), (1, 3), (2, 2)] {
        assert!(fifo_push(&mut buf, &mut len, 256, conn, 0x98, id, b"pp"));
    }
    let n = fifo_remove(&mut buf, &mut len, |c, id| c == 1 && id <= 2);
    assert_eq!(n, 2);
    assert_eq!(ids(&buf, len), vec![(2, 1), (1, 3), (2, 2)]);
    assert_eq!(fifo_count(&buf, len, 1), 1);
    assert_eq!(fifo_count(&buf, len, 2), 2);
}

// ── Operator swap command ───────────────────────────────────────────────────

#[test]
fn a_swap_command_round_trips() {
    let cmd = SwapCmd {
        anchor_id: *b"QANCHOR0",
        mode: SWAP_WORKER_LOST,
        seq: 77,
    };
    let mut b = [0u8; SWAP_CMD_LEN];
    assert_eq!(encode_swap(&mut b, &cmd), SWAP_CMD_LEN);
    assert_eq!(decode_swap(&b), Some(cmd));
    assert!(decode_swap(&b[..SWAP_CMD_LEN - 1]).is_none());
}

fn swap(id: [u8; 8], seq: u64) -> SwapCmd {
    SwapCmd {
        anchor_id: id,
        mode: SWAP_PLANNED,
        seq,
    }
}

/// The same committed command forwarded by both workers executes once.
#[test]
fn a_swap_command_executes_once_and_only_for_its_anchor() {
    let me = *b"QANCHOR0";
    let mut seen = SwapSeqs::new();
    assert!(seen.admit(&swap(me, swap_seq(0, 5)), &me));
    assert!(
        !seen.admit(&swap(me, swap_seq(0, 5)), &me),
        "the second copy"
    );
    assert!(
        !seen.admit(&swap(me, swap_seq(0, 4)), &me),
        "an older record"
    );
    assert!(
        !seen.admit(&swap(*b"QANCHOR1", swap_seq(0, 9)), &me),
        "another anchor's"
    );
    assert!(
        seen.admit(&swap([0; 8], swap_seq(0, 9)), &me),
        "all anchors"
    );
    assert!(seen.admit(&swap(me, 0), &me), "unsequenced");
    assert!(seen.admit(&swap(me, 0), &me), "unsequenced, again");
}

/// Raft indices order records within a partition only: a later command
/// on a partition whose indices sort below another's still executes.
#[test]
fn swap_sequences_are_ordered_per_partition() {
    let me = *b"QANCHOR0";
    let mut seen = SwapSeqs::new();
    assert!(seen.admit(&swap(me, swap_seq(1, 900)), &me));
    assert!(
        seen.admit(&swap(me, swap_seq(0, 3)), &me),
        "partition 0's first record, committed after partition 1's"
    );
    assert!(seen.admit(&swap(me, swap_seq(0, 4)), &me));
    assert!(!seen.admit(&swap(me, swap_seq(0, 4)), &me));
    assert!(!seen.admit(&swap(me, swap_seq(1, 900)), &me));
    assert!(seen.admit(&swap(me, swap_seq(1, 901)), &me));
}

#[test]
fn a_swap_sequence_names_its_partition_and_index() {
    assert_eq!(swap_seq(3, 7), (3u64 << 48) | 7);
    assert_eq!(swap_seq(0, u64::MAX) >> 48, 0, "the index cannot spill");
}

// ── Worker loss ─────────────────────────────────────────────────────────────

#[test]
fn a_worker_is_lost_only_past_the_declared_window() {
    assert!(!worker_lost(1_000, 2_000, 100, 1_000));
    assert!(worker_lost(1_000, 2_001, 100, 1_000));
    assert!(
        !worker_lost(0, u64::MAX, 100, 0),
        "no window disables detection"
    );
    assert!(
        !worker_lost(0, u64::MAX, 0, 1_000),
        "without probes, silence says nothing"
    );
}

fn bound(worker: u8, phase: Phase) -> AnchorConn {
    let mut c = AnchorConn::zero();
    c.worker = worker;
    c.phase = phase;
    c
}

/// Only a session with an acknowledged checkpoint and no replay gap is
/// adopted; the rest are closed honestly rather than resumed inexactly.
#[test]
fn a_loss_adopts_only_what_it_can_resume_exactly() {
    let mut c = bound(0, Phase::Active);
    assert_eq!(loss_action(&c, 0, true), LossAction::Close, "no checkpoint");
    c.checkpoint_acked(10);
    assert_eq!(loss_action(&c, 0, true), LossAction::Adopt);
    assert_eq!(loss_action(&c, 0, false), LossAction::Close, "no standby");
    c.note_replay_gap(12);
    assert_eq!(
        loss_action(&c, 0, true),
        LossAction::Close,
        "a frame not retained"
    );
    c.checkpoint_acked(12);
    assert_eq!(
        loss_action(&c, 0, true),
        LossAction::Adopt,
        "the gap is covered"
    );
    assert_eq!(c.replay_gap_hw, 0);
    assert_eq!(
        loss_action(&c, 1, true),
        LossAction::Unaffected,
        "the other worker"
    );
}

#[test]
fn a_handoff_toward_the_lost_worker_is_refused() {
    for phase in [Phase::DrainWait, Phase::ImportWait, Phase::ResumeWait] {
        assert_eq!(
            loss_action(&bound(0, phase), 1, true),
            LossAction::RefuseHandoff
        );
    }
    assert_eq!(
        loss_action(&bound(0, Phase::Closing), 0, true),
        LossAction::Unaffected
    );
}

/// A checkpoint covers a gap only when it reaches the last frame that
/// could not be retained: one between two gaps leaves the later frame
/// missing from both the checkpoint and the replay window.
#[test]
fn a_checkpoint_closes_a_gap_only_past_every_missing_frame() {
    let mut c = bound(0, Phase::Active);
    c.checkpoint_acked(4);
    c.note_replay_gap(5);
    c.note_replay_gap(9);
    c.checkpoint_acked(6);
    assert_eq!(
        loss_action(&c, 0, true),
        LossAction::Close,
        "frame 9 is in neither"
    );
    c.checkpoint_acked(9);
    assert_eq!(loss_action(&c, 0, true), LossAction::Adopt);
}

/// A planned move invalidates the standby's checkpoint: it is of the
/// worker the session left.
#[test]
fn a_planned_move_resets_the_checkpoint() {
    let mut c = bound(0, Phase::Active);
    c.checkpoint_acked(9);
    c.checkpoint_reset();
    assert!(!c.has(FLAG_CKPT_VALID));
    assert_eq!(loss_action(&c, 0, true), LossAction::Close);
}

/// An older acknowledgement arriving late cannot move the checkpoint back.
#[test]
fn a_late_checkpoint_ack_does_not_rewind() {
    let mut c = AnchorConn::zero();
    c.checkpoint_acked(20);
    c.checkpoint_acked(15);
    assert_eq!(c.checkpoint_delivery, 20);
}
