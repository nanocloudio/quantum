//! Session replay: the handoff tail a record carries, the rule an
//! importer decides a record's cursor by, and the outbound delivery
//! ledger the log rebuilds.
//!
//! The failures prevented are an imported session that applies an entry
//! twice (its record already contained it), one that silently misses an
//! entry (the importer skipped it while the session was not its own),
//! and a `QoS` 1/2 delivery a crashed worker owed that nobody remembers.

use super::session_replay::*;

fn tail(hw: u64, cursors: &[(u16, u64)]) -> HandoffTail {
    let mut t = HandoffTail::empty();
    t.delivery_hw = hw;
    for &c in cursors {
        t.cursors[t.count as usize] = c;
        t.count += 1;
    }
    t
}

// ── The tail ────────────────────────────────────────────────────────────────

#[test]
fn a_tail_round_trips() {
    let t = tail(42, &[(0, 17), (3, 900)]);
    let mut b = [0u8; TAIL_MAX];
    let n = t.encode(&mut b);
    assert_eq!(n, TAIL_FIXED + 2 * TAIL_CURSOR);
    assert_eq!(HandoffTail::decode(&b[..n]), Some(t));
    assert_eq!(t.cursor(3), 900);
    assert_eq!(t.cursor(1), 0, "a partition the exporter never applied");
}

#[test]
fn a_tail_is_built_from_the_dense_apply_cursor() {
    let mut apply = [0u64; 8];
    apply[0] = 10;
    apply[5] = 7;
    let t = HandoffTail::from_apply(3, &apply);
    assert_eq!(t.count, 2);
    assert_eq!(t.cursor(0), 10);
    assert_eq!(t.cursor(5), 7);
    assert_eq!(t.delivery_hw, 3);
}

#[test]
fn a_short_or_oversized_tail_is_refused() {
    assert!(HandoffTail::decode(&[0u8; TAIL_FIXED - 1]).is_none());
    let mut b = [0u8; TAIL_FIXED];
    b[8] = 1; // one cursor promised, none present
    assert!(HandoffTail::decode(&b).is_none());
    b[8] = u8::try_from(TAIL_MAX_CURSORS + 1).expect("fits a byte");
    assert!(HandoffTail::decode(&b).is_none());
}

// ── The import cursor rule ──────────────────────────────────────────────────

#[test]
fn an_importer_level_with_the_record_admits_it() {
    let t = tail(0, &[(0, 50)]);
    assert_eq!(cursor_verdict(&t, &[50], None), CursorVerdict::Admit);
    assert_eq!(
        cursor_verdict(&t, &[60], Some((0, 40))),
        CursorVerdict::Admit
    );
}

/// The importer passed an entry of the session the record does not
/// contain: it will never see it again, so the import is refused.
#[test]
fn a_record_behind_a_skipped_entry_is_a_cursor_mismatch() {
    let t = tail(0, &[(0, 50)]);
    assert_eq!(
        cursor_verdict(&t, &[60], Some((0, 55))),
        CursorVerdict::Mismatch
    );
    // A skip on a partition the record never applied is past its cursor.
    assert_eq!(
        cursor_verdict(&t, &[60, 9], Some((1, 9))),
        CursorVerdict::Mismatch
    );
}

/// An importer behind the record must not apply what the record already
/// contains: it is admitted with a floor, and the floor filters exactly
/// the entries at or below the record's cursor.
#[test]
fn an_importer_behind_the_record_replays_from_its_cursor() {
    let t = tail(0, &[(0, 50), (2, 8)]);
    assert_eq!(
        cursor_verdict(&t, &[48, 0, 8], None),
        CursorVerdict::AdmitWithFloor
    );
    assert!(below_floor(&t, 0, 49));
    assert!(below_floor(&t, 0, 50));
    assert!(
        !below_floor(&t, 0, 51),
        "the first entry past the record applies"
    );
    assert!(!floor_passed(&t, &[49, 0, 8]));
    assert!(floor_passed(&t, &[50, 0, 8]));
}

// ── QOP_DELIVER / QOP_DELIVER_ACK ───────────────────────────────────────────

#[test]
fn deliver_and_ack_bodies_round_trip() {
    let deliver = Deliver {
        stream_hash: 0xABCD,
        generation: 3,
        packet_id: 0x0102,
        qos: 2,
        body: b"\x00\x05topic\x01\x02payload",
    };
    let mut buf = [0u8; 64];
    let n = encode_deliver(&mut buf, &deliver);
    assert_eq!(n, DELIVER_FIXED + deliver.body.len());
    assert_eq!(decode_deliver(&buf[..n]), Some(deliver));
    let deliver_ack = DeliverAck {
        stream_hash: 0xABCD,
        generation: 3,
        packet_id: 0x0102,
        stage: ACK_RELEASED,
    };
    let n = encode_deliver_ack(&mut buf, &deliver_ack);
    assert_eq!(n, DELIVER_ACK_LEN);
    assert_eq!(decode_deliver_ack(&buf[..n]), Some(deliver_ack));
    assert!(decode_deliver_ack(&buf[..n - 1]).is_none(), "short");
    buf[14] = 0;
    assert!(decode_deliver_ack(&buf[..n]).is_none(), "not a stage");
}

#[test]
fn a_body_past_the_ledger_bound_is_not_logged() {
    let big = vec![0u8; DELIVER_BODY_MAX + 1];
    let d = Deliver {
        stream_hash: 1,
        generation: 1,
        packet_id: 1,
        qos: 1,
        body: &big,
    };
    let mut b = vec![0u8; DELIVER_FIXED + big.len()];
    assert_eq!(encode_deliver(&mut b, &d), 0);
}

// ── The ledger ──────────────────────────────────────────────────────────────

fn deliver(stream: u64, gen: u32, pid: u16, body: &[u8]) -> Deliver<'_> {
    Deliver {
        stream_hash: stream,
        generation: gen,
        packet_id: pid,
        qos: 1,
        body,
    }
}

fn ack(stream: u64, gen: u32, pid: u16, stage: u8) -> DeliverAck {
    DeliverAck {
        stream_hash: stream,
        generation: gen,
        packet_id: pid,
        stage,
    }
}

/// Applying the log alone rebuilds what a session is owed: two
/// deliveries, one acknowledged, leaves exactly the other.
#[test]
fn the_log_rebuilds_exactly_the_owed_deliveries() {
    let mut l = Box::new(DeliveryLedger::new());
    assert!(l.record(0, &deliver(7, 1, 1, b"one")));
    assert!(l.record(0, &deliver(7, 1, 2, b"two")));
    assert!(l.record(0, &deliver(8, 1, 1, b"other session")));
    assert!(l.ack(0, &ack(7, 1, 1, ACK_COMPLETE)));
    let mut out = [0usize; 8];
    let n = l.owed(0, 7, &mut out);
    assert_eq!(n, 1);
    let e = &l.entries[out[0]];
    assert_eq!((e.packet_id, e.body()), (2, &b"two"[..]));
    assert_eq!(l.owed(0, 8, &mut out), 1);
}

/// A replayed entry, or a packet id reused after its ack applied,
/// replaces rather than duplicates.
#[test]
fn a_replayed_delivery_replaces_rather_than_duplicates() {
    let mut l = Box::new(DeliveryLedger::new());
    l.record(0, &deliver(7, 1, 5, b"a"));
    l.record(0, &deliver(7, 1, 5, b"a"));
    assert_eq!(l.live(), 1);
    l.record(0, &deliver(7, 2, 5, b"b"));
    assert_eq!(l.live(), 1);
    let mut out = [0usize; 2];
    l.owed(0, 7, &mut out);
    assert_eq!(l.entries[out[0]].generation, 2);
}

/// A delivery owed across a resume is redelivered under the same packet
/// id, so the client's ack carries the resumed generation and completes
/// it; an ack can never complete a NEWER delivery that reused the id.
#[test]
fn an_ack_completes_its_generation_or_older_never_newer() {
    let mut l = Box::new(DeliveryLedger::new());
    l.record(0, &deliver(7, 2, 5, b"a"));
    assert!(
        !l.ack(0, &ack(7, 1, 5, ACK_COMPLETE)),
        "an older ack names an earlier delivery"
    );
    assert_eq!(l.live(), 1);
    assert!(
        !l.ack(1, &ack(7, 2, 5, ACK_COMPLETE)),
        "another tenant's session"
    );
    assert!(
        l.ack(0, &ack(7, 3, 5, ACK_COMPLETE)),
        "the resumed generation's ack completes it"
    );
    assert_eq!(l.live(), 0);
}

/// MQTT resends what is owed in the order it was first sent; the table
/// slots the ledger happens to use say nothing about that order.
#[test]
fn what_is_owed_comes_back_in_the_order_it_was_sent() {
    let mut l = Box::new(DeliveryLedger::new());
    l.record(0, &deliver(7, 1, 1, b"a"));
    l.record(0, &deliver(7, 1, 2, b"b"));
    l.record(0, &deliver(7, 1, 3, b"c"));
    // Free the first slot, then log a later delivery into it.
    assert!(l.ack(0, &ack(7, 1, 1, ACK_COMPLETE)));
    l.record(0, &deliver(7, 1, 4, b"d"));
    let mut out = [0usize; 8];
    let n = l.owed(0, 7, &mut out);
    let pids: Vec<u16> = out[..n].iter().map(|&i| l.entries[i].packet_id).collect();
    assert_eq!(pids, vec![2, 3, 4]);
    let mut two = [0usize; 2];
    let n = l.owed(0, 7, &mut two);
    let pids: Vec<u16> = two[..n].iter().map(|&i| l.entries[i].packet_id).collect();
    assert_eq!(pids, vec![2, 3], "a short list keeps the oldest");
}

/// A `QoS 2` delivery whose PUBREC arrived is owed its PUBREL, not the
/// PUBLISH again; PUBCOMP then ends it. A `QoS 1` delivery has no such
/// stage.
#[test]
fn a_released_qos2_delivery_is_owed_its_pubrel() {
    let mut l = Box::new(DeliveryLedger::new());
    let mut d = deliver(7, 1, 5, b"m");
    d.qos = 2;
    l.record(0, &d);
    assert!(l.ack(0, &ack(7, 1, 5, ACK_RELEASED)));
    let mut out = [0usize; 1];
    assert_eq!(l.owed(0, 7, &mut out), 1, "still owed");
    assert_eq!(l.entries[out[0]].released, 1);
    assert!(l.ack(0, &ack(7, 1, 5, ACK_COMPLETE)));
    assert_eq!(l.live(), 0);

    l.record(0, &deliver(7, 1, 6, b"q1"));
    assert!(
        !l.ack(0, &ack(7, 1, 6, ACK_RELEASED)),
        "QoS 1 is never released"
    );
    assert_eq!(l.owed(0, 7, &mut out), 1);
    assert_eq!(l.entries[out[0]].released, 0);
}

#[test]
fn a_clean_start_forgets_what_the_session_was_owed() {
    let mut l = Box::new(DeliveryLedger::new());
    l.record(0, &deliver(7, 1, 1, b"a"));
    l.record(0, &deliver(7, 1, 2, b"b"));
    l.record(0, &deliver(9, 1, 1, b"c"));
    assert_eq!(l.drop_stream(0, 7), 2);
    assert_eq!(l.live(), 1);
}

/// A full ledger refuses and counts, never evicts an owed delivery.
#[test]
fn a_full_ledger_refuses_and_counts() {
    let mut l = Box::new(DeliveryLedger::new());
    for i in 0..u16::try_from(LEDGER_SLOTS).expect("fits a packet id") {
        assert!(l.record(0, &deliver(1, 1, i, b"x")));
    }
    assert!(!l.record(0, &deliver(1, 1, 60_000, b"y")));
    assert_eq!(l.refused, 1);
    assert_eq!(l.live(), LEDGER_SLOTS);
}
