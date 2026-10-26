//! The worker's side of anchor continuity as `session_processor` holds it
//! (`production_seat_core.rs`): a session attached as transport-migratable
//! takes a seat, its responses become numbered productions retained until
//! the anchor's horizon covers them, a replacement anchor is answered
//! from the seat, and what it retains above the replacement's horizon is
//! offered again under the original ids — driven against the anchor's own
//! gate (`ProductionGate`) and `AnchorReanchor`, so both halves of the
//! exchange are the real ones.
//!
//! The failures prevented: a response the standby never held counted as
//! given (released before the horizon); a response emitted twice after a
//! takeover; a production the worker could not re-offer silently skipped
//! by a replacement; a full ring evicting rather than holding back; an
//! oversize response stalling the stream forever; an id spent on a
//! production the anchor never received; a stale seat on a recycled
//! connection id answering for the new session; a horizon report that
//! would place a replacement's re-offers at the wrong bytes.

use super::anchor_continuity::*;

const SID: [u8; 16] = [0x5A; 16];
const OTHER: [u8; 16] = [0x6B; 16];
const A2: [u8; 8] = *b"ANCHOR02";

/// A response envelope `[conn:2][proto][type][flags][body]` of `len` body
/// bytes.
fn response(conn: u16, tag: u8, len: usize) -> Vec<u8> {
    let mut v = conn.to_le_bytes().to_vec();
    v.extend_from_slice(&[0, 4, 0]);
    v.extend(std::iter::repeat(tag).take(len));
    v
}

fn produce(seats: &mut ProductionSeats, conn: u16, inner: &[u8]) -> (u64, Vec<u8>) {
    let mut out = vec![0u8; inner.len() + PRODUCTION_INSERT + 8];
    match seats.produce(conn, 0xC1, inner, &mut out) {
        Produced::Wrapped { id, len } => (id, out[..len].to_vec()),
        other => panic!("not wrapped: {other:?}"),
    }
}

/// A horizon report through production `through`, each test production
/// taken as ten bytes of output.
fn horizon(sid: [u8; 16], epoch: u32, through: u64) -> Vec<u8> {
    let mut b = vec![0u8; HORIZON_LEN];
    HorizonReport {
        session_id: sid,
        epoch,
        accepted_through: through,
        bytes_through: through * 10,
    }
    .encode(&mut b);
    b
}

#[test]
fn an_unseated_session_answers_unnumbered() {
    let mut seats = ProductionSeats::new();
    let mut out = [0u8; 64];
    assert_eq!(
        seats.produce(7, 0xC1, &response(7, 1, 4), &mut out),
        Produced::Unseated
    );
}

#[test]
fn productions_are_numbered_wrapped_and_released_by_the_horizon() {
    let mut seats = ProductionSeats::new();
    assert!(seats.open(7, &SID, 1));
    let r1 = response(7, 1, 4);
    let (id1, env1) = produce(&mut seats, 7, &r1);
    let (id2, _) = produce(&mut seats, 7, &response(7, 2, 4));
    assert_eq!((id1, id2), (1, 2));
    let h = decode_production(&env1).unwrap();
    assert_eq!(
        (h.conn, h.session_id, h.epoch, h.production_id),
        (7, SID, 1, 1)
    );
    assert_eq!(production_body(&env1), &r1[2..]);
    // Another session's, another epoch's and a stale anchor's reports
    // change nothing.
    assert_eq!(seats.horizon(&horizon(OTHER, 1, 2)), 0);
    assert_eq!(seats.horizon(&horizon(SID, 2, 2)), 0);
    assert_eq!(seats.horizon(&[1, 2, 3]), 0);
    assert_eq!(seats.stale_reports, 3);
    assert_eq!(seats.horizon(&horizon(SID, 1, 1)), 1);
    let s = &seats.seats[seats.seat_of(7).unwrap()];
    assert_eq!((s.ring.released(), s.ring.len()), (1, 1));
}

#[test]
fn a_full_ring_holds_back_and_never_evicts() {
    let mut seats = ProductionSeats::new();
    seats.open(3, &SID, 1);
    for k in 0..SEAT_RING {
        produce(
            &mut seats,
            3,
            &response(3, u8::try_from(k).expect("fits"), 8),
        );
    }
    let mut out = [0u8; 64];
    assert_eq!(
        seats.produce(3, 0xC1, &response(3, 9, 8), &mut out),
        Produced::Full
    );
    assert_eq!(seats.full, 1);
    // The horizon frees room; the held-back response is numbered next.
    seats.horizon(&horizon(SID, 1, 4));
    let (id, _) = produce(&mut seats, 3, &response(3, 9, 8));
    assert_eq!(id, SEAT_RING as u64 + 1);
}

#[test]
fn a_full_seat_table_refuses_the_attachment() {
    let mut seats = ProductionSeats::new();
    for c in 0..u16::try_from(SEAT_COUNT).expect("fits") {
        let mut sid = [0u8; 16];
        sid[..2].copy_from_slice(&c.to_le_bytes());
        assert!(seats.open(c, &sid, 1));
    }
    assert!(!seats.open(999, &SID, 1));
    assert_eq!(seats.refused, 1);
    // A new attachment of a seated session replaces its seat.
    let mut sid0 = [0u8; 16];
    sid0[..2].copy_from_slice(&0u16.to_le_bytes());
    assert!(seats.open(42, &sid0, 2));
    seats.close(42);
    assert!(seats.open(999, &SID, 1));
}

#[test]
fn an_oversize_response_is_numbered_but_never_reoffered() {
    let mut seats = ProductionSeats::new();
    seats.open(5, &SID, 1);
    produce(&mut seats, 5, &response(5, 1, 8));
    let big = response(5, 2, RETAIN_BYTES_MAX + 10);
    let (id, env) = produce(&mut seats, 5, &big);
    assert_eq!(id, 2);
    assert_eq!(production_body(&env).len(), big.len() - 2);
    produce(&mut seats, 5, &response(5, 3, 8));
    assert_eq!(seats.unretained, 1);
    let ring = &seats.seats[seats.seat_of(5).unwrap()].ring;
    // A replacement must hold everything through the oversize one.
    assert_eq!(ring.first_offerable(), 3);
    // Released through 1 only: production 2 can be neither placed nor
    // offered again, so a replacement is refused.
    seats.horizon(&horizon(SID, 1, 1));
    let req = Reanchor {
        session_id: SID,
        new_epoch: 2,
        anchor_id: A2,
        next_delivery: 5,
        bytes_held: 30,
    };
    assert_eq!(seats.reanchor(9, &req, 4).status, AC_CURSOR_MISMATCH);
    // Once a horizon released it, the replacement re-anchors and
    // production 3 is re-offered.
    seats.horizon(&horizon(SID, 1, 2));
    assert_eq!(seats.reanchor(9, &req, 4).status, AC_OK);
    assert_eq!(seats.next_reoffer(9).map(|(id, _)| id), Some(3));
    seats.reoffered(9, 3);
    assert!(seats.next_reoffer(9).is_none());
    assert!(!seats.reoffer_owed(9));
}

/// A crash: the old anchor's horizon released production 1 (ten bytes),
/// and the standby holds 15 bytes — half of production 2 — when the
/// primary dies. The replacement imports with 15 bytes held, the worker
/// accepts it and re-offers 2 and 3 under their ids; the replacement
/// writes only the rest of 2, then 3, and a re-offer of 1 is answered
/// without a send.
#[test]
fn a_crash_takeover_reoffers_exactly_what_the_standby_lacks() {
    let mut seats = ProductionSeats::new();
    seats.open(7, &SID, 1);
    let r: Vec<Vec<u8>> = (1..=3).map(|k| response(7, k, 5)).collect();
    for x in &r {
        produce(&mut seats, 7, x);
    }
    seats.horizon(&horizon(SID, 1, 1));
    let mut rec = AnchorRecord::zero();
    rec.session_id = SID;
    rec.epoch = 1;
    rec.next_delivery = 5;
    rec.next_production = 4;
    rec.production_accepted = 1;
    let mut imp = anchor_import(
        &rec,
        &TransportCursors {
            next_delivery: 5,
            delivery_acked: 4,
            out_bytes: 15,
        },
    )
    .expect("imports");
    let req = imp.request(&A2);
    let ans = seats.reanchor(11, &req, 4);
    assert_eq!(ans.status, AC_OK);
    assert_eq!((ans.released, ans.released_bytes), (1, 10));
    assert_eq!(imp.apply(&ans), AC_OK);
    let i = seats.seat_of(11).expect("rebound");
    assert_eq!(seats.seats[i].epoch, 2);
    // Re-offers: 2 then 3, under their ids, byte for byte; each is ten
    // bytes of output.
    let mut gate = imp.gate;
    let mut hz = imp.horizon;
    let mut written = Vec::new();
    // New productions wait behind the re-offers.
    let mut out = [0u8; 64];
    assert_eq!(
        seats.produce(11, 0xC1, &response(11, 9, 4), &mut out),
        Produced::Full
    );
    while let Some((id, inner)) = seats.next_reoffer(11) {
        assert_eq!(inner, &r[usize::try_from(id - 1).expect("fits")][..]);
        let skip = hz.held_prefix(10);
        assert_ne!(gate.offer(&mut hz, id, 10), Offer::Gap);
        written.push((id, 10 - skip));
        seats.reoffered(11, id);
    }
    assert!(!seats.reoffer_owed(11));
    assert_eq!(written, vec![(2, 5), (3, 10)], "the rest of 2, all of 3");
    // A re-offer of 1 (already the standby's) is accepted, not written.
    let before = hz.written;
    assert_eq!(gate.offer(&mut hz, 1, 0), Offer::Accepted);
    assert_eq!(hz.written, before);
    // The transport's horizon reaches the end; the worker releases all.
    hz.report(30);
    assert_eq!(gate.observe(&hz), Some(3));
    assert_eq!(seats.horizon(&horizon(SID, 2, 3)), 2);
}

#[test]
fn a_reanchor_for_a_session_never_seated_is_refused() {
    let mut seats = ProductionSeats::new();
    let req = Reanchor {
        session_id: SID,
        new_epoch: 2,
        anchor_id: A2,
        next_delivery: 1,
        bytes_held: 0,
    };
    assert_eq!(seats.reanchor(1, &req, 0).status, AC_UNKNOWN_SESSION);
    assert_eq!(seats.reanchor_refused, 1);
}

#[test]
fn a_production_the_channel_refused_gives_its_id_back() {
    let mut seats = ProductionSeats::new();
    seats.open(7, &SID, 1);
    let (id1, _) = produce(&mut seats, 7, &response(7, 1, 4));
    let (id2, _) = produce(&mut seats, 7, &response(7, 2, 4));
    // Only the last one numbered can be taken back.
    assert!(!seats.unproduce(7, id1));
    assert!(seats.unproduce(7, id2));
    let (again, _) = produce(&mut seats, 7, &response(7, 2, 4));
    assert_eq!(again, id2, "the anchor never sees a gap");
}

#[test]
fn a_malformed_response_takes_no_id() {
    let mut seats = ProductionSeats::new();
    seats.open(7, &SID, 1);
    let mut out = [0u8; 64];
    assert_eq!(seats.produce(7, 0xC1, &[7], &mut out), Produced::Malformed);
    let mut short = [0u8; 4];
    assert_eq!(
        seats.produce(7, 0xC1, &response(7, 1, 4), &mut short),
        Produced::Malformed
    );
    assert_eq!(produce(&mut seats, 7, &response(7, 1, 4)).0, 1);
}

#[test]
fn a_new_attachment_on_a_recycled_conn_starts_from_production_one() {
    let mut seats = ProductionSeats::new();
    seats.open(7, &SID, 1);
    produce(&mut seats, 7, &response(7, 1, 4));
    produce(&mut seats, 7, &response(7, 2, 4));
    // The connection id is reused by another session: the stale seat goes,
    // and the new session numbers from 1 under its own id.
    assert!(seats.open(7, &OTHER, 1));
    let (id, env) = produce(&mut seats, 7, &response(7, 3, 4));
    assert_eq!(id, 1);
    assert_eq!(decode_production(&env).unwrap().session_id, OTHER);
    assert!(seats.seat_of_session(&SID).is_none());
}

#[test]
fn a_horizon_whose_bytes_do_not_move_on_is_ignored() {
    let mut seats = ProductionSeats::new();
    seats.open(7, &SID, 1);
    for k in 1..=3 {
        produce(&mut seats, 7, &response(7, k, 4));
    }
    assert_eq!(seats.horizon(&horizon(SID, 1, 1)), 1);
    // Production 2 claims to end at byte 5, before production 1 did.
    let mut b = vec![0u8; HORIZON_LEN];
    HorizonReport {
        session_id: SID,
        epoch: 1,
        accepted_through: 2,
        bytes_through: 5,
    }
    .encode(&mut b);
    assert_eq!(seats.horizon(&b), 0);
    let s = &seats.seats[seats.seat_of(7).unwrap()];
    assert_eq!((s.ring.released(), s.released_bytes), (1, 10));
    // A report past what was produced is not this session's either.
    assert_eq!(seats.horizon(&horizon(SID, 1, 9)), 0);
}

/// A session moves to another worker under the same anchor (a planned
/// handoff, or an adoption): the anchor's gate hands over where it
/// stands, the new worker's seat numbers on from there, and the gate
/// takes its productions as the next ones, placed after every byte the
/// old worker's wrote.
#[test]
fn a_session_moved_to_another_worker_numbers_on_from_the_anchors_gate() {
    let mut a = ProductionSeats::new();
    a.open(7, &SID, 1);
    let mut gate = ProductionGate::new();
    let mut hz = TransportHorizon::new();
    for k in 1..=3 {
        let (id, _) = produce(&mut a, 7, &response(7, k, 5));
        assert_eq!(gate.offer(&mut hz, id, 10), Offer::Pending);
    }
    // Not settled until the horizon covers every production.
    assert!(!a.settled(7));
    hz.report(30);
    gate.observe(&hz);
    a.horizon(&horizon(SID, 1, 3));
    assert!(a.settled(7));
    // The new worker seats where the gate stands.
    let mut b = ProductionSeats::new();
    assert!(b.can_open(7, &SID));
    assert!(b.open_resumed(7, &SID, 2, gate.next_submit, hz.written));
    let (id, _) = produce(&mut b, 7, &response(7, 4, 5));
    assert_eq!(id, 4);
    assert_eq!(gate.offer(&mut hz, id, 10), Offer::Pending);
    assert_eq!(hz.held_prefix(10), 0, "placed after the old worker's bytes");
    hz.report(40);
    gate.observe(&hz);
    let mut rep = vec![0u8; HORIZON_LEN];
    HorizonReport {
        session_id: SID,
        epoch: 2,
        accepted_through: 4,
        bytes_through: 40,
    }
    .encode(&mut rep);
    assert_eq!(b.horizon(&rep), 1);
    assert!(b.settled(7));
}

/// A replacement anchor that asks again — its first answer lost on the
/// way — is answered again with the same cursors, and the re-offers start
/// over; nothing on the seat moves a second time.
#[test]
fn a_repeated_reanchor_is_answered_again() {
    let mut seats = ProductionSeats::new();
    seats.open(7, &SID, 1);
    for k in 1..=3 {
        produce(&mut seats, 7, &response(7, k, 5));
    }
    seats.horizon(&horizon(SID, 1, 1));
    let req = Reanchor {
        session_id: SID,
        new_epoch: 2,
        anchor_id: A2,
        next_delivery: 5,
        bytes_held: 15,
    };
    let first = seats.reanchor(11, &req, 4);
    assert_eq!(first.status, AC_OK);
    while let Some((id, _)) = seats.next_reoffer(11) {
        seats.reoffered(11, id);
    }
    let again = seats.reanchor(11, &req, 4);
    assert_eq!(again, first);
    assert_eq!(seats.next_reoffer(11).map(|(id, _)| id), Some(2));
    // Another anchor at that epoch is stale.
    let other = Reanchor {
        anchor_id: *b"ANCHOR03",
        ..req
    };
    assert_eq!(seats.reanchor(11, &other, 4).status, AC_STALE_EPOCH);
}
