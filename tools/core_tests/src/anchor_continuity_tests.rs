//! Anchor continuity: the ingress host under a session is replaced while
//! the worker stays — the production envelope, the output-horizon gate,
//! the worker's retained productions, the anchor's `QAC1` record and the
//! re-anchor exchange.
//!
//! The failures prevented are a frame the worker consumed delivered to
//! it again by the replacement anchor, a response counted as given before
//! the standby held it (lost with the primary host), a response emitted
//! twice, and a replacement that silently resumes a session it does not
//! have the state for.

use super::anchor_continuity::*;

const SID: [u8; 16] = [0x5A; 16];
const A1: [u8; 8] = *b"ANCHOR01";
const A2: [u8; 8] = *b"ANCHOR02";

fn len(b: &[u8]) -> u64 {
    u64::try_from(b.len()).expect("fits")
}

fn conn(epoch: u32) -> AnchorConn {
    let mut c = AnchorConn::zero();
    c.session_id = SID;
    c.epoch = epoch;
    c.phase = Phase::Active;
    c.mqtt_version = 5;
    c.keepalive_s = 30;
    c.tenant = 7;
    c.listener = 2;
    c.client_hash = fnv1a_64(b"client-1");
    c.set(FLAG_CONNECT_ADMITTED, true);
    c
}

// ── The production envelope ─────────────────────────────────────────────────

#[test]
fn a_production_envelope_round_trips_and_is_never_truncated() {
    let inner = [9u8, 0, 0, 0x40, 0, 2, 0, 1];
    let mut out = [0u8; 64];
    let n = encode_production(&mut out, 0x99, &inner, &SID, 3, 41);
    assert_eq!(n, inner.len() + PRODUCTION_INSERT);
    let h = decode_production(&out[..n]).expect("decodes");
    assert_eq!(h.conn, 9);
    assert_eq!(h.session_id, SID);
    assert_eq!(h.epoch, 3);
    assert_eq!(h.production_id, 41);
    assert_eq!(h.inner_msg_type, 0x99);
    assert_eq!(production_body(&out[..n]), &inner[2..]);
    let mut short = [0u8; 10];
    assert_eq!(encode_production(&mut short, 0x99, &inner, &SID, 3, 41), 0);
    assert_eq!(encode_production(&mut out, 0x99, &[1], &SID, 3, 41), 0);
    assert!(decode_production(&out[..PRODUCTION_INSERT]).is_none());
}

// ── The horizon gate over the transport's byte horizon ──────────────────────

/// RPO zero: a PUBACK is given only once the standby holds its last byte.
#[test]
fn a_production_is_accepted_only_once_the_horizon_reaches_its_last_byte() {
    let mut h = TransportHorizon::new();
    let mut g = ProductionGate::new();
    assert_eq!(g.offer(&mut h, 1, 8), Offer::Pending);
    assert_eq!(g.offer(&mut h, 2, 8), Offer::Pending);
    assert_eq!(h.written, 16);
    assert_eq!(g.pending(), 2);
    assert_eq!(
        g.report(),
        None,
        "nothing to report before the standby holds it"
    );
    assert!(!h.report(7), "one byte short of production 1");
    assert_eq!(g.observe(&h), None);
    assert!(h.report(8));
    assert_eq!(g.observe(&h), Some(1));
    assert_eq!((h.accepted, h.accepted_bytes), (1, 8));
    assert_eq!(g.report(), Some(1));
    assert_eq!(g.report(), None, "a horizon is reported once");
    assert!(!h.report(8), "a repeat");
    assert!(!h.report(3), "backwards");
    assert!(h.report(16));
    assert_eq!(g.observe(&h), Some(2));
    assert_eq!(g.observe(&h), None, "the horizon only moves forward");
    assert_eq!(g.pending(), 0);
}

#[test]
fn a_reoffer_inside_the_horizon_is_accepted_without_resubmitting() {
    let mut h = TransportHorizon::new();
    let mut g = ProductionGate::new();
    g.offer(&mut h, 1, 5);
    h.report(5);
    assert_eq!(g.offer(&mut h, 1, 5), Offer::Accepted);
    assert_eq!(h.written, 5, "written once");
    g.offer(&mut h, 2, 4);
    assert_eq!(g.offer(&mut h, 2, 4), Offer::Pending);
    assert_eq!(h.written, 9);
}

#[test]
fn a_gap_is_refused_and_a_full_horizon_keeps_the_id() {
    let mut h = TransportHorizon::new();
    let mut g = ProductionGate::new();
    assert_eq!(g.offer(&mut h, 2, 1), Offer::Gap);
    assert_eq!(g.offer(&mut h, 0, 1), Offer::Gap);
    for id in 1..=HORIZON_PENDING as u64 {
        assert_eq!(g.offer(&mut h, id, 1), Offer::Pending);
    }
    assert!(!h.ready());
    let next = HORIZON_PENDING as u64 + 1;
    assert_eq!(g.offer(&mut h, next, 1), Offer::NotReady);
    assert_eq!(g.next_submit, next, "the id is not consumed");
    h.report(1);
    assert!(h.ready());
    assert_eq!(g.offer(&mut h, next, 1), Offer::Pending);
}

/// A horizon the transport reports past what the anchor wrote is not
/// this gate's productions: nothing unwritten is accepted.
#[test]
fn the_horizon_never_runs_past_what_was_written() {
    let mut h = TransportHorizon::new();
    let mut g = ProductionGate::new();
    g.offer(&mut h, 1, 4);
    h.report(900);
    assert_eq!(g.observe(&h), Some(1));
    assert_eq!(g.accepted, 1);
    assert_eq!(h.accepted_bytes, 4);
    // The over-report does not stand: the next production is written in
    // full and waits for the horizon to reach it.
    assert_eq!(h.held_prefix(10), 0);
    assert_eq!(g.offer(&mut h, 2, 10), Offer::Pending);
    assert!(h.report(14));
    assert_eq!(g.observe(&h), Some(2));
}

/// A replacement places a re-offered production against what the
/// standby holds: held whole (nothing written), held in part (the rest
/// written), or not at all.
#[test]
fn a_replacement_writes_only_what_the_standby_does_not_hold() {
    // Released through production 2, which ended at byte 10; the standby
    // holds 23 bytes.
    let mut h = TransportHorizon::resumed(23, 2, 10);
    let mut g = ProductionGate::resumed(3);
    assert_eq!(h.held_prefix(6), 6, "production 3 (10..16) is held whole");
    assert_eq!(g.offer(&mut h, 3, 6), Offer::Accepted);
    assert_eq!(
        h.held_prefix(9),
        7,
        "production 4 (16..25) is held to byte 23"
    );
    assert_eq!(g.offer(&mut h, 4, 9), Offer::Pending);
    assert_eq!(h.held_prefix(5), 0, "production 5 (25..30) is not held");
    assert_eq!(g.offer(&mut h, 5, 5), Offer::Pending);
    assert_eq!(h.written, 30);
    h.report(25);
    assert_eq!(g.observe(&h), Some(4));
    assert_eq!(h.accepted_bytes, 25);
}

// ── The worker's retained productions ───────────────────────────────────────

#[test]
fn the_ring_retains_until_the_horizon_and_refuses_rather_than_evicts() {
    let mut r = ProductionRing::<3>::new();
    assert_eq!(r.retain(b"one"), Some(1));
    assert_eq!(r.retain(b"two"), Some(2));
    assert_eq!(r.retain(b"three"), Some(3));
    assert_eq!(r.retain(b"four"), None, "full: backpressure, not eviction");
    assert_eq!(r.next_id(), 4);
    assert_eq!(r.release_through(2), 2);
    assert_eq!(r.first_retained(), 3);
    assert_eq!(r.get(0).map(Retained::body), Some(&b"three"[..]));
    assert_eq!(r.retain(b"four"), Some(4));
    assert_eq!(r.len(), 2);
    let big = [0u8; RETAIN_BYTES_MAX + 1];
    assert_eq!(r.retain(&big), None, "a response past a slot is held back");
}

#[test]
fn a_horizon_report_backwards_or_past_production_changes_nothing() {
    let mut r = ProductionRing::<4>::new();
    r.retain(b"a");
    r.retain(b"b");
    assert_eq!(r.release_through(5), 0, "past what was produced");
    assert_eq!(r.release_through(1), 1);
    assert_eq!(r.release_through(1), 0, "a repeat");
    assert_eq!(r.release_through(0), 0, "backwards");
    assert_eq!(r.released(), 1);
    assert_eq!(r.len(), 1);
}

#[test]
fn a_horizon_report_is_taken_only_at_the_bound_epoch() {
    let h = HorizonReport {
        session_id: SID,
        epoch: 4,
        accepted_through: 9,
        bytes_through: 310,
    };
    let mut b = [0u8; HORIZON_LEN];
    assert_eq!(h.encode(&mut b), HORIZON_LEN);
    assert_eq!(HorizonReport::decode(&b), Some(h));
    assert!(horizon_admitted(&SID, 4, &h));
    assert!(!horizon_admitted(&SID, 5, &h), "a stale emitter's report");
    assert!(!horizon_admitted(&[1; 16], 4, &h));
    assert!(HorizonReport::decode(&b[..HORIZON_LEN - 1]).is_none());
}

// ── The anchor record ───────────────────────────────────────────────────────

#[test]
fn the_anchor_record_round_trips_and_every_flipped_bit_is_refused() {
    let mut c = conn(3);
    c.next_delivery = 12;
    c.next_production = 9;
    c.checkpoint_delivery = 5;
    let rec = AnchorRecord::of(&c, &A1, 6);
    let mut bytes = [0u8; QAC_LEN];
    assert_eq!(rec.encode(&mut bytes), QAC_LEN);
    assert_eq!(AnchorRecord::decode(&bytes), Ok(rec));
    for at in 0..QAC_LEN {
        let mut flipped = bytes;
        flipped[at] ^= 0x01;
        assert_eq!(AnchorRecord::decode(&flipped), Err(AC_CORRUPT), "byte {at}");
    }
    assert_eq!(AnchorRecord::decode(&bytes[..QAC_LEN - 1]), Err(AC_CORRUPT));
    let mut short = [0u8; QAC_LEN - 1];
    assert_eq!(rec.encode(&mut short), 0);
}

/// A record that cannot be one anchor's is refused even with a good CRC.
#[test]
fn an_impossible_record_is_refused() {
    let mut c = conn(3);
    c.next_production = 4;
    let mut b = [0u8; QAC_LEN];
    for (accepted, epoch) in [(4u64, 3u32), (9, 3), (1, 0)] {
        let mut cc = c;
        cc.epoch = epoch;
        AnchorRecord::of(&cc, &A1, accepted).encode(&mut b);
        assert_eq!(AnchorRecord::decode(&b), Err(AC_CORRUPT));
    }
    // No session, and delivery ids that do not start from 1.
    let mut cc = c;
    cc.session_id = [0; 16];
    AnchorRecord::of(&cc, &A1, 1).encode(&mut b);
    assert_eq!(AnchorRecord::decode(&b), Err(AC_CORRUPT));
    let mut cc = c;
    cc.next_delivery = 0;
    AnchorRecord::of(&cc, &A1, 1).encode(&mut b);
    assert_eq!(AnchorRecord::decode(&b), Err(AC_CORRUPT));
}

#[test]
fn an_import_takes_the_standby_transports_counters() {
    let mut c = conn(3);
    c.next_delivery = 10;
    c.next_production = 7;
    let rec = AnchorRecord::of(&c, &A1, 5);
    let t = TransportCursors {
        next_delivery: 14,
        delivery_acked: 11,
        out_bytes: 120,
    };
    let imported = anchor_import(&rec, &t).expect("imports");
    assert_eq!(imported.conn.next_delivery, 14);
    assert_eq!(imported.conn.next_production, 7);
    assert_eq!(imported.bytes_held, 120);
    assert_eq!(imported.new_epoch, 4);
    assert_eq!(
        imported.conn.phase,
        Phase::WaitAttached,
        "held until the worker answers"
    );
    assert_eq!(imported.conn.client_hash, c.client_hash);
    assert!(!imported.owes(11) && imported.owes(12));
    let req = imported.request(&A2);
    assert_eq!(req.anchor_id, A2);
    assert_eq!(req.bytes_held, 120);
    assert_eq!(
        req.next_delivery, 12,
        "the first delivery the transport hands again"
    );
}

/// The record is exported at a binding change; the transport's last
/// acknowledgement may be behind it, and the numbering continues from
/// the transport's.
#[test]
fn an_import_continues_from_a_transport_behind_the_record() {
    let mut c = conn(3);
    c.next_delivery = 10;
    c.next_production = 7;
    let rec = AnchorRecord::of(&c, &A1, 5);
    let behind = TransportCursors {
        next_delivery: 9,
        delivery_acked: 8,
        out_bytes: 8,
    };
    let imported = anchor_import(&rec, &behind).expect("imports");
    assert_eq!(imported.conn.next_delivery, 9);
    assert_eq!(imported.request(&A2).next_delivery, 9);
}

/// A transport that never had an acknowledgement mirrored would hand the
/// connection from its first byte, CONNECT and all: not a session to
/// continue.
#[test]
fn an_import_without_a_mirrored_acknowledgement_is_refused() {
    let mut c = conn(3);
    c.next_delivery = 10;
    c.next_production = 7;
    let rec = AnchorRecord::of(&c, &A1, 5);
    let none = TransportCursors {
        next_delivery: 0,
        delivery_acked: 0,
        out_bytes: 0,
    };
    assert_eq!(anchor_import(&rec, &none).err(), Some(AC_CURSOR_MISMATCH));
}

#[test]
fn an_import_over_cursors_no_transport_could_hold_is_refused() {
    let mut c = conn(3);
    c.next_delivery = 10;
    c.next_production = 7;
    let rec = AnchorRecord::of(&c, &A1, 5);
    // Acknowledged a delivery it has not assembled yet.
    let t = TransportCursors {
        next_delivery: 12,
        delivery_acked: 12,
        out_bytes: 8,
    };
    assert_eq!(anchor_import(&rec, &t).err(), Some(AC_CURSOR_MISMATCH));
}

#[test]
fn a_session_at_the_last_epoch_cannot_be_re_anchored() {
    let mut c = conn(u32::MAX);
    c.next_delivery = 10;
    c.next_production = 7;
    let rec = AnchorRecord::of(&c, &A1, 5);
    let t = TransportCursors {
        next_delivery: 10,
        delivery_acked: 9,
        out_bytes: 8,
    };
    assert_eq!(anchor_import(&rec, &t).err(), Some(AC_STALE_EPOCH));
}

// ── The re-anchor exchange ──────────────────────────────────────────────────

fn seat() -> WorkerSeat {
    WorkerSeat {
        session_id: SID,
        epoch: 3,
        delivery_hw: 9,
        next_production: 8,
        released: 5,
        released_bytes: 60,
        first_offerable: 6,
    }
}

fn request(epoch: u32, next_delivery: u64, bytes_held: u64) -> Reanchor {
    Reanchor {
        session_id: SID,
        new_epoch: epoch,
        anchor_id: A2,
        next_delivery,
        bytes_held,
    }
}

#[test]
fn the_reanchor_frames_round_trip_exactly() {
    let req = request(4, 10, 75);
    let mut out = [0u8; REANCHOR_LEN];
    assert_eq!(req.encode(&mut out), REANCHOR_LEN);
    assert_eq!(Reanchor::decode(&out), Some(req));
    assert!(Reanchor::decode(&out[..REANCHOR_LEN - 1]).is_none());
    let answer = reanchor_verdict(&seat(), &req);
    assert_eq!((answer.released, answer.released_bytes), (5, 60));
    let mut back = [0u8; REANCHORED_LEN];
    assert_eq!(answer.encode(&mut back), REANCHORED_LEN);
    assert_eq!(Reanchored::decode(&back), Some(answer));
    assert!(Reanchored::decode(&back[..REANCHORED_LEN - 1]).is_none());
}

#[test]
fn the_worker_admits_a_replacement_only_on_exact_cursors() {
    let s = seat();
    // The transport hands everything after what the worker consumed.
    assert_eq!(reanchor_verdict(&s, &request(4, 10, 75)).status, AC_OK);
    // From further back: what the worker consumed comes again under its
    // own ids, and is refused as a duplicate.
    assert_eq!(reanchor_verdict(&s, &request(4, 7, 75)).status, AC_OK);
    // The standby holds exactly through the last released production.
    assert_eq!(reanchor_verdict(&s, &request(4, 10, 60)).status, AC_OK);
    let mut other = request(4, 10, 75);
    other.session_id = [1; 16];
    assert_eq!(reanchor_verdict(&s, &other).status, AC_UNKNOWN_SESSION);
    assert_eq!(
        reanchor_verdict(&s, &request(3, 10, 75)).status,
        AC_STALE_EPOCH
    );
    assert_eq!(
        reanchor_verdict(&s, &request(2, 10, 75)).status,
        AC_STALE_EPOCH
    );
    // Delivery 10 was acknowledged to the transport but never reached
    // the worker: the transport will not hand it again.
    assert_eq!(
        reanchor_verdict(&s, &request(4, 11, 75)).status,
        AC_CURSOR_MISMATCH
    );
    // The worker released output the standby does not hold: a response
    // the client will never see.
    assert_eq!(
        reanchor_verdict(&s, &request(4, 10, 59)).status,
        AC_CURSOR_MISMATCH
    );
    // A production above the release whose bytes the worker did not keep
    // cannot be placed or offered again.
    let mut unkept = s;
    unkept.first_offerable = 7;
    assert_eq!(
        reanchor_verdict(&unkept, &request(4, 10, 75)).status,
        AC_CURSOR_MISMATCH
    );
    let mut lost = s;
    lost.epoch = 0;
    assert_eq!(
        reanchor_verdict(&lost, &request(4, 10, 75)).status,
        AC_UNKNOWN_SESSION
    );
}

#[test]
fn the_replacement_applies_only_its_own_answer() {
    let mut c = conn(3);
    c.next_delivery = 10;
    c.next_production = 7;
    let rec = AnchorRecord::of(&c, &A1, 5);
    let t = TransportCursors {
        next_delivery: 12,
        delivery_acked: 6,
        out_bytes: 70,
    };
    let fresh = anchor_import(&rec, &t).expect("imports");
    let ok = Reanchored {
        session_id: SID,
        new_epoch: 4,
        status: AC_OK,
        delivery_hw: 9,
        next_production: 9,
        released: 5,
        released_bytes: 60,
    };
    let mut r = fresh;
    let mut wrong = ok;
    wrong.new_epoch = 5;
    assert_eq!(r.apply(&wrong), AC_UNKNOWN_SESSION);
    let mut refused = ok;
    refused.status = AC_STALE_EPOCH;
    assert_eq!(r.apply(&refused), AC_STALE_EPOCH);
    // The worker has not consumed what the transport was told it had.
    let mut behind = ok;
    behind.delivery_hw = 5;
    assert_eq!(r.apply(&behind), AC_CURSOR_MISMATCH);
    let mut past = ok;
    past.released_bytes = 71;
    assert_eq!(
        r.apply(&past),
        AC_CURSOR_MISMATCH,
        "released past the standby"
    );
    let mut beyond = ok;
    beyond.released = 9;
    assert_eq!(r.apply(&beyond), AC_CURSOR_MISMATCH);
    assert_eq!(r.conn.phase, Phase::WaitAttached, "nothing applied yet");
    assert_eq!(r.apply(&ok), AC_OK);
    assert_eq!(r.conn.phase, Phase::Active);
    assert_eq!(r.conn.epoch, 4);
    assert_eq!(r.gate.next_submit, 6, "re-offers from the first unreleased");
    assert_eq!((r.horizon.written, r.horizon.through), (60, 70));
    assert!(!r.owes(9) && r.owes(10), "skips what the worker consumed");
}

// ── A crash takeover, end to end ────────────────────────────────────────────

/// The response bytes of production `id`, of varying lengths, as every
/// anchor encodes them.
fn encoded(id: u64) -> Vec<u8> {
    format!(
        "PUBACK {id}{}",
        "-".repeat(usize::try_from(id % 4).expect("fits"))
    )
    .into_bytes()
}

/// The worker of the walk: Quantum's delivery admission and the retained
/// productions, bound at `epoch`.
struct Seat {
    epoch: u32,
    hw: u64,
    applied: Vec<u64>,
    ring: ProductionRing<16>,
    released_bytes: u64,
}

impl Seat {
    fn new() -> Self {
        Self {
            epoch: 3,
            hw: 0,
            applied: Vec::new(),
            ring: ProductionRing::new(),
            released_bytes: 0,
        }
    }

    fn deliver(&mut self, id: u64) {
        let h = DeliveryHeader {
            conn: 1,
            session_id: SID,
            epoch: self.epoch,
            delivery_id: id,
            inner_msg_type: MQTT_PUBLISH,
        };
        match admit_delivery(&SID, self.epoch, self.hw, &h) {
            DeliveryVerdict::Accept => {
                self.hw = id;
                self.applied.push(id);
                let pid = self.ring.next_id();
                assert!(self.ring.retain(&encoded(pid)).is_some());
            }
            v => panic!("delivery {id}: {v:?}"),
        }
    }

    fn view(&self) -> WorkerSeat {
        WorkerSeat {
            session_id: SID,
            epoch: self.epoch,
            delivery_hw: self.hw,
            next_production: self.ring.next_id(),
            released: self.ring.released(),
            released_bytes: self.released_bytes,
            first_offerable: self.ring.first_offerable(),
        }
    }

    /// A horizon report reached the worker.
    fn told(&mut self, g: &mut ProductionGate, h: &TransportHorizon) {
        if let Some(through) = g.report() {
            assert_eq!(h.accepted, through);
            self.released_bytes = h.accepted_bytes;
            self.ring.release_through(through);
        }
    }
}

/// An anchor over a connection: the bytes it wrote, in the transport's
/// numbering from `base`.
struct Anchor {
    gate: ProductionGate,
    horizon: TransportHorizon,
    out: Vec<u8>,
}

impl Anchor {
    /// Offer every retained production, in order, writing what the
    /// transport does not hold.
    fn offer(&mut self, seat: &Seat) {
        for k in 0..seat.ring.len() {
            let r = *seat.ring.get(k).expect("retained");
            if r.id < self.gate.next_submit {
                self.gate.offer(&mut self.horizon, r.id, 0);
                continue;
            }
            let body = r.body();
            let skip = usize::try_from(self.horizon.held_prefix(len(body))).expect("fits");
            match self.gate.offer(&mut self.horizon, r.id, len(body)) {
                Offer::Pending | Offer::Accepted => self.out.extend_from_slice(&body[skip..]),
                Offer::NotReady | Offer::Gap => break,
            }
        }
    }
}

/// The primary host loses power with deliveries consumed past the mirrored
/// delivery cursor, productions written past the horizon, and the
/// standby's horizon ending inside a production. The replacement anchor
/// imports the record over the standby's transport, re-anchors the worker,
/// skips what it consumed, and places its re-offers against the standby's
/// bytes: every delivery applied once, and the client reads every
/// response once, whole, in order.
#[test]
#[allow(
    clippy::many_single_char_names,
    reason = "the walk's two anchors, record, cursors and seat read as the protocol names them"
)]
fn a_crash_takeover_applies_every_delivery_once_and_gives_every_response_once() {
    let mut held_all = false;
    for cut in 0usize.. {
        let mut seat = Seat::new();
        let mut a = Anchor {
            gate: ProductionGate::new(),
            horizon: TransportHorizon::new(),
            out: Vec::new(),
        };
        let mut c = conn(3);
        // Deliveries 1..=7 go to the worker; the horizon reaches through
        // production 4 and the worker hears of it.
        for id in 1..=7 {
            seat.deliver(id);
            c.commit_delivery();
            a.offer(&seat);
            if id == 4 {
                let through = len(&a.out);
                a.horizon.report(through);
                a.gate.observe(&a.horizon);
                seat.told(&mut a.gate, &a.horizon);
            }
        }
        assert_eq!(seat.ring.released(), 4);
        assert_eq!(seat.ring.len(), 3, "5..=7 not yet given");
        // The standby held `cut` bytes past production 4 — every byte
        // from none of 5 to all of 7 — when the power went.
        let held = usize::try_from(seat.released_bytes).expect("fits") + cut;
        if held > a.out.len() {
            break;
        }
        held_all = held == a.out.len();
        c.next_production = a.gate.next_submit;
        let rec = AnchorRecord::of(&c, &A1, a.gate.accepted);
        let mut wire = [0u8; QAC_LEN];
        rec.encode(&mut wire);
        let t = TransportCursors {
            next_delivery: 9,
            delivery_acked: 5,
            out_bytes: len(&a.out[..held]),
        };
        let rec = AnchorRecord::decode(&wire).expect("the record arrives whole");
        let mut b = anchor_import(&rec, &t).expect("imports");
        let answer = reanchor_verdict(&seat.view(), &b.request(&A2));
        assert_eq!(answer.status, AC_OK, "cut {cut}");
        seat.epoch = answer.new_epoch;
        assert_eq!(b.apply(&answer), AC_OK);
        let mut r = Anchor {
            gate: b.gate,
            horizon: b.horizon,
            out: Vec::new(),
        };
        // The replacement owes the worker only delivery 8.
        for id in 6..t.next_delivery {
            if b.owes(id) {
                seat.deliver(id);
            }
        }
        r.offer(&seat);
        let total = t.out_bytes + len(&r.out);
        r.horizon.report(total);
        r.gate.observe(&r.horizon);
        seat.told(&mut r.gate, &r.horizon);
        assert_eq!(seat.applied, (1..=8).collect::<Vec<_>>());
        // What the client reads: the standby's bytes, then the
        // replacement's.
        let mut client = a.out[..held].to_vec();
        client.extend_from_slice(&r.out);
        let want: Vec<u8> = (1..=8).flat_map(encoded).collect();
        assert_eq!(client, want, "cut {cut}: each response once, whole");
        assert_eq!(seat.ring.len(), 0);
        assert_eq!(seat.ring.released(), 8);
    }
    assert!(held_all, "the sweep reached a standby holding everything");
}

/// A worker told a horizon the standby does not hold (an asynchronous
/// standby) cannot be re-anchored: the replacement closes, honestly,
/// rather than resume with a response the client will never see.
#[test]
fn a_takeover_behind_a_released_response_is_refused() {
    let mut seat = Seat::new();
    let mut a = Anchor {
        gate: ProductionGate::new(),
        horizon: TransportHorizon::new(),
        out: Vec::new(),
    };
    for id in 1..=3 {
        seat.deliver(id);
        a.offer(&seat);
    }
    let through = len(&a.out);
    a.horizon.report(through);
    a.gate.observe(&a.horizon);
    seat.told(&mut a.gate, &a.horizon);
    assert_eq!(seat.ring.released(), 3);
    // The standby holds only the first production.
    let t = TransportCursors {
        next_delivery: 4,
        delivery_acked: 3,
        out_bytes: len(&encoded(1)),
    };
    let mut c = conn(3);
    c.next_delivery = 4;
    c.next_production = 4;
    let rec = AnchorRecord::of(&c, &A1, 3);
    let b = anchor_import(&rec, &t).expect("imports");
    assert_eq!(
        reanchor_verdict(&seat.view(), &b.request(&A2)).status,
        AC_CURSOR_MISMATCH
    );
}

// ── The inbound acknowledgement ─────────────────────────────────────────────

/// Nothing is acknowledged until the worker has consumed every delivery
/// before the boundary: one written toward it and lost with this host
/// must still be the transport's to hand again.
#[test]
fn a_boundary_is_acknowledged_only_once_the_worker_consumed_up_to_it() {
    let mut d = DeliveryCursor::new();
    assert_eq!(d.due(), None, "nothing received");
    // CONNECT (delivery 1) in the first 20 bytes, PUBLISH (2) to 50.
    d.boundary(2, 20);
    d.boundary(3, 50);
    assert_eq!(d.due(), None, "the worker consumed nothing yet");
    d.consumed(1);
    assert_eq!(d.due(), None, "latched at the newest boundary, 50");
    d.consumed(2);
    assert_eq!(d.due(), Some((3, 50)));
    d.acked();
    assert_eq!(d.acked_bytes, 50);
    assert_eq!(d.due(), None, "nothing new");
}

/// A steady stream still gets acknowledged: the latched boundary waits
/// for the worker while newer ones arrive, and the next is latched only
/// after it goes.
#[test]
fn a_latched_boundary_is_not_chased_by_newer_ones() {
    let mut d = DeliveryCursor::new();
    d.boundary(5, 100);
    assert_eq!(d.due(), None);
    d.boundary(9, 200);
    d.consumed(4);
    assert_eq!(d.due(), Some((5, 100)), "the latched one, not the newest");
    d.acked();
    assert_eq!(d.due(), None, "9 not consumed yet");
    d.consumed(8);
    assert_eq!(d.due(), Some((9, 200)));
}

/// Boundaries never move back, and a codec position at or behind the
/// last one changes nothing.
#[test]
fn boundaries_only_move_forward() {
    let mut d = DeliveryCursor::new();
    d.boundary(3, 40);
    d.boundary(2, 30);
    d.boundary(3, 40);
    d.consumed(10);
    assert_eq!(d.due(), Some((3, 40)));
}

/// A connection resumed here starts at the predecessor's acknowledgement,
/// and owes nothing until its own codec passes a boundary beyond it.
#[test]
fn a_resumed_connection_acknowledges_from_where_its_predecessor_stood() {
    let mut d = DeliveryCursor::resumed(7, 300);
    d.consumed(20);
    assert_eq!(d.due(), None, "the predecessor already acknowledged 300");
    d.boundary(7, 300);
    assert_eq!(d.due(), None);
    d.boundary(8, 340);
    assert_eq!(d.due(), Some((8, 340)));
}

/// After the session moves to another worker, that worker's own report
/// is waited for.
#[test]
fn a_rebound_connection_waits_for_its_new_workers_report() {
    let mut d = DeliveryCursor::new();
    d.consumed(9);
    d.rebound();
    d.boundary(6, 80);
    assert_eq!(d.due(), None);
    d.consumed(5);
    assert_eq!(d.due(), Some((6, 80)));
}
