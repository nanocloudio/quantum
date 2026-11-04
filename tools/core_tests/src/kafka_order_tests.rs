//! The order a Kafka connection's responses leave in
//! (`kafka_order_core.rs`).
//!
//! The failure prevented: a durability-gated Produce answered after a
//! later request on the same connection that was answered at once (a
//! Fetch, a Metadata, a `ListOffsets`), so the client reads a correlation
//! id that is not its oldest request's — Java's `NetworkClient` fails the
//! connection on it, librdkafka calls it a protocol error. And the ways a
//! fix could fail instead: an answer lost or written twice, a request
//! never answered holding the connection for ever, answers written past
//! one that will never come, slots that leak, a full pool that wedges
//! the response bus.

use super::kafka_order::*;

/// A frame as the codec builds it: `[size][corr][body]`, `len` bytes.
fn frame(corr: i32, len: usize) -> Vec<u8> {
    let mut f = vec![0u8; len.max(8)];
    let size = i32::try_from(len.max(8) - 4).expect("a frame length");
    f[0..4].copy_from_slice(&size.to_be_bytes());
    f[4..8].copy_from_slice(&corr.to_be_bytes());
    f
}

fn corr_of(f: &[u8]) -> i32 {
    i32::from_be_bytes([f[4], f[5], f[6], f[7]])
}

fn pool() -> Box<Pool> {
    Box::new(Pool::zero())
}

/// Take a request answering `corr` of slot class `class`; panics if
/// there is no room (the tests that want "no room" call `reserve`).
fn take(q: &mut Seq, p: &mut Pool, corr: i32, class: u8, now: u64) {
    let slot = reserve(q, p, class).expect("room for the request");
    push(q, corr, slot, now);
}

/// The codec's path for an answer: `answer`, then `flush`.
fn deliver(q: &mut Seq, p: &mut Pool, corr: i32, f: Option<&[u8]>, out: &mut Vec<i32>) -> Answered {
    let r = answer(q, p, corr, f, 0, |b| {
        out.push(corr_of(b));
        true
    });
    flush(q, p, 0, |b| {
        out.push(corr_of(b));
        true
    });
    r
}

#[test]
fn a_late_produce_answer_is_written_before_the_fetch_answered_after_it() {
    let (mut q, mut p) = (Seq::zero(), pool());
    let mut out = Vec::new();
    take(&mut q, &mut p, 1, class_for(0), 0); // Produce, durability-gated
    take(&mut q, &mut p, 2, class_for(1), 0); // Fetch, answered at once
    take(&mut q, &mut p, 3, class_for(3), 0); // Metadata, the codec's own
    assert_eq!(
        deliver(&mut q, &mut p, 2, Some(&frame(2, 300)), &mut out),
        Answered::Kept
    );
    assert_eq!(
        deliver(&mut q, &mut p, 3, Some(&frame(3, 100)), &mut out),
        Answered::Kept
    );
    assert!(out.is_empty(), "nothing overtakes the oldest request");
    assert_eq!(
        deliver(&mut q, &mut p, 1, Some(&frame(1, 60)), &mut out),
        Answered::Written
    );
    assert_eq!(out, vec![1, 2, 3]);
    assert!(is_idle(&q));
    assert_eq!(p.in_use, 0, "every slot given back");
}

#[test]
fn the_oldest_request_takes_no_slot_and_one_that_becomes_oldest_gives_its_back() {
    let (mut q, mut p) = (Seq::zero(), pool());
    let mut out = Vec::new();
    assert_eq!(reserve(&q, &mut p, CLASS_LARGE), Some(NO_SLOT));
    push(&mut q, 1, NO_SLOT, 0);
    take(&mut q, &mut p, 2, CLASS_SMALL, 0);
    take(&mut q, &mut p, 3, CLASS_LARGE, 0);
    assert_eq!(p.in_use, 2);
    deliver(&mut q, &mut p, 1, Some(&frame(1, 20)), &mut out);
    // 2 is the oldest now, still waiting: its slot is back.
    assert_eq!(p.in_use, 1);
    deliver(&mut q, &mut p, 2, Some(&frame(2, 20)), &mut out);
    deliver(&mut q, &mut p, 3, Some(&frame(3, 20)), &mut out);
    assert_eq!(out, vec![1, 2, 3]);
    assert_eq!(p.in_use, 0);
}

#[test]
fn a_connection_takes_at_most_depth_requests_in_flight() {
    let (mut q, mut p) = (Seq::zero(), pool());
    for c in 0..i32::try_from(DEPTH).expect("a small bound") {
        take(&mut q, &mut p, c, CLASS_SMALL, 0);
    }
    assert_eq!(len(&q), DEPTH);
    assert_eq!(
        reserve(&q, &mut p, CLASS_SMALL),
        None,
        "the next waits for room"
    );
    let mut out = Vec::new();
    deliver(&mut q, &mut p, 0, Some(&frame(0, 20)), &mut out);
    assert!(reserve(&q, &mut p, CLASS_SMALL).is_some());
}

#[test]
fn a_full_pool_stops_later_requests_but_never_the_oldest() {
    // Every large slot held by connections that are pipelining...
    let mut p = pool();
    let mut qs: Vec<Seq> = (0..LARGE_SLOTS).map(|_| Seq::zero()).collect();
    for (i, q) in qs.iter_mut().enumerate() {
        let i = i32::try_from(i).expect("a small index");
        take(q, &mut p, 100 + i, CLASS_LARGE, 0);
        take(q, &mut p, 200 + i, CLASS_LARGE, 0);
    }
    // ...a further pipelined Fetch on another connection waits...
    let mut other = Seq::zero();
    take(&mut other, &mut p, 1, CLASS_LARGE, 0);
    assert_eq!(reserve(&other, &mut p, CLASS_LARGE), None);
    // ...but a connection with nothing in flight is never stopped: its
    // answer needs no slot, so answers always drain and slots come back.
    let fresh = Seq::zero();
    assert_eq!(reserve(&fresh, &mut p, CLASS_LARGE), Some(NO_SLOT));
    let mut out = Vec::new();
    deliver(&mut qs[0], &mut p, 200, Some(&frame(200, 4000)), &mut out);
    assert!(out.is_empty());
    deliver(&mut qs[0], &mut p, 100, Some(&frame(100, 4000)), &mut out);
    assert_eq!(out, vec![100, 200]);
    assert!(reserve(&other, &mut p, CLASS_LARGE).is_some());
}

#[test]
fn an_answer_the_bus_refuses_changes_nothing_and_is_offered_again() {
    let (mut q, mut p) = (Seq::zero(), pool());
    take(&mut q, &mut p, 1, CLASS_SMALL, 0);
    take(&mut q, &mut p, 2, CLASS_SMALL, 0);
    let f1 = frame(1, 40);
    assert_eq!(
        answer(&mut q, &mut p, 1, Some(&f1), 0, |_| false),
        Answered::Refused
    );
    assert_eq!(len(&q), 2);
    // The later answer is kept; the front still waits on 1.
    let mut out = Vec::new();
    assert_eq!(
        deliver(&mut q, &mut p, 2, Some(&frame(2, 40)), &mut out),
        Answered::Kept
    );
    assert!(out.is_empty());
    assert_eq!(
        deliver(&mut q, &mut p, 1, Some(&f1), &mut out),
        Answered::Written
    );
    assert_eq!(out, vec![1, 2]);
}

#[test]
fn a_kept_answer_the_bus_refuses_blocks_the_front_until_it_takes_it() {
    let (mut q, mut p) = (Seq::zero(), pool());
    take(&mut q, &mut p, 1, CLASS_SMALL, 0);
    take(&mut q, &mut p, 2, CLASS_SMALL, 0);
    take(&mut q, &mut p, 3, CLASS_SMALL, 0);
    let mut out = Vec::new();
    for c in [3, 2] {
        answer(&mut q, &mut p, c, Some(&frame(c, 30)), 0, |_| true);
    }
    // 1 is written; 2 is refused; 3 must not pass it.
    let mut budget = 1;
    answer(&mut q, &mut p, 1, Some(&frame(1, 30)), 0, |b| {
        out.push(corr_of(b));
        true
    });
    let front = flush(&mut q, &mut p, 0, |b| {
        if budget == 0 {
            return false;
        }
        budget -= 1;
        out.push(corr_of(b));
        true
    });
    assert_eq!(out, vec![1, 2]);
    assert_eq!(front, Front::Blocked);
    assert!(has_work(&q));
    assert_eq!(
        flush(&mut q, &mut p, 0, |b| {
            out.push(corr_of(b));
            true
        }),
        Front::Idle
    );
    assert_eq!(out, vec![1, 2, 3]);
}

#[test]
fn a_request_never_answered_closes_the_connection_in_its_turn() {
    let (mut q, mut p) = (Seq::zero(), pool());
    for c in 1..=4 {
        take(&mut q, &mut p, c, CLASS_SMALL, 0);
    }
    let mut out = Vec::new();
    // 2 will never be answered; 3 and 4 are, early.
    assert_eq!(deliver(&mut q, &mut p, 2, None, &mut out), Answered::Kept);
    deliver(&mut q, &mut p, 3, Some(&frame(3, 30)), &mut out);
    deliver(&mut q, &mut p, 4, Some(&frame(4, 30)), &mut out);
    assert!(out.is_empty());
    answer(&mut q, &mut p, 1, Some(&frame(1, 30)), 0, |b| {
        out.push(corr_of(b));
        true
    });
    let front = flush(&mut q, &mut p, 0, |b| {
        out.push(corr_of(b));
        true
    });
    // What was ahead of it is written; nothing after it ever is.
    assert_eq!(out, vec![1]);
    assert_eq!(front, Front::Close);
    reset(&mut q, &mut p);
    assert!(is_idle(&q));
    assert_eq!(p.in_use, 0);
}

#[test]
fn an_answer_for_no_request_in_flight_is_never_written() {
    let (mut q, mut p) = (Seq::zero(), pool());
    let mut out = Vec::new();
    assert_eq!(
        deliver(&mut q, &mut p, 9, Some(&frame(9, 30)), &mut out),
        Answered::Unmatched
    );
    take(&mut q, &mut p, 1, CLASS_SMALL, 0);
    assert_eq!(
        deliver(&mut q, &mut p, 9, Some(&frame(9, 30)), &mut out),
        Answered::Unmatched
    );
    // An answer is taken once: a second one for the same request is not
    // written again.
    take(&mut q, &mut p, 2, CLASS_SMALL, 0);
    deliver(&mut q, &mut p, 2, Some(&frame(2, 30)), &mut out);
    assert_eq!(
        deliver(&mut q, &mut p, 2, Some(&frame(2, 30)), &mut out),
        Answered::Unmatched
    );
    assert!(out.is_empty());
}

#[test]
fn an_answer_the_codec_makes_itself_waits_behind_those_in_flight() {
    let (mut q, mut p) = (Seq::zero(), pool());
    take(&mut q, &mut p, 1, class_for(0), 0); // Produce in flight
    let slot = reserve(&q, &mut p, class_for(18)).expect("room"); // ApiVersions
    assert!(push_answered(&mut q, &mut p, 2, slot, &frame(2, 90), 0));
    let mut out = Vec::new();
    assert!(!has_work(&q), "the front still waits on the Produce");
    deliver(&mut q, &mut p, 1, Some(&frame(1, 60)), &mut out);
    assert_eq!(out, vec![1, 2]);
    // A frame too large for its slot is not taken, and the slot is back.
    take(&mut q, &mut p, 3, CLASS_SMALL, 0);
    let slot = reserve(&q, &mut p, CLASS_SMALL).expect("room");
    assert!(!push_answered(
        &mut q,
        &mut p,
        4,
        slot,
        &frame(4, SMALL_BYTES + 1),
        0
    ));
    assert_eq!(p.in_use, 0, "3 is the oldest and holds none; 4's came back");
}

#[test]
fn an_answer_too_large_for_its_slot_closes_rather_than_overtakes() {
    let (mut q, mut p) = (Seq::zero(), pool());
    take(&mut q, &mut p, 1, CLASS_SMALL, 0);
    take(&mut q, &mut p, 2, CLASS_SMALL, 0);
    let mut out = Vec::new();
    assert_eq!(
        deliver(
            &mut q,
            &mut p,
            2,
            Some(&frame(2, SMALL_BYTES + 1)),
            &mut out
        ),
        Answered::Overflow
    );
    answer(&mut q, &mut p, 1, Some(&frame(1, 30)), 0, |b| {
        out.push(corr_of(b));
        true
    });
    assert_eq!(flush(&mut q, &mut p, 0, |_| true), Front::Close);
    assert_eq!(out, vec![1]);
}

#[test]
fn the_slot_classes_hold_every_answer_their_apis_make() {
    // session_processor's largest small-class answers, framed (+8):
    // a ProduceResponse v5+ for 16 partitions of a 64-byte topic —
    // [1][topic][16]{partition, error, base_offset, log_append_time,
    // log_start_offset}[throttle] — and a parked OffsetCommit body
    // (`store::KIN_RESP_MAX` = 512).
    let produce = 4 + 2 + 64 + 4 + 16 * (4 + 2 + 8 + 8 + 8) + 4;
    assert!(produce + 8 <= SMALL_BYTES, "{produce}");
    const { assert!(512 + 8 <= SMALL_BYTES) };
    // Everything that carries records or lists is large.
    for k in [1i16, 2, 3, 9, 11] {
        assert_eq!(class_for(k), CLASS_LARGE, "api {k}");
    }
    for k in [0i16, 8, 10, 12, 13, 14, 18, 22] {
        assert_eq!(class_for(k), CLASS_SMALL, "api {k}");
    }
    // A large slot's frame fits the codec's frame edge (8 KiB less the
    // connection id).
    const { assert!(LARGE_BYTES + 2 <= 8192) };
}

#[test]
fn only_an_acks_zero_produce_goes_unanswered() {
    // v2: [acks][timeout]...
    assert!(!answered(0, 2, &[0, 0, 0, 0, 0x75, 0x30]));
    assert!(answered(0, 2, &[0xFF, 0xFF, 0, 0, 0x75, 0x30]));
    assert!(answered(0, 2, &[0, 1]));
    // v3+: [transactional_id][acks]...
    assert!(!answered(0, 3, &[0xFF, 0xFF, 0, 0, 0, 0, 0, 0]));
    assert!(!answered(0, 7, &[0, 2, b't', b'x', 0, 0, 0, 0, 0, 0]));
    assert!(answered(0, 7, &[0, 2, b't', b'x', 0, 1, 0, 0, 0, 0]));
    // Too short to say: answered (session_processor answers or closes).
    assert!(answered(0, 3, &[0, 9, b't']));
    assert!(answered(0, 2, &[0]));
    // Every other API is answered, whatever its body.
    assert!(answered(1, 4, &[0, 0]));
    assert!(answered(3, 1, &[]));
}

#[test]
fn an_unanswered_oldest_request_is_overdue_after_the_timeout_from_its_turn() {
    let (mut q, mut p) = (Seq::zero(), pool());
    take(&mut q, &mut p, 1, CLASS_SMALL, 1_000);
    take(&mut q, &mut p, 2, CLASS_SMALL, 5_000);
    assert!(!overdue(&q, 1_000 + 15_000, 15_000));
    assert!(overdue(&q, 1_000 + 15_001, 15_000));
    // The clock restarts when the next request becomes the oldest.
    let mut out = Vec::new();
    answer(&mut q, &mut p, 1, Some(&frame(1, 20)), 20_000, |b| {
        out.push(corr_of(b));
        true
    });
    assert!(!overdue(&q, 30_000, 15_000));
    assert!(overdue(&q, 35_001, 15_000));
    // A refused answer leaves the front waiting: the answer went back
    // to its caller, to be offered again.
    answer(&mut q, &mut p, 2, Some(&frame(2, 20)), 0, |_| false);
    assert!(overdue(&q, 35_001, 15_000));
}

#[test]
fn reset_forgets_what_was_in_flight_and_gives_its_slots_back() {
    let (mut q, mut p) = (Seq::zero(), pool());
    for c in 1..=5 {
        take(
            &mut q,
            &mut p,
            c,
            if c % 2 == 0 { CLASS_LARGE } else { CLASS_SMALL },
            0,
        );
    }
    answer(&mut q, &mut p, 3, Some(&frame(3, 30)), 0, |_| true);
    assert_eq!(p.in_use, 4);
    reset(&mut q, &mut p);
    assert_eq!(p.in_use, 0);
    let mut out = Vec::new();
    assert_eq!(
        deliver(&mut q, &mut p, 1, Some(&frame(1, 20)), &mut out),
        Answered::Unmatched
    );
}

/// A small deterministic generator, so the property test needs no crate.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    /// A value in `0..n`.
    fn below(&mut self, n: usize) -> usize {
        let n = u64::try_from(n).expect("a small bound");
        usize::try_from(self.next() % n).expect("below a usize bound")
    }

    /// True one time in `n`.
    fn one_in(&mut self, n: usize) -> bool {
        self.below(n) == 0
    }
}

/// One connection under the property test: its queue, what it sent and
/// what it read.
struct Conn {
    queue: Seq,
    next: i32,
    sent: Vec<i32>,
    read: Vec<i32>,
}

/// An answer still to come.
struct Owed {
    conn: usize,
    corr: i32,
    bytes: usize,
}

#[test]
fn whatever_order_answers_come_in_every_connection_reads_its_own_request_order() {
    const CONNS: usize = 12;
    let mut rng = Lcg(0x5eed);
    for _round in 0..200 {
        let mut slots = pool();
        let mut conns: Vec<Conn> = (0..CONNS)
            .map(|_| Conn {
                queue: Seq::zero(),
                next: 0,
                sent: Vec::new(),
                read: Vec::new(),
            })
            .collect();
        let mut owed: Vec<Owed> = Vec::new();
        for _ in 0..400 {
            let at = rng.below(CONNS);
            if !rng.one_in(3) {
                // A request, of either class, if there is room.
                let class = u8::from(rng.one_in(2));
                let conn = &mut conns[at];
                if let Some(slot) = reserve(&conn.queue, &mut slots, class) {
                    let corr = conn.next;
                    conn.next += 1;
                    push(&mut conn.queue, corr, slot, 0);
                    conn.sent.push(corr);
                    let max = if class == CLASS_LARGE {
                        LARGE_BYTES
                    } else {
                        SMALL_BYTES
                    };
                    owed.push(Owed {
                        conn: at,
                        corr,
                        bytes: 8 + rng.below(max - 8),
                    });
                }
            } else if !owed.is_empty() {
                // Some answer, any one, arrives; the bus sometimes refuses.
                let pick = rng.below(owed.len());
                let o = &owed[pick];
                let f = frame(o.corr, o.bytes);
                let taken = !rng.one_in(4);
                let conn = &mut conns[o.conn];
                let read = &mut conn.read;
                let r = answer(&mut conn.queue, &mut slots, o.corr, Some(&f), 0, |b| {
                    if taken {
                        read.push(corr_of(b));
                    }
                    taken
                });
                if r != Answered::Refused {
                    assert!(matches!(r, Answered::Written | Answered::Kept), "{r:?}");
                    owed.swap_remove(pick);
                }
            }
            // Every step the codec flushes, and the bus may refuse.
            for conn in &mut conns {
                let taken = !rng.one_in(4);
                let read = &mut conn.read;
                flush(&mut conn.queue, &mut slots, 0, |b| {
                    if taken {
                        read.push(corr_of(b));
                    }
                    taken
                });
            }
        }
        // Everything still owed arrives, and the bus takes it all.
        for o in owed.drain(..) {
            let f = frame(o.corr, o.bytes);
            let conn = &mut conns[o.conn];
            let read = &mut conn.read;
            let r = answer(&mut conn.queue, &mut slots, o.corr, Some(&f), 0, |b| {
                read.push(corr_of(b));
                true
            });
            assert_ne!(r, Answered::Refused);
        }
        for conn in &mut conns {
            let read = &mut conn.read;
            assert_eq!(
                flush(&mut conn.queue, &mut slots, 0, |b| {
                    read.push(corr_of(b));
                    true
                }),
                Front::Idle
            );
            assert_eq!(
                conn.read, conn.sent,
                "each connection reads its requests' order, each answer once"
            );
        }
        assert_eq!(slots.in_use, 0, "no slot leaks");
    }
}
