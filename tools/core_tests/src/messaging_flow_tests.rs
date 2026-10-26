//! The worker's side of its exchange with `messaging`: the ordered outbox
//! (`messaging_outbox_core.rs`) and the credit ledger
//! (`messaging_flow_core.rs`) — and, driven together with `messaging`'s
//! own hold (`messaging_hold_core.rs`) over bounded edges both ways, the
//! whole exchange saturated: a full op edge, a full result edge and a full
//! delivery park at once.
//!
//! The failures prevented: a request the op edge refused dropped (a lost
//! dedup check is a committed publish that never fans out); requests
//! leaving out of order; an answer arriving with nowhere to go, so the
//! worker would have to drop it or stop reading the answers that carry
//! the verdicts; credit leaking until nothing more is asked; a drained
//! session's fresh deliveries overtaking its backlog; a read or drain for a
//! session that has gone, or for the next one in its slot; the exchange
//! deadlocking when every edge is full.

use std::collections::VecDeque;

use super::messaging_flow::*;
use super::messaging_hold::{self as hold, BusHold, Marker, Owed, ReadCursor, Replies};
use super::messaging_outbox::*;

// ── The outbox ─────────────────────────────────────────────────────────

#[test]
fn a_request_the_edge_refuses_waits_and_leaves_in_order() {
    let mut ob = Box::new(Outbox::new());
    let mut sent: Vec<(u8, Vec<u8>)> = Vec::new();
    let mut room = 1;
    for k in 0..5u8 {
        assert!(ob.send(
            |mt, p| {
                if room == 0 {
                    return false;
                }
                room -= 1;
                sent.push((mt, p.to_vec()));
                true
            },
            k,
            &[k; 3],
        ));
    }
    assert_eq!(sent, vec![(0, vec![0; 3])], "the first went at once");
    assert_eq!(ob.waited, 4);
    assert_eq!(ob.used(), 4 * Outbox::cost(3));
    room = 2;
    ob.flush(|mt, p| {
        if room == 0 {
            return false;
        }
        room -= 1;
        sent.push((mt, p.to_vec()));
        true
    });
    room = 10;
    // A new one goes behind those still waiting, never ahead.
    assert!(ob.send(
        |mt, p| {
            if room == 0 {
                return false;
            }
            room -= 1;
            sent.push((mt, p.to_vec()));
            true
        },
        9,
        &[9],
    ));
    let order: Vec<u8> = sent.iter().map(|(mt, _)| *mt).collect();
    assert_eq!(order, vec![0, 1, 2, 3, 4, 9]);
    assert!(ob.is_empty());
    assert_eq!(ob.refused, 0);
}

#[test]
fn frames_wrap_the_ring_whole() {
    let mut ob = Box::new(Outbox::new());
    let big = vec![0x5A; FRAME_MAX];
    let mut got = Vec::new();
    for round in 0..(3 * OUTBOX_BYTES / FRAME_MAX) {
        let tag = u8::try_from(round % 251).expect("fits");
        let mut frame = big.clone();
        frame[0] = tag;
        assert!(ob.send(|_, _| false, tag, &frame));
        ob.flush(|mt, p| {
            got.push((mt, p.len(), p[0]));
            true
        });
    }
    assert!(got.iter().enumerate().all(|(i, &(mt, len, first))| {
        let tag = u8::try_from(i % 251).expect("fits");
        mt == tag && len == FRAME_MAX && first == tag
    }));
}

#[test]
fn a_full_outbox_refuses_and_counts_rather_than_overwrite() {
    let mut ob = Box::new(Outbox::new());
    let frame = vec![1u8; FRAME_MAX];
    let mut n = 0;
    while ob.free() >= Outbox::cost(FRAME_MAX) {
        assert!(ob.send(|_, _| false, 1, &frame));
        n += 1;
    }
    assert!(!ob.send(|_, _| false, 2, &frame));
    assert_eq!(ob.refused, 1);
    let mut flushed = 0;
    ob.flush(|_, _| {
        flushed += 1;
        true
    });
    assert_eq!(flushed, n, "every queued frame, none overwritten");
    assert!(
        !ob.send(|_, _| true, 3, &vec![0; FRAME_MAX + 1]),
        "larger than a frame"
    );
}

// ── The credit ledger ──────────────────────────────────────────────────

/// Every slot holds the session with identity `stream`.
fn holds(stream: u64) -> impl Fn(u32) -> Option<u64> {
    move |_| Some(stream)
}

#[test]
fn a_read_is_granted_from_its_own_pool_whatever_the_park_room() {
    let mut f = Box::new(Flow::new());
    assert!(f.read_add(3, 33, 1, 1, b"a/#"));
    let Some(Request::Read { read, max }) = f.next_request(0, holds(33)) else {
        panic!("a read, with no park room at all");
    };
    assert_eq!((max, f.read_credit()), (CREDIT_PER_REQUEST, 4));
    assert_eq!(f.drain_credit(), 0);
    assert_eq!(f.read(read).next, 0);
    assert_eq!(f.read_answer(3, 33), Answer::Deliver);
    assert_eq!(f.read_credit(), 3);
    f.read_end(3, 17);
    assert_eq!(f.read_credit(), 0, "what was not spent comes back");
    // Asked again from where it stopped.
    let Some(Request::Read { read, .. }) = f.next_request(0, holds(33)) else {
        panic!("again");
    };
    assert_eq!(f.read(read).next, 17);
    f.read_end(3, FLOW_READ_DONE);
    assert_eq!(f.reads_free(), READ_SLOTS);
    assert_eq!(f.read_credit(), 0);
}

#[test]
fn reads_in_flight_never_hold_more_than_the_pool() {
    let mut f = Box::new(Flow::new());
    for session in 0..4 {
        assert!(f.read_add(session, 1, 0, 0, b"#"));
    }
    let mut granted = 0;
    while let Some(Request::Read { max, .. }) = f.next_request(0, holds(1)) {
        granted += u32::from(max);
    }
    assert_eq!((granted, f.read_credit()), (READ_CREDIT, READ_CREDIT));
    f.read_end(0, FLOW_READ_DONE);
    assert!(f.next_request(0, holds(1)).is_some(), "its credit went on");
}

#[test]
fn a_drain_is_granted_no_more_than_the_park_room() {
    let mut f = Box::new(Flow::new());
    f.want_drain(7);
    assert_eq!(f.next_request(0, holds(70)), None, "no room, no drain");
    let Some(Request::Drain { session: 7, max }) = f.next_request(2, holds(70)) else {
        panic!("a drain");
    };
    assert_eq!((max, f.drain_credit(), f.read_credit()), (2, 2, 0));
    assert_eq!(f.drain_answer(7, 70), Answer::Deliver);
    assert_eq!(f.drain_credit(), 1);
    f.drained(7, 0);
    assert_eq!(f.drain_credit(), 0);
}

#[test]
fn an_answer_no_credit_was_granted_for_is_counted_not_delivered() {
    let mut f = Box::new(Flow::new());
    assert_eq!(f.read_answer(5, 1), Answer::Unasked);
    assert_eq!(f.drain_answer(5, 1), Answer::Unasked);
    let _ = f.read_add(5, 1, 0, 0, b"#");
    let Some(Request::Read { max, .. }) = f.next_request(0, holds(1)) else {
        panic!("read");
    };
    for _ in 0..max {
        assert_eq!(f.read_answer(5, 1), Answer::Deliver);
    }
    assert_eq!(f.read_answer(5, 1), Answer::Unasked, "past its credit");
    assert_eq!(f.unasked, 3);
}

#[test]
fn one_read_per_session_is_in_flight_and_another_sessions_goes_meanwhile() {
    let mut f = Box::new(Flow::new());
    assert!(f.read_add(1, 1, 0, 0, b"a"));
    assert!(f.read_add(1, 1, 0, 0, b"b"));
    assert!(f.read_add(2, 1, 0, 0, b"c"));
    let Some(Request::Read { read: r1, .. }) = f.next_request(0, holds(1)) else {
        panic!();
    };
    assert_eq!(f.read(r1).session, 1);
    let Some(Request::Read { read: r2, .. }) = f.next_request(0, holds(1)) else {
        panic!();
    };
    assert_eq!(f.read(r2).session, 2, "session 1's second waits");
    f.read_end(2, FLOW_READ_DONE);
    assert_eq!(f.next_request(0, holds(1)), None);
    f.read_end(1, FLOW_READ_DONE);
    let Some(Request::Read { read: r3, .. }) = f.next_request(0, holds(1)) else {
        panic!();
    };
    assert_eq!(f.read(r3).pattern(), b"b");
}

#[test]
fn a_read_for_a_session_gone_or_replaced_is_let_go() {
    let mut f = Box::new(Flow::new());
    assert!(f.read_add(4, 0xAA, 0, 0, b"x"));
    // The slot now holds another session.
    assert_eq!(f.next_request(0, holds(0xBB)), None);
    assert_eq!(f.reads_free(), READ_SLOTS);
    // The slot holds no session.
    assert!(f.read_add(4, 0xAA, 0, 0, b"x"));
    assert_eq!(f.next_request(0, |_| None), None);
    assert_eq!(f.reads_free(), READ_SLOTS);
    // Released with a request in flight: its answers are dropped and its
    // credit returns at the marker.
    assert!(f.read_add(4, 0xAA, 0, 0, b"x"));
    let Some(Request::Read { .. }) = f.next_request(0, holds(0xAA)) else {
        panic!();
    };
    f.release(4);
    assert_eq!(f.read_answer(4, 0xAA), Answer::Drop);
    f.read_end(4, 99);
    assert_eq!(f.read_credit(), 0);
    assert_eq!(f.reads_free(), READ_SLOTS, "not asked again");
}

#[test]
fn an_answer_reaching_a_slot_another_session_now_holds_is_dropped() {
    let mut f = Box::new(Flow::new());
    assert!(f.read_add(4, 0xAA, 0, 0, b"x"));
    let Some(Request::Read { .. }) = f.next_request(0, holds(0xAA)) else {
        panic!();
    };
    assert_eq!(f.read_answer(4, 0xBB), Answer::Drop);
    assert_eq!(f.read_answer(4, 0xAA), Answer::Deliver);
    f.read_end(4, FLOW_READ_DONE);
    assert_eq!(f.read_credit(), 0);

    f.want_drain(9);
    let Some(Request::Drain { session: 9, .. }) = f.next_request(4, holds(0xAA)) else {
        panic!();
    };
    assert_eq!(f.drain_answer(9, 0xBB), Answer::Drop);
    assert_eq!(f.drain_answer(9, 0xAA), Answer::Deliver);
    f.drained(9, 0);
    assert_eq!(
        f.drain_credit(),
        0,
        "dropped or delivered, the credit is back"
    );
}

#[test]
fn a_drain_whose_session_is_released_drops_its_answers_and_frees_at_its_marker() {
    let mut f = Box::new(Flow::new());
    f.want_drain(7);
    let Some(Request::Drain { session: 7, .. }) = f.next_request(4, holds(70)) else {
        panic!();
    };
    f.release(7);
    assert!(!f.backlog(7));
    assert_eq!(
        f.drain_answer(7, 70),
        Answer::Drop,
        "even under its identity"
    );
    f.drained(7, 3);
    assert_eq!(f.drain_credit(), 0);
    assert_eq!(f.next_request(4, holds(70)), None, "nothing asked for it");

    // A new session takes the slot while the cancelled drain is out: its
    // own drain follows the marker, under its identity.
    f.want_drain(7);
    let Some(Request::Drain { .. }) = f.next_request(4, holds(70)) else {
        panic!();
    };
    f.release(7);
    f.want_drain(7);
    assert!(f.backlog(7));
    assert_eq!(f.next_request(4, holds(71)), None, "one drain per slot");
    assert_eq!(f.drain_answer(7, 71), Answer::Drop, "the old session's");
    f.drained(7, 0);
    let Some(Request::Drain { session: 7, .. }) = f.next_request(4, holds(71)) else {
        panic!("the new session's drain");
    };
    assert_eq!(f.drain_answer(7, 71), Answer::Deliver);
}

#[test]
fn a_session_is_idle_only_with_no_read_filed_and_no_drain_out() {
    // A session moves to another worker only when idle: answers asked for
    // here could reach it nowhere else.
    let mut f = Box::new(Flow::new());
    assert!(f.idle(6));
    assert!(f.read_add(6, 1, 0, 0, b"r"));
    assert!(!f.idle(6), "a read filed, not yet asked");
    let Some(Request::Read { .. }) = f.next_request(0, holds(1)) else {
        panic!();
    };
    assert!(!f.idle(6), "a read in flight");
    f.read_end(6, FLOW_READ_DONE);
    assert!(f.idle(6));
    f.want_drain(6);
    assert!(f.idle(6), "a drain wanted but not asked holds nothing");
    let Some(Request::Drain { .. }) = f.next_request(4, holds(1)) else {
        panic!();
    };
    assert!(!f.idle(6), "a drain in flight");
    f.drained(6, 0);
    assert!(f.idle(6));
}

#[test]
fn a_backlog_clears_only_when_a_drain_finds_nothing_left_and_nothing_joined() {
    let mut f = Box::new(Flow::new());
    f.want_drain(7);
    assert!(f.backlog(7));
    let Some(Request::Drain { session: 7, max }) = f.next_request(4, holds(1)) else {
        panic!();
    };
    assert_eq!(max, 4);
    assert_eq!(f.next_request(4, holds(1)), None, "one drain per session");
    assert_eq!(f.drain_answer(7, 1), Answer::Deliver);
    f.drained(7, 2);
    assert!(f.backlog(7), "two still queued");
    let Some(Request::Drain { .. }) = f.next_request(4, holds(1)) else {
        panic!();
    };
    // A fresh delivery joins the queue while the drain is out.
    f.joined(7);
    f.drained(7, 0);
    assert!(f.backlog(7), "it joined after the drain was asked for");
    let Some(Request::Drain { .. }) = f.next_request(4, holds(1)) else {
        panic!();
    };
    f.drained(7, 0);
    assert!(!f.backlog(7));
    assert_eq!(f.drain_credit(), 0);
    assert_eq!(f.next_request(4, holds(1)), None);
}

#[test]
fn a_drain_for_a_slot_with_no_session_is_let_go_with_its_backlog() {
    let mut f = Box::new(Flow::new());
    f.want_drain(3);
    assert_eq!(f.next_request(4, |_| None), None);
    assert!(!f.backlog(3));
}

#[test]
fn a_request_that_could_not_be_sent_is_asked_again_with_its_credit_back() {
    let mut f = Box::new(Flow::new());
    f.want_drain(2);
    let req = f.next_request(3, holds(1)).expect("drain");
    assert_eq!(f.drain_credit(), 3);
    f.unsent(req);
    assert_eq!(f.drain_credit(), 0);
    assert_eq!(f.next_request(3, holds(1)), Some(req));

    assert!(f.read_add(5, 1, 0, 0, b"r"));
    let req = f.next_request(0, holds(1)).expect("read");
    f.unsent(req);
    assert_eq!(f.read_credit(), 0);
    assert_eq!(f.next_request(0, holds(1)), Some(req));
}

// ── The whole exchange, saturated ──────────────────────────────────────

const CHECK: u8 = 1;
const ENQUEUE: u8 = 2;
const DRAIN_REQ: u8 = 3;
const READ_REQ: u8 = 4;
const VERDICT: u8 = 11;
const DRAIN_ENV: u8 = 12;
const DRAINED: u8 = 13;
const READ_ANS: u8 = 14;
const READ_END: u8 = 15;

/// A bounded edge of whole frames.
struct EdgeQ {
    q: VecDeque<(u8, Vec<u8>)>,
    cap: usize,
}

impl EdgeQ {
    fn new(cap: usize) -> Self {
        Self {
            q: VecDeque::new(),
            cap,
        }
    }

    fn push(&mut self, mt: u8, p: &[u8]) -> bool {
        if self.q.len() == self.cap {
            return false;
        }
        self.q.push_back((mt, p.to_vec()));
        true
    }
}

fn u32_at(p: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([p[at], p[at + 1], p[at + 2], p[at + 3]])
}

/// `messaging`: its offline queue and retained store, answering on the
/// result edge through its hold.
struct Messaging {
    /// `(session, item)`, oldest first.
    queue: Vec<(u32, u32)>,
    /// Retained entries every filter matches: `(index, item)`.
    retained: Vec<(u32, u32)>,
    hold: BusHold,
}

struct MsgOut<'a> {
    edge: &'a mut EdgeQ,
    queue: &'a mut Vec<(u32, u32)>,
    retained: &'a [(u32, u32)],
}

impl Replies for MsgOut<'_> {
    fn verdict(&mut self, v: &[u8; hold::VERDICT_LEN]) -> bool {
        self.edge.push(VERDICT, v)
    }

    fn drain(&mut self, session: u32, left: &mut u16) -> Option<u32> {
        loop {
            let at = self.queue.iter().position(|(s, _)| *s == session);
            let remaining = self.queue.iter().filter(|(s, _)| *s == session).count();
            if *left == 0 || at.is_none() {
                return Some(u32::try_from(remaining).expect("fits"));
            }
            let at = at.expect("checked");
            let mut p = session.to_le_bytes().to_vec();
            p.extend_from_slice(&self.queue[at].1.to_le_bytes());
            if !self.edge.push(DRAIN_ENV, &p) {
                return None;
            }
            self.queue.remove(at);
            *left -= 1;
        }
    }

    fn read(&mut self, c: &mut ReadCursor) -> Option<u32> {
        let from = c.next;
        for &(i, item) in self.retained.iter().filter(|(i, _)| *i >= from) {
            if c.left == 0 {
                return Some(i);
            }
            let mut p = c.session_slot.to_le_bytes().to_vec();
            p.extend_from_slice(&item.to_le_bytes());
            if !self.edge.push(READ_ANS, &p) {
                c.next = i;
                return None;
            }
            c.left -= 1;
            c.next = i + 1;
        }
        Some(hold::READ_DONE)
    }

    fn marker(&mut self, m: &Marker) -> bool {
        let (mt, a, b) = match *m {
            Marker::Drained { session, remaining } => (DRAINED, session, remaining),
            Marker::ReadEnd { session, next } => (READ_END, session, next),
        };
        let mut p = a.to_le_bytes().to_vec();
        p.extend_from_slice(&b.to_le_bytes());
        self.edge.push(mt, &p)
    }
}

impl Messaging {
    /// One step: what is owed first, then up to `budget` requests while
    /// the hold can take what they may leave owed.
    fn step(&mut self, ops: &mut EdgeQ, results: &mut EdgeQ, budget: usize) {
        let mut out = MsgOut {
            edge: results,
            queue: &mut self.queue,
            retained: &self.retained,
        };
        if !self.hold.resume(&mut out) {
            return;
        }
        for _ in 0..budget {
            if !self.hold.can_read() {
                return;
            }
            let Some((mt, p)) = ops.q.pop_front() else {
                return;
            };
            let mut out = MsgOut {
                edge: results,
                queue: &mut self.queue,
                retained: &self.retained,
            };
            match mt {
                CHECK => {
                    let mut v = [0u8; hold::VERDICT_LEN];
                    v[..4].copy_from_slice(&p[..4]);
                    assert!(self.hold.verdict(&mut out, &v));
                }
                ENQUEUE => out.queue.push((u32_at(&p, 0), u32_at(&p, 4))),
                DRAIN_REQ => {
                    let left = u16::from_le_bytes([p[4], p[5]]);
                    self.hold.start(
                        &mut out,
                        Owed::Drain {
                            session: u32_at(&p, 0),
                            left,
                        },
                    );
                }
                READ_REQ => {
                    let c = ReadCursor::new(
                        0,
                        u32_at(&p, 0),
                        1,
                        b"#",
                        u32_at(&p, 4),
                        u16::from_le_bytes([p[8], p[9]]),
                    )
                    .expect("fits");
                    self.hold.start(&mut out, Owed::Read(c));
                }
                other => panic!("unknown op {other}"),
            }
        }
    }
}

/// The worker: its outbox, its ledger, a delivery park the client drains
/// slowly, and what it is to file. A delivery with no park room joins its
/// session's offline queue (`spill`).
struct Worker {
    ob: Box<Outbox>,
    flow: Box<Flow>,
    park: VecDeque<(u32, u32)>,
    park_cap: usize,
    spilled: u32,
    /// What the client received, per session, in order.
    got: Vec<(u32, u32)>,
    verdicts: Vec<u32>,
    checks_left: VecDeque<u32>,
    fresh_left: VecDeque<(u32, u32)>,
}

impl Worker {
    fn park(&mut self, session: u32, item: u32) {
        assert!(
            self.park.len() < self.park_cap,
            "an answer arrived with nowhere to go"
        );
        self.park.push_back((session, item));
    }

    /// Park slots free beyond those credited to drains.
    fn room(&self) -> u32 {
        u32::try_from(self.park_cap - self.park.len())
            .expect("fits")
            .saturating_sub(self.flow.drain_credit())
    }

    /// Park a delivery while there is room, else queue it for its session
    /// and drain the queue to it in turn.
    fn park_or_spill(&mut self, ops: &mut EdgeQ, session: u32, item: u32) {
        if self.room() > 0 {
            self.park(session, item);
            return;
        }
        let mut p = session.to_le_bytes().to_vec();
        p.extend_from_slice(&item.to_le_bytes());
        assert!(self.ob.send(|mt, p| ops.push(mt, p), ENQUEUE, &p));
        self.flow.want_drain(session);
        self.spilled += 1;
    }

    fn step(&mut self, ops: &mut EdgeQ, results: &mut EdgeQ, acks: usize) {
        self.ob.flush(|mt, p| ops.push(mt, p));
        // The answers are always read.
        for _ in 0..16 {
            let Some((mt, p)) = results.q.pop_front() else {
                break;
            };
            match mt {
                VERDICT => self.verdicts.push(u32_at(&p, 0)),
                DRAIN_ENV => {
                    assert_eq!(self.flow.drain_answer(u32_at(&p, 0), 1), Answer::Deliver);
                    // Credited a park slot.
                    self.park(u32_at(&p, 0), u32_at(&p, 4));
                }
                READ_ANS => {
                    assert_eq!(self.flow.read_answer(u32_at(&p, 0), 1), Answer::Deliver);
                    self.park_or_spill(ops, u32_at(&p, 0), u32_at(&p, 4));
                }
                DRAINED => self.flow.drained(u32_at(&p, 0), u32_at(&p, 4)),
                READ_END => self.flow.read_end(u32_at(&p, 0), u32_at(&p, 4)),
                other => panic!("unknown answer {other}"),
            }
        }
        // The client acknowledges slowly.
        for _ in 0..acks {
            if let Some(d) = self.park.pop_front() {
                self.got.push(d);
            }
        }
        // Reads their pool allows, drains the park can credit.
        while self.ob.free() >= 64 {
            let room = self.room();
            let Some(req) = self.flow.next_request(room, holds(1)) else {
                break;
            };
            let (mt, p) = match req {
                Request::Read { read, max } => {
                    let r = self.flow.read(read);
                    let mut p = r.session.to_le_bytes().to_vec();
                    p.extend_from_slice(&r.next.to_le_bytes());
                    p.extend_from_slice(&max.to_le_bytes());
                    (READ_REQ, p)
                }
                Request::Drain { session, max } => {
                    let mut p = session.to_le_bytes().to_vec();
                    p.extend_from_slice(&max.to_le_bytes());
                    (DRAIN_REQ, p)
                }
            };
            assert!(self.ob.send(|mt, p| ops.push(mt, p), mt, &p));
        }
        // Inputs that file requests, while the outbox has room.
        for _ in 0..8 {
            if self.ob.free() < 1024 {
                break;
            }
            if let Some(c) = self.checks_left.pop_front() {
                assert!(self
                    .ob
                    .send(|mt, p| ops.push(mt, p), CHECK, &c.to_le_bytes()));
            }
            // A fresh delivery: behind its session's backlog, else parked
            // or queued — it is always taken.
            if let Some((session, item)) = self.fresh_left.pop_front() {
                if self.flow.backlog(session) {
                    let mut p = session.to_le_bytes().to_vec();
                    p.extend_from_slice(&item.to_le_bytes());
                    assert!(self.ob.send(|mt, p| ops.push(mt, p), ENQUEUE, &p));
                    self.flow.joined(session);
                } else {
                    self.park_or_spill(ops, session, item);
                }
            }
        }
    }
}

#[test]
fn the_exchange_saturated_both_ways_loses_nothing_reorders_nothing_and_drains() {
    const SESSION: u32 = 7;
    const READER: u32 = 9;
    let mut ops = EdgeQ::new(3);
    let mut results = EdgeQ::new(2);
    let mut m = Messaging {
        // Session 7's backlog: items 1..=40.
        queue: (1..=40).map(|i| (SESSION, i)).collect(),
        // Session 9 subscribes to `#` over 30 retained entries.
        retained: (0..30).map(|i| (i * 3, 1000 + i)).collect(),
        hold: BusHold::new(),
    };
    let mut w = Worker {
        ob: Box::new(Outbox::new()),
        flow: Box::new(Flow::new()),
        park: VecDeque::new(),
        park_cap: 4,
        spilled: 0,
        got: Vec::new(),
        verdicts: Vec::new(),
        checks_left: (0..500).collect(),
        // Session 7's fresh deliveries, items 41..=60, arrive while its
        // backlog drains.
        fresh_left: (41..=60).map(|i| (SESSION, i)).collect(),
    };
    w.flow.want_drain(SESSION);
    assert!(w.flow.read_add(READER, 1, 0, 1, b"#"));
    let mut steps = 0;
    loop {
        w.step(&mut ops, &mut results, 1);
        m.step(&mut ops, &mut results, 8);
        steps += 1;
        let done = w.checks_left.is_empty()
            && w.fresh_left.is_empty()
            && w.ob.is_empty()
            && ops.q.is_empty()
            && results.q.is_empty()
            && m.hold.is_clear()
            && w.park.is_empty()
            && m.queue.is_empty()
            && !w.flow.backlog(SESSION)
            && !w.flow.backlog(READER)
            && w.flow.reads_free() == READ_SLOTS;
        if done {
            break;
        }
        assert!(steps < 20_000, "the exchange must not deadlock");
    }
    assert_eq!(
        w.verdicts,
        (0..500).collect::<Vec<_>>(),
        "every check answered once, in order"
    );
    let mine: Vec<u32> = w
        .got
        .iter()
        .filter(|(s, _)| *s == SESSION)
        .map(|&(_, i)| i)
        .collect();
    assert_eq!(
        mine,
        (1..=60).collect::<Vec<_>>(),
        "the backlog, then the fresh ones, in order"
    );
    let mut retained: Vec<u32> = w
        .got
        .iter()
        .filter(|(s, _)| *s == READER)
        .map(|&(_, i)| i)
        .collect();
    // A retained set arrives in any order.
    retained.sort_unstable();
    assert_eq!(
        retained,
        (1000..1030).collect::<Vec<_>>(),
        "every retained entry, once"
    );
    assert_eq!(w.ob.refused, 0);
    assert_eq!((w.flow.read_credit(), w.flow.drain_credit()), (0, 0));
    assert_eq!(w.flow.unasked, 0);
    assert!(w.ob.waited > 0 && m.hold.held > 0, "both edges did fill");
    assert!(w.spilled > 0, "the park did fill");
}

#[test]
fn a_retained_read_finishes_with_the_park_full_and_no_client_acknowledging() {
    // Nothing the apply path waits on — a free read slot — may wait on a
    // client: a client's acknowledgement can sit behind a packet that
    // waits on a commit.
    const STUCK: u32 = 5;
    const READER: u32 = 9;
    let mut ops = EdgeQ::new(3);
    let mut results = EdgeQ::new(2);
    let mut m = Messaging {
        queue: Vec::new(),
        retained: (0..30).map(|i| (i, 2000 + i)).collect(),
        hold: BusHold::new(),
    };
    let mut w = Worker {
        ob: Box::new(Outbox::new()),
        flow: Box::new(Flow::new()),
        park: VecDeque::new(),
        park_cap: 2,
        spilled: 0,
        got: Vec::new(),
        verdicts: Vec::new(),
        checks_left: VecDeque::new(),
        // A session that never acknowledges fills the park.
        fresh_left: (0..2).map(|i| (STUCK, i)).collect(),
    };
    assert!(w.flow.read_add(READER, 1, 0, 1, b"#"));
    let mut steps = 0;
    while w.flow.reads_free() < READ_SLOTS || !w.ob.is_empty() || !ops.q.is_empty() {
        w.step(&mut ops, &mut results, 0);
        m.step(&mut ops, &mut results, 8);
        steps += 1;
        assert!(steps < 2_000, "the read must finish without the client");
    }
    assert!(w.got.is_empty(), "nothing was acknowledged");
    assert_eq!(w.park.len(), 2);
    let mut queued: Vec<u32> = m
        .queue
        .iter()
        .filter(|(s, _)| *s == READER)
        .map(|&(_, i)| i)
        .collect();
    queued.sort_unstable();
    assert_eq!(
        queued,
        (2000..2030).collect::<Vec<_>>(),
        "every answer is in the reader's queue, to be drained to it"
    );
    assert!(w.flow.backlog(READER));
    assert_eq!(w.flow.read_credit(), 0);
}
