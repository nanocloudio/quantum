//! `messaging`'s per-bus hold (`messaging_hold_core.rs`): a dedup verdict
//! its result edge had no room for waits in order while the bus goes on
//! being read; a retained read or an offline drain answers at most its
//! request's credit, ends with a marker saying where it stopped, and one
//! the edge cuts short is kept where it stopped — marker included — and
//! finished before the bus is read again. Driven here the way
//! `messaging`'s step drives it, against an edge whose room comes and goes.
//!
//! The failures prevented: a dedup verdict dropped on a full edge, which
//! leaves the committed publish it resolves stuck in its stash and never
//! fanned out; the bus paused on every full edge, which backs up the op
//! edge the worker files its dedup checks on; a read or drain answering
//! past its credit, cut short and never finished, or ending without its
//! marker; verdicts leaving out of order; a second read or drain started
//! over the one unfinished.

use super::messaging_hold::*;

/// A result edge with room for `room` more frames, recording each frame
/// written; the drain and read sources it answers from.
struct Edge {
    room: usize,
    written: Vec<Vec<u8>>,
    /// Envelopes queued per session, in sequence order.
    queued: Vec<(u32, Vec<u8>)>,
    /// Retained entries the read matches: `(index, frame)`.
    entries: Vec<(u32, Vec<u8>)>,
}

impl Edge {
    fn new(room: usize) -> Self {
        Self {
            room,
            written: Vec::new(),
            queued: Vec::new(),
            entries: Vec::new(),
        }
    }

    fn take(&mut self, frame: &[u8]) -> bool {
        if self.room == 0 {
            return false;
        }
        self.room -= 1;
        self.written.push(frame.to_vec());
        true
    }
}

/// How a marker is recorded on the edge.
fn marker_frame(m: &Marker) -> Vec<u8> {
    match *m {
        Marker::Drained { session, remaining } => {
            let mut v = vec![0xD0];
            v.extend_from_slice(&session.to_le_bytes());
            v.extend_from_slice(&remaining.to_le_bytes());
            v
        }
        Marker::ReadEnd { session, next } => {
            let mut v = vec![0xE0];
            v.extend_from_slice(&session.to_le_bytes());
            v.extend_from_slice(&next.to_le_bytes());
            v
        }
    }
}

impl Replies for Edge {
    fn verdict(&mut self, v: &[u8; VERDICT_LEN]) -> bool {
        self.take(v)
    }

    fn drain(&mut self, session: u32, left: &mut u16) -> Option<u32> {
        loop {
            let remaining = self.queued.iter().filter(|(s, _)| *s == session).count();
            if *left == 0 || remaining == 0 {
                return Some(u32::try_from(remaining).expect("fits"));
            }
            let at = self
                .queued
                .iter()
                .position(|(s, _)| *s == session)
                .expect("one queued");
            let env = self.queued[at].1.clone();
            if !self.take(&env) {
                return None;
            }
            self.queued.remove(at);
            *left -= 1;
        }
    }

    fn read(&mut self, c: &mut ReadCursor) -> Option<u32> {
        let pending: Vec<(u32, Vec<u8>)> = self
            .entries
            .iter()
            .filter(|(i, _)| *i >= c.next)
            .cloned()
            .collect();
        for (i, frame) in pending {
            if c.left == 0 {
                return Some(i);
            }
            if !self.take(&frame) {
                c.next = i;
                return None;
            }
            c.left -= 1;
            c.next = i + 1;
        }
        Some(READ_DONE)
    }

    fn marker(&mut self, m: &Marker) -> bool {
        self.take(&marker_frame(m))
    }
}

fn verdict(tag: u8) -> [u8; VERDICT_LEN] {
    let mut v = [tag; VERDICT_LEN];
    v[20] = 0;
    v
}

/// `messaging`'s step for one bus over the dedup checks `requests`: what
/// is owed goes first, and a request is read only while the hold can
/// take what it may leave owed. Returns how many requests were read.
fn step(hold: &mut BusHold, edge: &mut Edge, requests: &mut Vec<[u8; VERDICT_LEN]>) -> usize {
    if !hold.resume(edge) {
        return 0;
    }
    let mut read = 0;
    while !requests.is_empty() && hold.can_read() {
        let v = requests.remove(0);
        read += 1;
        assert!(hold.verdict(edge, &v), "read only with room to wait");
    }
    read
}

#[test]
fn a_verdict_the_edge_refused_waits_and_is_written_once_it_has_room() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(0);
    assert!(hold.verdict(&mut edge, &verdict(7)));
    assert_eq!(hold.verdicts_held(), 1);
    assert!(hold.can_read(), "the bus goes on being read");
    hold.resume(&mut edge);
    assert!(edge.written.is_empty());
    edge.room = 1;
    hold.resume(&mut edge);
    assert!(hold.is_clear());
    assert_eq!(
        edge.written,
        vec![verdict(7).to_vec()],
        "written whole, once"
    );
    assert_eq!(hold.held, 1);
}

#[test]
fn a_verdict_waits_behind_those_already_waiting() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(0);
    assert!(hold.verdict(&mut edge, &verdict(1)));
    edge.room = 5;
    assert!(hold.verdict(&mut edge, &verdict(2)));
    assert!(edge.written.is_empty(), "not ahead of the one waiting");
    hold.resume(&mut edge);
    assert_eq!(edge.written, vec![verdict(1).to_vec(), verdict(2).to_vec()]);
}

#[test]
fn verdicts_leave_in_request_order_however_the_edge_room_comes() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(2);
    let mut requests: Vec<_> = (1..=200u8).map(verdict).collect();
    let want: Vec<Vec<u8>> = requests.iter().map(|v| v.to_vec()).collect();
    let mut steps = 0;
    while !requests.is_empty() || !hold.is_clear() {
        step(&mut hold, &mut edge, &mut requests);
        // The worker reads a few verdicts back between steps.
        edge.room += 3;
        steps += 1;
        assert!(steps < 1000, "the bus drains");
    }
    assert_eq!(edge.written, want, "every verdict, once, in order");
    assert!(hold.held > 0, "the edge did fill");
}

#[test]
fn a_full_verdict_queue_stops_the_bus_until_one_goes() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(0);
    let mut requests: Vec<_> = (0..u8::try_from(HOLD_VERDICTS + 5).expect("fits"))
        .map(verdict)
        .collect();
    assert_eq!(step(&mut hold, &mut edge, &mut requests), HOLD_VERDICTS);
    assert!(!hold.can_read());
    assert!(
        !hold.verdict(&mut edge, &verdict(0xEE)),
        "never past the bound"
    );
    assert_eq!(
        step(&mut hold, &mut edge, &mut requests),
        0,
        "the rest wait in the op edge"
    );
    edge.room = 1;
    assert_eq!(step(&mut hold, &mut edge, &mut requests), 1);
    assert_eq!(edge.written, vec![verdict(0).to_vec()]);
}

#[test]
fn one_read_or_drain_is_unfinished_at_a_time_and_stops_the_bus() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(0);
    edge.queued = vec![(4, vec![1])];
    assert!(hold.start(
        &mut edge,
        Owed::Drain {
            session: 4,
            left: 4
        }
    ));
    assert!(!hold.can_read());
    let c = ReadCursor::new(1, 2, 1, b"a", 0, 4).expect("fits");
    assert!(!hold.start(&mut edge, Owed::Read(c)));
    assert_eq!(
        hold.owed,
        Owed::Drain {
            session: 4,
            left: 4
        },
        "the first is kept"
    );
    assert_eq!(hold.held, 1);
}

#[test]
fn a_drain_answers_at_most_its_credit_and_says_how_many_are_left() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(10);
    edge.queued = vec![(4, vec![1]), (9, vec![50]), (4, vec![2]), (4, vec![3])];
    assert!(hold.start(
        &mut edge,
        Owed::Drain {
            session: 4,
            left: 2
        }
    ));
    assert!(hold.is_clear(), "finished at once with room");
    let drained = marker_frame(&Marker::Drained {
        session: 4,
        remaining: 1,
    });
    assert_eq!(edge.written, vec![vec![1], vec![2], drained]);
    assert_eq!(edge.queued, vec![(9, vec![50]), (4, vec![3])]);
}

#[test]
fn a_drain_cut_short_is_finished_in_sequence_order_and_then_marked() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(1);
    edge.queued = vec![(4, vec![1]), (9, vec![50]), (4, vec![2]), (4, vec![3])];
    assert!(hold.start(
        &mut edge,
        Owed::Drain {
            session: 4,
            left: 8
        }
    ));
    assert!(!hold.can_read(), "cut short: owed");
    edge.room = 2;
    assert!(!hold.resume(&mut edge), "the marker still waits");
    edge.room = 1;
    assert!(hold.resume(&mut edge));
    let drained = marker_frame(&Marker::Drained {
        session: 4,
        remaining: 0,
    });
    assert_eq!(edge.written, vec![vec![1], vec![2], vec![3], drained]);
    assert_eq!(edge.queued, vec![(9, vec![50])], "another session's stays");
}

#[test]
fn a_read_answers_its_credit_then_says_where_to_resume() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(10);
    edge.entries = vec![(3, vec![3]), (8, vec![8]), (11, vec![11]), (40, vec![40])];
    let c = ReadCursor::new(1, 2, 1, b"a/#", 0, 2).expect("fits");
    assert!(hold.start(&mut edge, Owed::Read(c)));
    let end = marker_frame(&Marker::ReadEnd {
        session: 2,
        next: 11,
    });
    assert_eq!(edge.written, vec![vec![3], vec![8], end]);
    // Asked again from there, it finishes the scan.
    edge.written.clear();
    let c = ReadCursor::new(1, 2, 1, b"a/#", 11, 4).expect("fits");
    assert!(hold.start(&mut edge, Owed::Read(c)));
    let end = marker_frame(&Marker::ReadEnd {
        session: 2,
        next: READ_DONE,
    });
    assert_eq!(edge.written, vec![vec![11], vec![40], end]);
}

#[test]
fn a_read_cut_short_resumes_at_the_entry_it_had_no_room_for() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(2);
    edge.entries = vec![(3, vec![3]), (8, vec![8]), (11, vec![11]), (40, vec![40])];
    let c = ReadCursor::new(1, 2, 1, b"a/#", 0, 8).expect("fits");
    assert!(hold.start(&mut edge, Owed::Read(c)));
    match hold.owed {
        Owed::Read(c) => assert_eq!((c.next, c.left), (11, 6)),
        other => panic!("owed {other:?}"),
    }
    edge.room = 1;
    assert!(!hold.resume(&mut edge));
    edge.room = 2;
    assert!(hold.resume(&mut edge));
    let end = marker_frame(&Marker::ReadEnd {
        session: 2,
        next: READ_DONE,
    });
    assert_eq!(
        edge.written,
        vec![vec![3], vec![8], vec![11], vec![40], end]
    );
}

#[test]
fn waiting_verdicts_go_before_a_read_resumes() {
    let mut hold = BusHold::new();
    let mut edge = Edge::new(0);
    assert!(hold.verdict(&mut edge, &verdict(9)));
    edge.entries = vec![(0, vec![0xAA])];
    let c = ReadCursor::new(1, 2, 1, b"#", 0, 4).expect("fits");
    assert!(hold.start(&mut edge, Owed::Read(c)));
    edge.room = 3;
    assert!(hold.resume(&mut edge));
    let end = marker_frame(&Marker::ReadEnd {
        session: 2,
        next: READ_DONE,
    });
    assert_eq!(edge.written, vec![verdict(9).to_vec(), vec![0xAA], end]);
}

#[test]
fn a_read_cursor_keeps_its_request() {
    let c = ReadCursor::new(5, 6, 2, b"x/+/y", 7, 3).expect("fits");
    assert_eq!(
        (c.tenant, c.session_slot, c.sub_qos, c.next, c.left),
        (5, 6, 2, 7, 3)
    );
    assert_eq!(c.pattern(), b"x/+/y");
    assert!(ReadCursor::new(0, 0, 0, &[b'a'; PATTERN_MAX], 0, 1).is_some());
    assert!(ReadCursor::new(0, 0, 0, &[b'a'; PATTERN_MAX + 1], 0, 1).is_none());
}
