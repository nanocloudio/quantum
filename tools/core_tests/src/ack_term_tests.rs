//! Whether the entry a durable publish was assigned is the one committed at
//! its index (`ack_term_core.rs`).
//!
//! The failure prevented: an entry appended at `(index, term)` that a new
//! leader overwrote before it committed — a deposed leader's uncommitted
//! tail, or a follower-forwarded write whose leader then failed — answered
//! PUBACK / `ProduceResponse` / Basic.Ack because the new log's commit
//! horizon passed its index. Also: a registration or run misread, a stale
//! horizon frame moving the view back, and an entry the node cannot place
//! acknowledged rather than failed.

use super::ack_term::*;
use super::proposal_refusal as refusal;

fn reg(pid: u16, index: u64, term: u64) -> Registration {
    Registration {
        session_slot: 7,
        message_id: 42,
        partition_id: pid,
        wal_index: index,
        session_generation: 3,
        term,
    }
}

fn run(first: u64, last: u64, term: u64) -> Run {
    Run { first, last, term }
}

#[test]
fn a_registration_round_trips_and_names_an_entry_or_nothing() {
    let r = reg(5, 0x0102_0304_0506_0708, 9);
    let b = encode_register(&r);
    assert_eq!(b.len(), REGISTER_LEN);
    assert_eq!(decode_register(&b), Some(r));
    // The term sits at the end, after the generation.
    assert_eq!(&b[22..30], &9u64.to_le_bytes());
    // Short, index 0 and term 0 name no entry.
    assert_eq!(decode_register(&b[..REGISTER_LEN - 1]), None);
    assert_eq!(decode_register(&encode_register(&reg(5, 0, 9))), None);
    assert_eq!(decode_register(&encode_register(&reg(5, 4, 0))), None);
}

#[test]
fn an_applied_run_round_trips_and_an_empty_one_is_refused() {
    let b = encode_applied(3, run(10, 20, 4));
    assert_eq!(b.len(), APPLIED_LEN);
    assert_eq!(decode_applied(&b), Some((3, run(10, 20, 4))));
    assert_eq!(decode_applied(&b[..APPLIED_LEN - 1]), None);
    assert_eq!(decode_applied(&encode_applied(3, run(0, 2, 4))), None);
    assert_eq!(decode_applied(&encode_applied(3, run(5, 4, 4))), None);
    assert_eq!(decode_applied(&encode_applied(3, run(5, 6, 0))), None);
}

#[test]
fn a_completion_carries_its_verdict_in_refusal_terms() {
    let b = encode_emit(1, 2, 3, 99, STATUS_SUPERSEDED);
    assert_eq!(b.len(), EMIT_LEN);
    assert_eq!(&b[12..20], &99u64.to_le_bytes());
    assert_eq!(emit_status(&b), Some(STATUS_SUPERSEDED));
    assert_eq!(emit_status(&b[..EMIT_LEN - 1]), None);
    // The failure statuses ARE Clustor's refusal codes, so one mapping
    // fails a refused proposal and a superseded entry alike.
    assert_eq!(STATUS_SUPERSEDED, refusal::REFUSED_NOT_LEADER);
    assert_eq!(STATUS_UNKNOWN, refusal::REFUSED_OUTCOME_UNKNOWN);
    assert_eq!(Verdict::Wait.status(), None);
    assert_eq!(Verdict::Committed.status(), Some(STATUS_COMMITTED));
    assert_eq!(Verdict::Superseded.status(), Some(STATUS_SUPERSEDED));
    assert_eq!(Verdict::Unknown.status(), Some(STATUS_UNKNOWN));
}

#[test]
fn the_horizon_commits_an_entry_of_its_own_term_and_no_other() {
    let mut p = PartitionTerms::new();
    assert_eq!(p.resolve(5, 2), Verdict::Wait);
    p.note_horizon(4, 2);
    assert_eq!(p.resolve(5, 2), Verdict::Wait, "not reached yet");
    p.note_horizon(5, 2);
    assert_eq!(p.resolve(5, 2), Verdict::Committed);
    assert_eq!(p.resolve(3, 2), Verdict::Committed);
    // Reached, but by a horizon of another term: no verdict from it.
    assert_eq!(p.resolve(5, 1), Verdict::Wait);
    p.note_horizon(9, 3);
    assert_eq!(p.resolve(5, 2), Verdict::Wait);
}

#[test]
fn a_stale_horizon_frame_never_moves_the_view_back() {
    let mut p = PartitionTerms::new();
    p.note_horizon(10, 4);
    p.note_horizon(8, 3);
    assert_eq!(p.horizon(), (10, 4));
    p.note_horizon(10, 3);
    assert_eq!(p.horizon(), (10, 4));
    p.note_horizon(10, 5);
    assert_eq!(p.horizon(), (10, 5));
}

#[test]
fn an_applied_run_commits_its_term_once_the_horizon_reached_it() {
    let mut p = PartitionTerms::new();
    p.note_applied(run(1, 6, 2));
    // Applied (committed) at term 2, but no horizon yet: durability first.
    assert_eq!(p.resolve(4, 2), Verdict::Wait);
    // The horizon reaches it in a later term: the run decides.
    p.note_horizon(6, 3);
    assert_eq!(p.resolve(4, 2), Verdict::Committed);
}

#[test]
fn an_applied_run_of_another_term_supersedes_at_once() {
    let mut p = PartitionTerms::new();
    p.note_applied(run(3, 5, 3));
    // No horizon needed: another entry holds the index for good.
    assert_eq!(p.resolve(4, 2), Verdict::Superseded);
    assert_eq!(p.resolve(6, 2), Verdict::Wait, "not applied yet");
}

#[test]
fn an_index_applied_past_without_a_run_naming_its_term_is_unknown() {
    let mut p = PartitionTerms::new();
    p.note_applied(run(1, 3, 1));
    // A notice for 4..=6 was lost; 7.. arrived.
    p.note_applied(run(7, 9, 2));
    p.note_horizon(9, 2);
    assert_eq!(p.resolve(5, 1), Verdict::Unknown);
    assert_eq!(p.resolve(10, 1), Verdict::Wait);
}

#[test]
fn runs_past_the_ring_are_forgotten_and_resolve_unknown() {
    let mut p = PartitionTerms::new();
    for t in 1..=(RUNS as u64 + 1) {
        p.note_applied(run(t * 10, t * 10 + 9, t));
    }
    p.note_horizon(1000, 99);
    // Term 1's run (10..=19) was evicted; term 2's is held.
    assert_eq!(p.resolve(15, 1), Verdict::Unknown);
    assert_eq!(p.resolve(25, 2), Verdict::Committed);
    assert_eq!(p.resolve(25, 1), Verdict::Superseded);
}

#[test]
fn runs_of_one_term_merge_and_disagreeing_runs_are_not_trusted() {
    let mut p = PartitionTerms::new();
    p.note_applied(run(1, 4, 2));
    p.note_applied(run(5, 8, 2));
    p.note_applied(run(3, 6, 2)); // a replay after an apply reset
    p.note_horizon(8, 2);
    assert_eq!(p.applied(), 8);
    assert_eq!(p.resolve(7, 2), Verdict::Committed);
    // Two runs naming different terms at one index: unknown, not trusted.
    let mut q = PartitionTerms::new();
    q.note_applied(run(1, 4, 2));
    q.note_applied(run(4, 6, 3));
    q.note_horizon(6, 4);
    assert_eq!(q.resolve(4, 2), Verdict::Unknown);
    assert_eq!(q.resolve(4, 3), Verdict::Unknown);
    assert_eq!(q.resolve(2, 2), Verdict::Committed);
}

#[test]
fn the_tracker_extends_a_run_and_closes_it_on_a_term_change() {
    let mut t = RunTracker::new();
    assert_eq!(t.owed(), None);
    assert_eq!(t.note(1, 1), None);
    assert_eq!(t.note(2, 1), None);
    assert_eq!(t.owed(), Some(run(1, 2, 1)));
    t.mark_sent(2);
    assert_eq!(t.owed(), None);
    assert_eq!(t.note(3, 1), None);
    // A term change closes the run with its unsent tail still owed.
    assert_eq!(t.note(4, 2), Some(run(1, 3, 1)));
    assert_eq!(t.owed(), Some(run(4, 4, 2)));
    t.mark_sent(4);
    // A closed run fully sent is not owed again; a jump opens a new run.
    assert_eq!(t.note(9, 2), None);
    assert_eq!(t.owed(), Some(run(9, 9, 2)));
    assert_eq!(t.note(0, 2), None);
    assert_eq!(t.note(10, 0), None);
}

#[test]
fn the_spill_holds_closed_runs_in_order_and_refuses_when_full() {
    let mut s = Spill::new();
    assert!(s.is_empty());
    for i in 0..SPILL as u64 {
        assert!(s.push(1, run(i + 1, i + 1, 1)));
    }
    assert!(!s.push(1, run(99, 99, 1)));
    assert_eq!(s.len(), SPILL);
    assert_eq!(s.front(), Some((1, run(1, 1, 1))));
    s.pop_front();
    assert_eq!(s.front(), Some((1, run(2, 2, 1))));
    while !s.is_empty() {
        s.pop_front();
    }
    assert_eq!(s.front(), None);
}

// ── The two modules together: an overwrite, end to end ─────────────────

/// `session_processor`'s side, as `note_applied_entry` and
/// `flush_applied_runs` drive the core: track each partition's applied
/// run, spill closed runs, and send what is owed as `MSG_ACK_APPLIED`
/// bodies.
struct Session {
    runs: Vec<RunTracker>,
    spill: Spill,
    bus: Vec<[u8; APPLIED_LEN]>,
}

impl Session {
    fn new(parts: usize) -> Self {
        Self {
            runs: vec![RunTracker::new(); parts],
            spill: Spill::new(),
            bus: Vec::new(),
        }
    }

    fn apply(&mut self, pid: u16, index: u64, term: u64) {
        if let Some(closed) = self.runs[usize::from(pid)].note(index, term) {
            assert!(self.spill.push(pid, closed));
        }
    }

    fn flush(&mut self) {
        while let Some((pid, r)) = self.spill.front() {
            self.bus.push(encode_applied(pid, r));
            self.spill.pop_front();
        }
        for (pid, t) in self.runs.iter_mut().enumerate() {
            if let Some(r) = t.owed() {
                self.bus.push(encode_applied(
                    u16::try_from(pid).expect("a partition id"),
                    r,
                ));
                t.mark_sent(r.last);
            }
        }
    }
}

/// `flow`'s ack component, as `on_register` / `on_applied` /
/// `on_durability` / `step_acks` drive the core: the inflight table and
/// one view per partition; a sweep emits each resolved entry's
/// `MSG_ACK_EMIT` body.
struct Flow {
    entries: Vec<Registration>,
    parts: Vec<PartitionTerms>,
}

impl Flow {
    fn new(parts: usize) -> Self {
        Self {
            entries: Vec::new(),
            parts: vec![PartitionTerms::new(); parts],
        }
    }

    fn register(&mut self, body: &[u8]) {
        self.entries
            .push(decode_register(body).expect("a registration"));
    }

    fn drain(&mut self, s: &mut Session) {
        for body in s.bus.drain(..) {
            let (pid, r) = decode_applied(&body).expect("a run");
            self.parts[usize::from(pid)].note_applied(r);
        }
    }

    fn horizon(&mut self, pid: u16, index: u64, term: u64) {
        self.parts[usize::from(pid)].note_horizon(index, term);
    }

    /// One `step_acks` sweep: `(wal_index, status)` per entry resolved.
    fn sweep(&mut self) -> Vec<(u64, u8)> {
        let mut out = Vec::new();
        let parts = &self.parts;
        self.entries.retain(|e| {
            let Some(status) = parts[usize::from(e.partition_id)]
                .resolve(e.wal_index, e.term)
                .status()
            else {
                return true;
            };
            let b = encode_emit(
                e.session_slot,
                e.message_id,
                e.session_generation,
                e.wal_index,
                status,
            );
            out.push((e.wal_index, emit_status(&b).expect("a status")));
            false
        });
        out
    }
}

/// Raft's overwrite: the leader of term 2 appends 3..=5 (the publish at 5)
/// and is deposed before they commit; the leader of term 3 writes its own
/// 3..=6 and commits them. The new log's horizon passes index 5 — what the
/// term-blind rule acked on — but the entry there is term 3's.
#[test]
fn an_entry_a_new_leader_overwrote_is_failed_and_never_acked() {
    let mut session = Session::new(2);
    let mut flow = Flow::new(2);
    // Committed before the change, under term 2's leader.
    session.apply(1, 1, 2);
    session.apply(1, 2, 2);
    flow.horizon(1, 2, 2);
    // The publish appended at (5, 2): MSG_PROPOSAL_ASSIGNED → register.
    flow.register(&encode_register(&reg(1, 5, 2)));
    // A publish the new leader appended at (6, 3).
    flow.register(&encode_register(&reg(1, 6, 3)));
    assert!(flow.sweep().is_empty());

    // Term 3 commits its own 3..=6. Its horizon reaches index 6 first.
    flow.horizon(1, 6, 3);
    let swept = flow.sweep();
    // Term 3's own entry is committed by the horizon at once; the
    // overwritten one is NOT acked — the horizon's term is not its own.
    assert_eq!(swept, vec![(6, STATUS_COMMITTED)]);

    // The entries apply; the runs reach flow: 1..=2 at term 2, 3..=6 at 3.
    for i in 3..=6 {
        session.apply(1, i, 3);
    }
    session.flush();
    flow.drain(&mut session);
    assert_eq!(flow.sweep(), vec![(5, STATUS_SUPERSEDED)]);
    // Which the session state machine answers as a refusal: DISCONNECT so
    // the MQTT client resends, NOT_LEADER_OR_FOLLOWER to a producer.
    assert_eq!(
        refusal::mqtt_disconnect_reason(STATUS_SUPERSEDED),
        refusal::MQTT_REASON_SERVER_BUSY
    );
    assert_eq!(
        refusal::kafka_produce_error(STATUS_SUPERSEDED),
        refusal::KERR_NOT_LEADER_OR_FOLLOWER
    );
    assert!(flow.entries.is_empty());
}

/// The other side of the same leader change: an entry of term 2 that term
/// 3's leader DID carry into its log commits and is acked — once the
/// applied runs name its term, since the horizon's cannot.
#[test]
fn an_older_terms_entry_the_new_leader_committed_is_acked() {
    let mut session = Session::new(1);
    let mut flow = Flow::new(1);
    flow.register(&encode_register(&reg(0, 4, 2)));
    for i in 1..=4 {
        session.apply(0, i, 2);
    }
    for i in 5..=6 {
        session.apply(0, i, 3);
    }
    flow.horizon(0, 6, 3);
    assert!(flow.sweep().is_empty(), "the horizon alone cannot say");
    session.flush();
    flow.drain(&mut session);
    assert_eq!(flow.sweep(), vec![(4, STATUS_COMMITTED)]);
}

/// A follower-forwarded write: the follower registers the `(index, term)`
/// the leader appended it at, and its own horizon (its commit index in its
/// own current term) acks it when the terms agree — and a leader failure
/// that left the index to another term fails it the same way.
#[test]
fn a_forwarded_write_resolves_on_the_followers_own_view() {
    let mut session = Session::new(1);
    let mut flow = Flow::new(1);
    flow.register(&encode_register(&reg(0, 10, 4)));
    flow.horizon(0, 10, 4);
    assert_eq!(flow.sweep(), vec![(10, STATUS_COMMITTED)]);

    flow.register(&encode_register(&reg(0, 11, 4)));
    // Term 4's leader failed; term 5's committed 11..=12 of its own.
    for i in 1..=10 {
        session.apply(0, i, 4);
    }
    session.apply(0, 11, 5);
    session.apply(0, 12, 5);
    flow.horizon(0, 12, 5);
    assert!(flow.sweep().is_empty());
    session.flush();
    flow.drain(&mut session);
    assert_eq!(flow.sweep(), vec![(11, STATUS_SUPERSEDED)]);
}

/// Partitions never vouch for each other: partition 1's runs and horizon
/// say nothing about an entry registered on partition 0.
#[test]
fn one_partitions_log_never_resolves_anothers_entry() {
    let mut session = Session::new(2);
    let mut flow = Flow::new(2);
    flow.register(&encode_register(&reg(0, 3, 1)));
    for i in 1..=5 {
        session.apply(1, i, 2);
    }
    session.flush();
    flow.drain(&mut session);
    flow.horizon(1, 5, 1);
    assert!(flow.sweep().is_empty());
    flow.horizon(0, 3, 1);
    assert_eq!(flow.sweep(), vec![(3, STATUS_COMMITTED)]);
}
