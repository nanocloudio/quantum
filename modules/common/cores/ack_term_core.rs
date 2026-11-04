// Bounded, no_std, no-alloc ACK TERM core — whether the entry a durable
// publish was assigned is the entry the log committed at that index.
// `include!`d by `flow` (the resolver), by `session_processor` (the
// applied-run tracker that feeds it) and by the host test crate; it names
// nothing outside this file.
//
// A Raft index names an entry only together with its term. Clustor answers
// a tagged proposal with MSG_PROPOSAL_ASSIGNED `[correlation][partition]
// [index][term]`: the entry was APPENDED at `(index, term)`. If that
// term's leader is deposed before the entry commits, the next leader may
// put a different entry at the same index — and, since Clustor forwards a
// follower's writes to the leader, the node waiting on the entry need not
// be the one that appended it. The log's progress past `index` therefore
// proves nothing about the entry on its own: acknowledging on it would tell
// a client "committed" (MQTT PUBACK, Kafka ProduceResponse, AMQP confirm)
// for a write no log holds.
//
// So every registration carries its term, and is resolved against two
// facts the broker sees:
//
//   - the COMMIT HORIZON `(index I, term T)` on `flow.durability_in`
//     (`consensus.durable_horizon`: this node's commit index and its
//     current term). With `I >= i` and `T == t` the entry IS committed:
//     the node's committed entry at `I` has a term at most `T`; if it is
//     `t`, log matching puts `(i, t)` beneath it; if it were lower, the
//     entry at `I` would have been committed either before term `t` began
//     (so term `t`'s leader held it and appended past it, not at `i <= I`)
//     or by term `t`'s leader itself (whose log holds `(i, t)` beneath
//     `I`, so the entry at `I` is of term `t` or later). Any other `T` is
//     no verdict: the horizon cannot name the term at `i`.
//   - the APPLIED RUNS: `session_processor` sees every committed entry with
//     its exact `(partition, term, index)` and tells `flow` which terms the
//     committed log holds over which index ranges (MSG_ACK_APPLIED
//     `[partition][first][last][term]`). An applied run that covers `i`
//     names the committed term there: `t` is committed (acknowledged once
//     the horizon has also reached `i`, so an ack still follows quorum
//     durability), any other term means another entry took the index —
//     SUPERSEDED, and the operation fails so the client retries. An index
//     the node has applied past but holds no run for (the run was evicted,
//     or its notice lost) is UNKNOWN, and fails the same way: a client
//     retry is always correct, an unproven ack never is.
//
// The verdict travels back on MSG_ACK_EMIT's status byte, in Clustor's
// refusal vocabulary (`proposal_refusal_core.rs`): `0` committed,
// `REFUSED_NOT_LEADER` superseded, `REFUSED_OUTCOME_UNKNOWN` unknown.
//
// NOT a sufficient fast path: a quorum-durability proof
// (`durability.quorum_durable`) whose term equals the entry's. Its term is
// the lowest durable tip among the supporting replicas, and an entry
// durable on a quorum can still be overwritten when it is not of the
// counting leader's term (Raft §5.4.2). `flow.durability_in` therefore
// carries the commit horizon, never the ledger's proof.

/// MSG_ACK_REGISTER body: `[session_slot:u32][message_id:u32]
/// [partition_id:u16][wal_index:u64][session_generation:u32][term:u64]`, LE.
pub const REGISTER_LEN: usize = 30;
/// MSG_ACK_APPLIED body: `[partition_id:u16][first:u64][last:u64][term:u64]`, LE.
pub const APPLIED_LEN: usize = 26;
/// MSG_ACK_EMIT body: `[session_slot:u32][message_id:u32]
/// [session_generation:u32][wal_index:u64][status:u8]`, LE.
pub const EMIT_LEN: usize = 21;

/// MSG_ACK_EMIT status: the entry is committed at its `(index, term)`.
pub const STATUS_COMMITTED: u8 = 0x00;
/// MSG_ACK_EMIT status: another term's entry was committed at the index —
/// the write is in no log (Clustor's `CLIENT_REJECT_NOT_LEADER`).
pub const STATUS_SUPERSEDED: u8 = 0x02;
/// MSG_ACK_EMIT status: the node applied past the index but cannot name
/// the term committed there (Clustor's `CLIENT_REJECT_OUTCOME_UNKNOWN`).
pub const STATUS_UNKNOWN: u8 = 0x07;

/// Applied runs kept per partition: the committed log's last few term
/// changes. A registration older than all of them that the horizon cannot
/// settle resolves UNKNOWN.
pub const RUNS: usize = 8;

/// Closed runs a session processor holds while the edge to `flow` is full.
pub const SPILL: usize = 16;

/// One registration: which client operation waits on which entry.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Registration {
    pub session_slot: u32,
    pub message_id: u32,
    pub partition_id: u16,
    pub wal_index: u64,
    pub session_generation: u32,
    /// The term the entry was appended in, from MSG_PROPOSAL_ASSIGNED.
    pub term: u64,
}

fn le16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}

fn le32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

fn le64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes([
        b[at],
        b[at + 1],
        b[at + 2],
        b[at + 3],
        b[at + 4],
        b[at + 5],
        b[at + 6],
        b[at + 7],
    ])
}

/// Encode a MSG_ACK_REGISTER body.
pub fn encode_register(r: &Registration) -> [u8; REGISTER_LEN] {
    let mut b = [0u8; REGISTER_LEN];
    b[0..4].copy_from_slice(&r.session_slot.to_le_bytes());
    b[4..8].copy_from_slice(&r.message_id.to_le_bytes());
    b[8..10].copy_from_slice(&r.partition_id.to_le_bytes());
    b[10..18].copy_from_slice(&r.wal_index.to_le_bytes());
    b[18..22].copy_from_slice(&r.session_generation.to_le_bytes());
    b[22..30].copy_from_slice(&r.term.to_le_bytes());
    b
}

/// Decode a MSG_ACK_REGISTER body; `None` when short, or when it names no
/// entry (index 0, or term 0 — no Raft entry has either).
pub fn decode_register(p: &[u8]) -> Option<Registration> {
    if p.len() < REGISTER_LEN {
        return None;
    }
    let r = Registration {
        session_slot: le32(p, 0),
        message_id: le32(p, 4),
        partition_id: le16(p, 8),
        wal_index: le64(p, 10),
        session_generation: le32(p, 18),
        term: le64(p, 22),
    };
    if r.wal_index == 0 || r.term == 0 {
        return None;
    }
    Some(r)
}

/// A run of the committed log: entries `first..=last` all carry `term`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Run {
    pub first: u64,
    pub last: u64,
    pub term: u64,
}

impl Run {
    fn covers(&self, index: u64) -> bool {
        self.first <= index && index <= self.last
    }
}

/// Encode a MSG_ACK_APPLIED body.
pub fn encode_applied(partition_id: u16, run: Run) -> [u8; APPLIED_LEN] {
    let mut b = [0u8; APPLIED_LEN];
    b[0..2].copy_from_slice(&partition_id.to_le_bytes());
    b[2..10].copy_from_slice(&run.first.to_le_bytes());
    b[10..18].copy_from_slice(&run.last.to_le_bytes());
    b[18..26].copy_from_slice(&run.term.to_le_bytes());
    b
}

/// Decode a MSG_ACK_APPLIED body; `None` when short or not a run (an empty
/// range, index 0, term 0).
pub fn decode_applied(p: &[u8]) -> Option<(u16, Run)> {
    if p.len() < APPLIED_LEN {
        return None;
    }
    let run = Run {
        first: le64(p, 2),
        last: le64(p, 10),
        term: le64(p, 18),
    };
    if run.first == 0 || run.last < run.first || run.term == 0 {
        return None;
    }
    Some((le16(p, 0), run))
}

/// Encode a MSG_ACK_EMIT body.
pub fn encode_emit(
    session_slot: u32,
    message_id: u32,
    session_generation: u32,
    wal_index: u64,
    status: u8,
) -> [u8; EMIT_LEN] {
    let mut b = [0u8; EMIT_LEN];
    b[0..4].copy_from_slice(&session_slot.to_le_bytes());
    b[4..8].copy_from_slice(&message_id.to_le_bytes());
    b[8..12].copy_from_slice(&session_generation.to_le_bytes());
    b[12..20].copy_from_slice(&wal_index.to_le_bytes());
    b[20] = status;
    b
}

/// The status byte of a MSG_ACK_EMIT body; `None` when short.
pub fn emit_status(p: &[u8]) -> Option<u8> {
    p.get(EMIT_LEN - 1).copied()
}

/// What a registration's entry is, as far as this node can tell.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Verdict {
    /// Not yet decidable: keep waiting.
    Wait,
    /// Committed at its `(index, term)`: acknowledge.
    Committed,
    /// Another term's entry was committed at its index: fail the operation.
    Superseded,
    /// Applied past, term at its index unknown here: fail the operation.
    Unknown,
}

impl Verdict {
    /// The MSG_ACK_EMIT status this verdict answers with; `None` for
    /// [`Verdict::Wait`].
    pub fn status(self) -> Option<u8> {
        match self {
            Verdict::Wait => None,
            Verdict::Committed => Some(STATUS_COMMITTED),
            Verdict::Superseded => Some(STATUS_SUPERSEDED),
            Verdict::Unknown => Some(STATUS_UNKNOWN),
        }
    }
}

/// The committed log's term at one index, as the applied runs know it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TermAt {
    /// No run covers the index.
    Unseen,
    Term(u64),
    /// Runs disagree — never for a log whose committed entries are
    /// immutable; decided as unknown rather than trusted.
    Conflict,
}

/// One partition's view, held by `flow`: the commit horizon and the
/// committed log's latest term runs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PartitionTerms {
    horizon_index: u64,
    horizon_term: u64,
    /// Highest index any applied run reached.
    applied: u64,
    runs: [Run; RUNS],
    /// Runs held (at most [`RUNS`]) and the ring slot the next one takes.
    len: u8,
    next: u8,
}

impl Default for PartitionTerms {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionTerms {
    pub const fn new() -> Self {
        Self {
            horizon_index: 0,
            horizon_term: 0,
            applied: 0,
            runs: [Run {
                first: 0,
                last: 0,
                term: 0,
            }; RUNS],
            len: 0,
            next: 0,
        }
    }

    /// The commit horizon `(index, term)` this partition has reached.
    pub fn horizon(&self) -> (u64, u64) {
        (self.horizon_index, self.horizon_term)
    }

    /// The highest index an applied run has reached.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// Note a commit horizon. Both its index and its term are monotone at
    /// the source; keep the frame furthest along (a later term at the same
    /// index replaces the earlier one), so a reordered or stale frame never
    /// moves the view back.
    pub fn note_horizon(&mut self, index: u64, term: u64) {
        if index > self.horizon_index || (index == self.horizon_index && term > self.horizon_term) {
            self.horizon_index = index;
            self.horizon_term = term;
        }
    }

    /// Note an applied run. A run of the latest run's term that touches or
    /// overlaps it extends it; anything else takes a ring slot, evicting
    /// the oldest.
    pub fn note_applied(&mut self, run: Run) {
        if run.first == 0 || run.last < run.first || run.term == 0 {
            return;
        }
        if run.last > self.applied {
            self.applied = run.last;
        }
        if self.len > 0 {
            let at = (usize::from(self.next) + RUNS - 1) % RUNS;
            let top = &mut self.runs[at];
            if top.term == run.term
                && run.first <= top.last.saturating_add(1)
                && run.last.saturating_add(1) >= top.first
            {
                top.first = top.first.min(run.first);
                top.last = top.last.max(run.last);
                return;
            }
        }
        self.runs[usize::from(self.next)] = run;
        self.next = ((usize::from(self.next) + 1) % RUNS) as u8;
        if usize::from(self.len) < RUNS {
            self.len += 1;
        }
    }

    fn term_at(&self, index: u64) -> TermAt {
        let mut found = TermAt::Unseen;
        for r in self.runs.iter().take(usize::from(self.len)) {
            if !r.covers(index) {
                continue;
            }
            found = match found {
                TermAt::Unseen => TermAt::Term(r.term),
                TermAt::Term(t) if t == r.term => TermAt::Term(t),
                _ => TermAt::Conflict,
            };
        }
        found
    }

    /// Resolve the entry appended at `(wal_index, term)`.
    pub fn resolve(&self, wal_index: u64, term: u64) -> Verdict {
        let reached = self.horizon_index >= wal_index;
        if reached && self.horizon_term == term {
            return Verdict::Committed;
        }
        match self.term_at(wal_index) {
            TermAt::Term(t) if t == term => {
                if reached {
                    Verdict::Committed
                } else {
                    Verdict::Wait
                }
            }
            TermAt::Term(_) => Verdict::Superseded,
            TermAt::Conflict => Verdict::Unknown,
            TermAt::Unseen if self.applied >= wal_index => Verdict::Unknown,
            TermAt::Unseen => Verdict::Wait,
        }
    }
}

/// One partition's applied-run tracker, held by `session_processor`: the
/// run the committed entries it applies are extending, and how much of it
/// `flow` has been told.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct RunTracker {
    run: Run,
    /// Last index of `run` already sent.
    sent_last: u64,
}

impl RunTracker {
    pub const fn new() -> Self {
        Self {
            run: Run {
                first: 0,
                last: 0,
                term: 0,
            },
            sent_last: 0,
        }
    }

    /// Note one applied committed entry. The run extends while the entries
    /// stay contiguous and of one term; otherwise the current run closes
    /// and is returned if `flow` is still owed any of it, and a new run
    /// opens at this entry.
    pub fn note(&mut self, index: u64, term: u64) -> Option<Run> {
        if index == 0 || term == 0 {
            return None;
        }
        if self.run.term == term && self.run.last != 0 && index == self.run.last + 1 {
            self.run.last = index;
            return None;
        }
        let closed = self.owed();
        self.run = Run {
            first: index,
            last: index,
            term,
        };
        self.sent_last = index - 1;
        closed
    }

    /// The open run, when `flow` has not yet been told all of it.
    pub fn owed(&self) -> Option<Run> {
        (self.run.term != 0 && self.run.last > self.sent_last).then_some(self.run)
    }

    /// The open run was sent through `last`.
    pub fn mark_sent(&mut self, last: u64) {
        if last > self.sent_last {
            self.sent_last = last;
        }
    }
}

/// Closed runs waiting for room on the edge to `flow`, oldest first.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Spill {
    items: [(u16, Run); SPILL],
    len: u8,
}

impl Default for Spill {
    fn default() -> Self {
        Self::new()
    }
}

impl Spill {
    pub const fn new() -> Self {
        Self {
            items: [(
                0,
                Run {
                    first: 0,
                    last: 0,
                    term: 0,
                },
            ); SPILL],
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        usize::from(self.len)
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Hold a closed run. False when full: the run is not held, and `flow`
    /// resolves the indices it covered as unknown.
    pub fn push(&mut self, partition_id: u16, run: Run) -> bool {
        if usize::from(self.len) == SPILL {
            return false;
        }
        self.items[usize::from(self.len)] = (partition_id, run);
        self.len += 1;
        true
    }

    /// The oldest held run.
    pub fn front(&self) -> Option<(u16, Run)> {
        (self.len > 0).then(|| self.items[0])
    }

    /// Drop the oldest held run.
    pub fn pop_front(&mut self) {
        if self.len == 0 {
            return;
        }
        let n = usize::from(self.len);
        let mut i = 1;
        while i < n {
            self.items[i - 1] = self.items[i];
            i += 1;
        }
        self.len -= 1;
    }
}
