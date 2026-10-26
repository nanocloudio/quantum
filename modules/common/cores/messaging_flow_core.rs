// Bounded, no_std, no-alloc MESSAGING FLOW core — the worker's credit for
// what `messaging` sends it unasked-for in number: the answers to a
// retained read (one per matching entry) and an offline drain (one per
// queued envelope). `include!`d by `session_processor` and by the host
// test crate; it names nothing outside this file.
//
// The worker always reads `messaging`'s answers — they carry the dedup
// verdicts committed publishes wait on, and `messaging` holds its answers
// while they are not read, which would back up the op edge the worker
// files its dedup checks on. So the deliveries they carry must always have
// somewhere to go. Each read or drain request carries a credit, `max`, and
// the worker reserves that much room before it asks; `messaging` answers
// at most that many and ends with a marker saying where it stopped, and
// the worker asks again for the rest.
//
// The two credits are backed differently. A drain's answers are the
// oldest of its session's queue and must reach it in order, so each is
// credited a delivery park slot (`drain_room`). A retained read's answers
// may instead join their session's offline queue, so its credit is a fixed
// pool (`READ_CREDIT`) the worker backs with outbox room for that: a read
// never waits on a client acknowledging a delivery, and so neither does
// anything that waits on a free read slot.
//
// Every request remembers the identity of the session it was asked for
// (`stream`), and every answer is checked against the identity its slot
// holds when it comes: slots are reused, and an answer asked for one
// session is never delivered to the next. One for a session that went
// away (`release`) is dropped too, its credit returned.
//
// While a session's offline queue is being drained its fresh deliveries
// join the queue behind what is there (`backlog`), so the session receives
// them in order; the backlog clears once a drain finds nothing left and
// nothing joined meanwhile.

/// Session slots the worker numbers.
pub const FLOW_SESSIONS: usize = 16384;
/// Retained reads waiting to be answered in full.
pub const READ_SLOTS: usize = 64;
/// Drains asked for and not yet ended.
pub const DRAIN_SLOTS: usize = 16;
/// The most credit one request carries.
pub const CREDIT_PER_REQUEST: u16 = 4;
/// Credit granted to retained reads and not yet spent, at most.
pub const READ_CREDIT: u32 = 8;
/// The longest subscription filter a retained read carries.
pub const FLOW_PATTERN_MAX: usize = 256;
/// `next` of a retained read whose scan is complete.
pub const FLOW_READ_DONE: u32 = u32::MAX;

const WORDS: usize = FLOW_SESSIONS / 64;

/// A retained read, answered in part or not yet asked for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PendingRead {
    pub active: bool,
    pub session: u32,
    /// The session's identity when the read was filed: a slot reused by
    /// another session drops it.
    pub stream: u64,
    pub tenant: u32,
    pub qos: u8,
    pub pattern: [u8; FLOW_PATTERN_MAX],
    pub pattern_len: u16,
    /// The entry to resume the scan from.
    pub next: u32,
    /// Credit the request in flight carries (0: none in flight).
    pub granted: u16,
    /// Whether a request is in flight.
    pub requested: bool,
    /// The session went away with a request in flight: answers are
    /// dropped, and the slot frees at its marker.
    pub cancelled: bool,
}

impl PendingRead {
    const fn empty() -> Self {
        Self {
            active: false,
            session: 0,
            stream: 0,
            tenant: 0,
            qos: 0,
            pattern: [0; FLOW_PATTERN_MAX],
            pattern_len: 0,
            next: 0,
            granted: 0,
            requested: false,
            cancelled: false,
        }
    }

    pub fn pattern(&self) -> &[u8] {
        &self.pattern[..usize::from(self.pattern_len)]
    }
}

/// A drain asked for and not yet ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Drain {
    active: bool,
    session: u32,
    /// The session's identity when the drain was asked for.
    stream: u64,
    granted: u16,
    /// Fresh deliveries that joined the queue since it was asked for.
    joined: u32,
    /// The session went away: answers are dropped, and the slot frees at
    /// its marker.
    cancelled: bool,
}

impl Drain {
    const fn empty() -> Self {
        Self {
            active: false,
            session: 0,
            stream: 0,
            granted: 0,
            joined: 0,
            cancelled: false,
        }
    }
}

/// A request to send now.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Request {
    /// Ask `read`'s retained read again, from its `next`, for `max`.
    Read { read: usize, max: u16 },
    /// Drain `session`'s offline queue, `max` envelopes.
    Drain { session: u32, max: u16 },
}

/// What an answer carrying a delivery is for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Answer {
    /// It spends credit reserved for it: deliver it.
    Deliver,
    /// For a session that went away, or a slot another session now holds:
    /// drop it, its credit returned.
    Drop,
    /// Nothing asked for it.
    Unasked,
}

/// The ledger.
pub struct Flow {
    reads: [PendingRead; READ_SLOTS],
    drains: [Drain; DRAIN_SLOTS],
    want: [u64; WORDS],
    backlog: [u64; WORDS],
    /// Credit granted to reads, and to drains, not yet spent or returned.
    read_credit: u32,
    drain_credit: u32,
    next_read: usize,
    next_word: usize,
    /// Answers that came with no credit for them.
    pub unasked: u32,
}

fn bit(set: &[u64; WORDS], session: u32) -> bool {
    let i = session as usize;
    i < FLOW_SESSIONS && set[i / 64] & (1 << (i % 64)) != 0
}

fn set_bit(set: &mut [u64; WORDS], session: u32, on: bool) {
    let i = session as usize;
    if i < FLOW_SESSIONS {
        if on {
            set[i / 64] |= 1 << (i % 64);
        } else {
            set[i / 64] &= !(1 << (i % 64));
        }
    }
}

impl Flow {
    pub const fn new() -> Self {
        Self {
            reads: [PendingRead::empty(); READ_SLOTS],
            drains: [Drain::empty(); DRAIN_SLOTS],
            want: [0; WORDS],
            backlog: [0; WORDS],
            read_credit: 0,
            drain_credit: 0,
            next_read: 0,
            next_word: 0,
            unasked: 0,
        }
    }

    /// Empty in place — a module's state is too large to build on the
    /// stack and move.
    pub fn init(&mut self) {
        for r in self.reads.iter_mut() {
            r.active = false;
            r.requested = false;
            r.cancelled = false;
            r.granted = 0;
        }
        for d in self.drains.iter_mut() {
            *d = Drain::empty();
        }
        self.want = [0; WORDS];
        self.backlog = [0; WORDS];
        self.read_credit = 0;
        self.drain_credit = 0;
        self.next_read = 0;
        self.next_word = 0;
        self.unasked = 0;
    }

    /// Credit granted to reads and not yet spent: answers still coming
    /// that may each join an offline queue.
    pub fn read_credit(&self) -> u32 {
        self.read_credit
    }

    /// Credit granted to drains and not yet spent: park slots the answers
    /// still coming will take.
    pub fn drain_credit(&self) -> u32 {
        self.drain_credit
    }

    /// Free read slots.
    pub fn reads_free(&self) -> usize {
        self.reads.iter().filter(|r| !r.active).count()
    }

    pub fn read(&self, read: usize) -> &PendingRead {
        &self.reads[read]
    }

    /// File a retained read for `session`; `false` with every slot taken
    /// or a filter longer than a read carries.
    pub fn read_add(
        &mut self,
        session: u32,
        stream: u64,
        tenant: u32,
        qos: u8,
        pattern: &[u8],
    ) -> bool {
        if pattern.len() > FLOW_PATTERN_MAX {
            return false;
        }
        let Some(r) = self.reads.iter_mut().find(|r| !r.active) else {
            return false;
        };
        *r = PendingRead::empty();
        r.active = true;
        r.session = session;
        r.stream = stream;
        r.tenant = tenant;
        r.qos = qos;
        r.pattern[..pattern.len()].copy_from_slice(pattern);
        r.pattern_len = pattern.len() as u16;
        true
    }

    /// `session`'s offline queue is to be drained: until it is empty its
    /// fresh deliveries join it.
    pub fn want_drain(&mut self, session: u32) {
        set_bit(&mut self.backlog, session, true);
        // A drain already in flight is followed by another.
        self.joined(session);
    }

    /// Whether `session`'s fresh deliveries join its offline queue.
    pub fn backlog(&self, session: u32) -> bool {
        bit(&self.backlog, session)
    }

    /// A fresh delivery joined `session`'s offline queue.
    pub fn joined(&mut self, session: u32) {
        match self.drain_of(session) {
            Some(d) => self.drains[d].joined = self.drains[d].joined.wrapping_add(1),
            None => set_bit(&mut self.want, session, true),
        }
    }

    /// Whether nothing is owed to or asked for `session`: no retained read
    /// filed and no drain in flight. A session moves to another worker
    /// only then — answers asked for here could reach it nowhere else.
    pub fn idle(&self, session: u32) -> bool {
        !self.reads.iter().any(|r| r.active && r.session == session)
            && self.drain_of(session).is_none()
    }

    /// `session` left this worker: nothing more is asked for it. A request
    /// in flight has its answers dropped and still ends with its marker,
    /// which returns its credit.
    pub fn release(&mut self, session: u32) {
        set_bit(&mut self.want, session, false);
        set_bit(&mut self.backlog, session, false);
        for r in self.reads.iter_mut() {
            if r.active && r.session == session {
                if r.requested {
                    r.cancelled = true;
                } else {
                    *r = PendingRead::empty();
                }
            }
        }
        if let Some(d) = self.drain_of(session) {
            self.drains[d].cancelled = true;
        }
    }

    /// The next request to send: a read granted what is left of
    /// [`READ_CREDIT`], or a drain granted up to `drain_room` — the park
    /// slots free beyond those already credited. `stream_of(session)` is
    /// the identity of the session active in that slot, `None` when none
    /// is; a read for a session that is no longer the one that filed it,
    /// or a drain for a slot with no session, is let go.
    pub fn next_request<A: Fn(u32) -> Option<u64>>(
        &mut self,
        drain_room: u32,
        stream_of: A,
    ) -> Option<Request> {
        if let Some(r) = self.next_read(&stream_of) {
            return Some(r);
        }
        self.next_drain(drain_room, &stream_of)
    }

    fn next_read<A: Fn(u32) -> Option<u64>>(&mut self, stream_of: &A) -> Option<Request> {
        let max = (READ_CREDIT - self.read_credit).min(u32::from(CREDIT_PER_REQUEST)) as u16;
        if max == 0 {
            return None;
        }
        for k in 0..READ_SLOTS {
            let i = (self.next_read + k) % READ_SLOTS;
            let r = self.reads[i];
            if !r.active || r.requested {
                continue;
            }
            if stream_of(r.session) != Some(r.stream) {
                self.reads[i] = PendingRead::empty();
                continue;
            }
            // One read in flight per session: its answers name only the
            // session.
            if self
                .reads
                .iter()
                .any(|o| o.active && o.requested && o.session == r.session)
            {
                continue;
            }
            self.reads[i].requested = true;
            self.reads[i].granted = max;
            self.read_credit += u32::from(max);
            self.next_read = (i + 1) % READ_SLOTS;
            return Some(Request::Read { read: i, max });
        }
        None
    }

    fn next_drain<A: Fn(u32) -> Option<u64>>(
        &mut self,
        room: u32,
        stream_of: &A,
    ) -> Option<Request> {
        let max = room.min(u32::from(CREDIT_PER_REQUEST)) as u16;
        if max == 0 {
            return None;
        }
        let free = self.drains.iter().position(|d| !d.active)?;
        for k in 0..WORDS {
            let w = (self.next_word + k) % WORDS;
            while self.want[w] != 0 {
                let b = self.want[w].trailing_zeros();
                let session = (w * 64) as u32 + b;
                set_bit(&mut self.want, session, false);
                let Some(stream) = stream_of(session) else {
                    set_bit(&mut self.backlog, session, false);
                    continue;
                };
                self.drains[free] = Drain {
                    active: true,
                    session,
                    stream,
                    granted: max,
                    joined: 0,
                    cancelled: false,
                };
                self.drain_credit += u32::from(max);
                self.next_word = w;
                return Some(Request::Drain { session, max });
            }
        }
        None
    }

    /// A request returned by [`Self::next_request`] could not be sent:
    /// it is asked for again later, its credit returned.
    pub fn unsent(&mut self, req: Request) {
        match req {
            Request::Read { read, max } => {
                self.reads[read].requested = false;
                self.reads[read].granted = 0;
                self.read_credit -= u32::from(max);
            }
            Request::Drain { session, max } => {
                if let Some(d) = self.drain_of(session) {
                    self.drains[d] = Drain::empty();
                    self.drain_credit -= u32::from(max);
                    set_bit(&mut self.want, session, true);
                }
            }
        }
    }

    /// One answer to `session`'s retained read came, `stream` the identity
    /// the slot holds now.
    pub fn read_answer(&mut self, session: u32, stream: u64) -> Answer {
        let Some(i) = self.read_in_flight(session) else {
            self.unasked = self.unasked.wrapping_add(1);
            return Answer::Unasked;
        };
        let r = &mut self.reads[i];
        if r.granted == 0 {
            self.unasked = self.unasked.wrapping_add(1);
            return Answer::Unasked;
        }
        r.granted -= 1;
        self.read_credit -= 1;
        if r.cancelled || r.stream != stream {
            Answer::Drop
        } else {
            Answer::Deliver
        }
    }

    /// `session`'s retained read ended at `next` ([`FLOW_READ_DONE`]: the
    /// scan is complete).
    pub fn read_end(&mut self, session: u32, next: u32) {
        let Some(i) = self.read_in_flight(session) else {
            return;
        };
        let r = &mut self.reads[i];
        self.read_credit -= u32::from(r.granted);
        r.granted = 0;
        r.requested = false;
        if next == FLOW_READ_DONE || r.cancelled {
            *r = PendingRead::empty();
        } else {
            r.next = next;
        }
    }

    /// One envelope of `session`'s drain came, `stream` the identity the
    /// slot holds now.
    pub fn drain_answer(&mut self, session: u32, stream: u64) -> Answer {
        match self.drain_of(session) {
            Some(d) if self.drains[d].granted > 0 => {
                let d = &mut self.drains[d];
                d.granted -= 1;
                self.drain_credit -= 1;
                if d.cancelled || d.stream != stream {
                    Answer::Drop
                } else {
                    Answer::Deliver
                }
            }
            _ => {
                self.unasked = self.unasked.wrapping_add(1);
                Answer::Unasked
            }
        }
    }

    /// `session`'s drain ended with `remaining` still queued.
    pub fn drained(&mut self, session: u32, remaining: u32) {
        let Some(d) = self.drain_of(session) else {
            return;
        };
        let joined = self.drains[d].joined;
        self.drain_credit -= u32::from(self.drains[d].granted);
        self.drains[d] = Drain::empty();
        if !self.backlog(session) {
            return;
        }
        if remaining > 0 || joined > 0 {
            set_bit(&mut self.want, session, true);
        } else {
            set_bit(&mut self.backlog, session, false);
        }
    }

    fn read_in_flight(&self, session: u32) -> Option<usize> {
        self.reads
            .iter()
            .position(|r| r.active && r.requested && r.session == session)
    }

    fn drain_of(&self, session: u32) -> Option<usize> {
        self.drains
            .iter()
            .position(|d| d.active && d.session == session)
    }
}

impl Default for Flow {
    fn default() -> Self {
        Self::new()
    }
}
