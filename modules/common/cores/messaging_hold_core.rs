// Bounded, no_std, no-alloc MESSAGING HOLD core — what one of
// `messaging`'s worker buses still owes the worker when the bus's result
// edge had no room for it. `include!`d by the `messaging` module and by
// the host test crate; it names nothing outside this file.
//
// Every answer `messaging` gives is one a worker waits on: a dedup
// verdict resolves a committed publish's stash, and a retained read or
// an offline drain carries deliveries the session is owed. None is ever
// asked for twice, so an answer the edge refuses must not be dropped.
//
// Verdicts are the hot path, so they queue: the bus goes on being read
// while they wait, in order, for the edge — pausing the bus would back up
// the op edge the worker files its dedup checks on. A retained read or an
// offline drain answers at most the `max` its request carries — the room
// the worker reserved — and ends with a marker saying where it stopped;
// one the edge cuts short is kept as where it stopped, marker included,
// and the bus is not read again until it has gone. The worker reads its
// answers without waiting on anything, so that always comes.

/// A dedup verdict: `[dedup_key:20][verdict:u8][phase:u8]`.
pub const VERDICT_LEN: usize = 22;
/// Verdicts one bus holds while its edge is full.
pub const HOLD_VERDICTS: usize = 64;
/// The longest subscription filter a retained read carries.
pub const PATTERN_MAX: usize = 256;
/// `next` of a retained read whose scan is complete.
pub const READ_DONE: u32 = u32::MAX;

/// A retained read: the request, the next retained entry to match, and
/// how many more answers its credit allows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ReadCursor {
    pub tenant: u32,
    pub session_slot: u32,
    pub sub_qos: u8,
    pub pattern: [u8; PATTERN_MAX],
    pub pattern_len: u16,
    pub next: u32,
    pub left: u16,
}

impl ReadCursor {
    /// A read of `pattern` from entry `from`, answering at most `max`;
    /// `None` when the filter is longer than a read carries.
    pub fn new(
        tenant: u32,
        session_slot: u32,
        sub_qos: u8,
        pattern: &[u8],
        from: u32,
        max: u16,
    ) -> Option<Self> {
        if pattern.len() > PATTERN_MAX {
            return None;
        }
        let mut p = [0u8; PATTERN_MAX];
        p[..pattern.len()].copy_from_slice(pattern);
        Some(Self {
            tenant,
            session_slot,
            sub_qos,
            pattern: p,
            pattern_len: pattern.len() as u16,
            next: from,
            left: max,
        })
    }

    pub fn pattern(&self) -> &[u8] {
        &self.pattern[..usize::from(self.pattern_len)]
    }
}

/// The marker that ends a read or a drain.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Marker {
    /// A drain's envelopes are sent: `[session][remaining]`.
    Drained { session: u32, remaining: u32 },
    /// A read's answers are sent: `[session][next]`, `next` the entry to
    /// resume from or [`READ_DONE`].
    ReadEnd { session: u32, next: u32 },
}

/// A read or a drain a bus has not finished answering.
#[expect(
    clippy::large_enum_variant,
    reason = "held in fixed module state, one per bus, with no heap to box \
              a read's filter into; the size is the bound"
)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Owed {
    None,
    /// A session's offline queue: `left` more envelopes may go.
    Drain {
        session: u32,
        left: u16,
    },
    /// A retained read.
    Read(ReadCursor),
    /// Everything sent but the marker.
    Marker(Marker),
}

/// Writes what a bus owes, as far as its edge takes it.
pub trait Replies {
    /// Write the verdict whole; `false` when the edge has no room.
    fn verdict(&mut self, v: &[u8; VERDICT_LEN]) -> bool;
    /// Drain `session`'s queue on, oldest first, counting `left` down.
    /// `Some(remaining)` once `left` is spent or nothing is left queued;
    /// `None` when the edge stopped it.
    fn drain(&mut self, session: u32, left: &mut u16) -> Option<u32>;
    /// Answer the read from `c.next` on, counting `c.left` down.
    /// `Some(next)` once `c.left` is spent (the entry to resume from) or
    /// the scan is complete ([`READ_DONE`]); `None` when the edge stopped
    /// it, `c.next` at the answer it had no room for.
    fn read(&mut self, c: &mut ReadCursor) -> Option<u32>;
    /// Write the marker; `false` when the edge has no room.
    fn marker(&mut self, m: &Marker) -> bool;
}

/// One bus's hold.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BusHold {
    verdicts: [[u8; VERDICT_LEN]; HOLD_VERDICTS],
    head: usize,
    count: usize,
    /// The read or drain not finished.
    pub owed: Owed,
    /// Answers that had to wait (each counted once).
    pub held: u32,
}

impl BusHold {
    pub const fn new() -> Self {
        Self {
            verdicts: [[0; VERDICT_LEN]; HOLD_VERDICTS],
            head: 0,
            count: 0,
            owed: Owed::None,
            held: 0,
        }
    }

    /// Whether the bus may be read: no read or drain is unfinished, and a
    /// verdict the next request may give has room to wait.
    pub fn can_read(&self) -> bool {
        self.owed == Owed::None && self.count < HOLD_VERDICTS
    }

    /// Whether nothing at all is owed.
    pub fn is_clear(&self) -> bool {
        self.owed == Owed::None && self.count == 0
    }

    /// Verdicts waiting for the edge.
    pub fn verdicts_held(&self) -> usize {
        self.count
    }

    /// Answer with verdict `v`: written now when none waits ahead of it
    /// and the edge takes it, else queued behind those waiting. `false`
    /// only with the queue full — the bus is read only while
    /// [`Self::can_read`], so that is never reached.
    pub fn verdict<R: Replies>(&mut self, r: &mut R, v: &[u8; VERDICT_LEN]) -> bool {
        if self.count == 0 && r.verdict(v) {
            return true;
        }
        if self.count == HOLD_VERDICTS {
            return false;
        }
        self.verdicts[(self.head + self.count) % HOLD_VERDICTS] = *v;
        self.count += 1;
        self.held = self.held.wrapping_add(1);
        true
    }

    /// Start answering a read or a drain, as far as the edge takes it now;
    /// what it cannot finish is kept. `false` (nothing started) if one is
    /// already unfinished — the bus is not read while one is.
    pub fn start<R: Replies>(&mut self, r: &mut R, owed: Owed) -> bool {
        if self.owed != Owed::None {
            return false;
        }
        self.owed = owed;
        self.answer(r);
        if self.owed != Owed::None {
            self.held = self.held.wrapping_add(1);
        }
        true
    }

    /// Write on what is owed — the waiting verdicts in order, then the
    /// read or drain and its marker — as far as the edge takes it.
    /// Returns [`Self::can_read`].
    pub fn resume<R: Replies>(&mut self, r: &mut R) -> bool {
        while self.count > 0 && r.verdict(&self.verdicts[self.head]) {
            self.head = (self.head + 1) % HOLD_VERDICTS;
            self.count -= 1;
        }
        self.answer(r);
        self.can_read()
    }

    fn answer<R: Replies>(&mut self, r: &mut R) {
        loop {
            self.owed = match self.owed {
                Owed::None => return,
                Owed::Drain { session, mut left } => match r.drain(session, &mut left) {
                    Some(remaining) => Owed::Marker(Marker::Drained { session, remaining }),
                    None => {
                        self.owed = Owed::Drain { session, left };
                        return;
                    }
                },
                Owed::Read(mut c) => match r.read(&mut c) {
                    Some(next) => Owed::Marker(Marker::ReadEnd {
                        session: c.session_slot,
                        next,
                    }),
                    None => {
                        self.owed = Owed::Read(c);
                        return;
                    }
                },
                Owed::Marker(m) => {
                    if r.marker(&m) {
                        Owed::None
                    } else {
                        return;
                    }
                }
            };
        }
    }
}

impl Default for BusHold {
    fn default() -> Self {
        Self::new()
    }
}
