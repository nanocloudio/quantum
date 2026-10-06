//! The provider half of a PUBLISH exchange, for every broker connector.
//!
//! A sink takes records for a durable destination as exchange requests and
//! answers each once: 200 with an empty body when the broker has durably
//! accepted it, a contract status when it will not carry it. What differs
//! between MQTT, Kafka and AMQP is how a record reaches the broker and what
//! the broker says back; what does not differ is everything here:
//!
//! - collecting each request whole (`Collector`), granting the body credit it
//!   owes and answering its refusals;
//! - judging the request: `METHOD_PUBLISH` with `MORE` and `BROADCAST` the only
//!   flags, anything else 400;
//! - the in-flight window, which maps the broker's own acknowledgement handle
//!   (an MQTT packet id, a Kafka correlation id, an AMQP delivery tag) to the
//!   exchange it answers;
//! - the queue of records owed on `response_out`, which never drops one: the
//!   module reads a request only while the queue has room for every answer it
//!   could then owe;
//! - LINK: `DOWN` when the broker connection the window rode on is lost,
//!   `UP` when it is back.
//!
//! LINK reports a change, so neither record is written for the first
//! connection: requests that wait in the channel for it are read and answered
//! once it opens. After a `DOWN`, every request record read is dropped until
//! the `UP` leaves the queue — the requester re-issues each exchange it holds
//! open once it reads the `UP`, and reading the originals too would publish
//! them twice.
//!
//! `#[path]`-mounted by each connector, like `authority.rs`. It does no I/O:
//! the module reads `request_in`, writes `response_out` and talks to the
//! broker; this decides what each record means and what is owed.

#![allow(
    dead_code,
    reason = "shared provider; each connector uses the subset its broker's acknowledgement shape needs"
)]

// The single mount of the SDK contract for this file. A connector reaches
// the contract through this mount (`publish_exchange::exchange`) so the
// types it hands back here are the same types.
#[path = "../../target/fluxor/fluxor-abi/sdk/contracts/exchange.rs"]
pub mod exchange;

use exchange::{
    flag, link, parse_request, status, write_credit, write_link, write_response, Collector,
    ExchangeId, Record, Request, KEY_MAX, METHOD_PUBLISH,
};

/// Largest header block a publish may carry. A sink reads none of it, so
/// the bound is room for what a requester attaches, not a field it uses.
pub const HEADERS_MAX: usize = 512;

/// Request flags a publish may carry: a body to follow, and delivery to
/// every ordering unit.
const FLAGS_ALLOWED: u8 = flag::MORE | flag::BROADCAST;

/// Records owed per request read, at most: one answer or one credit.
const OWED_PER_READ: usize = 1;
/// LINK records the queue can hold at its tail: a `DOWN` and the `UP`
/// after it. A later `DOWN` cancels a queued `UP` rather than following it.
const LINK_RESERVE: usize = 2;

/// One record owed on `response_out`.
#[derive(Clone, Copy)]
enum Owed {
    Answer { id: ExchangeId, status: u16 },
    Credit { id: ExchangeId, bytes: u32 },
    Link(u8),
}

impl Owed {
    const NONE: Owed = Owed::Link(0);

    fn exchange(&self) -> Option<ExchangeId> {
        match *self {
            Owed::Answer { id, .. } | Owed::Credit { id, .. } => Some(id),
            Owed::Link(_) => None,
        }
    }
}

/// What the requester last learned of the broker link.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LinkSeen {
    /// Never connected: nothing is open to invalidate.
    Unknown,
    Up,
    Down,
}

/// One publish handed to the broker and not yet acknowledged.
#[derive(Clone, Copy)]
struct InFlight {
    live: bool,
    /// The requester aborted the exchange: the broker's verdict is
    /// still awaited (it holds the window slot), but nothing is written.
    aborted: bool,
    /// The broker's acknowledgement handle.
    handle: u64,
    id: ExchangeId,
    sent_ms: u64,
}

impl InFlight {
    const EMPTY: InFlight = InFlight {
        live: false,
        aborted: false,
        handle: 0,
        id: ExchangeId::NONE,
        sent_ms: 0,
    };
}

/// A sink's exchange state: `SLOTS` requests collected at once, each body
/// up to `BODY` bytes; `WINDOW` publishes awaiting the broker; `QUEUE`
/// records owed on `response_out`.
pub struct PublishProvider<
    const SLOTS: usize,
    const BODY: usize,
    const WINDOW: usize,
    const QUEUE: usize,
> {
    requests: Collector<SLOTS, KEY_MAX, HEADERS_MAX, BODY>,
    window: [InFlight; WINDOW],
    used: usize,
    queue: [Owed; QUEUE],
    head: usize,
    len: usize,
    seen: LinkSeen,
    discarding: bool,
}

impl<const SLOTS: usize, const BODY: usize, const WINDOW: usize, const QUEUE: usize>
    PublishProvider<SLOTS, BODY, WINDOW, QUEUE>
{
    /// The queue holds every answer the window can owe, the record one read
    /// can add, and the LINK pair — so no owed record is ever refused room.
    const QUEUE_FITS: () = assert!(QUEUE >= WINDOW + OWED_PER_READ + LINK_RESERVE);

    pub const fn new() -> Self {
        let () = Self::QUEUE_FITS;
        PublishProvider {
            requests: Collector::new(),
            window: [InFlight::EMPTY; WINDOW],
            used: 0,
            queue: [Owed::NONE; QUEUE],
            head: 0,
            len: 0,
            seen: LinkSeen::Unknown,
            discarding: false,
        }
    }

    /// Return to the state of [`PublishProvider::new`], in place.
    pub fn reset(&mut self) {
        let () = Self::QUEUE_FITS;
        for at in 0..SLOTS {
            self.requests.release(at);
        }
        let _ = self.requests.take_grant();
        let _ = self.requests.take_refusal();
        self.window = [InFlight::EMPTY; WINDOW];
        self.used = 0;
        self.head = 0;
        self.len = 0;
        self.seen = LinkSeen::Unknown;
        self.discarding = false;
    }

    // ── Reading requests ─────────────────────────────────────────────

    /// Whether the module may read one request record and act on it: the
    /// window has a slot for a publish it completes, and the queue has room
    /// for everything that read could owe. Reading stops otherwise, which is
    /// the backpressure the requester sees.
    pub fn can_take(&self) -> bool {
        !self.discarding
            && self.used < WINDOW
            && self.len + self.used + OWED_PER_READ + LINK_RESERVE <= QUEUE
    }

    /// Whether request records are being read and dropped: a `DOWN` is
    /// owed or written, and its `UP` has not left the queue.
    pub fn discarding(&self) -> bool {
        self.discarding
    }

    /// Take one request record. `Some(at)` is a publish complete and
    /// judged well-formed: read it with [`PublishProvider::request`], then
    /// [`PublishProvider::dispatch`] it or [`PublishProvider::refuse`] it.
    /// Everything else — a body still to come, a refusal, an abort — is
    /// settled here, with any record it owes queued.
    pub fn accept(&mut self, record: &[u8]) -> Option<usize> {
        if self.discarding {
            return None;
        }
        // An abort ends the exchange wherever it is: collecting (the
        // collector frees it), awaiting the broker, or answered and queued.
        if let Some(Record::Abort { id, .. }) = parse_request(record) {
            self.abort(id);
        }
        let taken = self.requests.accept(record);
        if let Some((id, bytes)) = self.requests.take_grant() {
            self.push(Owed::Credit { id, bytes });
        }
        if let Some((id, why)) = self.requests.take_refusal() {
            self.push(Owed::Answer {
                id,
                status: why.status(),
            });
        }
        let at = taken.ok().flatten()?;
        let (id, method, flags) = match self.requests.request(at) {
            Some(r) => (r.id, r.method, r.flags),
            None => {
                self.requests.release(at);
                return None;
            }
        };
        if method != METHOD_PUBLISH || flags & !FLAGS_ALLOWED != 0 {
            self.requests.release(at);
            self.push(Owed::Answer {
                id,
                status: status::BAD_REQUEST,
            });
            return None;
        }
        Some(at)
    }

    /// The publish in slot `at`: `target` is its ordering key, `body` the
    /// record.
    pub fn request(&self, at: usize) -> Option<Request<'_>> {
        self.requests.request(at)
    }

    /// Whether the publish in slot `at` asks for every ordering unit.
    pub fn broadcast(&self, at: usize) -> bool {
        self.requests
            .request(at)
            .is_some_and(|r| r.flags & flag::BROADCAST != 0)
    }

    /// Answer the publish in slot `at` with a refusal of the module's own
    /// (413 for a record the broker cannot take, 502 for one it will not
    /// route) and free the slot.
    pub fn refuse(&mut self, at: usize, status: u16) {
        if let Some(r) = self.requests.request(at) {
            let id = r.id;
            self.push(Owed::Answer { id, status });
        }
        self.requests.release(at);
    }

    /// The publish in slot `at` went to the broker under `handle`: it is
    /// answered when [`PublishProvider::settle`] names that handle. False
    /// (and the slot kept) when the window is full — [`PublishProvider::can_take`]
    /// rules that out for a caller that checks it.
    pub fn dispatch(&mut self, at: usize, handle: u64, now_ms: u64) -> bool {
        let Some(id) = self.requests.request(at).map(|r| r.id) else {
            return false;
        };
        let Some(free) = self.window.iter().position(|f| !f.live) else {
            return false;
        };
        self.window[free] = InFlight {
            live: true,
            aborted: false,
            handle,
            id,
            sent_ms: now_ms,
        };
        self.used += 1;
        self.requests.release(at);
        true
    }

    // ── The broker's verdicts ────────────────────────────────────────

    /// The broker answered `handle`: owe `status` to its exchange (200 for
    /// durable acceptance). False when no publish awaits that handle.
    pub fn settle(&mut self, handle: u64, status: u16) -> bool {
        match self
            .window
            .iter()
            .position(|f| f.live && f.handle == handle)
        {
            Some(at) => {
                self.settle_at(at, status);
                true
            }
            None => false,
        }
    }

    /// The broker answered every handle up to and including `handle` (an
    /// AMQP `multiple` confirm; handles there are sequential). Returns how
    /// many publishes it settled.
    pub fn settle_through(&mut self, handle: u64, status: u16) -> usize {
        let mut settled = 0;
        // Oldest first, so answers leave in the order the broker took them.
        while let Some(at) = self
            .window
            .iter()
            .enumerate()
            .filter(|(_, f)| f.live && f.handle <= handle)
            .min_by_key(|(_, f)| f.handle)
            .map(|(at, _)| at)
        {
            self.settle_at(at, status);
            settled += 1;
        }
        settled
    }

    fn settle_at(&mut self, at: usize, status: u16) {
        let f = self.window[at];
        self.window[at] = InFlight::EMPTY;
        self.used -= 1;
        if !f.aborted {
            self.push(Owed::Answer { id: f.id, status });
        }
    }

    /// Publishes awaiting the broker.
    pub fn in_flight(&self) -> usize {
        self.used
    }

    /// When the longest-waiting publish went to the broker.
    pub fn oldest_sent_ms(&self) -> Option<u64> {
        self.window
            .iter()
            .filter(|f| f.live)
            .map(|f| f.sent_ms)
            .min()
    }

    // ── The broker link ──────────────────────────────────────────────

    /// The broker connection is lost. Every publish awaiting it is now
    /// unknowable: the window is cleared, requests being collected are
    /// dropped with the credit owed them, and `DOWN` is owed — unless the
    /// link never came up, when nothing was read and nothing is open.
    pub fn link_down(&mut self) {
        if self.seen != LinkSeen::Up {
            return;
        }
        self.seen = LinkSeen::Down;
        self.window = [InFlight::EMPTY; WINDOW];
        self.used = 0;
        for at in 0..SLOTS {
            self.requests.release(at);
        }
        let _ = self.requests.take_grant();
        self.retain(|o| !matches!(o, Owed::Credit { .. }));
        self.discarding = true;
        // A `DOWN` after an `UP` still queued: the requester has not been
        // told the link came back, so withdrawing the `UP` tells it the
        // same thing in one record.
        if self.len > 0 && matches!(self.at(self.len - 1), Owed::Link(link::UP)) {
            self.len -= 1;
        } else {
            self.push(Owed::Link(link::DOWN));
        }
    }

    /// The broker connection is (re)established and accepting. Owes `UP`
    /// after a `DOWN`; the first connection owes nothing.
    pub fn link_up(&mut self) {
        match self.seen {
            LinkSeen::Up => {}
            LinkSeen::Unknown => self.seen = LinkSeen::Up,
            LinkSeen::Down => {
                self.seen = LinkSeen::Up;
                self.push(Owed::Link(link::UP));
            }
        }
    }

    // ── Writing answers ──────────────────────────────────────────────

    /// Records owed on `response_out`.
    pub fn owed(&self) -> usize {
        self.len
    }

    /// Encode the oldest owed record into `out` and take it off the queue.
    /// The caller places it (through an `ExchangeOutbox`, which holds it
    /// if the port has no room). `None` when nothing is owed.
    pub fn next_record(&mut self, out: &mut [u8]) -> Option<usize> {
        if self.len == 0 {
            return None;
        }
        let owed = self.at(0);
        let n = match owed {
            Owed::Answer { id, status } => write_response(&id, status, &[], &[], out)?,
            Owed::Credit { id, bytes } => write_credit(&id, bytes, out)?,
            Owed::Link(state) => write_link(state, out)?,
        };
        self.head = (self.head + 1) % QUEUE;
        self.len -= 1;
        if matches!(owed, Owed::Link(link::UP)) {
            self.discarding = false;
        }
        Some(n)
    }

    // ── Internals ────────────────────────────────────────────────────

    /// The requester ended exchange `id`: nothing more is written for it.
    fn abort(&mut self, id: ExchangeId) {
        for f in self.window.iter_mut().filter(|f| f.live && f.id == id) {
            f.aborted = true;
        }
        self.retain(|o| o.exchange() != Some(id));
    }

    fn at(&self, i: usize) -> Owed {
        self.queue[(self.head + i) % QUEUE]
    }

    fn push(&mut self, owed: Owed) {
        // `can_take` keeps a slot free for every record a read, a verdict
        // or a link change can owe, so this never finds the queue full.
        if self.len < QUEUE {
            self.queue[(self.head + self.len) % QUEUE] = owed;
            self.len += 1;
        }
    }

    /// Keep the owed records `keep` accepts, in order.
    fn retain(&mut self, keep: impl Fn(&Owed) -> bool) {
        let mut kept = 0;
        for i in 0..self.len {
            let o = self.at(i);
            if keep(&o) {
                self.queue[(self.head + kept) % QUEUE] = o;
                kept += 1;
            }
        }
        self.len = kept;
    }
}

impl<const SLOTS: usize, const BODY: usize, const WINDOW: usize, const QUEUE: usize> Default
    for PublishProvider<SLOTS, BODY, WINDOW, QUEUE>
{
    fn default() -> Self {
        Self::new()
    }
}
