// Bounded, no_std, no-alloc KAFKA RESPONSE ORDER core — the responses on
// one Kafka connection leave in the order its requests arrived.
// `include!`d by `protocol` (the Kafka codec) and by the host test crate;
// it names nothing outside this file.
//
// The Kafka protocol answers the requests on a connection in the order
// they were sent. A client matches each response to the oldest request it
// has in flight: Java's `NetworkClient` fails the connection on a
// correlation id that is not the oldest's, librdkafka treats one as a
// protocol error. Quantum answers some requests at once (ApiVersions and
// Metadata in the codec; Fetch, ListOffsets and the group calls in
// `session_processor`) and others later (a Produce once it is quorum
// durable, an OffsetCommit likewise, a Fetch served from the WAL), so the
// answers can be ready out of order. This core puts them back in order.
//
// Every request that will be answered takes the next place in its
// connection's queue when the codec takes it. An answer for the oldest
// place is written at once; an answer for a later place is kept in a slot
// until every answer ahead of it has been written. A request whose answer
// will never come (`session_processor` says so with a close marker, an
// answer of `None` to [`answer`]) closes the connection when it reaches
// the front: no later answer may be written past it, and the client would
// wait on it for ever.
//
// ## Bounds
//
// - At most [`DEPTH`] requests are in flight per connection (Kafka's
//   `max.in.flight.requests.per.connection`, whose Java default is 5). A
//   request past it waits in the connection's reassembly buffer — the
//   backpressure goes to the client.
// - Slots are shared across connections and reserved when a request is
//   taken, not when its answer arrives: an answer that arrives always has
//   the room it needs, so taking answers off the response bus never waits
//   on another answer still behind it there. The oldest request takes no
//   slot — its answer is written as it arrives — and a request that
//   becomes the oldest gives its slot back, so a connection that is not
//   pipelining holds none. A request no slot is free for waits like one
//   past `DEPTH`.
// - A slot is sized for the most its request's API can answer
//   ([`class_for`]): small for the fixed-shape answers, large for those
//   that carry records or member lists.
//
// ## Per-step bound
//
// Every entry point is O(DEPTH) over one connection, or O(slots) to find
// a free one.

/// Requests in flight on one connection.
pub const DEPTH: usize = 8;
/// The largest frame (`[size][corr][body]`) a small slot holds: a
/// ProduceResponse for 16 partitions of a 64-byte topic (566 bytes), a
/// parked OffsetCommit answer (512 + 8), the group calls' fixed shapes.
pub const SMALL_BYTES: usize = 640;
/// Small slots, across every connection.
pub const SMALL_SLOTS: usize = 192;
/// The largest frame a large slot holds: the largest a codec frame can
/// carry to the client (the frame edge's 8 KiB less the conn id).
pub const LARGE_BYTES: usize = 8190;
/// Large slots, across every connection.
pub const LARGE_SLOTS: usize = 8;
const SLOTS: usize = SMALL_SLOTS + LARGE_SLOTS;
const _: () = assert!(SLOTS < NO_SLOT as usize);

/// No slot: the oldest request, whose answer is written as it arrives.
pub const NO_SLOT: u8 = 0xFF;

/// A slot class ([`class_for`]).
pub const CLASS_SMALL: u8 = 0;
pub const CLASS_LARGE: u8 = 1;

/// Waiting for its answer.
const WAIT: u8 = 0;
/// Answered; the frame is in its slot.
const READY: u8 = 1;
/// Will never be answered: the connection closes when it reaches the
/// front.
const CLOSE: u8 = 2;

const API_PRODUCE: i16 = 0;
const API_FETCH: i16 = 1;
const API_LIST_OFFSETS: i16 = 2;
const API_METADATA: i16 = 3;
const API_OFFSET_FETCH: i16 = 9;
const API_JOIN_GROUP: i16 = 11;

/// The slot class an answer to `api_key` needs: large for the answers
/// that carry records, partition lists or member lists (Fetch,
/// ListOffsets, Metadata, OffsetFetch, JoinGroup), small for every other.
pub fn class_for(api_key: i16) -> u8 {
    match api_key {
        API_FETCH | API_LIST_OFFSETS | API_METADATA | API_OFFSET_FETCH | API_JOIN_GROUP => {
            CLASS_LARGE
        }
        _ => CLASS_SMALL,
    }
}

/// Whether a request is answered at all: every one is but a Produce with
/// `acks = 0`, which the protocol forbids answering. `body` is the
/// request body after the client id. A body too short to say counts as
/// answered — `session_processor` answers a malformed Produce, or closes
/// the connection.
pub fn answered(api_key: i16, api_ver: i16, body: &[u8]) -> bool {
    if api_key != API_PRODUCE {
        return true;
    }
    let mut off = 0usize;
    // transactional_id: nullable string, v3+.
    if api_ver >= 3 {
        let Some(b) = body.get(off..off + 2) else {
            return true;
        };
        let tl = i16::from_be_bytes([b[0], b[1]]);
        off += 2;
        if tl > 0 {
            off += tl as usize;
        }
    }
    match body.get(off..off + 2) {
        Some(b) => i16::from_be_bytes([b[0], b[1]]) != 0,
        None => true,
    }
}

#[derive(Clone, Copy)]
struct Ent {
    corr: i32,
    len: u16,
    slot: u8,
    state: u8,
}

impl Ent {
    const fn zero() -> Self {
        Self {
            corr: 0,
            len: 0,
            slot: NO_SLOT,
            state: WAIT,
        }
    }
}

/// One connection's requests in flight, oldest first.
#[derive(Clone, Copy)]
pub struct Seq {
    ent: [Ent; DEPTH],
    head: u8,
    count: u8,
    /// When the oldest became the oldest (ms).
    since_ms: u64,
}

impl Seq {
    pub const fn zero() -> Self {
        Self {
            ent: [Ent::zero(); DEPTH],
            head: 0,
            count: 0,
            since_ms: 0,
        }
    }

    fn at(&self, i: usize) -> &Ent {
        &self.ent[(usize::from(self.head) + i) % DEPTH]
    }

    fn at_mut(&mut self, i: usize) -> &mut Ent {
        &mut self.ent[(usize::from(self.head) + i) % DEPTH]
    }
}

/// The slots answers wait in, shared by every connection.
pub struct Pool {
    small: [[u8; SMALL_BYTES]; SMALL_SLOTS],
    large: [[u8; LARGE_BYTES]; LARGE_SLOTS],
    used: [bool; SLOTS],
    /// Slots in use, for the heartbeat.
    pub in_use: u16,
}

impl Pool {
    pub const fn zero() -> Self {
        Self {
            small: [[0; SMALL_BYTES]; SMALL_SLOTS],
            large: [[0; LARGE_BYTES]; LARGE_SLOTS],
            used: [false; SLOTS],
            in_use: 0,
        }
    }
}

/// Free every slot. The module state starts zeroed, which is already
/// empty; this is for a reset.
pub fn pool_init(p: &mut Pool) {
    p.used = [false; SLOTS];
    p.in_use = 0;
}

fn cap(slot: u8) -> usize {
    if usize::from(slot) < SMALL_SLOTS {
        SMALL_BYTES
    } else {
        LARGE_BYTES
    }
}

fn bytes(p: &Pool, slot: u8, len: usize) -> &[u8] {
    let i = usize::from(slot);
    if i < SMALL_SLOTS {
        &p.small[i][..len]
    } else {
        &p.large[i - SMALL_SLOTS][..len]
    }
}

fn bytes_mut(p: &mut Pool, slot: u8) -> &mut [u8] {
    let i = usize::from(slot);
    if i < SMALL_SLOTS {
        &mut p.small[i][..]
    } else {
        &mut p.large[i - SMALL_SLOTS][..]
    }
}

fn alloc(p: &mut Pool, class: u8) -> Option<u8> {
    let range = if class == CLASS_LARGE {
        SMALL_SLOTS..SLOTS
    } else {
        0..SMALL_SLOTS
    };
    let i = range.into_iter().find(|&i| !p.used[i])?;
    p.used[i] = true;
    p.in_use += 1;
    Some(i as u8)
}

fn free(p: &mut Pool, slot: u8) {
    let i = usize::from(slot);
    if i < SLOTS && p.used[i] {
        p.used[i] = false;
        p.in_use -= 1;
    }
}

/// Requests in flight on `q`.
#[inline]
pub fn len(q: &Seq) -> usize {
    usize::from(q.count)
}

/// Whether `q` has nothing in flight: an answer now is in order.
#[inline]
pub fn is_idle(q: &Seq) -> bool {
    q.count == 0
}

/// Room for a further request of slot class `class` on `q`:
/// `Some(NO_SLOT)` when nothing is in flight (its answer will be the
/// oldest and is written as it arrives), `Some(slot)` with the slot its
/// answer will wait in, `None` when `q` is at [`DEPTH`] or no slot of
/// the class is free — the request waits. A slot taken here is given
/// back by [`unreserve`] if the request is not taken after all, else it
/// belongs to the place [`push`] makes.
pub fn reserve(q: &Seq, p: &mut Pool, class: u8) -> Option<u8> {
    if usize::from(q.count) >= DEPTH {
        return None;
    }
    if q.count == 0 {
        return Some(NO_SLOT);
    }
    alloc(p, class)
}

/// Give back a slot [`reserve`] took for a request that was not taken.
pub fn unreserve(p: &mut Pool, slot: u8) {
    if slot != NO_SLOT {
        free(p, slot);
    }
}

/// Take the request answering `corr` into the next place on `q`, waiting
/// in `slot` (from [`reserve`]).
pub fn push(q: &mut Seq, corr: i32, slot: u8, now: u64) {
    push_state(q, corr, slot, WAIT, 0, now);
}

/// Take a request whose answer is already made (one the codec answers
/// itself, behind others in flight): `frame` goes into `slot` (from
/// [`reserve`]) to be written in its turn. `false` — the request is not
/// taken and the slot is given back — when the frame does not fit it.
pub fn push_answered(
    q: &mut Seq,
    p: &mut Pool,
    corr: i32,
    slot: u8,
    frame: &[u8],
    now: u64,
) -> bool {
    if slot == NO_SLOT || frame.len() > cap(slot) || frame.is_empty() {
        unreserve(p, slot);
        return false;
    }
    bytes_mut(p, slot)[..frame.len()].copy_from_slice(frame);
    push_state(q, corr, slot, READY, frame.len() as u16, now);
    true
}

fn push_state(q: &mut Seq, corr: i32, slot: u8, state: u8, len: u16, now: u64) {
    if usize::from(q.count) >= DEPTH {
        return;
    }
    if q.count == 0 {
        q.since_ms = now;
    }
    let i = usize::from(q.count);
    *q.at_mut(i) = Ent {
        corr,
        len,
        slot,
        state,
    };
    q.count += 1;
}

fn pop(q: &mut Seq, p: &mut Pool, now: u64) {
    if q.count == 0 {
        return;
    }
    let slot = q.at(0).slot;
    unreserve(p, slot);
    *q.at_mut(0) = Ent::zero();
    q.head = ((usize::from(q.head) + 1) % DEPTH) as u8;
    q.count -= 1;
    q.since_ms = now;
}

/// Forget everything in flight on `q` (its connection closed), giving
/// its slots back.
pub fn reset(q: &mut Seq, p: &mut Pool) {
    for i in 0..usize::from(q.count) {
        let slot = q.at(i).slot;
        unreserve(p, slot);
    }
    *q = Seq::zero();
}

/// What [`answer`] did.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Answered {
    /// Written now: the request was the oldest.
    Written,
    /// Kept in its slot for its turn, or marked to close the connection
    /// in its turn.
    Kept,
    /// The request was the oldest and the bus refused the write: nothing
    /// changed, and the same answer must be offered again.
    Refused,
    /// No request in flight answers `corr`: the answer is for one this
    /// queue never took (or forgot when its connection closed) and must
    /// not be written — it would be out of order.
    Unmatched,
    /// The answer did not fit its slot: the request is marked to close
    /// the connection, as one never answered is. The slot classes make
    /// this unreachable; it is counted.
    Overflow,
}

/// The answer to the request in flight that `corr` names, the oldest
/// waiting one with it: `Some(frame)`, or `None` for "this request will
/// never be answered" (the connection closes in its turn). The oldest
/// request's frame goes to `write` at once (`false`: the bus refused it);
/// a later one's waits in its slot. [`flush`] writes what is in order
/// afterwards.
pub fn answer(
    q: &mut Seq,
    p: &mut Pool,
    corr: i32,
    frame: Option<&[u8]>,
    now: u64,
    mut write: impl FnMut(&[u8]) -> bool,
) -> Answered {
    let Some(i) = (0..usize::from(q.count)).find(|&i| {
        let e = q.at(i);
        e.state == WAIT && e.corr == corr
    }) else {
        return Answered::Unmatched;
    };
    let Some(frame) = frame else {
        let e = q.at_mut(i);
        let slot = e.slot;
        e.state = CLOSE;
        e.slot = NO_SLOT;
        unreserve(p, slot);
        return Answered::Kept;
    };
    if i == 0 {
        if !write(frame) {
            return Answered::Refused;
        }
        pop(q, p, now);
        return Answered::Written;
    }
    let slot = q.at(i).slot;
    if slot == NO_SLOT || frame.len() > cap(slot) || frame.is_empty() {
        let e = q.at_mut(i);
        e.state = CLOSE;
        e.slot = NO_SLOT;
        unreserve(p, slot);
        return Answered::Overflow;
    }
    bytes_mut(p, slot)[..frame.len()].copy_from_slice(frame);
    let e = q.at_mut(i);
    e.len = frame.len() as u16;
    e.state = READY;
    Answered::Kept
}

/// What [`flush`] left at the front of the queue.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Front {
    /// Nothing in flight.
    Idle,
    /// The oldest request waits for its answer.
    Waiting,
    /// The bus refused the oldest answer; it stays, to be offered again.
    Blocked,
    /// The oldest request will never be answered: the connection must be
    /// closed. The queue is left as it is for the caller to [`reset`].
    Close,
}

/// Write `q`'s answers that are in order, oldest first, until one is
/// still to come or the bus refuses one. A request left the oldest still
/// waiting gives its slot back: its answer will be written as it
/// arrives.
pub fn flush(q: &mut Seq, p: &mut Pool, now: u64, mut write: impl FnMut(&[u8]) -> bool) -> Front {
    loop {
        if q.count == 0 {
            return Front::Idle;
        }
        let e = *q.at(0);
        match e.state {
            READY => {
                if !write(bytes(p, e.slot, usize::from(e.len))) {
                    return Front::Blocked;
                }
                pop(q, p, now);
            }
            CLOSE => return Front::Close,
            _ => {
                if e.slot != NO_SLOT {
                    unreserve(p, e.slot);
                    q.at_mut(0).slot = NO_SLOT;
                }
                return Front::Waiting;
            }
        }
    }
}

/// Whether the oldest request on `q` has waited for its answer longer
/// than `timeout_ms`. Every request is answered or marked never to be
/// (`session_processor` answers its own expiry), so this is the backstop
/// for an answer lost on the way — a connection that would otherwise
/// hold every later answer for ever.
pub fn overdue(q: &Seq, now: u64, timeout_ms: u64) -> bool {
    q.count > 0 && q.at(0).state == WAIT && now.wrapping_sub(q.since_ms) > timeout_ms
}

/// Whether the front of `q` has something to write or a close to make
/// (the caller should [`flush`] it on its next step).
pub fn has_work(q: &Seq) -> bool {
    q.count > 0 && q.at(0).state != WAIT
}
