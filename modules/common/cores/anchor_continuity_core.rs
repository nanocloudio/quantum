// Bounded, no_std, no-alloc ANCHOR CONTINUITY core — what lets the ingress
// host under an MQTT session be replaced (a paired-anchor takeover,
// planned or after the primary host's power is lost) while the client
// stays connected and the session worker behind it stays where it is.
// `include!`d AFTER `mqtt_anchor_core.rs`, in the same module: it names
// `AnchorConn`, `Phase`, `SESSION_ID_LEN` and `CONN_ID_LEN` by bare name.
// The host test crate and the Wormhole continuity harness mount it that
// way; see docs/architecture/anchor_continuity.md.
//
// `quantum_mqtt_anchor` runs it under `continuity: 1` and
// `session_processor` holds the worker's side through
// `production_seat_core.rs`; the transport under the anchor is Fluxor's
// `TRANSPORT_CONTINUITY` (tcp/tls), checkpointed and moved by the
// paired anchors.
//
// ## What crosses the takeover
//
// The transport (TCP/TLS, mirrored to the standby host by the paired
// anchors) carries the client's bytes, the delivery cursor and the TLS
// records with their production ids. What the MQTT anchor adds is its
// own record ([`AnchorRecord`], `QAC1`): the session's identity, binding
// and negotiated parameters, and the four counters below. A replacement
// anchor imports it together with the cursors the standby transport
// holds ([`TransportCursors`]) and is then exactly where the old one
// was, as far as the transport's mirrored horizon reaches.
//
// What the record cannot say is what the WORKER did with the frames the
// old anchor handed it after the last mirrored delivery cursor, and which
// of its responses never reached the horizon. So the replacement asks:
// `MSG_ANCHOR_REANCHOR` on the SessionCtrlV1 sideband at a higher epoch,
// answered `MSG_ANCHOR_REANCHORED` with the worker's consumed delivery
// high-water and the next production id it will issue
// ([`reanchor_verdict`], [`AnchorReanchor::apply`]). From the answer:
//
//   - a delivery at or below the worker's high-water was consumed: the
//     anchor skips it (and the worker would refuse it as a duplicate
//     anyway, `admit_delivery`) — no duplicate delivery;
//   - a production at or below the horizon is the standby's already: the
//     anchor answers a re-offer of it `Accepted` without submitting it
//     again — no response emitted twice;
//   - a production above the horizon is re-offered by the worker under
//     its ORIGINAL id from what it retained ([`ProductionRing`]) — no
//     acknowledged message lost.
//
// ## Production ids and the output horizon
//
// The worker numbers its responses (`MSG_ANCHOR_PRODUCTION`, the
// production envelope, 1 up per session) and keeps each one until the
// anchor reports it inside the transport's MIRRORED OUTPUT HORIZON
// (`MSG_ANCHOR_HORIZON`): the highest production id the standby holds.
// Only then does a PUBACK / PUBREC / SUBACK count as given — the RPO-zero
// rule of Wormhole's paired anchors: a response keeps its production id
// until the mirrored output horizon accepts it.
// The hook is [`OutputHorizon`]: the anchor submits a production to it
// and reads back `accepted_through`. The transport's is
// [`TransportHorizon`]: Fluxor's `tls` counts the connection's output
// bytes and reports how many of them the standby holds (`MSG_HORIZON`),
// and a production is inside once that reaches its last byte.

// ── Statuses (mirror session_ctrl::STATUS_*) ────────────────────────────────

pub const AC_OK: u8 = 0;
pub const AC_STALE_EPOCH: u8 = 1;
pub const AC_UNKNOWN_SESSION: u8 = 2;
pub const AC_CORRUPT: u8 = 4;
pub const AC_CURSOR_MISMATCH: u8 = 7;

// ── The production envelope ─────────────────────────────────────────────────

/// Bytes the production envelope inserts after the connection id:
/// `[session_id:16][session_epoch:u32 LE][production_id:u64 LE]
/// [inner_msg_type:u8]` — the delivery envelope's layout, outbound.
pub const PRODUCTION_INSERT: usize = SESSION_ID_LEN + 4 + 8 + 1;

/// The fields a production envelope carries in front of the response.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ProductionHeader {
    pub conn: u16,
    pub session_id: [u8; SESSION_ID_LEN],
    pub epoch: u32,
    pub production_id: u64,
    pub inner_msg_type: u8,
}

/// Wrap the response envelope `inner = [conn_id:2][rest]` of type
/// `inner_mt` as a production envelope payload into `out`. Returns the
/// bytes written, or 0 when `inner` has no connection id or `out` is too
/// short — never a truncated frame.
pub fn encode_production(
    out: &mut [u8],
    inner_mt: u8,
    inner: &[u8],
    session_id: &[u8; SESSION_ID_LEN],
    epoch: u32,
    production_id: u64,
) -> usize {
    if inner.len() < CONN_ID_LEN {
        return 0;
    }
    let total = inner.len() + PRODUCTION_INSERT;
    if out.len() < total {
        return 0;
    }
    out[0] = inner[0];
    out[1] = inner[1];
    let mut at = CONN_ID_LEN;
    out[at..at + SESSION_ID_LEN].copy_from_slice(session_id);
    at += SESSION_ID_LEN;
    out[at..at + 4].copy_from_slice(&epoch.to_le_bytes());
    at += 4;
    out[at..at + 8].copy_from_slice(&production_id.to_le_bytes());
    at += 8;
    out[at] = inner_mt;
    at += 1;
    let rest = &inner[CONN_ID_LEN..];
    out[at..at + rest.len()].copy_from_slice(rest);
    total
}

/// Read a production envelope's header, or `None` when it is too short
/// to carry one.
pub fn decode_production(p: &[u8]) -> Option<ProductionHeader> {
    if p.len() < CONN_ID_LEN + PRODUCTION_INSERT {
        return None;
    }
    let mut sid = [0u8; SESSION_ID_LEN];
    sid.copy_from_slice(&p[CONN_ID_LEN..CONN_ID_LEN + SESSION_ID_LEN]);
    let e = CONN_ID_LEN + SESSION_ID_LEN;
    Some(ProductionHeader {
        conn: u16::from_le_bytes([p[0], p[1]]),
        session_id: sid,
        epoch: ac_rd_u32(p, e),
        production_id: ac_rd_u64(p, e + 4),
        inner_msg_type: p[e + 12],
    })
}

/// The response bytes after a production envelope's header.
pub fn production_body(p: &[u8]) -> &[u8] {
    let at = CONN_ID_LEN + PRODUCTION_INSERT;
    if p.len() < at {
        &p[..0]
    } else {
        &p[at..]
    }
}

// ── The output horizon hook ─────────────────────────────────────────────────

/// What the transport did with a submitted production.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Submit {
    /// Taken; it is accepted once `accepted_through` reaches it.
    Pending,
    /// The transport already holds it (a re-offer after a takeover):
    /// taken, and accepted under the same rule.
    Duplicate,
    /// Not now — backpressure, or more productions in flight than the
    /// horizon tracks. Offer it again later, same id.
    NotReady,
}

/// The transport's mirrored output horizon — the hook production
/// acceptance waits on — seen from the anchor.
pub trait OutputHorizon {
    /// Production `id` went onto the transport as `len` bytes of the
    /// connection's output. Ids are submitted strictly in order.
    fn submit(&mut self, id: u64, len: u64) -> Submit;
    /// The highest production id inside the mirrored output horizon: the
    /// standby holds all of it, so it survives the primary host's loss.
    fn accepted_through(&self) -> u64;
}

/// Productions one connection may have written beyond the horizon.
pub const HORIZON_PENDING: usize = 32;

/// The output horizon of a TLS connection under Fluxor's
/// `TRANSPORT_CONTINUITY` (`net_proto::MSG_HORIZON`, relayed to the
/// anchor as `MSG_CLIENT_HORIZON`): the transport counts the
/// connection's clear bytes as its consumer wrote them, and reports how
/// many of them the standby holds. A production is inside the horizon
/// once the horizon reaches the last of its bytes.
///
/// Positions are exact because, on a connection under continuity, every
/// byte the anchor writes after the attachment is a production (the
/// worker answers PINGREQ too) and a production's encoding depends only
/// on the response and the negotiated version (no outbound topic alias):
/// a replacement anchor that knows where the last released production
/// ended (`released_bytes`) and how far the standby's output reached
/// (`bytes_held`) places each re-offered production by its length — held
/// whole (given, nothing written), held in part (only the rest written),
/// or not at all (written).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TransportHorizon {
    /// The connection's bytes written, in the transport's numbering.
    pub written: u64,
    /// The horizon the transport last reported.
    pub through: u64,
    /// Highest production inside it, and the byte its last one ends at.
    pub accepted: u64,
    pub accepted_bytes: u64,
    /// The output the standby held when this connection was taken over
    /// (0 for a connection that was not): bytes the transport reports
    /// before the anchor has re-written up to them.
    pub held: u64,
    pend_id: [u64; HORIZON_PENDING],
    pend_end: [u64; HORIZON_PENDING],
    head: usize,
    count: usize,
}

impl TransportHorizon {
    pub const fn new() -> Self {
        Self::resumed(0, 0, 0)
    }

    /// The horizon of a connection taken over with `bytes_held` of its
    /// output in the standby, whose worker released through production
    /// `released`, which ended at `released_bytes`: re-offers are placed
    /// from there.
    pub const fn resumed(bytes_held: u64, released: u64, released_bytes: u64) -> Self {
        Self {
            written: released_bytes,
            through: bytes_held,
            accepted: released,
            accepted_bytes: released_bytes,
            held: bytes_held,
            pend_id: [0; HORIZON_PENDING],
            pend_end: [0; HORIZON_PENDING],
            head: 0,
            count: 0,
        }
    }

    /// Of the next production's `len` bytes, how many the transport
    /// already holds (a re-offer after a takeover): those are not
    /// written again.
    pub fn held_prefix(&self, len: u64) -> u64 {
        self.through.saturating_sub(self.written).min(len)
    }

    /// Whether another production may be written now.
    pub fn ready(&self) -> bool {
        self.count < HORIZON_PENDING
    }

    /// The transport reported its horizon at `through` bytes. It never
    /// moves back, and never past what was written (or, after a takeover,
    /// what the standby held): a report beyond both cannot be about this
    /// connection's output and would place later productions as held.
    /// Returns whether a production entered it.
    pub fn report(&mut self, through: u64) -> bool {
        let through = through.min(self.written.max(self.held));
        if through <= self.through {
            return false;
        }
        self.through = through;
        self.settle()
    }

    fn settle(&mut self) -> bool {
        let mut moved = false;
        while self.count > 0 && self.pend_end[self.head] <= self.through {
            self.accepted = self.pend_id[self.head];
            self.accepted_bytes = self.pend_end[self.head];
            self.head = (self.head + 1) % HORIZON_PENDING;
            self.count -= 1;
            moved = true;
        }
        moved
    }
}

impl Default for TransportHorizon {
    fn default() -> Self {
        Self::new()
    }
}

impl OutputHorizon for TransportHorizon {
    fn submit(&mut self, id: u64, len: u64) -> Submit {
        if id <= self.accepted {
            return Submit::Duplicate;
        }
        if self.count == HORIZON_PENDING {
            return Submit::NotReady;
        }
        let end = self.written + len;
        self.written = end;
        let at = (self.head + self.count) % HORIZON_PENDING;
        self.pend_id[at] = id;
        self.pend_end[at] = end;
        self.count += 1;
        self.settle();
        Submit::Pending
    }

    fn accepted_through(&self) -> u64 {
        self.accepted
    }
}

/// The inbound side of a TLS connection under continuity: which of the
/// client's bytes the anchor may tell the transport are consumed
/// (`MSG_CLIENT_DELIVERED`, relayed as Fluxor's `CMD_DELIVERED
/// [conn][consumer_cursor][in_bytes]`). The transport keeps every record
/// past the last acknowledgement for its standby, which hands them again
/// from exactly that byte after a takeover, with the cursor
/// (`MSG_CLIENT_RESUMED`).
///
/// An acknowledgement names a packet boundary — `in_bytes` where the
/// codec holds no partial packet, `cursor` the delivery id the next
/// packet takes — and goes only once the worker has consumed every
/// delivery before it: a delivery written toward the worker but lost
/// with this host is then still the transport's to hand again.
/// Boundaries are latched one at a time, so a steady stream still gets
/// acknowledged every worker round trip instead of chasing the newest.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DeliveryCursor {
    /// Highest delivery id the bound worker reported consumed.
    pub consumed: u64,
    /// The bytes the last acknowledgement covered.
    pub acked_bytes: u64,
    /// The newest packet boundary: `(cursor, in_bytes)`.
    latest: (u64, u64),
    /// The boundary waiting for the worker: `(cursor, in_bytes)`, or none.
    pending: Option<(u64, u64)>,
}

impl DeliveryCursor {
    /// A connection from its first byte: its first delivery is id 1.
    pub const fn new() -> Self {
        Self::resumed(1, 0)
    }

    /// A connection the transport resumed here with the predecessor's
    /// last acknowledgement: delivery `cursor` starts at byte `in_bytes`.
    pub const fn resumed(cursor: u64, in_bytes: u64) -> Self {
        Self {
            consumed: 0,
            acked_bytes: in_bytes,
            latest: (cursor, in_bytes),
            pending: None,
        }
    }

    /// Every packet in the connection's first `at` clear bytes has its
    /// delivery id, and the next packet takes `next_delivery`: a boundary.
    pub fn boundary(&mut self, next_delivery: u64, at: u64) {
        if at > self.latest.1 {
            self.latest = (next_delivery, at);
        }
    }

    /// The bound worker consumed through delivery `hw`.
    pub fn consumed(&mut self, hw: u64) {
        self.consumed = self.consumed.max(hw);
    }

    /// The binding moved to a worker whose consumption has not been
    /// reported yet: nothing more is acknowledged until it is.
    pub fn rebound(&mut self) {
        self.consumed = 0;
    }

    /// The acknowledgement owed now, if any: `(cursor, in_bytes)`.
    pub fn due(&mut self) -> Option<(u64, u64)> {
        if self.pending.is_none() && self.latest.1 > self.acked_bytes {
            self.pending = Some(self.latest);
        }
        let (cursor, at) = self.pending?;
        (self.consumed.saturating_add(1) >= cursor).then_some((cursor, at))
    }

    /// The transport took the acknowledgement [`due`](Self::due) named.
    pub fn acked(&mut self) {
        if let Some((_, at)) = self.pending.take() {
            self.acked_bytes = at;
        }
    }
}

impl Default for DeliveryCursor {
    fn default() -> Self {
        Self::new()
    }
}

/// What the anchor answers a production offered by the worker.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Offer {
    /// Inside the horizon: the worker may count it as given and release
    /// it. Also the answer to a re-offer of an id already accepted.
    Accepted,
    /// Submitted and waiting for the horizon (or submitted now).
    Pending,
    /// The transport is not ready: offer it again, same id.
    NotReady,
    /// Above the next id the anchor expects: one before it never
    /// arrived. Refused; the session's productions stop at the gap.
    Gap,
}

/// The anchor's production gate over an [`OutputHorizon`]: the next id it
/// will submit and the horizon the worker has been told.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ProductionGate {
    /// The next production id to submit to the transport.
    pub next_submit: u64,
    /// Highest production id accepted into the horizon.
    pub accepted: u64,
    /// Highest `accepted` the worker has been sent (`MSG_ANCHOR_HORIZON`).
    pub reported: u64,
}

impl ProductionGate {
    pub const fn new() -> Self {
        Self {
            next_submit: 1,
            accepted: 0,
            reported: 0,
        }
    }

    /// A gate resumed over a transport that already holds every
    /// production below `next_submit`: all of them are in the horizon
    /// (the standby is the horizon), none needs submitting again.
    pub const fn resumed(next_submit: u64) -> Self {
        let n = if next_submit == 0 { 1 } else { next_submit };
        Self {
            next_submit: n,
            accepted: n - 1,
            reported: 0,
        }
    }

    /// Offer production `id` (`len` bytes of output) to the transport
    /// through `h`, then read the horizon back.
    pub fn offer<H: OutputHorizon>(&mut self, h: &mut H, id: u64, len: u64) -> Offer {
        if id == 0 {
            return Offer::Gap;
        }
        if id > self.next_submit {
            return Offer::Gap;
        }
        if id == self.next_submit {
            match h.submit(id, len) {
                Submit::Pending | Submit::Duplicate => self.next_submit = id + 1,
                Submit::NotReady => return Offer::NotReady,
            }
        }
        self.observe(h);
        if id <= self.accepted {
            Offer::Accepted
        } else {
            Offer::Pending
        }
    }

    /// Read the horizon. It never exceeds what was submitted and never
    /// moves back. Returns the new horizon when it advanced.
    pub fn observe<H: OutputHorizon>(&mut self, h: &H) -> Option<u64> {
        let through = h.accepted_through().min(self.next_submit - 1);
        if through > self.accepted {
            self.accepted = through;
            Some(through)
        } else {
            None
        }
    }

    /// The horizon to report to the worker, once, when it moved past
    /// what the worker was last told.
    pub fn report(&mut self) -> Option<u64> {
        if self.accepted > self.reported {
            self.reported = self.accepted;
            Some(self.accepted)
        } else {
            None
        }
    }

    /// Productions submitted but not yet inside the horizon.
    pub fn pending(&self) -> u64 {
        (self.next_submit - 1).saturating_sub(self.accepted)
    }
}

impl Default for ProductionGate {
    fn default() -> Self {
        Self::new()
    }
}

// ── The worker's retained productions ───────────────────────────────────────

/// Bytes of one retained response the ring holds inline.
pub const RETAIN_BYTES_MAX: usize = 1024;

/// One retained production.
#[derive(Clone, Copy)]
pub struct Retained {
    pub id: u64,
    pub len: u16,
    /// Numbered but not kept: a response larger than a slot
    /// ([`ProductionRing::number_unretained`]). It cannot be re-offered.
    pub unretained: bool,
    pub bytes: [u8; RETAIN_BYTES_MAX],
}

impl Retained {
    pub const fn zero() -> Self {
        Self {
            id: 0,
            len: 0,
            unretained: false,
            bytes: [0; RETAIN_BYTES_MAX],
        }
    }

    pub fn body(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len).min(RETAIN_BYTES_MAX)]
    }
}

/// The worker's productions not yet inside the horizon, in id order, for
/// one session: what it re-offers to a replacement anchor. Bounded by
/// `N`: a full ring refuses the next production (`retain` = `None`), and
/// the worker must not emit it — the bound is backpressure, never an
/// eviction, because an evicted response is an acknowledgement lost.
pub struct ProductionRing<const N: usize> {
    slots: [Retained; N],
    head: usize,
    count: usize,
    /// The id the next production takes.
    next_id: u64,
    /// Highest id released (inside the horizon the anchor reported).
    released: u64,
}

impl<const N: usize> ProductionRing<N> {
    pub const fn new() -> Self {
        Self {
            slots: [Retained::zero(); N],
            head: 0,
            count: 0,
            next_id: 1,
            released: 0,
        }
    }

    /// A ring for a session whose productions below `next_id` are all
    /// inside the horizon (an import at a coherent point).
    pub const fn resumed(next_id: u64) -> Self {
        let mut r = Self::new();
        let n = if next_id == 0 { 1 } else { next_id };
        r.next_id = n;
        r.released = n - 1;
        r
    }

    /// The id the next production takes.
    pub fn next_id(&self) -> u64 {
        self.next_id
    }

    /// Highest id the horizon covers, as the anchor reported it.
    pub fn released(&self) -> u64 {
        self.released
    }

    /// Lowest id still retained (`next_id` when none is).
    pub fn first_retained(&self) -> u64 {
        if self.count == 0 {
            self.next_id
        } else {
            self.slots[self.head].id
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Number and keep the next production. `None` when the ring is full
    /// or the response is larger than a slot: the worker holds the
    /// response back (it has not been given) and tries again.
    pub fn retain(&mut self, frame: &[u8]) -> Option<u64> {
        if self.count == N || frame.len() > RETAIN_BYTES_MAX {
            return None;
        }
        let id = self.next_id;
        let s = &mut self.slots[(self.head + self.count) % N];
        s.id = id;
        s.len = frame.len() as u16;
        s.unretained = false;
        s.bytes[..frame.len()].copy_from_slice(frame);
        self.count += 1;
        self.next_id = id + 1;
        Some(id)
    }

    /// Number the next production without keeping its bytes: a response
    /// larger than a slot (a big PUBLISH to a subscriber, whose QoS 1/2
    /// delivery the delivery ledger owes again anyway). It takes a slot
    /// and an id like any production, so ids stay gap-free and the ring
    /// stays the bound, but it can never be re-offered: until a horizon
    /// covers it, a replacement anchor whose horizon is below it is
    /// refused (`first_offerable`). `None` when the ring is full.
    pub fn number_unretained(&mut self) -> Option<u64> {
        if self.count == N {
            return None;
        }
        let id = self.next_id;
        let s = &mut self.slots[(self.head + self.count) % N];
        s.id = id;
        s.len = 0;
        s.unretained = true;
        self.count += 1;
        self.next_id = id + 1;
        Some(id)
    }

    /// Take back production `id`, the last one numbered, which never left
    /// the worker (the channel refused it): the next production takes its
    /// id again. `false` when `id` is not the last one numbered.
    pub fn unproduce(&mut self, id: u64) -> bool {
        if self.count == 0 || id + 1 != self.next_id || id <= self.released {
            return false;
        }
        let last = (self.head + self.count - 1) % N;
        if self.slots[last].id != id {
            return false;
        }
        self.count -= 1;
        self.next_id = id;
        true
    }

    /// The lowest id from which every production up to `next_id` can be
    /// offered again: past the last unretained one still held, else the
    /// first retained. A replacement anchor's horizon must reach at least
    /// one below it.
    pub fn first_offerable(&self) -> u64 {
        let mut first = self.first_retained();
        for k in 0..self.count {
            let r = &self.slots[(self.head + k) % N];
            if r.unretained {
                first = r.id + 1;
            }
        }
        first
    }

    /// The anchor reported the horizon at `through`: every production at
    /// or below it is given. Returns how many were released. A report
    /// past what was produced is ignored — it cannot be about this ring.
    pub fn release_through(&mut self, through: u64) -> usize {
        if through >= self.next_id || through <= self.released {
            return 0;
        }
        let mut n = 0usize;
        while self.count > 0 && self.slots[self.head].id <= through {
            self.head = (self.head + 1) % N;
            self.count -= 1;
            n += 1;
        }
        self.released = through;
        n
    }

    /// The `k`-th retained production, oldest first.
    pub fn get(&self, k: usize) -> Option<&Retained> {
        if k >= self.count {
            None
        } else {
            Some(&self.slots[(self.head + k) % N])
        }
    }
}

impl<const N: usize> Default for ProductionRing<N> {
    fn default() -> Self {
        Self::new()
    }
}

// ── The anchor's continuity record (QAC1) ───────────────────────────────────

/// Magic of the anchor continuity record.
pub const QAC_MAGIC: [u8; 4] = *b"QAC1";
/// Bytes of the record, CRC included:
/// `["QAC1":4][session_id:16][epoch:u32][anchor_id:8][worker:u8]
///  [mqtt_version:u8][flags:u8][reserved:u8][tenant:u32][listener:u16]
///  [keepalive_s:u16][client_hash:u64][next_delivery:u64]
///  [next_production:u64][production_accepted:u64]
///  [checkpoint_delivery:u64][crc32:u32]`, all little-endian.
pub const QAC_LEN: usize = 4 + 16 + 4 + 8 + 4 + 4 + 2 + 2 + 8 + 8 + 8 + 8 + 8 + 4;

/// What the anchor exports for its replacement: identity, binding and
/// negotiated parameters, and where its counters stood.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct AnchorRecord {
    pub session_id: [u8; SESSION_ID_LEN],
    pub epoch: u32,
    /// The anchor that exported it: the old emitter, never the importer.
    pub anchor_id: [u8; 8],
    pub worker: u8,
    pub mqtt_version: u8,
    /// `FLAG_*` of the connection. Connect-admitted, liveness-only and
    /// ping-owed carry; the importer recomputes the rest from its own
    /// state.
    pub flags: u8,
    pub tenant: u32,
    pub listener: u16,
    pub keepalive_s: u16,
    pub client_hash: u64,
    pub next_delivery: u64,
    pub next_production: u64,
    pub production_accepted: u64,
    pub checkpoint_delivery: u64,
}

impl AnchorRecord {
    pub const fn zero() -> Self {
        Self {
            session_id: [0; SESSION_ID_LEN],
            epoch: 0,
            anchor_id: [0; 8],
            worker: 0,
            mqtt_version: 0,
            flags: 0,
            tenant: 0,
            listener: 0,
            keepalive_s: 0,
            client_hash: 0,
            next_delivery: 0,
            next_production: 0,
            production_accepted: 0,
            checkpoint_delivery: 0,
        }
    }

    /// The record of connection `c`, served by anchor `anchor_id`, with
    /// the horizon its production gate reached.
    pub fn of(c: &AnchorConn, anchor_id: &[u8; 8], production_accepted: u64) -> Self {
        Self {
            session_id: c.session_id,
            epoch: c.epoch,
            anchor_id: *anchor_id,
            worker: c.worker,
            mqtt_version: c.mqtt_version,
            flags: c.flags,
            tenant: c.tenant,
            listener: c.listener,
            keepalive_s: c.keepalive_s,
            client_hash: c.client_hash,
            next_delivery: c.next_delivery,
            next_production: c.next_production,
            production_accepted,
            checkpoint_delivery: c.checkpoint_delivery,
        }
    }

    /// Encode into `out`. Returns `QAC_LEN`, or 0 when `out` is short.
    pub fn encode(&self, out: &mut [u8]) -> usize {
        if out.len() < QAC_LEN {
            return 0;
        }
        out[..4].copy_from_slice(&QAC_MAGIC);
        out[4..20].copy_from_slice(&self.session_id);
        out[20..24].copy_from_slice(&self.epoch.to_le_bytes());
        out[24..32].copy_from_slice(&self.anchor_id);
        out[32] = self.worker;
        out[33] = self.mqtt_version;
        out[34] = self.flags;
        out[35] = 0;
        out[36..40].copy_from_slice(&self.tenant.to_le_bytes());
        out[40..42].copy_from_slice(&self.listener.to_le_bytes());
        out[42..44].copy_from_slice(&self.keepalive_s.to_le_bytes());
        out[44..52].copy_from_slice(&self.client_hash.to_le_bytes());
        out[52..60].copy_from_slice(&self.next_delivery.to_le_bytes());
        out[60..68].copy_from_slice(&self.next_production.to_le_bytes());
        out[68..76].copy_from_slice(&self.production_accepted.to_le_bytes());
        out[76..84].copy_from_slice(&self.checkpoint_delivery.to_le_bytes());
        let crc = ac_crc32(&out[..QAC_LEN - 4]);
        out[QAC_LEN - 4..QAC_LEN].copy_from_slice(&crc.to_le_bytes());
        QAC_LEN
    }

    /// Decode a record: exact length, magic, CRC, a zero reserved byte,
    /// a non-zero epoch and session, and counters that can be one
    /// anchor's (delivery and production ids from 1, the horizon below
    /// the next production).
    pub fn decode(b: &[u8]) -> Result<Self, u8> {
        if b.len() != QAC_LEN || b[..4] != QAC_MAGIC {
            return Err(AC_CORRUPT);
        }
        if ac_rd_u32(b, QAC_LEN - 4) != ac_crc32(&b[..QAC_LEN - 4]) || b[35] != 0 {
            return Err(AC_CORRUPT);
        }
        let mut sid = [0u8; SESSION_ID_LEN];
        sid.copy_from_slice(&b[4..20]);
        let mut aid = [0u8; 8];
        aid.copy_from_slice(&b[24..32]);
        let r = Self {
            session_id: sid,
            epoch: ac_rd_u32(b, 20),
            anchor_id: aid,
            worker: b[32],
            mqtt_version: b[33],
            flags: b[34],
            tenant: ac_rd_u32(b, 36),
            listener: u16::from_le_bytes([b[40], b[41]]),
            keepalive_s: u16::from_le_bytes([b[42], b[43]]),
            client_hash: ac_rd_u64(b, 44),
            next_delivery: ac_rd_u64(b, 52),
            next_production: ac_rd_u64(b, 60),
            production_accepted: ac_rd_u64(b, 68),
            checkpoint_delivery: ac_rd_u64(b, 76),
        };
        if r.epoch == 0
            || r.session_id == [0u8; SESSION_ID_LEN]
            || r.next_delivery == 0
            || r.next_production == 0
            || r.production_accepted >= r.next_production
        {
            return Err(AC_CORRUPT);
        }
        Ok(r)
    }
}

/// The standby transport's own counters for the flow, as its mirrored
/// state holds them (`MSG_CLIENT_RESUMED`): authoritative for everything
/// the horizon reached.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TransportCursors {
    /// The id the next delivery the transport hands takes: its first
    /// redelivery, when it holds any.
    pub next_delivery: u64,
    /// Highest delivery id the old anchor acknowledged to the transport
    /// as consumed: the transport hands everything after it again.
    pub delivery_acked: u64,
    /// The connection's output bytes the standby holds (the transport
    /// state's `out_bytes`): its horizon at the takeover, in the old
    /// anchor's numbering.
    pub out_bytes: u64,
}

/// A replacement anchor, imported and waiting for the worker's answer.
#[derive(Clone, Copy)]
pub struct AnchorReanchor {
    pub conn: AnchorConn,
    pub gate: ProductionGate,
    /// The epoch the replacement claims (`MSG_ANCHOR_REANCHOR`).
    pub new_epoch: u32,
    /// Deliveries at or below this were consumed by the worker: the
    /// transport does not hand them again.
    pub skip_through: u64,
    /// The output the standby holds, as `TransportCursors::out_bytes`.
    pub bytes_held: u64,
    /// The horizon re-offers are placed against (set by `apply`).
    pub horizon: TransportHorizon,
}

/// Import `rec` on the replacement anchor over the standby transport's
/// `t` (the connection's slot is the transport's id on this host). The
/// delivery numbering continues from the transport's cursors, not the
/// record's: the record was exported at a binding change, and the
/// transport's last acknowledgement may be on either side of it. Whether
/// they leave a delivery out is the worker's to judge
/// (`reanchor_verdict`); cursors that cannot be one connection's are
/// refused here (`AC_CURSOR_MISMATCH`).
pub fn anchor_import(rec: &AnchorRecord, t: &TransportCursors) -> Result<AnchorReanchor, u8> {
    if t.next_delivery == 0 || t.delivery_acked >= t.next_delivery {
        return Err(AC_CURSOR_MISMATCH);
    }
    // The replacement claims the next epoch; a session at the last one
    // has none left to claim.
    let Some(new_epoch) = rec.epoch.checked_add(1) else {
        return Err(AC_STALE_EPOCH);
    };
    let mut c = AnchorConn::zero();
    c.session_id = rec.session_id;
    c.epoch = rec.epoch;
    c.tenant = rec.tenant;
    c.listener = rec.listener;
    // Re-attaching: frames are held until the worker answers.
    c.phase = Phase::WaitAttached;
    c.worker = rec.worker;
    c.mqtt_version = rec.mqtt_version;
    c.flags = rec.flags & (FLAG_CONNECT_ADMITTED | FLAG_LIVENESS_ONLY | FLAG_PING_OWED);
    c.keepalive_s = rec.keepalive_s;
    c.client_hash = rec.client_hash;
    c.next_delivery = t.next_delivery;
    c.next_production = rec.next_production;
    // A checkpoint the old anchor's standby worker acknowledged still
    // stands: the worker side did not move.
    c.checkpoint_delivery = rec.checkpoint_delivery;
    if rec.checkpoint_delivery != 0 {
        c.set(FLAG_CKPT_VALID, true);
    }
    Ok(AnchorReanchor {
        conn: c,
        gate: ProductionGate::resumed(rec.production_accepted + 1),
        new_epoch,
        skip_through: t.delivery_acked,
        bytes_held: t.out_bytes,
        horizon: TransportHorizon::new(),
    })
}

// ── The re-anchor exchange (SessionCtrlV1 sideband) ─────────────────────────

/// `MSG_ANCHOR_REANCHOR`, anchor -> worker:
/// `[session_id:16][new_epoch:u32 LE][anchor_id:8][next_delivery:u64 LE]
///  [bytes_held:u64 LE]` — the SessionCtrlV1 session header,
/// then the replacement's identity and cursors: the first delivery it
/// hands the worker, and the output the standby holds.
pub const REANCHOR_LEN: usize = SESSION_ID_LEN + 4 + 8 + 8 + 8;
/// `MSG_ANCHOR_REANCHORED`, worker -> anchor:
/// `[session_id:16][new_epoch:u32 LE][status:u8][delivery_hw:u64 LE]
///  [next_production:u64 LE][released:u64 LE][released_bytes:u64 LE]`.
pub const REANCHORED_LEN: usize = SESSION_ID_LEN + 4 + 1 + 8 + 8 + 8 + 8;
/// `MSG_ANCHOR_HORIZON`, anchor -> worker:
/// `[session_id:16][epoch:u32 LE][accepted_through:u64 LE]
///  [bytes_through:u64 LE]`.
pub const HORIZON_LEN: usize = SESSION_ID_LEN + 4 + 8 + 8;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Reanchor {
    pub session_id: [u8; SESSION_ID_LEN],
    pub new_epoch: u32,
    pub anchor_id: [u8; 8],
    /// The first delivery the replacement hands the worker: the transport
    /// hands every one from it again.
    pub next_delivery: u64,
    /// The connection's output horizon this host's transport holds, in the
    /// consumer's bytes (`MSG_HORIZON` at the takeover): everything the old
    /// anchor wrote up to it reaches the client from here.
    pub bytes_held: u64,
}

impl Reanchor {
    pub fn encode(&self, out: &mut [u8]) -> usize {
        if out.len() < REANCHOR_LEN {
            return 0;
        }
        out[..16].copy_from_slice(&self.session_id);
        out[16..20].copy_from_slice(&self.new_epoch.to_le_bytes());
        out[20..28].copy_from_slice(&self.anchor_id);
        out[28..36].copy_from_slice(&self.next_delivery.to_le_bytes());
        out[36..44].copy_from_slice(&self.bytes_held.to_le_bytes());
        REANCHOR_LEN
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != REANCHOR_LEN {
            return None;
        }
        let mut sid = [0u8; SESSION_ID_LEN];
        sid.copy_from_slice(&b[..16]);
        let mut aid = [0u8; 8];
        aid.copy_from_slice(&b[20..28]);
        Some(Self {
            session_id: sid,
            new_epoch: ac_rd_u32(b, 16),
            anchor_id: aid,
            next_delivery: ac_rd_u64(b, 28),
            bytes_held: ac_rd_u64(b, 36),
        })
    }
}

impl AnchorReanchor {
    /// The request this replacement sends, as anchor `anchor_id`.
    pub fn request(&self, anchor_id: &[u8; 8]) -> Reanchor {
        Reanchor {
            session_id: self.conn.session_id,
            new_epoch: self.new_epoch,
            anchor_id: *anchor_id,
            next_delivery: self.skip_through + 1,
            bytes_held: self.bytes_held,
        }
    }

    /// The worker answered. On `AC_OK` the connection is live at the new
    /// epoch, deliveries at or below the worker's high-water are
    /// duplicates, and the gate expects the worker's re-offers from the
    /// horizon up.
    /// Anything else — a refusal, an answer for another session or epoch,
    /// or cursors this anchor cannot be on the same session with — is
    /// returned as the status the anchor closes the connection on.
    pub fn apply(&mut self, r: &Reanchored) -> u8 {
        if r.session_id != self.conn.session_id || r.new_epoch != self.new_epoch {
            return AC_UNKNOWN_SESSION;
        }
        if r.status != AC_OK {
            return r.status;
        }
        if self.skip_through > r.delivery_hw
            || r.released_bytes > self.bytes_held
            || r.released >= r.next_production
        {
            return AC_CURSOR_MISMATCH;
        }
        // The worker's release is where the re-offers' numbering starts:
        // what it released the transport holds; above it, each re-offered
        // production is either inside what the transport holds (given, not
        // sent again) or past it (sent).
        self.gate = ProductionGate::resumed(r.released + 1);
        self.horizon = TransportHorizon::resumed(self.bytes_held, r.released, r.released_bytes);
        self.conn.epoch = self.new_epoch;
        self.conn.phase = Phase::Active;
        self.skip_through = self.skip_through.max(r.delivery_hw);
        AC_OK
    }

    /// Whether delivery `id` is still owed to the worker.
    pub fn owes(&self, id: u64) -> bool {
        id > self.skip_through
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Reanchored {
    pub session_id: [u8; SESSION_ID_LEN],
    pub new_epoch: u32,
    pub status: u8,
    pub delivery_hw: u64,
    pub next_production: u64,
    /// The highest production the worker released (a horizon covered it),
    /// and the bytes that horizon reached: where the replacement's
    /// numbering of re-offered productions starts.
    pub released: u64,
    pub released_bytes: u64,
}

impl Reanchored {
    pub fn encode(&self, out: &mut [u8]) -> usize {
        if out.len() < REANCHORED_LEN {
            return 0;
        }
        out[..16].copy_from_slice(&self.session_id);
        out[16..20].copy_from_slice(&self.new_epoch.to_le_bytes());
        out[20] = self.status;
        out[21..29].copy_from_slice(&self.delivery_hw.to_le_bytes());
        out[29..37].copy_from_slice(&self.next_production.to_le_bytes());
        out[37..45].copy_from_slice(&self.released.to_le_bytes());
        out[45..53].copy_from_slice(&self.released_bytes.to_le_bytes());
        REANCHORED_LEN
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != REANCHORED_LEN {
            return None;
        }
        let mut sid = [0u8; SESSION_ID_LEN];
        sid.copy_from_slice(&b[..16]);
        Some(Self {
            session_id: sid,
            new_epoch: ac_rd_u32(b, 16),
            status: b[20],
            delivery_hw: ac_rd_u64(b, 21),
            next_production: ac_rd_u64(b, 29),
            released: ac_rd_u64(b, 37),
            released_bytes: ac_rd_u64(b, 45),
        })
    }
}

/// The session as the worker holds it, for a re-anchor decision.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct WorkerSeat {
    pub session_id: [u8; SESSION_ID_LEN],
    pub epoch: u32,
    /// Highest delivery id consumed.
    pub delivery_hw: u64,
    /// The id its next production takes.
    pub next_production: u64,
    /// Highest production id released by a horizon report, and the
    /// connection's bytes through its end.
    pub released: u64,
    pub released_bytes: u64,
    /// The lowest id from which every production can be offered again
    /// (`ProductionRing::first_offerable`).
    pub first_offerable: u64,
}

/// The worker's decision on `MSG_ANCHOR_REANCHOR`. `AC_OK` only when the
/// replacement names this session at a higher epoch, its transport hands
/// again every delivery the worker has not consumed (else one was
/// acknowledged to the transport and lost before it got here), its horizon
/// claims no production the worker never made, and every production
/// above its horizon is one the worker still retains (else a response
/// the client may never see is gone). The answer always carries the
/// worker's own cursors.
pub fn reanchor_verdict(seat: &WorkerSeat, req: &Reanchor) -> Reanchored {
    let status = if req.session_id != seat.session_id || seat.epoch == 0 {
        AC_UNKNOWN_SESSION
    } else if req.new_epoch <= seat.epoch {
        AC_STALE_EPOCH
    } else if req.next_delivery > seat.delivery_hw.saturating_add(1)
        || req.bytes_held < seat.released_bytes
        || seat.first_offerable > seat.released + 1
    {
        AC_CURSOR_MISMATCH
    } else {
        AC_OK
    };
    Reanchored {
        session_id: req.session_id,
        new_epoch: req.new_epoch,
        status,
        delivery_hw: seat.delivery_hw,
        next_production: seat.next_production,
        released: seat.released,
        released_bytes: seat.released_bytes,
    }
}

/// A decoded `MSG_ANCHOR_HORIZON`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HorizonReport {
    pub session_id: [u8; SESSION_ID_LEN],
    pub epoch: u32,
    pub accepted_through: u64,
    /// The connection's bytes the transport's horizon reached when
    /// `accepted_through` entered it (the end of that production).
    pub bytes_through: u64,
}

impl HorizonReport {
    pub fn encode(&self, out: &mut [u8]) -> usize {
        if out.len() < HORIZON_LEN {
            return 0;
        }
        out[..16].copy_from_slice(&self.session_id);
        out[16..20].copy_from_slice(&self.epoch.to_le_bytes());
        out[20..28].copy_from_slice(&self.accepted_through.to_le_bytes());
        out[28..36].copy_from_slice(&self.bytes_through.to_le_bytes());
        HORIZON_LEN
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != HORIZON_LEN {
            return None;
        }
        let mut sid = [0u8; SESSION_ID_LEN];
        sid.copy_from_slice(&b[..16]);
        Some(Self {
            session_id: sid,
            epoch: ac_rd_u32(b, 16),
            accepted_through: ac_rd_u64(b, 20),
            bytes_through: ac_rd_u64(b, 28),
        })
    }
}

/// Whether the worker bound at `(sid, epoch)` takes horizon report `h`:
/// its session, its current epoch. A report from an anchor the session
/// has left (a stale emitter's) is ignored.
pub fn horizon_admitted(sid: &[u8; SESSION_ID_LEN], epoch: u32, h: &HorizonReport) -> bool {
    &h.session_id == sid && epoch != 0 && h.epoch == epoch
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn ac_rd_u32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

fn ac_rd_u64(b: &[u8], at: usize) -> u64 {
    let mut x = [0u8; 8];
    x.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(x)
}

/// CRC32 (IEEE 802.3, reflected 0xEDB88320) — the polynomial the
/// SessionCtrlV1 handoff frames use, so the record is self-checking on
/// whatever carries it.
pub fn ac_crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= u32::from(b);
        let mut i = 0;
        while i < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            i += 1;
        }
    }
    !crc
}
