//! The provider half of a SUBSCRIPTION: a held exchange per subscriber.
//!
//! A subscription is a request — `GET`, `target` = an MQTT topic filter —
//! answered by one response HEAD flagged `HOLD | MORE` once the broker has
//! taken the subscription, then one BODY record per message, never past the
//! response credit the requester has granted (the credit IS the subscriber's
//! backpressure). The exchange ends with an ABORT from the requester, or with
//! one the provider writes when the broker can no longer carry it.
//!
//! Each subscription is its own broker session. This file holds everything
//! that is not I/O: judging the request, the subscription's phase, its
//! credit, the messages it holds until credit admits them, and the records
//! owed on the response port. The module (`broker_front`) writes the session
//! envelopes this asks for and places the records it produces.
//!
//! A message travels as one BODY whose data is
//! `[topic_len u16 BE][topic][payload_len u32 BE][payload]`: the topic as an
//! MQTT PUBLISH carries it, and the payload byte for byte what an MQTT
//! subscriber receives. The framing is self-delimiting, so it survives a
//! relay that joins BODY records into one byte stream (an HTTP/1.1 response
//! with no declared length, say).
//!
//! `#[path]`-mounted by `broker_front` and by the host tests, beside
//! `publish_exchange.rs`, whose mount of the SDK contract it shares. It does
//! no I/O.

#![allow(
    dead_code,
    reason = "shared provider; the module and the host tests each use a subset"
)]

// The SDK contract is mounted once per crate, by `publish_exchange.rs`
// (a second mount would be a second set of types); a crate that mounts this
// file mounts that one beside it, at its root.
pub use super::publish_exchange::exchange;

use exchange::{
    abort, flag, header, parse_request, status, write_abort, write_body, write_refusal,
    write_response_head, ExchangeId, Record, ResponseHead, BODY_MAX, METHOD_GET,
};

/// The longest topic filter a subscription names.
pub const FILTER_MAX: usize = 256;
/// The longest session name the `session` header carries; it is the MQTT
/// client id of the durable session, so it holds to the same bound.
pub const SESSION_MAX: usize = 64;
/// Request header naming a durable session to create or resume.
pub const SESSION_HEADER: &[u8] = b"session";
/// The content type of a subscription's response.
pub const CONTENT_TYPE: &[u8] = b"application/vnd.quantum.message";
/// Refusal: a durable session already has a live subscription.
pub const CONFLICT: u16 = 409;

/// Response HEAD header block: whether the broker held the session.
const PRESENT_YES: &[u8] = b"session-present: 1\r\n";
const PRESENT_NO: &[u8] = b"session-present: 0\r\n";

/// Held-message entry header: `[framed_len u16 LE][qos u8][packet_id u16 LE]`.
const ENTRY_HDR: usize = 5;

/// The bytes one message occupies as a BODY's data.
pub const fn framed_len(topic_len: usize, payload_len: usize) -> usize {
    2 + topic_len + 4 + payload_len
}

/// The largest payload one BODY carries on `topic_len` bytes of topic.
pub const fn payload_room(topic_len: usize) -> usize {
    BODY_MAX.saturating_sub(framed_len(topic_len, 0))
}

/// The hold that admits two of the largest messages a BODY carries.
pub const HOLD_TWO: usize = 2 * (ENTRY_HDR + BODY_MAX);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    Free,
    /// CONNECT written, CONNACK awaited.
    Connecting,
    /// SUBSCRIBE written, SUBACK awaited.
    Subscribing,
    /// Subscribed: the HEAD is owed or written, messages flow.
    Live,
    /// Ended: the slot's last record is owed, then it is free.
    Ending,
}

/// What a request record asks of the module.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Taken {
    Nothing,
    /// A subscription opened in slot `at`: write its CONNECT.
    Open(usize),
    /// A subscription ended: write a DISCONNECT for session `conn`.
    Close(u16),
}

/// What a delivery asks of the module.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Delivered {
    /// Held until credit admits it.
    Held,
    /// No subscription holds that session.
    Unknown,
    /// The subscription ended over it: write a DISCONNECT for `conn`, and
    /// a PUBACK for `ack` when one is given.
    Ended { conn: u16, ack: Option<u16> },
}

/// One record placed on the response port, and what it owes the broker.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Emitted {
    pub len: usize,
    /// A delivery passed on: PUBACK `(conn, packet_id)`.
    pub ack: Option<(u16, u16)>,
}

#[derive(Clone, Copy)]
struct Sub<const HOLD: usize> {
    phase: Phase,
    id: ExchangeId,
    conn: u16,
    credit: u32,
    deadline_ms: u64,
    present: bool,
    head_owed: bool,
    /// Before the HEAD: the refusal status owed. After it: the ABORT
    /// reason owed. 0 for none.
    end_status: u16,
    filter: [u8; FILTER_MAX],
    filter_len: u16,
    session: [u8; SESSION_MAX],
    session_len: u8,
    hold: [u8; HOLD],
    hold_len: usize,
}

impl<const HOLD: usize> Sub<HOLD> {
    const EMPTY: Self = Sub {
        phase: Phase::Free,
        id: ExchangeId::NONE,
        conn: 0,
        credit: 0,
        deadline_ms: 0,
        present: false,
        head_owed: false,
        end_status: 0,
        filter: [0; FILTER_MAX],
        filter_len: 0,
        session: [0; SESSION_MAX],
        session_len: 0,
        hold: [0; HOLD],
        hold_len: 0,
    };

    fn open(&self) -> bool {
        !matches!(self.phase, Phase::Free | Phase::Ending)
    }

    /// End the subscription: a refusal before its HEAD, an ABORT after.
    fn end(&mut self, refusal: u16, reason: u8) {
        self.end_status = if self.head_owed || self.phase != Phase::Live {
            refusal
        } else {
            u16::from(reason)
        };
        self.head_owed = false;
        self.phase = Phase::Ending;
        self.hold_len = 0;
    }
}

/// A refusal owed to a request that never held a slot.
#[derive(Clone, Copy)]
struct Refusal {
    id: ExchangeId,
    status: u16,
}

/// The subscriptions one front serves: `SLOTS` at once, each holding up to
/// `HOLD` bytes of messages credit has not yet admitted.
pub struct Subscriptions<const SLOTS: usize, const HOLD: usize> {
    subs: [Sub<HOLD>; SLOTS],
    refusals: [Refusal; SLOTS],
    refusals_len: usize,
    conn_first: u16,
    conn_count: u16,
    conn_next: u16,
    turn: usize,
    timeout_ms: u64,
}

impl<const SLOTS: usize, const HOLD: usize> Subscriptions<SLOTS, HOLD> {
    /// The hold admits at least one of the largest messages a BODY carries.
    const HOLD_FITS: () = assert!(HOLD >= ENTRY_HDR + BODY_MAX);

    pub const fn new() -> Self {
        let () = Self::HOLD_FITS;
        Subscriptions {
            subs: [Sub::EMPTY; SLOTS],
            refusals: [Refusal {
                id: ExchangeId::NONE,
                status: 0,
            }; SLOTS],
            refusals_len: 0,
            conn_first: 0,
            conn_count: 0,
            conn_next: 0,
            turn: 0,
            timeout_ms: 0,
        }
    }

    /// Return to the state of [`Subscriptions::new`], serving sessions on
    /// connection ids `conn_first ..= conn_first + conn_count - 1` (a range
    /// wider than `SLOTS`, so a free id always exists), and
    /// refusing a subscription the broker has not taken within
    /// `timeout_ms`. A new subscription takes the next id in turn, so an id
    /// is reused only after the whole range: an answer still in flight for
    /// an ended session is never read as the next one's.
    pub fn reset(&mut self, conn_first: u16, conn_count: u16, timeout_ms: u64) {
        let () = Self::HOLD_FITS;
        for s in self.subs.iter_mut() {
            *s = Sub::EMPTY;
        }
        self.refusals_len = 0;
        self.conn_first = conn_first;
        self.conn_count = conn_count;
        self.conn_next = 0;
        self.turn = 0;
        self.timeout_ms = timeout_ms;
    }

    /// Whether a request record may be read: whatever it owes has room.
    pub fn can_take(&self) -> bool {
        self.refusals_len < SLOTS
    }

    /// Subscriptions open.
    pub fn live(&self) -> usize {
        self.subs.iter().filter(|s| s.open()).count()
    }

    // ── Requests ─────────────────────────────────────────────────────

    /// Take one request record.
    pub fn accept(&mut self, record: &[u8], now_ms: u64) -> Taken {
        match parse_request(record) {
            Some(Record::Head(h)) => {
                let refusal = self.judge(h.method, h.flags, h.target, h.headers);
                if let Some(code) = refusal {
                    self.refuse(h.id, code);
                    return Taken::Nothing;
                }
                if self.find_id(&h.id).is_some() {
                    self.refuse(h.id, status::BAD_REQUEST);
                    return Taken::Nothing;
                }
                let session = header(h.headers, SESSION_HEADER).unwrap_or(&[]);
                if !session.is_empty() && self.find_session(session).is_some() {
                    self.refuse(h.id, CONFLICT);
                    return Taken::Nothing;
                }
                let Some(at) = self.subs.iter().position(|s| s.phase == Phase::Free) else {
                    self.refuse(h.id, status::BUSY);
                    return Taken::Nothing;
                };
                let conn = self.next_conn();
                let s = &mut self.subs[at];
                *s = Sub::EMPTY;
                s.phase = Phase::Connecting;
                s.id = h.id;
                s.conn = conn;
                s.credit = h.resp_credit;
                s.deadline_ms = now_ms.saturating_add(self.timeout_ms);
                put(&mut s.filter, h.target);
                s.filter_len = h.target.len() as u16;
                put(&mut s.session, session);
                s.session_len = session.len() as u8;
                Taken::Open(at)
            }
            Some(Record::Credit { id, bytes }) => {
                if let Some(at) = self.find_id(&id) {
                    let s = &mut self.subs[at];
                    s.credit = s.credit.saturating_add(bytes);
                }
                Taken::Nothing
            }
            Some(Record::Abort { id, .. }) => {
                // Nothing further is written for an aborted exchange: its
                // owed refusal goes, and so does an ended subscription's
                // last record (its session was closed when it ended).
                self.refusals_drop(&id);
                let Some(at) = self
                    .subs
                    .iter()
                    .position(|s| s.phase != Phase::Free && s.id == id)
                else {
                    return Taken::Nothing;
                };
                let s = core::mem::replace(&mut self.subs[at], Sub::EMPTY);
                if s.phase == Phase::Ending {
                    Taken::Nothing
                } else {
                    Taken::Close(s.conn)
                }
            }
            // A subscription carries no request body, and the rest names
            // nothing this provider holds.
            _ => Taken::Nothing,
        }
    }

    /// The refusal a subscription request is owed, or `None` to take it.
    fn judge(&self, method: u8, flags: u8, target: &[u8], headers: &[u8]) -> Option<u16> {
        if method != METHOD_GET || flags != 0 {
            return Some(status::BAD_REQUEST);
        }
        if target.len() > FILTER_MAX {
            return Some(status::TOO_LARGE);
        }
        if !valid_filter(target) {
            return Some(status::BAD_REQUEST);
        }
        if let Some(session) = header(headers, SESSION_HEADER) {
            if session.len() > SESSION_MAX {
                return Some(status::TOO_LARGE);
            }
            if session.is_empty() || !session.iter().all(|c| (0x21..=0x7E).contains(c)) {
                return Some(status::BAD_REQUEST);
            }
        }
        None
    }

    fn next_conn(&mut self) -> u16 {
        // Skip ids an open subscription still holds; the range is wider
        // than the slot count, so one is always free.
        loop {
            let conn = self.conn_first.wrapping_add(self.conn_next);
            self.conn_next = (self.conn_next + 1) % self.conn_count.max(1);
            if !self
                .subs
                .iter()
                .any(|s| s.phase != Phase::Free && s.conn == conn)
            {
                return conn;
            }
        }
    }

    // ── The slot a subscription holds ────────────────────────────────

    pub fn conn(&self, at: usize) -> u16 {
        self.subs[at].conn
    }

    pub fn filter(&self, at: usize) -> &[u8] {
        &self.subs[at].filter[..usize::from(self.subs[at].filter_len)]
    }

    /// The durable session's name, or `None` for a live-only subscription.
    pub fn session(&self, at: usize) -> Option<&[u8]> {
        let s = &self.subs[at];
        (s.session_len > 0).then(|| &s.session[..usize::from(s.session_len)])
    }

    pub fn phase(&self, at: usize) -> Phase {
        self.subs[at].phase
    }

    pub fn credit(&self, at: usize) -> u32 {
        self.subs[at].credit
    }

    pub fn held_bytes(&self, at: usize) -> usize {
        self.subs[at].hold_len
    }

    /// The slot open on session `conn`.
    pub fn slot_of(&self, conn: u16) -> Option<usize> {
        self.subs.iter().position(|s| s.open() && s.conn == conn)
    }

    // ── The broker's answers ─────────────────────────────────────────

    /// The broker answered a subscription's CONNECT. `Some(at)`: write its
    /// SUBSCRIBE. A refused session is refused to the requester, 502.
    pub fn on_connack(
        &mut self,
        conn: u16,
        present: bool,
        reason: u8,
        now_ms: u64,
    ) -> Option<usize> {
        let at = self.slot_of(conn)?;
        let s = &mut self.subs[at];
        if s.phase != Phase::Connecting {
            return None;
        }
        if reason != 0 {
            s.end(status::BAD_GATEWAY, 0);
            return None;
        }
        s.present = present;
        s.phase = Phase::Subscribing;
        s.deadline_ms = now_ms.saturating_add(self.timeout_ms);
        Some(at)
    }

    /// The broker answered a subscription's SUBSCRIBE. The HEAD is owed
    /// when it granted the filter; otherwise the request is refused 502
    /// and `Some(conn)` names the session to DISCONNECT.
    pub fn on_suback(&mut self, conn: u16, granted: u8) -> Option<u16> {
        let at = self.slot_of(conn)?;
        let s = &mut self.subs[at];
        if s.phase != Phase::Subscribing {
            return None;
        }
        if granted >= 0x80 {
            s.end(status::BAD_GATEWAY, 0);
            return Some(conn);
        }
        s.phase = Phase::Live;
        s.head_owed = true;
        None
    }

    /// The broker delivered a message on session `conn`.
    pub fn on_publish(
        &mut self,
        conn: u16,
        qos: u8,
        packet_id: u16,
        topic: &[u8],
        payload: &[u8],
    ) -> Delivered {
        let Some(at) = self.slot_of(conn) else {
            return Delivered::Unknown;
        };
        let s = &mut self.subs[at];
        let ack = (qos > 0).then_some(packet_id);
        let framed = framed_len(topic.len(), payload.len());
        if framed > BODY_MAX {
            // No BODY can carry it. Acknowledged, so a durable session is
            // not handed the same message on every resume.
            s.end(status::BAD_GATEWAY, abort::TOO_LARGE);
            return Delivered::Ended { conn, ack };
        }
        if s.hold_len + ENTRY_HDR + framed > HOLD {
            // Only a QoS 0 stream can outrun the hold: QoS 1 deliveries
            // are capped by the session's receive maximum. The subscriber
            // has stopped crediting what it is sent; dropping messages
            // under it would be silent, so the subscription ends.
            s.end(status::BAD_GATEWAY, abort::STALLED);
            return Delivered::Ended { conn, ack: None };
        }
        let at = s.hold_len;
        s.hold[at..at + 2].copy_from_slice(&(framed as u16).to_le_bytes());
        s.hold[at + 2] = qos;
        s.hold[at + 3..at + 5].copy_from_slice(&packet_id.to_le_bytes());
        let d = at + ENTRY_HDR;
        s.hold[d..d + 2].copy_from_slice(&(topic.len() as u16).to_be_bytes());
        put(&mut s.hold[d + 2..], topic);
        let p = d + 2 + topic.len();
        s.hold[p..p + 4].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        put(&mut s.hold[p + 4..], payload);
        s.hold_len = d + framed;
        Delivered::Held
    }

    /// The broker ended session `conn`: its subscription ends with it.
    pub fn on_disconnect(&mut self, conn: u16) -> bool {
        match self.slot_of(conn) {
            Some(at) => {
                self.subs[at].end(status::BAD_GATEWAY, abort::FAILED);
                true
            }
            None => false,
        }
    }

    /// One subscription the broker has not taken in time: refused 504, and
    /// `Some(conn)` names its session to DISCONNECT. Called until `None`.
    pub fn expire(&mut self, now_ms: u64) -> Option<u16> {
        let s = self.subs.iter_mut().find(|s| {
            matches!(s.phase, Phase::Connecting | Phase::Subscribing) && now_ms >= s.deadline_ms
        })?;
        s.end(status::TIMEOUT, 0);
        Some(s.conn)
    }

    // ── Records owed ─────────────────────────────────────────────────

    /// Encode the next record owed on the response port into `out`.
    /// `may_ack` says whether a PUBACK can be written now: a delivery that
    /// owes one waits until it can. `None` when nothing is owed.
    pub fn next_record(&mut self, out: &mut [u8], may_ack: bool) -> Option<Emitted> {
        if self.refusals_len > 0 {
            let r = self.refusals[0];
            let len = write_refusal(&r.id, r.status, out)?;
            self.refusals.copy_within(1..self.refusals_len, 0);
            self.refusals_len -= 1;
            return Some(Emitted { len, ack: None });
        }
        for k in 0..SLOTS {
            let at = (self.turn + k) % SLOTS;
            if let Some(e) = self.record_for(at, out, may_ack) {
                self.turn = (at + 1) % SLOTS;
                return Some(e);
            }
        }
        None
    }

    fn record_for(&mut self, at: usize, out: &mut [u8], may_ack: bool) -> Option<Emitted> {
        let s = &mut self.subs[at];
        match s.phase {
            Phase::Ending => {
                let len = if s.end_status >= 100 {
                    write_refusal(&s.id, s.end_status, out)?
                } else {
                    write_abort(&s.id, s.end_status as u8, out)?
                };
                *s = Sub::EMPTY;
                Some(Emitted { len, ack: None })
            }
            Phase::Live if s.head_owed => {
                let headers = match s.session_len {
                    0 => &[][..],
                    _ if s.present => PRESENT_YES,
                    _ => PRESENT_NO,
                };
                let len = write_response_head(
                    &ResponseHead {
                        id: s.id,
                        flags: flag::HOLD | flag::MORE,
                        status: status::OK,
                        content_type: CONTENT_TYPE,
                        headers,
                        body: &[],
                    },
                    out,
                )?;
                s.head_owed = false;
                Some(Emitted { len, ack: None })
            }
            Phase::Live if s.hold_len > 0 => {
                let framed = usize::from(u16::from_le_bytes([s.hold[0], s.hold[1]]));
                let qos = s.hold[2];
                let packet_id = u16::from_le_bytes([s.hold[3], s.hold[4]]);
                if framed as u32 > s.credit || (qos > 0 && !may_ack) {
                    return None;
                }
                let len = write_body(
                    &s.id,
                    flag::MORE,
                    &s.hold[ENTRY_HDR..ENTRY_HDR + framed],
                    out,
                )?;
                let used = ENTRY_HDR + framed;
                s.hold.copy_within(used..s.hold_len, 0);
                s.hold_len -= used;
                s.credit -= framed as u32;
                Some(Emitted {
                    len,
                    ack: (qos > 0).then_some((s.conn, packet_id)),
                })
            }
            _ => None,
        }
    }

    // ── Internals ────────────────────────────────────────────────────

    fn find_id(&self, id: &ExchangeId) -> Option<usize> {
        self.subs.iter().position(|s| s.open() && s.id == *id)
    }

    fn find_session(&self, name: &[u8]) -> Option<usize> {
        self.subs.iter().position(|s| {
            s.open() && s.session_len > 0 && &s.session[..usize::from(s.session_len)] == name
        })
    }

    fn refuse(&mut self, id: ExchangeId, status: u16) {
        // `can_take` keeps a slot free for the one refusal a read can owe.
        if self.refusals_len < SLOTS {
            self.refusals[self.refusals_len] = Refusal { id, status };
            self.refusals_len += 1;
        }
    }

    fn refusals_drop(&mut self, id: &ExchangeId) {
        let mut kept = 0;
        for i in 0..self.refusals_len {
            if self.refusals[i].id != *id {
                self.refusals[kept] = self.refusals[i];
                kept += 1;
            }
        }
        self.refusals_len = kept;
    }
}

impl<const SLOTS: usize, const HOLD: usize> Default for Subscriptions<SLOTS, HOLD> {
    fn default() -> Self {
        Self::new()
    }
}

/// Copy `src` to the head of `dst`, holding to the shorter of the two
/// rather than linking a panic path; the callers size `dst` first.
fn put(dst: &mut [u8], src: &[u8]) {
    for (d, s) in dst.iter_mut().zip(src) {
        *d = *s;
    }
}

/// Whether `filter` is an MQTT topic filter (MQTT 3.1.1 §4.7): non-empty,
/// no NUL, `+` alone in its level, `#` alone in the last level.
pub fn valid_filter(filter: &[u8]) -> bool {
    if filter.is_empty() {
        return false;
    }
    let last = filter.len() - 1;
    // Bytes in the current level, and whether a wildcard is among them.
    let mut level = 0usize;
    let mut wild = false;
    for (i, &c) in filter.iter().enumerate() {
        match c {
            0 => return false,
            b'/' => {
                if wild && level != 1 {
                    return false;
                }
                level = 0;
                wild = false;
            }
            b'+' | b'#' => {
                if c == b'#' && i != last {
                    return false;
                }
                wild = true;
                level += 1;
            }
            _ => level += 1,
        }
    }
    !wild || level == 1
}
