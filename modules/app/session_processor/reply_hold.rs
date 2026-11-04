//! reply_hold — client responses the codec bus refused, held in order
//! until it takes them.
//!
//! A response (`MSG_SESSION_RESPONSE`: an MQTT PUBACK / PUBREC / PUBCOMP /
//! SUBACK / CONNACK / DISCONNECT, a Kafka response, an AMQP Basic.Ack /
//! Nack, ...) is the only word a client gets about the work it handed
//! over. When the output edge has no room for one, writing it later is
//! the only correct thing to do: a PUBACK lost is a publish the client
//! resends after its own timeout, a DISCONNECT lost is a client left
//! waiting on a flow that failed, a Nack lost is a confirm-mode publisher
//! waiting for ever.
//!
//! So a response the bus refuses is held here, one ring per output
//! (`codec_out`, `codec2_out`, `front_out`), and written from the ring,
//! oldest first, as the bus takes it. Order on a session is order on its
//! output — a session's responses all leave on one output — so a ring
//! that holds anything takes every later response for that output behind
//! it: no response overtakes one held before it.
//!
//! The rings are bounded, and what keeps them from filling depends on
//! whether the input that answers can wait:
//!
//! - An input whose sender holds what it has not delivered — a client's
//!   codec envelopes, `flow`'s completions — is read only while the ring
//!   it answers on has [`has_reserve`] (room for the most one input can
//!   answer: `RESERVE_BYTES` in at most `RESERVE_ENTRIES` responses). Its
//!   answers ([`Reply::Must`]) never meet a full ring; the input waits in
//!   its channel, and the pressure goes back to the client.
//! - An input that cannot wait — a committed entry (no commit waits on a
//!   client), Clustor's assignments and refusals (it drops what its edge
//!   refuses), a cold read, the expiry sweep — answers with
//!   [`Reply::Kept`]: held while the ring has room, else refused, and the
//!   caller keeps the operation with its answer owed in its own table
//!   (the Kafka / AMQP inflight slot, the cold-fetch slot) and answers it
//!   on a later step.
//! - A DISCONNECT the ring has no room for is owed in [`Owed`], one per
//!   connection, with its reason — as are the "server moved" ones a
//!   placement update owes every session it releases, which no ring
//!   could hold. It is written once its output holds nothing ahead of it
//!   for the connection, and nothing later for the connection is written
//!   at all: the DISCONNECT ends what the connection is told.

//! ## Per-step bound
//!
//! Every entry point is O(entries) over fixed tables; [`flush`] stops at
//! the first write the bus refuses.

use super::abi::SyscallTable;
use super::wire;

/// The outputs a response can leave on: `codec_out`, `codec2_out`,
/// `front_out`.
pub const OUTPUTS: usize = 3;
/// Bytes one output's ring holds.
pub const RING_BYTES: usize = 32768;
/// Responses one output's ring holds.
pub const RING_ENTRIES: usize = 256;
/// The largest response payload a channel message can carry (envelope
/// header excluded). A larger one could never be written, so it is never
/// held: it would stop its ring for good.
pub const FRAME_MAX: usize = super::abi::CHANNEL_BUFFER_SIZE - wire::ENVELOPE_HDR;
/// The most one input can answer, in bytes: one response of any size
/// plus a few small ones (a CONNACK and a DISCONNECT for another
/// connection, say).
pub const RESERVE_BYTES: usize = FRAME_MAX + 256;
/// The most responses one input can answer.
pub const RESERVE_ENTRIES: usize = 4;

/// How a response meets an output with no room for it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reply {
    /// The client's only word about work it handed over (an ack, a
    /// refusal, a request's answer): held in order until the bus takes
    /// it. Every input that can cause one is read only while the rings
    /// have [`has_reserve`], so it is never refused.
    Must,
    /// Work the caller still owns and offers again on its own (a
    /// delivery, a Basic.Deliver): refused, so it stays where it is —
    /// and refused, too, while anything is held ahead of it, so it never
    /// overtakes a held response.
    Offer,
    /// A resend a CONNECT owes: held when the ring keeps its reserve
    /// afterwards ([`has_reserve_after`]), else refused and left owed.
    Spare,
    /// An answer decided where the input cannot wait for room (a
    /// committed entry, a Clustor refusal, a cold read, the expiry sweep):
    /// held when the ring has room, else refused — the caller keeps the
    /// operation, its answer owed in its own table, and answers it again
    /// on a later step. Never counted lost.
    Kept,
}

/// What [`respond`] did with a response.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Sent {
    /// Written to the bus now.
    Written,
    /// Held, behind what the ring held.
    Held,
    /// Not taken: an [`Reply::Offer`] / [`Reply::Spare`] with no room, or
    /// a payload no channel message can carry.
    Refused,
    /// A [`Reply::Must`] the ring had no room for: lost. The input gates
    /// make this unreachable; it is counted.
    Full,
}

impl Sent {
    /// Whether the response was written or will be.
    pub fn taken(self) -> bool {
        matches!(self, Sent::Written | Sent::Held)
    }
}

/// Write `payload` (a `msg_type` envelope's payload for connection
/// `conn`) to `chan` through ring `r`: what the ring holds is written
/// first, as far as the bus takes it; then `payload` is written at once
/// when nothing is held ahead of it and the bus takes it, else met as
/// `reply` says. `written(conn, bytes, counted)` is told of every write
/// made, `bytes` with the envelope header; `counted` is carried from the
/// response's own call.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
#[allow(
    clippy::too_many_arguments,
    reason = "the response, where it goes, how a refusal is met and who hears of each write"
)]
pub unsafe fn respond(
    r: &mut Ring,
    sys: &SyscallTable,
    chan: i32,
    conn: u16,
    msg_type: u8,
    payload: &[u8],
    reply: Reply,
    counted: bool,
    mut written: impl FnMut(u16, usize, bool),
) -> Sent {
    if chan < 0 || payload.is_empty() || payload.len() > FRAME_MAX {
        return Sent::Refused;
    }
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        if r.count > 0 {
            flush(r, sys, chan, &mut written);
        }
        if r.count == 0 && wire::channel_write_msg(sys, chan, msg_type, payload) > 0 {
            written(conn, wire::ENVELOPE_HDR + payload.len(), counted);
            return Sent::Written;
        }
    }
    match reply {
        Reply::Offer => return Sent::Refused,
        Reply::Spare if !has_reserve_after(r, payload.len()) => return Sent::Refused,
        _ => {}
    }
    if push(r, conn, msg_type, payload, counted) {
        Sent::Held
    } else if reply == Reply::Kept {
        Sent::Refused
    } else {
        Sent::Full
    }
}

/// The entry is still to be written (cleared when its connection closes).
const LIVE: u8 = 1;
/// The write is counted against the connection's delivery cursor.
const COUNTED: u8 = 2;

#[repr(C)]
#[derive(Clone, Copy)]
struct Held {
    off: u32,
    len: u16,
    /// Bytes skipped at the ring's end so this entry sits whole at 0.
    pad: u16,
    conn: u16,
    msg_type: u8,
    flags: u8,
}

impl Held {
    const fn zero() -> Self {
        Self {
            off: 0,
            len: 0,
            pad: 0,
            conn: 0,
            msg_type: 0,
            flags: 0,
        }
    }
}

/// One output's held responses: payloads in a byte ring, in the order
/// they were refused.
#[repr(C)]
pub struct Ring {
    bytes: [u8; RING_BYTES],
    held: [Held; RING_ENTRIES],
    head: u16,
    count: u16,
    wr: u32,
    used: u32,
}

/// Every output's ring, and the counts the heartbeat reports.
#[repr(C)]
pub struct Hold {
    pub rings: [Ring; OUTPUTS],
    /// Responses ever held (the bus refused them, or one was held ahead).
    pub held: u32,
    /// Responses a full ring could not take. The input gates keep this at
    /// zero; anything else is a defect in a gate.
    pub overflow: u32,
}

/// Empty every ring. The module state starts zeroed, which is already
/// empty; this is for a reset.
pub fn init(h: &mut Hold) {
    for r in h.rings.iter_mut() {
        r.head = 0;
        r.count = 0;
        r.wr = 0;
        r.used = 0;
        for e in r.held.iter_mut() {
            *e = Held::zero();
        }
    }
    h.held = 0;
    h.overflow = 0;
}

/// Whether ring `r` holds nothing.
#[inline]
pub fn is_empty(r: &Ring) -> bool {
    r.count == 0
}

/// How many responses ring `r` holds (closed connections' included until
/// they reach the front).
#[inline]
pub fn len(r: &Ring) -> usize {
    usize::from(r.count)
}

/// Whether ring `r` can take the most one input can answer
/// (`RESERVE_BYTES` in at most `RESERVE_ENTRIES` responses) whatever
/// their sizes: the bytes are counted with one skipped ring end, the most
/// a run of responses can waste.
#[inline]
pub fn has_reserve(r: &Ring) -> bool {
    usize::from(r.count) + RESERVE_ENTRIES <= RING_ENTRIES
        && r.used as usize + RESERVE_BYTES + FRAME_MAX <= RING_BYTES
}

/// Whether ring `r` still has [`has_reserve`] once it holds a further
/// `len`-byte payload: room that may be spent on work beyond what the
/// input being served can answer.
#[inline]
pub fn has_reserve_after(r: &Ring, len: usize) -> bool {
    usize::from(r.count) + 1 + RESERVE_ENTRIES <= RING_ENTRIES
        && r.used as usize + len + FRAME_MAX + RESERVE_BYTES + FRAME_MAX <= RING_BYTES
}

/// Where a `len`-byte payload goes: `(offset, pad)`, or `None` when the
/// ring has no contiguous room for it.
fn place(r: &Ring, len: usize) -> Option<(usize, usize)> {
    if r.count == 0 {
        return (len <= RING_BYTES).then_some((0, 0));
    }
    let rd = r.held[usize::from(r.head)].off as usize;
    let wr = r.wr as usize;
    if wr > rd {
        // Live bytes are [rd, wr): room at the end, then at the front.
        if wr + len <= RING_BYTES {
            Some((wr, 0))
        } else if len <= rd {
            Some((0, RING_BYTES - wr))
        } else {
            None
        }
    } else {
        // Wrapped: live bytes are [rd, end) and [0, wr).
        (wr + len <= rd).then_some((wr, 0))
    }
}

/// Hold `payload` (a `msg_type` envelope's payload, for connection
/// `conn`) behind everything ring `r` holds. `counted` says the write is
/// counted against the connection's delivery cursor when it is made.
/// Returns false when the ring cannot take it.
pub fn push(r: &mut Ring, conn: u16, msg_type: u8, payload: &[u8], counted: bool) -> bool {
    let len = payload.len();
    if len == 0 || len > FRAME_MAX || usize::from(r.count) >= RING_ENTRIES {
        return false;
    }
    if r.count == 0 {
        r.wr = 0;
        r.used = 0;
    }
    let Some((off, pad)) = place(r, len) else {
        return false;
    };
    r.bytes[off..off + len].copy_from_slice(payload);
    let tail = (usize::from(r.head) + usize::from(r.count)) % RING_ENTRIES;
    r.held[tail] = Held {
        off: off as u32,
        len: len as u16,
        pad: pad as u16,
        conn,
        msg_type,
        flags: LIVE | if counted { COUNTED } else { 0 },
    };
    r.count += 1;
    r.wr = (off + len) as u32;
    r.used += (pad + len) as u32;
    true
}

fn pop(r: &mut Ring) {
    let h = r.held[usize::from(r.head)];
    r.used -= u32::from(h.pad) + u32::from(h.len);
    r.head = ((usize::from(r.head) + 1) % RING_ENTRIES) as u16;
    r.count -= 1;
    if r.count == 0 {
        r.head = 0;
        r.wr = 0;
        r.used = 0;
    }
}

/// Write ring `r`'s responses to `chan`, oldest first, until the bus
/// refuses one (it stays, at the front). `written(conn, bytes, counted)`
/// is told of each write, `bytes` with the envelope header. A response
/// whose connection closed while it was held is dropped unwritten.
/// Returns how many were written.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
pub unsafe fn flush(
    r: &mut Ring,
    sys: &SyscallTable,
    chan: i32,
    mut written: impl FnMut(u16, usize, bool),
) -> u32 {
    let mut n = 0u32;
    while r.count > 0 {
        let h = r.held[usize::from(r.head)];
        if h.flags & LIVE != 0 {
            if chan < 0 {
                break;
            }
            let (off, len) = (h.off as usize, usize::from(h.len));
            // SAFETY: caller guarantees `sys` is live.
            let w =
                unsafe { wire::channel_write_msg(sys, chan, h.msg_type, &r.bytes[off..off + len]) };
            if w <= 0 {
                break;
            }
            written(h.conn, wire::ENVELOPE_HDR + len, h.flags & COUNTED != 0);
            n += 1;
        }
        pop(r);
    }
    n
}

/// Connection `conn` closed: nothing held for it is written (its id may
/// be the next connection's). The entries keep their place and are
/// dropped as they reach the front.
pub fn forget_conn(h: &mut Hold, conn: u16) {
    for r in h.rings.iter_mut() {
        for i in 0..usize::from(r.count) {
            let e = &mut r.held[(usize::from(r.head) + i) % RING_ENTRIES];
            if e.conn == conn {
                e.flags &= !LIVE;
            }
        }
    }
}

/// Coordinator call answers (`wire::MSG_COORD_RESPONSE`) held for `conn`
/// are not written: the connection closed (the coordinator hears its close
/// on its own path, whichever worker the connection was bound to). The
/// session's own responses, if this worker holds its binding, are left to
/// the session's close notice ([`forget_conn`]).
pub fn forget_coord(h: &mut Hold, conn: u16) {
    for r in h.rings.iter_mut() {
        for i in 0..usize::from(r.count) {
            let e = &mut r.held[(usize::from(r.head) + i) % RING_ENTRIES];
            if e.conn == conn && e.msg_type == wire::MSG_COORD_RESPONSE {
                e.flags &= !LIVE;
            }
        }
    }
}

/// Whether anything of connection `conn`'s session is held, on any
/// output: what a handoff must see written first. A coordinator answer is
/// not the session's — counted in no cursor, written by the coordinator
/// wherever the connection is bound — and does not count.
pub fn holds_for(h: &Hold, conn: u16) -> bool {
    h.rings.iter().any(|r| {
        (0..usize::from(r.count)).any(|i| {
            let e = &r.held[(usize::from(r.head) + i) % RING_ENTRIES];
            e.conn == conn && e.flags & LIVE != 0 && e.msg_type != wire::MSG_COORD_RESPONSE
        })
    })
}

/// Connections owed a DISCONNECT, with its reason: one per conn id, so a
/// placement update releasing every session, or refusals failing more
/// flows than a ring holds, never need room. A DISCONNECT ends what the
/// connection is told: once one is owed, nothing later for the connection
/// is written ahead of it, or after it.
#[repr(C)]
pub struct Owed {
    bits: [u64; 1024],
    reason: [u8; 1 << 16],
    pub count: u32,
}

/// Owe nothing.
pub fn owed_init(o: &mut Owed) {
    o.bits = [0; 1024];
    o.count = 0;
}

/// Connection `conn` is owed a DISCONNECT with `reason`. The first reason
/// owed stands: it is the one the connection was failed on.
pub fn owe(o: &mut Owed, conn: u16, reason: u8) {
    let (w, b) = (usize::from(conn) / 64, conn % 64);
    if o.bits[w] & (1u64 << b) == 0 {
        o.bits[w] |= 1u64 << b;
        o.reason[usize::from(conn)] = reason;
        o.count += 1;
    }
}

/// Connection `conn` is owed nothing more: answered, or closed.
pub fn settle(o: &mut Owed, conn: u16) {
    let (w, b) = (usize::from(conn) / 64, conn % 64);
    if o.bits[w] & (1u64 << b) != 0 {
        o.bits[w] &= !(1u64 << b);
        o.count -= 1;
    }
}

/// The reason connection `conn` is owed a DISCONNECT with, if it is.
pub fn owed(o: &Owed, conn: u16) -> Option<u8> {
    (o.bits[usize::from(conn) / 64] & (1u64 << (conn % 64)) != 0)
        .then(|| o.reason[usize::from(conn)])
}

/// The owed connections from conn id `from` up, wrapping, into `out`;
/// returns how many. A caller passing on from the last one it was given
/// gives every owed connection its turn.
pub fn owed_conns(o: &Owed, from: u16, out: &mut [u16]) -> usize {
    let mut n = 0usize;
    if o.count == 0 {
        return 0;
    }
    let words = o.bits.len();
    let first = usize::from(from) / 64;
    // The first word's bits below `from` come last, on the wrap.
    for step in 0..=words {
        let w = (first + step) % words;
        let mut bits = o.bits[w];
        if step == 0 {
            bits &= !0u64 << (from % 64);
        } else if step == words {
            bits &= !(!0u64 << (from % 64));
        }
        while bits != 0 && n < out.len() {
            let b = bits.trailing_zeros() as usize;
            out[n] = (w * 64 + b) as u16;
            n += 1;
            bits &= bits - 1;
        }
        if n == out.len() {
            break;
        }
    }
    n
}

/// Whether ring `r` holds anything still to be written for `conn`.
pub fn ring_holds_for(r: &Ring, conn: u16) -> bool {
    (0..usize::from(r.count)).any(|i| {
        let e = &r.held[(usize::from(r.head) + i) % RING_ENTRIES];
        e.conn == conn && e.flags & LIVE != 0
    })
}
