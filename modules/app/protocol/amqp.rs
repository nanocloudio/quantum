//! amqp — AMQP 0-9-1 wire codec.
//!
//! Decodes the method/header/body frame sequence into session proposals
//! and encodes responses back, holding per-connection and per-channel
//! state (publish assembly, consumer tags, delivery tags).
//!
//! ## Per-step bound
//!
//! `RX_QUOTA` records off `in_raw` and `RESP_QUOTA` off `in_responses`.

use super::abi::SyscallTable;
use super::{dev_channel_port, dev_log, dev_millis, wire};

const FRAME_METHOD: u8 = 1;
const FRAME_HEADER: u8 = 2;
const FRAME_BODY: u8 = 3;
const FRAME_HEARTBEAT: u8 = 8;
const FRAME_END: u8 = 0xCE;

const PROTO_AMQP: u8 = 2;
const OP_PUBLISH: u8 = 1;
const OP_GET: u8 = 2;
const OP_CONSUME: u8 = 3;
const OP_CANCEL: u8 = 4;
const OP_ACK: u8 = 5;

/// Consumer-tag cap (bytes) for consume-start; longer → Channel.Close 406.
const MAX_CTAG: usize = 48;
/// Queue-name cap (bytes) for consume-start; longer → Channel.Close 406.
const MAX_QNAME: usize = 64;

/// Per-connection reassembly capacity. We advertise `frame-max = RASM`
/// in Connection.Tune, so a compliant client never sends a frame the
/// buffer cannot hold (frame-max covers the whole frame incl. the
/// 7-byte header and end octet).
const RASM: usize = 8192;
/// Largest accepted frame payload `size`: whole frame (7 + size + 1)
/// must fit RASM (bound kept one byte conservative).
const MAX_FSIZE: usize = RASM - 9;
const ACONNS: usize = 16;
/// Frame/scratch buffer size (responses, outbound envelopes).
const OUT_BUF: usize = 8192;
/// Per-tick drain quotas. Ingress must outrun the router component's 64
/// records/tick; response encode matches the session side's burst.
const RX_QUOTA: usize = 64;
const RESP_QUOTA: usize = 64;

/// Channels 1..=MAX_CHANNELS are accepted (advertised as channel-max).
const MAX_CHANNELS: u16 = 16;
/// Advertised heartbeat interval (seconds).
const HEARTBEAT_S: u16 = 60;
/// Max assembled publish body (substrate entry cap headroom).
const MAX_BODY: usize = 1800;
/// Routing key cap ("exchange.routing-key" joined, truncated).
const MAX_RK: usize = 64;
/// Concurrent in-flight Basic.Publish content assemblies per conn
/// (one per channel; slot table like the kafka component's conn table).
const MAX_PENDING: usize = 8;
/// Method-argument scratch: covers class/method + the longest argument
/// run we parse (two 255-byte shortstrs + fixed fields).
const ARG_BUF: usize = 600;

const HDR_AMQP: [u8; 8] = *b"AMQP\x00\x00\x09\x01";

/// Pending-publish assembly states.
const P_NONE: u8 = 0;
/// Method frame seen, waiting for the content header.
const P_AWAIT_HEADER: u8 = 1;
/// Header seen, accumulating body frames.
const P_BODY: u8 = 2;
/// Oversize publish: consume body frames silently until body_size, drop.
const P_DISCARD: u8 = 3;

#[repr(C)]
#[derive(Clone, Copy)]
struct PendingPub {
    body: [u8; MAX_BODY],
    rk: [u8; MAX_RK],
    body_size: u64,
    received: u64,
    channel: u16,
    state: u8,
    rk_len: u8,
}

impl PendingPub {
    const fn zero() -> Self {
        Self {
            body: [0; MAX_BODY],
            rk: [0; MAX_RK],
            body_size: 0,
            received: 0,
            channel: 0,
            state: P_NONE,
            rk_len: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AConn {
    buf: [u8; RASM],
    pending: [PendingPub; MAX_PENDING],
    /// Next delivery-tag per channel (index 1..=MAX_CHANNELS; [0] unused).
    pub_seq: [u64; MAX_CHANNELS as usize + 1],
    /// Bit N set = channel N open (channels 1..=16).
    chan_mask: u32,
    /// Bit N set = channel N in confirm mode (Confirm.Select seen).
    confirm_mask: u32,
    /// Last Basic.Qos prefetch-count per channel (index 1..=MAX_CHANNELS;
    /// [0] unused; 0 = unlimited). Zeroed with the rest of channel state
    /// on Channel.Open/Close and with the conn on Connection.Close.
    prefetch: [u16; MAX_CHANNELS as usize + 1],
    /// Bounded counter for generated consumer tags ("ctag-<c>-<ch>-<n>").
    ctag_seq: u16,
    len: u16,
    conn_id: u8,
    active: u8,
    /// Protocol header "AMQP\x00\x00\x09\x01" consumed.
    saw_header: u8,
}

impl AConn {
    const fn zero() -> Self {
        Self {
            buf: [0; RASM],
            pending: [PendingPub::zero(); MAX_PENDING],
            pub_seq: [0; MAX_CHANNELS as usize + 1],
            chan_mask: 0,
            confirm_mask: 0,
            prefetch: [0; MAX_CHANNELS as usize + 1],
            ctag_seq: 0,
            len: 0,
            conn_id: 0,
            active: 0,
            saw_header: 0,
        }
    }
}

#[repr(C)]
pub struct Amqp {
    pub out_proposals: i32,
    pub out_frames: i32,

    frames_decoded: u32,
    publishes_forwarded: u32,
    responses_encoded: u32,
    parse_errors: u32,
    resync_resets: u32,
    unsupported: u32,
    consumes_started: u32,
    consumes_cancelled: u32,
    delivers_encoded: u32,
    client_acks_forwarded: u32,

    conns: [AConn; ACONNS],
    frame: [u8; OUT_BUF],
    scratch: [u8; OUT_BUF],
}

/// Component defaults. Channel handles are assigned by the
/// composite after this returns.
pub fn init(s: &mut Amqp) {
    for c in s.conns.iter_mut() {
        *c = AConn::zero();
    }
    s.frames_decoded = 0;
    s.publishes_forwarded = 0;
    s.responses_encoded = 0;
    s.parse_errors = 0;
    s.resync_resets = 0;
    s.unsupported = 0;
    s.consumes_started = 0;
    s.consumes_cancelled = 0;
    s.delivers_encoded = 0;
    s.client_acks_forwarded = 0;
}

/// Emit a `[conn_id][bytes]` payload as a `MSG_CLIENT_FRAME` envelope
/// toward peer_router. See `the mqtt component::write_conn_frame` for the
/// rationale.
/// # Safety
unsafe fn write_conn(sys: &SyscallTable, chan: i32, conn_id: u8, bytes: &[u8]) -> bool {
    if chan < 0 {
        return false;
    }
    let total = 1 + bytes.len();
    if total > OUT_BUF {
        return false;
    }
    let mut out = [0u8; OUT_BUF];
    out[0] = conn_id;
    out[1..total].copy_from_slice(bytes);
    let w = wire::channel_write_msg(sys, chan, wire::MSG_CLIENT_FRAME, &out[..total]);
    w > 0
}

fn find_conn(s: &mut Amqp, conn_id: u8) -> usize {
    for i in 0..ACONNS {
        if s.conns[i].active == 1 && s.conns[i].conn_id == conn_id {
            return i;
        }
    }
    for i in 0..ACONNS {
        if s.conns[i].active == 0 {
            s.conns[i] = AConn::zero();
            s.conns[i].conn_id = conn_id;
            s.conns[i].active = 1;
            return i;
        }
    }
    // Table full: evict slot 0. peer_router caps client conns at 64 but
    // only a fraction speak AMQP; the bench profile stays well inside 16.
    s.conns[0] = AConn::zero();
    s.conns[0].conn_id = conn_id;
    s.conns[0].active = 1;
    0
}

// ── Frame builders ──────────────────────────────────────────────────────────

/// Wrap `payload` in an AMQP frame `[type][channel BE][size BE][payload][0xCE]`
/// (built in `s.frame`) and send to the client.
/// # Safety
unsafe fn send_frame(
    s: &mut Amqp,
    sys: &SyscallTable,
    conn_id: u8,
    ftype: u8,
    channel: u16,
    payload: &[u8],
) {
    // 7-byte header + payload + frame-end octet.
    let total = 7 + payload.len() + 1;
    if total > OUT_BUF {
        return;
    }
    s.frame[0] = ftype;
    s.frame[1..3].copy_from_slice(&channel.to_be_bytes());
    s.frame[3..7].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    s.frame[7..7 + payload.len()].copy_from_slice(payload);
    s.frame[7 + payload.len()] = FRAME_END;
    let out_frames = s.out_frames;
    if write_conn(sys, out_frames, conn_id, &s.frame[..total]) {
        s.responses_encoded = s.responses_encoded.wrapping_add(1);
    }
}

/// Method frame: payload = `[class:i16 BE][method:i16 BE][args]`.
/// # Safety
unsafe fn send_method(
    s: &mut Amqp,
    sys: &SyscallTable,
    conn_id: u8,
    channel: u16,
    class: u16,
    method: u16,
    args: &[u8],
) {
    let mut pay = [0u8; ARG_BUF];
    if 4 + args.len() > ARG_BUF {
        return;
    }
    pay[0..2].copy_from_slice(&class.to_be_bytes());
    pay[2..4].copy_from_slice(&method.to_be_bytes());
    pay[4..4 + args.len()].copy_from_slice(args);
    send_frame(
        s,
        sys,
        conn_id,
        FRAME_METHOD,
        channel,
        &pay[..4 + args.len()],
    );
}

/// Basic.Ack (multiple=0) or Basic.Nack (multiple=0, requeue=0).
/// # Safety
unsafe fn send_ack_nack(
    s: &mut Amqp,
    sys: &SyscallTable,
    conn_id: u8,
    channel: u16,
    delivery_tag: u64,
    ack: bool,
) {
    let mut args = [0u8; 9];
    args[0..8].copy_from_slice(&delivery_tag.to_be_bytes());
    args[8] = 0;
    let method: u16 = if ack { 80 } else { 120 };
    send_method(s, sys, conn_id, channel, 60, method, &args);
}

/// Drop all per-channel state: open/confirm bits, prefetch, pending
/// publish assemblies. Used by Channel.Close and server-initiated
/// channel closes.
fn reset_channel_state(conn: &mut AConn, channel: u16) {
    if (1..=MAX_CHANNELS).contains(&channel) {
        conn.chan_mask &= !(1u32 << channel);
        conn.confirm_mask &= !(1u32 << channel);
        conn.prefetch[channel as usize] = 0;
    }
    for i in 0..MAX_PENDING {
        if conn.pending[i].channel == channel {
            conn.pending[i].state = P_NONE;
        }
    }
}

/// Server-initiated Channel.Close 406 PRECONDITION_FAILED, blaming
/// (class, method). Clears the channel's state like the other close
/// paths.
/// # Safety
unsafe fn close_channel_406(
    s: &mut Amqp,
    sys: &SyscallTable,
    conn_id: u8,
    ci: usize,
    channel: u16,
    class: u16,
    method: u16,
) {
    let text: &[u8] = b"PRECONDITION_FAILED";
    let mut args = [0u8; 32];
    let mut p = 0usize;
    args[p..p + 2].copy_from_slice(&406i16.to_be_bytes());
    p += 2;
    args[p] = text.len() as u8;
    p += 1;
    args[p..p + text.len()].copy_from_slice(text);
    p += text.len();
    args[p..p + 2].copy_from_slice(&(class as i16).to_be_bytes());
    p += 2;
    args[p..p + 2].copy_from_slice(&(method as i16).to_be_bytes());
    p += 2;
    reset_channel_state(&mut s.conns[ci], channel);
    send_method(s, sys, conn_id, channel, 20, 40, &args[..p]);
}

/// Write `v` as decimal into `buf[off..]`; returns the new offset.
/// Caller guarantees room (max 10 digits for u32).
fn put_dec(buf: &mut [u8], off: usize, mut v: u32) -> usize {
    let mut tmp = [0u8; 10];
    let mut n = 0usize;
    loop {
        tmp[n] = b'0' + (v % 10) as u8;
        n += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    for i in 0..n {
        buf[off + i] = tmp[n - 1 - i];
    }
    off + n
}

/// Emit a content header (class 60, weight 0, property-flags 0x0000)
/// followed by one body frame whose payload is
/// `s.scratch[body_off..body_off + body_len]` (skipped when empty).
/// Shared by the Basic.GetOk and Basic.Deliver paths.
/// # Safety
unsafe fn send_content(
    s: &mut Amqp,
    sys: &SyscallTable,
    conn_id: u8,
    channel: u16,
    body_off: usize,
    body_len: usize,
) {
    // Content header: class 60, weight 0, body-size, property-flags 0.
    let mut hdr = [0u8; 14];
    hdr[0..2].copy_from_slice(&60i16.to_be_bytes());
    hdr[2..4].copy_from_slice(&0i16.to_be_bytes());
    hdr[4..12].copy_from_slice(&(body_len as u64).to_be_bytes());
    hdr[12..14].copy_from_slice(&0i16.to_be_bytes());
    send_frame(s, sys, conn_id, FRAME_HEADER, channel, &hdr);

    // Body frame (payload ≤ entry cap, one frame).
    // 7-byte header + payload + frame-end octet.
    if body_len > 0 {
        let total = 7 + body_len + 1;
        if total > OUT_BUF {
            return;
        }
        s.frame[0] = FRAME_BODY;
        s.frame[1..3].copy_from_slice(&channel.to_be_bytes());
        s.frame[3..7].copy_from_slice(&(body_len as u32).to_be_bytes());
        core::ptr::copy_nonoverlapping(
            s.scratch.as_ptr().add(body_off),
            s.frame.as_mut_ptr().add(7),
            body_len,
        );
        s.frame[7 + body_len] = FRAME_END;
        let out_frames = s.out_frames;
        if write_conn(sys, out_frames, conn_id, &s.frame[..total]) {
            s.responses_encoded = s.responses_encoded.wrapping_add(1);
        }
    }
}

// ── Publish assembly ────────────────────────────────────────────────────────

fn pending_for_channel(conn: &mut AConn, channel: u16) -> Option<usize> {
    conn.pending
        .iter()
        .position(|p| p.state != P_NONE && p.channel == channel)
}

fn pending_free(conn: &mut AConn) -> Option<usize> {
    conn.pending.iter().position(|p| p.state == P_NONE)
}

/// Take the next delivery-tag for a confirm-mode channel; 0 otherwise.
fn next_delivery_tag(conn: &mut AConn, channel: u16) -> u64 {
    if channel == 0 || channel > MAX_CHANNELS {
        return 0;
    }
    if conn.confirm_mask & (1u32 << channel) == 0 {
        return 0;
    }
    let ch = channel as usize;
    if conn.pub_seq[ch] == 0 {
        conn.pub_seq[ch] = 1;
    }
    let t = conn.pub_seq[ch];
    conn.pub_seq[ch] = conn.pub_seq[ch].wrapping_add(1);
    t
}

/// Forward the fully assembled publish at `s.conns[ci].pending[pi]` to
/// session_processor and clear the slot.
///
/// Payload: `[conn_id][proto=2][op=1][channel:u16 LE][delivery_tag:u64 LE]
///           [rk_len:u16 LE][routing_key][body]`
/// # Safety
unsafe fn forward_publish(s: &mut Amqp, sys: &SyscallTable, conn_id: u8, ci: usize, pi: usize) {
    let channel = s.conns[ci].pending[pi].channel;
    let rk_len = s.conns[ci].pending[pi].rk_len as usize;
    let body_len = s.conns[ci].pending[pi].body_size as usize; // ≤ MAX_BODY here
    let dt = next_delivery_tag(&mut s.conns[ci], channel);

    let env_len = 14 + rk_len + body_len;
    if env_len > OUT_BUF {
        s.conns[ci].pending[pi].state = P_NONE;
        s.parse_errors = s.parse_errors.wrapping_add(1);
        return;
    }
    s.scratch[0] = conn_id;
    s.scratch[1] = PROTO_AMQP;
    s.scratch[2] = OP_PUBLISH;
    s.scratch[3..5].copy_from_slice(&channel.to_le_bytes());
    s.scratch[5..13].copy_from_slice(&dt.to_le_bytes());
    s.scratch[13..15].copy_from_slice(&(rk_len as u16).to_le_bytes());
    // SAFETY: rk/body live in s.conns, scratch is a distinct field.
    core::ptr::copy_nonoverlapping(
        s.conns[ci].pending[pi].rk.as_ptr(),
        s.scratch.as_mut_ptr().add(15),
        rk_len,
    );
    core::ptr::copy_nonoverlapping(
        s.conns[ci].pending[pi].body.as_ptr(),
        s.scratch.as_mut_ptr().add(15 + rk_len),
        body_len,
    );
    s.conns[ci].pending[pi].state = P_NONE;
    let out = s.out_proposals;
    let w = wire::channel_write_msg(
        sys,
        out,
        wire::MSG_SESSION_PROPOSAL,
        &s.scratch[..1 + env_len],
    );
    if w > 0 {
        s.publishes_forwarded = s.publishes_forwarded.wrapping_add(1);
    } else {
        // codec_in saturated — drop; publisher retries / times out.
        s.parse_errors = s.parse_errors.wrapping_add(1);
    }
}

// ── Method dispatch ─────────────────────────────────────────────────────────

/// Read a shortstr (`u8 len + bytes`) from `a[off..]`. Returns
/// `(next_off, str_off, str_len)` or None on truncation.
fn shortstr(a: &[u8], off: usize) -> Option<(usize, usize, usize)> {
    if off >= a.len() {
        return None;
    }
    let l = a[off] as usize;
    if off + 1 + l > a.len() {
        return None;
    }
    Some((off + 1 + l, off + 1, l))
}

/// Handle one complete method frame. `m` is the frame payload (class,
/// method, args) copied out of the reassembly buffer; may be truncated
/// to ARG_BUF, which covers every argument run we parse. Returns true
/// if the conn state was reset (caller must stop extracting).
/// # Safety
unsafe fn handle_method(
    s: &mut Amqp,
    sys: &SyscallTable,
    conn_id: u8,
    ci: usize,
    channel: u16,
    m: &[u8],
) -> bool {
    if m.len() < 4 {
        s.parse_errors = s.parse_errors.wrapping_add(1);
        return false;
    }
    let class = u16::from_be_bytes([m[0], m[1]]);
    let method = u16::from_be_bytes([m[2], m[3]]);
    let a = &m[4..];

    match (class, method) {
        // Connection.StartOk → Connection.Tune
        (10, 11) => {
            let mut args = [0u8; 8];
            args[0..2].copy_from_slice(&(MAX_CHANNELS as i16).to_be_bytes());
            args[2..6].copy_from_slice(&(RASM as i32).to_be_bytes());
            args[6..8].copy_from_slice(&(HEARTBEAT_S as i16).to_be_bytes());
            send_method(s, sys, conn_id, 0, 10, 30, &args);
        }
        // Connection.TuneOk → no response
        (10, 31) => {}
        // Connection.Open → Connection.OpenOk (reserved shortstr = 0x00)
        (10, 40) => send_method(s, sys, conn_id, 0, 10, 41, &[0u8]),
        // Connection.Close → Connection.CloseOk, reset conn state
        (10, 50) => {
            send_method(s, sys, conn_id, 0, 10, 51, &[]);
            s.conns[ci] = AConn::zero();
            return true;
        }
        // Connection.CloseOk (client ack of a server-side close) → ignore
        (10, 51) => {}
        // Channel.Open → Channel.OpenOk (reserved longstr = u32 0)
        (20, 10) => {
            if (1..=MAX_CHANNELS).contains(&channel) {
                s.conns[ci].chan_mask |= 1u32 << channel;
                s.conns[ci].confirm_mask &= !(1u32 << channel);
                s.conns[ci].prefetch[channel as usize] = 0;
                s.conns[ci].pub_seq[channel as usize] = 1;
            }
            send_method(s, sys, conn_id, channel, 20, 11, &[0u8; 4]);
        }
        // Channel.Close → Channel.CloseOk
        (20, 40) => {
            reset_channel_state(&mut s.conns[ci], channel);
            send_method(s, sys, conn_id, channel, 20, 41, &[]);
        }
        // Channel.CloseOk (client ack of a server-side close) → ignore
        (20, 41) => {}
        // Exchange.Declare → Exchange.DeclareOk (accept everything)
        (40, 10) => send_method(s, sys, conn_id, channel, 40, 11, &[]),
        // Queue.Declare → Queue.DeclareOk (echo name; empty → "amq.gen-1")
        (50, 10) => {
            // args: reserved i16, queue shortstr, bits, table (ignored).
            let mut name: [u8; 255] = [0; 255];
            let mut name_len = 0usize;
            if let Some((_, so, sl)) = shortstr(a, 2) {
                name[..sl].copy_from_slice(&a[so..so + sl]);
                name_len = sl;
            }
            if name_len == 0 {
                let gen = b"amq.gen-1";
                name[..gen.len()].copy_from_slice(gen);
                name_len = gen.len();
            }
            let mut args = [0u8; 1 + 255 + 8];
            args[0] = name_len as u8;
            args[1..1 + name_len].copy_from_slice(&name[..name_len]);
            // message-count i32 0, consumer-count i32 0 (already zeroed)
            send_method(s, sys, conn_id, channel, 50, 11, &args[..1 + name_len + 8]);
        }
        // Queue.Bind → Queue.BindOk
        (50, 20) => send_method(s, sys, conn_id, channel, 50, 21, &[]),
        // Basic.Qos → Basic.QosOk; remember prefetch-count for
        // consume-start proposals (0 = unlimited).
        (60, 10) => {
            // args: prefetch-size i32, prefetch-count i16 BE, global bit.
            if a.len() >= 6 && (1..=MAX_CHANNELS).contains(&channel) {
                s.conns[ci].prefetch[channel as usize] = u16::from_be_bytes([a[4], a[5]]);
            }
            send_method(s, sys, conn_id, channel, 60, 11, &[]);
        }
        // Basic.Consume → Basic.ConsumeOk (unless nowait) + op=3 forward
        (60, 20) => {
            // args: reserved i16, queue shortstr, consumer-tag shortstr,
            // bits (bit0 no-local IGNORED, bit1 no-ack, bit2 exclusive
            // IGNORED, bit3 nowait), arguments table (never parsed —
            // skipped by not reading past the bits octet).
            let Some((o1, qo, ql)) = shortstr(a, 2) else {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            };
            let Some((o2, to, tl)) = shortstr(a, o1) else {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            };
            if o2 >= a.len() {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            }
            let bits = a[o2];
            let no_ack = bits & 0x02 != 0;
            let nowait = bits & 0x08 != 0;
            if tl > MAX_CTAG || ql > MAX_QNAME {
                close_channel_406(s, sys, conn_id, ci, channel, 60, 20);
                return false;
            }
            // Client tag, or a generated "ctag-<conn>-<channel>-<n>"
            // (bounded: 5 + 3 + 1 + 5 + 1 + 5 = 20 ≤ MAX_CTAG).
            let mut tag = [0u8; MAX_CTAG];
            let tag_len;
            if tl > 0 {
                tag[..tl].copy_from_slice(&a[to..to + tl]);
                tag_len = tl;
            } else {
                let seq = s.conns[ci].ctag_seq;
                s.conns[ci].ctag_seq = seq.wrapping_add(1);
                let mut p = 0usize;
                tag[p..p + 5].copy_from_slice(b"ctag-");
                p += 5;
                p = put_dec(&mut tag, p, conn_id as u32);
                tag[p] = b'-';
                p += 1;
                p = put_dec(&mut tag, p, channel as u32);
                tag[p] = b'-';
                p += 1;
                p = put_dec(&mut tag, p, seq as u32);
                tag_len = p;
            }
            let prefetch = if (1..=MAX_CHANNELS).contains(&channel) {
                s.conns[ci].prefetch[channel as usize]
            } else {
                0
            };
            if !nowait {
                // Basic.ConsumeOk: consumer-tag shortstr
                let mut ok = [0u8; 1 + MAX_CTAG];
                ok[0] = tag_len as u8;
                ok[1..1 + tag_len].copy_from_slice(&tag[..tag_len]);
                send_method(s, sys, conn_id, channel, 60, 21, &ok[..1 + tag_len]);
            }
            // op=3: [prefix][flags][prefetch:u16 LE][tag_len:u16 LE][tag]
            //       [q_len:u16 LE][queue]
            let total = 13 + 1 + 2 + 2 + tag_len + 2 + ql;
            if total > OUT_BUF {
                return false;
            }
            s.scratch[0] = conn_id;
            s.scratch[1] = PROTO_AMQP;
            s.scratch[2] = OP_CONSUME;
            s.scratch[3..5].copy_from_slice(&channel.to_le_bytes());
            s.scratch[5..13].copy_from_slice(&0u64.to_le_bytes());
            s.scratch[13] = if no_ack { 0x01 } else { 0 };
            s.scratch[14..16].copy_from_slice(&prefetch.to_le_bytes());
            s.scratch[16..18].copy_from_slice(&(tag_len as u16).to_le_bytes());
            s.scratch[18..18 + tag_len].copy_from_slice(&tag[..tag_len]);
            let qb = 18 + tag_len;
            s.scratch[qb..qb + 2].copy_from_slice(&(ql as u16).to_le_bytes());
            s.scratch[qb + 2..qb + 2 + ql].copy_from_slice(&a[qo..qo + ql]);
            let out = s.out_proposals;
            let w =
                wire::channel_write_msg(sys, out, wire::MSG_SESSION_PROPOSAL, &s.scratch[..total]);
            if w > 0 {
                s.consumes_started = s.consumes_started.wrapping_add(1);
            }
        }
        // Basic.Cancel → Basic.CancelOk (unless nowait) + op=4 forward
        (60, 30) => {
            // args: consumer-tag shortstr, nowait bit.
            let Some((o1, to, tl)) = shortstr(a, 0) else {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            };
            let nowait = o1 < a.len() && a[o1] & 0x01 != 0;
            if tl > MAX_CTAG {
                close_channel_406(s, sys, conn_id, ci, channel, 60, 30);
                return false;
            }
            let mut tag = [0u8; MAX_CTAG];
            tag[..tl].copy_from_slice(&a[to..to + tl]);
            if !nowait {
                // Basic.CancelOk: consumer-tag shortstr
                let mut ok = [0u8; 1 + MAX_CTAG];
                ok[0] = tl as u8;
                ok[1..1 + tl].copy_from_slice(&tag[..tl]);
                send_method(s, sys, conn_id, channel, 60, 31, &ok[..1 + tl]);
            }
            // op=4: [prefix][tag_len:u16 LE][tag]
            s.scratch[0] = conn_id;
            s.scratch[1] = PROTO_AMQP;
            s.scratch[2] = OP_CANCEL;
            s.scratch[3..5].copy_from_slice(&channel.to_le_bytes());
            s.scratch[5..13].copy_from_slice(&0u64.to_le_bytes());
            s.scratch[13..15].copy_from_slice(&(tl as u16).to_le_bytes());
            s.scratch[15..15 + tl].copy_from_slice(&tag[..tl]);
            let out = s.out_proposals;
            let w = wire::channel_write_msg(
                sys,
                out,
                wire::MSG_SESSION_PROPOSAL,
                &s.scratch[..15 + tl],
            );
            if w > 0 {
                s.consumes_cancelled = s.consumes_cancelled.wrapping_add(1);
            }
        }
        // Basic.Publish — start content assembly for this channel
        (60, 40) => {
            // args: reserved i16, exchange shortstr, routing-key shortstr, bits.
            let Some((o1, xo, xl)) = shortstr(a, 2) else {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            };
            let Some((_, ro, rl)) = shortstr(a, o1) else {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            };
            // Routing key: "exchange.routing-key" when exchange non-empty,
            // else routing-key alone; truncated to MAX_RK.
            let mut rk = [0u8; MAX_RK];
            let mut rk_len = 0usize;
            if xl > 0 {
                let n = xl.min(MAX_RK);
                rk[..n].copy_from_slice(&a[xo..xo + n]);
                rk_len = n;
                if rk_len < MAX_RK {
                    rk[rk_len] = b'.';
                    rk_len += 1;
                }
            }
            let n = rl.min(MAX_RK - rk_len);
            rk[rk_len..rk_len + n].copy_from_slice(&a[ro..ro + n]);
            rk_len += n;

            // One pending publish per (conn, channel): reuse the
            // channel's slot (an unfinished predecessor is abandoned),
            // else claim a free one.
            let slot = pending_for_channel(&mut s.conns[ci], channel)
                .or_else(|| pending_free(&mut s.conns[ci]));
            let Some(pi) = slot else {
                s.unsupported = s.unsupported.wrapping_add(1);
                return false;
            };
            let p = &mut s.conns[ci].pending[pi];
            p.channel = channel;
            p.state = P_AWAIT_HEADER;
            p.body_size = 0;
            p.received = 0;
            p.rk[..rk_len].copy_from_slice(&rk[..rk_len]);
            p.rk_len = rk_len as u8;
        }
        // Basic.Get → forward to session_processor
        (60, 70) => {
            // args: reserved i16, queue shortstr, no-ack bit (ignored).
            let Some((_, qo, ql)) = shortstr(a, 2) else {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            };
            let env_len = 1 + 14 + ql;
            if env_len > OUT_BUF {
                return false;
            }
            s.scratch[0] = conn_id;
            s.scratch[1] = PROTO_AMQP;
            s.scratch[2] = OP_GET;
            s.scratch[3..5].copy_from_slice(&channel.to_le_bytes());
            s.scratch[5..13].copy_from_slice(&0u64.to_le_bytes());
            s.scratch[13..15].copy_from_slice(&(ql as u16).to_le_bytes());
            s.scratch[15..15 + ql].copy_from_slice(&a[qo..qo + ql]);
            let out = s.out_proposals;
            wire::channel_write_msg(sys, out, wire::MSG_SESSION_PROPOSAL, &s.scratch[..env_len]);
        }
        // Basic.Ack / Basic.Nack / Basic.Reject → op=5 forward,
        // no response frame to the client.
        (60, 80) | (60, 90) | (60, 120) => {
            // Ack (80):    delivery-tag u64 BE, bits: bit0 multiple.
            // Reject (90): delivery-tag u64 BE, bits: bit0 requeue.
            // Nack (120):  delivery-tag u64 BE, bits: bit0 multiple,
            //              bit1 requeue.
            if a.len() < 9 {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            }
            let dt = u64::from_be_bytes([a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7]]);
            let bits = a[8];
            // flags: bit0 = multiple, bit1 = requeue-requested.
            let flags = match method {
                80 => bits & 0x01,
                90 => (bits & 0x01) << 1,
                _ => bits & 0x03,
            };
            // op=5: [prefix, dt = frame's delivery-tag][flags:u8]
            s.scratch[0] = conn_id;
            s.scratch[1] = PROTO_AMQP;
            s.scratch[2] = OP_ACK;
            s.scratch[3..5].copy_from_slice(&channel.to_le_bytes());
            s.scratch[5..13].copy_from_slice(&dt.to_le_bytes());
            s.scratch[13] = flags;
            let out = s.out_proposals;
            let w = wire::channel_write_msg(sys, out, wire::MSG_SESSION_PROPOSAL, &s.scratch[..14]);
            if w > 0 {
                s.client_acks_forwarded = s.client_acks_forwarded.wrapping_add(1);
            }
        }
        // Confirm.Select → Confirm.SelectOk, mark confirm mode
        (85, 10) => {
            if (1..=MAX_CHANNELS).contains(&channel) {
                s.conns[ci].confirm_mask |= 1u32 << channel;
                if s.conns[ci].pub_seq[channel as usize] == 0 {
                    s.conns[ci].pub_seq[channel as usize] = 1;
                }
            }
            send_method(s, sys, conn_id, channel, 85, 11, &[]);
        }
        _ => {
            s.unsupported = s.unsupported.wrapping_add(1);
        }
    }
    false
}

/// Handle one complete frame at `conn.buf[pay_off..pay_off+size]`.
/// One parsed AMQP frame header: its type, the channel it targets, and
/// where its payload sits in the reassembly buffer.
#[derive(Clone, Copy)]
struct FrameHdr {
    ftype: u8,
    channel: u16,
    pay_off: usize,
    size: usize,
}

/// Returns true if the conn state was reset.
/// # Safety
unsafe fn handle_frame(
    s: &mut Amqp,
    sys: &SyscallTable,
    conn_id: u8,
    ci: usize,
    f: FrameHdr,
) -> bool {
    let FrameHdr {
        ftype,
        channel,
        pay_off,
        size,
    } = f;
    match ftype {
        FRAME_METHOD => {
            // Copy the method payload out of the reassembly buffer so
            // dispatch can mutate conn state freely. ARG_BUF covers
            // class/method + every argument run we parse; longer
            // payloads (e.g. huge declare tables) are truncated —
            // tables are never parsed.
            let mut mbuf = [0u8; ARG_BUF];
            let n = size.min(ARG_BUF);
            core::ptr::copy_nonoverlapping(
                s.conns[ci].buf.as_ptr().add(pay_off),
                mbuf.as_mut_ptr(),
                n,
            );
            handle_method(s, sys, conn_id, ci, channel, &mbuf[..n])
        }
        FRAME_HEADER => {
            // [class:i16][weight:i16][body-size:u64 BE][prop-flags:i16][props]
            // We only need body-size; properties are skipped wholesale.
            let Some(pi) = pending_for_channel(&mut s.conns[ci], channel) else {
                s.unsupported = s.unsupported.wrapping_add(1);
                return false;
            };
            if s.conns[ci].pending[pi].state != P_AWAIT_HEADER || size < 12 {
                s.conns[ci].pending[pi].state = P_NONE;
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            }
            let mut h = [0u8; 12];
            core::ptr::copy_nonoverlapping(
                s.conns[ci].buf.as_ptr().add(pay_off),
                h.as_mut_ptr(),
                12,
            );
            let body_size = u64::from_be_bytes([h[4], h[5], h[6], h[7], h[8], h[9], h[10], h[11]]);
            if body_size as usize > MAX_BODY {
                // Oversize: nack in confirm mode (the publish still
                // consumed a sequence number), else count and discard.
                let dt = next_delivery_tag(&mut s.conns[ci], channel);
                if dt != 0 {
                    send_ack_nack(s, sys, conn_id, channel, dt, false);
                } else {
                    s.unsupported = s.unsupported.wrapping_add(1);
                }
                let p = &mut s.conns[ci].pending[pi];
                p.body_size = body_size;
                p.received = 0;
                p.state = P_DISCARD;
                return false;
            }
            let p = &mut s.conns[ci].pending[pi];
            p.body_size = body_size;
            p.received = 0;
            if body_size == 0 {
                p.state = P_BODY;
                forward_publish(s, sys, conn_id, ci, pi);
            } else {
                p.state = P_BODY;
            }
            false
        }
        FRAME_BODY => {
            let Some(pi) = pending_for_channel(&mut s.conns[ci], channel) else {
                s.unsupported = s.unsupported.wrapping_add(1);
                return false;
            };
            let st = s.conns[ci].pending[pi].state;
            if st == P_DISCARD {
                let p = &mut s.conns[ci].pending[pi];
                p.received = p.received.saturating_add(size as u64);
                if p.received >= p.body_size {
                    p.state = P_NONE;
                }
                return false;
            }
            if st != P_BODY {
                s.conns[ci].pending[pi].state = P_NONE;
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return false;
            }
            let already = s.conns[ci].pending[pi].received as usize;
            let want = (s.conns[ci].pending[pi].body_size as usize).saturating_sub(already);
            let take = size.min(want);
            if take > 0 {
                // SAFETY: src is the conn's reassembly buffer, dst the
                // pending body — distinct regions of s.conns[ci].
                core::ptr::copy_nonoverlapping(
                    s.conns[ci].buf.as_ptr().add(pay_off),
                    s.conns[ci].pending[pi].body.as_mut_ptr().add(already),
                    take,
                );
            }
            let p = &mut s.conns[ci].pending[pi];
            p.received = p.received.saturating_add(size as u64);
            if p.received >= p.body_size {
                forward_publish(s, sys, conn_id, ci, pi);
            }
            false
        }
        FRAME_HEARTBEAT => {
            send_frame(s, sys, conn_id, FRAME_HEARTBEAT, 0, &[]);
            false
        }
        _ => {
            s.unsupported = s.unsupported.wrapping_add(1);
            false
        }
    }
}

/// Send Connection.Start after the protocol header:
///   version 0.9, empty server-properties table, mechanisms "PLAIN",
///   locales "en_US".
/// # Safety
unsafe fn send_connection_start(s: &mut Amqp, sys: &SyscallTable, conn_id: u8) {
    let mechanisms = b"PLAIN";
    let locales = b"en_US";
    let mut args = [0u8; 32];
    let mut p = 0usize;
    args[p] = 0;
    p += 1; // version-major
    args[p] = 9;
    p += 1; // version-minor
    args[p..p + 4].copy_from_slice(&0u32.to_be_bytes());
    p += 4; // empty table
    args[p..p + 4].copy_from_slice(&(mechanisms.len() as u32).to_be_bytes());
    p += 4;
    args[p..p + mechanisms.len()].copy_from_slice(mechanisms);
    p += mechanisms.len();
    args[p..p + 4].copy_from_slice(&(locales.len() as u32).to_be_bytes());
    p += 4;
    args[p..p + locales.len()].copy_from_slice(locales);
    p += locales.len();
    send_method(s, sys, conn_id, 0, 10, 10, &args[..p]);
}

// ── Step ────────────────────────────────────────────────────────────────────

/// One step of this component.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
/// Handle one client record: `[conn_id][tcp chunk]` framed as
/// MSG_CLIENT_FRAME, or MSG_CONN_CLOSED carrying just the conn id.
/// Appends to the connection's reassembly buffer and drains every
/// complete AMQP frame from it.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_frame(s: &mut Amqp, sys: &SyscallTable, mtype: u8, payload: &[u8]) {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        // Stage the record exactly as the channel read did:
        // `frame` holds `[conn_id][tcp chunk]`.
        let n = payload.len();
        if n > s.frame.len() {
            return;
        }
        s.frame[..n].copy_from_slice(payload);
        if n < 1 {
            return;
        }
        let conn_id = s.frame[0];

        // Connection closed: release this conn's reassembly + all its
        // channel state, and tell session_processor to cancel the
        // conn's push consumers (closes the conn_id-reuse cross-
        // delivery hazard).
        if mtype == wire::MSG_CONN_CLOSED {
            for i in 0..ACONNS {
                if s.conns[i].active == 1 && s.conns[i].conn_id == conn_id {
                    s.conns[i] = AConn::zero();
                    break;
                }
            }
            // Forward as MSG_CONN_CLOSED (NOT MSG_SESSION_DISCONNECT —
            // that type already carries "MQTT DISCONNECT packet" on the
            // shared codec_in fan-in). session_processor cancels this
            // conn's push consumers on it.
            let cb = [conn_id];
            wire::channel_write_msg(sys, s.out_proposals, wire::MSG_CONN_CLOSED, &cb);
            return;
        }
        if n <= 1 {
            return;
        }
        let data_len = n - 1;
        let ci = find_conn(s, conn_id);

        // Append to the conn's reassembly buffer.
        let have = s.conns[ci].len as usize;
        if have + data_len > RASM {
            // Overflow — protocol desync or oversize frame.
            // Fail closed: drop buffered bytes, resync from empty.
            s.conns[ci].len = 0;
            s.resync_resets = s.resync_resets.wrapping_add(1);
            return;
        }
        core::ptr::copy_nonoverlapping(
            s.frame.as_ptr().add(1),
            s.conns[ci].buf.as_mut_ptr().add(have),
            data_len,
        );
        s.conns[ci].len = (have + data_len) as u16;

        // Extract every complete frame currently buffered.
        let mut off = 0usize;
        let mut avail = s.conns[ci].len as usize;
        let mut conn_reset = false;
        loop {
            // Protocol header "AMQP\x00\x00\x09\x01" must be the
            // first bytes on the conn.
            if s.conns[ci].saw_header == 0 {
                if avail < 8 {
                    break;
                }
                if s.conns[ci].buf[off..off + 8] == HDR_AMQP {
                    s.conns[ci].saw_header = 1;
                    off += 8;
                    avail -= 8;
                    s.frames_decoded = s.frames_decoded.wrapping_add(1);
                    send_connection_start(s, sys, conn_id);
                    continue;
                }
                // Not AMQP — fail closed.
                s.conns[ci] = AConn::zero();
                s.resync_resets = s.resync_resets.wrapping_add(1);
                conn_reset = true;
                break;
            }
            if avail < 8 {
                break;
            }
            let ftype = s.conns[ci].buf[off];
            let channel = u16::from_be_bytes([s.conns[ci].buf[off + 1], s.conns[ci].buf[off + 2]]);
            let size = u32::from_be_bytes([
                s.conns[ci].buf[off + 3],
                s.conns[ci].buf[off + 4],
                s.conns[ci].buf[off + 5],
                s.conns[ci].buf[off + 6],
            ]) as usize;
            if size > MAX_FSIZE {
                // Client ignored our frame-max — unrecoverable
                // desync for a stream protocol. Fail closed.
                s.conns[ci] = AConn::zero();
                s.resync_resets = s.resync_resets.wrapping_add(1);
                conn_reset = true;
                break;
            }
            // 7-byte header + payload + frame-end octet.
            let total = 7 + size + 1;
            if avail < total {
                break;
            }
            if s.conns[ci].buf[off + 7 + size] != FRAME_END {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                s.conns[ci] = AConn::zero();
                s.resync_resets = s.resync_resets.wrapping_add(1);
                conn_reset = true;
                break;
            }
            s.frames_decoded = s.frames_decoded.wrapping_add(1);
            if handle_frame(
                s,
                sys,
                conn_id,
                ci,
                FrameHdr {
                    ftype,
                    channel,
                    pay_off: off + 7,
                    size,
                },
            ) {
                conn_reset = true;
                break;
            }
            off += total;
            avail -= total;
        }
        if conn_reset {
            return;
        }
        // Compact leftover fragment to the front.
        if off > 0 && avail > 0 {
            core::ptr::copy(
                s.conns[ci].buf.as_ptr().add(off),
                s.conns[ci].buf.as_mut_ptr(),
                avail,
            );
        }
        if off > 0 {
            s.conns[ci].len = avail as u16;
        }
    }
}

/// Encode one session response into AMQP frames.
///
/// Payload is the session envelope `[conn_id][proto=2][op][channel:u16 LE][rest]`
/// (see session_processor's `emit_amqp_response`). `rest` is per-op:
///
///   op 1 PUBLISH — `[delivery_tag:u64 LE][nack:u8]` → Basic.Ack / Basic.Nack.
///   op 2 GET     — `[result:u8][delivery_tag:u64 LE][remaining:u32 LE][body]`
///                  → Basic.GetOk + content, or Basic.GetEmpty when
///                  `result != 0`. The session sends the fixed 13-byte
///                  prefix for both outcomes so this parses one layout.
///   op 3 CONSUME — `[delivery_tag:u64 LE][flags:u8][ctag_len:u16 LE][ctag][body]`
///                  → Basic.Deliver + content.
///   op 4 CANCEL  — `[ctag_len:u16 LE][ctag]` → broker-initiated Basic.Cancel.
///
/// Exchange and routing-key are emitted as empty shortstrs: this broker
/// publishes through the default exchange, and the codec keeps no
/// per-delivery routing key.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_response(s: &mut Amqp, sys: &SyscallTable, payload: &[u8]) {
    let n = payload.len();
    if n < 5 || n > s.scratch.len() {
        return;
    }
    s.scratch[..n].copy_from_slice(payload);
    let conn_id = s.scratch[0];
    // scratch[1] is the PROTO_AMQP discriminator; the dispatch table has
    // already matched it.
    let op = s.scratch[2];
    let channel = u16::from_le_bytes([s.scratch[3], s.scratch[4]]);

    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        match op {
            OP_PUBLISH => {
                // 5 header + tag(8) + nack(1)
                if n < 14 {
                    s.parse_errors = s.parse_errors.wrapping_add(1);
                    return;
                }
                let tag = u64::from_le_bytes([
                    s.scratch[5],
                    s.scratch[6],
                    s.scratch[7],
                    s.scratch[8],
                    s.scratch[9],
                    s.scratch[10],
                    s.scratch[11],
                    s.scratch[12],
                ]);
                let ack = s.scratch[13] == 0;
                send_ack_nack(s, sys, conn_id, channel, tag, ack);
            }
            OP_GET => {
                // 5 header + result(1) + tag(8) + remaining(4)
                if n < 18 {
                    s.parse_errors = s.parse_errors.wrapping_add(1);
                    return;
                }
                let result = s.scratch[5];
                let tag = u64::from_le_bytes([
                    s.scratch[6],
                    s.scratch[7],
                    s.scratch[8],
                    s.scratch[9],
                    s.scratch[10],
                    s.scratch[11],
                    s.scratch[12],
                    s.scratch[13],
                ]);
                let remaining = u32::from_le_bytes([
                    s.scratch[14],
                    s.scratch[15],
                    s.scratch[16],
                    s.scratch[17],
                ]);
                if result != 0 {
                    // Basic.GetEmpty: reserved shortstr (empty).
                    let args = [0u8; 1];
                    send_method(s, sys, conn_id, channel, 60, 72, &args);
                    return;
                }
                // Basic.GetOk: delivery-tag, redelivered, exchange,
                // routing-key, message-count.
                let mut args = [0u8; 8 + 1 + 1 + 1 + 4];
                args[0..8].copy_from_slice(&tag.to_be_bytes());
                args[8] = 0; // redelivered
                args[9] = 0; // exchange: empty shortstr
                args[10] = 0; // routing-key: empty shortstr
                args[11..15].copy_from_slice(&remaining.to_be_bytes());
                send_method(s, sys, conn_id, channel, 60, 71, &args);
                let body_off = 18;
                let body_len = n - body_off;
                send_content(s, sys, conn_id, channel, body_off, body_len);
            }
            OP_CONSUME => {
                // 5 header + tag(8) + flags(1) + ctag_len(2)
                if n < 16 {
                    s.parse_errors = s.parse_errors.wrapping_add(1);
                    return;
                }
                let tag = u64::from_le_bytes([
                    s.scratch[5],
                    s.scratch[6],
                    s.scratch[7],
                    s.scratch[8],
                    s.scratch[9],
                    s.scratch[10],
                    s.scratch[11],
                    s.scratch[12],
                ]);
                let tl = u16::from_le_bytes([s.scratch[14], s.scratch[15]]) as usize;
                if tl > MAX_CTAG || 16 + tl > n {
                    s.parse_errors = s.parse_errors.wrapping_add(1);
                    return;
                }
                // Basic.Deliver: consumer-tag, delivery-tag, redelivered,
                // exchange, routing-key.
                let mut args = [0u8; 1 + MAX_CTAG + 8 + 1 + 1 + 1];
                args[0] = tl as u8;
                args[1..1 + tl].copy_from_slice(&s.scratch[16..16 + tl]);
                let mut p = 1 + tl;
                args[p..p + 8].copy_from_slice(&tag.to_be_bytes());
                p += 8;
                args[p] = 0; // redelivered
                args[p + 1] = 0; // exchange: empty shortstr
                args[p + 2] = 0; // routing-key: empty shortstr
                p += 3;
                send_method(s, sys, conn_id, channel, 60, 60, &args[..p]);
                let body_off = 16 + tl;
                let body_len = n - body_off;
                send_content(s, sys, conn_id, channel, body_off, body_len);
            }
            OP_CANCEL => {
                // 5 header + ctag_len(2)
                if n < 7 {
                    s.parse_errors = s.parse_errors.wrapping_add(1);
                    return;
                }
                let tl = u16::from_le_bytes([s.scratch[5], s.scratch[6]]) as usize;
                if tl > MAX_CTAG || 7 + tl > n {
                    s.parse_errors = s.parse_errors.wrapping_add(1);
                    return;
                }
                // Basic.Cancel: consumer-tag, no-wait.
                let mut args = [0u8; 1 + MAX_CTAG + 1];
                args[0] = tl as u8;
                args[1..1 + tl].copy_from_slice(&s.scratch[7..7 + tl]);
                args[1 + tl] = 0; // no-wait
                send_method(s, sys, conn_id, channel, 60, 30, &args[..2 + tl]);
            }
            _ => {}
        }
    }
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(s: &Amqp, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&s.frames_decoded.to_le_bytes());
    m[4..8].copy_from_slice(&s.responses_encoded.to_le_bytes());
    m[8..12].copy_from_slice(&s.parse_errors.to_le_bytes());
    12
}
