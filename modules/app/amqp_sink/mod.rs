// amqp_sink — AMQP 0-9-1 publisher exposing the generic
// `stream.sink.ordered_ack` surface. See manifest.toml for the
// contract mapping; the surface frames are inlined byte-for-byte from
// their owner (lattice `modules/common/cdc_wire.rs`).
//
// Structure mirrors mqtt_sink; the protocol half leans on
// `modules/common/cores/amqp_core.rs` for the connection handshake and
// frame parsing, adding what the consumer-oriented core lacks:
// Confirm.Select, Basic.Publish + content header/body frames, and the
// Basic.Ack/Nack (delivery-tag) matching.
//
//   Init -> Connecting -> WaitConnect -> Header -> WaitStart
//        -> WaitOpenOk -> WaitChanOk -> WaitConfirmOk -> Running
//        -> Reconnect -> Connecting ...
//
// Running: drain MSG_CDC_PUBLISH while the confirm window has room
// (the broker allows at most 8 pending publishes per connection),
// three frames per publish (method + content header + body), delivery
// tags sequential from 1 mapped to publish corrs in an ordered ring.
// Basic.Ack answers status 0; Basic.Nack answers a typed refusal.
// Entering Running emits LINK_UP; any connection loss emits LINK_DOWN
// and clears the ring — those corrs are exactly the set the producer
// must re-publish per the surface contract.

#![cfg_attr(not(feature = "host-test"), no_std)]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/cores/amqp_core.rs"]
mod amqp_core;

use amqp_core::{
    amqp_channel_open, amqp_open, amqp_parse_frame, amqp_parse_tune, amqp_protocol_header,
    amqp_start_ok, amqp_tune_ok, FRAME_BODY, FRAME_END, FRAME_HEADER, FRAME_HEARTBEAT,
    FRAME_METHOD,
};

// ── Net protocol ─────────────────────────────────────────────────────

const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;
const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;
const NET_CMD_CONNECT: u8 = 0x13;

// ── ordered_ack surface constants (owner: lattice cdc_wire.rs) ───────

const MSG_CDC_PUBLISH: u8 = 0xED;
const MSG_CDC_ACK: u8 = 0xEE;
const SINK_STATUS_OK: u8 = 0;
const SINK_REFUSE_OVERSIZE: u8 = 1;
const SINK_REFUSE_UNROUTABLE: u8 = 2;
const SINK_STATUS_LINK_DOWN: u8 = 16;
const SINK_STATUS_LINK_UP: u8 = 17;
const SINK_PUBLISH_OVERHEAD: usize = 13;

// ── AMQP method ids the core does not name ───────────────────────────

const CLASS_BASIC: u16 = 60;
const CLASS_CONFIRM: u16 = 85;
const BASIC_PUBLISH: u16 = 40;
const BASIC_ACK: u16 = 80;
const BASIC_NACK: u16 = 120;
const CONFIRM_SELECT: u16 = 10;
const CONFIRM_SELECT_OK: u16 = 11;

// ── Sizing ───────────────────────────────────────────────────────────

/// This broker's per-publish body ceiling; larger payloads are refused
/// with the typed OVERSIZE status before anything hits the wire.
const MAX_PUBLISH_BODY: usize = 1800;
/// Confirm window — the broker allows at most 8 pending per conn.
const INFLIGHT_CAP: usize = 8;

const TX_BUF_SIZE: usize = 4096;
const RX_BUF_SIZE: usize = 2048;
const CHAN_BUF_SIZE: usize = 8192;
const NET_BUF_SIZE: usize = 1600;
const MAX_TOPIC_LEN: usize = 63;

const CONNECT_TIMEOUT_MS: u64 = 10000;
const BACKOFF_INIT_MS: u64 = 2000;
const BACKOFF_MAX_MS: u64 = 60000;
const CHANNEL: u16 = 1;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Init = 0,
    Connecting = 1,
    WaitConnect = 2,
    Header = 3,
    WaitStart = 4,
    WaitOpenOk = 5,
    WaitChanOk = 6,
    WaitConfirmOk = 7,
    Running = 8,
    Reconnect = 9,
    Error = 255,
}

#[repr(C)]
struct SinkState {
    syscalls: *const SyscallTable,
    net_in_chan: i32,
    net_out_chan: i32,
    publish_in_chan: i32,
    ack_out_chan: i32,

    broker_ip: u32,
    broker_port: u16,
    heartbeat_s: u16,
    topic_len: u8,
    conn_id: u8,
    conn_present: u8,

    phase: Phase,
    /// Next delivery tag the broker will assign (sequential from 1
    /// after Confirm.Select).
    next_tag: u64,

    state_start_ms: u64,
    last_hb_ms: u64,
    last_activity_ms: u64,
    reconnect_at_ms: u64,
    backoff_ms: u64,

    rx_have: u16,
    tx_len: u16,
    tx_sent: u16,

    // Confirm window: delivery_tag -> corr. Ordered ring (tags are
    // sequential), so `multiple=1` acks fold every earlier slot too.
    inflight_tag: [u64; INFLIGHT_CAP],
    inflight_corr: [u64; INFLIGHT_CAP],
    inflight_used: u16,

    topic: [u8; MAX_TOPIC_LEN],

    tx_buf: [u8; TX_BUF_SIZE],
    rx_buf: [u8; RX_BUF_SIZE],
    chan_buf: [u8; CHAN_BUF_SIZE],
    net_buf: [u8; NET_BUF_SIZE],
}

mod params_def {
    use super::p_u16;
    use super::p_u32;
    use super::ptr_copy;
    use super::SinkState;
    use super::MAX_TOPIC_LEN;
    use super::SCHEMA_MAX;

    define_params! {
        SinkState;

        1, broker_ip, u32, 0
            => |s, d, len| { s.broker_ip = p_u32(d, len, 0, 0); };

        2, broker_port, u16, 9090
            => |s, d, len| { s.broker_port = p_u16(d, len, 0, 9090); };

        3, heartbeat_s, u16, 60
            => |s, d, len| { s.heartbeat_s = p_u16(d, len, 0, 60); };

        // Routing key (the broker composes `exchange + '.' + key`, so
        // combined length must stay within its 64-byte cap; publishes
        // go to the default exchange).
        4, topic, str, 0
            => |s, d, len| {
                let n = if len > MAX_TOPIC_LEN { MAX_TOPIC_LEN } else { len };
                s.topic_len = n as u8;
                if n > 0 {
                    ptr_copy(s.topic.as_mut_ptr(), d, n);
                }
            };
    }
}

#[inline(always)]
unsafe fn millis(s: &SinkState) -> u64 {
    dev_millis(&*s.syscalls)
}

#[inline(always)]
unsafe fn log_msg(s: &SinkState, msg: &[u8]) {
    dev_log(&*s.syscalls, 3, msg.as_ptr(), msg.len());
}

#[inline(always)]
unsafe fn log_err(s: &SinkState, msg: &[u8]) {
    dev_log(&*s.syscalls, 1, msg.as_ptr(), msg.len());
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    let mut i = 0;
    while i < n {
        *dst.add(i) = *src.add(i);
        i += 1;
    }
}

// ── Frame builders the core lacks ────────────────────────────────────

/// `[type][channel BE][size BE][class BE][method BE][args][0xCE]`.
fn method_frame(
    channel: u16,
    class: u16,
    method: u16,
    args: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let size = 4 + args.len();
    let total = 7 + size + 1;
    if total > out.len() {
        return None;
    }
    out[0] = FRAME_METHOD;
    out[1..3].copy_from_slice(&channel.to_be_bytes());
    out[3..7].copy_from_slice(&(size as u32).to_be_bytes());
    out[7..9].copy_from_slice(&class.to_be_bytes());
    out[9..11].copy_from_slice(&method.to_be_bytes());
    out[11..11 + args.len()].copy_from_slice(args);
    out[total - 1] = FRAME_END;
    Some(total)
}

/// Build the three publish frames (method + content header + body)
/// contiguously into `out`. Returns total length.
fn publish_frames(channel: u16, routing_key: &[u8], body: &[u8], out: &mut [u8]) -> Option<usize> {
    if routing_key.is_empty() || routing_key.len() > 63 || body.len() > MAX_PUBLISH_BODY {
        return None;
    }
    // Method args: reserved i16, exchange shortstr (empty = default),
    // routing-key shortstr, bits (no mandatory/immediate).
    let mut args = [0u8; 3 + 1 + 1 + 63 + 1];
    let mut a = 0usize;
    args[a] = 0;
    args[a + 1] = 0;
    a += 2;
    args[a] = 0; // exchange: empty shortstr
    a += 1;
    args[a] = routing_key.len() as u8;
    a += 1;
    args[a..a + routing_key.len()].copy_from_slice(routing_key);
    a += routing_key.len();
    args[a] = 0; // bits
    a += 1;
    let mut p = method_frame(channel, CLASS_BASIC, BASIC_PUBLISH, &args[..a], out)?;

    // Content header: class 60, weight 0, body size u64 BE, no props.
    let hdr_payload_len = 2 + 2 + 8 + 2;
    let hdr_total = 7 + hdr_payload_len + 1;
    if p + hdr_total > out.len() {
        return None;
    }
    let h = &mut out[p..];
    h[0] = FRAME_HEADER;
    h[1..3].copy_from_slice(&channel.to_be_bytes());
    h[3..7].copy_from_slice(&(hdr_payload_len as u32).to_be_bytes());
    h[7..9].copy_from_slice(&CLASS_BASIC.to_be_bytes());
    h[9..11].copy_from_slice(&0u16.to_be_bytes());
    h[11..19].copy_from_slice(&(body.len() as u64).to_be_bytes());
    h[19..21].copy_from_slice(&0u16.to_be_bytes());
    h[hdr_total - 1] = FRAME_END;
    p += hdr_total;

    // Body frame (single — 1800 fits well inside frame-max 8192).
    let body_total = 7 + body.len() + 1;
    if p + body_total > out.len() {
        return None;
    }
    let b = &mut out[p..];
    b[0] = FRAME_BODY;
    b[1..3].copy_from_slice(&channel.to_be_bytes());
    b[3..7].copy_from_slice(&(body.len() as u32).to_be_bytes());
    b[7..7 + body.len()].copy_from_slice(body);
    b[body_total - 1] = FRAME_END;
    p += body_total;
    Some(p)
}

fn heartbeat_frame(out: &mut [u8]) -> Option<usize> {
    if out.len() < 8 {
        return None;
    }
    out[0] = FRAME_HEARTBEAT;
    out[1..3].copy_from_slice(&0u16.to_be_bytes());
    out[3..7].copy_from_slice(&0u32.to_be_bytes());
    out[7] = FRAME_END;
    Some(8)
}

// ── ack_out emission ─────────────────────────────────────────────────

unsafe fn send_ack(s: &mut SinkState, corr: u64, status: u8) -> bool {
    if s.ack_out_chan < 0 {
        return false;
    }
    let sys = &*s.syscalls;
    let mut buf = [0u8; 3 + 9];
    buf[0] = MSG_CDC_ACK;
    buf[1] = 9;
    buf[2] = 0;
    buf[3..11].copy_from_slice(&corr.to_le_bytes());
    buf[11] = status;
    (sys.channel_write)(s.ack_out_chan, buf.as_mut_ptr(), buf.len()) == buf.len() as i32
}

unsafe fn emit_link_down(s: &mut SinkState) {
    let mut i = 0;
    while i < INFLIGHT_CAP {
        s.inflight_tag[i] = 0;
        s.inflight_corr[i] = 0;
        i += 1;
    }
    s.inflight_used = 0;
    let _ = send_ack(s, 0, SINK_STATUS_LINK_DOWN);
}

// ── TX ───────────────────────────────────────────────────────────────

unsafe fn flush_tx(s: &mut SinkState) -> bool {
    if s.tx_sent >= s.tx_len {
        return true;
    }
    if s.net_out_chan < 0 {
        return false;
    }
    let sys = &*s.syscalls;
    let remaining = (s.tx_len - s.tx_sent) as usize;
    let max_data = NET_BUF_SIZE - NET_FRAME_HDR - 1;
    let to_send = if remaining < max_data {
        remaining
    } else {
        max_data
    };
    if to_send == 0 {
        return true;
    }
    let scratch = s.net_buf.as_mut_ptr();
    let payload_ptr = scratch.add(NET_FRAME_HDR);
    *payload_ptr = s.conn_id;
    let src = s.tx_buf.as_ptr().add(s.tx_sent as usize);
    let mut i = 0;
    while i < to_send {
        *payload_ptr.add(1 + i) = *src.add(i);
        i += 1;
    }
    let payload_len = 1 + to_send;
    let len_le = (payload_len as u16).to_le_bytes();
    *scratch = NET_CMD_SEND;
    *scratch.add(1) = len_le[0];
    *scratch.add(2) = len_le[1];
    let total = NET_FRAME_HDR + payload_len;
    let written = (sys.channel_write)(s.net_out_chan, scratch, total);
    if written > 0 {
        s.tx_sent += to_send as u16;
    }
    s.tx_sent >= s.tx_len
}

#[inline(always)]
unsafe fn start_send(s: &mut SinkState, len: usize) -> bool {
    s.tx_len = len as u16;
    s.tx_sent = 0;
    flush_tx(s)
}

// ── Publish intake ───────────────────────────────────────────────────

unsafe fn handle_publish_intake(s: &mut SinkState) {
    if s.phase != Phase::Running {
        return; // backpressure by channel
    }
    loop {
        if s.tx_sent < s.tx_len && !flush_tx(s) {
            return;
        }
        if s.inflight_used as usize >= INFLIGHT_CAP || s.publish_in_chan < 0 {
            return;
        }
        let sys = &*s.syscalls;
        let poll = (sys.channel_poll)(s.publish_in_chan, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let mut hdr = [0u8; 3];
        if (sys.channel_read)(s.publish_in_chan, hdr.as_mut_ptr(), 3) < 3 {
            return;
        }
        let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
        if len > CHAN_BUF_SIZE {
            return;
        }
        if len > 0
            && ((sys.channel_read)(s.publish_in_chan, s.chan_buf.as_mut_ptr(), len) as usize) < len
        {
            return;
        }
        if hdr[0] != MSG_CDC_PUBLISH || len < SINK_PUBLISH_OVERHEAD {
            continue;
        }
        let cb = s.chan_buf.as_ptr();
        let corr = u64::from_le_bytes([
            *cb,
            *cb.add(1),
            *cb.add(2),
            *cb.add(3),
            *cb.add(4),
            *cb.add(5),
            *cb.add(6),
            *cb.add(7),
        ]);
        let klen = u16::from_le_bytes([*cb.add(9), *cb.add(10)]) as usize;
        let plen = u16::from_le_bytes([*cb.add(11), *cb.add(12)]) as usize;
        if corr == 0 || SINK_PUBLISH_OVERHEAD + klen + plen != len {
            continue;
        }
        if plen > MAX_PUBLISH_BODY {
            // Broker body ceiling: typed refusal, never truncation.
            let _ = send_ack(s, corr, SINK_REFUSE_OVERSIZE);
            continue;
        }
        let body = core::slice::from_raw_parts(cb.add(SINK_PUBLISH_OVERHEAD + klen), plen);
        let topic = &s.topic[..s.topic_len as usize];
        let mut frames = [0u8; TX_BUF_SIZE];
        let Some(flen) = publish_frames(CHANNEL, topic, body, &mut frames) else {
            let _ = send_ack(s, corr, SINK_REFUSE_UNROUTABLE);
            continue;
        };
        // Record the confirm slot BEFORE the send: the broker's tag
        // for this publish is `next_tag`.
        let mut slot = usize::MAX;
        let mut i = 0;
        while i < INFLIGHT_CAP {
            if s.inflight_corr[i] == 0 {
                slot = i;
                break;
            }
            i += 1;
        }
        if slot == usize::MAX {
            return;
        }
        s.inflight_tag[slot] = s.next_tag;
        s.inflight_corr[slot] = corr;
        s.inflight_used += 1;
        s.next_tag = s.next_tag.wrapping_add(1);
        s.tx_buf[..flen].copy_from_slice(&frames[..flen]);
        let _ = start_send(s, flen);
        // Serialize: publishes hit the broker in intake order, so
        // confirm tags stay sequential with the ring.
    }
}

// ── RX ───────────────────────────────────────────────────────────────

/// Fold a Basic.Ack / Basic.Nack. This broker always sends
/// `multiple = 0`, but `multiple = 1` is folded correctly anyway (tags
/// are sequential, so it covers every slot at or below `tag`).
unsafe fn fold_confirm(s: &mut SinkState, tag: u64, multiple: bool, nack: bool) {
    let mut i = 0;
    while i < INFLIGHT_CAP {
        if s.inflight_corr[i] != 0
            && (s.inflight_tag[i] == tag || (multiple && s.inflight_tag[i] <= tag))
        {
            let corr = s.inflight_corr[i];
            s.inflight_corr[i] = 0;
            s.inflight_tag[i] = 0;
            s.inflight_used = s.inflight_used.saturating_sub(1);
            let status = if nack {
                SINK_REFUSE_UNROUTABLE
            } else {
                SINK_STATUS_OK
            };
            let _ = send_ack(s, corr, status);
        }
        i += 1;
    }
}

unsafe fn process_frames(s: &mut SinkState) {
    loop {
        let have = s.rx_have as usize;
        let Some(frame) = amqp_parse_frame(&s.rx_buf[..have]) else {
            return;
        };
        let payload_start = frame.payload_start;
        let payload_end = frame.payload_end;
        let total = frame.total;
        if frame.ftype == FRAME_METHOD && payload_end - payload_start >= 4 {
            let class = u16::from_be_bytes([s.rx_buf[payload_start], s.rx_buf[payload_start + 1]]);
            let method =
                u16::from_be_bytes([s.rx_buf[payload_start + 2], s.rx_buf[payload_start + 3]]);
            let args_at = payload_start + 4;
            match (class, method) {
                (10, 10) if s.phase == Phase::WaitStart => {
                    // Connection.Start → StartOk, then wait for Tune.
                    let mut buf = [0u8; 128];
                    if let Some(n) = amqp_start_ok(b"sink", b"sink", &mut buf) {
                        s.tx_buf[..n].copy_from_slice(&buf[..n]);
                        let _ = start_send(s, n);
                    }
                }
                (10, 30) => {
                    // Connection.Tune → TuneOk + Open, one staged send.
                    let (chmax, fmax, hb) =
                        amqp_parse_tune(&s.rx_buf[args_at..payload_end]).unwrap_or((1, 8192, 60));
                    let hb = if s.heartbeat_s < hb {
                        s.heartbeat_s
                    } else {
                        hb
                    };
                    let mut buf = [0u8; 128];
                    let mut n = amqp_tune_ok(chmax, fmax, hb, &mut buf).unwrap_or(0);
                    if let Some(m) = amqp_open(b"/", &mut buf[n..]) {
                        n += m;
                    }
                    if n > 0 {
                        s.tx_buf[..n].copy_from_slice(&buf[..n]);
                        let _ = start_send(s, n);
                        s.phase = Phase::WaitOpenOk;
                    }
                }
                (10, 41) if s.phase == Phase::WaitOpenOk => {
                    let mut buf = [0u8; 64];
                    if let Some(n) = amqp_channel_open(CHANNEL, &mut buf) {
                        s.tx_buf[..n].copy_from_slice(&buf[..n]);
                        let _ = start_send(s, n);
                        s.phase = Phase::WaitChanOk;
                    }
                }
                (20, 11) if s.phase == Phase::WaitChanOk => {
                    let mut buf = [0u8; 32];
                    if let Some(n) =
                        method_frame(CHANNEL, CLASS_CONFIRM, CONFIRM_SELECT, &[], &mut buf)
                    {
                        s.tx_buf[..n].copy_from_slice(&buf[..n]);
                        let _ = start_send(s, n);
                        s.phase = Phase::WaitConfirmOk;
                    }
                }
                (CLASS_CONFIRM, CONFIRM_SELECT_OK) if s.phase == Phase::WaitConfirmOk => {
                    log_msg(s, b"[amqpsink] confirm mode on");
                    s.phase = Phase::Running;
                    s.next_tag = 1;
                    s.backoff_ms = BACKOFF_INIT_MS;
                    s.last_hb_ms = millis(s);
                    let _ = send_ack(s, 0, SINK_STATUS_LINK_UP);
                }
                (CLASS_BASIC, BASIC_ACK) | (CLASS_BASIC, BASIC_NACK)
                    if payload_end - args_at >= 9 =>
                {
                    let tag = u64::from_be_bytes([
                        s.rx_buf[args_at],
                        s.rx_buf[args_at + 1],
                        s.rx_buf[args_at + 2],
                        s.rx_buf[args_at + 3],
                        s.rx_buf[args_at + 4],
                        s.rx_buf[args_at + 5],
                        s.rx_buf[args_at + 6],
                        s.rx_buf[args_at + 7],
                    ]);
                    let bits = s.rx_buf[args_at + 8];
                    fold_confirm(s, tag, bits & 0x01 != 0, method == BASIC_NACK);
                }
                (10, 50) => {
                    // Connection.Close from the broker: honour + drop.
                    log_err(s, b"[amqpsink] server close");
                    enter_reconnect(s);
                    return;
                }
                _ => {}
            }
        }
        // Heartbeats and anything else: consumed as activity.
        s.last_activity_ms = millis(s);
        let remaining = s.rx_have as usize - total;
        if remaining > 0 {
            let buf = s.rx_buf.as_mut_ptr();
            let mut i = 0;
            while i < remaining {
                *buf.add(i) = *buf.add(total + i);
                i += 1;
            }
        }
        s.rx_have = remaining as u16;
    }
}

unsafe fn handle_rx(s: &mut SinkState) {
    if s.net_in_chan < 0 {
        return;
    }
    let sys = &*s.syscalls;
    let poll = (sys.channel_poll)(s.net_in_chan, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return;
    }
    let nbuf = s.net_buf.as_mut_ptr();
    let (msg_type, payload_len, full) =
        net_read_frame_aligned(sys, s.net_in_chan, nbuf, NET_BUF_SIZE);
    if matches!(msg_type, NET_MSG_DATA | NET_MSG_CLOSED | NET_MSG_ERROR)
        && payload_len >= 1
        && *nbuf.add(NET_FRAME_HDR) != s.conn_id
    {
        return;
    }
    if msg_type == NET_MSG_CLOSED || msg_type == NET_MSG_ERROR {
        log_err(s, b"[amqpsink] connection lost");
        enter_reconnect(s);
        return;
    }
    if msg_type == NET_MSG_DATA && payload_len > 1 {
        if full > payload_len {
            log_err(s, b"[amqpsink] truncated segment");
            enter_reconnect(s);
            return;
        }
        let data_len = payload_len - 1;
        process_frames(s);
        let space = RX_BUF_SIZE - s.rx_have as usize;
        if data_len > space {
            log_err(s, b"[amqpsink] oversized frame");
            enter_reconnect(s);
            return;
        }
        let src = nbuf.add(NET_FRAME_HDR + 1);
        let dst = s.rx_buf.as_mut_ptr().add(s.rx_have as usize);
        let mut i = 0;
        while i < data_len {
            *dst.add(i) = *src.add(i);
            i += 1;
        }
        s.rx_have += data_len as u16;
        s.last_activity_ms = millis(s);
        process_frames(s);
    }
}

unsafe fn handle_heartbeat(s: &mut SinkState) {
    if s.heartbeat_s == 0 {
        return;
    }
    let now = millis(s);
    let interval_ms = (s.heartbeat_s as u64) * 1000;
    if now.wrapping_sub(s.last_hb_ms) >= interval_ms / 2 && s.tx_sent >= s.tx_len {
        let mut buf = [0u8; 8];
        if let Some(n) = heartbeat_frame(&mut buf) {
            s.tx_buf[..n].copy_from_slice(&buf[..n]);
            let _ = start_send(s, n);
            s.last_hb_ms = now;
        }
    }
    if now.wrapping_sub(s.last_activity_ms) >= interval_ms * 2 {
        log_err(s, b"[amqpsink] broker timeout");
        enter_reconnect(s);
    }
}

unsafe fn enter_reconnect(s: &mut SinkState) {
    emit_link_down(s);
    if s.conn_present != 0 && s.net_out_chan >= 0 {
        let sys = &*s.syscalls;
        let mut payload = [0u8; 1];
        payload[0] = s.conn_id;
        net_write_frame(
            sys,
            s.net_out_chan,
            NET_CMD_CLOSE,
            payload.as_ptr(),
            1,
            s.net_buf.as_mut_ptr(),
            NET_BUF_SIZE,
        );
        s.conn_id = 0;
        s.conn_present = 0;
    }
    s.rx_have = 0;
    s.tx_len = 0;
    s.tx_sent = 0;
    let now = millis(s);
    s.reconnect_at_ms = now.wrapping_add(s.backoff_ms);
    log_msg(s, b"[amqpsink] reconnecting");
    s.backoff_ms = (s.backoff_ms * 2).min(BACKOFF_MAX_MS);
    s.phase = Phase::Reconnect;
}

// ── PIC module ABI ───────────────────────────────────────────────────

#[cfg_attr(not(feature = "host-test"), unsafe(no_mangle))]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<SinkState>() as u32
}

/// # Safety
/// `syscalls` is a kernel-owned table valid for the process lifetime.
#[cfg_attr(not(feature = "host-test"), unsafe(no_mangle))]
#[link_section = ".text.module_init"]
pub unsafe extern "C" fn module_init(_syscalls: *const c_void) {}

/// # Safety
/// `state` / `params` / `syscalls` are kernel-owned buffers passed
/// across the module ABI, sized per the manifest contract.
#[cfg_attr(not(feature = "host-test"), unsafe(no_mangle))]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() {
            return -2;
        }
        if state.is_null() {
            return -5;
        }
        if state_size < core::mem::size_of::<SinkState>() {
            return -6;
        }
        let s = &mut *(state as *mut SinkState);
        let state_bytes = state_size.min(core::mem::size_of::<SinkState>());
        #[cfg(not(feature = "host-test"))]
        __aeabi_memclr(state, state_bytes);
        #[cfg(feature = "host-test")]
        core::ptr::write_bytes(state, 0, state_bytes);

        s.syscalls = syscalls as *const SyscallTable;
        let sys = &*(syscalls as *const SyscallTable);
        s.net_in_chan = in_chan;
        s.net_out_chan = out_chan;
        s.publish_in_chan = dev_channel_port(sys, 0, 1);
        s.ack_out_chan = dev_channel_port(sys, 1, 1);
        params_def::parse_tlv(s, params, params_len);
        s.phase = Phase::Init;
        s.next_tag = 1;
        s.backoff_ms = BACKOFF_INIT_MS;
        log_msg(s, b"[amqpsink] init");
        if s.broker_ip == 0 || s.topic_len == 0 {
            log_err(s, b"[amqpsink] missing broker ip / topic");
            return -10;
        }
        0
    }
}

/// # Safety
/// `state` is the buffer a prior `module_new` initialised, exclusively
/// borrowed for the call.
#[cfg_attr(not(feature = "host-test"), unsafe(no_mangle))]
#[link_section = ".text.module_step"]
pub unsafe extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        if state.is_null() {
            return -1;
        }
        let s = &mut *(state as *mut SinkState);
        if s.syscalls.is_null() {
            return -1;
        }
        let sys_ptr = s.syscalls;
        let sys = &*sys_ptr;

        loop {
            match s.phase {
                Phase::Init => {
                    log_msg(s, b"[amqpsink] connecting");
                    s.phase = Phase::Connecting;
                    continue;
                }
                Phase::Connecting => {
                    if s.net_out_chan < 0 {
                        log_err(s, b"[amqpsink] no net_out");
                        s.phase = Phase::Error;
                        return -1;
                    }
                    let mut payload = [0u8; 8];
                    payload[0] = SOCK_TYPE_STREAM;
                    let ip_bytes = s.broker_ip.to_le_bytes();
                    payload[1] = ip_bytes[0];
                    payload[2] = ip_bytes[1];
                    payload[3] = ip_bytes[2];
                    payload[4] = ip_bytes[3];
                    payload[5] = (s.broker_port & 0xFF) as u8;
                    payload[6] = (s.broker_port >> 8) as u8;
                    payload[7] = dev_requester_tag(sys);
                    let wrote = net_write_frame(
                        sys,
                        s.net_out_chan,
                        NET_CMD_CONNECT,
                        payload.as_ptr(),
                        8,
                        s.net_buf.as_mut_ptr(),
                        NET_BUF_SIZE,
                    );
                    if wrote == 0 {
                        return 0;
                    }
                    s.state_start_ms = dev_millis(sys);
                    s.phase = Phase::WaitConnect;
                    return 0;
                }
                Phase::WaitConnect => {
                    if s.net_in_chan < 0 {
                        return 0;
                    }
                    let poll = (sys.channel_poll)(s.net_in_chan, POLL_IN);
                    if poll > 0 && ((poll as u32) & POLL_IN) != 0 {
                        let nbuf = s.net_buf.as_mut_ptr();
                        let (msg_type, payload_len) =
                            net_read_frame(sys, s.net_in_chan, nbuf, NET_BUF_SIZE);
                        if msg_type == NET_MSG_CONNECTED && payload_len >= 1 {
                            let tag = if payload_len >= 2 {
                                *nbuf.add(NET_FRAME_HDR + 1)
                            } else {
                                0
                            };
                            if payload_len < 2 || tag == dev_requester_tag(sys) {
                                s.conn_id = *nbuf.add(NET_FRAME_HDR);
                                s.conn_present = 1;
                                s.phase = Phase::Header;
                                continue;
                            }
                        } else if msg_type == NET_MSG_ERROR {
                            enter_reconnect(s);
                            return 0;
                        }
                    }
                    if dev_millis(sys).wrapping_sub(s.state_start_ms) > CONNECT_TIMEOUT_MS {
                        log_err(s, b"[amqpsink] connect timeout");
                        enter_reconnect(s);
                    }
                    return 0;
                }
                Phase::Header => {
                    let mut buf = [0u8; 8];
                    let Some(n) = amqp_protocol_header(&mut buf) else {
                        s.phase = Phase::Error;
                        return -1;
                    };
                    s.tx_buf[..n].copy_from_slice(&buf[..n]);
                    if !start_send(s, n) {
                        return 0;
                    }
                    s.state_start_ms = dev_millis(sys);
                    s.phase = Phase::WaitStart;
                    return 0;
                }
                Phase::WaitStart | Phase::WaitOpenOk | Phase::WaitChanOk | Phase::WaitConfirmOk => {
                    if !flush_tx(s) {
                        return 0;
                    }
                    handle_rx(s);
                    if !matches!(s.phase, Phase::Running | Phase::Reconnect | Phase::Error)
                        && dev_millis(sys).wrapping_sub(s.state_start_ms) > CONNECT_TIMEOUT_MS
                    {
                        log_err(s, b"[amqpsink] handshake timeout");
                        enter_reconnect(s);
                    }
                    return 0;
                }
                Phase::Running => {
                    handle_rx(s);
                    if s.phase != Phase::Running {
                        return 0;
                    }
                    let _ = flush_tx(s);
                    handle_publish_intake(s);
                    if s.phase != Phase::Running {
                        return 0;
                    }
                    handle_heartbeat(s);
                    return 0;
                }
                Phase::Reconnect => {
                    if dev_millis(sys) >= s.reconnect_at_ms {
                        s.phase = Phase::Connecting;
                        continue;
                    }
                    return 0;
                }
                Phase::Error => {
                    return -1;
                }
            }
        }
    }
}
