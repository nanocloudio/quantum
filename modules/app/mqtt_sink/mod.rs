// mqtt_sink — MQTT 3.1.1 QoS 1 producer exposing the generic
// `stream.sink.ordered_ack` surface. See manifest.toml for the
// contract mapping; the frame layouts below are the surface's wire
// (currently owned by lattice `modules/common/cdc_wire.rs`) and
// inlined byte-for-byte — that owner's conformance vectors are the
// cross-repo drift check.
//
// Structure is a fork of `mqtt_client` (same net_proto handling, same
// reconnect discipline), reduced to a pure QoS-1 PRODUCER:
//
//   Init -> Connecting -> WaitConnect -> MqttConnect -> WaitConnack
//        -> Running -> Reconnect -> Connecting ...
//
// Running: drain MSG_CDC_PUBLISH frames while the in-flight window
// has room, one serialized MQTT PUBLISH each (packet-id mapped to the
// publish corr in a fixed ring); PUBACK answers status 0 on ack_out.
// Entering Running emits LINK_UP; any connection loss emits LINK_DOWN
// and clears the in-flight ring — those corrs are exactly the set the
// producer must re-publish per the surface contract.

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

// ── Net protocol (same vocabulary as mqtt_client) ────────────────────

const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;
const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;
const NET_CMD_CONNECT: u8 = 0x13;

// ── ordered_ack surface constants (owner: lattice cdc_wire.rs) ───────

/// Channel envelope types on the pump pair.
const MSG_CDC_PUBLISH: u8 = 0xED;
const MSG_CDC_ACK: u8 = 0xEE;
/// Ack statuses.
const SINK_STATUS_OK: u8 = 0;
const SINK_REFUSE_OVERSIZE: u8 = 1;
const SINK_STATUS_LINK_DOWN: u8 = 16;
const SINK_STATUS_LINK_UP: u8 = 17;
/// SinkPublish fixed head: [corr:u64][flags:u8][klen:u16][plen:u16].
const SINK_PUBLISH_OVERHEAD: usize = 13;

// ── Sizing ───────────────────────────────────────────────────────────

/// One worst-case CDC envelope (lattice CDC_ENVELOPE_MAX = 4478) plus
/// MQTT topic + headers — the C-1 frame budget.
const TX_BUF_SIZE: usize = 8192;
const RX_BUF_SIZE: usize = 2048;
const CHAN_BUF_SIZE: usize = 8192;
const NET_BUF_SIZE: usize = 1600;
const MAX_CLIENT_ID_LEN: usize = 32;
const MAX_TOPIC_LEN: usize = 96;

/// QoS-1 in-flight window: publishes sent, PUBACK not yet seen. Full
/// window = stop draining publish_in (backpressure by channel).
const INFLIGHT_CAP: usize = 32;

const CONNECT_TIMEOUT_MS: u64 = 10000;
const BACKOFF_INIT_MS: u64 = 2000;
const BACKOFF_MAX_MS: u64 = 60000;

// MQTT packets.
const MQTT_CONNECT: u8 = 0x10;
const MQTT_CONNACK: u8 = 0x20;
/// PUBLISH with QoS 1 (bits 1-2 = 01).
const MQTT_PUBLISH_QOS1: u8 = 0x32;
const MQTT_PUBACK: u8 = 0x40;
const MQTT_PINGREQ: u8 = 0xC0;
const MQTT_PINGRESP: u8 = 0xD0;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Init = 0,
    Connecting = 1,
    WaitConnect = 2,
    MqttConnect = 3,
    WaitConnack = 4,
    Running = 5,
    Reconnect = 6,
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
    keepalive_s: u8,
    client_id_len: u8,
    topic_len: u8,
    conn_id: u16,
    conn_present: u8,

    phase: Phase,
    packet_id: u16,

    state_start_ms: u64,
    last_ping_ms: u64,
    last_activity_ms: u64,
    reconnect_at_ms: u64,
    backoff_ms: u64,

    rx_have: u16,
    tx_len: u16,
    tx_sent: u16,

    // In-flight window: packet_id -> corr. Slot i used when corr != 0.
    inflight_pkt: [u16; INFLIGHT_CAP],
    inflight_corr: [u64; INFLIGHT_CAP],
    inflight_used: u16,

    client_id: [u8; MAX_CLIENT_ID_LEN],
    topic: [u8; MAX_TOPIC_LEN],

    tx_buf: [u8; TX_BUF_SIZE],
    rx_buf: [u8; RX_BUF_SIZE],
    chan_buf: [u8; CHAN_BUF_SIZE],
    net_buf: [u8; NET_BUF_SIZE],
}

mod params_def {
    use super::p_u16;
    use super::p_u32;
    use super::p_u8;
    use super::ptr_copy;
    use super::SinkState;
    use super::MAX_CLIENT_ID_LEN;
    use super::MAX_TOPIC_LEN;
    use super::SCHEMA_MAX;

    define_params! {
        SinkState;

        1, broker_ip, u32, 0
            => |s, d, len| { s.broker_ip = p_u32(d, len, 0, 0); };

        2, broker_port, u16, 9090
            => |s, d, len| { s.broker_port = p_u16(d, len, 0, 9090); };

        3, keepalive_s, u8, 60
            => |s, d, len| { s.keepalive_s = p_u8(d, len, 0, 60); };

        4, client_id, str, 0
            => |s, d, len| {
                let n = if len > MAX_CLIENT_ID_LEN { MAX_CLIENT_ID_LEN } else { len };
                s.client_id_len = n as u8;
                if n > 0 {
                    ptr_copy(s.client_id.as_mut_ptr(), d, n);
                }
            };

        5, topic, str, 0
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

#[inline(always)]
unsafe fn encode_remaining_length(buf: *mut u8, mut len: usize) -> usize {
    let mut offset = 0;
    loop {
        let mut byte = (len & 0x7F) as u8;
        len >>= 7;
        if len > 0 {
            byte |= 0x80;
        }
        *buf.add(offset) = byte;
        offset += 1;
        if len == 0 {
            break;
        }
    }
    offset
}

#[inline(always)]
unsafe fn decode_remaining_length(buf: *const u8, available: usize) -> (usize, usize) {
    let mut value: usize = 0;
    let mut multiplier: usize = 1;
    let mut i = 0;
    loop {
        if i >= available || i >= 4 {
            return (0, 0);
        }
        let byte = *buf.add(i);
        value += (byte & 0x7F) as usize * multiplier;
        multiplier *= 128;
        i += 1;
        if (byte & 0x80) == 0 {
            return (value, i);
        }
    }
}

#[inline(always)]
unsafe fn write_mqtt_string(buf: *mut u8, s: *const u8, len: usize) -> usize {
    *buf = (len >> 8) as u8;
    *buf.add(1) = (len & 0xFF) as u8;
    ptr_copy(buf.add(2), s, len);
    2 + len
}

// ── ack_out emission ─────────────────────────────────────────────────

/// Write one `[MSG_CDC_ACK][len][corr:u64][status]` envelope.
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

/// LINK_DOWN: every in-flight corr becomes unknowable — the ring is
/// cleared, and the signal tells the pump to replay after LINK_UP.
unsafe fn emit_link_down(s: &mut SinkState) {
    let mut i = 0;
    while i < INFLIGHT_CAP {
        s.inflight_corr[i] = 0;
        s.inflight_pkt[i] = 0;
        i += 1;
    }
    s.inflight_used = 0;
    let _ = send_ack(s, 0, SINK_STATUS_LINK_DOWN);
}

// ── MQTT builders ────────────────────────────────────────────────────

unsafe fn build_connect(s: &mut SinkState) -> usize {
    let buf = s.tx_buf.as_mut_ptr();
    let payload_len = 2 + s.client_id_len as usize;
    let remaining = 10 + payload_len;
    let mut offset = 0;
    *buf.add(offset) = MQTT_CONNECT;
    offset += 1;
    offset += encode_remaining_length(buf.add(offset), remaining);
    offset += write_mqtt_string(buf.add(offset), b"MQTT".as_ptr(), 4);
    *buf.add(offset) = 4; // 3.1.1
    offset += 1;
    *buf.add(offset) = 0x02; // clean session
    offset += 1;
    *buf.add(offset) = 0;
    *buf.add(offset + 1) = s.keepalive_s;
    offset += 2;
    offset += write_mqtt_string(
        buf.add(offset),
        s.client_id.as_ptr(),
        s.client_id_len as usize,
    );
    offset
}

/// QoS-1 PUBLISH with packet id. Returns total length, 0 = won't fit.
unsafe fn build_publish_qos1(
    buf: *mut u8,
    buf_size: usize,
    topic: *const u8,
    topic_len: usize,
    packet_id: u16,
    payload: *const u8,
    payload_len: usize,
) -> usize {
    let remaining = 2 + topic_len + 2 + payload_len;
    let rl_bytes = if remaining < 128 {
        1
    } else if remaining < 16384 {
        2
    } else {
        3
    };
    let total = 1 + rl_bytes + remaining;
    if total > buf_size {
        return 0;
    }
    let mut offset = 0;
    *buf.add(offset) = MQTT_PUBLISH_QOS1;
    offset += 1;
    offset += encode_remaining_length(buf.add(offset), remaining);
    offset += write_mqtt_string(buf.add(offset), topic, topic_len);
    *buf.add(offset) = (packet_id >> 8) as u8;
    *buf.add(offset + 1) = (packet_id & 0xFF) as u8;
    offset += 2;
    ptr_copy(buf.add(offset), payload, payload_len);
    offset += payload_len;
    offset
}

#[inline(always)]
unsafe fn build_pingreq(buf: *mut u8) -> usize {
    *buf = MQTT_PINGREQ;
    *buf.add(1) = 0;
    2
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
    let max_data = NET_BUF_SIZE - NET_FRAME_HDR - 2;
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
    let cid = s.conn_id.to_le_bytes();
    *payload_ptr = cid[0];
    *payload_ptr.add(1) = cid[1];
    let src = s.tx_buf.as_ptr().add(s.tx_sent as usize);
    let mut i = 0;
    while i < to_send {
        *payload_ptr.add(2 + i) = *src.add(i);
        i += 1;
    }
    let payload_len = 2 + to_send;
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

// ── Publish intake (the sink half of the ordered_ack pair) ───────────

/// Drain publish frames while connected, TX free, and window room.
unsafe fn handle_publish_intake(s: &mut SinkState) {
    if s.phase != Phase::Running {
        return; // backpressure by channel (§11 term 4)
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
        // SinkPublish: [corr:u64][flags:u8][klen:u16][plen:u16][key][payload]
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
            continue; // malformed; nothing addressable to refuse
        }
        let payload_ptr = cb.add(SINK_PUBLISH_OVERHEAD + klen);

        // Fresh packet id (1..=65535, never 0).
        s.packet_id = s.packet_id.wrapping_add(1);
        if s.packet_id == 0 {
            s.packet_id = 1;
        }
        let pkt_len = build_publish_qos1(
            s.tx_buf.as_mut_ptr(),
            TX_BUF_SIZE,
            s.topic.as_ptr(),
            s.topic_len as usize,
            s.packet_id,
            payload_ptr,
            plen,
        );
        if pkt_len == 0 {
            // Frame budget exceeded: typed refusal, never truncation.
            let _ = send_ack(s, corr, SINK_REFUSE_OVERSIZE);
            continue;
        }
        // Record in-flight BEFORE the send so a PUBACK can never race
        // an unrecorded corr.
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
            return; // window full (checked above; defensive)
        }
        s.inflight_corr[slot] = corr;
        s.inflight_pkt[slot] = s.packet_id;
        s.inflight_used += 1;
        let _ = start_send(s, pkt_len);
        // Serialize: one MQTT packet in TX at a time keeps broker
        // order = publish order (§11 term 2). Loop re-checks flush.
    }
}

// ── RX ───────────────────────────────────────────────────────────────

unsafe fn process_rx_packet(s: &mut SinkState) -> usize {
    let have = s.rx_have as usize;
    if have < 2 {
        return 0;
    }
    let buf = s.rx_buf.as_ptr();
    let pkt_type = *buf & 0xF0;
    let (rem_len, rl_bytes) = decode_remaining_length(buf.add(1), have - 1);
    if rl_bytes == 0 {
        return 0;
    }
    let total = 1 + rl_bytes + rem_len;
    if have < total {
        return 0;
    }
    let var_start = 1 + rl_bytes;
    match pkt_type {
        0x20 => {
            // CONNACK
            if rem_len >= 2 {
                let rc = *buf.add(var_start + 1);
                if rc == 0 {
                    log_msg(s, b"[mqttsink] connack ok");
                    if s.phase == Phase::WaitConnack {
                        s.phase = Phase::Running;
                        s.last_ping_ms = millis(s);
                        s.last_activity_ms = s.last_ping_ms;
                        s.backoff_ms = BACKOFF_INIT_MS;
                        // (Re)connected and writable: the §11 signal
                        // the pump gates publishing on.
                        let _ = send_ack(s, 0, SINK_STATUS_LINK_UP);
                    }
                } else {
                    log_err(s, b"[mqttsink] connack rejected");
                    enter_reconnect(s);
                }
            }
        }
        0x40 => {
            // PUBACK: [packet_id:2] — durable acceptance at QoS 1.
            if rem_len >= 2 {
                let pid = ((*buf.add(var_start) as u16) << 8) | (*buf.add(var_start + 1) as u16);
                let mut i = 0;
                while i < INFLIGHT_CAP {
                    if s.inflight_corr[i] != 0 && s.inflight_pkt[i] == pid {
                        let corr = s.inflight_corr[i];
                        s.inflight_corr[i] = 0;
                        s.inflight_pkt[i] = 0;
                        s.inflight_used = s.inflight_used.saturating_sub(1);
                        let _ = send_ack(s, corr, SINK_STATUS_OK);
                        break;
                    }
                    i += 1;
                }
            }
            s.last_activity_ms = millis(s);
        }
        0xD0 => {
            s.last_activity_ms = millis(s);
        }
        _ => {}
    }
    total
}

unsafe fn compact_rx(s: &mut SinkState, consumed: usize) {
    let remaining = s.rx_have as usize - consumed;
    if remaining > 0 {
        let buf = s.rx_buf.as_mut_ptr();
        let mut i = 0;
        while i < remaining {
            *buf.add(i) = *buf.add(consumed + i);
            i += 1;
        }
    }
    s.rx_have = remaining as u16;
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
        && payload_len >= 2
        && u16::from_le_bytes([*nbuf.add(NET_FRAME_HDR), *nbuf.add(NET_FRAME_HDR + 1)]) != s.conn_id
    {
        return;
    }
    if msg_type == NET_MSG_CLOSED || msg_type == NET_MSG_ERROR {
        log_err(s, b"[mqttsink] connection lost");
        enter_reconnect(s);
        return;
    }
    if msg_type == NET_MSG_DATA && payload_len > 2 {
        if full > payload_len {
            log_err(s, b"[mqttsink] truncated segment");
            enter_reconnect(s);
            return;
        }
        let data_len = payload_len - 2;
        loop {
            let consumed = process_rx_packet(s);
            if consumed == 0 {
                break;
            }
            compact_rx(s, consumed);
        }
        let space = RX_BUF_SIZE - s.rx_have as usize;
        if data_len > space {
            log_err(s, b"[mqttsink] oversized packet");
            enter_reconnect(s);
            return;
        }
        let src = nbuf.add(NET_FRAME_HDR + 2);
        let dst = s.rx_buf.as_mut_ptr().add(s.rx_have as usize);
        let mut i = 0;
        while i < data_len {
            *dst.add(i) = *src.add(i);
            i += 1;
        }
        s.rx_have += data_len as u16;
        s.last_activity_ms = millis(s);
        loop {
            let consumed = process_rx_packet(s);
            if consumed == 0 {
                break;
            }
            compact_rx(s, consumed);
        }
    }
}

unsafe fn handle_keepalive(s: &mut SinkState) {
    if s.keepalive_s == 0 {
        return;
    }
    let now = millis(s);
    let interval_ms = (s.keepalive_s as u64) * 1000;
    let ping_interval = interval_ms * 3 / 4;
    if now.wrapping_sub(s.last_ping_ms) >= ping_interval && s.tx_sent >= s.tx_len {
        let len = build_pingreq(s.tx_buf.as_mut_ptr());
        start_send(s, len);
        s.last_ping_ms = now;
    }
    if now.wrapping_sub(s.last_activity_ms) >= interval_ms * 2 {
        log_err(s, b"[mqttsink] broker timeout");
        enter_reconnect(s);
    }
}

unsafe fn enter_reconnect(s: &mut SinkState) {
    // The unacked window is now unknowable: LINK_DOWN first, so the
    // pump learns before any LINK_UP re-opens publishing.
    emit_link_down(s);
    if s.conn_present != 0 && s.net_out_chan >= 0 {
        let sys = &*s.syscalls;
        let mut payload = [0u8; 2];
        payload[..2].copy_from_slice(&s.conn_id.to_le_bytes());
        net_write_frame(
            sys,
            s.net_out_chan,
            NET_CMD_CLOSE,
            payload.as_ptr(),
            2,
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
    log_msg(s, b"[mqttsink] reconnecting");
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
        s.packet_id = 0;
        s.backoff_ms = BACKOFF_INIT_MS;
        log_msg(s, b"[mqttsink] init");
        if s.broker_ip == 0 || s.topic_len == 0 {
            log_err(s, b"[mqttsink] missing broker ip / topic");
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
                    log_msg(s, b"[mqttsink] connecting");
                    s.phase = Phase::Connecting;
                    continue;
                }
                Phase::Connecting => {
                    if s.net_out_chan < 0 {
                        log_err(s, b"[mqttsink] no net_out");
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
                        if msg_type == NET_MSG_CONNECTED && payload_len >= 2 {
                            let tag = if payload_len >= 3 {
                                *nbuf.add(NET_FRAME_HDR + 2)
                            } else {
                                0
                            };
                            if payload_len < 3 || tag == dev_requester_tag(sys) {
                                s.conn_id = u16::from_le_bytes([
                                    *nbuf.add(NET_FRAME_HDR),
                                    *nbuf.add(NET_FRAME_HDR + 1),
                                ]);
                                s.conn_present = 1;
                                s.phase = Phase::MqttConnect;
                                continue;
                            }
                        } else if msg_type == NET_MSG_ERROR {
                            enter_reconnect(s);
                            return 0;
                        }
                    }
                    if dev_millis(sys).wrapping_sub(s.state_start_ms) > CONNECT_TIMEOUT_MS {
                        log_err(s, b"[mqttsink] connect timeout");
                        enter_reconnect(s);
                    }
                    return 0;
                }
                Phase::MqttConnect => {
                    let len = build_connect(s);
                    if !start_send(s, len) {
                        return 0;
                    }
                    s.state_start_ms = dev_millis(sys);
                    s.phase = Phase::WaitConnack;
                    return 0;
                }
                Phase::WaitConnack => {
                    if !flush_tx(s) {
                        return 0;
                    }
                    handle_rx(s);
                    if s.phase == Phase::WaitConnack
                        && dev_millis(sys).wrapping_sub(s.state_start_ms) > CONNECT_TIMEOUT_MS
                    {
                        log_err(s, b"[mqttsink] connack timeout");
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
                    handle_keepalive(s);
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
