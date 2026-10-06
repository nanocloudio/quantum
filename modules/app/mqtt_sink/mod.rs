// mqtt_sink — MQTT 3.1.1 QoS 1 producer, a PROVIDER of the exchange
// contract. See manifest.toml for the delivery terms it declares.
//
// Structure follows `mqtt_client` (same net_proto handling, same
// reconnect discipline), reduced to a pure QoS-1 PRODUCER:
//
//   Init -> Connecting -> WaitConnect -> MqttConnect -> WaitConnack
//        -> Running -> Reconnect -> Connecting ...
//
// Running: read `request_in` while the in-flight window has room, one
// serialized MQTT PUBLISH per collected PUBLISH exchange, its body the
// payload byte for byte (the packet id maps to the exchange in the
// window); PUBACK answers 200 with an empty body on `response_out`.
// A connection lost after CONNACK writes LINK DOWN and clears the
// window — those exchanges are exactly the set the requester re-issues
// after the LINK UP that the next CONNACK writes. The exchange
// mechanics live in `modules/common/publish_exchange.rs`.

#![cfg_attr(not(feature = "host-test"), no_std)]
#![allow(
    dead_code,
    unused_imports,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

// The exchange contract, through the provider core's own mount of it.
#[path = "../../common/publish_exchange.rs"]
mod publish_exchange;
use publish_exchange::exchange::{status, PAYLOAD_MAX, RECORD_MAX};
use publish_exchange::PublishProvider;

// ── Net protocol (same vocabulary as mqtt_client) ────────────────────

const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;
const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;

// The dial: the peer's authority travels on the record, and the
// provider resolves a name.
use abi::contracts::net::net_proto::{
    connected_parts, error_parts, CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CONNECT_TO_MAX,
    CONN_ID_LEN, REQUESTER_TAG_NONE,
};

#[path = "../../common/authority.rs"]
mod authority;
use authority::{Authority, PORT_QUANTUM_BROKER};

// ── Sizing ───────────────────────────────────────────────────────────

const MAX_CLIENT_ID_LEN: usize = 32;
const MAX_TOPIC_LEN: usize = 96;
/// One PUBLISH: fixed header (1 + up to 3 length bytes), the topic with
/// its length prefix, the packet id, and the largest body the contract
/// collects — so every collected record fits, and none is refused here.
const TX_BUF_SIZE: usize = 4 + 2 + MAX_TOPIC_LEN + 2 + PAYLOAD_MAX;
const RX_BUF_SIZE: usize = 2048;
const NET_BUF_SIZE: usize = 1600;

/// QoS-1 in-flight window: publishes sent, PUBACK not yet seen. A full
/// window stops `request_in` being read (backpressure by channel).
const INFLIGHT_CAP: usize = 32;
/// Requests collected at once while their bodies arrive.
const REQUEST_SLOTS: usize = 2;
/// Records owed on `response_out`: one per window slot, one per read,
/// and the LINK pair.
const OWED_CAP: usize = INFLIGHT_CAP + 3;

type Desk = PublishProvider<REQUEST_SLOTS, PAYLOAD_MAX, INFLIGHT_CAP, OWED_CAP>;

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
    request_in_chan: i32,
    response_out_chan: i32,

    authority: Authority,
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

    client_id: [u8; MAX_CLIENT_ID_LEN],
    topic: [u8; MAX_TOPIC_LEN],

    /// Requests being collected, publishes awaiting PUBACK (keyed by
    /// packet id), and the records owed on `response_out`.
    desk: Desk,
    outbox: ExchangeOutbox,

    tx_buf: [u8; TX_BUF_SIZE],
    rx_buf: [u8; RX_BUF_SIZE],
    req_buf: [u8; RECORD_MAX],
    resp_buf: [u8; RECORD_MAX],
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

        // Tags 1 and 2 are retired.
        // The broker, `host[:port]`; the port defaults to 9090.
        6, authority, str, 0
            => |s, d, len| {
                if len > 0 {
                    s.authority.set(core::slice::from_raw_parts(d, len));
                }
            };

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

// ── response_out ─────────────────────────────────────────────────────

/// Place every record owed on `response_out`, in order, until the port
/// has no room; the outbox holds the one that did not fit.
unsafe fn flush_answers(s: &mut SinkState) {
    let sys = &*s.syscalls;
    loop {
        if !s.outbox.flush(sys, s.response_out_chan, &s.resp_buf) {
            return;
        }
        let Some(n) = s.desk.next_record(&mut s.resp_buf) else {
            return;
        };
        if !s.outbox.send(sys, s.response_out_chan, &s.resp_buf, n) {
            return;
        }
    }
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

// ── Request intake ───────────────────────────────────────────────────

/// After a LINK DOWN, read and drop request records until the LINK UP
/// is away: the requester re-issues every exchange it held open.
unsafe fn discard_requests(s: &mut SinkState) {
    let sys = &*s.syscalls;
    while s.desk.discarding() && s.request_in_chan >= 0 {
        let poll = (sys.channel_poll)(s.request_in_chan, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        if (sys.channel_read)(s.request_in_chan, s.req_buf.as_mut_ptr(), RECORD_MAX) <= 0 {
            return;
        }
    }
}

/// Read request records while connected, TX free, and the desk can take
/// one; each completed PUBLISH goes to the broker as one QoS-1 PUBLISH.
unsafe fn handle_requests(s: &mut SinkState) {
    if s.phase != Phase::Running {
        return; // backpressure by channel
    }
    loop {
        if s.tx_sent < s.tx_len && !flush_tx(s) {
            return;
        }
        if !s.desk.can_take() || s.request_in_chan < 0 {
            return;
        }
        let sys = &*s.syscalls;
        let poll = (sys.channel_poll)(s.request_in_chan, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.request_in_chan, s.req_buf.as_mut_ptr(), RECORD_MAX);
        if n <= 0 {
            return;
        }
        let Some(at) = s.desk.accept(&s.req_buf[..n as usize]) else {
            flush_answers(s);
            continue;
        };
        // One topic is one ordering unit: the target (the ordering key)
        // and BROADCAST both name it, so neither changes the PUBLISH.
        let Some(body) = s.desk.request(at).map(|r| r.body) else {
            continue;
        };

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
            body.as_ptr(),
            body.len(),
        );
        if pkt_len == 0 {
            // Past the frame budget: refused, never truncated.
            s.desk.refuse(at, status::TOO_LARGE);
            flush_answers(s);
            continue;
        }
        // Into the window BEFORE the send, so a PUBACK can never race an
        // unrecorded packet id.
        let now = millis(s);
        if !s.desk.dispatch(at, s.packet_id as u64, now) {
            return; // window full (ruled out by can_take)
        }
        let _ = start_send(s, pkt_len);
        // Serialize: one MQTT packet in TX at a time keeps broker
        // order = request order. The loop re-checks the flush.
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
                        // (Re)connected and accepting: LINK UP after a
                        // LINK DOWN.
                        s.desk.link_up();
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
                let _ = s.desk.settle(pid as u64, status::OK);
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
    // The unacknowledged window is now unknowable: LINK DOWN first, so
    // the requester learns before any LINK UP.
    s.desk.link_down();
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
        s.request_in_chan = dev_channel_port(sys, 0, 1);
        s.response_out_chan = dev_channel_port(sys, 1, 1);
        s.desk.reset();
        s.outbox = ExchangeOutbox::new();
        params_def::parse_tlv(s, params, params_len);
        s.phase = Phase::Init;
        s.packet_id = 0;
        s.backoff_ms = BACKOFF_INIT_MS;
        log_msg(s, b"[mqttsink] init");
        if !s.authority.adopt(PORT_QUANTUM_BROKER) {
            log_err(
                s,
                b"[mqttsink] refusing to construct: authority (host[:port]) is required",
            );
            return -10;
        }
        if s.topic_len == 0 {
            log_err(s, b"[mqttsink] missing topic");
            return -10;
        }
        if s.request_in_chan < 0 || s.response_out_chan < 0 {
            log_err(
                s,
                b"[mqttsink] refusing to construct: request_in and response_out must both be wired",
            );
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

        // Answers and LINK records leave whatever the phase, and requests
        // a LINK DOWN invalidated are dropped while the link is away.
        flush_answers(s);
        discard_requests(s);

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
                    let mut payload = [0u8; CONNECT_TO_MAX];
                    let n = s
                        .authority
                        .connect_record(&mut payload, Some(dev_requester_tag(sys)));
                    if n == 0 {
                        log_err(s, b"[mqttsink] authority does not fit a connect record");
                        s.phase = Phase::Error;
                        return -1;
                    }
                    let wrote = net_write_frame(
                        sys,
                        s.net_out_chan,
                        NET_CMD_CONNECT_TO,
                        payload.as_ptr(),
                        n,
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
                        let pl = core::slice::from_raw_parts(
                            nbuf.add(NET_FRAME_HDR) as *const u8,
                            payload_len,
                        );
                        if msg_type == NET_MSG_CONNECTED && payload_len >= CONN_ID_LEN {
                            let (id, tag) = connected_parts(pl);
                            if tag == dev_requester_tag(sys) || tag == REQUESTER_TAG_NONE {
                                s.conn_id = id;
                                s.conn_present = 1;
                                s.phase = Phase::MqttConnect;
                                continue;
                            }
                        } else if msg_type == NET_MSG_ERROR && payload_len > CONN_ID_LEN {
                            let (_id, _errno, tag) = error_parts(pl);
                            if tag == dev_requester_tag(sys) || tag == REQUESTER_TAG_NONE {
                                enter_reconnect(s);
                                return 0;
                            }
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
                    handle_requests(s);
                    flush_answers(s);
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
