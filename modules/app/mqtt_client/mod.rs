// MQTT 3.1.1 PIC Module
//
// Implements a minimal MQTT 3.1.1 client over TCP. QoS 0 only.
// Connects to a broker, subscribes to one topic, publishes events,
// and forwards received messages to an output channel.
//
// # Channels
//
// - in[0]: net_in — from IP module (net_proto TLV frames)
// - in[1]: app_in — from the app (framed messages to PUBLISH)
// - out[0]: net_out — to IP module (net_proto TLV frames)
// - out[1]: app_out — to the app (received PUBLISH payloads)
//
// # Channel Wire Protocol (mqtt <-> app)
//
//   [0]        topic_len: u8
//   [1..N]     topic: [u8; topic_len]
//   [N..N+32]  EventHeader (32 bytes)
//   [N+32..]   Event payload
//
// This module does not parse Event headers. It treats the entire
// channel payload after the topic as opaque bytes.
//
// # State Machine
//
//   Init -> Connecting -> WaitConnect -> MqttConnect ->
//   WaitConnack -> Subscribe -> WaitSuback -> Running -> Reconnect
//
// # MQTT Packets (QoS 0 subset)
//
// - CONNECT (0x10): Protocol "MQTT" v4, Clean Session, keepalive, client_id
// - CONNACK (0x20): Verify return code == 0
// - SUBSCRIBE (0x82): Packet ID, one topic filter + QoS 0
// - SUBACK (0x90): Verify granted QoS
// - PUBLISH (0x30): QoS 0, no packet ID, topic + payload
// - PINGREQ (0xC0): 2 bytes, sent every keepalive_s
// - PINGRESP (0xD0): 2 bytes, ignored
// - DISCONNECT (0xE0): 2 bytes (not used — we send CMD_CLOSE)

#![cfg_attr(not(feature = "host-test"), no_std)]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset. unreachable_patterns: defensive `_ => Error` arms in enum state-machine matches are intentional — adding a new variant should not silently bypass the error path"
)]


use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

// ============================================================================
// Constants
// ============================================================================

// Net protocol message types (downstream: IP → consumer)
const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;

// Net protocol command types (upstream: consumer → IP)
const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;
const NET_CMD_CONNECT: u8 = 0x13;

// Buffer sizes
const TX_BUF_SIZE: usize = 256;
/// Reassembly buffer. Sized to hold a full MAX_DATA_FRAGMENT (1460) TCP segment
/// plus headroom, because TCP legitimately coalesces several small MQTT packets
/// into one fragment — failing on those would reconnect on valid traffic.
const RX_BUF_SIZE: usize = 2048;
const CHAN_BUF_SIZE: usize = 192;
const NET_BUF_SIZE: usize = 1600; // > one TCP MSS (~1460) so a full segment fits

// Param layout sizes
const MAX_CLIENT_ID_LEN: usize = 32;
const MAX_TOPIC_LEN: usize = 96;

// Timeouts
const CONNECT_TIMEOUT_MS: u64 = 10000;
const CONNACK_TIMEOUT_MS: u64 = 10000;
const SUBACK_TIMEOUT_MS: u64 = 10000;

// Reconnect backoff
const BACKOFF_INIT_MS: u64 = 5000;
const BACKOFF_MAX_MS: u64 = 60000;

// MQTT packet types (high nibble of first byte)
const MQTT_CONNECT: u8 = 0x10;
const MQTT_CONNACK: u8 = 0x20;
const MQTT_PUBLISH: u8 = 0x30;
const MQTT_SUBSCRIBE: u8 = 0x82;
const MQTT_SUBACK: u8 = 0x90;
const MQTT_PINGREQ: u8 = 0xC0;
const MQTT_PINGRESP: u8 = 0xD0;

// ============================================================================
// Parameter Definitions
// ============================================================================

mod params_def {
    use super::MqttState;
    use super::p_u32;
    use super::p_u16;
    use super::p_u8;
    use super::ptr_copy;
    use super::MAX_CLIENT_ID_LEN;
    use super::MAX_TOPIC_LEN;
    use super::SCHEMA_MAX;

    define_params! {
        MqttState;

        1, broker_ip, u32, 0
            => |s, d, len| { s.broker_ip = p_u32(d, len, 0, 0); };

        2, broker_port, u16, 1883
            => |s, d, len| { s.broker_port = p_u16(d, len, 0, 1883); };

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

        5, subscribe_topic, str, 0
            => |s, d, len| {
                let n = if len > MAX_TOPIC_LEN { MAX_TOPIC_LEN } else { len };
                s.subscribe_topic_len = n as u8;
                if n > 0 {
                    ptr_copy(s.subscribe_topic.as_mut_ptr(), d, n);
                }
            };

        6, publish_topic, str, 0
            => |s, d, len| {
                let n = if len > MAX_TOPIC_LEN { MAX_TOPIC_LEN } else { len };
                s.publish_topic_len = n as u8;
                if n > 0 {
                    ptr_copy(s.publish_topic.as_mut_ptr(), d, n);
                }
            };

        // Optional second subscribe filter, sent in the same SUBSCRIBE packet
        // (MQTT allows many (filter, qos) pairs). Lets one node subscribe to a
        // per-node inbox AND a shared broadcast topic (e.g. sector/presence)
        // without a wildcard that would over-deliver other nodes' traffic.
        7, subscribe_topic2, str, 0
            => |s, d, len| {
                let n = if len > MAX_TOPIC_LEN { MAX_TOPIC_LEN } else { len };
                s.subscribe_topic2_len = n as u8;
                if n > 0 {
                    ptr_copy(s.subscribe_topic2.as_mut_ptr(), d, n);
                }
            };
    }
}

// ============================================================================
// State Machine
// ============================================================================

/// MQTT 3.1.1 connection lifecycle phases.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
enum MqttPhase {
    Init = 0,
    Connecting = 1,
    WaitConnect = 2,
    MqttConnect = 3,
    WaitConnack = 4,
    Subscribe = 5,
    WaitSuback = 6,
    Running = 7,
    Reconnect = 8,
    Error = 255,
}

// ============================================================================
// State Struct
// ============================================================================

#[repr(C)]
struct MqttState {
    syscalls: *const SyscallTable,
    app_in_chan: i32,
    app_out_chan: i32,
    net_in_chan: i32,
    net_out_chan: i32,

    // Connection params
    broker_ip: u32,
    broker_port: u16,
    keepalive_s: u8,
    client_id_len: u8,
    subscribe_topic_len: u8,
    subscribe_topic2_len: u8,
    publish_topic_len: u8,
    conn_id: u8,
    /// 1 once `MSG_CONNECTED` established the TCP connection — connection
    /// PRESENCE tracked separately from `conn_id`'s value because IP may assign
    /// conn_id 0 (keying off `conn_id != 0` would leak the first connection).
    conn_present: u8,

    // State machine
    phase: MqttPhase,
    retry_count: u8,
    packet_id: u16,

    // Timing
    state_start_ms: u64,
    last_ping_ms: u64,
    last_activity_ms: u64,
    reconnect_at_ms: u64,
    backoff_ms: u64,

    // RX reassembly (MQTT framing over net data)
    rx_have: u16,
    _rx_pad: u16,

    // TX tracking
    tx_len: u16,
    tx_sent: u16,

    // Module-scope telemetry: cumulative byte counters + last emit timestamp
    // (cadence gated on the wallclock since mqtt has no per-step counter).
    tlm: TlmCounters,
    tlm_last_ms: u64,

    // Strings
    client_id: [u8; MAX_CLIENT_ID_LEN],
    subscribe_topic: [u8; MAX_TOPIC_LEN],
    subscribe_topic2: [u8; MAX_TOPIC_LEN],
    publish_topic: [u8; MAX_TOPIC_LEN],

    // Buffers
    tx_buf: [u8; TX_BUF_SIZE],
    rx_buf: [u8; RX_BUF_SIZE],
    chan_buf: [u8; CHAN_BUF_SIZE],
    net_buf: [u8; NET_BUF_SIZE],
}

// ============================================================================
// Helpers
// ============================================================================

#[inline(always)]
unsafe fn millis(s: &MqttState) -> u64 {
    dev_millis(&*s.syscalls)
}

#[inline(always)]
unsafe fn log_msg(s: &MqttState, msg: &[u8]) {
    dev_log(&*s.syscalls, 3, msg.as_ptr(), msg.len());
}

#[inline(always)]
unsafe fn log_err(s: &MqttState, msg: &[u8]) {
    dev_log(&*s.syscalls, 1, msg.as_ptr(), msg.len());
}

/// Encode a u16 value as MQTT remaining length (1-2 bytes for values < 16384).
/// Returns number of bytes written.
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

/// Decode MQTT remaining length from buffer.
/// Returns (value, bytes_consumed). Returns (0, 0) if incomplete.
#[inline(always)]
unsafe fn decode_remaining_length(buf: *const u8, available: usize) -> (usize, usize) {
    let mut value: usize = 0;
    let mut multiplier: usize = 1;
    let mut i = 0;
    loop {
        if i >= available || i >= 4 {
            return (0, 0); // incomplete
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

/// Copy bytes using pointer arithmetic (no bounds checks).
#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    let mut i = 0;
    while i < n {
        *dst.add(i) = *src.add(i);
        i += 1;
    }
}

/// Write MQTT UTF-8 string (2-byte length prefix + string bytes).
/// Returns bytes written.
#[inline(always)]
unsafe fn write_mqtt_string(buf: *mut u8, s: *const u8, len: usize) -> usize {
    *buf = (len >> 8) as u8;
    *buf.add(1) = (len & 0xFF) as u8;
    ptr_copy(buf.add(2), s, len);
    2 + len
}

// ============================================================================
// MQTT Packet Builders
// ============================================================================

/// Build CONNECT packet in tx_buf. Returns total packet length.
unsafe fn build_connect(s: &mut MqttState) -> usize {
    let buf = s.tx_buf.as_mut_ptr();

    // Variable header: protocol name + level + flags + keepalive = 10 bytes
    // Payload: client_id string
    let payload_len = 2 + s.client_id_len as usize;
    let remaining = 10 + payload_len;

    let mut offset = 0;
    // Fixed header
    *buf.add(offset) = MQTT_CONNECT;
    offset += 1;
    offset += encode_remaining_length(buf.add(offset), remaining);

    // Protocol name "MQTT"
    offset += write_mqtt_string(buf.add(offset), b"MQTT".as_ptr(), 4);
    // Protocol level (4 = MQTT 3.1.1)
    *buf.add(offset) = 4;
    offset += 1;
    // Connect flags: Clean Session (bit 1)
    *buf.add(offset) = 0x02;
    offset += 1;
    // Keep alive (big-endian)
    *buf.add(offset) = 0;
    *buf.add(offset + 1) = s.keepalive_s;
    offset += 2;

    // Payload: client_id
    offset += write_mqtt_string(
        buf.add(offset),
        s.client_id.as_ptr(),
        s.client_id_len as usize,
    );

    offset
}

/// Build SUBSCRIBE packet in tx_buf. Returns total packet length.
unsafe fn build_subscribe(s: &mut MqttState) -> usize {
    let buf = s.tx_buf.as_mut_ptr();

    // Variable header: packet_id (2 bytes)
    // Payload: one or two (topic string + QoS byte) filter entries.
    let topic_len = s.subscribe_topic_len as usize;
    let topic2_len = s.subscribe_topic2_len as usize;
    let mut remaining = 2 + 2 + topic_len + 1; // packet_id + filter1 (string + qos)
    if topic2_len > 0 {
        remaining += 2 + topic2_len + 1; // filter2 (string + qos)
    }

    let mut offset = 0;
    // Fixed header
    *buf.add(offset) = MQTT_SUBSCRIBE;
    offset += 1;
    offset += encode_remaining_length(buf.add(offset), remaining);

    // Variable header: packet ID
    s.packet_id = s.packet_id.wrapping_add(1);
    if s.packet_id == 0 { s.packet_id = 1; }
    *buf.add(offset) = (s.packet_id >> 8) as u8;
    *buf.add(offset + 1) = (s.packet_id & 0xFF) as u8;
    offset += 2;

    // Payload: filter 1 (topic + QoS 0)
    offset += write_mqtt_string(
        buf.add(offset),
        s.subscribe_topic.as_ptr(),
        topic_len,
    );
    *buf.add(offset) = 0; // QoS 0
    offset += 1;

    // Payload: filter 2 (optional broadcast/presence topic)
    if topic2_len > 0 {
        offset += write_mqtt_string(
            buf.add(offset),
            s.subscribe_topic2.as_ptr(),
            topic2_len,
        );
        *buf.add(offset) = 0; // QoS 0
        offset += 1;
    }

    offset
}

/// Build PUBLISH packet in tx_buf. topic and payload come from chan_buf framing.
/// Returns total packet length, or 0 if it won't fit.
unsafe fn build_publish(
    buf: *mut u8,
    buf_size: usize,
    topic: *const u8,
    topic_len: usize,
    payload: *const u8,
    payload_len: usize,
) -> usize {
    // QoS 0 PUBLISH: no packet ID
    let remaining = 2 + topic_len + payload_len;

    // Estimate total: 1 (type) + 1-4 (remaining len) + remaining
    let rl_bytes = if remaining < 128 { 1 } else if remaining < 16384 { 2 } else { 3 };
    let total = 1 + rl_bytes + remaining;
    if total > buf_size {
        return 0;
    }

    let mut offset = 0;
    // Fixed header: PUBLISH, QoS 0, no DUP, no retain
    *buf.add(offset) = MQTT_PUBLISH;
    offset += 1;
    offset += encode_remaining_length(buf.add(offset), remaining);

    // Variable header: topic
    offset += write_mqtt_string(buf.add(offset), topic, topic_len);

    // Payload
    ptr_copy(buf.add(offset), payload, payload_len);
    offset += payload_len;

    offset
}

/// Build PINGREQ packet (always 2 bytes).
#[inline(always)]
unsafe fn build_pingreq(buf: *mut u8) -> usize {
    *buf = MQTT_PINGREQ;
    *buf.add(1) = 0;
    2
}

// ============================================================================
// Net send helper (CMD_SEND via channel framing)
// ============================================================================

/// Try to send tx_buf[tx_sent..tx_len] via CMD_SEND frames on net_out.
/// Returns true if all sent.
unsafe fn flush_tx(s: &mut MqttState) -> bool {
    if s.tx_sent >= s.tx_len {
        return true;
    }
    if s.net_out_chan < 0 { return false; }
    let sys = &*s.syscalls;
    let remaining = (s.tx_len - s.tx_sent) as usize;
    // Max data per frame: net_buf - frame_hdr(3) - conn_id(1)
    let max_data = NET_BUF_SIZE - NET_FRAME_HDR - 1;
    let to_send = if remaining < max_data { remaining } else { max_data };
    if to_send == 0 { return true; }

    // Build CMD_SEND payload: [conn_id][data...]
    let scratch = s.net_buf.as_mut_ptr();
    // Assemble payload starting at scratch[NET_FRAME_HDR]
    // net_write_frame will place header at scratch[0..3], payload at scratch[3..]
    // So we build a temporary payload buffer
    let payload_ptr = scratch.add(NET_FRAME_HDR);
    *payload_ptr = s.conn_id;
    let src = s.tx_buf.as_ptr().add(s.tx_sent as usize);
    let mut i = 0;
    while i < to_send {
        *payload_ptr.add(1 + i) = *src.add(i);
        i += 1;
    }
    let payload_len = 1 + to_send;

    // Write frame header manually to keep it in one buffer
    let len_le = (payload_len as u16).to_le_bytes();
    *scratch = NET_CMD_SEND;
    *scratch.add(1) = len_le[0];
    *scratch.add(2) = len_le[1];
    let total = NET_FRAME_HDR + payload_len;

    let written = (sys.channel_write)(s.net_out_chan, scratch, total);
    if written > 0 {
        s.tx_sent += to_send as u16;
        s.tlm.bytes_out = s.tlm.bytes_out.wrapping_add(to_send as u32);
    }
    s.tx_sent >= s.tx_len
}

/// Module-scope telemetry: emit cumulative `bytes_in` / `bytes_out` counters to
/// the `observe` collector when the telemetry port is wired (no-op otherwise),
/// at a ~5s wallclock cadence. Metric ids follow `[observability].metrics`
/// order: 0 = bytes_in, 1 = bytes_out. Counter semantics are monotonic, so the
/// deltas are NOT reset here.
#[inline(never)]
unsafe fn maybe_emit_telemetry(s: &mut MqttState) {
    let sys = &*s.syscalls;
    // Ring-based emission (§5.2): zero-cost when no consumer is subscribed.
    if !dev_telemetry_enabled(sys) {
        return;
    }
    let now = dev_millis(sys);
    if now.wrapping_sub(s.tlm_last_ms) < 5000 {
        return;
    }
    s.tlm_last_ms = now;
    let me = dev_self_index(sys);
    if me < 0 {
        return;
    }
    let midx = me as u16;
    let t = dev_micros(sys);
    let counter = abi::contracts::telemetry::METRIC_COUNTER;
    dev_telemetry_metric(sys, -1, midx, t, counter, 0, s.tlm.bytes_in as u64);
    dev_telemetry_metric(sys, -1, midx, t, counter, 1, s.tlm.bytes_out as u64);
}

/// Start sending: set tx_len and tx_sent=0, attempt initial flush.
#[inline(always)]
unsafe fn start_send(s: &mut MqttState, len: usize) -> bool {
    s.tx_len = len as u16;
    s.tx_sent = 0;
    flush_tx(s)
}

// ============================================================================
// RX Processing
// ============================================================================

/// Process a complete MQTT packet from rx_buf.
/// Returns bytes consumed, or 0 if incomplete.
unsafe fn process_rx_packet(s: &mut MqttState) -> usize {
    let have = s.rx_have as usize;
    if have < 2 {
        return 0;
    }

    let buf = s.rx_buf.as_ptr();
    let pkt_type = *buf & 0xF0;

    // Decode remaining length
    let (rem_len, rl_bytes) = decode_remaining_length(buf.add(1), have - 1);
    if rl_bytes == 0 {
        return 0; // incomplete remaining length
    }

    let total = 1 + rl_bytes + rem_len;
    if have < total {
        return 0; // incomplete packet
    }

    let var_start = 1 + rl_bytes;

    match pkt_type {
        0x20 => {
            // CONNACK: [session_present, return_code]
            if rem_len >= 2 {
                let rc = *buf.add(var_start + 1);
                if rc == 0 {
                    log_msg(s, b"[mqtt] connack ok");
                    if s.phase == MqttPhase::WaitConnack {
                        s.phase = MqttPhase::Subscribe;
                    }
                } else {
                    log_err(s, b"[mqtt] connack rejected");
                    enter_reconnect(s);
                }
            }
        }
        0x90 => {
            // SUBACK: [packet_id(2), return_code...]
            if rem_len >= 3 {
                let granted_qos = *buf.add(var_start + 2);
                if granted_qos <= 2 {
                    log_msg(s, b"[mqtt] suback ok");
                    if s.phase == MqttPhase::WaitSuback {
                        s.phase = MqttPhase::Running;
                        s.last_ping_ms = millis(s);
                        s.last_activity_ms = s.last_ping_ms;
                        s.backoff_ms = BACKOFF_INIT_MS;
                    }
                } else {
                    log_err(s, b"[mqtt] sub rejected");
                    enter_reconnect(s);
                }
            }
        }
        0x30 => {
            // PUBLISH (QoS 0): [topic_len(2), topic, payload]
            if rem_len >= 2 {
                let topic_len_hi = *buf.add(var_start) as usize;
                let topic_len_lo = *buf.add(var_start + 1) as usize;
                let topic_len = (topic_len_hi << 8) | topic_len_lo;
                let topic_start = var_start + 2;

                if topic_len + 2 <= rem_len {
                    let payload_start = topic_start + topic_len;
                    let payload_len = rem_len - 2 - topic_len;

                    // Write framed message to out_chan:
                    //   [topic_len:u8][topic][payload]
                    if s.app_out_chan >= 0 && topic_len <= 255 {
                        let total_frame = 1 + topic_len + payload_len;
                        if total_frame <= CHAN_BUF_SIZE {
                            let cb = s.chan_buf.as_mut_ptr();
                            *cb = topic_len as u8;
                            ptr_copy(cb.add(1), buf.add(topic_start), topic_len);
                            ptr_copy(cb.add(1 + topic_len), buf.add(payload_start), payload_len);

                            let sys = &*s.syscalls;
                            let poll = (sys.channel_poll)(s.app_out_chan, POLL_OUT);
                            if poll > 0 && ((poll as u32) & POLL_OUT) != 0 {
                                (sys.channel_write)(s.app_out_chan, cb, total_frame);
                            }
                            // If channel not ready, drop this message (QoS 0)
                        }
                    }
                }
            }
            s.last_activity_ms = millis(s);
        }
        0xD0 => {
            // PINGRESP — just note activity
            s.last_activity_ms = millis(s);
        }
        _ => {
            // Ignore unknown packet types
        }
    }

    total
}

/// Read from net_in channel, extract MSG_DATA payloads into rx_buf, process MQTT packets.
/// Also handles MSG_CLOSED and MSG_ERROR frames.
unsafe fn handle_rx(s: &mut MqttState) {
    if s.net_in_chan < 0 { return; }
    let sys = &*s.syscalls;

    // Try to process any buffered data first if buffer is full
    let space = RX_BUF_SIZE - s.rx_have as usize;
    if space == 0 {
        let consumed = process_rx_packet(s);
        if consumed > 0 {
            compact_rx(s, consumed);
        } else {
            s.rx_have = 0;
        }
        return;
    }

    // Poll net_in for data
    let poll = (sys.channel_poll)(s.net_in_chan, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return;
    }

    let nbuf = s.net_buf.as_mut_ptr();
    // Alignment-safe: net_in may be fanned to other consumers whose frames can
    // exceed our scratch; the reader drains the tail to keep the FIFO aligned and
    // reports the FULL payload length so we can detect a truncated segment.
    let (msg_type, payload_len, full) =
        net_read_frame_aligned(sys, s.net_in_chan, nbuf, NET_BUF_SIZE);

    // Established-stream isolation: only act on frames for OUR connection.
    if matches!(msg_type, NET_MSG_DATA | NET_MSG_CLOSED | NET_MSG_ERROR)
        && payload_len >= 1
        && *nbuf.add(NET_FRAME_HDR) != s.conn_id
    {
        return;
    }

    if msg_type == NET_MSG_CLOSED || msg_type == NET_MSG_ERROR {
        log_err(s, b"[mqtt] connection lost");
        enter_reconnect(s); // CMD_CLOSE + state reset + backoff
        return;
    }

    if msg_type == NET_MSG_DATA && payload_len > 1 {
        // Payload: [conn_id][data...]. A net-level truncation (`full > payload_len`,
        // frame bigger than our scratch) means the byte stream is unrecoverable.
        if full > payload_len {
            log_err(s, b"[mqtt] truncated segment; dropping connection");
            enter_reconnect(s);
            return;
        }
        let data_len = payload_len - 1;

        // INCREMENTAL DRAIN: process already-buffered complete MQTT packets
        // first, freeing the maximum reassembly space before admitting this
        // segment. TCP coalesces several small packets into one fragment (up to
        // MAX_DATA_FRAGMENT), and that's valid — never reconnect over it.
        loop {
            let consumed = process_rx_packet(s);
            if consumed == 0 {
                break;
            }
            compact_rx(s, consumed);
        }
        let space = RX_BUF_SIZE - s.rx_have as usize;
        if data_len > space {
            // Even drained, a single MQTT packet exceeds rx_buf — genuinely too
            // big for this minimal client. Reconnect rather than mis-parse.
            log_err(s, b"[mqtt] oversized packet; dropping connection");
            enter_reconnect(s);
            return;
        }
        let src = nbuf.add(NET_FRAME_HDR + 1); // skip frame hdr + conn_id
        let dst = s.rx_buf.as_mut_ptr().add(s.rx_have as usize);
        let mut i = 0;
        while i < data_len {
            *dst.add(i) = *src.add(i);
            i += 1;
        }
        s.rx_have += data_len as u16;
        s.tlm.bytes_in = s.tlm.bytes_in.wrapping_add(data_len as u32);
        s.last_activity_ms = millis(s);

        // Process the newly-coalesced packets.
        loop {
            let consumed = process_rx_packet(s);
            if consumed == 0 {
                break;
            }
            compact_rx(s, consumed);
        }
    }
}

/// Compact rx_buf by removing `consumed` bytes from the front.
#[inline(always)]
unsafe fn compact_rx(s: &mut MqttState, consumed: usize) {
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

/// Check app_in_chan for messages to publish.
unsafe fn handle_tx_from_channel(s: &mut MqttState) {
    // Don't read channel if we still have unsent data
    if s.tx_sent < s.tx_len {
        if !flush_tx(s) {
            return;
        }
    }

    let sys = &*s.syscalls;
    if s.app_in_chan < 0 { return; }

    let poll = (sys.channel_poll)(s.app_in_chan, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return;
    }

    // Read framed message: [topic_len:u8][topic][EventHeader+payload]
    let rc = (sys.channel_read)(
        s.app_in_chan,
        s.chan_buf.as_mut_ptr(),
        CHAN_BUF_SIZE,
    );
    if rc <= 1 { return; }
    let msg_len = rc as usize;

    let cb = s.chan_buf.as_ptr();
    let topic_len = *cb as usize;
    if topic_len == 0 || 1 + topic_len >= msg_len {
        return; // Invalid framing
    }

    let topic_ptr = cb.add(1);
    let payload_ptr = cb.add(1 + topic_len);
    let payload_len = msg_len - 1 - topic_len;

    // Build full topic: publish_topic_prefix + topic_from_channel
    // The channel provides the full topic, so we publish as-is.
    let pkt_len = build_publish(
        s.tx_buf.as_mut_ptr(),
        TX_BUF_SIZE,
        topic_ptr,
        topic_len,
        payload_ptr,
        payload_len,
    );

    if pkt_len > 0 {
        start_send(s, pkt_len);
    }
}

/// Send PINGREQ if keepalive interval elapsed.
unsafe fn handle_keepalive(s: &mut MqttState) {
    if s.keepalive_s == 0 { return; }

    let now = millis(s);
    let interval_ms = (s.keepalive_s as u64) * 1000;
    // Send ping at 75% of keepalive interval to stay ahead
    let ping_interval = interval_ms * 3 / 4;

    if now.wrapping_sub(s.last_ping_ms) >= ping_interval {
        // Only send if tx_buf is free
        if s.tx_sent >= s.tx_len {
            let len = build_pingreq(s.tx_buf.as_mut_ptr());
            start_send(s, len);
            s.last_ping_ms = now;
        }
    }

    // Check for broker silence (2x keepalive = dead)
    if now.wrapping_sub(s.last_activity_ms) >= interval_ms * 2 {
        log_err(s, b"[mqtt] broker timeout");
        enter_reconnect(s);
    }
}

/// Check net_in for MSG_CLOSED/MSG_ERROR (connection health).
unsafe fn check_net_health(s: &mut MqttState) {
    if s.net_in_chan < 0 { return; }
    let sys = &*s.syscalls;
    let poll = (sys.channel_poll)(s.net_in_chan, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return;
    }
    // Peek: if there's a frame, handle_rx will process it on next tick.
    // We don't consume here to avoid losing data frames.
}

// ============================================================================
// Transition to reconnect
// ============================================================================

unsafe fn enter_reconnect(s: &mut MqttState) {
    // Send CMD_CLOSE for current connection (presence, not conn_id != 0).
    if s.conn_present != 0 && s.net_out_chan >= 0 {
        let sys = &*s.syscalls;
        let mut payload = [0u8; 1];
        payload[0] = s.conn_id;
        net_write_frame(
            sys, s.net_out_chan, NET_CMD_CLOSE,
            payload.as_ptr(), 1,
            s.net_buf.as_mut_ptr(), NET_BUF_SIZE,
        );
        s.conn_id = 0;
        s.conn_present = 0;
    }

    // Reset RX/TX state
    s.rx_have = 0;
    s.tx_len = 0;
    s.tx_sent = 0;

    // Calculate backoff
    let now = millis(s);
    s.reconnect_at_ms = now.wrapping_add(s.backoff_ms);
    log_msg(s, b"[mqtt] reconnecting");

    // Increase backoff for next time (double, capped)
    s.backoff_ms = (s.backoff_ms * 2).min(BACKOFF_MAX_MS);

    s.phase = MqttPhase::Reconnect;
}

// ============================================================================
// Exported PIC Module Interface
// ============================================================================

#[cfg_attr(not(feature = "host-test"), unsafe(no_mangle))]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<MqttState>() as u32
}

#[cfg_attr(not(feature = "host-test"), unsafe(no_mangle))]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[cfg_attr(not(feature = "host-test"), unsafe(no_mangle))]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
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
        if syscalls.is_null() { return -2; }
        if state.is_null() { return -5; }
        if state_size < core::mem::size_of::<MqttState>() { return -6; }

        let s = &mut *(state as *mut MqttState);
        // Zero-init via memset. The SDK's `__aeabi_memclr` is only compiled for
        // bare-metal/wasm targets (libc provides it on the host), so the
        // host-test build uses the portable `write_bytes` equivalent. Firmware
        // is byte-identical.
        let state_bytes = state_size.min(core::mem::size_of::<MqttState>());
        #[cfg(not(feature = "host-test"))]
        __aeabi_memclr(state, state_bytes);
        #[cfg(feature = "host-test")]
        core::ptr::write_bytes(state, 0, state_bytes);

        s.syscalls = syscalls as *const SyscallTable;
        let sys = &*(syscalls as *const SyscallTable);

        // Port 0 in/out = net channels (from/to IP module)
        s.net_in_chan = in_chan;
        s.net_out_chan = out_chan;

        // Port 1 in/out = app channels (discovered via dev_channel_port)
        s.app_in_chan = dev_channel_port(sys, 0, 1);  // in[1]: from the app
        s.app_out_chan = dev_channel_port(sys, 1, 1); // out[1]: to the app
        s.conn_id = 0;
        s.conn_present = 0;

        // Parse params
        let is_tlv = !params.is_null() && params_len >= 4
            && *params == 0xFE && *params.add(1) == 0x01;

        if is_tlv {
            params_def::parse_tlv(s, params, params_len);
        } else if !params.is_null() && params_len >= 4 {
            // Legacy binary params:
            //   [0-3]     broker_ip: u32
            //   [4-5]     broker_port: u16
            //   [6]       keepalive_s: u8
            //   [7]       client_id_len: u8
            //   [8-39]    client_id: [u8; 32]
            //   [40]      subscribe_topic_len: u8
            //   [41]      publish_topic_len: u8
            //   [42-43]   reserved
            //   [44-139]  subscribe_topic: [u8; 96]
            //   [140-235] publish_topic_prefix: [u8; 96]
            let p = params;
            let plen = params_len;

            s.broker_ip = p_u32(p, plen, 0, 0);
            s.broker_port = p_u16(p, plen, 4, 1883);
            s.keepalive_s = p_u8(p, plen, 6, 60);
            s.client_id_len = p_u8(p, plen, 7, 0);

            let cid_len = (s.client_id_len as usize).min(MAX_CLIENT_ID_LEN);
            s.client_id_len = cid_len as u8;
            if cid_len > 0 && plen >= 8 + cid_len {
                ptr_copy(s.client_id.as_mut_ptr(), p.add(8), cid_len);
            }

            s.subscribe_topic_len = p_u8(p, plen, 40, 0);
            s.publish_topic_len = p_u8(p, plen, 41, 0);

            let sub_len = (s.subscribe_topic_len as usize).min(MAX_TOPIC_LEN);
            s.subscribe_topic_len = sub_len as u8;
            if sub_len > 0 && plen >= 44 + sub_len {
                ptr_copy(s.subscribe_topic.as_mut_ptr(), p.add(44), sub_len);
            }

            let pub_len = (s.publish_topic_len as usize).min(MAX_TOPIC_LEN);
            s.publish_topic_len = pub_len as u8;
            if pub_len > 0 && plen >= 140 + pub_len {
                ptr_copy(s.publish_topic.as_mut_ptr(), p.add(140), pub_len);
            }
        } else {
            params_def::set_defaults(s);
        }

        s.phase = MqttPhase::Init;
        s.packet_id = 0;
        s.backoff_ms = BACKOFF_INIT_MS;

        log_msg(s, b"[mqtt] init");

        // Validate
        if s.broker_ip == 0 {
            log_err(s, b"[mqtt] no broker ip");
            return -10;
        }

        0
    }
}

#[cfg_attr(not(feature = "host-test"), unsafe(no_mangle))]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        if state.is_null() { return -1; }
        let s = &mut *(state as *mut MqttState);
        if s.syscalls.is_null() { return -1; }
        // Copy pointer to avoid borrow issues
        let sys_ptr = s.syscalls;
        let sys = &*sys_ptr;

        // Module-scope metrics: cumulative byte counters, ~5s cadence, no-op
        // when the telemetry port is unwired.
        maybe_emit_telemetry(s);

        loop {
            match s.phase {
                MqttPhase::Init => {
                    log_msg(s, b"[mqtt] connecting");
                    s.phase = MqttPhase::Connecting;
                    continue;
                }

                MqttPhase::Connecting => {
                    if s.net_out_chan < 0 {
                        log_err(s, b"[mqtt] no net_out");
                        s.phase = MqttPhase::Error;
                        return -1;
                    }
                    // CMD_CONNECT: [sock_type][ip:4][port:2][requester_tag].
                    // The tag (our module index) lets us claim only our own
                    // MSG_CONNECTED on an ip.net_out fanned to other consumers.
                    let mut payload = [0u8; 8];
                    payload[0] = SOCK_TYPE_STREAM;
                    let ip_bytes = s.broker_ip.to_le_bytes();
                    *payload.as_mut_ptr().add(1) = *ip_bytes.as_ptr();
                    *payload.as_mut_ptr().add(2) = *ip_bytes.as_ptr().add(1);
                    *payload.as_mut_ptr().add(3) = *ip_bytes.as_ptr().add(2);
                    *payload.as_mut_ptr().add(4) = *ip_bytes.as_ptr().add(3);
                    *payload.as_mut_ptr().add(5) = (s.broker_port & 0xFF) as u8;
                    *payload.as_mut_ptr().add(6) = (s.broker_port >> 8) as u8;
                    *payload.as_mut_ptr().add(7) = dev_requester_tag(sys);
                    let wrote = net_write_frame(
                        sys, s.net_out_chan, NET_CMD_CONNECT,
                        payload.as_ptr(), 8,
                        s.net_buf.as_mut_ptr(), NET_BUF_SIZE,
                    );
                    if wrote == 0 { return 0; } // channel full, retry next tick
                    s.state_start_ms = dev_millis(sys);
                    s.phase = MqttPhase::WaitConnect;
                    return 0;
                }

                MqttPhase::WaitConnect => {
                    if s.net_in_chan < 0 { return 0; }
                    let poll = (sys.channel_poll)(s.net_in_chan, POLL_IN);
                    if poll > 0 && ((poll as u32) & POLL_IN) != 0 {
                        let nbuf = s.net_buf.as_mut_ptr();
                        let (msg_type, payload_len) = net_read_frame(sys, s.net_in_chan, nbuf, NET_BUF_SIZE);
                        if msg_type == NET_MSG_CONNECTED && payload_len >= 1 {
                            // Claim only our own outbound connection by tag.
                            let tag = if payload_len >= 2 { *nbuf.add(NET_FRAME_HDR + 1) } else { 0 };
                            let me = dev_requester_tag(sys);
                            if tag != 0 && tag != me {
                                return 0; // another consumer's connection.
                            }
                            s.conn_id = *nbuf.add(NET_FRAME_HDR);
                            s.conn_present = 1;
                            log_msg(s, b"[mqtt] tcp connected");
                            s.phase = MqttPhase::MqttConnect;
                            continue;
                        }
                        if msg_type == NET_MSG_ERROR {
                            // Connect failure carries our tag at payload[2]
                            // ([conn_id][errno][tag]); ignore another consumer's.
                            let etag = if payload_len >= 3 { *nbuf.add(NET_FRAME_HDR + 2) } else { 0 };
                            if etag == 0 || etag == dev_requester_tag(sys) {
                                log_err(s, b"[mqtt] connect rejected");
                                enter_reconnect(s);
                            }
                            return 0;
                        }
                        // A MSG_CLOSED here is NOT ours: we have no conn_id yet
                        // (still awaiting MSG_CONNECTED), so any close on the
                        // fanned net_in belongs to another connection — ignore it
                        // rather than treat it as our own rejection.
                    }
                    // Timeout
                    if dev_millis(sys).wrapping_sub(s.state_start_ms) >= CONNECT_TIMEOUT_MS {
                        log_err(s, b"[mqtt] connect timeout");
                        enter_reconnect(s);
                    }
                    return 0;
                }

                MqttPhase::MqttConnect => {
                    let len = build_connect(s);
                    start_send(s, len);
                    s.state_start_ms = dev_millis(sys);
                    s.rx_have = 0;
                    s.phase = MqttPhase::WaitConnack;
                    return 0;
                }

                MqttPhase::WaitConnack => {
                    // Flush pending TX
                    if s.tx_sent < s.tx_len {
                        flush_tx(s);
                        if s.tx_sent < s.tx_len {
                            return 0;
                        }
                    }

                    // Read and process (CONNACK handler changes state)
                    handle_rx(s);

                    // Check timeout
                    if s.phase == MqttPhase::WaitConnack {
                        if dev_millis(sys).wrapping_sub(s.state_start_ms) >= CONNACK_TIMEOUT_MS {
                            log_err(s, b"[mqtt] connack timeout");
                            enter_reconnect(s);
                        }
                    }
                    return 0;
                }

                MqttPhase::Subscribe => {
                    if s.subscribe_topic_len == 0 {
                        // No subscription — go straight to running
                        log_msg(s, b"[mqtt] no sub topic, running");
                        s.phase = MqttPhase::Running;
                        s.last_ping_ms = dev_millis(sys);
                        s.last_activity_ms = s.last_ping_ms;
                        s.backoff_ms = BACKOFF_INIT_MS;
                        continue;
                    }
                    let len = build_subscribe(s);
                    start_send(s, len);
                    s.state_start_ms = dev_millis(sys);
                    s.phase = MqttPhase::WaitSuback;
                    return 0;
                }

                MqttPhase::WaitSuback => {
                    // Flush pending TX
                    if s.tx_sent < s.tx_len {
                        flush_tx(s);
                        if s.tx_sent < s.tx_len {
                            return 0;
                        }
                    }

                    // Read and process (SUBACK handler changes state)
                    handle_rx(s);

                    // Check timeout
                    if s.phase == MqttPhase::WaitSuback {
                        if dev_millis(sys).wrapping_sub(s.state_start_ms) >= SUBACK_TIMEOUT_MS {
                            log_err(s, b"[mqtt] suback timeout");
                            enter_reconnect(s);
                        }
                    }
                    return 0;
                }

                MqttPhase::Running => {
                    // 1. Keepalive
                    handle_keepalive(s);
                    if s.phase != MqttPhase::Running { return 0; }

                    // 2. RX from broker
                    handle_rx(s);
                    if s.phase != MqttPhase::Running { return 0; }

                    // 3. TX from channel (app -> publish)
                    handle_tx_from_channel(s);
                    if s.phase != MqttPhase::Running { return 0; }

                    // 4. Net health (MSG_CLOSED/MSG_ERROR handled in handle_rx)
                    check_net_health(s);

                    return 0;
                }

                MqttPhase::Reconnect => {
                    // Wait for backoff to elapse
                    let now = dev_millis(sys);
                    if now.wrapping_sub(s.reconnect_at_ms) < 0x8000_0000_0000_0000 {
                        // Backoff elapsed (wrapping-safe: treat < 2^63 as "past")
                        s.phase = MqttPhase::Connecting;
                        continue;
                    }
                    return 0;
                }

                MqttPhase::Error => {
                    if s.conn_id != 0 && s.net_out_chan >= 0 {
                        let mut payload = [0u8; 1];
                        payload[0] = s.conn_id;
                        net_write_frame(
                            sys, s.net_out_chan, NET_CMD_CLOSE,
                            payload.as_ptr(), 1,
                            s.net_buf.as_mut_ptr(), NET_BUF_SIZE,
                        );
                        s.conn_id = 0;
                    }
                    return -1;
                }

                _ => return -1,
            }
        }
    }
}

// Wasm entry-point wrappers — no-op on non-wasm targets. See
// `modules/sdk/runtime/wasm_entry.rs` for the wasm32 module_init_wasm /
// module_step_wasm definitions.
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
