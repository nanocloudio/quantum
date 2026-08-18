// kafka_sink — Kafka producer exposing the generic
// `stream.sink.ordered_ack` surface. See manifest.toml for the
// contract mapping; the surface frames are inlined byte-for-byte from
// their owner (lattice `modules/common/cdc_wire.rs`).
//
// Structure mirrors mqtt_sink; the protocol half leans on
// `modules/common/cores/kafka_core.rs` (request framing, response
// reassembly, CRC-32C) and adds what the consumer-oriented core
// lacks: a KEYED, partition-addressed Produce v7 builder that can
// name several partitions in one request (that multi-partition form
// IS the broadcast — the broker acks it only after every named
// partition is quorum-durable).
//
//   Init -> Connecting -> WaitConnect -> Metadata -> Running
//        -> Reconnect -> Connecting ...
//
// Metadata registers the topic with the broker (auto-create by
// mention) and reads back the real partition count, which the msg_key
// hash then addresses. Running drains MSG_CDC_PUBLISH while the
// produce window has room; Kafka correlation ids map to publish corrs
// in a fixed ring. Entering Running emits LINK_UP; any connection
// loss emits LINK_DOWN and clears the ring — those corrs are exactly
// the set the producer must re-publish per the surface contract.

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

#[path = "../../common/cores/kafka_core.rs"]
mod kafka_core;

use kafka_core::{kafka_crc32c, kafka_request, kafka_response_header, kafka_response_len, KReader};

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
const SINK_FLAG_BROADCAST: u8 = 0x01;

// ── Kafka constants ──────────────────────────────────────────────────

const API_PRODUCE: i16 = 0;
const API_METADATA: i16 = 3;
const PRODUCE_VERSION: i16 = 7;
const METADATA_VERSION: i16 = 1;
/// Kafka error code the broker uses for an over-budget records blob.
const ERR_MESSAGE_TOO_LARGE: i16 = 10;

// ── Sizing ───────────────────────────────────────────────────────────

/// The broker's per-partition records-blob ceiling. The RecordBatch
/// overhead is ~70 bytes, so payloads above this are refused with the
/// typed OVERSIZE status before anything hits the wire.
const MAX_RECORDS_BYTES: usize = 1900;
const RECORD_OVERHEAD: usize = 96;
const MAX_PAYLOAD: usize = MAX_RECORDS_BYTES - RECORD_OVERHEAD;
/// Produce window: requests sent, response not yet seen.
const INFLIGHT_CAP: usize = 8;
/// Partition ceiling (the broker clamps its `partitions` param 1..=16).
const MAX_PARTS: usize = 16;

const TX_BUF_SIZE: usize = 8192;
const RX_BUF_SIZE: usize = 4096;
const CHAN_BUF_SIZE: usize = 8192;
const NET_BUF_SIZE: usize = 1600;
const MAX_TOPIC_LEN: usize = 64;
const MAX_KEY_LEN: usize = 260;

const CONNECT_TIMEOUT_MS: u64 = 10000;
const REPLY_TIMEOUT_MS: u64 = 15000;
const BACKOFF_INIT_MS: u64 = 2000;
const BACKOFF_MAX_MS: u64 = 60000;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Init = 0,
    Connecting = 1,
    WaitConnect = 2,
    Metadata = 3,
    WaitMetadata = 4,
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
    topic_len: u8,
    conn_id: u8,
    conn_present: u8,

    phase: Phase,
    /// Monotonic Kafka correlation id.
    kcorr: i32,
    /// Correlation id of the outstanding Metadata request.
    meta_corr: i32,
    /// Partition count learned from Metadata (1..=16).
    partitions: u32,

    state_start_ms: u64,
    last_activity_ms: u64,
    reconnect_at_ms: u64,
    backoff_ms: u64,

    rx_have: u16,
    tx_len: u16,
    tx_sent: u16,

    // Produce window: kafka correlation id -> publish corr.
    inflight_kcorr: [i32; INFLIGHT_CAP],
    inflight_corr: [u64; INFLIGHT_CAP],
    inflight_sent_ms: [u64; INFLIGHT_CAP],
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

        3, topic, str, 0
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

// ── Big-endian put helpers (the core's are private) ──────────────────

fn bput(out: &mut [u8], p: &mut usize, b: &[u8]) -> Option<()> {
    out.get_mut(*p..*p + b.len())?.copy_from_slice(b);
    *p += b.len();
    Some(())
}

fn bput_i8(out: &mut [u8], p: &mut usize, v: i8) -> Option<()> {
    bput(out, p, &[v as u8])
}

fn bput_i16(out: &mut [u8], p: &mut usize, v: i16) -> Option<()> {
    bput(out, p, &v.to_be_bytes())
}

fn bput_i32(out: &mut [u8], p: &mut usize, v: i32) -> Option<()> {
    bput(out, p, &v.to_be_bytes())
}

fn bput_i64(out: &mut [u8], p: &mut usize, v: i64) -> Option<()> {
    bput(out, p, &v.to_be_bytes())
}

fn bput_str(out: &mut [u8], p: &mut usize, s: &[u8]) -> Option<()> {
    bput_i16(out, p, s.len() as i16)?;
    bput(out, p, s)
}

/// Zig-zag varint (Kafka record framing).
fn bput_varint(out: &mut [u8], p: &mut usize, v: i64) -> Option<()> {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let mut byte = (z & 0x7F) as u8;
        z >>= 7;
        if z != 0 {
            byte |= 0x80;
        }
        bput(out, p, &[byte])?;
        if z == 0 {
            return Some(());
        }
    }
}

/// One RecordBatch v2 with a single keyed record, into `out`.
/// Returns the batch length (the "records" blob).
fn record_batch(key: &[u8], value: &[u8], timestamp: i64, out: &mut [u8]) -> Option<usize> {
    let mut p = 0usize;
    bput_i64(out, &mut p, 0)?; // baseOffset
    let batch_len_pos = p;
    bput_i32(out, &mut p, 0)?; // batchLength (patched)
    bput_i32(out, &mut p, -1)?; // partitionLeaderEpoch
    bput_i8(out, &mut p, 2)?; // magic v2
    let crc_pos = p;
    bput_i32(out, &mut p, 0)?; // crc (patched)
    let body_start = p;
    bput_i16(out, &mut p, 0)?; // attributes
    bput_i32(out, &mut p, 0)?; // lastOffsetDelta
    bput_i64(out, &mut p, timestamp)?;
    bput_i64(out, &mut p, timestamp)?;
    bput_i64(out, &mut p, -1)?; // producerId
    bput_i16(out, &mut p, -1)?; // producerEpoch
    bput_i32(out, &mut p, -1)?; // baseSequence
    bput_i32(out, &mut p, 1)?; // record count
    let mut rec = [0u8; MAX_KEY_LEN + MAX_PAYLOAD + 32];
    let mut rp = 0usize;
    bput_i8(&mut rec, &mut rp, 0)?; // attributes
    bput_varint(&mut rec, &mut rp, 0)?; // timestampDelta
    bput_varint(&mut rec, &mut rp, 0)?; // offsetDelta
    if key.is_empty() {
        bput_varint(&mut rec, &mut rp, -1)?;
    } else {
        bput_varint(&mut rec, &mut rp, key.len() as i64)?;
        bput(&mut rec, &mut rp, key)?;
    }
    bput_varint(&mut rec, &mut rp, value.len() as i64)?;
    bput(&mut rec, &mut rp, value)?;
    bput_varint(&mut rec, &mut rp, 0)?; // header count
    bput_varint(out, &mut p, rp as i64)?;
    bput(out, &mut p, &rec[..rp])?;
    let body_end = p;
    let crc = kafka_crc32c(&out[body_start..body_end]);
    out[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
    let batch_len = (body_end - (batch_len_pos + 4)) as i32;
    out[batch_len_pos..batch_len_pos + 4].copy_from_slice(&batch_len.to_be_bytes());
    Some(body_end)
}

/// Produce v7 body: one topic, the given partitions each carrying the
/// same keyed record, acks = -1 (all).
fn produce_body(
    topic: &[u8],
    partitions: &[u32],
    key: &[u8],
    value: &[u8],
    timestamp: i64,
    out: &mut [u8],
) -> Option<usize> {
    let mut batch = [0u8; MAX_RECORDS_BYTES + 128];
    let blen = record_batch(key, value, timestamp, &mut batch)?;
    if blen > MAX_RECORDS_BYTES {
        return None;
    }
    let mut p = 0usize;
    bput_i16(out, &mut p, -1)?; // transactional_id null
    bput_i16(out, &mut p, -1)?; // acks = all
    bput_i32(out, &mut p, 30_000)?; // timeout
    bput_i32(out, &mut p, 1)?; // topic count
    bput_str(out, &mut p, topic)?;
    bput_i32(out, &mut p, partitions.len() as i32)?;
    for &part in partitions {
        bput_i32(out, &mut p, part as i32)?;
        bput_i32(out, &mut p, blen as i32)?;
        bput(out, &mut p, &batch[..blen])?;
    }
    Some(p)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
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
        s.inflight_kcorr[i] = 0;
        s.inflight_corr[i] = 0;
        s.inflight_sent_ms[i] = 0;
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
        let flags = *cb.add(8);
        let klen = u16::from_le_bytes([*cb.add(9), *cb.add(10)]) as usize;
        let plen = u16::from_le_bytes([*cb.add(11), *cb.add(12)]) as usize;
        if corr == 0 || SINK_PUBLISH_OVERHEAD + klen + plen != len || klen > MAX_KEY_LEN {
            continue;
        }
        if plen > MAX_PAYLOAD {
            // Broker records-blob ceiling: typed refusal, never
            // truncation.
            let _ = send_ack(s, corr, SINK_REFUSE_OVERSIZE);
            continue;
        }
        let msg_key = core::slice::from_raw_parts(cb.add(SINK_PUBLISH_OVERHEAD), klen);
        let payload = core::slice::from_raw_parts(cb.add(SINK_PUBLISH_OVERHEAD + klen), plen);

        // Partition addressing (term 2/5): key hash normally; EVERY
        // partition in one request for broadcast — the broker acks
        // that request only after the slowest partition is durable.
        let nparts = s.partitions.max(1).min(MAX_PARTS as u32);
        let mut parts = [0u32; MAX_PARTS];
        let parts_used: usize = if flags & SINK_FLAG_BROADCAST != 0 {
            let mut i = 0usize;
            while i < nparts as usize {
                parts[i] = i as u32;
                i += 1;
            }
            nparts as usize
        } else {
            parts[0] = (fnv1a64(msg_key) % nparts as u64) as u32;
            1
        };

        let now = millis(s) as i64;
        let mut body = [0u8; TX_BUF_SIZE];
        let Some(blen) = produce_body(
            &s.topic[..s.topic_len as usize],
            &parts[..parts_used],
            msg_key,
            payload,
            now,
            &mut body,
        ) else {
            let _ = send_ack(s, corr, SINK_REFUSE_OVERSIZE);
            continue;
        };
        s.kcorr = s.kcorr.wrapping_add(1);
        if s.kcorr <= 0 {
            s.kcorr = 1;
        }
        let mut req = [0u8; TX_BUF_SIZE];
        let Some(rlen) = kafka_request(
            API_PRODUCE,
            PRODUCE_VERSION,
            s.kcorr,
            b"sink",
            &body[..blen],
            &mut req,
        ) else {
            let _ = send_ack(s, corr, SINK_REFUSE_OVERSIZE);
            continue;
        };
        // Record in-flight BEFORE the send.
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
        s.inflight_kcorr[slot] = s.kcorr;
        s.inflight_corr[slot] = corr;
        s.inflight_sent_ms[slot] = millis(s);
        s.inflight_used += 1;
        s.tx_buf[..rlen].copy_from_slice(&req[..rlen]);
        let _ = start_send(s, rlen);
        // Serialize sends: broker order = publish order (term 2).
    }
}

// ── RX ───────────────────────────────────────────────────────────────

/// Parse a ProduceResponse body: `true` when every partition reported
/// error 0; `Some(err)` = the first non-zero error code.
fn produce_errors(body: &[u8]) -> Option<i16> {
    let mut r = KReader::new(body);
    let topics = r.i32()?;
    let mut first_err: i16 = 0;
    let mut t = 0;
    while t < topics {
        let _topic = r.string()?;
        let parts = r.i32()?;
        let mut p = 0;
        while p < parts {
            let _idx = r.i32()?;
            let err = r.i16()?;
            let _base = r.i64()?;
            let _log_append = r.i64()?; // v2+
            let _log_start = r.i64()?; // v5+
            if err != 0 && first_err == 0 {
                first_err = err;
            }
            p += 1;
        }
        t += 1;
    }
    Some(first_err)
}

unsafe fn process_responses(s: &mut SinkState) {
    loop {
        let have = s.rx_have as usize;
        let Some(total) = kafka_response_len(&s.rx_buf[..have]) else {
            return;
        };
        let mut resp = [0u8; RX_BUF_SIZE];
        resp[..total].copy_from_slice(&s.rx_buf[..total]);
        // Compact first so any early-return below cannot re-process.
        let remaining = have - total;
        if remaining > 0 {
            let buf = s.rx_buf.as_mut_ptr();
            let mut i = 0;
            while i < remaining {
                *buf.add(i) = *buf.add(total + i);
                i += 1;
            }
        }
        s.rx_have = remaining as u16;
        s.last_activity_ms = millis(s);

        let Some((corr, body_off)) = kafka_response_header(&resp[..total]) else {
            continue;
        };
        let body = &resp[body_off..total];

        if s.phase == Phase::WaitMetadata && corr == s.meta_corr {
            // Metadata v1: brokers array, controller, topics; we only
            // need the first topic's partition count.
            if let Some(nparts) = parse_metadata_partitions(body) {
                s.partitions = nparts.clamp(1, MAX_PARTS as u32);
                s.phase = Phase::Running;
                s.backoff_ms = BACKOFF_INIT_MS;
                log_msg(s, b"[kafkasink] metadata ok");
                let _ = send_ack(s, 0, SINK_STATUS_LINK_UP);
            } else {
                log_err(s, b"[kafkasink] metadata unparseable");
                enter_reconnect(s);
                return;
            }
            continue;
        }

        // Produce response: match the correlation id in the window.
        let mut i = 0;
        while i < INFLIGHT_CAP {
            if s.inflight_corr[i] != 0 && s.inflight_kcorr[i] == corr {
                let pub_corr = s.inflight_corr[i];
                s.inflight_corr[i] = 0;
                s.inflight_kcorr[i] = 0;
                s.inflight_sent_ms[i] = 0;
                s.inflight_used = s.inflight_used.saturating_sub(1);
                match produce_errors(body) {
                    Some(0) => {
                        let _ = send_ack(s, pub_corr, SINK_STATUS_OK);
                    }
                    Some(ERR_MESSAGE_TOO_LARGE) => {
                        let _ = send_ack(s, pub_corr, SINK_REFUSE_OVERSIZE);
                    }
                    _ => {
                        let _ = send_ack(s, pub_corr, SINK_REFUSE_UNROUTABLE);
                    }
                }
                break;
            }
            i += 1;
        }
    }
}

/// Metadata v1 response: skip brokers + controller, read the first
/// topic's partition count. v1 layout per this broker:
/// `[brokers:i32]{node:i32, host:STRING, port:i32, rack:NSTRING}`
/// `[controller:i32][topics:i32]{err:i16, name:STRING,
/// is_internal:u8, partitions:i32 ...}` — is_internal is one BYTE,
/// which KReader cannot express, so the tail is indexed manually.
fn parse_metadata_partitions(body: &[u8]) -> Option<u32> {
    let mut r = KReader::new(body);
    let brokers = r.i32()?;
    let mut b = 0;
    while b < brokers {
        let _node = r.i32()?;
        let _host = r.string()?;
        let _port = r.i32()?;
        let _rack = r.string()?; // nullable, v1+
        b += 1;
    }
    let _controller = r.i32()?;
    let topics = r.i32()?;
    if topics < 1 {
        return None;
    }
    let _err = r.i16()?;
    let _name = r.string()?;
    let at = r.pos();
    // is_internal:u8, then the partitions array count.
    let parts = body.get(at + 1..at + 5)?;
    let n = i32::from_be_bytes([parts[0], parts[1], parts[2], parts[3]]);
    if n < 1 {
        return None;
    }
    Some(n as u32)
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
        log_err(s, b"[kafkasink] connection lost");
        enter_reconnect(s);
        return;
    }
    if msg_type == NET_MSG_DATA && payload_len > 1 {
        if full > payload_len {
            log_err(s, b"[kafkasink] truncated segment");
            enter_reconnect(s);
            return;
        }
        let data_len = payload_len - 1;
        process_responses(s);
        let space = RX_BUF_SIZE - s.rx_have as usize;
        if data_len > space {
            log_err(s, b"[kafkasink] oversized response");
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
        process_responses(s);
    }
}

/// A produce that never got a response past the reply timeout means
/// the stream is wedged — reconnect (LINK_DOWN invalidates the
/// window, the producer replays).
unsafe fn sweep_stale(s: &mut SinkState) {
    let now = millis(s);
    let mut i = 0;
    while i < INFLIGHT_CAP {
        if s.inflight_corr[i] != 0 && now.wrapping_sub(s.inflight_sent_ms[i]) >= REPLY_TIMEOUT_MS {
            log_err(s, b"[kafkasink] produce timeout");
            enter_reconnect(s);
            return;
        }
        i += 1;
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
    log_msg(s, b"[kafkasink] reconnecting");
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
        s.kcorr = 0;
        s.partitions = 1;
        s.backoff_ms = BACKOFF_INIT_MS;
        log_msg(s, b"[kafkasink] init");
        if s.broker_ip == 0 || s.topic_len == 0 {
            log_err(s, b"[kafkasink] missing broker ip / topic");
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
                    log_msg(s, b"[kafkasink] connecting");
                    s.phase = Phase::Connecting;
                    continue;
                }
                Phase::Connecting => {
                    if s.net_out_chan < 0 {
                        log_err(s, b"[kafkasink] no net_out");
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
                                s.phase = Phase::Metadata;
                                continue;
                            }
                        } else if msg_type == NET_MSG_ERROR {
                            enter_reconnect(s);
                            return 0;
                        }
                    }
                    if dev_millis(sys).wrapping_sub(s.state_start_ms) > CONNECT_TIMEOUT_MS {
                        log_err(s, b"[kafkasink] connect timeout");
                        enter_reconnect(s);
                    }
                    return 0;
                }
                Phase::Metadata => {
                    // Register the topic (auto-create by mention) and
                    // learn the partition count.
                    let mut body = [0u8; 128];
                    let mut p = 0usize;
                    if bput_i32(&mut body, &mut p, 1).is_none()
                        || bput_str(&mut body, &mut p, &s.topic[..s.topic_len as usize]).is_none()
                    {
                        s.phase = Phase::Error;
                        return -1;
                    }
                    s.kcorr = s.kcorr.wrapping_add(1);
                    if s.kcorr <= 0 {
                        s.kcorr = 1;
                    }
                    s.meta_corr = s.kcorr;
                    let mut req = [0u8; 256];
                    let Some(rlen) = kafka_request(
                        API_METADATA,
                        METADATA_VERSION,
                        s.kcorr,
                        b"sink",
                        &body[..p],
                        &mut req,
                    ) else {
                        s.phase = Phase::Error;
                        return -1;
                    };
                    s.tx_buf[..rlen].copy_from_slice(&req[..rlen]);
                    if !start_send(s, rlen) {
                        return 0;
                    }
                    s.state_start_ms = dev_millis(sys);
                    s.phase = Phase::WaitMetadata;
                    return 0;
                }
                Phase::WaitMetadata => {
                    if !flush_tx(s) {
                        return 0;
                    }
                    handle_rx(s);
                    if s.phase == Phase::WaitMetadata
                        && dev_millis(sys).wrapping_sub(s.state_start_ms) > CONNECT_TIMEOUT_MS
                    {
                        log_err(s, b"[kafkasink] metadata timeout");
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
                    sweep_stale(s);
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
