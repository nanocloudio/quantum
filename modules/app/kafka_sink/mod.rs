// kafka_sink — Kafka producer, a PROVIDER of the exchange contract.
// See manifest.toml for the delivery terms it declares.
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
// mention) and reads back the real partition count, which the
// ordering-key hash then addresses. Running reads `request_in` while
// the produce window has room; each Kafka correlation id maps to its
// exchange in the window. A connection lost after Metadata writes LINK
// DOWN and clears the window — those exchanges are exactly the set the
// requester re-issues after the LINK UP the next Metadata writes. The
// exchange mechanics live in `modules/common/publish_exchange.rs`.

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
use publish_exchange::exchange::{status, KEY_MAX, PAYLOAD_MAX, RECORD_MAX};
use publish_exchange::PublishProvider;

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

// The dial: the broker's authority travels on the record, and the
// provider resolves a name.
use abi::contracts::net::net_proto::{
    connected_parts, error_parts, CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CONNECT_TO_MAX,
    CONN_ID_LEN, REQUESTER_TAG_NONE,
};

#[path = "../../common/authority.rs"]
mod authority;
use authority::{Authority, PORT_QUANTUM_BROKER};

// ── Kafka constants ──────────────────────────────────────────────────

const API_PRODUCE: i16 = 0;
const API_METADATA: i16 = 3;
const PRODUCE_VERSION: i16 = 7;
const METADATA_VERSION: i16 = 1;
/// Kafka error code the broker uses for an over-budget records blob.
const ERR_MESSAGE_TOO_LARGE: i16 = 10;

// ── Sizing ───────────────────────────────────────────────────────────

/// The largest record value: the contract's collected-body ceiling.
const MAX_PAYLOAD: usize = PAYLOAD_MAX;
/// The largest record key: the contract's ordering-key ceiling.
const MAX_KEY_LEN: usize = KEY_MAX;
/// RecordBatch framing around one record's key and value.
const RECORD_OVERHEAD: usize = 96;
/// One RecordBatch holding the largest key and value. The broker's
/// `message.max.bytes` must be at least this; the `max_payload`
/// capability fact is what tells a requester the value ceiling.
const MAX_RECORDS_BYTES: usize = MAX_KEY_LEN + MAX_PAYLOAD + RECORD_OVERHEAD;
/// Produce window: requests sent, response not yet seen.
const INFLIGHT_CAP: usize = 8;
/// Partition ceiling (the broker clamps its `partitions` param 1..=16).
const MAX_PARTS: usize = 16;
/// Requests collected at once while their bodies arrive.
const REQUEST_SLOTS: usize = 2;
/// Records owed on `response_out`: one per window slot, one per read,
/// and the LINK pair.
const OWED_CAP: usize = INFLIGHT_CAP + 3;

/// One Produce request naming one partition with the largest batch,
/// plus the request header. A BROADCAST names every partition with the
/// same batch, so its request grows by a batch per partition; one that
/// does not fit is refused with 413, never truncated.
const TX_BUF_SIZE: usize = MAX_RECORDS_BYTES + 4096;
const RX_BUF_SIZE: usize = 8192;
const NET_BUF_SIZE: usize = 1600;
const MAX_TOPIC_LEN: usize = 64;

type Desk = PublishProvider<REQUEST_SLOTS, PAYLOAD_MAX, INFLIGHT_CAP, OWED_CAP>;

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
    request_in_chan: i32,
    response_out_chan: i32,

    authority: Authority,
    topic_len: u8,
    conn_id: u16,
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

    topic: [u8; MAX_TOPIC_LEN],

    /// Requests being collected, produces awaiting their response (keyed
    /// by Kafka correlation id), and the records owed on `response_out`.
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
    use super::ptr_copy;
    use super::SinkState;
    use super::MAX_TOPIC_LEN;
    use super::SCHEMA_MAX;

    define_params! {
        SinkState;

        // Tags 1 and 2 are retired.
        // The broker, `host[:port]`; the port defaults to 9090.
        5, authority, str, 0
            => |s, d, len| {
                if len > 0 {
                    s.authority.set(core::slice::from_raw_parts(d, len));
                }
            };

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
/// one; each completed PUBLISH goes to the broker as one Produce.
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
        let broadcast = s.desk.broadcast(at);
        let Some(r) = s.desk.request(at) else {
            continue;
        };
        // The ordering key is the Kafka record key: it selects the
        // partition, so same-key records land on one partition in
        // request order. The collector bounds it at KEY_MAX and the body
        // at PAYLOAD_MAX, which are this module's own ceilings.
        let msg_key = r.target;
        let payload = r.body;

        // Partition addressing: key hash normally; EVERY partition in
        // one request for BROADCAST — the broker acks that request only
        // after the slowest partition is durable.
        let nparts = s.partitions.clamp(1, MAX_PARTS as u32);
        let mut parts = [0u32; MAX_PARTS];
        let parts_used: usize = if broadcast {
            let mut i = 0usize;
            while i < nparts as usize {
                parts[i] = i as u32;
                i += 1;
            }
            nparts as usize
        } else {
            // `nparts` is at least 1; `checked_rem` says so without a
            // division-by-zero path in the image.
            parts[0] = fnv1a64(msg_key).checked_rem(nparts as u64).unwrap_or(0) as u32;
            1
        };

        let now = millis(s) as i64;
        let mut body = [0u8; TX_BUF_SIZE];
        let produced = produce_body(
            &s.topic[..s.topic_len as usize],
            &parts[..parts_used],
            msg_key,
            payload,
            now,
            &mut body,
        );
        let Some(blen) = produced else {
            s.desk.refuse(at, status::TOO_LARGE);
            flush_answers(s);
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
            s.desk.refuse(at, status::TOO_LARGE);
            flush_answers(s);
            continue;
        };
        // Into the window BEFORE the send.
        if !s.desk.dispatch(at, s.kcorr as u64, millis(s)) {
            return; // window full (ruled out by can_take)
        }
        s.tx_buf[..rlen].copy_from_slice(&req[..rlen]);
        let _ = start_send(s, rlen);
        // Serialize sends: broker order = request order.
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
                s.desk.link_up();
            } else {
                log_err(s, b"[kafkasink] metadata unparseable");
                enter_reconnect(s);
                return;
            }
            continue;
        }

        // Produce response: the correlation id names the exchange. Every
        // partition durable is 200; the broker's over-budget refusal is
        // 413; any other partition error, or a response that does not
        // parse, means the broker would not take the record: 502.
        let verdict = match produce_errors(body) {
            Some(0) => status::OK,
            Some(ERR_MESSAGE_TOO_LARGE) => status::TOO_LARGE,
            _ => status::BAD_GATEWAY,
        };
        if corr > 0 {
            let _ = s.desk.settle(corr as u64, verdict);
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
        && payload_len >= 2
        && u16::from_le_bytes([*nbuf.add(NET_FRAME_HDR), *nbuf.add(NET_FRAME_HDR + 1)]) != s.conn_id
    {
        return;
    }
    if msg_type == NET_MSG_CLOSED || msg_type == NET_MSG_ERROR {
        log_err(s, b"[kafkasink] connection lost");
        enter_reconnect(s);
        return;
    }
    if msg_type == NET_MSG_DATA && payload_len > 2 {
        if full > payload_len {
            log_err(s, b"[kafkasink] truncated segment");
            enter_reconnect(s);
            return;
        }
        let data_len = payload_len - 2;
        process_responses(s);
        let space = RX_BUF_SIZE - s.rx_have as usize;
        if data_len > space {
            log_err(s, b"[kafkasink] oversized response");
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
        process_responses(s);
    }
}

/// A produce that never got a response past the reply timeout means
/// the stream is wedged — reconnect (LINK DOWN invalidates the window,
/// and the requester re-issues it).
unsafe fn sweep_stale(s: &mut SinkState) {
    let now = millis(s);
    if let Some(sent) = s.desk.oldest_sent_ms() {
        if now.wrapping_sub(sent) >= REPLY_TIMEOUT_MS {
            log_err(s, b"[kafkasink] produce timeout");
            enter_reconnect(s);
        }
    }
}

unsafe fn enter_reconnect(s: &mut SinkState) {
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
        s.request_in_chan = dev_channel_port(sys, 0, 1);
        s.response_out_chan = dev_channel_port(sys, 1, 1);
        s.desk.reset();
        s.outbox = ExchangeOutbox::new();
        params_def::parse_tlv(s, params, params_len);
        s.phase = Phase::Init;
        s.kcorr = 0;
        s.partitions = 1;
        s.backoff_ms = BACKOFF_INIT_MS;
        log_msg(s, b"[kafkasink] init");
        if !s.authority.adopt(PORT_QUANTUM_BROKER) {
            log_err(
                s,
                b"[kafkasink] refusing to construct: authority (host[:port]) is required",
            );
            return -10;
        }
        if s.topic_len == 0 {
            log_err(s, b"[kafkasink] missing topic");
            return -10;
        }
        if s.request_in_chan < 0 || s.response_out_chan < 0 {
            log_err(
                s,
                b"[kafkasink] refusing to construct: request_in and response_out must both be wired",
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
                    let mut payload = [0u8; CONNECT_TO_MAX];
                    let n = s
                        .authority
                        .connect_record(&mut payload, Some(dev_requester_tag(sys)));
                    if n == 0 {
                        log_err(s, b"[kafkasink] authority does not fit a connect record");
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
                                s.phase = Phase::Metadata;
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
                    handle_requests(s);
                    flush_answers(s);
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
