//! Protocol Router — Per-connection protocol classifier.
//!
//! Single authority for classifying inbound client traffic. Reads raw
//! `[conn_id: u8][tcp bytes...]` envelopes from peer_router.cleartext,
//! sniffs the protocol from the first bytes seen on a given conn_id,
//! and forwards the bytes to the matching codec or http_surface.
//!
//! Classification rules (first-seen per conn_id, sticky thereafter):
//!
//!   MQTT:   byte[0] == 0x10 and byte[2..6] == "MQTT"  (CONNECT signature)
//!           — also match bare 0x10 for MQTT 3.1 fallback
//!   Kafka:  byte[0..4] == big-endian u32 (request size) > 0,
//!           and bytes[4..6] == valid api_key i16 BE
//!   AMQP:   byte[0..8] == b"AMQP\x00\x00\x09\x01"  (protocol header)
//!   HTTP:   starts with "GET ", "POST ", "PUT ", "HEAD", "DELE", "PATC",
//!           "OPTI", "TRAC", "CONN" (HTTP methods)
//!   else:   unclassified → drop
//!
//! Response path is NOT through this module — codecs and http_surface
//! write directly to peer_router.client_resp with the same conn_id
//! envelope.
//!
//! Ports:
//!   in[0]  raw_in   — `[conn_id][bytes]` from peer_router.cleartext
//!   out[0] mqtt_out — `[conn_id][bytes]` to mqtt_codec.raw_in
//!   out[1] kafka_out — `[conn_id][bytes]` to kafka_codec.raw_in
//!   out[2] amqp_out — `[conn_id][bytes]` to amqp_codec.raw_in
//!   out[3] http_out — `[conn_id][bytes]` to http_surface.requests

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK is include!'d wholesale and each module consumes only a subset; pending upstream allow attributes in target/fluxor/fluxor-abi/sdk/"
)]

use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "see file-level allow: SDK surface is shared across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

const MAX_CONNS: usize = 64;
const READ_BUF: usize = 1024;

const PROTO_UNKNOWN: u8 = 0;
const PROTO_MQTT: u8 = 1;
const PROTO_KAFKA: u8 = 2;
const PROTO_AMQP: u8 = 3;
const PROTO_HTTP: u8 = 4;
const PROTO_UNSUPPORTED: u8 = 0xFF;

#[repr(C)]
#[derive(Clone, Copy)]
struct ConnState {
    conn_id: u8,
    protocol: u8,
    sniff_buf_len: u8,
    sniff_buf: [u8; 16],
    active: u8,
}

impl ConnState {
    const fn zero() -> Self {
        Self {
            conn_id: 0, protocol: PROTO_UNKNOWN,
            sniff_buf_len: 0, sniff_buf: [0; 16],
            active: 0,
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_raw: i32,
    out_mqtt: i32,
    out_kafka: i32,
    out_amqp: i32,
    out_http: i32,

    conns: [ConnState; MAX_CONNS],

    // Params / stats
    default_protocol: u8,
    strict_classification: u8,
    classified_mqtt: u32,
    classified_kafka: u32,
    classified_amqp: u32,
    classified_http: u32,
    unclassified: u32,

    buf: [u8; READ_BUF],
}

impl ModuleState {
    fn find_or_create(&mut self, conn_id: u8) -> usize {
        for i in 0..MAX_CONNS {
            if self.conns[i].active == 1 && self.conns[i].conn_id == conn_id {
                return i;
            }
        }
        for i in 0..MAX_CONNS {
            if self.conns[i].active == 0 {
                self.conns[i] = ConnState {
                    conn_id, protocol: PROTO_UNKNOWN,
                    sniff_buf_len: 0, sniff_buf: [0; 16],
                    active: 1,
                };
                return i;
            }
        }
        // Table full: reuse slot 0 (LRU would be nicer)
        self.conns[0] = ConnState {
            conn_id, protocol: PROTO_UNKNOWN,
            sniff_buf_len: 0, sniff_buf: [0; 16],
            active: 1,
        };
        0
    }
}

/// First-byte protocol classification. Returns (protocol, bytes_consumed).
/// `bytes_consumed` indicates how much of the sniff buffer was used for
/// classification (meaningful only for UNSUPPORTED / deep checks).
fn classify(bytes: &[u8]) -> u8 {
    if bytes.is_empty() { return PROTO_UNKNOWN; }

    // MQTT: CONNECT packet type nibble = 0x1, flags nibble = 0x0.
    // Full signature check: [0x10][varint len][0x00 0x04 'M' 'Q' 'T' 'T'] for MQTT 3.1.1/5
    // or [0x10][varint len][0x00 0x06 'M' 'Q' 'I' 's' 'd' 'p'] for MQTT 3.1.
    // Accept bare 0x10 + varint if protocol name not yet arrived (partial packet).
    if bytes[0] == 0x10 {
        if bytes.len() >= 8
            && bytes[2] == 0x00
            && bytes[3] == 0x04
            && &bytes[4..8] == b"MQTT"
        {
            return PROTO_MQTT;
        }
        if bytes.len() >= 10
            && bytes[2] == 0x00
            && bytes[3] == 0x06
            && &bytes[4..10] == b"MQIsdp"
        {
            return PROTO_MQTT;
        }
        // First byte matches CONNECT; let it through as MQTT even if we
        // haven't seen the protocol name yet — subsequent bytes will be
        // validated by mqtt_codec itself.
        if bytes.len() < 8 {
            return PROTO_MQTT;
        }
    }

    // AMQP: protocol header is exactly "AMQP\x00\x00\x09\x01"
    if bytes.len() >= 8 && &bytes[..4] == b"AMQP" {
        return PROTO_AMQP;
    }

    // HTTP: starts with method token
    if bytes.len() >= 4 {
        let method = &bytes[..4];
        if method == b"GET "
            || method == b"POST"
            || method == b"PUT "
            || method == b"HEAD"
            || method == b"DELE"
            || method == b"OPTI"
            || method == b"PATC"
            || method == b"TRAC"
            || method == b"CONN"
        {
            return PROTO_HTTP;
        }
    }

    // Kafka: first 4 bytes are request size (big-endian i32), must be positive
    // and < 16 MB. Bytes [4..6] are api_key (i16 BE, 0..=67).
    if bytes.len() >= 6 {
        let size = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let api_key = i16::from_be_bytes([bytes[4], bytes[5]]);
        if size > 0 && size < 16 * 1024 * 1024 && api_key >= 0 && api_key <= 67 {
            return PROTO_KAFKA;
        }
    }

    PROTO_UNKNOWN
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 { core::mem::size_of::<ModuleState>() as u32 }

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32, out_chan: i32, _ctrl_chan: i32,
    _params: *const u8, _params_len: usize,
    state: *mut u8, state_size: usize, syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() { return -1; }
        if state_size < core::mem::size_of::<ModuleState>() { return -2; }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_raw = in_chan;
        s.out_mqtt = out_chan;
        s.out_kafka = dev_channel_port(sys, 1, 1);
        s.out_amqp = dev_channel_port(sys, 1, 2);
        s.out_http = dev_channel_port(sys, 1, 3);
        s.default_protocol = PROTO_MQTT;
        s.strict_classification = 0;  // if 1, unclassified → dropped; else → default
        for i in 0..MAX_CONNS { s.conns[i] = ConnState::zero(); }
        dev_log(sys, 3, b"[prot] init v3".as_ptr(), 14);
        0
    }
}

/// Forward `[conn_id][data]` to the named output channel as a single
/// length-delimited `MSG_CLIENT_FRAME` envelope.
///
/// Client channels carry `[conn_id][raw tcp bytes]` records multiplexed
/// across many connections. On a byte-FIFO channel, back-to-back raw
/// writes for different conn_ids coalesce into one downstream read, and
/// the second conn_id byte is misread as stream data — silently dropping
/// that connection's traffic. Wrapping each record in the wire envelope
/// preserves the per-record boundary so the consumer can demarcate them
/// with `channel_read_msg`. This mirrors the already-framed response path
/// (codec.frames_out → peer_router.client_resp).
///
/// # Safety
/// `sys` and `chan` must be valid.
unsafe fn forward_raw(sys: &SyscallTable, chan: i32, conn_id: u8, data: &[u8]) -> bool {
    if chan < 0 || data.is_empty() { return false; }
    let total = 1 + data.len();
    if total > 1024 { return false; }
    let mut out = [0u8; 1024];
    out[0] = conn_id;
    out[1..total].copy_from_slice(data);
    wire::channel_write_msg(sys, chan, wire::MSG_CLIENT_FRAME, &out[..total]) > 0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;

        // peer_router writes each `[conn_id][data...]` client record as a
        // length-delimited MSG_CLIENT_FRAME envelope. Read one envelope per
        // iteration so records from different conn_ids never coalesce on the
        // byte FIFO (see forward_raw for the failure mode).
        for _ in 0..16 {
            let poll = (sys.channel_poll)(s.in_raw, 0x01);
            if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }

            let (_mtype, plen) = wire::channel_read_msg(sys, s.in_raw, &mut s.buf);
            let n = plen as usize;
            if n <= 1 { continue; } // need at least conn_id + 1 byte

            let conn_id = s.buf[0];
            let data_len = n - 1;
            // Take a temporary copy pointer for the data slice
            let data_ptr = s.buf.as_ptr().add(1);
            let data = core::slice::from_raw_parts(data_ptr, data_len);

            let idx = s.find_or_create(conn_id);

            // On first bytes for this conn, classify from up to 16 bytes.
            // Whether classification succeeds or we fall back to default,
            // we forward the FULL incoming `data` slice — the sniff buffer
            // is only used for the classifier's inspection.
            if s.conns[idx].protocol == PROTO_UNKNOWN {
                // Accumulate into sniff buffer for classification
                let sniff_have = s.conns[idx].sniff_buf_len as usize;
                let take = core::cmp::min(16 - sniff_have, data_len);
                for i in 0..take {
                    s.conns[idx].sniff_buf[sniff_have + i] = data[i];
                }
                s.conns[idx].sniff_buf_len = (sniff_have + take) as u8;

                let sniff_len = s.conns[idx].sniff_buf_len as usize;
                let sniff_ptr = s.conns[idx].sniff_buf.as_ptr();
                let sniff = core::slice::from_raw_parts(sniff_ptr, sniff_len);
                let proto = classify(sniff);

                if proto != PROTO_UNKNOWN {
                    s.conns[idx].protocol = proto;
                } else if sniff_len >= 16 {
                    // 16 bytes and still can't classify
                    if s.strict_classification == 0 {
                        s.conns[idx].protocol = s.default_protocol;
                    } else {
                        s.conns[idx].protocol = PROTO_UNSUPPORTED;
                        s.unclassified = s.unclassified.wrapping_add(1);
                        continue;
                    }
                } else {
                    // Need more bytes to decide; can't forward yet
                    continue;
                }

                match s.conns[idx].protocol {
                    PROTO_MQTT => s.classified_mqtt = s.classified_mqtt.wrapping_add(1),
                    PROTO_KAFKA => s.classified_kafka = s.classified_kafka.wrapping_add(1),
                    PROTO_AMQP => s.classified_amqp = s.classified_amqp.wrapping_add(1),
                    PROTO_HTTP => s.classified_http = s.classified_http.wrapping_add(1),
                    _ => {}
                }
                // fall through to the per-connection forwarder below
            }

            // Protocol already classified — forward to the matching output
            let target = match s.conns[idx].protocol {
                PROTO_MQTT => s.out_mqtt,
                PROTO_KAFKA => s.out_kafka,
                PROTO_AMQP => s.out_amqp,
                PROTO_HTTP => s.out_http,
                _ => -1,
            };
            if target >= 0 {
                forward_raw(sys, target, conn_id, data);
            }
        }

        0
    }
}
