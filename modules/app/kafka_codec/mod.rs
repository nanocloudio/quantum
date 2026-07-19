//! Kafka Codec — Kafka binary protocol parser/framer.
//!
//! All envelopes use `[conn_id: u8][bytes...]` to match peer_router format.
//! Direct ApiVersions response is generated here (no session_processor involvement).
//!
//! Kafka request: [size: i32 BE][api_key: i16 BE][api_version: i16 BE]
//!                [correlation_id: i32 BE][client_id: nullable string][body]
//! Kafka response: [size: i32 BE][correlation_id: i32 BE][body]

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

const MAX_REQ: usize = 4096;
const API_API_VERSIONS: i16 = 18;
const API_METADATA: i16 = 3;
const API_PRODUCE: i16 = 0;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_raw: i32,
    in_responses: i32,
    out_proposals: i32,
    out_frames: i32,

    requests_decoded: u32,
    responses_encoded: u32,
    parse_errors: u32,

    buf: [u8; MAX_REQ],
    envelope: [u8; MAX_REQ],
    frame: [u8; MAX_REQ],
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
        s.out_proposals = out_chan;
        s.in_responses = dev_channel_port(sys, 0, 1);
        s.out_frames = dev_channel_port(sys, 1, 1);
        dev_log(sys, 3, b"[kafka] init v2".as_ptr(), 15);
        0
    }
}

/// Emit a `[conn_id][bytes]` payload as a `MSG_CLIENT_FRAME` envelope
/// to response_mux. See `mqtt_codec::write_conn_frame` for the
/// rationale.
/// # Safety
unsafe fn write_conn(sys: &SyscallTable, chan: i32, conn_id: u8, bytes: &[u8]) -> bool {
    if chan < 0 { return false; }
    let total = 1 + bytes.len();
    if total > MAX_REQ { return false; }
    let mut out = [0u8; MAX_REQ];
    out[0] = conn_id;
    out[1..total].copy_from_slice(bytes);
    let w = wire::channel_write_msg(sys, chan, wire::MSG_CLIENT_FRAME, &out[..total]);
    w > 0
}

/// Wire-envelope write to session_processor.codec_in. Format:
///   `[mtype:u8][len:u16 LE]` envelope, payload = `[conn_id][bytes]`.
/// Envelope framing demarcates messages when codecs fan in or bursts
/// queue multiple writes between consumer reads. See the matching
/// rationale on `mqtt_codec::write_conn_frame_with_mtype`.
///
/// # Safety
unsafe fn write_conn_mtype(sys: &SyscallTable, chan: i32, conn_id: u8, mtype: u8, bytes: &[u8]) -> bool {
    if chan < 0 { return false; }
    let total = 1 + bytes.len();
    if total > MAX_REQ { return false; }
    let mut out = [0u8; MAX_REQ];
    out[0] = conn_id;
    out[1..total].copy_from_slice(bytes);
    let w = wire::channel_write_msg(sys, chan, mtype, &out[..total]);
    w > 0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;

        // ── Decode inbound [conn_id][kafka request] ──
        for _ in 0..8 {
            let poll = (sys.channel_poll)(s.in_raw, 0x01);
            if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }

            // protocol_router frames each client record as a MSG_CLIENT_FRAME
            // envelope (payload `[conn_id][kafka request]`) so records for
            // distinct conn_ids don't coalesce on the byte FIFO.
            let (_mtype, plen) = wire::channel_read_msg(sys, s.in_raw, &mut s.buf);
            let n = plen as i32;
            if n < 13 { continue; } // conn_id + size(4) + apikey(2) + ver(2) + corr(4) = 13 min
            let conn_id = s.buf[0];

            // Kafka wire: [size:i32 BE][api_key:i16 BE][api_version:i16 BE][correlation_id:i32 BE]...
            let size = i32::from_be_bytes([s.buf[1], s.buf[2], s.buf[3], s.buf[4]]);
            if size < 4 || size as usize + 5 > n as usize { s.parse_errors += 1; continue; }
            let api_key = i16::from_be_bytes([s.buf[5], s.buf[6]]);
            let api_version = i16::from_be_bytes([s.buf[7], s.buf[8]]);
            let correlation_id = i32::from_be_bytes([s.buf[9], s.buf[10], s.buf[11], s.buf[12]]);

            // Direct ApiVersions response — no session_processor hop
            if api_key == API_API_VERSIONS {
                let mut resp = [0u8; 64];
                let mut p = 0usize;
                resp[p..p + 4].copy_from_slice(&correlation_id.to_be_bytes()); p += 4;
                resp[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
                resp[p..p + 4].copy_from_slice(&3i32.to_be_bytes()); p += 4;
                for &(k, mn, mx) in &[
                    (API_API_VERSIONS, 0i16, 3i16),
                    (API_METADATA, 0, 7),
                    (API_PRODUCE, 0, 8),
                ] {
                    resp[p..p + 2].copy_from_slice(&k.to_be_bytes()); p += 2;
                    resp[p..p + 2].copy_from_slice(&mn.to_be_bytes()); p += 2;
                    resp[p..p + 2].copy_from_slice(&mx.to_be_bytes()); p += 2;
                }
                resp[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;

                // Prepend size header
                let mut framed = [0u8; 64];
                framed[0..4].copy_from_slice(&(p as i32).to_be_bytes());
                framed[4..4 + p].copy_from_slice(&resp[..p]);

                write_conn(sys, s.out_frames, conn_id, &framed[..4 + p]);
                s.responses_encoded += 1;
                continue;
            }

            // Build envelope for session_processor:
            //   [proto=1 Kafka][api_key_lo,hi LE][api_ver_lo,hi LE][corr_id LE][body...]
            let body_start = 13usize;
            let body_len = n as usize - body_start;
            let mut p = 0usize;
            s.envelope[p] = 1; p += 1;  // PROTO_KAFKA
            s.envelope[p..p + 2].copy_from_slice(&api_key.to_le_bytes()); p += 2;
            s.envelope[p..p + 2].copy_from_slice(&api_version.to_le_bytes()); p += 2;
            s.envelope[p..p + 4].copy_from_slice(&correlation_id.to_le_bytes()); p += 4;
            if p + body_len > s.envelope.len() { continue; }
            if body_len > 0 {
                s.envelope[p..p + body_len].copy_from_slice(&s.buf[body_start..body_start + body_len]);
            }
            p += body_len;

            write_conn_mtype(sys, s.out_proposals, conn_id, wire::MSG_SESSION_PROPOSAL, &s.envelope[..p]);
            s.requests_decoded += 1;
        }

        // ── Encode responses ─────────────────────────────────────────
        // Wire envelope from session_processor; payload layout (post-strip):
        //   [conn_id:u8][proto:u8][api_key:i16 LE][api_ver:i16 LE]
        //   [corr_id:i32 LE][body...]
        if s.in_responses >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_responses, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }

                let (mtype, plen) = wire::channel_read_msg(sys, s.in_responses, &mut s.buf);
                if plen < 10 { continue; }
                if mtype != wire::MSG_SESSION_RESPONSE { continue; }
                let conn_id = s.buf[0];
                if s.buf[1] != 1 { continue; } // must be PROTO_KAFKA

                let corr = i32::from_le_bytes([s.buf[6], s.buf[7], s.buf[8], s.buf[9]]);
                let body = core::slice::from_raw_parts(s.buf.as_ptr().add(10), plen as usize - 10);

                // Kafka response: [size:i32 BE][corr_id:i32 BE][body]
                let body_total = 4 + body.len();
                let mut framed = [0u8; MAX_REQ];
                framed[0..4].copy_from_slice(&(body_total as i32).to_be_bytes());
                framed[4..8].copy_from_slice(&corr.to_be_bytes());
                framed[8..8 + body.len()].copy_from_slice(body);

                write_conn(sys, s.out_frames, conn_id, &framed[..8 + body.len()]);
                s.responses_encoded += 1;
            }
        }

        0
    }
}
