//! AMQP 0-9-1 Codec — frame parser/framer.
//!
//! All envelopes use `[conn_id: u8][bytes...]`. Per-conn content reassembly
//! tracks partial Basic.Publish content spanning method/header/body frames.
//!
//! AMQP frame: [type:u8][channel:u16 BE][size:u32 BE][payload][FRAME_END=0xCE]

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

const FRAME_METHOD: u8 = 1;
const FRAME_HEADER: u8 = 2;
const FRAME_BODY: u8 = 3;
const FRAME_END: u8 = 0xCE;

const MAX_FRAME: usize = 4096;
const MAX_CONNS: usize = 32;
const MAX_PENDING: usize = 8;

#[repr(C)]
#[derive(Clone, Copy)]
struct PendingContent {
    channel: u16,
    body_size: u64,
    received_bytes: u64,
    active: u8,
}

impl PendingContent {
    const fn zero() -> Self {
        Self { channel: 0, body_size: 0, received_bytes: 0, active: 0 }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ConnCtx {
    conn_id: u8,
    pending: [PendingContent; MAX_PENDING],
    active: u8,
}

impl ConnCtx {
    const fn zero() -> Self {
        Self { conn_id: 0, pending: [PendingContent::zero(); MAX_PENDING], active: 0 }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_raw: i32,
    in_responses: i32,
    out_proposals: i32,
    out_frames: i32,

    frames_decoded: u32,
    frames_encoded: u32,
    parse_errors: u32,
    contents_completed: u32,

    conns: [ConnCtx; MAX_CONNS],
    buf: [u8; MAX_FRAME],
    envelope: [u8; MAX_FRAME],
    frame: [u8; MAX_FRAME],
}

impl ModuleState {
    fn find_or_create(&mut self, conn_id: u8) -> usize {
        for i in 0..MAX_CONNS {
            if self.conns[i].active == 1 && self.conns[i].conn_id == conn_id { return i; }
        }
        for i in 0..MAX_CONNS {
            if self.conns[i].active == 0 {
                self.conns[i] = ConnCtx {
                    conn_id, pending: [PendingContent::zero(); MAX_PENDING], active: 1,
                };
                return i;
            }
        }
        0
    }
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
        for i in 0..MAX_CONNS { s.conns[i] = ConnCtx::zero(); }
        dev_log(sys, 3, b"[amqp] init v2".as_ptr(), 14);
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
    if total > MAX_FRAME { return false; }
    let mut out = [0u8; MAX_FRAME];
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
    if total > MAX_FRAME { return false; }
    let mut out = [0u8; MAX_FRAME];
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

        // ── Decode inbound frames ──
        for _ in 0..8 {
            let poll = (sys.channel_poll)(s.in_raw, 0x01);
            if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }

            // protocol_router frames each client record as a MSG_CLIENT_FRAME
            // envelope (payload `[conn_id][tcp bytes]`) so records for distinct
            // conn_ids don't coalesce on the byte FIFO.
            let (_mtype, plen) = wire::channel_read_msg(sys, s.in_raw, &mut s.buf);
            let n = plen as i32;
            // AMQP protocol header ("AMQP\x00\x00\x09\x01") is 8 bytes on first connect.
            // Minimum frame size is 8 bytes (type+channel+size+end).
            if n < 9 { continue; }
            let conn_id = s.buf[0];

            // Handle protocol header: respond with Connection.Start
            // directly from the codec (same pattern as kafka_codec's
            // direct ApiVersions response — no session_processor hop
            // for handshake frames).
            //
            // Connection.Start frame layout (AMQP 0-9-1 §1.2):
            //   [type=1][channel=0][size:u32 BE][payload][0xCE]
            // payload = [class_id=10 BE][method_id=10 BE]
            //         + version_major(1)=0 + version_minor(1)=9
            //         + server-properties (field-table; empty)
            //         + mechanisms (longstr)
            //         + locales (longstr)
            if n >= 9 && &s.buf[1..5] == b"AMQP" {
                // Build payload.
                let mechanisms = b"PLAIN";
                let locales = b"en_US";
                let mut pay = [0u8; 64];
                let mut pp = 0usize;
                // class+method
                pay[pp] = 0; pay[pp+1] = 10; pp += 2;       // class 10 = connection
                pay[pp] = 0; pay[pp+1] = 10; pp += 2;       // method 10 = start
                pay[pp] = 0; pp += 1;                       // version major
                pay[pp] = 9; pp += 1;                       // version minor
                // server-properties: empty field-table = u32 length 0
                pay[pp] = 0; pay[pp+1] = 0; pay[pp+2] = 0; pay[pp+3] = 0; pp += 4;
                // mechanisms longstr = u32 len + bytes
                let m_len = mechanisms.len() as u32;
                pay[pp..pp+4].copy_from_slice(&m_len.to_be_bytes()); pp += 4;
                pay[pp..pp+mechanisms.len()].copy_from_slice(mechanisms); pp += mechanisms.len();
                // locales longstr
                let l_len = locales.len() as u32;
                pay[pp..pp+4].copy_from_slice(&l_len.to_be_bytes()); pp += 4;
                pay[pp..pp+locales.len()].copy_from_slice(locales); pp += locales.len();
                let payload_len = pp;

                // Wrap in AMQP frame: type(1) + chan(2) + size(4) + payload + 0xCE
                let total_frame = 1 + 2 + 4 + payload_len + 1;
                if total_frame > s.frame.len() { continue; }
                s.frame[0] = FRAME_METHOD;
                s.frame[1] = 0; s.frame[2] = 0;                 // channel 0
                let size_be = (payload_len as u32).to_be_bytes();
                s.frame[3..7].copy_from_slice(&size_be);
                s.frame[7..7+payload_len].copy_from_slice(&pay[..payload_len]);
                s.frame[7+payload_len] = FRAME_END;

                // Emit to client via response_mux/peer_router.
                write_conn(sys, s.out_frames, conn_id, &s.frame[..total_frame]);
                s.frames_encoded += 1;
                s.frames_decoded += 1;
                continue;
            }

            let frame_type = s.buf[1];
            let channel = u16::from_be_bytes([s.buf[2], s.buf[3]]);
            let size = u32::from_be_bytes([s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);
            let payload_end = 8 + size as usize;
            if payload_end >= n as usize || s.buf[payload_end] != FRAME_END {
                s.parse_errors += 1;
                continue;
            }

            let body_ptr = s.buf.as_ptr().add(8);
            let body = core::slice::from_raw_parts(body_ptr, size as usize);

            let ci = s.find_or_create(conn_id);

            // Content reassembly tracking
            if frame_type == FRAME_HEADER && body.len() >= 12 {
                let body_size = u64::from_be_bytes([
                    body[4], body[5], body[6], body[7],
                    body[8], body[9], body[10], body[11],
                ]);
                for i in 0..MAX_PENDING {
                    if s.conns[ci].pending[i].active == 0 {
                        s.conns[ci].pending[i] = PendingContent {
                            channel, body_size, received_bytes: 0, active: 1,
                        };
                        break;
                    }
                }
            } else if frame_type == FRAME_BODY {
                for i in 0..MAX_PENDING {
                    if s.conns[ci].pending[i].active == 1 && s.conns[ci].pending[i].channel == channel {
                        s.conns[ci].pending[i].received_bytes =
                            s.conns[ci].pending[i].received_bytes.saturating_add(body.len() as u64);
                        if s.conns[ci].pending[i].received_bytes >= s.conns[ci].pending[i].body_size {
                            s.conns[ci].pending[i].active = 0;
                            s.contents_completed = s.contents_completed.wrapping_add(1);
                        }
                        break;
                    }
                }
            }

            // Method class/method ids for FRAME_METHOD
            let (class_id, method_id) = if frame_type == FRAME_METHOD && body.len() >= 4 {
                (
                    u16::from_be_bytes([body[0], body[1]]),
                    u16::from_be_bytes([body[2], body[3]]),
                )
            } else { (0u16, 0u16) };

            // Envelope: [proto=2 AMQP][frame_type][channel LE][class LE][method LE][body]
            let mut p = 0usize;
            s.envelope[p] = 2; p += 1;
            s.envelope[p] = frame_type; p += 1;
            s.envelope[p..p + 2].copy_from_slice(&channel.to_le_bytes()); p += 2;
            s.envelope[p..p + 2].copy_from_slice(&class_id.to_le_bytes()); p += 2;
            s.envelope[p..p + 2].copy_from_slice(&method_id.to_le_bytes()); p += 2;
            if p + body.len() > s.envelope.len() { continue; }
            s.envelope[p..p + body.len()].copy_from_slice(body);
            p += body.len();

            write_conn_mtype(sys, s.out_proposals, conn_id, wire::MSG_SESSION_PROPOSAL, &s.envelope[..p]);
            s.frames_decoded += 1;
        }

        // ── Encode responses ─────────────────────────────────────────
        // Wire envelope from session_processor; payload layout (post-strip):
        //   [conn_id:u8][proto:u8=2][frame_type:u8][channel:u16 LE]
        //   [class:u16 LE][method:u16 LE][body]
        if s.in_responses >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_responses, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }

                let (mtype, plen) = wire::channel_read_msg(sys, s.in_responses, &mut s.buf);
                if plen < 9 { continue; }
                if mtype != wire::MSG_SESSION_RESPONSE { continue; }
                let conn_id = s.buf[0];
                if s.buf[1] != 2 { continue; }

                let frame_type = s.buf[2];
                let channel = u16::from_le_bytes([s.buf[3], s.buf[4]]);
                let body = core::slice::from_raw_parts(s.buf.as_ptr().add(9), plen as usize - 9);

                let total = 7 + body.len() + 1;
                if total > s.frame.len() { continue; }
                s.frame[0] = frame_type;
                s.frame[1..3].copy_from_slice(&channel.to_be_bytes());
                s.frame[3..7].copy_from_slice(&(body.len() as u32).to_be_bytes());
                s.frame[7..7 + body.len()].copy_from_slice(body);
                s.frame[7 + body.len()] = FRAME_END;

                write_conn(sys, s.out_frames, conn_id, &s.frame[..total]);
                s.frames_encoded += 1;
            }
        }

        0
    }
}
