//! MQTT-over-QUIC adapter.
//!
//! Bridges fluxor's `quic` foundation module to quantum's `protocol`
//! without modifying either side. Lives between `quic.app_out` /
//! `quic.app_in` and `protocol.raw_in` / `protocol.frames_out`.
//!
//! Inbound (quic → protocol): strips fluxor's MSG_QUIC_STREAM_DATA
//! envelope (`[0x13][cid][sid:varint=0][fin][data…]`) and emits the
//! plain `[cid][data]` shape `protocol.raw_in` already accepts from the
//! TCP path. STREAM_OPEN, PEER_IDENTITY, and DATAGRAM
//! envelopes are observed but not propagated this revision — MQTT-over-
//! QUIC carries its control channel over a single bidi stream (id 0)
//! per the WG MQTT QUIC mapping draft, and the datagram path is reserved
//! for an optional unreliable QoS 0 fast-path that quantum doesn't
//! plumb yet.
//!
//! Outbound (protocol → quic): consumes envelope-framed
//! MSG_CLIENT_FRAME (0xEA) messages from `protocol.frames_out`,
//! payload shape `[cid][mqtt bytes]`, and re-emits as a raw
//! MSG_QUIC_STREAM_WRITE envelope (`[0x14][cid][sid=0][fin=0][data…]`)
//! to `quic.app_in`. The QUIC pump appends `data…` to the named
//! connection's send buffer and the next module_step flushes it as a
//! STREAM frame.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK is include!'d wholesale and each module consumes only a subset"
)]

use core::ffi::c_void;

#[allow(unused_imports, dead_code, reason = "see file-level allow")]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

// Multiplexed Session Surface v1 (fluxor SDK contracts/net/mux.rs).
// The quic foundation module speaks this contract on app_out/app_in:
// every message is a net-protocol frame `[msg_type:u8][len:u16 LE]
// [payload]`, and stream payloads are prefixed with
// `[session_id:u32 LE][stream_id:u32 LE]`. In the QUIC v1 constrained
// profile the session_id IS the provider's connection index (small)
// and the single application stream is the client-initiated bidi
// stream 0. Constants duplicated here to avoid cross-repo header
// sharing; they're part of the foundation module's public contract.
const MSG_MUX_STREAM_ACCEPTED: u8 = 0xC3; // [session][stream][flags]
const MSG_MUX_STREAM_RX: u8 = 0xC5; // [session][stream][data]
const MSG_MUX_STREAM_CLOSED: u8 = 0xC4;
const MSG_MUX_DATAGRAM_RX: u8 = 0xC7;
const MSG_MUX_PEER_IDENTITY: u8 = 0xC8;
const CMD_MUX_STREAM_SEND: u8 = 0xB4; // [session][stream][data]
const SESSION_ID_BYTES: usize = 4;
const STREAM_DATA_PREFIX: usize = 8; // session_id(4) + stream_id(4)

/// Largest MQTT control packet we will forward in either direction.
/// Matches the mqtt codec's `MAX_PACKET` so the adapter cannot become the
/// gating clamp.
const MAX_PACKET: usize = 4096;

/// Scratch for an inbound mux frame: 3-byte net-frame header
/// (msg_type + len:u16) + 8-byte stream prefix (session + stream) +
/// MAX_PACKET of data.
const QUIC_RX_BUF: usize = NET_FRAME_HDR + STREAM_DATA_PREFIX + MAX_PACKET;
/// Scratch for an outbound CMD_MUX_STREAM_SEND frame, same shape.
const QUIC_TX_BUF: usize = NET_FRAME_HDR + STREAM_DATA_PREFIX + MAX_PACKET;
/// Scratch for the codec→adapter response path. Envelope payload is
/// `[cid][mqtt bytes]`; one extra byte beyond MAX_PACKET covers the
/// conn_id prefix.
const FRAME_BUF: usize = 1 + MAX_PACKET;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    quic_in: i32,
    frames_in: i32,
    mqtt_out: i32,
    quic_out: i32,

    stream_frames_in: u32,
    stream_frames_out: u32,
    dropped: u32,
    parse_errors: u32,

    rx_buf: [u8; QUIC_RX_BUF],
    tx_buf: [u8; QUIC_TX_BUF],
    frame_buf: [u8; FRAME_BUF],
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

/// PIC module ABI entry: one-time process-wide init, before any instance
/// exists.
///
/// # Safety
/// `syscalls` is a kernel-owned table whose function pointers reach live
/// kernel routines for the lifetime of the process.
#[no_mangle]
#[link_section = ".text.module_init"]
pub unsafe extern "C" fn module_init(_syscalls: *const c_void) {}

/// PIC module ABI entry: construct module state in `state` (kernel-allocated
/// from the manifest-declared `state_size`).
///
/// # Safety
/// `state` / `params` / `syscalls` are kernel-owned buffers passed across the
/// module ABI. The kernel guarantees `state` is at least `state_size` bytes,
/// `params` is at least `params_len` bytes, and `state` is zero-initialised.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<ModuleState>() {
            return -2;
        }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.quic_in = in_chan;
        s.mqtt_out = out_chan;
        s.frames_in = dev_channel_port(sys, 0, 1);
        s.quic_out = dev_channel_port(sys, 1, 1);
        s.stream_frames_in = 0;
        s.stream_frames_out = 0;
        s.dropped = 0;
        s.parse_errors = 0;
        dev_log(sys, 3, b"[mqtt_quic] init".as_ptr(), 16);
        0
    }
}

/// PIC module ABI entry: run one scheduler step against this instance.
///
/// # Safety
/// `state` is the kernel-owned buffer a prior `module_new` initialised, and is
/// exclusively borrowed for the duration of the call.
#[no_mangle]
#[link_section = ".text.module_step"]
pub unsafe extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;

        // ── Inbound: quic.app_out → protocol.raw_in ───────────────
        //
        // quic emits mux frames via net_write_frame, i.e. one
        // `[msg_type:u8][len:u16 LE][payload]` frame per message. Read
        // exactly one frame per iteration (net_read_frame) so back-to-back
        // mux messages on the byte FIFO don't coalesce. The MQTT control
        // bytes arrive as MSG_MUX_STREAM_RX with payload
        // `[session_id:4][stream_id:4][data]`.
        for _ in 0..8 {
            let poll = (sys.channel_poll)(s.quic_in, 0x01);
            if poll <= 0 || (poll as u32 & 0x01) == 0 {
                break;
            }
            let (mtype, plen) =
                net_read_frame(sys, s.quic_in, s.rx_buf.as_mut_ptr(), s.rx_buf.len());
            if mtype == 0 && plen == 0 {
                break;
            }
            let pl = plen;
            if mtype != MSG_MUX_STREAM_RX {
                // STREAM_ACCEPTED / PEER_IDENTITY / STREAM_CLOSED / DATAGRAM_RX
                // are observed but not propagated: the mqtt codec auto-creates a
                // conn slot on first byte, identity is a future mTLS hook, and
                // the MQTT control channel rides the bidi stream, not datagrams.
                continue;
            }
            if pl < STREAM_DATA_PREFIX {
                s.parse_errors += 1;
                continue;
            }
            let base = NET_FRAME_HDR;
            // session_id (provider connection index) → mqtt conn_id; the
            // constrained profile keeps it small (< MAX_CONNS).
            let cid = s.rx_buf[base];
            // Only stream 0 (the MQTT control stream) carries packets.
            let stream0 = s.rx_buf[base + SESSION_ID_BYTES] == 0
                && s.rx_buf[base + SESSION_ID_BYTES + 1] == 0
                && s.rx_buf[base + SESSION_ID_BYTES + 2] == 0
                && s.rx_buf[base + SESSION_ID_BYTES + 3] == 0;
            if !stream0 {
                s.dropped += 1;
                continue;
            }
            let data_off = base + STREAM_DATA_PREFIX;
            let data_len = pl - STREAM_DATA_PREFIX;
            if data_len == 0 {
                continue;
            }
            let total = 1 + data_len;
            if total > s.frame_buf.len() {
                s.dropped += 1;
                continue;
            }
            s.frame_buf[0] = cid;
            core::ptr::copy_nonoverlapping(
                s.rx_buf.as_ptr().add(data_off),
                s.frame_buf.as_mut_ptr().add(1),
                data_len,
            );
            // protocol.raw_in is length-delimited (MSG_CLIENT_FRAME
            // envelope) so per-conn records don't coalesce on the byte FIFO —
            // same framing the router uses on the TCP path.
            let w = wire::channel_write_msg(
                sys,
                s.mqtt_out,
                wire::MSG_CLIENT_FRAME,
                &s.frame_buf[..total],
            );
            if w > 0 {
                s.stream_frames_in += 1;
            } else {
                s.dropped += 1;
            }
        }

        // ── Outbound: protocol.frames_out → quic.app_in ───────────
        //
        // frames_out emits envelope-framed MSG_CLIENT_FRAME (0xEA) with payload
        // `[cid][mqtt bytes]`. Re-emit as a CMD_MUX_STREAM_SEND net-frame:
        // payload `[session_id:4 LE][stream_id:4 LE][data]`, where session_id
        // is the provider connection index (== cid) and stream_id is 0. quic
        // reads app_in with net_read_frame_aligned, so the net_write_frame
        // header is exactly what it expects.
        if s.frames_in >= 0 && s.quic_out >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.frames_in, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mtype, plen) = wire::channel_read_msg(sys, s.frames_in, &mut s.frame_buf);
                if plen < 1 {
                    continue;
                }
                if mtype != wire::MSG_CLIENT_FRAME {
                    s.dropped += 1;
                    continue;
                }
                let plen = plen as usize;
                let cid = s.frame_buf[0];
                let data_len = plen - 1;
                let payload_len = STREAM_DATA_PREFIX + data_len;
                if NET_FRAME_HDR + payload_len > s.tx_buf.len() {
                    s.dropped += 1;
                    continue;
                }
                // payload built at tx_buf[NET_FRAME_HDR..]; net_write_frame
                // prepends the [msg_type][len] header in place via scratch.
                let mut payload = [0u8; STREAM_DATA_PREFIX + MAX_PACKET];
                payload[0..4].copy_from_slice(&(cid as u32).to_le_bytes());
                // stream_id 0 (payload[4..8] already zero).
                if data_len > 0 {
                    core::ptr::copy_nonoverlapping(
                        s.frame_buf.as_ptr().add(1),
                        payload.as_mut_ptr().add(STREAM_DATA_PREFIX),
                        data_len,
                    );
                }
                let w = net_write_frame(
                    sys,
                    s.quic_out,
                    CMD_MUX_STREAM_SEND,
                    payload.as_ptr(),
                    payload_len,
                    s.tx_buf.as_mut_ptr(),
                    s.tx_buf.len(),
                );
                if w > 0 {
                    s.stream_frames_out += 1;
                } else {
                    s.dropped += 1;
                }
            }
        }

        0
    }
}
