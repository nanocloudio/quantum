//! MQTT-over-QUIC adapter.
//!
//! Bridges fluxor's `quic` foundation module to quantum's `protocol`
//! without modifying either side. Lives between `quic.app_out` /
//! `quic.app_in` and `protocol.raw_in` / `protocol.frames_out`.
//!
//! Inbound (quic → protocol): takes `MSG_MUX_STREAM_RX` on the session's
//! MQTT control stream and emits the plain `[cid:u16 LE][data]` shape
//! `protocol.raw_in` already accepts from the TCP path. PEER_IDENTITY and
//! DATAGRAM_RX are observed but not propagated — identity is a future
//! mTLS hook, and the datagram path is reserved for an optional
//! unreliable QoS 0 fast-path quantum doesn't plumb yet.
//!
//! Outbound (protocol → quic): consumes envelope-framed
//! MSG_CLIENT_FRAME (0xEA) messages from `protocol.frames_out`, payload
//! shape `[cid:u16 LE][mqtt bytes]`, and re-emits as `CMD_MUX_STREAM_SEND` on
//! that same stream. The QUIC engine appends `data…` to the stream's send
//! buffer and the next module_step flushes it as a STREAM frame.
//!
//! # Stream handles are learned, never assumed
//!
//! MQTT-over-QUIC carries its control channel over one bidirectional
//! stream per connection (the WG MQTT QUIC mapping draft), so there is
//! exactly one stream to track per session — but WHICH handle the
//! transport gives it is the transport's to decide. This adapter latches
//! it from the `MSG_MUX_STREAM_ACCEPTED` that announces the stream.
//!
//! The contract's `stream_id` is an opaque local handle, deliberately
//! not derivable from the QUIC wire id, so it cannot be computed and
//! must be read from the announcement. Guessing it is silent in both
//! directions: inbound packets are discarded as "wrong stream", and
//! outbound writes address a stream that does not exist. Neither
//! reports an error, because both are legal questions to ask about a
//! stream that is simply not there.

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
// profile the session_id IS the provider's connection index, and its
// low 16 bits are the MQTT conn_id; the stream_id is an opaque
// handle (see the module docs above). Constants duplicated here to
// avoid cross-repo header sharing; they're part of the foundation
// module's public contract.
const MSG_MUX_SESSION_OPENED: u8 = 0xC0; // [session][status][flags][alpn]
const MSG_MUX_SESSION_CLOSED: u8 = 0xC1; // [session][reason]
const MSG_MUX_STREAM_ACCEPTED: u8 = 0xC3; // [session][stream][flags][quic_id]
const MSG_MUX_STREAM_RX: u8 = 0xC5; // [session][stream][data]
const MSG_MUX_STREAM_CLOSED: u8 = 0xC4;
const MSG_MUX_STREAM_RESET: u8 = 0xCA;
const MSG_MUX_DATAGRAM_RX: u8 = 0xC7;
const MSG_MUX_PEER_IDENTITY: u8 = 0xC8;
const CMD_MUX_STREAM_SEND: u8 = 0xB4; // [session][stream][data]
const CMD_MUX_STREAM_ACK: u8 = 0xB5; // [session][stream][bytes]
const SESSION_ID_BYTES: usize = 4;
const STREAM_DATA_PREFIX: usize = 8; // session_id(4) + stream_id(4)
/// `MSG_MUX_STREAM_ACCEPTED` body after the prefix: `[flags][quic_id:8]`.
const STREAM_ACCEPTED_BODY: usize = 1 + 8;
/// Bit 1 of a stream's flags: unidirectional.
const STREAM_FLAG_UNI: u8 = 1 << 1;

/// Sessions tracked at once. Matches the QUIC engine's connection pool,
/// so a session the transport can carry always has somewhere to live.
const MAX_SESSIONS: usize = 4;

/// One QUIC session and the MQTT control stream on it.
#[derive(Clone, Copy)]
struct SessionSlot {
    live: bool,
    session_id: u32,
    /// The transport's opaque handle for the control stream, learned from
    /// `MSG_MUX_STREAM_ACCEPTED`. `have_stream` distinguishes "not yet
    /// announced" from a legitimately zero handle.
    stream: u32,
    have_stream: bool,
}

impl SessionSlot {
    const fn empty() -> Self {
        Self {
            live: false,
            session_id: 0,
            stream: 0,
            have_stream: false,
        }
    }
}

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
/// Bytes of conn-id prefix on a client frame (`wire::ConnId`, LE).
const CID: usize = 2;
/// Scratch for the codec→adapter response path. Envelope payload is
/// `[cid:u16 LE][mqtt bytes]`; the extra bytes beyond MAX_PACKET cover
/// the conn_id prefix.
const FRAME_BUF: usize = CID + MAX_PACKET;

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

    sessions: [SessionSlot; MAX_SESSIONS],

    rx_buf: [u8; QUIC_RX_BUF],
    tx_buf: [u8; QUIC_TX_BUF],
    frame_buf: [u8; FRAME_BUF],
}

/// Find a session by its transport id.
fn session_find(s: &ModuleState, session_id: u32) -> Option<usize> {
    let mut i = 0;
    while i < MAX_SESSIONS {
        if s.sessions[i].live && s.sessions[i].session_id == session_id {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Find or claim a slot for a session.
fn session_slot(s: &mut ModuleState, session_id: u32) -> Option<usize> {
    if let Some(i) = session_find(s, session_id) {
        return Some(i);
    }
    let mut i = 0;
    while i < MAX_SESSIONS {
        if !s.sessions[i].live {
            s.sessions[i] = SessionSlot::empty();
            s.sessions[i].live = true;
            s.sessions[i].session_id = session_id;
            return Some(i);
        }
        i += 1;
    }
    None
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
        s.sessions = [SessionSlot::empty(); MAX_SESSIONS];
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
            let base = NET_FRAME_HDR;
            if pl < SESSION_ID_BYTES {
                s.parse_errors += 1;
                continue;
            }
            let session = u32::from_le_bytes([
                s.rx_buf[base],
                s.rx_buf[base + 1],
                s.rx_buf[base + 2],
                s.rx_buf[base + 3],
            ]);

            // Session lifecycle. Tracked so the control stream's handle
            // has somewhere to be recorded, and so a closed session's
            // slot is returned rather than held for a connection that is
            // gone.
            if mtype == MSG_MUX_SESSION_OPENED {
                let _ = session_slot(s, session);
                continue;
            }
            if mtype == MSG_MUX_SESSION_CLOSED {
                if let Some(i) = session_find(s, session) {
                    s.sessions[i] = SessionSlot::empty();
                }
                continue;
            }
            if pl < STREAM_DATA_PREFIX {
                if mtype == MSG_MUX_PEER_IDENTITY || mtype == MSG_MUX_DATAGRAM_RX {
                    continue;
                }
                s.parse_errors += 1;
                continue;
            }
            let stream = u32::from_le_bytes([
                s.rx_buf[base + SESSION_ID_BYTES],
                s.rx_buf[base + SESSION_ID_BYTES + 1],
                s.rx_buf[base + SESSION_ID_BYTES + 2],
                s.rx_buf[base + SESSION_ID_BYTES + 3],
            ]);

            if mtype == MSG_MUX_STREAM_ACCEPTED {
                // Latch the control stream's handle. MQTT-over-QUIC uses
                // one bidirectional stream per connection; a
                // unidirectional one is not it, and taking its handle
                // would send every MQTT packet down a stream the peer
                // cannot answer on.
                if pl < STREAM_DATA_PREFIX + STREAM_ACCEPTED_BODY {
                    s.parse_errors += 1;
                    continue;
                }
                let flags = s.rx_buf[base + STREAM_DATA_PREFIX];
                if flags & STREAM_FLAG_UNI != 0 {
                    continue;
                }
                if let Some(i) = session_slot(s, session) {
                    if !s.sessions[i].have_stream {
                        s.sessions[i].stream = stream;
                        s.sessions[i].have_stream = true;
                        // One line per connection, naming the handle this
                        // adapter will address for the rest of the
                        // session. An operator debugging a silent session
                        // otherwise has no way to see whether the control
                        // stream was ever identified.
                        dev_log(sys, 3, b"[mqtt_quic] stream up".as_ptr(), 21);
                    }
                }
                continue;
            }
            if mtype == MSG_MUX_STREAM_CLOSED || mtype == MSG_MUX_STREAM_RESET {
                if let Some(i) = session_find(s, session) {
                    if s.sessions[i].have_stream && s.sessions[i].stream == stream {
                        s.sessions[i].have_stream = false;
                    }
                }
                continue;
            }
            if mtype != MSG_MUX_STREAM_RX {
                // PEER_IDENTITY / DATAGRAM_RX and anything else are
                // observed but not propagated.
                continue;
            }
            // Only the MQTT control stream carries packets. Which stream
            // that is was learned from its accepted event, never assumed.
            let known = match session_find(s, session) {
                Some(i) => s.sessions[i].have_stream && s.sessions[i].stream == stream,
                None => false,
            };
            if !known {
                s.dropped += 1;
                dev_log(sys, 2, b"[mqtt_quic] rx unknown".as_ptr(), 22);
                continue;
            }
            // session_id (provider connection index) → mqtt conn_id:
            // the low 16 bits, u16 LE on the frame.
            let cid = session as u16;
            let data_off = base + STREAM_DATA_PREFIX;
            let data_len = pl - STREAM_DATA_PREFIX;
            if data_len == 0 {
                continue;
            }
            let total = CID + data_len;
            if total > s.frame_buf.len() {
                s.dropped += 1;
                continue;
            }
            s.frame_buf[..CID].copy_from_slice(&cid.to_le_bytes());
            core::ptr::copy_nonoverlapping(
                s.rx_buf.as_ptr().add(data_off),
                s.frame_buf.as_mut_ptr().add(CID),
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
                // Return the flow-control credit for what we consumed.
                //
                // The transport advances MAX_STREAM_DATA and MAX_DATA from
                // these acknowledgements and from nothing else — it cannot
                // know this adapter has drained its buffer. A consumer that
                // never acknowledges runs until the initial window is
                // exhausted and then stalls with no error on either side,
                // which on a long-lived MQTT session means "it worked in
                // testing and stopped in production".
                let mut ack = [0u8; STREAM_DATA_PREFIX + 4];
                ack[0..4].copy_from_slice(&session.to_le_bytes());
                ack[4..8].copy_from_slice(&stream.to_le_bytes());
                ack[8..12].copy_from_slice(&(data_len as u32).to_le_bytes());
                let _ = net_write_frame(
                    sys,
                    s.quic_out,
                    CMD_MUX_STREAM_ACK,
                    ack.as_ptr(),
                    ack.len(),
                    s.tx_buf.as_mut_ptr(),
                    s.tx_buf.len(),
                );
            } else {
                // The codec would not take it. Logged rather than only
                // counted: a silently dropped inbound packet looks like a
                // transport fault from both ends, and this is the one
                // place that knows it was neither.
                s.dropped += 1;
                dev_log(sys, 2, b"[mqtt_quic] rx wr fail".as_ptr(), 22);
            }
        }

        // ── Outbound: protocol.frames_out → quic.app_in ───────────
        //
        // frames_out emits envelope-framed MSG_CLIENT_FRAME (0xEA) with payload
        // `[cid:u16 LE][mqtt bytes]`. Re-emit as a CMD_MUX_STREAM_SEND
        // net-frame: payload `[session_id:4 LE][stream_id:4 LE][data]`, where
        // session_id is the provider connection index (== cid widened) and
        // stream_id is the handle learned for that session. quic reads app_in
        // with net_read_frame_aligned, so the net_write_frame header is
        // exactly what it expects.
        if s.frames_in >= 0 && s.quic_out >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.frames_in, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mtype, plen) = wire::channel_read_msg(sys, s.frames_in, &mut s.frame_buf);
                let plen = plen as usize;
                if plen < CID {
                    continue;
                }
                if mtype != wire::MSG_CLIENT_FRAME {
                    s.dropped += 1;
                    continue;
                }
                let cid = u16::from_le_bytes([s.frame_buf[0], s.frame_buf[1]]);
                let data_len = plen - CID;
                let payload_len = STREAM_DATA_PREFIX + data_len;
                if NET_FRAME_HDR + payload_len > s.tx_buf.len() {
                    s.dropped += 1;
                    continue;
                }
                // The codec names the connection; the adapter turns that
                // into the session and the stream the transport actually
                // gave us. Without the lookup this addressed handle 0,
                // which is never a live stream — the write is refused and
                // the client waits for a reply that was never sent.
                let session = cid as u32;
                let stream = match session_find(s, session) {
                    Some(i) if s.sessions[i].have_stream => s.sessions[i].stream,
                    _ => {
                        s.dropped += 1;
                        dev_log(sys, 2, b"[mqtt_quic] no stream".as_ptr(), 21);
                        continue;
                    }
                };
                // payload built at tx_buf[NET_FRAME_HDR..]; net_write_frame
                // prepends the [msg_type][len] header in place via scratch.
                let mut payload = [0u8; STREAM_DATA_PREFIX + MAX_PACKET];
                payload[0..4].copy_from_slice(&session.to_le_bytes());
                payload[4..8].copy_from_slice(&stream.to_le_bytes());
                if data_len > 0 {
                    core::ptr::copy_nonoverlapping(
                        s.frame_buf.as_ptr().add(CID),
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
                    dev_log(sys, 2, b"[mqtt_quic] tx DROP".as_ptr(), 19);
                }
            }
        }

        0
    }
}
