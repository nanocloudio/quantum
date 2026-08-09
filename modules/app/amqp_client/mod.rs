//! AMQP 0-9-1 connector — a GENUINE per-protocol Fluxor foundation module adding
//! a structural class the others don't: CHANNEL MULTIPLEXING. A five-message
//! connection handshake (protocol-header -> Start/Start-Ok -> Tune/Tune-Ok ->
//! Open/Open-Ok) brings the connection up, then a Channel.Open establishes a
//! logical channel over the single socket; every frame is tagged with a channel
//! number so many independent conversations share one connection. Negotiating a
//! multi-stream connection across a staged handshake is not something a stateless
//! request/reply codec can express, so it is a compiled module.
//!
//! The protocol logic lives in the host-tested `amqp_core.rs`; this file is the
//! I/O pump. On success it reports "amqp: connection + channel 1 open".
//!
//! Ports:  net_in/net_out (transport), status_out (handshake result).
//! Params: `endpoint` (hex `[ip:4][port:2 LE]`), `user`, `pass`, `vhost`.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK + shared cores are include!'d wholesale; each module consumes only a subset"
)]

use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "shared SDK surface across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

include!("../../common/cores/amqp_core.rs");
include!("../../common/cores/hex_core.rs");

const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;
const NET_CMD_CONNECT: u8 = 0x13;
const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;

const NET_BUF: usize = 2048;
const REQ_BUF: usize = 512;
const ACC_BUF: usize = 8192;
const NAME_BUF: usize = 128;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;

#[repr(C)]
struct AmqpState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    status_out: i32,

    ip: [u8; 4],
    port: u16,
    ep_hex: [u8; 16],
    ep_hex_len: u16,
    user: [u8; NAME_BUF],
    user_len: u16,
    pass: [u8; NAME_BUF],
    pass_len: u16,
    vhost: [u8; NAME_BUF],
    vhost_len: u16,

    phase: APhase,
    conn_id: u8,
    tag: u8,
    started_ms: u64,
    draining: u8,

    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,
    acc: [u8; ACC_BUF],
    acc_len: u32,

    nbuf: [u8; NET_BUF],
    ready: u32,
    errors: u32,
}

define_params! {
    AmqpState;

    1, endpoint, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.ep_hex_len as usize) < 16 {
            s.ep_hex[s.ep_hex_len as usize] = *d.add(i); s.ep_hex_len += 1; i += 1;
        }
    };
    2, user, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.user_len as usize) < NAME_BUF {
            s.user[s.user_len as usize] = *d.add(i); s.user_len += 1; i += 1;
        }
    };
    3, pass, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.pass_len as usize) < NAME_BUF {
            s.pass[s.pass_len as usize] = *d.add(i); s.pass_len += 1; i += 1;
        }
    };
    4, vhost, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.vhost_len as usize) < NAME_BUF {
            s.vhost[s.vhost_len as usize] = *d.add(i); s.vhost_len += 1; i += 1;
        }
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<AmqpState>() as u32
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

/// PIC module ABI entry: drain any work queued for this instance without
/// admitting new input.
///
/// # Safety
/// `state` is the kernel-owned buffer a prior `module_new` initialised, and is
/// exclusively borrowed for the duration of the call.
#[no_mangle]
#[link_section = ".text.module_drain"]
pub unsafe extern "C" fn module_drain(state: *mut u8) -> i32 {
    unsafe {
        (*(state as *mut AmqpState)).draining = 1;
        0
    }
}

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
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<AmqpState>() {
            return -2;
        }
        let s = &mut *(state as *mut AmqpState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.net_in = in_chan;
        s.net_out = out_chan;
        s.status_out = dev_channel_port(sys, 1, 1);
        s.ip = [0u8; 4];
        s.port = 0;
        s.ep_hex_len = 0;
        s.user_len = 0;
        s.pass_len = 0;
        s.vhost_len = 0;
        s.phase = APhase::Disconnected;
        s.conn_id = 0;
        s.tag = dev_requester_tag(sys);
        s.started_ms = 0;
        s.draining = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.acc_len = 0;
        s.ready = 0;
        s.errors = 0;
        parse_tlv(s, params, params_len);
        let mut ep = [0u8; 8];
        if let Some(n) = hex_decode(&s.ep_hex[..s.ep_hex_len as usize], &mut ep) {
            if n >= 6 {
                s.ip = [ep[0], ep[1], ep[2], ep[3]];
                s.port = u16::from_le_bytes([ep[4], ep[5]]);
            }
        }
        // default vhost "/"
        if s.vhost_len == 0 {
            s.vhost[0] = b'/';
            s.vhost_len = 1;
        }
        dev_log(sys, 3, b"[amqp] init".as_ptr(), 11);
        0
    }
}

unsafe fn stage(s: &mut AmqpState, n: usize, now: u64) {
    s.req_len = n as u16;
    s.req_sent = 0;
    s.started_ms = now;
}

unsafe fn emit_status(s: &mut AmqpState, text: &[u8]) {
    let sys = &*s.syscalls;
    if s.status_out >= 0 {
        let poll = (sys.channel_poll)(s.status_out, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            (sys.channel_write)(s.status_out, text.as_ptr(), text.len());
        }
    }
}

/// Feed one event into the handshake machine and perform its action. `tune`
/// carries the negotiated Tune values when the event is `GotTune`.
unsafe fn feed(s: &mut AmqpState, sys: &SyscallTable, ev: AEv, now: u64, tune: (u16, u32, u16)) {
    let (action, next) = amqp_transition(s.phase, ev);
    match action {
        AAct::Connect => {
            let mut payload = [0u8; 8];
            payload[0] = SOCK_TYPE_STREAM;
            payload[1] = s.ip[3];
            payload[2] = s.ip[2];
            payload[3] = s.ip[1];
            payload[4] = s.ip[0];
            let port = s.port.to_le_bytes();
            payload[5] = port[0];
            payload[6] = port[1];
            payload[7] = s.tag;
            net_write_frame(
                sys,
                s.net_out,
                NET_CMD_CONNECT,
                payload.as_ptr(),
                8,
                s.nbuf.as_mut_ptr(),
                NET_BUF,
            );
            s.started_ms = now;
        }
        AAct::SendProtocolHeader => {
            let mut out = [0u8; 8];
            if let Some(n) = amqp_protocol_header(&mut out) {
                s.req[..n].copy_from_slice(&out[..n]);
                stage(s, n, now);
            }
        }
        AAct::SendStartOk => {
            let ul = s.user_len as usize;
            let pl = s.pass_len as usize;
            let mut u = [0u8; NAME_BUF];
            u[..ul].copy_from_slice(&s.user[..ul]);
            let mut pw = [0u8; NAME_BUF];
            pw[..pl].copy_from_slice(&s.pass[..pl]);
            let mut out = [0u8; REQ_BUF];
            if let Some(n) = amqp_start_ok(&u[..ul], &pw[..pl], &mut out) {
                s.req[..n].copy_from_slice(&out[..n]);
                stage(s, n, now);
            }
        }
        AAct::SendTuneOkOpen => {
            // Tune-Ok (echo the server's negotiated values) then Connection.Open.
            let mut out = [0u8; REQ_BUF];
            let mut p = 0usize;
            if let Some(n) = amqp_tune_ok(tune.0, tune.1, tune.2, &mut out[p..]) {
                p += n;
            }
            let vl = s.vhost_len as usize;
            let mut vh = [0u8; NAME_BUF];
            vh[..vl].copy_from_slice(&s.vhost[..vl]);
            if let Some(n) = amqp_open(&vh[..vl], &mut out[p..]) {
                p += n;
            }
            s.req[..p].copy_from_slice(&out[..p]);
            stage(s, p, now);
        }
        AAct::SendChannelOpen => {
            let mut out = [0u8; REQ_BUF];
            if let Some(n) = amqp_channel_open(1, &mut out) {
                s.req[..n].copy_from_slice(&out[..n]);
                stage(s, n, now);
            }
        }
        AAct::Fail => {
            if s.conn_id != 0 {
                let close = [s.conn_id];
                net_write_frame(
                    sys,
                    s.net_out,
                    NET_CMD_CLOSE,
                    close.as_ptr(),
                    1,
                    s.nbuf.as_mut_ptr(),
                    NET_BUF,
                );
            }
            s.conn_id = 0;
            s.acc_len = 0;
            s.req_len = 0;
            s.req_sent = 0;
            s.errors = s.errors.wrapping_add(1);
            emit_status(s, b"amqp: handshake failed\n");
        }
        AAct::None => {}
    }
    if next == APhase::Ready && s.phase != APhase::Ready {
        s.ready = s.ready.wrapping_add(1);
        emit_status(s, b"amqp: connection + channel 1 open\n");
    }
    s.phase = next;
}

/// Parse and dispatch every complete frame in the accumulation buffer.
unsafe fn drain_frames(s: &mut AmqpState, sys: &SyscallTable, now: u64) {
    while let Some(f) = amqp_parse_frame(&s.acc[..s.acc_len as usize]) {
        if f.ftype == FRAME_METHOD {
            if let Some((class, meth)) = amqp_method_id(&s.acc[f.payload_start..f.payload_end]) {
                if let Some(ev) = amqp_classify(class, meth) {
                    let tune = if ev == AEv::GotTune {
                        amqp_parse_tune(&s.acc[f.payload_start..f.payload_end])
                            .unwrap_or((0, 131072, 0))
                    } else {
                        (0, 0, 0)
                    };
                    feed(s, sys, ev, now, tune);
                }
            }
        }
        // consume the frame
        let total = f.total;
        let rem = s.acc_len as usize - total;
        let mut k = 0usize;
        while k < rem {
            s.acc[k] = s.acc[total + k];
            k += 1;
        }
        s.acc_len = rem as u32;
        if s.acc_len == 0 {
            break;
        }
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
        let s = &mut *(state as *mut AmqpState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        if s.phase == APhase::Disconnected && s.draining == 0 && s.user_len > 0 {
            feed(s, sys, AEv::Start, now, (0, 0, 0));
        }

        if s.net_in >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.net_in, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (msg, plen) = net_read_frame(sys, s.net_in, s.nbuf.as_mut_ptr(), NET_BUF);
                if msg == 0 {
                    break;
                }
                let payload = s.nbuf.as_ptr().add(NET_FRAME_HDR);
                match msg {
                    NET_MSG_CONNECTED if s.phase == APhase::Connecting => {
                        if plen >= 2 && *payload.add(1) == s.tag {
                            s.conn_id = *payload;
                            feed(s, sys, AEv::Connected, now, (0, 0, 0));
                        }
                    }
                    NET_MSG_DATA if s.phase != APhase::Disconnected => {
                        if plen > 1 && *payload == s.conn_id {
                            let data_len = plen - 1;
                            let space = ACC_BUF - s.acc_len as usize;
                            let take = if data_len < space { data_len } else { space };
                            core::ptr::copy_nonoverlapping(
                                payload.add(1),
                                s.acc.as_mut_ptr().add(s.acc_len as usize),
                                take,
                            );
                            s.acc_len += take as u32;
                            drain_frames(s, sys, now);
                        }
                    }
                    NET_MSG_CLOSED if s.phase != APhase::Disconnected => {
                        if plen >= 1 && *payload == s.conn_id {
                            feed(s, sys, AEv::PeerClosed, now, (0, 0, 0));
                        }
                    }
                    NET_MSG_ERROR => {
                        let ours = (s.phase == APhase::Connecting
                            && plen >= 3
                            && *payload.add(2) == s.tag)
                            || (s.phase != APhase::Disconnected
                                && plen >= 1
                                && *payload == s.conn_id);
                        if ours {
                            feed(s, sys, AEv::NetError, now, (0, 0, 0));
                        }
                    }
                    _ => {}
                }
            }
        }

        // Send pump.
        if s.conn_id != 0 && s.req_sent < s.req_len {
            let max_chunk = NET_BUF - NET_FRAME_HDR - 1;
            while s.req_sent < s.req_len {
                let poll = (sys.channel_poll)(s.net_out, 0x02);
                if poll <= 0 || (poll as u32 & 0x02) == 0 {
                    break;
                }
                let remaining = (s.req_len - s.req_sent) as usize;
                let chunk = if remaining < max_chunk {
                    remaining
                } else {
                    max_chunk
                };
                let total_payload = chunk + 1;
                s.nbuf[0] = NET_CMD_SEND;
                s.nbuf[1] = (total_payload & 0xff) as u8;
                s.nbuf[2] = (total_payload >> 8) as u8;
                s.nbuf[3] = s.conn_id;
                core::ptr::copy_nonoverlapping(
                    s.req.as_ptr().add(s.req_sent as usize),
                    s.nbuf.as_mut_ptr().add(NET_FRAME_HDR + 1),
                    chunk,
                );
                (sys.channel_write)(s.net_out, s.nbuf.as_ptr(), NET_FRAME_HDR + total_payload);
                s.req_sent += chunk as u16;
            }
        }

        if !matches!(s.phase, APhase::Disconnected | APhase::Ready) {
            let budget = if s.phase == APhase::Connecting {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                feed(s, sys, AEv::NetError, now, (0, 0, 0));
            }
        }

        if s.draining == 1 && matches!(s.phase, APhase::Disconnected | APhase::Ready) {
            if s.conn_id != 0 {
                let close = [s.conn_id];
                net_write_frame(
                    sys,
                    s.net_out,
                    NET_CMD_CLOSE,
                    close.as_ptr(),
                    1,
                    s.nbuf.as_mut_ptr(),
                    NET_BUF,
                );
                s.conn_id = 0;
            }
            return 1;
        }
        0
    }
}
