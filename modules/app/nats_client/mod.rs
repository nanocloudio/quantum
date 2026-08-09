//! NATS connector — a GENUINE per-protocol Fluxor foundation module, and a
//! protocol CLASS the other connectors don't reach: server-pushed pub/sub
//! streaming. After a short INFO/CONNECT/SUB handshake this module does not poll
//! — it stays in `Ready` receiving unsolicited `MSG` frames the server pushes
//! whenever anyone publishes to the subscribed subject, indefinitely, while
//! answering `PING` keepalives with `PONG` (miss them and the server drops the
//! connection). An endpoint that emits frames on its own schedule and demands
//! liveness responses is a stateful bidirectional session — not request/reply —
//! so it must be a compiled module.
//!
//! The protocol logic lives in the host-tested `nats_core.rs`; this file is the
//! I/O pump. Every pushed message's payload is emitted on `message_out`.
//!
//! Ports:  net_in/net_out (transport), publish_in (payload to PUB), message_out
//!         (pushed message payloads).
//! Params: `endpoint` (hex `[ip:4][port:2 LE]`), `subject`, `user`, `pass`.

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

// Shared, host-tested cores — identical source to the `chronicle-bytecode` crate.
// `resp_core` supplies `itoa`, used by `nats_pub`.
include!("../../common/cores/resp_core.rs");
include!("../../common/cores/nats_core.rs");
include!("../../common/cores/hex_core.rs");

const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;
const NET_CMD_CONNECT: u8 = 0x13;
const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;

const NET_BUF: usize = 2048;
const REQ_BUF: usize = 2048;
const ACC_BUF: usize = 16384;
const NAME_BUF: usize = 128;
const CONNECT_TIMEOUT_MS: u64 = 10_000;

#[repr(C)]
struct NatsState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    publish_in: i32,
    message_out: i32,

    ip: [u8; 4],
    port: u16,
    ep_hex: [u8; 16],
    ep_hex_len: u16,
    subject: [u8; NAME_BUF],
    subject_len: u16,
    user: [u8; NAME_BUF],
    user_len: u16,
    pass: [u8; NAME_BUF],
    pass_len: u16,

    phase: NPhase,
    conn_id: u8,
    tag: u8,
    started_ms: u64,
    draining: u8,

    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,
    acc: [u8; ACC_BUF],
    acc_len: u32,

    // A publish payload staged from publish_in (sent once Ready).
    pubbuf: [u8; NAME_BUF],
    pub_len: u16,
    have_pub: u8,

    nbuf: [u8; NET_BUF],
    delivered: u32,
    pongs: u32,
    errors: u32,
}

define_params! {
    NatsState;

    1, endpoint, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.ep_hex_len as usize) < 16 {
            s.ep_hex[s.ep_hex_len as usize] = *d.add(i); s.ep_hex_len += 1; i += 1;
        }
    };
    2, subject, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.subject_len as usize) < NAME_BUF {
            s.subject[s.subject_len as usize] = *d.add(i); s.subject_len += 1; i += 1;
        }
    };
    3, user, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.user_len as usize) < NAME_BUF {
            s.user[s.user_len as usize] = *d.add(i); s.user_len += 1; i += 1;
        }
    };
    4, pass, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.pass_len as usize) < NAME_BUF {
            s.pass[s.pass_len as usize] = *d.add(i); s.pass_len += 1; i += 1;
        }
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<NatsState>() as u32
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
        (*(state as *mut NatsState)).draining = 1;
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
        if state_size < core::mem::size_of::<NatsState>() {
            return -2;
        }
        let s = &mut *(state as *mut NatsState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.net_in = in_chan;
        s.net_out = out_chan;
        s.publish_in = dev_channel_port(sys, 0, 1);
        s.message_out = dev_channel_port(sys, 1, 1);
        s.ip = [0u8; 4];
        s.port = 0;
        s.ep_hex_len = 0;
        s.subject_len = 0;
        s.user_len = 0;
        s.pass_len = 0;
        s.phase = NPhase::Disconnected;
        s.conn_id = 0;
        s.tag = dev_requester_tag(sys);
        s.started_ms = 0;
        s.draining = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.acc_len = 0;
        s.pub_len = 0;
        s.have_pub = 0;
        s.delivered = 0;
        s.pongs = 0;
        s.errors = 0;
        parse_tlv(s, params, params_len);
        let mut ep = [0u8; 8];
        if let Some(n) = hex_decode(&s.ep_hex[..s.ep_hex_len as usize], &mut ep) {
            if n >= 6 {
                s.ip = [ep[0], ep[1], ep[2], ep[3]];
                s.port = u16::from_le_bytes([ep[4], ep[5]]);
            }
        }
        dev_log(sys, 3, b"[nats] init".as_ptr(), 11);
        0
    }
}

unsafe fn stage_send(s: &mut NatsState, n: usize) {
    s.req_len = n as u16;
    s.req_sent = 0;
}

/// Feed one event into the subscriber machine and perform its action.
unsafe fn feed(
    s: &mut NatsState,
    sys: &SyscallTable,
    ev: NEv,
    now: u64,
    msg: Option<(usize, usize)>,
) {
    let (action, next) = nats_transition(s.phase, ev);
    match action {
        NAct::Connect => {
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
        NAct::SendConnectSub => {
            // CONNECT then SUB, concatenated into the flush buffer.
            let mut out = [0u8; REQ_BUF];
            let mut p = 0usize;
            let ul = s.user_len as usize;
            let pl = s.pass_len as usize;
            let mut u = [0u8; NAME_BUF];
            u[..ul].copy_from_slice(&s.user[..ul]);
            let mut pw = [0u8; NAME_BUF];
            pw[..pl].copy_from_slice(&s.pass[..pl]);
            if let Some(n) = nats_connect(false, &u[..ul], &pw[..pl], &mut out[p..]) {
                p += n;
            }
            let sl = s.subject_len as usize;
            let mut subj = [0u8; NAME_BUF];
            subj[..sl].copy_from_slice(&s.subject[..sl]);
            if let Some(n) = nats_sub(&subj[..sl], b"1", &mut out[p..]) {
                p += n;
            }
            s.req[..p].copy_from_slice(&out[..p]);
            stage_send(s, p);
        }
        NAct::SendPong => {
            let mut out = [0u8; 8];
            if let Some(n) = nats_pong(&mut out) {
                s.req[..n].copy_from_slice(&out[..n]);
                stage_send(s, n);
            }
            s.pongs = s.pongs.wrapping_add(1);
        }
        NAct::DeliverMsg => {
            if let Some((ps, pe)) = msg {
                if s.message_out >= 0 && pe <= s.acc_len as usize && pe >= ps {
                    let poll = (sys.channel_poll)(s.message_out, 0x02);
                    if poll > 0 && (poll as u32 & 0x02) != 0 {
                        (sys.channel_write)(s.message_out, s.acc.as_ptr().add(ps), pe - ps);
                    }
                }
                s.delivered = s.delivered.wrapping_add(1);
            }
        }
        NAct::Fail => {
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
        }
        NAct::None => {}
    }
    s.phase = next;
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
        let s = &mut *(state as *mut NatsState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // 1. Subscribe on boot (nothing else required to start the session).
        if s.phase == NPhase::Disconnected && s.draining == 0 && s.subject_len > 0 {
            feed(s, sys, NEv::Start, now, None);
        }

        // 2. Stage a publish payload from publish_in (sent while Ready, below).
        if s.have_pub == 0 && s.publish_in >= 0 {
            let poll = (sys.channel_poll)(s.publish_in, 0x01);
            if poll > 0 && (poll as u32 & 0x01) != 0 {
                let n = (sys.channel_read)(s.publish_in, s.pubbuf.as_mut_ptr(), NAME_BUF);
                if n > 0 {
                    s.pub_len = n as u16;
                    s.have_pub = 1;
                }
            }
        }
        if s.have_pub == 1 && s.phase == NPhase::Ready && s.req_sent >= s.req_len {
            let sl = s.subject_len as usize;
            let mut subj = [0u8; NAME_BUF];
            subj[..sl].copy_from_slice(&s.subject[..sl]);
            let plen = s.pub_len as usize;
            let mut pl = [0u8; NAME_BUF];
            pl[..plen].copy_from_slice(&s.pubbuf[..plen]);
            let mut out = [0u8; REQ_BUF];
            if let Some(n) = nats_pub(&subj[..sl], &pl[..plen], &mut out) {
                s.req[..n].copy_from_slice(&out[..n]);
                stage_send(s, n);
            }
            s.have_pub = 0;
        }

        // 3. Drain network events; parse every complete server frame.
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
                    NET_MSG_CONNECTED if s.phase == NPhase::Connecting => {
                        if plen >= 2 && *payload.add(1) == s.tag {
                            s.conn_id = *payload;
                            feed(s, sys, NEv::Connected, now, None);
                        }
                    }
                    NET_MSG_DATA if s.phase != NPhase::Disconnected => {
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
                    NET_MSG_CLOSED if s.phase != NPhase::Disconnected => {
                        if plen >= 1 && *payload == s.conn_id {
                            feed(s, sys, NEv::PeerClosed, now, None);
                        }
                    }
                    NET_MSG_ERROR => {
                        let ours = (s.phase == NPhase::Connecting
                            && plen >= 3
                            && *payload.add(2) == s.tag)
                            || (s.phase != NPhase::Disconnected
                                && plen >= 1
                                && *payload == s.conn_id);
                        if ours {
                            feed(s, sys, NEv::NetError, now, None);
                        }
                    }
                    _ => {}
                }
            }
        }

        // 4. Send pump.
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

        // 5. Connect timeout (a live subscription has no reply deadline).
        if matches!(s.phase, NPhase::Connecting | NPhase::AwaitInfo)
            && now.wrapping_sub(s.started_ms) > CONNECT_TIMEOUT_MS
        {
            feed(s, sys, NEv::NetError, now, None);
        }

        // 6. Drain: leave once idle.
        if s.draining == 1
            && matches!(s.phase, NPhase::Disconnected | NPhase::Ready)
            && s.req_sent >= s.req_len
        {
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

/// Parse and dispatch every complete server frame in the accumulation buffer.
unsafe fn drain_frames(s: &mut NatsState, sys: &SyscallTable, now: u64) {
    loop {
        let f = nats_parse(&s.acc[..s.acc_len as usize]);
        if f.kind == NatsKind::Incomplete {
            break;
        }
        let ev = match f.kind {
            NatsKind::Info => NEv::GotInfo,
            NatsKind::Msg => NEv::GotMsg,
            NatsKind::Ping => NEv::GotPing,
            NatsKind::Err => NEv::GotErr,
            _ => {
                // +OK / PONG / Unknown: consume and ignore.
                consume(s, f.total);
                continue;
            }
        };
        let payload = if f.kind == NatsKind::Msg {
            Some((f.payload_start, f.payload_end))
        } else {
            None
        };
        feed(s, sys, ev, now, payload);
        consume(s, f.total);
        if s.acc_len == 0 {
            break;
        }
    }
}

/// Drop the first `total` bytes of the accumulation buffer (manual compaction —
/// no `copy_within` panic path).
unsafe fn consume(s: &mut NatsState, total: usize) {
    let used = s.acc_len as usize;
    if total == 0 || total > used {
        s.acc_len = 0;
        return;
    }
    let rem = used - total;
    let mut k = 0usize;
    while k < rem {
        s.acc[k] = s.acc[total + k];
        k += 1;
    }
    s.acc_len = rem as u32;
}
