//! Kafka consumer-group connector — a GENUINE per-protocol Fluxor foundation
//! module. Where the Postgres connector's proof is cryptographic, this one's is
//! structural: a Kafka consumer cannot read a single record until it has driven
//! a FIVE-STEP multi-round-trip membership handshake —
//!
//!   ApiVersions -> FindCoordinator -> JoinGroup -> SyncGroup -> Stable
//!
//! — where each request is built from fields the PREVIOUS response returned
//! (coordinator host/port, coordinator-assigned member_id, generation_id), and
//! membership is then retained only by heartbeating on a TIMER. A heartbeat
//! answered REBALANCE_IN_PROGRESS drops straight back to JoinGroup. Cross-request
//! state, timers, and reply-dependent control flow: none of it is expressible in
//! a stateless encode/decode program, so this must be a compiled module.
//!
//! The protocol logic lives in the host-tested `kafka_core.rs`; this file is the
//! I/O pump mapping its actions onto net_proto frames.
//!
//! Ports:  net_in/net_out (transport), publish_in (unused; reserved for fetch/
//!         produce), status_out (membership transitions).
//! Params: `endpoint` (hex `[ip:4][port:2 LE]`), `group_id`, `client_id`,
//!         `session_timeout_ms`, `heartbeat_ms`.
//!
//! Scope: single-broker. The coordinator returned by FindCoordinator is assumed
//! reachable at the configured endpoint; reconnecting to a different coordinator
//! host/port is a documented follow-on, not built here.

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
include!("../../common/cores/kafka_core.rs");
include!("../../common/cores/hex_core.rs");

const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;
const NET_CMD_CONNECT: u8 = 0x13;
const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;

/// True when a `net_proto` frame's leading `[conn_id: u16 LE]` names
/// `want`. Stated once because every inbound frame carries it, and a
/// conn-id read of the wrong WIDTH matches nothing while looking
/// entirely reasonable at the call site.
#[inline]
unsafe fn conn_matches(payload: *const u8, plen: usize, want: u16) -> bool {
    plen >= 2 && u16::from_le_bytes([*payload, *payload.add(1)]) == want
}

const NET_BUF: usize = 2048;
const REQ_BUF: usize = 1024;
const ACC_BUF: usize = 8192;
const NAME_BUF: usize = 96;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;

#[repr(C)]
struct KafkaState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    publish_in: i32,
    status_out: i32,

    ip: [u8; 4],
    port: u16,
    ep_hex: [u8; 16],
    ep_hex_len: u16,
    group_id: [u8; NAME_BUF],
    group_id_len: u16,
    client_id: [u8; NAME_BUF],
    client_id_len: u16,
    session_timeout_ms: u32,
    heartbeat_ms: u32,

    phase: KPhase,
    /// `net_proto` conn ids are `u16 LE` on every frame. Held as a
    /// `u8`, the `MSG_CONNECTED` tag check read the HIGH BYTE of the
    /// conn id instead of the requester tag, never matched, and the
    /// socket opened without a single byte ever being sent.
    conn_id: u16,
    tag: u8,
    started_ms: u64,
    last_hb_ms: u64,
    draining: u8,
    corr: i32,

    // Membership identity handed back by the coordinator.
    member_id: [u8; NAME_BUF],
    member_id_len: u16,
    generation_id: i32,

    // PRODUCER mode: when `produce_topic` is set the module skips the
    // consumer-group dance and produces each `publish_in` message to the topic.
    produce_topic: [u8; NAME_BUF],
    produce_topic_len: u16,
    producing: u8,
    produced: u32,

    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,
    acc: [u8; ACC_BUF],
    acc_len: u32,

    nbuf: [u8; NET_BUF],
    joins: u32,
    heartbeats: u32,
    rebalances: u32,
    errors: u32,
}

define_params! {
    KafkaState;

    1, endpoint, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.ep_hex_len as usize) < 16 {
            s.ep_hex[s.ep_hex_len as usize] = *d.add(i); s.ep_hex_len += 1; i += 1;
        }
    };
    2, group_id, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.group_id_len as usize) < NAME_BUF {
            s.group_id[s.group_id_len as usize] = *d.add(i); s.group_id_len += 1; i += 1;
        }
    };
    3, client_id, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.client_id_len as usize) < NAME_BUF {
            s.client_id[s.client_id_len as usize] = *d.add(i); s.client_id_len += 1; i += 1;
        }
    };
    4, session_timeout_ms, u32, 30000 => |s, d, len| {
        s.session_timeout_ms = p_u32(d, len, 0, 30000);
    };
    5, heartbeat_ms, u32, 3000 => |s, d, len| {
        s.heartbeat_ms = p_u32(d, len, 0, 3000);
    };
    6, produce_topic, str, 0 => |s, d, len| {
        // Gate on len>0: set_defaults() fires every closure, so an absent
        // param must not flip the module into producer mode.
        if len > 0 {
            let mut i = 0usize;
            while i < len && (s.produce_topic_len as usize) < NAME_BUF {
                s.produce_topic[s.produce_topic_len as usize] = *d.add(i);
                s.produce_topic_len += 1;
                i += 1;
            }
            s.producing = 1;
        }
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<KafkaState>() as u32
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
        (*(state as *mut KafkaState)).draining = 1;
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
        if state_size < core::mem::size_of::<KafkaState>() {
            return -2;
        }
        let s = &mut *(state as *mut KafkaState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.net_in = in_chan;
        s.net_out = out_chan;
        s.publish_in = dev_channel_port(sys, 0, 1);
        s.status_out = dev_channel_port(sys, 1, 1);
        s.ip = [0u8; 4];
        s.port = 0;
        s.ep_hex_len = 0;
        s.group_id_len = 0;
        s.client_id_len = 0;
        s.session_timeout_ms = 30_000;
        s.heartbeat_ms = 3_000;
        s.phase = KPhase::Disconnected;
        s.conn_id = 0;
        s.tag = dev_requester_tag(sys);
        s.started_ms = 0;
        s.last_hb_ms = 0;
        s.draining = 0;
        s.corr = 0;
        s.member_id_len = 0;
        s.generation_id = -1;
        s.produce_topic_len = 0;
        s.producing = 0;
        s.produced = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.acc_len = 0;
        s.joins = 0;
        s.heartbeats = 0;
        s.rebalances = 0;
        s.errors = 0;
        parse_tlv(s, params, params_len);
        let mut ep = [0u8; 8];
        if let Some(n) = hex_decode(&s.ep_hex[..s.ep_hex_len as usize], &mut ep) {
            if n >= 6 {
                s.ip = [ep[0], ep[1], ep[2], ep[3]];
                s.port = u16::from_le_bytes([ep[4], ep[5]]);
            }
        }
        dev_log(sys, 3, b"[kafka] init".as_ptr(), 12);
        0
    }
}

/// Stage a framed request built from `body` for the given API (version 0 —
/// the membership APIs all use v0 here).
unsafe fn send_request(s: &mut KafkaState, api_key: i16, body: &[u8], now: u64) {
    send_request_v(s, api_key, 0, body, now);
}

/// Stage a framed request at an explicit API version (Produce needs v7).
unsafe fn send_request_v(
    s: &mut KafkaState,
    api_key: i16,
    api_version: i16,
    body: &[u8],
    now: u64,
) {
    s.corr = s.corr.wrapping_add(1);
    let cid_len = s.client_id_len as usize;
    let mut cid = [0u8; NAME_BUF];
    cid[..cid_len].copy_from_slice(&s.client_id[..cid_len]);
    let mut out = [0u8; REQ_BUF];
    match kafka_request(
        api_key,
        api_version,
        s.corr,
        &cid[..cid_len],
        body,
        &mut out,
    ) {
        Some(n) => {
            s.req[..n].copy_from_slice(&out[..n]);
            s.req_len = n as u16;
            s.req_sent = 0;
            s.started_ms = now;
            s.acc_len = 0;
        }
        None => {
            s.req_len = 0;
            s.errors = s.errors.wrapping_add(1);
        }
    }
}

unsafe fn emit_status(s: &mut KafkaState, text: &[u8]) {
    let sys = &*s.syscalls;
    if s.status_out >= 0 {
        let poll = (sys.channel_poll)(s.status_out, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            (sys.channel_write)(s.status_out, text.as_ptr(), text.len());
        }
    }
}

/// Feed one event into the membership machine and perform the action it returns.
unsafe fn feed(s: &mut KafkaState, sys: &SyscallTable, ev: KEv, now: u64) {
    let (action, next) = kafka_transition(s.phase, ev);
    // Producer mode skips consumer-group membership: once the TCP connection is
    // up (the transition would otherwise send ApiVersions), go straight to
    // ProduceIdle and wait for messages on publish_in.
    if s.producing != 0 && matches!(action, KAct::SendApiVersions) {
        s.phase = KPhase::ProduceIdle;
        return;
    }
    let gid_len = s.group_id_len as usize;
    let mut gid = [0u8; NAME_BUF];
    gid[..gid_len].copy_from_slice(&s.group_id[..gid_len]);
    let mid_len = s.member_id_len as usize;
    let mut mid = [0u8; NAME_BUF];
    mid[..mid_len].copy_from_slice(&s.member_id[..mid_len]);

    match action {
        KAct::Connect => {
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
        KAct::SendApiVersions => send_request(s, api::API_VERSIONS, &[], now),
        KAct::SendFindCoordinator => {
            let mut body = [0u8; 128];
            if let Some(n) = kafka_find_coordinator_body(&gid[..gid_len], &mut body) {
                send_request(s, api::FIND_COORDINATOR, &body[..n], now);
            }
        }
        KAct::SendJoinGroup => {
            let mut body = [0u8; 256];
            if let Some(n) = kafka_join_group_body(
                &gid[..gid_len],
                s.session_timeout_ms as i32,
                &mid[..mid_len],
                b"range",
                &[],
                &mut body,
            ) {
                send_request(s, api::JOIN_GROUP, &body[..n], now);
                s.joins = s.joins.wrapping_add(1);
            }
        }
        KAct::SendSyncGroup => {
            let mut body = [0u8; 256];
            if let Some(n) =
                kafka_sync_group_body(&gid[..gid_len], s.generation_id, &mid[..mid_len], &mut body)
            {
                send_request(s, api::SYNC_GROUP, &body[..n], now);
            }
        }
        KAct::SendHeartbeat => {
            let mut body = [0u8; 256];
            if let Some(n) =
                kafka_heartbeat_body(&gid[..gid_len], s.generation_id, &mid[..mid_len], &mut body)
            {
                send_request(s, api::HEARTBEAT, &body[..n], now);
                s.heartbeats = s.heartbeats.wrapping_add(1);
            }
            s.last_hb_ms = now;
        }
        KAct::Fail => {
            if s.conn_id != 0 {
                let close = s.conn_id.to_le_bytes();
                net_write_frame(
                    sys,
                    s.net_out,
                    NET_CMD_CLOSE,
                    close.as_ptr(),
                    2,
                    s.nbuf.as_mut_ptr(),
                    NET_BUF,
                );
            }
            s.conn_id = 0;
            s.acc_len = 0;
            s.req_len = 0;
            s.req_sent = 0;
            s.member_id_len = 0;
            s.generation_id = -1;
            s.errors = s.errors.wrapping_add(1);
            emit_status(s, b"kafka: membership lost\n");
        }
        KAct::None => {}
    }
    if next == KPhase::Stable && s.phase != KPhase::Stable {
        s.last_hb_ms = now;
        emit_status(s, b"kafka: group member stable\n");
    }
    if next == KPhase::AwaitJoin && s.phase == KPhase::AwaitHeartbeat {
        s.rebalances = s.rebalances.wrapping_add(1);
    }
    s.phase = next;
}

/// Dispatch one complete response body according to the phase that asked for it.
unsafe fn on_response(s: &mut KafkaState, body: &[u8], now: u64) {
    let sys = &*s.syscalls;
    // Producer: the Produce ack drives the produce cursor directly — the
    // consumer-group transition table isn't involved.
    if s.phase == KPhase::ProduceWait {
        match kafka_parse_produce_response(body) {
            Some((0, _offset)) => {
                s.produced = s.produced.wrapping_add(1);
                emit_status(s, b"kafka: produced\n");
                s.phase = KPhase::ProduceIdle;
            }
            Some((code, _)) => {
                s.errors = s.errors.wrapping_add(1);
                let msg: &[u8] = match code {
                    2 => b"kafka: produce error CORRUPT_MESSAGE\n",
                    3 => b"kafka: produce error UNKNOWN_TOPIC_OR_PARTITION\n",
                    6 => b"kafka: produce error NOT_LEADER\n",
                    42 => b"kafka: produce error INVALID_RECORD\n",
                    _ => b"kafka: produce error (other)\n",
                };
                emit_status(s, msg);
                s.phase = KPhase::ProduceIdle;
            }
            None => feed(s, sys, KEv::RespFatal, now),
        }
        return;
    }
    let ev = match s.phase {
        KPhase::AwaitApiVersions => match kafka_parse_error_code(body) {
            Some(c) => kafka_classify_error(c),
            None => KEv::RespFatal,
        },
        KPhase::AwaitCoordinator => match kafka_parse_find_coordinator(body) {
            Some(c) => kafka_classify_error(c.error_code),
            None => KEv::RespFatal,
        },
        KPhase::AwaitJoin => match kafka_parse_join_group(body) {
            Some(j) => {
                // The coordinator-assigned identity every later request carries.
                let n = j.member_id.len().min(NAME_BUF);
                s.member_id[..n].copy_from_slice(&j.member_id[..n]);
                s.member_id_len = n as u16;
                s.generation_id = j.generation_id;
                kafka_classify_error(j.error_code)
            }
            None => KEv::RespFatal,
        },
        KPhase::AwaitSync => match kafka_parse_sync_group(body) {
            Some((code, _assignment)) => kafka_classify_error(code),
            None => KEv::RespFatal,
        },
        KPhase::AwaitHeartbeat => match kafka_parse_error_code(body) {
            Some(c) => kafka_classify_error(c),
            None => KEv::RespFatal,
        },
        _ => return,
    };
    feed(s, sys, ev, now);
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
        let s = &mut *(state as *mut KafkaState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // 1. A consumer joins its group (or a producer connects) on boot —
        //    nothing else is required to start.
        if s.phase == KPhase::Disconnected
            && s.draining == 0
            && (s.group_id_len > 0 || s.producing != 0)
        {
            feed(s, sys, KEv::Start, now);
        }

        // 1b. Producer: pull one message off publish_in and produce it to the
        //     configured topic (partition 0). One in flight at a time.
        if s.producing != 0 && s.phase == KPhase::ProduceIdle && s.publish_in >= 0 {
            let poll = (sys.channel_poll)(s.publish_in, 0x01);
            if poll > 0 && (poll as u32 & 0x01) != 0 {
                let mut msg = [0u8; 512];
                let n = (sys.channel_read)(s.publish_in, msg.as_mut_ptr(), msg.len());
                if n > 0 {
                    let tl = s.produce_topic_len as usize;
                    let mut topic = [0u8; NAME_BUF];
                    topic[..tl].copy_from_slice(&s.produce_topic[..tl]);
                    let ts = dev_unix_millis(sys) as i64;
                    let mut body = [0u8; REQ_BUF];
                    if let Some(bn) =
                        kafka_produce_body(&topic[..tl], &msg[..n as usize], ts, &mut body)
                    {
                        send_request_v(s, api::PRODUCE, 7, &body[..bn], now);
                        s.phase = KPhase::ProduceWait;
                    }
                }
            }
        }

        // 2. Membership upkeep: the heartbeat is TIMER-driven, not request-driven.
        if s.phase == KPhase::Stable && now.wrapping_sub(s.last_hb_ms) >= s.heartbeat_ms as u64 {
            feed(s, sys, KEv::HeartbeatDue, now);
        }

        // 3. Drain network events.
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
                    // `[conn_id: u16 LE][requester_tag: u8]`
                    NET_MSG_CONNECTED if s.phase == KPhase::Connecting => {
                        if plen >= 3 && *payload.add(2) == s.tag {
                            s.conn_id = u16::from_le_bytes([*payload, *payload.add(1)]);
                            feed(s, sys, KEv::Connected, now);
                        }
                    }
                    // `[conn_id: u16 LE][data…]`
                    NET_MSG_DATA if s.phase != KPhase::Disconnected => {
                        if plen > 2 && conn_matches(payload, plen, s.conn_id) {
                            let data_len = plen - 2;
                            let space = ACC_BUF - s.acc_len as usize;
                            let take = if data_len < space { data_len } else { space };
                            core::ptr::copy_nonoverlapping(
                                payload.add(2),
                                s.acc.as_mut_ptr().add(s.acc_len as usize),
                                take,
                            );
                            s.acc_len += take as u32;
                            // One size-prefixed response at a time.
                            if let Some(total) = kafka_response_len(&s.acc[..s.acc_len as usize]) {
                                if let Some((_corr, boff)) = kafka_response_header(&s.acc[..total])
                                {
                                    let mut body = [0u8; 1024];
                                    let blen = (total - boff).min(body.len());
                                    body[..blen].copy_from_slice(&s.acc[boff..boff + blen]);
                                    // Consume the response before dispatching.
                                    let rem = s.acc_len as usize - total;
                                    let mut k = 0usize;
                                    while k < rem {
                                        s.acc[k] = s.acc[total + k];
                                        k += 1;
                                    }
                                    s.acc_len = rem as u32;
                                    on_response(s, &body[..blen], now);
                                }
                            }
                        }
                    }
                    // `[conn_id: u16 LE]`
                    NET_MSG_CLOSED if s.phase != KPhase::Disconnected => {
                        if conn_matches(payload, plen, s.conn_id) {
                            feed(s, sys, KEv::PeerClosed, now);
                        }
                    }
                    NET_MSG_ERROR => {
                        // `[conn_id: u16 LE][errno: i8][requester_tag: u8?]`
                        // — a connect-phase failure has a meaningless
                        // conn_id, so the tag is the only way to know it
                        // is ours.
                        let ours = (s.phase == KPhase::Connecting
                            && plen >= 4
                            && *payload.add(3) == s.tag)
                            || (s.phase != KPhase::Disconnected
                                && conn_matches(payload, plen, s.conn_id));
                        if ours {
                            feed(s, sys, KEv::NetError, now);
                        }
                    }
                    _ => {}
                }
            }
        }

        // 4. Send pump.
        if s.conn_id != 0 && s.req_sent < s.req_len {
            // -2 for the `[conn_id: u16 LE]` prefix on CMD_SEND.
            let max_chunk = NET_BUF - NET_FRAME_HDR - 2;
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
                let total_payload = chunk + 2;
                s.nbuf[0] = NET_CMD_SEND;
                s.nbuf[1] = (total_payload & 0xff) as u8;
                s.nbuf[2] = (total_payload >> 8) as u8;
                let cid = s.conn_id.to_le_bytes();
                s.nbuf[3] = cid[0];
                s.nbuf[4] = cid[1];
                core::ptr::copy_nonoverlapping(
                    s.req.as_ptr().add(s.req_sent as usize),
                    s.nbuf.as_mut_ptr().add(NET_FRAME_HDR + 2),
                    chunk,
                );
                (sys.channel_write)(s.net_out, s.nbuf.as_ptr(), NET_FRAME_HDR + total_payload);
                s.req_sent += chunk as u16;
            }
        }

        // 5. Timeouts on any in-flight exchange.
        if !matches!(s.phase, KPhase::Disconnected | KPhase::Stable) {
            let budget = if s.phase == KPhase::Connecting {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                feed(s, sys, KEv::NetError, now);
            }
        }

        // 6. Drain: leave the group cleanly once idle.
        if s.draining == 1 && matches!(s.phase, KPhase::Disconnected | KPhase::Stable) {
            if s.conn_id != 0 {
                let close = s.conn_id.to_le_bytes();
                net_write_frame(
                    sys,
                    s.net_out,
                    NET_CMD_CLOSE,
                    close.as_ptr(),
                    2,
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
