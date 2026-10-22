// broker_front — quantum's broker as exchange providers in the application's
// own graph. See manifest.toml for the surface and the delivery terms, and
// docs/architecture/broker_front.md for the design.
//
// The module is a broker CLIENT that speaks the codec envelope instead of
// MQTT: it writes session envelopes to `session_processor.front_in` and reads
// the broker's answers off `session_processor.front_out`, on connection ids
// from `wire::FRONT_CONN_BASE` up. One session carries the publish front;
// each subscription is a session of its own.
//
//   publish_in  ──PUBLISH──▶ desk ──QoS 1 PUBLISH──▶ session_out
//   publish_out ◀──200──────  desk ◀──PUBACK──────── session_in
//
//   subscribe_in  ──GET filter──▶ CONNECT, SUBSCRIBE ──▶ session_out
//   subscribe_out ◀──HEAD HOLD|MORE, BODY per message ◀── session_in
//                                     PUBACK once placed ──▶ session_out
//
// The exchange mechanics live in `modules/common/publish_exchange.rs` (the
// publish front, shared with mqtt_sink, kafka_sink and amqp_sink) and
// `modules/common/subscribe_exchange.rs`; the envelopes in
// `modules/common/front_session.rs`.

#![no_std]
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

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/front_session.rs"]
mod front_session;
use front_session::Answer;

// The exchange contract, through `publish_exchange`'s mount of it, which
// `subscribe_exchange` shares.
#[path = "../../common/publish_exchange.rs"]
mod publish_exchange;
use publish_exchange::exchange::{status, PAYLOAD_MAX, RECORD_MAX};
use publish_exchange::PublishProvider;

#[path = "../../common/subscribe_exchange.rs"]
mod subscribe_exchange;
use subscribe_exchange::{Delivered, Subscriptions, Taken, HOLD_TWO};

// The envelope numbering has one owner, `wire.rs`; the front's copy is held
// to it here.
const _: () = {
    assert!(front_session::MSG_SESSION_PROPOSAL == wire::MSG_SESSION_PROPOSAL);
    assert!(front_session::MSG_SESSION_RESPONSE == wire::MSG_SESSION_RESPONSE);
    assert!(front_session::MSG_SESSION_CONNECT == wire::MSG_SESSION_CONNECT);
    assert!(front_session::MSG_SESSION_DISCONNECT == wire::MSG_SESSION_DISCONNECT);
    assert!(front_session::WIRE_MAX == abi::CHANNEL_BUFFER_SIZE);
};

// ── Sizing ───────────────────────────────────────────────────────────

/// The longest publish topic (`topic`) this module takes: the most a string
/// param carries.
const TOPIC_MAX: usize = 255;

/// The largest record the publish front takes on `topic_len` bytes of
/// topic: the least of what one session envelope carries, what the broker
/// commits as a QoS 1 publish (`wire::qos1_payload_room`), and what one
/// BODY of the subscription front hands back — so a record this front
/// accepts is one the broker carries and this front's subscribers receive.
const fn record_room(topic_len: usize) -> usize {
    let mut room = front_session::publish_room(topic_len);
    let broker = wire::qos1_payload_room(topic_len);
    if broker < room {
        room = broker;
    }
    let body = subscribe_exchange::payload_room(topic_len);
    if body < room {
        room = body;
    }
    room
}
/// The capability fact `max_payload`: the room on the longest topic.
const _: () = assert!(record_room(TOPIC_MAX) == 1752);
/// The longest `client_id`: room for the `-sXXXX` a live-only
/// subscription's session appends, inside the client-id bound.
const CLIENT_ID_MAX: usize = front_session::CLIENT_ID_MAX - 6;

/// The publish session's connection id; subscriptions take the rest of the
/// front namespace in turn.
const PUBLISH_CONN: u16 = wire::FRONT_CONN_BASE;
const SUB_CONN_FIRST: u16 = wire::FRONT_CONN_BASE + 1;
const SUB_CONN_COUNT: u16 = u16::MAX - SUB_CONN_FIRST + 1;

/// QoS 1 publishes awaiting the broker's acknowledgement. A full window
/// stops `publish_in` being read (backpressure by channel).
const INFLIGHT_CAP: usize = 32;
/// Publish requests collected at once while their bodies arrive.
const REQUEST_SLOTS: usize = 2;
/// Records owed on `publish_out`: one per window slot, one per read, and
/// the LINK pair.
const OWED_CAP: usize = INFLIGHT_CAP + 3;
type Desk = PublishProvider<REQUEST_SLOTS, PAYLOAD_MAX, INFLIGHT_CAP, OWED_CAP>;

/// Subscriptions served at once.
const SUB_SLOTS: usize = 8;
const _: () = assert!(SUB_CONN_COUNT as usize > SUB_SLOTS);
/// QoS 1 deliveries the broker holds unacknowledged per subscription: the
/// one the front holds until credit admits it.
const SUB_RECV_MAX: u16 = 1;
type Subs = Subscriptions<SUB_SLOTS, HOLD_TWO>;

/// Session envelopes owed on `session_out`, whole messages back to back:
/// room for one PUBLISH and the control traffic behind it.
const TX_CAP: usize = 2 * front_session::WIRE_MAX;
/// What any one control step may owe: a SUBSCRIBE on the longest filter, a
/// PUBACK and a DISCONNECT. Reading a record waits until this much is free.
const CONTROL_RESERVE: usize = 512;
const _: () = assert!(
    front_session::CONTROL_MAX + 8 + 5 + subscribe_exchange::FILTER_MAX + 16 <= CONTROL_RESERVE
);

/// Answers read off `session_in` per step.
const SESSION_READS_PER_STEP: usize = 32;

const BACKOFF_INIT_MS: u64 = 100;
const BACKOFF_MAX_MS: u64 = 5000;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq)]
enum PubPhase {
    /// `publish_in` is not wired: no publish session.
    Idle = 0,
    Connect = 1,
    WaitConnack = 2,
    Running = 3,
    Backoff = 4,
}

#[repr(C)]
struct FrontState {
    syscalls: *const SyscallTable,
    session_in: i32,
    session_out: i32,
    publish_in: i32,
    publish_out: i32,
    subscribe_in: i32,
    subscribe_out: i32,

    topic: [u8; TOPIC_MAX],
    topic_len: u16,
    /// A param past its bound: refused at construction, never truncated.
    topic_over: u8,
    client_id: [u8; CLIENT_ID_MAX],
    client_id_len: u8,
    client_id_over: u8,
    ack_timeout_ms: u32,
    session_expiry_s: u32,

    pub_phase: PubPhase,
    pub_deadline_ms: u64,
    packet_id: u16,
    backoff_ms: u64,

    /// Publish requests being collected, publishes awaiting the broker
    /// (keyed by packet id), and the records owed on `publish_out`.
    desk: Desk,
    pub_outbox: ExchangeOutbox,
    /// Subscriptions, and the record held for `subscribe_out`.
    subs: Subs,
    sub_outbox: ExchangeOutbox,

    tx: [u8; TX_CAP],
    tx_len: usize,

    pub_req: [u8; RECORD_MAX],
    pub_resp: [u8; RECORD_MAX],
    sub_req: [u8; RECORD_MAX],
    sub_resp: [u8; RECORD_MAX],
    rx: [u8; front_session::WIRE_MAX],
    env: [u8; front_session::WIRE_MAX],
}

mod params_def {
    use super::p_u32;
    use super::FrontState;
    use super::SCHEMA_MAX;
    use super::{CLIENT_ID_MAX, TOPIC_MAX};

    define_params! {
        FrontState;

        // The topic every publish lands on, as mqtt_sink's `topic`: the
        // request's target is its ordering key, never its topic. Required
        // when `publish_in` is wired.
        1, topic, str, 0
            => |s, d, len| {
                s.topic_over = u8::from(len > TOPIC_MAX);
                if len <= TOPIC_MAX {
                    s.topic_len = len as u16;
                    core::ptr::copy_nonoverlapping(d, s.topic.as_mut_ptr(), len);
                }
            };

        // The publish session's MQTT client id (`quantum-front` when unset);
        // a live-only subscription's session is `<client_id>-s<conn hex>`.
        2, client_id, str, 0
            => |s, d, len| {
                s.client_id_over = u8::from(len > CLIENT_ID_MAX);
                if len <= CLIENT_ID_MAX {
                    s.client_id_len = len as u8;
                    core::ptr::copy_nonoverlapping(d, s.client_id.as_mut_ptr(), len);
                }
            };

        // How long the broker may take to acknowledge a publish, a CONNECT
        // or a SUBSCRIBE before the front gives up on it: LINK DOWN for the
        // publish front, 504 for a subscription.
        3, ack_timeout_ms, u32, 10000
            => |s, d, len| { s.ack_timeout_ms = p_u32(d, len, 0, 10000); };

        // How long a durable subscription's session outlives its exchange.
        4, session_expiry_s, u32, 3600
            => |s, d, len| { s.session_expiry_s = p_u32(d, len, 0, 3600); };
    }
}

#[inline(always)]
unsafe fn sys(s: &FrontState) -> &'static SyscallTable {
    &*s.syscalls
}

#[inline(always)]
unsafe fn log(s: &FrontState, level: u8, msg: &[u8]) {
    dev_log(sys(s), level, msg.as_ptr(), msg.len());
}

// ── session_out ──────────────────────────────────────────────────────

fn tx_free(s: &FrontState) -> usize {
    TX_CAP - s.tx_len
}

/// Queue the envelope built in `s.env[..len]`. The callers hold room for
/// it (`CONTROL_RESERVE`, or a whole message for a PUBLISH), so a refusal
/// here is a sizing fault and is logged.
unsafe fn tx_push(s: &mut FrontState, len: usize) -> bool {
    if len > tx_free(s) {
        log(s, 1, b"[front] session_out queue full");
        return false;
    }
    front_session::put(&mut s.tx[s.tx_len..], &s.env[..len]);
    s.tx_len += len;
    true
}

/// Place queued envelopes, whole and in order, until the port has no room.
unsafe fn tx_flush(s: &mut FrontState) {
    while s.tx_len >= front_session::WIRE_HDR {
        let msg = front_session::WIRE_HDR + usize::from(u16::from_le_bytes([s.tx[1], s.tx[2]]));
        if (sys(s).channel_write)(s.session_out, s.tx.as_ptr(), msg) <= 0 {
            return;
        }
        s.tx.copy_within(msg..s.tx_len, 0);
        s.tx_len -= msg;
    }
}

unsafe fn push_disconnect(s: &mut FrontState, conn: u16) {
    if let Some(n) = front_session::disconnect(&mut s.env, conn) {
        tx_push(s, n);
    }
}

unsafe fn push_puback(s: &mut FrontState, conn: u16, packet_id: u16) {
    if let Some(n) = front_session::puback(&mut s.env, conn, packet_id) {
        tx_push(s, n);
    }
}

// ── The publish front ────────────────────────────────────────────────

/// Place every record owed on `publish_out`, in order, until the port has
/// no room; the outbox holds the one that did not fit.
unsafe fn flush_publish_answers(s: &mut FrontState) {
    if s.publish_out < 0 {
        return;
    }
    loop {
        if !s.pub_outbox.flush(sys(s), s.publish_out, &s.pub_resp) {
            return;
        }
        let Some(n) = s.desk.next_record(&mut s.pub_resp) else {
            return;
        };
        if !s.pub_outbox.send(sys(s), s.publish_out, &s.pub_resp, n) {
            return;
        }
    }
}

/// After a LINK DOWN, read and drop request records until the LINK UP is
/// away: the requester re-issues every exchange it held open.
unsafe fn discard_publishes(s: &mut FrontState) {
    while s.desk.discarding() && s.publish_in >= 0 {
        let poll = (sys(s).channel_poll)(s.publish_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        if (sys(s).channel_read)(s.publish_in, s.pub_req.as_mut_ptr(), RECORD_MAX) <= 0 {
            return;
        }
    }
}

/// Read publish requests while the session is up, the desk can take one
/// and `session_out` has room for the PUBLISH it may become.
unsafe fn take_publishes(s: &mut FrontState, now: u64) {
    if s.pub_phase != PubPhase::Running {
        return; // backpressure by channel
    }
    while s.desk.can_take() && tx_free(s) >= front_session::WIRE_MAX + CONTROL_RESERVE {
        let poll = (sys(s).channel_poll)(s.publish_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        let n = (sys(s).channel_read)(s.publish_in, s.pub_req.as_mut_ptr(), RECORD_MAX);
        if n <= 0 {
            return;
        }
        let Some(at) = s.desk.accept(&s.pub_req[..n as usize]) else {
            continue;
        };
        let packet_id = next_packet_id(s);
        // One topic is one ordering unit: the target (the ordering key)
        // and BROADCAST both name it, so neither changes the PUBLISH.
        let Some(body) = s.desk.request(at).map(|r| r.body) else {
            continue;
        };
        let topic_len = usize::from(s.topic_len);
        if body.len() > record_room(topic_len) {
            // Past what this front carries: refused, never truncated.
            s.desk.refuse(at, status::TOO_LARGE);
            continue;
        }
        let built = front_session::publish(
            &mut s.env,
            PUBLISH_CONN,
            packet_id,
            &s.topic[..topic_len],
            body,
        );
        let Some(len) = built else {
            s.desk.refuse(at, status::TOO_LARGE);
            continue;
        };
        // Into the window BEFORE the write, so an acknowledgement can never
        // race an unrecorded packet id.
        if !s.desk.dispatch(at, u64::from(packet_id), now) {
            return; // window full (ruled out by can_take)
        }
        tx_push(s, len);
    }
}

fn next_packet_id(s: &mut FrontState) -> u16 {
    s.packet_id = s.packet_id.wrapping_add(1);
    if s.packet_id == 0 {
        s.packet_id = 1;
    }
    s.packet_id
}

/// The publish session is lost: every publish awaiting it is unknowable
/// (LINK DOWN), and a new session is opened after the backoff.
unsafe fn publish_session_lost(s: &mut FrontState, now: u64, why: &[u8]) {
    log(s, 1, why);
    s.desk.link_down();
    if s.pub_phase == PubPhase::Running {
        push_disconnect(s, PUBLISH_CONN);
    }
    s.pub_phase = PubPhase::Backoff;
    s.pub_deadline_ms = now.saturating_add(s.backoff_ms);
    s.backoff_ms = (s.backoff_ms * 2).min(BACKOFF_MAX_MS);
}

/// Advance the publish session: open it, notice an overdue CONNACK or
/// acknowledgement, reopen it after a loss.
unsafe fn drive_publish_session(s: &mut FrontState, now: u64) {
    match s.pub_phase {
        PubPhase::Idle => {}
        PubPhase::Connect => {
            if tx_free(s) < CONTROL_RESERVE {
                return;
            }
            let id_len = usize::from(s.client_id_len);
            let mut cid = [0u8; CLIENT_ID_MAX];
            front_session::put(&mut cid, &s.client_id[..id_len]);
            if let Some(n) =
                front_session::connect(&mut s.env, PUBLISH_CONN, &cid[..id_len], true, 0, 0)
            {
                tx_push(s, n);
                s.pub_phase = PubPhase::WaitConnack;
                s.pub_deadline_ms = now.saturating_add(u64::from(s.ack_timeout_ms));
            }
        }
        PubPhase::WaitConnack => {
            if now >= s.pub_deadline_ms {
                publish_session_lost(s, now, b"[front] publish session: connack overdue");
            }
        }
        PubPhase::Running => {
            let overdue = s
                .desk
                .oldest_sent_ms()
                .is_some_and(|t| now.saturating_sub(t) >= u64::from(s.ack_timeout_ms));
            if overdue {
                publish_session_lost(s, now, b"[front] publish session: acknowledgement overdue");
            }
        }
        PubPhase::Backoff => {
            if now >= s.pub_deadline_ms {
                s.pub_phase = PubPhase::Connect;
            }
        }
    }
}

unsafe fn on_publish_answer(s: &mut FrontState, answer: Answer<'_>, now: u64) {
    match answer {
        Answer::Connack { reason, .. } => {
            if s.pub_phase != PubPhase::WaitConnack {
                return;
            }
            if reason == 0 {
                log(s, 3, b"[front] publish session up");
                s.pub_phase = PubPhase::Running;
                s.backoff_ms = BACKOFF_INIT_MS;
                // (Re)connected and accepting: LINK UP after a LINK DOWN.
                s.desk.link_up();
            } else {
                publish_session_lost(s, now, b"[front] publish session refused");
            }
        }
        Answer::Puback { packet_id, .. } => {
            // Durable acceptance at QoS 1.
            let _ = s.desk.settle(u64::from(packet_id), status::OK);
        }
        Answer::Disconnect { .. } => {
            if s.pub_phase == PubPhase::Running {
                s.pub_phase = PubPhase::WaitConnack; // no DISCONNECT owed back
                publish_session_lost(s, now, b"[front] publish session ended by the broker");
            }
        }
        _ => {}
    }
}

// ── The subscription front ───────────────────────────────────────────

/// Write the CONNECT for the subscription opened in slot `at`.
unsafe fn open_subscription(s: &mut FrontState, at: usize) {
    let conn = s.subs.conn(at);
    let mut cid = [0u8; front_session::CLIENT_ID_MAX];
    let (len, clean, expiry) = match s.subs.session(at) {
        // Durable: the session's name is its client id, and it outlives
        // the exchange by `session_expiry_s`.
        Some(name) => {
            front_session::put(&mut cid, name);
            (name.len(), false, s.session_expiry_s)
        }
        // Live-only: a session of its own that ends with the exchange.
        None => {
            let id_len = usize::from(s.client_id_len);
            front_session::put(&mut cid, &s.client_id[..id_len]);
            cid[id_len] = b'-';
            cid[id_len + 1] = b's';
            const HEX: &[u8; 16] = b"0123456789abcdef";
            for i in 0..4 {
                cid[id_len + 2 + i] = HEX[usize::from((conn >> (12 - 4 * i)) & 0xF)];
            }
            (id_len + 6, true, 0)
        }
    };
    if let Some(n) =
        front_session::connect(&mut s.env, conn, &cid[..len], clean, expiry, SUB_RECV_MAX)
    {
        tx_push(s, n);
        log(s, 3, b"[front] subscription opening");
    }
}

unsafe fn on_subscription_answer(s: &mut FrontState, answer: Answer<'_>, now: u64) {
    match answer {
        Answer::Connack {
            conn,
            session_present,
            reason,
        } => {
            if let Some(at) = s.subs.on_connack(conn, session_present, reason, now) {
                let flen = s.subs.filter(at).len();
                let mut filter = [0u8; subscribe_exchange::FILTER_MAX];
                front_session::put(&mut filter, s.subs.filter(at));
                if let Some(n) = front_session::subscribe(&mut s.env, conn, 1, &filter[..flen]) {
                    tx_push(s, n);
                }
            }
        }
        Answer::Suback { conn, granted, .. } => {
            if let Some(conn) = s.subs.on_suback(conn, granted) {
                push_disconnect(s, conn);
            } else if s.subs.slot_of(conn).is_some() {
                log(s, 3, b"[front] subscription live");
            }
        }
        Answer::Publish {
            conn,
            qos,
            packet_id,
            topic,
            payload,
        } => match s.subs.on_publish(conn, qos, packet_id, topic, payload) {
            Delivered::Held | Delivered::Unknown => {}
            Delivered::Ended { conn, ack } => {
                log(s, 2, b"[front] subscription ended: delivery not carried");
                if let Some(id) = ack {
                    push_puback(s, conn, id);
                }
                push_disconnect(s, conn);
            }
        },
        Answer::Disconnect { conn } => {
            if s.subs.on_disconnect(conn) {
                log(s, 2, b"[front] subscription ended by the broker");
            }
        }
        _ => {}
    }
}

/// Read subscription requests while what each may owe has room.
unsafe fn take_subscriptions(s: &mut FrontState, now: u64) {
    if s.subscribe_in < 0 {
        return;
    }
    while s.subs.can_take() && tx_free(s) >= CONTROL_RESERVE {
        let poll = (sys(s).channel_poll)(s.subscribe_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        let n = (sys(s).channel_read)(s.subscribe_in, s.sub_req.as_mut_ptr(), RECORD_MAX);
        if n <= 0 {
            return;
        }
        match s.subs.accept(&s.sub_req[..n as usize], now) {
            Taken::Open(at) => open_subscription(s, at),
            Taken::Close(conn) => {
                log(s, 3, b"[front] subscription closed by the requester");
                push_disconnect(s, conn);
            }
            Taken::Nothing => {}
        }
    }
}

/// Place records owed on `subscribe_out` until the port has no room, and
/// acknowledge each delivery once its BODY is placed or held for placing.
unsafe fn flush_subscription_records(s: &mut FrontState) {
    if s.subscribe_out < 0 {
        return;
    }
    loop {
        if !s.sub_outbox.flush(sys(s), s.subscribe_out, &s.sub_resp) {
            return;
        }
        let may_ack = tx_free(s) >= CONTROL_RESERVE;
        let Some(e) = s.subs.next_record(&mut s.sub_resp, may_ack) else {
            return;
        };
        let placed = s
            .sub_outbox
            .send(sys(s), s.subscribe_out, &s.sub_resp, e.len);
        // Placed, or held by the outbox, which places it before anything
        // else: either way it is on its way to the subscriber.
        if let Some((conn, packet_id)) = e.ack {
            push_puback(s, conn, packet_id);
        }
        if !placed {
            return;
        }
    }
}

// ── session_in ───────────────────────────────────────────────────────

unsafe fn read_session(s: &mut FrontState, now: u64) {
    if s.session_in < 0 {
        return;
    }
    for _ in 0..SESSION_READS_PER_STEP {
        // Whatever one answer owes (a SUBSCRIBE, a PUBACK, a DISCONNECT)
        // has room before it is read.
        if tx_free(s) < CONTROL_RESERVE {
            return;
        }
        let poll = (sys(s).channel_poll)(s.session_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        let (mt, plen) = wire::channel_read_msg(sys(s), s.session_in, &mut s.rx);
        if mt != wire::MSG_SESSION_RESPONSE || usize::from(plen) > s.rx.len() {
            continue;
        }
        // The answer borrows `rx`, which nothing it reaches writes: the
        // handlers build into `env` and queue into `tx`.
        let payload = core::slice::from_raw_parts(s.rx.as_ptr(), usize::from(plen));
        let Some(answer) = front_session::read_answer(payload) else {
            continue;
        };
        let conn = u16::from_le_bytes([payload[0], payload[1]]);
        if conn == PUBLISH_CONN {
            on_publish_answer(s, answer, now);
        } else {
            on_subscription_answer(s, answer, now);
        }
    }
}

// ── PIC module ABI ───────────────────────────────────────────────────

#[unsafe(no_mangle)]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<FrontState>() as u32
}

/// # Safety
/// `syscalls` is a kernel-owned table valid for the process lifetime.
#[unsafe(no_mangle)]
#[link_section = ".text.module_init"]
pub unsafe extern "C" fn module_init(_syscalls: *const c_void) {}

/// # Safety
/// `state` / `params` / `syscalls` are kernel-owned buffers passed across
/// the module ABI, sized per the manifest contract.
#[unsafe(no_mangle)]
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
        if state_size < core::mem::size_of::<FrontState>() {
            return -6;
        }
        let state_bytes = core::mem::size_of::<FrontState>();
        __aeabi_memclr(state, state_bytes);
        let s = &mut *(state as *mut FrontState);
        s.syscalls = syscalls as *const SyscallTable;
        let sys = &*(syscalls as *const SyscallTable);
        s.session_in = in_chan;
        s.session_out = out_chan;
        s.publish_in = dev_channel_port(sys, 0, 1);
        s.publish_out = dev_channel_port(sys, 1, 1);
        s.subscribe_in = dev_channel_port(sys, 0, 2);
        s.subscribe_out = dev_channel_port(sys, 1, 2);
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        s.desk.reset();
        s.pub_outbox = ExchangeOutbox::new();
        s.sub_outbox = ExchangeOutbox::new();
        s.subs
            .reset(SUB_CONN_FIRST, SUB_CONN_COUNT, u64::from(s.ack_timeout_ms));
        s.backoff_ms = BACKOFF_INIT_MS;

        if s.session_in < 0 || s.session_out < 0 {
            log(
                s,
                1,
                b"[front] refusing to construct: session_in and session_out must both be wired",
            );
            return -10;
        }
        if (s.publish_in < 0) != (s.publish_out < 0)
            || (s.subscribe_in < 0) != (s.subscribe_out < 0)
        {
            log(
                s,
                1,
                b"[front] refusing to construct: a front's request and response ports are wired together",
            );
            return -10;
        }
        if s.topic_over != 0 || s.client_id_over != 0 {
            log(
                s,
                1,
                b"[front] refusing to construct: topic or client_id past its bound",
            );
            return -10;
        }
        if s.client_id_len == 0 {
            const DEFAULT_ID: &[u8] = b"quantum-front";
            front_session::put(&mut s.client_id, DEFAULT_ID);
            s.client_id_len = DEFAULT_ID.len() as u8;
        }
        if s.publish_in >= 0 && !front_session::valid_topic(&s.topic[..usize::from(s.topic_len)]) {
            log(
                s,
                1,
                b"[front] refusing to construct: a wired publish front needs a topic without wildcards",
            );
            return -10;
        }
        s.pub_phase = if s.publish_in >= 0 {
            PubPhase::Connect
        } else {
            PubPhase::Idle
        };
        log(s, 3, b"[front] init");
        0
    }
}

/// # Safety
/// `state` is the buffer a prior `module_new` initialised, exclusively
/// borrowed for the call.
#[unsafe(no_mangle)]
#[link_section = ".text.module_step"]
pub unsafe extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        if state.is_null() {
            return -1;
        }
        let s = &mut *(state as *mut FrontState);
        if s.syscalls.is_null() {
            return -1;
        }
        let now = dev_millis(sys(s));

        // Answers leave whatever the phase, and publishes a LINK DOWN
        // invalidated are dropped while the session is away.
        flush_publish_answers(s);
        flush_subscription_records(s);
        discard_publishes(s);

        read_session(s, now);
        drive_publish_session(s, now);
        take_publishes(s, now);
        take_subscriptions(s, now);
        while s.tx_len + CONTROL_RESERVE <= TX_CAP {
            let Some(conn) = s.subs.expire(now) else {
                break;
            };
            log(
                s,
                2,
                b"[front] subscription refused: the broker did not take it in time",
            );
            push_disconnect(s, conn);
        }

        flush_publish_answers(s);
        flush_subscription_records(s);
        tx_flush(s);
        0
    }
}
