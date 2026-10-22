//! The in-graph front's side of the codec envelope.
//!
//! `broker_front` reaches the broker the way a transport codec does: it
//! writes session envelopes to `session_processor` and reads its answers,
//! with no MQTT bytes and no socket in between. The envelope is the one
//! `protocol::mqtt` writes to `codec_in` and reads from `codec_out` (see the
//! codec's per-packet builder and `on_response`); this file builds and reads
//! the subset a front session speaks, so the layouts have one definition on
//! this side of the seam.
//!
//! Every envelope is a wire message `[msg_type u8][len u16 LE][payload]`
//! whose payload is `[conn_id u16 LE][proto u8][pkt_type u8][flags u8][body]`:
//!
//! | Packet      | msg_type                | body |
//! |-------------|-------------------------|------|
//! | CONNECT     | `MSG_SESSION_CONNECT`   | `[proto_ver][clean_start][keep_alive u16 BE][session_expiry u32 LE][recv_max u16 LE][cid_len u16 BE][cid][un_len u16 BE = 0][will_flag = 0]` |
//! | PUBLISH     | `MSG_SESSION_PROPOSAL`  | `[packet_id u16 BE][topic_len u16 BE][topic][user_props_count = 0][payload]` |
//! | SUBSCRIBE   | `MSG_SESSION_PROPOSAL`  | `[packet_id u16 BE][req_qos][filter_len u16 BE][filter]` |
//! | PUBACK      | `MSG_SESSION_PROPOSAL`  | `[packet_id u16 BE]` |
//! | DISCONNECT  | `MSG_SESSION_DISCONNECT`| empty |
//!
//! Answers arrive as `MSG_SESSION_RESPONSE` with the same header: CONNACK
//! `[session_present][reason]`, SUBACK `[packet_id u16 BE][granted]`, PUBACK
//! `[packet_id u16 BE]`, a server DISCONNECT `[reason]`, and a delivered
//! PUBLISH `[topic_len u16 BE][topic][packet_id u16 BE if qos > 0]
//! [user_props_block][payload]`, the user-property block being
//! `[count u8]` then per pair `[key_len u16 BE][key][val_len u16 BE][val]`.

#![allow(
    dead_code,
    reason = "shared envelope definitions; the module and the host tests each use a subset"
)]

/// Wire envelope header: `[msg_type u8][len u16 LE]`.
pub const WIRE_HDR: usize = 3;
/// Session header after the wire header: `[conn_id u16 LE][proto][pkt_type][flags]`.
pub const SESSION_HDR: usize = 5;
/// The largest wire message a channel record carries (the SDK's
/// `CHANNEL_BUFFER_SIZE`, which `wire::channel_write_msg` holds to).
pub const WIRE_MAX: usize = 8192;
/// The largest session envelope payload: what a wire message leaves after
/// its header.
pub const ENVELOPE_MAX: usize = WIRE_MAX - WIRE_HDR;

/// `MSG_SESSION_PROPOSAL` / `_RESPONSE` / `_CONNECT` / `_DISCONNECT`, as
/// `modules/common/wire.rs` numbers them.
pub const MSG_SESSION_PROPOSAL: u8 = 0x98;
pub const MSG_SESSION_RESPONSE: u8 = 0x99;
pub const MSG_SESSION_CONNECT: u8 = 0x9A;
pub const MSG_SESSION_DISCONNECT: u8 = 0x9B;

/// The MQTT protocol tag in the session header.
pub const PROTO_MQTT: u8 = 0;

/// MQTT control packet types, as the envelope's `pkt_type` carries them.
pub mod pkt {
    pub const CONNECT: u8 = 1;
    pub const CONNACK: u8 = 2;
    pub const PUBLISH: u8 = 3;
    pub const PUBACK: u8 = 4;
    pub const SUBSCRIBE: u8 = 8;
    pub const SUBACK: u8 = 9;
    pub const DISCONNECT: u8 = 14;
}

/// The session protocol version a front session declares: MQTT 5, so a
/// durable session carries its own expiry interval.
pub const PROTO_VER: u8 = 5;
/// PUBLISH flags for QoS 1 (bits 1-2 = 01).
pub const FLAGS_QOS1: u8 = 0x02;
/// SUBSCRIBE carries the fixed-header flags MQTT requires of it.
pub const FLAGS_SUBSCRIBE: u8 = 0x02;
/// SUBACK `granted` value that refuses the filter.
pub const SUBACK_FAILURE: u8 = 0x80;

/// The longest client id a front session presents.
pub const CLIENT_ID_MAX: usize = 64;

/// Fixed part of a PUBLISH envelope: session header, packet id, topic
/// length and the empty user-property block.
pub const PUBLISH_FIXED: usize = SESSION_HDR + 2 + 2 + 1;

/// The largest payload one PUBLISH envelope carries on `topic_len` bytes of
/// topic.
pub const fn publish_room(topic_len: usize) -> usize {
    ENVELOPE_MAX.saturating_sub(PUBLISH_FIXED + topic_len)
}

/// Copy `src` to the head of `dst`. The callers size `dst` first; this
/// holds to the shorter of the two rather than linking a panic path.
pub fn put(dst: &mut [u8], src: &[u8]) {
    for (d, s) in dst.iter_mut().zip(src) {
        *d = *s;
    }
}

fn header(out: &mut [u8], msg_type: u8, payload_len: usize, conn: u16, pkt_type: u8, flags: u8) {
    out[0] = msg_type;
    out[1..3].copy_from_slice(&(payload_len as u16).to_le_bytes());
    out[3..5].copy_from_slice(&conn.to_le_bytes());
    out[5] = PROTO_MQTT;
    out[6] = pkt_type;
    out[7] = flags;
}

const AT_BODY: usize = WIRE_HDR + SESSION_HDR;

/// A CONNECT for session `conn`, presenting `client_id`. `clean` starts the
/// session afresh and ends it with the connection; otherwise it persists
/// `expiry_s` seconds past a disconnect. `recv_max` caps the QoS 1
/// deliveries the broker holds unacknowledged on it (0: no cap). Returns the
/// wire message length, `None` when it does not fit `out`.
pub fn connect(
    out: &mut [u8],
    conn: u16,
    client_id: &[u8],
    clean: bool,
    expiry_s: u32,
    recv_max: u16,
) -> Option<usize> {
    if client_id.len() > CLIENT_ID_MAX {
        return None;
    }
    let body = 1 + 1 + 2 + 4 + 2 + 2 + client_id.len() + 2 + 1;
    let total = AT_BODY + body;
    if total > out.len() {
        return None;
    }
    header(
        out,
        MSG_SESSION_CONNECT,
        SESSION_HDR + body,
        conn,
        pkt::CONNECT,
        0,
    );
    let b = &mut out[AT_BODY..total];
    b[0] = PROTO_VER;
    b[1] = u8::from(clean);
    // No keep-alive: the session lives exactly as long as the front holds
    // it, and the front ends it explicitly.
    b[2..4].copy_from_slice(&0u16.to_be_bytes());
    b[4..8].copy_from_slice(&expiry_s.to_le_bytes());
    b[8..10].copy_from_slice(&recv_max.to_le_bytes());
    b[10..12].copy_from_slice(&(client_id.len() as u16).to_be_bytes());
    let mut at = 12;
    put(&mut b[at..], client_id);
    at += client_id.len();
    b[at..at + 2].copy_from_slice(&0u16.to_be_bytes()); // no username
    b[at + 2] = 0; // no will
    Some(total)
}

/// A QoS 1 PUBLISH of `payload` on `topic` under `packet_id`. `None` when it
/// does not fit `out` or one wire message.
pub fn publish(
    out: &mut [u8],
    conn: u16,
    packet_id: u16,
    topic: &[u8],
    payload: &[u8],
) -> Option<usize> {
    if topic.len() > u16::MAX as usize || payload.len() > publish_room(topic.len()) {
        return None;
    }
    let body = 2 + 2 + topic.len() + 1 + payload.len();
    let total = AT_BODY + body;
    if total > out.len() {
        return None;
    }
    header(
        out,
        MSG_SESSION_PROPOSAL,
        SESSION_HDR + body,
        conn,
        pkt::PUBLISH,
        FLAGS_QOS1,
    );
    let b = &mut out[AT_BODY..total];
    b[0..2].copy_from_slice(&packet_id.to_be_bytes());
    b[2..4].copy_from_slice(&(topic.len() as u16).to_be_bytes());
    let mut at = 4;
    put(&mut b[at..], topic);
    at += topic.len();
    b[at] = 0; // no user properties
    at += 1;
    put(&mut b[at..], payload);
    Some(total)
}

/// A SUBSCRIBE to `filter` at QoS 1 under `packet_id`.
pub fn subscribe(out: &mut [u8], conn: u16, packet_id: u16, filter: &[u8]) -> Option<usize> {
    let body = 2 + 1 + 2 + filter.len();
    let total = AT_BODY + body;
    if total > out.len() || total > WIRE_MAX {
        return None;
    }
    header(
        out,
        MSG_SESSION_PROPOSAL,
        SESSION_HDR + body,
        conn,
        pkt::SUBSCRIBE,
        FLAGS_SUBSCRIBE,
    );
    let b = &mut out[AT_BODY..total];
    b[0..2].copy_from_slice(&packet_id.to_be_bytes());
    b[2] = 1;
    b[3..5].copy_from_slice(&(filter.len() as u16).to_be_bytes());
    put(&mut b[5..], filter);
    Some(total)
}

/// A PUBACK of a delivery the front has passed on.
pub fn puback(out: &mut [u8], conn: u16, packet_id: u16) -> Option<usize> {
    let total = AT_BODY + 2;
    if total > out.len() {
        return None;
    }
    header(
        out,
        MSG_SESSION_PROPOSAL,
        SESSION_HDR + 2,
        conn,
        pkt::PUBACK,
        0,
    );
    out[AT_BODY..total].copy_from_slice(&packet_id.to_be_bytes());
    Some(total)
}

/// A DISCONNECT ending session `conn`: a clean session ends with it, a
/// durable one is kept for its expiry interval.
pub fn disconnect(out: &mut [u8], conn: u16) -> Option<usize> {
    if AT_BODY > out.len() {
        return None;
    }
    header(
        out,
        MSG_SESSION_DISCONNECT,
        SESSION_HDR,
        conn,
        pkt::DISCONNECT,
        0,
    );
    Some(AT_BODY)
}

/// The largest control envelope a front session writes: a CONNECT with the
/// longest client id. A SUBSCRIBE is bounded by its filter separately.
pub const CONTROL_MAX: usize = AT_BODY + 15 + CLIENT_ID_MAX;

/// One answer the broker wrote for a front session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer<'a> {
    Connack {
        conn: u16,
        session_present: bool,
        reason: u8,
    },
    Suback {
        conn: u16,
        packet_id: u16,
        granted: u8,
    },
    Puback {
        conn: u16,
        packet_id: u16,
    },
    /// A delivery. `packet_id` is 0 at QoS 0, which owes no PUBACK.
    Publish {
        conn: u16,
        qos: u8,
        packet_id: u16,
        topic: &'a [u8],
        payload: &'a [u8],
    },
    /// The broker ended the session (it is served elsewhere).
    Disconnect {
        conn: u16,
    },
    /// Anything else the session protocol can say; a front session has no
    /// use for it.
    Other {
        conn: u16,
    },
}

/// Read one `MSG_SESSION_RESPONSE` payload (the wire header already
/// stripped). `None` when it is not a well-formed answer.
pub fn read_answer(payload: &[u8]) -> Option<Answer<'_>> {
    if payload.len() < SESSION_HDR {
        return None;
    }
    let conn = u16::from_le_bytes([payload[0], payload[1]]);
    let pkt_type = payload[3];
    let flags = payload[4];
    let body = &payload[SESSION_HDR..];
    let be16 = |b: &[u8], at: usize| -> Option<u16> {
        Some(u16::from_be_bytes([*b.get(at)?, *b.get(at + 1)?]))
    };
    Some(match pkt_type {
        pkt::CONNACK => Answer::Connack {
            conn,
            session_present: *body.first()? & 1 != 0,
            reason: *body.get(1)?,
        },
        pkt::SUBACK => Answer::Suback {
            conn,
            packet_id: be16(body, 0)?,
            granted: *body.get(2)?,
        },
        pkt::PUBACK => Answer::Puback {
            conn,
            packet_id: be16(body, 0)?,
        },
        pkt::DISCONNECT => Answer::Disconnect { conn },
        pkt::PUBLISH => {
            let qos = (flags >> 1) & 0x03;
            let topic_len = usize::from(be16(body, 0)?);
            let topic = body.get(2..2 + topic_len)?;
            let mut at = 2 + topic_len;
            let packet_id = if qos > 0 {
                let id = be16(body, at)?;
                at += 2;
                id
            } else {
                0
            };
            at += user_props_len(body.get(at..)?)?;
            Answer::Publish {
                conn,
                qos,
                packet_id,
                topic,
                payload: body.get(at..)?,
            }
        }
        _ => Answer::Other { conn },
    })
}

/// Length of the user-property block at the head of `b`.
fn user_props_len(b: &[u8]) -> Option<usize> {
    let count = *b.first()?;
    let mut at = 1usize;
    for _ in 0..count {
        for _ in 0..2 {
            let n = usize::from(u16::from_be_bytes([*b.get(at)?, *b.get(at + 1)?]));
            at = at.checked_add(2 + n)?;
            if at > b.len() {
                return None;
            }
        }
    }
    Some(at)
}

/// Whether `topic` is a topic a PUBLISH may name: non-empty, no wildcard,
/// no NUL.
pub fn valid_topic(topic: &[u8]) -> bool {
    !topic.is_empty() && !topic.iter().any(|&c| c == b'+' || c == b'#' || c == 0)
}
