//! Tests for the broker front's session envelopes.
//!
//! The envelopes are the ones `protocol::mqtt` writes to `codec_in` and
//! `session_processor` answers on `codec_out`; these tests hold each layout
//! byte for byte to the offsets the session processor reads, and read back
//! answers laid out as it writes them.

use crate::front_session::{
    connect, disconnect, pkt, puback, publish, publish_room, read_answer, subscribe, valid_topic,
    Answer, CLIENT_ID_MAX, ENVELOPE_MAX, MSG_SESSION_CONNECT, MSG_SESSION_DISCONNECT,
    MSG_SESSION_PROPOSAL, PROTO_MQTT, PROTO_VER, SESSION_HDR, WIRE_HDR, WIRE_MAX,
};

/// Split a wire message into `(msg_type, conn, pkt_type, flags, body)`,
/// checking the length the header declares against the message.
fn split(msg: &[u8]) -> (u8, u16, u8, u8, &[u8]) {
    let len = usize::from(u16::from_le_bytes([msg[1], msg[2]]));
    assert_eq!(WIRE_HDR + len, msg.len(), "the header declares the message");
    let p = &msg[WIRE_HDR..];
    assert_eq!(p[2], PROTO_MQTT, "every front envelope is MQTT's");
    (
        msg[0],
        u16::from_le_bytes([p[0], p[1]]),
        p[3],
        p[4],
        &p[SESSION_HDR..],
    )
}

/// A session answer as `session_processor` writes one: the payload of a
/// `MSG_SESSION_RESPONSE`.
fn answer(conn: u16, pkt_type: u8, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut out = conn.to_le_bytes().to_vec();
    out.extend_from_slice(&[PROTO_MQTT, pkt_type, flags]);
    out.extend_from_slice(body);
    out
}

/// A delivery body: `[topic_len BE][topic][packet_id BE if qos > 0]
/// [user props][payload]`.
fn delivery(topic: &[u8], packet_id: Option<u16>, props: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut b = u16::try_from(topic.len()).unwrap().to_be_bytes().to_vec();
    b.extend_from_slice(topic);
    if let Some(id) = packet_id {
        b.extend_from_slice(&id.to_be_bytes());
    }
    b.extend_from_slice(props);
    b.extend_from_slice(payload);
    b
}

// ── Envelopes written ────────────────────────────────────────────────

/// CONNECT carries the fields at the offsets the session processor reads:
/// version, clean flag, keep-alive, expiry, receive maximum, client id,
/// then an empty username and no will.
#[test]
fn connect_lays_out_the_codec_connect_envelope() {
    let mut out = [0u8; 256];
    let n = connect(&mut out, 0xF001, b"orders-sub", false, 3600, 1).unwrap();
    let (mt, conn, p, flags, b) = split(&out[..n]);
    assert_eq!(
        (mt, conn, p, flags),
        (MSG_SESSION_CONNECT, 0xF001, pkt::CONNECT, 0)
    );
    assert_eq!(b[0], PROTO_VER);
    assert_eq!(b[1], 0, "a durable session is not clean");
    assert_eq!(u16::from_be_bytes([b[2], b[3]]), 0, "no keep-alive");
    assert_eq!(u32::from_le_bytes([b[4], b[5], b[6], b[7]]), 3600);
    assert_eq!(u16::from_le_bytes([b[8], b[9]]), 1, "receive maximum");
    assert_eq!(usize::from(u16::from_be_bytes([b[10], b[11]])), 10);
    assert_eq!(&b[12..22], b"orders-sub");
    assert_eq!(&b[22..], &[0, 0, 0], "empty username, no will");

    let n = connect(&mut out, 0xF000, b"front", true, 0, 0).unwrap();
    assert_eq!(split(&out[..n]).4[1], 1, "a live session is clean");
}

/// A client id past the bound is refused, not truncated.
#[test]
fn connect_refuses_a_client_id_past_its_bound() {
    let mut out = [0u8; 512];
    let id = [b'a'; CLIENT_ID_MAX + 1];
    assert!(connect(&mut out, 1, &id, true, 0, 0).is_none());
    assert!(connect(&mut out, 1, &id[..CLIENT_ID_MAX], true, 0, 0).is_some());
}

/// A `QoS 1` PUBLISH: packet id, topic, an empty user-property block, then
/// the payload byte for byte.
#[test]
fn publish_lays_out_the_codec_publish_envelope() {
    let mut out = vec![0u8; WIRE_MAX];
    let n = publish(&mut out, 0xF000, 7, b"orders", b"\x00\x01rec").unwrap();
    let (mt, conn, p, flags, b) = split(&out[..n]);
    assert_eq!(
        (mt, conn, p, flags),
        (MSG_SESSION_PROPOSAL, 0xF000, pkt::PUBLISH, 0x02)
    );
    assert_eq!(u16::from_be_bytes([b[0], b[1]]), 7);
    assert_eq!(u16::from_be_bytes([b[2], b[3]]), 6);
    assert_eq!(&b[4..10], b"orders");
    assert_eq!(b[10], 0, "no user properties");
    assert_eq!(&b[11..], b"\x00\x01rec");
}

/// A payload of exactly the room fits one wire message; one byte more is
/// refused.
#[test]
fn publish_holds_to_one_wire_message() {
    let mut out = vec![0u8; WIRE_MAX];
    let topic = b"t/x";
    let room = publish_room(topic.len());
    let n = publish(&mut out, 1, 1, topic, &vec![9u8; room]).unwrap();
    assert_eq!(n, WIRE_MAX, "the room fills the message exactly");
    assert!(publish(&mut out, 1, 1, topic, &vec![9u8; room + 1]).is_none());
    assert_eq!(ENVELOPE_MAX, WIRE_MAX - WIRE_HDR);
}

#[test]
fn subscribe_puback_and_disconnect_lay_out_their_envelopes() {
    let mut out = [0u8; 512];
    let n = subscribe(&mut out, 0xF002, 1, b"a/+/c").unwrap();
    let (mt, conn, p, flags, b) = split(&out[..n]);
    assert_eq!(
        (mt, conn, p, flags),
        (MSG_SESSION_PROPOSAL, 0xF002, pkt::SUBSCRIBE, 0x02)
    );
    assert_eq!(u16::from_be_bytes([b[0], b[1]]), 1);
    assert_eq!(b[2], 1, "QoS 1");
    assert_eq!(u16::from_be_bytes([b[3], b[4]]), 5);
    assert_eq!(&b[5..], b"a/+/c");

    let n = puback(&mut out, 0xF002, 0x0102).unwrap();
    let (mt, _, p, _, b) = split(&out[..n]);
    assert_eq!(
        (mt, p, b),
        (MSG_SESSION_PROPOSAL, pkt::PUBACK, &[1u8, 2][..])
    );

    let n = disconnect(&mut out, 0xF002).unwrap();
    let (mt, conn, p, _, b) = split(&out[..n]);
    assert_eq!(
        (mt, conn, p),
        (MSG_SESSION_DISCONNECT, 0xF002, pkt::DISCONNECT)
    );
    assert!(b.is_empty());
}

#[test]
fn an_envelope_that_does_not_fit_is_refused() {
    let mut small = [0u8; 8];
    assert!(connect(&mut small, 1, b"x", true, 0, 0).is_none());
    assert!(publish(&mut small, 1, 1, b"t", b"p").is_none());
    assert!(subscribe(&mut small, 1, 1, b"t").is_none());
    assert!(puback(&mut small, 1, 1).is_none());
    assert!(disconnect(&mut small[..7], 1).is_none());
}

// ── Answers read ─────────────────────────────────────────────────────

#[test]
fn acknowledgements_are_read_with_their_session() {
    assert_eq!(
        read_answer(&answer(0xF000, pkt::CONNACK, 0, &[1, 0])),
        Some(Answer::Connack {
            conn: 0xF000,
            session_present: true,
            reason: 0
        })
    );
    assert_eq!(
        read_answer(&answer(0xF003, pkt::SUBACK, 0, &[0, 1, 0x80])),
        Some(Answer::Suback {
            conn: 0xF003,
            packet_id: 1,
            granted: 0x80
        })
    );
    assert_eq!(
        read_answer(&answer(0xF000, pkt::PUBACK, 0, &[0x12, 0x34])),
        Some(Answer::Puback {
            conn: 0xF000,
            packet_id: 0x1234
        })
    );
    assert_eq!(
        read_answer(&answer(0xF004, pkt::DISCONNECT, 0, &[0xFF])),
        Some(Answer::Disconnect { conn: 0xF004 })
    );
}

/// A delivery's payload is what follows the user-property block, whatever
/// the block holds; a `QoS 0` delivery carries no packet id.
#[test]
fn deliveries_yield_topic_and_exact_payload() {
    let at_q1 = answer(
        0xF005,
        pkt::PUBLISH,
        0x02,
        &delivery(b"a/b", Some(9), &[0], b"\x00payload\xff"),
    );
    assert_eq!(
        read_answer(&at_q1),
        Some(Answer::Publish {
            conn: 0xF005,
            qos: 1,
            packet_id: 9,
            topic: b"a/b",
            payload: b"\x00payload\xff"
        })
    );
    // Two user properties: k=v and key=value.
    let props = b"\x02\x00\x01k\x00\x01v\x00\x03key\x00\x05value";
    let at_q0 = answer(
        0xF005,
        pkt::PUBLISH,
        0,
        &delivery(b"t", None, props, b"body"),
    );
    assert_eq!(
        read_answer(&at_q0),
        Some(Answer::Publish {
            conn: 0xF005,
            qos: 0,
            packet_id: 0,
            topic: b"t",
            payload: b"body"
        })
    );
}

#[test]
fn malformed_answers_are_refused() {
    assert_eq!(
        read_answer(&[0, 0xF0, 0, 2]),
        None,
        "shorter than the header"
    );
    assert_eq!(read_answer(&answer(1, pkt::CONNACK, 0, &[0])), None);
    assert_eq!(read_answer(&answer(1, pkt::PUBACK, 0, &[1])), None);
    // A topic longer than the body.
    assert_eq!(
        read_answer(&answer(1, pkt::PUBLISH, 0, &[0, 9, b'a'])),
        None
    );
    // A user-property block that runs past the body.
    let cut = delivery(b"t", None, b"\x01\x00\x09k", b"");
    assert_eq!(read_answer(&answer(1, pkt::PUBLISH, 0, &cut)), None);
    assert_eq!(
        read_answer(&answer(1, 13, 0, &[])),
        Some(Answer::Other { conn: 1 }),
        "a packet a front has no use for is named, not misread"
    );
}

#[test]
fn a_publish_topic_names_one_topic() {
    assert!(valid_topic(b"orders"));
    assert!(valid_topic(b"/e2e/orders"));
    assert!(!valid_topic(b""));
    assert!(!valid_topic(b"a/+"));
    assert!(!valid_topic(b"a/#"));
    assert!(!valid_topic(b"a\0b"));
}
