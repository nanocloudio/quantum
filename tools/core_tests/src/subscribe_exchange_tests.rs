//! Tests for the subscription front's held exchanges.
//!
//! The requester's records are written with the contract's own encoders and
//! the provider's are read back with its decoders, so every assertion is
//! about what crosses the ports: which exchange a record names, its status,
//! its flags, and the bytes each BODY carries — and, on the broker side,
//! which session envelopes the provider asks for.

use crate::subscribe_exchange::exchange::{
    abort, flag, parse_response, status, write_abort, write_credit, write_request_head, ExchangeId,
    Record, RequestHead, BODY_MAX, METHOD_GET, METHOD_POST, RECORD_MAX,
};
use crate::subscribe_exchange::{
    framed_len, payload_room, valid_filter, Delivered, Phase, Subscriptions, Taken, CONFLICT,
    CONTENT_TYPE, FILTER_MAX, HOLD_TWO, SESSION_MAX,
};

const SLOTS: usize = 2;
type Subs = Subscriptions<SLOTS, HOLD_TWO>;
const CONN_FIRST: u16 = 0xF001;
const TIMEOUT: u64 = 1000;

fn subs() -> Subs {
    let mut s = Subs::new();
    s.reset(CONN_FIRST, 16, TIMEOUT);
    s
}

fn id(n: u64) -> ExchangeId {
    ExchangeId::from_u64(n)
}

fn get(n: u64, filter: &[u8], headers: &[u8], credit: u32) -> Vec<u8> {
    head(n, METHOD_GET, 0, filter, headers, credit)
}

fn head(n: u64, method: u8, flags: u8, target: &[u8], headers: &[u8], credit: u32) -> Vec<u8> {
    let mut out = vec![0u8; RECORD_MAX];
    let len = write_request_head(
        &RequestHead {
            id: id(n),
            flags,
            method,
            target,
            headers,
            peer: &[],
            resp_credit: credit,
            body: &[],
        },
        &mut out,
    )
    .expect("request head fits a record");
    out.truncate(len);
    out
}

fn credit(n: u64, bytes: u32) -> Vec<u8> {
    let mut out = vec![0u8; 32];
    let len = write_credit(&id(n), bytes, &mut out).unwrap();
    out.truncate(len);
    out
}

fn abort_record(n: u64) -> Vec<u8> {
    let mut out = vec![0u8; 32];
    let len = write_abort(&id(n), abort::PEER_GONE, &mut out).unwrap();
    out.truncate(len);
    out
}

/// A record the provider wrote, decoded.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Head {
        id: u64,
        status: u16,
        flags: u8,
        content_type: Vec<u8>,
        headers: Vec<u8>,
    },
    Body {
        id: u64,
        flags: u8,
        data: Vec<u8>,
        ack: Option<(u16, u16)>,
    },
    Abort {
        id: u64,
        reason: u8,
    },
}

fn drain(s: &mut Subs, may_ack: bool) -> Vec<Seen> {
    let mut seen = Vec::new();
    let mut buf = vec![0u8; RECORD_MAX];
    while let Some(e) = s.next_record(&mut buf, may_ack) {
        seen.push(
            match parse_response(&buf[..e.len]).expect("a well-formed record") {
                Record::Head(h) => {
                    assert!(e.ack.is_none(), "a HEAD owes no acknowledgement");
                    Seen::Head {
                        id: h.id.as_u64(),
                        status: h.status,
                        flags: h.flags,
                        content_type: h.content_type.to_vec(),
                        headers: h.headers.to_vec(),
                    }
                }
                Record::Body { id, flags, data } => Seen::Body {
                    id: id.as_u64(),
                    flags,
                    data: data.to_vec(),
                    ack: e.ack,
                },
                Record::Abort { id, reason } => Seen::Abort {
                    id: id.as_u64(),
                    reason,
                },
                other => panic!("unexpected record {other:?}"),
            },
        );
    }
    seen
}

fn refusal(n: u64, code: u16) -> Seen {
    Seen::Head {
        id: n,
        status: code,
        flags: flag::RAISED,
        content_type: Vec::new(),
        headers: Vec::new(),
    }
}

fn live_head(n: u64, headers: &[u8]) -> Seen {
    Seen::Head {
        id: n,
        status: status::OK,
        flags: flag::HOLD | flag::MORE,
        content_type: CONTENT_TYPE.to_vec(),
        headers: headers.to_vec(),
    }
}

/// The BODY data one message travels as.
fn framed(topic: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut b = u16::try_from(topic.len()).unwrap().to_be_bytes().to_vec();
    b.extend_from_slice(topic);
    b.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    b.extend_from_slice(payload);
    b
}

fn body(n: u64, data: Vec<u8>, ack: Option<(u16, u16)>) -> Seen {
    Seen::Body {
        id: n,
        flags: flag::MORE,
        data,
        ack,
    }
}

/// Open exchange `n` on `filter` and take it through CONNACK and SUBACK.
/// Returns the slot and its session's connection id.
fn subscribed(s: &mut Subs, n: u64, filter: &[u8], headers: &[u8], cr: u32) -> (usize, u16) {
    let Taken::Open(at) = s.accept(&get(n, filter, headers, cr), 0) else {
        panic!("a well-formed subscription opens");
    };
    let conn = s.conn(at);
    assert_eq!(s.on_connack(conn, false, 0, 0), Some(at), "SUBSCRIBE owed");
    assert_eq!(s.on_suback(conn, 1), None);
    assert_eq!(s.phase(at), Phase::Live);
    (at, conn)
}

// ── Round trips ──────────────────────────────────────────────────────

/// GET <filter> opens a session; once the broker has taken the
/// subscription the HEAD is 200 HOLD|MORE, and each message is one BODY
/// with the exact payload, acknowledged to the broker as it leaves.
#[test]
fn a_subscription_is_a_held_exchange_one_body_per_message() {
    let mut s = subs();
    let Taken::Open(at) = s.accept(&get(7, b"orders/+", b"", 4096), 0) else {
        panic!("opens");
    };
    let conn = s.conn(at);
    assert_eq!(conn, CONN_FIRST);
    assert_eq!(s.filter(at), b"orders/+");
    assert_eq!(s.session(at), None, "no session header: live-only");
    assert_eq!(
        drain(&mut s, true),
        vec![],
        "no HEAD before the broker takes it"
    );

    assert_eq!(s.on_connack(conn, false, 0, 0), Some(at));
    assert_eq!(drain(&mut s, true), vec![], "nor after CONNACK alone");
    assert_eq!(s.on_suback(conn, 1), None);
    assert_eq!(drain(&mut s, true), vec![live_head(7, b"")]);

    let payload = b"\x00\x01binary\xff";
    assert_eq!(
        s.on_publish(conn, 1, 11, b"orders/7", payload),
        Delivered::Held
    );
    assert_eq!(
        s.on_publish(conn, 0, 0, b"orders/8", b"two"),
        Delivered::Held
    );
    assert_eq!(
        drain(&mut s, true),
        vec![
            body(7, framed(b"orders/7", payload), Some((conn, 11))),
            body(7, framed(b"orders/8", b"two"), None),
        ],
        "in order; QoS 1 owes its PUBACK, QoS 0 none"
    );
}

/// Credit IS the subscriber's backpressure: no BODY passes the credit the
/// requester granted, and a CREDIT record releases what is held, in order.
#[test]
fn messages_never_pass_the_requesters_credit() {
    let mut s = subs();
    let (at, conn) = subscribed(&mut s, 1, b"t", b"", 0);
    assert_eq!(
        drain(&mut s, true),
        vec![live_head(1, b"")],
        "the HEAD needs no credit"
    );

    let one = framed(b"t", b"first");
    let two = framed(b"t", b"second");
    assert_eq!(s.on_publish(conn, 1, 1, b"t", b"first"), Delivered::Held);
    assert_eq!(s.on_publish(conn, 0, 0, b"t", b"second"), Delivered::Held);
    assert_eq!(drain(&mut s, true), vec![], "no credit, nothing sent");

    let short = u32::try_from(one.len()).unwrap() - 1;
    assert_eq!(s.accept(&credit(1, short), 0), Taken::Nothing);
    assert_eq!(
        drain(&mut s, true),
        vec![],
        "a byte short of the first message"
    );

    assert_eq!(s.accept(&credit(1, 1), 0), Taken::Nothing);
    assert_eq!(drain(&mut s, true), vec![body(1, one, Some((conn, 1)))]);
    assert_eq!(s.credit(at), 0);

    let need = u32::try_from(two.len()).unwrap();
    assert_eq!(s.accept(&credit(1, need + 5), 0), Taken::Nothing);
    assert_eq!(drain(&mut s, true), vec![body(1, two, None)]);
    assert_eq!(s.credit(at), 5, "credit left over carries forward");
    assert_eq!(s.held_bytes(at), 0);
}

/// A `QoS 1` delivery waits while its PUBACK has no room; a `QoS 0` one owes
/// none and passes.
#[test]
fn a_delivery_owing_an_acknowledgement_waits_for_room_to_write_it() {
    let mut s = subs();
    let (_, conn) = subscribed(&mut s, 1, b"t", b"", 1 << 20);
    drain(&mut s, true);
    assert_eq!(s.on_publish(conn, 1, 3, b"t", b"q1"), Delivered::Held);
    assert_eq!(drain(&mut s, false), vec![]);
    assert_eq!(
        drain(&mut s, true),
        vec![body(1, framed(b"t", b"q1"), Some((conn, 3)))]
    );

    let mut s = subs();
    let (_, conn) = subscribed(&mut s, 1, b"t", b"", 1 << 20);
    drain(&mut s, true);
    assert_eq!(s.on_publish(conn, 0, 0, b"t", b"q0"), Delivered::Held);
    assert_eq!(
        drain(&mut s, false),
        vec![body(1, framed(b"t", b"q0"), None)]
    );
}

/// Deliveries that reach a resumed session before its SUBACK (its offline
/// queue) are held and follow the HEAD.
#[test]
fn deliveries_before_the_head_follow_it() {
    let mut s = subs();
    let Taken::Open(at) = s.accept(&get(4, b"t", b"session: s1\r\n", 1 << 20), 0) else {
        panic!("opens");
    };
    let conn = s.conn(at);
    assert_eq!(s.on_connack(conn, true, 0, 0), Some(at));
    assert_eq!(s.on_publish(conn, 1, 9, b"t", b"queued"), Delivered::Held);
    assert_eq!(drain(&mut s, true), vec![], "nothing before the HEAD");
    assert_eq!(s.on_suback(conn, 1), None);
    assert_eq!(
        drain(&mut s, true),
        vec![
            live_head(4, b"session-present: 1\r\n"),
            body(4, framed(b"t", b"queued"), Some((conn, 9))),
        ]
    );
}

// ── Durable sessions ─────────────────────────────────────────────────

/// `session: <name>` names the durable session; the HEAD says whether the
/// broker still held it.
#[test]
fn a_durable_subscription_reports_whether_its_session_was_present() {
    let mut s = subs();
    let Taken::Open(at) = s.accept(&get(1, b"t", b"session: billing-7\r\n", 0), 0) else {
        panic!("opens");
    };
    assert_eq!(s.session(at), Some(&b"billing-7"[..]));
    let conn = s.conn(at);
    s.on_connack(conn, false, 0, 0);
    s.on_suback(conn, 1);
    assert_eq!(
        drain(&mut s, true),
        vec![live_head(1, b"session-present: 0\r\n")]
    );
}

/// A durable session holds one live subscription: a second is 409 while
/// the first is open, and is taken once the first's ABORT has ended it.
#[test]
fn a_live_durable_session_is_not_subscribed_twice() {
    let mut s = subs();
    let (_, conn) = subscribed(&mut s, 1, b"t", b"session: s\r\n", 0);
    drain(&mut s, true);
    assert_eq!(
        s.accept(&get(2, b"t", b"session: s\r\n", 0), 0),
        Taken::Nothing
    );
    assert_eq!(drain(&mut s, true), vec![refusal(2, CONFLICT)]);

    assert_eq!(s.accept(&abort_record(1), 0), Taken::Close(conn));
    assert!(matches!(
        s.accept(&get(3, b"t", b"session: s\r\n", 0), 0),
        Taken::Open(_)
    ));
}

// ── Ending ───────────────────────────────────────────────────────────

/// The requester's ABORT ends the subscription: its session is closed,
/// nothing more is written for it, and later deliveries find no slot.
#[test]
fn abort_ends_the_subscription() {
    let mut s = subs();
    let (_, conn) = subscribed(&mut s, 1, b"t", b"", 0);
    assert_eq!(s.on_publish(conn, 1, 1, b"t", b"held"), Delivered::Held);
    assert_eq!(s.accept(&abort_record(1), 0), Taken::Close(conn));
    assert_eq!(drain(&mut s, true), vec![], "not even the owed HEAD");
    assert_eq!(s.live(), 0);
    assert_eq!(s.on_publish(conn, 1, 2, b"t", b"late"), Delivered::Unknown);
}

/// An ABORT before the broker took the subscription closes the session the
/// CONNECT opened.
#[test]
fn abort_while_opening_closes_the_session() {
    let mut s = subs();
    let Taken::Open(at) = s.accept(&get(1, b"t", b"", 0), 0) else {
        panic!("opens");
    };
    let conn = s.conn(at);
    assert_eq!(s.accept(&abort_record(1), 0), Taken::Close(conn));
    assert_eq!(
        s.on_connack(conn, false, 0, 0),
        None,
        "a late CONNACK finds nothing"
    );
    assert_eq!(drain(&mut s, true), vec![]);
}

/// An ABORT for a subscription that has already ended drops its last
/// record and asks for nothing more.
#[test]
fn abort_after_an_end_drops_the_owed_record() {
    let mut s = subs();
    let (_, conn) = subscribed(&mut s, 1, b"t", b"", 0);
    drain(&mut s, true);
    assert!(s.on_disconnect(conn));
    assert_eq!(s.accept(&abort_record(1), 0), Taken::Nothing);
    assert_eq!(drain(&mut s, true), vec![]);
}

/// The broker ending a live session ends its subscription with ABORT
/// FAILED; before the HEAD it is a refusal, 502.
#[test]
fn the_broker_ending_a_session_ends_its_subscription() {
    let mut s = subs();
    let (_, conn) = subscribed(&mut s, 1, b"t", b"", 0);
    drain(&mut s, true);
    assert!(s.on_disconnect(conn));
    assert_eq!(
        drain(&mut s, true),
        vec![Seen::Abort {
            id: 1,
            reason: abort::FAILED
        }]
    );
    assert_eq!(s.live(), 0);

    let mut s = subs();
    let Taken::Open(at) = s.accept(&get(2, b"t", b"", 0), 0) else {
        panic!("opens");
    };
    assert!(s.on_disconnect(s.conn(at)));
    assert_eq!(drain(&mut s, true), vec![refusal(2, status::BAD_GATEWAY)]);
}

/// A `QoS 0` stream that outruns the held messages ends the subscription
/// `STALLED` rather than dropping messages under it.
#[test]
fn a_stream_outrunning_the_hold_ends_stalled() {
    let mut s = subs();
    let (_, conn) = subscribed(&mut s, 1, b"t", b"", 0);
    drain(&mut s, true);
    let big = vec![7u8; payload_room(1)];
    assert_eq!(s.on_publish(conn, 0, 0, b"t", &big), Delivered::Held);
    assert_eq!(s.on_publish(conn, 0, 0, b"t", &big), Delivered::Held);
    assert_eq!(
        s.on_publish(conn, 0, 0, b"t", b"x"),
        Delivered::Ended { conn, ack: None }
    );
    assert_eq!(
        drain(&mut s, true),
        vec![Seen::Abort {
            id: 1,
            reason: abort::STALLED
        }]
    );
}

/// A message no BODY can carry ends the subscription `TOO_LARGE`, and is
/// acknowledged so a durable session is not handed it again.
#[test]
fn a_message_past_one_body_ends_too_large() {
    let mut s = subs();
    let (_, conn) = subscribed(&mut s, 1, b"t", b"", 1 << 20);
    drain(&mut s, true);
    let fits = vec![1u8; payload_room(1)];
    assert_eq!(framed_len(1, fits.len()), BODY_MAX);
    assert_eq!(s.on_publish(conn, 1, 5, b"t", &fits), Delivered::Held);
    assert_eq!(drain(&mut s, true).len(), 1);
    let over = vec![1u8; payload_room(1) + 1];
    assert_eq!(
        s.on_publish(conn, 1, 6, b"t", &over),
        Delivered::Ended { conn, ack: Some(6) }
    );
    assert_eq!(
        drain(&mut s, true),
        vec![Seen::Abort {
            id: 1,
            reason: abort::TOO_LARGE
        }]
    );
}

// ── Refusals ─────────────────────────────────────────────────────────

/// Every refusal is one terminal HEAD, marked RAISED, and opens nothing.
#[test]
fn requests_a_subscription_cannot_serve_are_refused() {
    let long_filter = vec![b'a'; FILTER_MAX + 1];
    let long_session = format!("session: {}\r\n", "s".repeat(SESSION_MAX + 1));
    let cases: Vec<(Vec<u8>, u16)> = vec![
        (head(1, METHOD_POST, 0, b"t", b"", 0), status::BAD_REQUEST),
        (
            head(2, METHOD_GET, flag::MORE, b"t", b"", 0),
            status::BAD_REQUEST,
        ),
        (get(3, b"", b"", 0), status::BAD_REQUEST),
        (get(4, b"a/#/b", b"", 0), status::BAD_REQUEST),
        (get(5, b"a+", b"", 0), status::BAD_REQUEST),
        (get(6, &long_filter, b"", 0), status::TOO_LARGE),
        (get(7, b"t", long_session.as_bytes(), 0), status::TOO_LARGE),
        (
            get(8, b"t", b"session: has space\r\n", 0),
            status::BAD_REQUEST,
        ),
        (get(9, b"t", b"session:\r\n", 0), status::BAD_REQUEST),
    ];
    for (rec, code) in cases {
        let mut s = subs();
        assert_eq!(s.accept(&rec, 0), Taken::Nothing);
        let n = crate::subscribe_exchange::exchange::record_id(&rec)
            .unwrap()
            .as_u64();
        assert_eq!(drain(&mut s, true), vec![refusal(n, code)], "request {n}");
        assert_eq!(s.live(), 0);
    }
}

#[test]
fn every_slot_taken_is_busy() {
    let mut s = subs();
    for n in 0..SLOTS as u64 {
        assert!(matches!(s.accept(&get(n, b"t", b"", 0), 0), Taken::Open(_)));
    }
    assert_eq!(s.accept(&get(99, b"t", b"", 0), 0), Taken::Nothing);
    assert_eq!(drain(&mut s, true), vec![refusal(99, status::BUSY)]);
}

#[test]
fn a_repeat_head_for_an_open_exchange_is_refused() {
    let mut s = subs();
    assert!(matches!(s.accept(&get(1, b"t", b"", 0), 0), Taken::Open(_)));
    assert_eq!(s.accept(&get(1, b"u", b"", 0), 0), Taken::Nothing);
    assert_eq!(drain(&mut s, true), vec![refusal(1, status::BAD_REQUEST)]);
    assert_eq!(s.live(), 1, "the open one stands");
}

/// The broker refusing the session or the filter is 502, before any HEAD;
/// a refused filter's session is closed.
#[test]
fn the_broker_refusing_is_bad_gateway() {
    let mut s = subs();
    let Taken::Open(at) = s.accept(&get(1, b"t", b"", 0), 0) else {
        panic!("opens");
    };
    assert_eq!(s.on_connack(s.conn(at), false, 0x9D, 0), None);
    assert_eq!(drain(&mut s, true), vec![refusal(1, status::BAD_GATEWAY)]);

    let Taken::Open(at) = s.accept(&get(2, b"t", b"", 0), 0) else {
        panic!("opens");
    };
    let conn = s.conn(at);
    s.on_connack(conn, false, 0, 0);
    assert_eq!(s.on_suback(conn, 0x80), Some(conn));
    assert_eq!(drain(&mut s, true), vec![refusal(2, status::BAD_GATEWAY)]);
}

/// A subscription the broker has not taken by its deadline is refused 504
/// and its session closed; one inside its deadline is left alone.
#[test]
fn a_subscription_not_taken_in_time_is_504() {
    let mut s = subs();
    let Taken::Open(at) = s.accept(&get(1, b"t", b"", 0), 100) else {
        panic!("opens");
    };
    let conn = s.conn(at);
    assert_eq!(s.expire(100 + TIMEOUT - 1), None);
    assert_eq!(
        s.on_connack(conn, false, 0, 500),
        Some(at),
        "the clock restarts at SUBSCRIBE"
    );
    assert_eq!(s.expire(100 + TIMEOUT), None);
    assert_eq!(s.expire(500 + TIMEOUT), Some(conn));
    assert_eq!(s.expire(500 + TIMEOUT), None, "once");
    assert_eq!(drain(&mut s, true), vec![refusal(1, status::TIMEOUT)]);
}

/// A refusal owed to an exchange the requester has aborted is not written.
#[test]
fn an_aborted_exchange_loses_its_owed_refusal() {
    let mut s = subs();
    s.accept(&get(1, b"a/#/b", b"", 0), 0);
    s.accept(&abort_record(1), 0);
    assert_eq!(drain(&mut s, true), vec![]);
}

/// Reading stops while the refusals a read could owe have no room.
#[test]
fn reading_waits_for_room_to_refuse() {
    let mut s = subs();
    for n in 0..SLOTS as u64 {
        assert!(s.can_take());
        s.accept(&get(n, b"", b"", 0), 0);
    }
    assert!(!s.can_take());
    assert_eq!(drain(&mut s, true).len(), SLOTS);
    assert!(s.can_take());
}

// ── Sessions on the broker ───────────────────────────────────────────

/// A new subscription takes the next connection id rather than the one
/// just freed, so an answer in flight for an ended session never reaches
/// its successor.
#[test]
fn connection_ids_rotate_through_the_range() {
    let mut s = subs();
    let (_, first) = subscribed(&mut s, 1, b"t", b"", 0);
    s.accept(&abort_record(1), 0);
    let (_, second) = subscribed(&mut s, 2, b"t", b"", 0);
    assert_ne!(first, second);
    assert_eq!(second, first + 1);
    // Answers are matched to their own session only.
    assert_eq!(
        s.on_publish(first, 0, 0, b"t", b"stale"),
        Delivered::Unknown
    );
}

/// Answers for a session in the wrong phase are ignored.
#[test]
fn out_of_phase_answers_are_ignored() {
    let mut s = subs();
    let (_, conn) = subscribed(&mut s, 1, b"t", b"", 0);
    assert_eq!(s.on_connack(conn, false, 0, 0), None);
    assert_eq!(s.on_suback(conn, 1), None);
    assert_eq!(
        drain(&mut s, true),
        vec![live_head(1, b"")],
        "one HEAD, exactly"
    );
    assert_eq!(drain(&mut s, true), vec![]);
}

#[test]
fn topic_filters_follow_mqtt() {
    for ok in [
        &b"a"[..],
        b"a/b",
        b"/a",
        b"a/",
        b"+",
        b"#",
        b"a/+/c",
        b"a/#",
        b"+/+",
        b"/",
    ] {
        assert!(valid_filter(ok), "{:?}", String::from_utf8_lossy(ok));
    }
    for bad in [
        &b""[..],
        b"a#",
        b"a/#/b",
        b"#/a",
        b"a+",
        b"+a/b",
        b"a/b+",
        b"a\0b",
        b"##",
    ] {
        assert!(!valid_filter(bad), "{:?}", String::from_utf8_lossy(bad));
    }
}
