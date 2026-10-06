//! Tests for the PUBLISH-exchange provider every broker sink answers through.
//!
//! The requester's side is written with the contract's own encoders and the
//! provider's records are read back with its own decoders, so every assertion
//! is about what crosses the two ports: which exchange an answer names, its
//! status, and when a LINK record is written.

use crate::publish_exchange::exchange::{
    abort, flag, kind, link, parse_response, status, write_abort, write_body, write_request_head,
    ExchangeId, Record, RequestHead, ResponseRecord, KEY_MAX, METHOD_GET, METHOD_PUBLISH,
    RECORD_MAX,
};
use crate::publish_exchange::PublishProvider;

/// Body bound for the tests: small, so a body past it is cheap to build.
const BODY: usize = 64;
/// Publishes the broker can hold unacknowledged.
const WINDOW: usize = 4;
type Desk = PublishProvider<2, BODY, WINDOW, { WINDOW + 3 }>;

fn id(n: u64) -> ExchangeId {
    ExchangeId::from_u64(n)
}

/// A request HEAD as a requester writes it.
fn head(n: u64, method: u8, flags: u8, target: &[u8], body: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; RECORD_MAX];
    let len = write_request_head(
        &RequestHead {
            id: id(n),
            flags,
            method,
            target,
            headers: b"content-type: application/octet-stream\r\n",
            peer: &[],
            resp_credit: 0,
            body,
        },
        &mut out,
    )
    .expect("request head fits a record");
    out.truncate(len);
    out
}

/// A PUBLISH carrying its whole record inline.
fn publish(n: u64, key: &[u8], body: &[u8]) -> Vec<u8> {
    head(n, METHOD_PUBLISH, 0, key, body)
}

fn body_record(n: u64, flags: u8, data: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; RECORD_MAX];
    let len = write_body(&id(n), flags, data, &mut out).expect("body fits a record");
    out.truncate(len);
    out
}

fn abort_record(n: u64) -> Vec<u8> {
    let mut out = vec![0u8; RECORD_MAX];
    let len = write_abort(&id(n), abort::PEER_GONE, &mut out).expect("abort fits");
    out.truncate(len);
    out
}

/// What the provider owes, decoded, oldest first.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Answer {
        id: u64,
        status: u16,
        body_len: usize,
    },
    Credit {
        id: u64,
        bytes: u32,
    },
    Link(u8),
}

fn drain(desk: &mut Desk) -> Vec<Seen> {
    let mut seen = Vec::new();
    let mut buf = vec![0u8; RECORD_MAX];
    while let Some(n) = desk.next_record(&mut buf) {
        let rec: ResponseRecord<'_> = parse_response(&buf[..n]).expect("a well-formed record");
        seen.push(match rec {
            Record::Head(h) => {
                assert!(
                    h.content_type.is_empty(),
                    "a sink answer has no content type"
                );
                assert!(h.headers.is_empty(), "a sink answer has no headers");
                assert_eq!(h.flags & flag::MORE, 0, "every sink answer is terminal");
                Seen::Answer {
                    id: h.id.as_u64(),
                    status: h.status,
                    body_len: h.body.len(),
                }
            }
            Record::Credit { id, bytes } => Seen::Credit {
                id: id.as_u64(),
                bytes,
            },
            Record::Link { state } => {
                assert_eq!(buf[0], kind::LINK);
                Seen::Link(state)
            }
            other => panic!("unexpected record {other:?}"),
        });
    }
    seen
}

fn answer(n: u64, status: u16) -> Seen {
    Seen::Answer {
        id: n,
        status,
        body_len: 0,
    }
}

/// Accept a whole inline publish and hand it to the broker under `handle`.
fn send(desk: &mut Desk, n: u64, handle: u64) {
    let at = desk
        .accept(&publish(n, b"order-7", b"record"))
        .expect("a well-formed publish completes");
    assert!(desk.dispatch(at, handle, 0));
}

// ── Round trips ──────────────────────────────────────────────────────

/// The request a requester sends is the record the broker gets, and the
/// broker's acceptance is answered 200 with an empty body under the
/// requester's own exchange id.
#[test]
fn publish_round_trip_answers_200_with_an_empty_body() {
    let mut desk = Desk::new();
    desk.link_up();
    let rec = publish(41, b"orders/7", b"\x02\x01\x05\x00ord-7");
    let at = desk.accept(&rec).expect("complete");
    let r = desk.request(at).expect("collected");
    assert_eq!(r.target, b"orders/7");
    assert_eq!(r.body, b"\x02\x01\x05\x00ord-7");
    assert!(!desk.broadcast(at));
    assert!(desk.dispatch(at, 9, 100));
    assert_eq!(desk.in_flight(), 1);
    assert!(
        drain(&mut desk).is_empty(),
        "nothing is owed before the broker answers"
    );

    assert!(desk.settle(9, status::OK));
    assert_eq!(desk.in_flight(), 0);
    assert_eq!(drain(&mut desk), vec![answer(41, 200)]);
}

/// The id echoed is all 14 bytes, not just the counter half.
#[test]
fn the_whole_exchange_id_is_echoed() {
    let mut desk = Desk::new();
    let mut raw = publish(0, b"k", b"v");
    let full = [7u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
    raw[2..16].copy_from_slice(&full);
    let at = desk.accept(&raw).expect("complete");
    assert!(desk.dispatch(at, 1, 0));
    assert!(desk.settle(1, status::OK));
    let mut buf = vec![0u8; RECORD_MAX];
    let n = desk.next_record(&mut buf).expect("answer owed");
    match parse_response(&buf[..n]) {
        Some(Record::Head(h)) => assert_eq!(h.id, ExchangeId(full)),
        other => panic!("expected a response head, got {other:?}"),
    }
}

/// Answers leave in the order the broker gave its verdicts, each naming its
/// own exchange.
#[test]
fn answers_follow_the_brokers_verdicts() {
    let mut desk = Desk::new();
    send(&mut desk, 1, 101);
    send(&mut desk, 2, 102);
    send(&mut desk, 3, 103);
    assert!(desk.settle(102, status::OK));
    assert!(desk.settle(101, status::BAD_GATEWAY));
    assert!(
        !desk.settle(999, status::OK),
        "an unknown handle answers nothing"
    );
    assert!(desk.settle(103, status::TOO_LARGE));
    assert_eq!(
        drain(&mut desk),
        vec![answer(2, 200), answer(1, 502), answer(3, 413)]
    );
}

/// An AMQP `multiple` confirm answers every publish up to its tag, oldest
/// first, and leaves the later ones waiting.
#[test]
fn a_multiple_confirm_settles_every_earlier_publish_in_order() {
    let mut desk = Desk::new();
    send(&mut desk, 10, 3);
    send(&mut desk, 11, 1);
    send(&mut desk, 12, 2);
    send(&mut desk, 13, 4);
    assert_eq!(desk.settle_through(3, status::OK), 3);
    assert_eq!(desk.in_flight(), 1);
    assert_eq!(
        drain(&mut desk),
        vec![answer(11, 200), answer(12, 200), answer(10, 200)]
    );
}

/// A body that follows its HEAD is granted the rest of the bound at once,
/// and the publish completes with the whole record in order.
#[test]
fn a_body_in_body_records_is_granted_credit_and_collected_whole() {
    let mut desk = Desk::new();
    assert!(desk
        .accept(&head(5, METHOD_PUBLISH, flag::MORE, b"key", b"abc"))
        .is_none());
    let room = u32::try_from(BODY - 3).expect("bound fits");
    assert_eq!(drain(&mut desk), vec![Seen::Credit { id: 5, bytes: room }]);
    assert!(desk.accept(&body_record(5, flag::MORE, b"def")).is_none());
    let at = desk
        .accept(&body_record(5, 0, b"ghi"))
        .expect("the BODY without MORE completes it");
    assert_eq!(desk.request(at).expect("collected").body, b"abcdefghi");
}

/// `BROADCAST` is a publish to every ordering unit, and the provider says so.
#[test]
fn broadcast_is_accepted_and_reported() {
    let mut desk = Desk::new();
    let at = desk
        .accept(&head(6, METHOD_PUBLISH, flag::BROADCAST, b"", b"all"))
        .expect("complete");
    assert!(desk.broadcast(at));
}

/// An empty record is still a record: it is published and answered.
#[test]
fn an_empty_body_is_a_publish() {
    let mut desk = Desk::new();
    let at = desk.accept(&publish(8, b"k", b"")).expect("complete");
    assert!(desk.request(at).expect("collected").body.is_empty());
}

// ── Refusals ─────────────────────────────────────────────────────────

/// Only `PUBLISH` is a request a sink performs.
#[test]
fn a_method_other_than_publish_is_400() {
    let mut desk = Desk::new();
    assert!(desk.accept(&head(1, METHOD_GET, 0, b"/", b"")).is_none());
    assert_eq!(drain(&mut desk), vec![answer(1, 400)]);
}

/// HTTP-only flags name a request no sink performs.
#[test]
fn a_websocket_or_route_flag_is_400() {
    let mut desk = Desk::new();
    assert!(desk
        .accept(&head(2, METHOD_PUBLISH, flag::WEBSOCKET, b"k", b"v"))
        .is_none());
    assert!(desk
        .accept(&head(3, METHOD_PUBLISH, flag::ROUTE_PROXY, b"k", b"v"))
        .is_none());
    assert_eq!(drain(&mut desk), vec![answer(2, 400), answer(3, 400)]);
}

/// An ordering key past `KEY_MAX` is refused, never cut to a different key.
#[test]
fn a_key_past_key_max_is_413() {
    let mut desk = Desk::new();
    let key = vec![b'k'; KEY_MAX + 1];
    assert!(desk.accept(&publish(4, &key, b"v")).is_none());
    assert_eq!(drain(&mut desk), vec![answer(4, 413)]);
    let key = vec![b'k'; KEY_MAX];
    assert!(
        desk.accept(&publish(5, &key, b"v")).is_some(),
        "KEY_MAX itself fits"
    );
}

/// A body past the bound is refused, whether it arrives inline or grows
/// past it in BODY records — never truncated.
#[test]
fn a_body_past_the_bound_is_413() {
    let mut desk = Desk::new();
    assert!(desk.accept(&publish(6, b"k", &[0u8; BODY + 1])).is_none());
    assert_eq!(drain(&mut desk), vec![answer(6, 413)]);

    assert!(desk
        .accept(&head(7, METHOD_PUBLISH, flag::MORE, b"k", &[0u8; BODY - 1]))
        .is_none());
    assert!(desk.accept(&body_record(7, 0, b"xy")).is_none());
    assert_eq!(
        drain(&mut desk),
        vec![Seen::Credit { id: 7, bytes: 1 }, answer(7, 413)]
    );
}

/// More exchanges collecting at once than the provider holds: 503, and the
/// requester may repeat it.
#[test]
fn collecting_past_the_slots_is_503() {
    let mut desk = Desk::new();
    assert!(desk
        .accept(&head(1, METHOD_PUBLISH, flag::MORE, b"k", b""))
        .is_none());
    assert!(desk
        .accept(&head(2, METHOD_PUBLISH, flag::MORE, b"k", b""))
        .is_none());
    assert!(desk
        .accept(&head(3, METHOD_PUBLISH, flag::MORE, b"k", b""))
        .is_none());
    let owed = drain(&mut desk);
    assert_eq!(owed.last(), Some(&answer(3, 503)));
}

/// A refusal of the module's own (the broker's frame budget) answers the
/// exchange and frees its slot.
#[test]
fn a_module_refusal_answers_and_frees_the_slot() {
    let mut desk = Desk::new();
    let at = desk.accept(&publish(9, b"k", b"v")).expect("complete");
    desk.refuse(at, status::TOO_LARGE);
    assert!(desk.request(at).is_none(), "the slot is free");
    assert_eq!(drain(&mut desk), vec![answer(9, 413)]);
}

/// A record that is not a record names no exchange: nothing to answer.
#[test]
fn a_malformed_record_is_dropped_unanswered() {
    let mut desk = Desk::new();
    assert!(desk.accept(&[kind::HEAD, 0, 1]).is_none());
    assert!(desk.accept(&body_record(77, 0, b"stray")).is_none());
    assert!(drain(&mut desk).is_empty());
}

// ── Backpressure ─────────────────────────────────────────────────────

/// A full window stops the module reading: requests wait in the channel.
#[test]
fn a_full_window_stops_intake() {
    let mut desk = Desk::new();
    for n in 0..4 {
        assert!(desk.can_take());
        send(&mut desk, n, n);
    }
    assert!(!desk.can_take());
    assert!(desk.settle(0, status::OK));
    assert!(desk.can_take());
}

/// Answers the port has not taken yet hold intake too: the queue always
/// has room for everything a read could owe.
#[test]
fn owed_answers_hold_intake_until_placed() {
    let mut desk = Desk::new();
    for n in 0..4 {
        send(&mut desk, n, n);
    }
    for n in 0..4 {
        assert!(desk.settle(n, status::OK));
    }
    assert_eq!(desk.owed(), 4);
    assert!(desk.can_take(), "four owed and one read fits the queue");
    send(&mut desk, 4, 4);
    assert!(!desk.can_take(), "another read could owe past the queue");
    let mut buf = vec![0u8; RECORD_MAX];
    assert!(desk.next_record(&mut buf).is_some());
    assert!(desk.can_take());
}

// ── Requester ABORT ──────────────────────────────────────────────────

/// An exchange the requester aborted is never answered, even when the
/// broker accepts the record afterwards.
#[test]
fn an_aborted_exchange_is_never_answered() {
    let mut desk = Desk::new();
    send(&mut desk, 1, 11);
    send(&mut desk, 2, 12);
    assert!(desk.accept(&abort_record(1)).is_none());
    assert!(
        desk.settle(11, status::OK),
        "the broker's verdict still frees the slot"
    );
    assert!(desk.settle(12, status::OK));
    assert_eq!(drain(&mut desk), vec![answer(2, 200)]);
    assert_eq!(desk.in_flight(), 0);
}

/// An answer owed but not yet placed is withdrawn by an abort.
#[test]
fn an_abort_withdraws_an_owed_answer() {
    let mut desk = Desk::new();
    send(&mut desk, 1, 11);
    assert!(desk.settle(11, status::OK));
    assert!(desk.accept(&abort_record(1)).is_none());
    assert!(drain(&mut desk).is_empty());
}

/// An abort mid-body frees the collecting slot and withdraws its credit.
#[test]
fn an_abort_mid_body_frees_the_slot() {
    let mut desk = Desk::new();
    assert!(desk
        .accept(&head(3, METHOD_PUBLISH, flag::MORE, b"k", b"a"))
        .is_none());
    assert!(desk.accept(&abort_record(3)).is_none());
    assert!(drain(&mut desk).is_empty());
    assert!(
        desk.accept(&body_record(3, 0, b"b")).is_none(),
        "nothing left to extend"
    );
    assert!(drain(&mut desk).is_empty());
}

// ── LINK ─────────────────────────────────────────────────────────────

/// LINK reports a change: the first connection writes nothing, so requests
/// that waited in the channel for it are not re-issued as well.
#[test]
fn the_first_connection_writes_no_link() {
    let mut desk = Desk::new();
    desk.link_up();
    desk.link_up();
    assert!(drain(&mut desk).is_empty());
    // A broker that never came up invalidates nothing.
    let mut never = Desk::new();
    never.link_down();
    assert!(drain(&mut never).is_empty());
    assert!(!never.discarding());
}

/// Losing the broker writes DOWN, forgets the window (those exchanges are
/// the requester's to re-issue), drops request records until the UP is
/// away, and the reconnect writes UP.
#[test]
fn link_down_invalidates_the_window_and_up_reopens_intake() {
    let mut desk = Desk::new();
    desk.link_up();
    send(&mut desk, 1, 101);
    send(&mut desk, 2, 102);
    assert!(desk.settle(101, status::OK));

    desk.link_down();
    assert_eq!(desk.in_flight(), 0);
    assert!(desk.discarding());
    assert!(!desk.can_take());
    // A PUBACK for the old connection's packet id answers nothing.
    assert!(!desk.settle(102, status::OK));
    // A request read while the link is away is dropped unanswered.
    assert!(desk.accept(&publish(3, b"k", b"v")).is_none());

    desk.link_up();
    assert!(
        desk.discarding(),
        "dropping continues until the UP is placed"
    );
    assert_eq!(
        drain(&mut desk),
        vec![answer(1, 200), Seen::Link(link::DOWN), Seen::Link(link::UP)],
        "the answer given before the loss still leaves ahead of DOWN"
    );
    assert!(!desk.discarding());
    assert!(desk.can_take());
    // The requester re-issues exchange 2; it is published afresh.
    send(&mut desk, 2, 1);
    assert!(desk.settle(1, status::OK));
    assert_eq!(drain(&mut desk), vec![answer(2, 200)]);
}

/// A flap whose UP the requester has not yet read collapses: the second
/// DOWN withdraws the UP, so the requester sees one DOWN, then one UP.
#[test]
fn a_flap_before_the_up_is_placed_collapses() {
    let mut desk = Desk::new();
    desk.link_up();
    desk.link_down();
    desk.link_up();
    desk.link_down();
    desk.link_down();
    assert_eq!(drain(&mut desk), vec![Seen::Link(link::DOWN)]);
    assert!(desk.discarding());
    desk.link_up();
    assert_eq!(drain(&mut desk), vec![Seen::Link(link::UP)]);
    assert!(!desk.discarding());
}

/// Requests being collected when the link drops are dropped with the
/// credit owed them: the requester re-issues them after UP.
#[test]
fn link_down_drops_collecting_requests_and_their_credit() {
    let mut desk = Desk::new();
    desk.link_up();
    assert!(desk
        .accept(&head(4, METHOD_PUBLISH, flag::MORE, b"k", b"a"))
        .is_none());
    desk.link_down();
    desk.link_up();
    assert_eq!(
        drain(&mut desk),
        vec![Seen::Link(link::DOWN), Seen::Link(link::UP)]
    );
    assert!(desk.accept(&body_record(4, 0, b"b")).is_none());
    assert!(
        drain(&mut desk).is_empty(),
        "a stray body names nothing held"
    );
}

/// The longest-waiting publish is what a reply timeout is measured from.
#[test]
fn the_oldest_publish_is_tracked_for_timeouts() {
    let mut desk = Desk::new();
    assert_eq!(desk.oldest_sent_ms(), None);
    let at = desk.accept(&publish(1, b"k", b"v")).expect("complete");
    assert!(desk.dispatch(at, 1, 500));
    let at = desk.accept(&publish(2, b"k", b"v")).expect("complete");
    assert!(desk.dispatch(at, 2, 200));
    assert_eq!(desk.oldest_sent_ms(), Some(200));
    assert!(desk.settle(2, status::OK));
    assert_eq!(desk.oldest_sent_ms(), Some(500));
}

/// `reset` returns a used provider to its first state.
#[test]
fn reset_forgets_everything() {
    let mut desk = Desk::new();
    desk.link_up();
    send(&mut desk, 1, 1);
    desk.link_down();
    desk.reset();
    assert_eq!(desk.in_flight(), 0);
    assert_eq!(desk.owed(), 0);
    assert!(!desk.discarding());
    assert!(desk.can_take());
    desk.link_down();
    assert!(drain(&mut desk).is_empty(), "a reset link is unknown again");
}
