//! The dial authority every Quantum connector composes its
//! `CMD_CONNECT_TO` record from.
//!
//! One record is the whole address: a literal travels as its address,
//! a name travels as a name for the network provider to resolve, and
//! the requester tag rides on the end so a connector claims only its
//! own `MSG_CONNECTED`. The record is what the seven diallers put on
//! the wire, so the assertions below are byte assertions — a field at
//! the wrong offset is a dial to a different peer, not a type error.

use super::authority::net_proto::{
    AF_INET, AF_INET6, AF_NAME, CONNECT_TO_HEAD, CONNECT_TO_MAX, SOCK_TYPE_STREAM,
};
use super::authority::{
    Authority, PORT_AMQP, PORT_KAFKA, PORT_MQTT, PORT_NATS, PORT_QUANTUM_BROKER,
};

/// A requester tag stands in for `dev_requester_tag(sys)`, which is the
/// instance's module index at run time.
const TAG: u8 = 9;

/// Compose one connector's record, or `None` when the authority is
/// refused.
fn dial(text: &str, default_port: u16) -> Option<Vec<u8>> {
    let mut a = Authority::empty();
    a.set(text.as_bytes());
    if !a.adopt(default_port) {
        return None;
    }
    let mut buf = [0u8; CONNECT_TO_MAX];
    let n = a.connect_record(&mut buf, Some(TAG));
    if n == 0 {
        return None;
    }
    Some(buf[..n].to_vec())
}

/// `[sock_type][af][port u16 LE]` — the head every record carries.
fn head(rec: &[u8]) -> (u8, u8, u16) {
    (rec[0], rec[1], u16::from_le_bytes([rec[2], rec[3]]))
}

/// A named target dials `AF_NAME` at `port`, carrying `name` as
/// `[len][name]` and the requester tag last.
fn assert_named(rec: &[u8], name: &str, port: u16) {
    let (sock, af, p) = head(rec);
    assert_eq!(sock, SOCK_TYPE_STREAM);
    assert_eq!(af, AF_NAME);
    assert_eq!(p, port);
    assert_eq!(rec[CONNECT_TO_HEAD] as usize, name.len());
    assert_eq!(
        &rec[CONNECT_TO_HEAD + 1..CONNECT_TO_HEAD + 1 + name.len()],
        name.as_bytes()
    );
    assert_eq!(rec.len(), CONNECT_TO_HEAD + 1 + name.len() + 1);
    assert_eq!(rec[rec.len() - 1], TAG);
}

/// A dotted quad dials `AF_INET` at `port`, carrying four bytes in
/// network order and the requester tag last.
fn assert_literal(rec: &[u8], addr: [u8; 4], port: u16) {
    let (sock, af, p) = head(rec);
    assert_eq!(sock, SOCK_TYPE_STREAM);
    assert_eq!(af, AF_INET);
    assert_eq!(p, port);
    assert_eq!(&rec[CONNECT_TO_HEAD..CONNECT_TO_HEAD + 4], &addr);
    assert_eq!(rec.len(), CONNECT_TO_HEAD + 4 + 1);
    assert_eq!(rec[rec.len() - 1], TAG);
}

// ── One test per dialler, at its own default port ───────────────────────────

/// `mqtt_client` dials MQTT's 1883 when the authority names no port.
#[test]
fn mqtt_client_dials_a_name_and_a_literal() {
    assert_named(
        &dial("broker.internal", PORT_MQTT).unwrap(),
        "broker.internal",
        1883,
    );
    assert_literal(&dial("10.0.0.7", PORT_MQTT).unwrap(), [10, 0, 0, 7], 1883);
}

/// `mqtt_sink` publishes to Quantum's own broker, which advertises 9090.
#[test]
fn mqtt_sink_dials_a_name_and_a_literal() {
    assert_named(
        &dial("broker.internal", PORT_QUANTUM_BROKER).unwrap(),
        "broker.internal",
        9090,
    );
    assert_literal(
        &dial("10.0.0.7", PORT_QUANTUM_BROKER).unwrap(),
        [10, 0, 0, 7],
        9090,
    );
}

/// `kafka_sink` publishes to Quantum's own broker, which advertises 9090.
#[test]
fn kafka_sink_dials_a_name_and_a_literal() {
    assert_named(
        &dial("kafka.internal", PORT_QUANTUM_BROKER).unwrap(),
        "kafka.internal",
        9090,
    );
    assert_literal(
        &dial("10.0.0.8", PORT_QUANTUM_BROKER).unwrap(),
        [10, 0, 0, 8],
        9090,
    );
}

/// `amqp_sink` publishes to Quantum's own broker, which advertises 9090.
#[test]
fn amqp_sink_dials_a_name_and_a_literal() {
    assert_named(
        &dial("rabbit.internal", PORT_QUANTUM_BROKER).unwrap(),
        "rabbit.internal",
        9090,
    );
    assert_literal(
        &dial("10.0.0.9", PORT_QUANTUM_BROKER).unwrap(),
        [10, 0, 0, 9],
        9090,
    );
}

/// `kafka_client` dials Kafka's 9092 when the authority names no port.
#[test]
fn kafka_client_dials_a_name_and_a_literal() {
    assert_named(
        &dial("kafka.internal", PORT_KAFKA).unwrap(),
        "kafka.internal",
        9092,
    );
    assert_literal(
        &dial("127.0.0.1", PORT_KAFKA).unwrap(),
        [127, 0, 0, 1],
        9092,
    );
}

/// `amqp_client` dials AMQP's 5672 when the authority names no port.
#[test]
fn amqp_client_dials_a_name_and_a_literal() {
    assert_named(
        &dial("rabbit.internal", PORT_AMQP).unwrap(),
        "rabbit.internal",
        5672,
    );
    assert_literal(&dial("127.0.0.1", PORT_AMQP).unwrap(), [127, 0, 0, 1], 5672);
}

/// `nats_client` dials NATS' 4222 when the authority names no port.
#[test]
fn nats_client_dials_a_name_and_a_literal() {
    assert_named(
        &dial("nats.internal", PORT_NATS).unwrap(),
        "nats.internal",
        4222,
    );
    assert_literal(&dial("127.0.0.1", PORT_NATS).unwrap(), [127, 0, 0, 1], 4222);
}

// ── What the authority refuses ──────────────────────────────────────────────

/// A port in the authority is the port dialled; the protocol default
/// only fills a gap.
#[test]
fn an_explicit_port_beats_the_default() {
    assert_named(
        &dial("broker.internal:8883", PORT_MQTT).unwrap(),
        "broker.internal",
        8883,
    );
    assert_literal(
        &dial("127.0.0.1:19092", PORT_KAFKA).unwrap(),
        [127, 0, 0, 1],
        19092,
    );
}

/// An authority longer than the connector keeps is DROPPED: a prefix of
/// a name is a different host, so construction is refused instead.
#[test]
fn an_oversized_authority_is_refused_not_truncated() {
    let long = "a".repeat(64) + ".example.com";
    let mut a = Authority::empty();
    a.set(long.as_bytes());
    assert!(a.offered(), "the text was offered and must not be ignored");
    assert!(!a.adopt(PORT_MQTT));
    assert!(!a.is_set());
    assert_eq!(a.text(), b"");
}

/// An absent authority, and text that is not `host[:port]`, both refuse
/// construction rather than dial something else.
#[test]
fn a_missing_or_malformed_authority_is_refused() {
    let mut empty = Authority::empty();
    assert!(!empty.adopt(PORT_NATS));
    assert!(dial("", PORT_NATS).is_none());
    assert!(dial(":4222", PORT_NATS).is_none());
    assert!(dial("host:0", PORT_NATS).is_none());
    assert!(dial("host:not-a-port", PORT_NATS).is_none());
    assert!(
        dial("::1", PORT_NATS).is_none(),
        "a bare v6 literal needs brackets"
    );
}

/// A bracketed IPv6 literal is an address, not a name.
#[test]
fn a_bracketed_v6_literal_dials_by_address() {
    let rec = dial("[::1]:5672", PORT_AMQP).unwrap();
    let (sock, af, port) = head(&rec);
    assert_eq!(sock, SOCK_TYPE_STREAM);
    assert_eq!(af, AF_INET6);
    assert_eq!(port, 5672);
    let mut expect = [0u8; 16];
    expect[15] = 1;
    assert_eq!(&rec[CONNECT_TO_HEAD..CONNECT_TO_HEAD + 16], &expect);
    assert_eq!(rec[rec.len() - 1], TAG);
}
