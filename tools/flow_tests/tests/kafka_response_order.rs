//! The order a Kafka connection's responses reach the client in:
//! `protocol`'s Kafka codec (`modules/app/protocol/kafka.rs`) and its
//! order core (`modules/common/cores/kafka_order_core.rs`) mounted
//! verbatim against a mock `SyscallTable`, the anchor it forwards through
//! stubbed, and `session_processor` played by the test.
//!
//! The defect these pin: a durability-gated Produce was answered after a
//! later request on the same connection that was answered at once — a
//! Fetch or ListOffsets by `session_processor`, a Metadata or ApiVersions
//! by the codec itself — so the client read a correlation id that was not
//! its oldest request's (Java's `NetworkClient` fails the connection on
//! it; librdkafka calls it a protocol error). And what the fix must not
//! break: a request the codec could not take was dropped and left
//! unanswered; one never answered must close the connection, not hold
//! every later answer; a pipelining client must be pushed back on, not
//! lose bytes; an acks=0 Produce has no answer to wait for.
//!
//! What these pin:
//!
//! - answers leave in request order, whichever component answered each
//!   and in whatever order they were made;
//! - an acks=0 Produce takes no place in the order;
//! - a request marked never to be answered (`wire::KAFKA_API_CLOSE`), an
//!   API the broker does not serve, a malformed request and an overdue
//!   answer each close the connection in their turn, behind the answers
//!   ahead and before any after;
//! - at most `kafka_order::DEPTH` requests are in flight per connection;
//!   the rest wait in the reassembly buffer, and the raw record that
//!   would overflow it is held, with the read stopped, until there is
//!   room — nothing is dropped, nothing is forwarded twice;
//! - a request the session edge refuses is offered again, not dropped;
//!   a response the frame edge refuses is offered again unchanged;
//! - a closed connection's late answers are dropped and its slots freed;
//! - `session_processor` answers every Kafka request it is handed, or
//!   says it never will, and its expiry sweep answers what it expires.

#![allow(
    dead_code,
    reason = "host test crate; the mounted codec and SDK surface are a superset of what the tests touch"
)]
#![allow(
    unsafe_code,
    reason = "drives the mounted codec's unsafe entry points against a mock SyscallTable; the unsafety is confined to the module ABI boundary"
)]
#![allow(
    unused_imports,
    reason = "the fluxor SDK runtime is include!'d wholesale and the tests consume only a subset"
)]
#![allow(
    clippy::all,
    clippy::pedantic,
    reason = "compiles no_std PIC module sources verbatim into a host test crate; those files are lint-scoped to the module build"
)]
#![expect(
    clippy::expect_used,
    clippy::panic,
    reason = "a sequence that finds the codec or the module source not as it must be fails the test, which is what expect and panic say"
)]
#![allow(
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    reason = "the mounted SDK sources carry their own #[allow] discipline; this host crate cannot rewrite them to #[expect]"
)]

#[rustfmt::skip]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");

#[rustfmt::skip]
#[path = "../../../modules/common/wire.rs"]
mod wire;
#[rustfmt::skip]
#[path = "../../../modules/common/cores/kafka_metadata_core.rs"]
mod kafka_metadata;
#[rustfmt::skip]
#[path = "../../../modules/common/cores/kafka_order_core.rs"]
mod kafka_order;
#[rustfmt::skip]
#[path = "../../../modules/app/protocol/kafka.rs"]
mod kafka;

/// The anchor the codec forwards every decoded envelope through, stubbed:
/// it records what it is handed, or refuses while told to.
mod anchor {
    use super::abi::SyscallTable;

    // Which calls are the coordinator's is the anchor core's rule, mounted
    // as `protocol` mounts it.
    include!("../../../modules/common/cores/mqtt_anchor_core.rs");

    pub struct Anchor;

    pub unsafe fn forward_env(
        _a: &mut Anchor,
        _sys: &SyscallTable,
        _now: u64,
        mt: u8,
        payload: &[u8],
    ) -> bool {
        let mut m = super::mock();
        if m.refuse_forward {
            return false;
        }
        m.forwarded.push((mt, payload.to_vec()));
        true
    }

    /// A coordinator call: recorded in order beside the bound worker's
    /// envelopes, and marked.
    pub unsafe fn forward_coord(
        _a: &mut Anchor,
        _sys: &SyscallTable,
        _now: u64,
        mt: u8,
        payload: &[u8],
    ) -> bool {
        let mut m = super::mock();
        if m.refuse_forward {
            return false;
        }
        m.forwarded.push((mt, payload.to_vec()));
        m.coord_forwarded.push(payload.to_vec());
        true
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, MutexGuard};

const KAFKA: &str = include_str!("../../../modules/app/protocol/kafka.rs");
const SESSION_PROCESSOR: &str = include_str!("../../../modules/app/session_processor/mod.rs");

// ── Mock kernel ──────────────────────────────────────────────────────────

#[derive(Default)]
struct Mock {
    fifos: HashMap<i32, Vec<u8>>,
    closed: HashSet<i32>,
    /// Writes allowed on a channel before it refuses (none: open).
    budget: HashMap<i32, usize>,
    now_ms: u64,
    refuse_forward: bool,
    forwarded: Vec<(u8, Vec<u8>)>,
    /// The forwarded envelopes that went to the coordinator.
    coord_forwarded: Vec<Vec<u8>>,
}

static MOCK: LazyLock<Mutex<Mock>> = LazyLock::new(|| Mutex::new(Mock::default()));
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn mock_reset() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *mock() = Mock::default();
    guard
}

fn mock() -> MutexGuard<'static, Mock> {
    MOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

unsafe extern "C" fn mock_channel_read(_h: i32, _buf: *mut u8, _len: usize) -> i32 {
    0
}

unsafe extern "C" fn mock_channel_write(handle: i32, data: *const u8, len: usize) -> i32 {
    let mut m = mock();
    if m.closed.contains(&handle) {
        return abi::errno::EAGAIN;
    }
    if let Some(b) = m.budget.get_mut(&handle) {
        if *b == 0 {
            return abi::errno::EAGAIN;
        }
        *b -= 1;
    }
    let fifo = m.fifos.entry(handle).or_default();
    for i in 0..len {
        // SAFETY: the caller's buffer holds `len` bytes.
        fifo.push(unsafe { *data.add(i) });
    }
    len as i32
}

unsafe extern "C" fn mock_channel_discard(_h: i32, len: usize) -> i32 {
    len as i32
}
unsafe extern "C" fn mock_channel_poll(_h: i32, events: u32) -> i32 {
    (0x02 & events) as i32
}
unsafe extern "C" fn mock_channel_peek(_h: i32, _buf: *mut u8, _len: usize) -> i32 {
    0
}
unsafe extern "C" fn mock_heap_alloc(_size: u32) -> *mut u8 {
    core::ptr::null_mut()
}
unsafe extern "C" fn mock_heap_free(_ptr: *mut u8) {}
unsafe extern "C" fn mock_heap_realloc(_ptr: *mut u8, _new_size: u32) -> *mut u8 {
    core::ptr::null_mut()
}
unsafe extern "C" fn mock_provider_open(_c: u32, _o: u32, _a: *const u8, _l: usize) -> i32 {
    -38
}
unsafe extern "C" fn mock_provider_query(_h: i32, _k: u32, _o: *mut u8, _l: usize) -> i32 {
    -38
}
unsafe extern "C" fn mock_provider_close(_handle: i32) -> i32 {
    0
}
unsafe extern "C" fn mock_provider_call(_h: i32, op: u32, arg: *mut u8, len: usize) -> i32 {
    // TIMER::MILLIS: the mock clock. Everything else (logging) is taken.
    if op == 0x0602 && len >= 8 {
        let b = mock().now_ms.to_le_bytes();
        for (i, v) in b.iter().enumerate() {
            // SAFETY: the caller's buffer holds `len >= 8` bytes.
            unsafe { *arg.add(i) = *v };
        }
    }
    0
}
unsafe extern "C" fn mock_provider_call_sel(
    _sel: *const u8,
    _sel_len: usize,
    _op_handle: i32,
    _op: u32,
    _arg: *mut u8,
    _arg_len: usize,
) -> i32 {
    -38
}

fn sys_table() -> SyscallTable {
    SyscallTable {
        version: 1,
        channel_read: mock_channel_read,
        channel_write: mock_channel_write,
        channel_poll: mock_channel_poll,
        heap_alloc: mock_heap_alloc,
        heap_free: mock_heap_free,
        heap_realloc: mock_heap_realloc,
        provider_open: mock_provider_open,
        provider_call: mock_provider_call,
        provider_query: mock_provider_query,
        provider_close: mock_provider_close,
        channel_peek: mock_channel_peek,
        telemetry_enabled: core::ptr::null(),
        provider_call_sel: mock_provider_call_sel,
        channel_discard: mock_channel_discard,
    }
}

// ── Harness ──────────────────────────────────────────────────────────────

const FRAMES: i32 = 21;
const CONN: u16 = 7;

const PRODUCE: i16 = 0;
const FETCH: i16 = 1;
const LIST_OFFSETS: i16 = 2;
const METADATA: i16 = 3;
const API_VERSIONS: i16 = 18;

/// The codec, zeroed on the heap as the module state is, initialised,
/// its frame edge and anchor wired.
struct Codec {
    k: Box<kafka::Kafka>,
    anchor: Box<anchor::Anchor>,
    sys: SyscallTable,
}

impl Codec {
    fn new() -> Self {
        let layout = std::alloc::Layout::new::<kafka::Kafka>();
        // SAFETY: the codec's state is integers, byte arrays, bools and a
        // raw pointer, for each of which all-zero is a valid value (the
        // module state starts zeroed); the layout is non-zero-sized and
        // the pointer is checked.
        let mut k = unsafe {
            let p = std::alloc::alloc_zeroed(layout).cast::<kafka::Kafka>();
            assert!(!p.is_null(), "allocation failed");
            Box::from_raw(p)
        };
        kafka::init(&mut k);
        kafka::finish_init(&mut k);
        let mut anchor = Box::new(anchor::Anchor);
        k.anchor = &mut *anchor;
        k.out_frames = FRAMES;
        Self {
            k,
            anchor,
            sys: sys_table(),
        }
    }

    /// The client's bytes arrive as one TCP chunk. `true` when the codec
    /// held the record (the caller stops reading raw records).
    fn client(&mut self, conn: u16, bytes: &[u8]) -> bool {
        let mut rec = conn.to_le_bytes().to_vec();
        rec.extend_from_slice(bytes);
        // SAFETY: the mock table is live for the codec's whole life.
        unsafe { kafka::on_frame(&mut self.k, &self.sys, wire::MSG_CLIENT_FRAME, &rec) }
    }

    fn closed(&mut self, conn: u16) {
        // SAFETY: as for `client`.
        unsafe {
            kafka::on_frame(
                &mut self.k,
                &self.sys,
                wire::MSG_CONN_CLOSED,
                &conn.to_le_bytes(),
            );
        }
    }

    /// `session_processor` answers: `false` when the frame edge refused
    /// the response and it must be offered again.
    fn session(&mut self, conn: u16, api_key: i16, corr: i32, body: &[u8]) -> bool {
        let mut env = conn.to_le_bytes().to_vec();
        env.push(1);
        env.extend_from_slice(&api_key.to_le_bytes());
        env.extend_from_slice(&3i16.to_le_bytes());
        env.extend_from_slice(&corr.to_le_bytes());
        env.extend_from_slice(body);
        // SAFETY: as for `client`.
        unsafe { kafka::on_response(&mut self.k, &self.sys, &env) }
    }

    /// One module step's worth of the codec's own work. `true` when raw
    /// records may be read.
    fn step(&mut self) -> bool {
        // SAFETY: as for `client`.
        unsafe { kafka::step(&mut self.k, &self.sys) }
    }
}

/// A request frame: `[size][api_key][api_ver][corr][client_id=null][body]`.
fn request(api_key: i16, api_ver: i16, corr: i32, body: &[u8]) -> Vec<u8> {
    let mut r = Vec::new();
    r.extend_from_slice(&((2 + 2 + 4 + 2 + body.len()) as i32).to_be_bytes());
    r.extend_from_slice(&api_key.to_be_bytes());
    r.extend_from_slice(&api_ver.to_be_bytes());
    r.extend_from_slice(&corr.to_be_bytes());
    r.extend_from_slice(&(-1i16).to_be_bytes());
    r.extend_from_slice(body);
    r
}

/// A Produce v3 request body with `acks` (no topic data: the codec reads
/// only up to acks; the test plays `session_processor`).
fn produce(corr: i32, acks: i16) -> Vec<u8> {
    let mut b = (-1i16).to_be_bytes().to_vec();
    b.extend_from_slice(&acks.to_be_bytes());
    b.extend_from_slice(&30_000i32.to_be_bytes());
    request(PRODUCE, 3, corr, &b)
}

fn metadata(corr: i32) -> Vec<u8> {
    // v1: topics = null (every topic).
    request(METADATA, 1, corr, &(-1i32).to_be_bytes())
}

fn api_versions(corr: i32) -> Vec<u8> {
    request(API_VERSIONS, 0, corr, &[])
}

fn fetch(corr: i32) -> Vec<u8> {
    request(FETCH, 4, corr, &[0; 16])
}

/// What reached the client on `conn`, in order: `Ok(corr)` per response
/// frame, `Err(())` for the close the codec asked of the transport.
fn client_read(conn: u16) -> Vec<Result<i32, ()>> {
    let bytes = mock().fifos.remove(&FRAMES).unwrap_or_default();
    let mut out = Vec::new();
    let mut at = 0usize;
    while at + wire::ENVELOPE_HDR <= bytes.len() {
        let (mt, len) = wire::decode_header(&bytes[at..]);
        let start = at + wire::ENVELOPE_HDR;
        let end = start + usize::from(len);
        let p = &bytes[start..end];
        assert_eq!(
            u16::from_le_bytes([p[0], p[1]]),
            conn,
            "a frame for another connection"
        );
        match mt {
            wire::MSG_CLIENT_FRAME => {
                let size = i32::from_be_bytes([p[2], p[3], p[4], p[5]]) as usize;
                assert_eq!(size + 4, p.len() - 2, "a frame whose size field lies");
                out.push(Ok(i32::from_be_bytes([p[6], p[7], p[8], p[9]])));
            }
            wire::MSG_CONN_CLOSE_REQUEST => out.push(Err(())),
            other => panic!("unexpected envelope {other:#x}"),
        }
        at = end;
    }
    assert_eq!(at, bytes.len(), "trailing bytes on the frame edge");
    out
}

/// The correlation ids forwarded to the session, in order, drained.
fn forwarded() -> Vec<i32> {
    std::mem::take(&mut mock().forwarded)
        .into_iter()
        .filter(|(mt, _)| *mt == wire::MSG_SESSION_PROPOSAL)
        .map(|(_, p)| i32::from_le_bytes([p[7], p[8], p[9], p[10]]))
        .collect()
}

fn ok(corrs: &[i32]) -> Vec<Result<i32, ()>> {
    corrs.iter().map(|&c| Ok(c)).collect()
}

// ── Order ────────────────────────────────────────────────────────────────

#[test]
fn a_durable_produce_is_answered_before_the_requests_sent_after_it() {
    let _g = mock_reset();
    let mut c = Codec::new();
    // One chunk: Produce, Metadata (the codec's), Fetch (the session's,
    // answered at once), ApiVersions (the codec's).
    let mut chunk = produce(1, -1);
    chunk.extend(metadata(2));
    chunk.extend(fetch(3));
    chunk.extend(api_versions(4));
    assert!(!c.client(CONN, &chunk));
    assert_eq!(forwarded(), vec![1, 3]);
    // The Fetch is answered at once; the Produce once durable.
    assert!(c.session(CONN, FETCH, 3, &[0xF; 40]));
    assert_eq!(client_read(CONN), vec![], "nothing passes the Produce");
    assert!(c.session(CONN, PRODUCE, 1, &[0x1; 30]));
    assert_eq!(client_read(CONN), ok(&[1, 2, 3, 4]));
    assert_eq!(c.k_pool_in_use(), 0);
}

#[test]
fn answers_in_order_are_written_as_they_come() {
    let _g = mock_reset();
    let mut c = Codec::new();
    c.client(CONN, &api_versions(1));
    assert_eq!(
        client_read(CONN),
        ok(&[1]),
        "nothing in flight: answered at once"
    );
    c.client(CONN, &fetch(2));
    c.session(CONN, FETCH, 2, &[0; 8]);
    assert_eq!(client_read(CONN), ok(&[2]));
}

#[test]
fn an_acks_zero_produce_takes_no_place_in_the_order() {
    let _g = mock_reset();
    let mut c = Codec::new();
    let mut chunk = produce(1, 0);
    chunk.extend(fetch(2));
    chunk.extend(metadata(3));
    c.client(CONN, &chunk);
    assert_eq!(forwarded(), vec![1, 2]);
    c.session(CONN, FETCH, 2, &[0; 8]);
    assert_eq!(client_read(CONN), ok(&[2, 3]), "no answer is waited for");
}

// ── Requests never answered ──────────────────────────────────────────────

#[test]
fn a_request_the_session_will_never_answer_closes_the_connection_in_its_turn() {
    let _g = mock_reset();
    let mut c = Codec::new();
    let mut chunk = fetch(1);
    chunk.extend(produce(2, -1));
    chunk.extend(request(LIST_OFFSETS, 1, 3, &[0; 8]));
    c.client(CONN, &chunk);
    assert_eq!(forwarded(), vec![1, 2, 3]);
    // 2 has no answer; 3 is answered early; 1 last.
    c.session(CONN, wire::KAFKA_API_CLOSE, 2, &[]);
    c.session(CONN, LIST_OFFSETS, 3, &[0; 8]);
    assert_eq!(client_read(CONN), vec![]);
    c.session(CONN, FETCH, 1, &[0; 8]);
    assert_eq!(
        client_read(CONN),
        vec![Ok(1), Err(())],
        "1, then the close; never 3"
    );
    // What the client still sends is not taken; late answers are dropped.
    c.client(CONN, &fetch(4));
    assert_eq!(forwarded(), vec![]);
    assert!(c.session(CONN, FETCH, 4, &[0; 8]));
    assert_eq!(client_read(CONN), vec![]);
}

#[test]
fn an_api_the_broker_does_not_serve_closes_the_connection_behind_the_answers_ahead() {
    let _g = mock_reset();
    let mut c = Codec::new();
    let mut chunk = produce(1, -1);
    chunk.extend(request(57, 0, 2, &[])); // not an API this broker serves
    chunk.extend(fetch(3));
    c.client(CONN, &chunk);
    assert_eq!(forwarded(), vec![1], "nothing after it is taken");
    assert_eq!(client_read(CONN), vec![]);
    c.session(CONN, PRODUCE, 1, &[0; 8]);
    assert_eq!(client_read(CONN), vec![Ok(1), Err(())]);
}

#[test]
fn a_malformed_request_closes_the_connection() {
    let _g = mock_reset();
    let mut c = Codec::new();
    // A header too short to carry a correlation id.
    let mut bad = 6i32.to_be_bytes().to_vec();
    bad.extend_from_slice(&[0, 1, 0, 4, 0, 0]);
    c.client(CONN, &bad);
    assert_eq!(client_read(CONN), vec![Err(())]);
}

#[test]
fn an_overdue_answer_closes_the_connection_rather_than_hold_it() {
    let _g = mock_reset();
    let mut c = Codec::new();
    let mut chunk = produce(1, -1);
    chunk.extend(api_versions(2));
    c.client(CONN, &chunk);
    mock().now_ms = 10_000;
    c.step();
    assert_eq!(client_read(CONN), vec![]);
    mock().now_ms = 16_000;
    c.step();
    assert_eq!(client_read(CONN), vec![Err(())]);
    assert_eq!(c.k_pool_in_use(), 0);
    // The answer, if it comes after all, matches nothing.
    c.session(CONN, PRODUCE, 1, &[0; 8]);
    assert_eq!(client_read(CONN), vec![]);
}

// ── Backpressure ─────────────────────────────────────────────────────────

#[test]
fn a_pipelining_client_has_at_most_depth_requests_in_flight() {
    let _g = mock_reset();
    let mut c = Codec::new();
    let n = kafka_order::DEPTH as i32 + 3;
    let mut chunk = Vec::new();
    for corr in 1..=n {
        chunk.extend(produce(corr, -1));
    }
    assert!(!c.client(CONN, &chunk));
    let first: Vec<i32> = (1..=kafka_order::DEPTH as i32).collect();
    assert_eq!(forwarded(), first, "the rest wait in the buffer");
    c.step();
    assert_eq!(
        forwarded(),
        vec![],
        "and stay there while nothing is answered"
    );
    // Each answer makes room for one more, taken at once.
    c.session(CONN, PRODUCE, 1, &[0; 8]);
    assert_eq!(forwarded(), vec![kafka_order::DEPTH as i32 + 1]);
    for corr in 2..=n {
        c.session(CONN, PRODUCE, corr, &[0; 8]);
    }
    assert_eq!(
        forwarded(),
        ((kafka_order::DEPTH as i32 + 2)..=n).collect::<Vec<_>>()
    );
    assert_eq!(client_read(CONN), ok(&(1..=n).collect::<Vec<_>>()));
}

#[test]
fn a_record_that_would_overflow_a_waiting_connection_is_held_not_dropped() {
    let _g = mock_reset();
    let mut c = Codec::new();
    // Fill the in-flight window, then keep sending until the reassembly
    // buffer cannot take the next record.
    let mut sent = 0i32;
    let mut held = false;
    while !held {
        sent += 1;
        assert!(sent < 4000, "the codec never pushed back");
        held = c.client(CONN, &produce(sent, -1));
    }
    assert!(!c.step(), "no raw record is read while one is held");
    // Answer everything as it is forwarded; the held record lands and
    // every request is forwarded once, in order.
    let mut seen = Vec::new();
    for _ in 0..10_000 {
        let f = forwarded();
        if f.is_empty() && c.step() && seen.len() == sent as usize {
            break;
        }
        for corr in f {
            seen.push(corr);
            c.session(CONN, PRODUCE, corr, &[0; 8]);
        }
        c.step();
    }
    assert_eq!(seen, (1..=sent).collect::<Vec<_>>());
    assert_eq!(client_read(CONN), ok(&(1..=sent).collect::<Vec<_>>()));
}

#[test]
fn a_chunk_larger_than_the_room_left_goes_in_as_requests_make_room() {
    let _g = mock_reset();
    let mut c = Codec::new();
    // A 6000-byte acks=0 Produce, then 272 small ones: no answer is waited
    // for, so nothing stalls, but the second chunk alone is larger than
    // the room the first left in the reassembly buffer.
    let mut big = produce(1, 0);
    let pad = 6000 - big.len();
    big.extend(std::iter::repeat(0u8).take(pad));
    let size = (big.len() - 4) as i32;
    big[0..4].copy_from_slice(&size.to_be_bytes());
    let mut stream = big;
    for corr in 2..=273 {
        stream.extend(produce(corr, 0));
    }
    let (first, second) = stream.split_at(4000);
    assert!(second.len() + first.len() > 8192 && second.len() <= 8190);
    assert!(!c.client(CONN, first));
    assert!(!c.client(CONN, second));
    assert_eq!(forwarded(), (1..=273).collect::<Vec<_>>());
    assert_eq!(
        client_read(CONN),
        vec![],
        "nothing closed, nothing answered"
    );
}

#[test]
fn a_request_the_session_edge_refuses_is_offered_again_not_dropped() {
    let _g = mock_reset();
    let mut c = Codec::new();
    mock().refuse_forward = true;
    let mut chunk = fetch(1);
    chunk.extend(fetch(2));
    c.client(CONN, &chunk);
    assert_eq!(forwarded(), vec![]);
    c.step();
    assert_eq!(forwarded(), vec![]);
    mock().refuse_forward = false;
    c.step();
    assert_eq!(forwarded(), vec![1, 2], "once each, in order");
    c.step();
    assert_eq!(forwarded(), vec![]);
}

#[test]
fn a_response_the_frame_edge_refuses_is_offered_again_unchanged() {
    let _g = mock_reset();
    let mut c = Codec::new();
    let mut chunk = fetch(1);
    chunk.extend(api_versions(2));
    c.client(CONN, &chunk);
    mock().closed.insert(FRAMES);
    assert!(
        !c.session(CONN, FETCH, 1, &[0; 8]),
        "the oldest's answer is refused"
    );
    mock().closed.remove(&FRAMES);
    assert!(c.session(CONN, FETCH, 1, &[0; 8]));
    assert_eq!(client_read(CONN), ok(&[1, 2]));
}

#[test]
fn a_kept_answer_the_frame_edge_refuses_is_written_by_the_step() {
    let _g = mock_reset();
    let mut c = Codec::new();
    let mut chunk = produce(1, -1);
    chunk.extend(fetch(2));
    chunk.extend(fetch(3));
    c.client(CONN, &chunk);
    c.session(CONN, FETCH, 3, &[0; 8]);
    c.session(CONN, FETCH, 2, &[0; 8]);
    // The edge takes the Produce's answer and refuses the next: the kept
    // answers stay, in order.
    mock().budget.insert(FRAMES, 1);
    assert!(c.session(CONN, PRODUCE, 1, &[0; 8]));
    assert_eq!(client_read(CONN), ok(&[1]));
    c.step();
    assert_eq!(client_read(CONN), vec![]);
    mock().budget.remove(&FRAMES);
    c.step();
    assert_eq!(client_read(CONN), ok(&[2, 3]));
    assert_eq!(c.k_pool_in_use(), 0);
}

// ── Connections ──────────────────────────────────────────────────────────

#[test]
fn a_closed_connection_forgets_its_requests_and_frees_its_slots() {
    let _g = mock_reset();
    let mut c = Codec::new();
    let mut chunk = produce(1, -1);
    chunk.extend(fetch(2));
    chunk.extend(fetch(3));
    c.client(CONN, &chunk);
    c.session(CONN, FETCH, 3, &[0; 8]);
    assert!(c.k_pool_in_use() > 0);
    c.closed(CONN);
    assert_eq!(c.k_pool_in_use(), 0);
    // The next connection under the same id starts clean.
    c.session(CONN, PRODUCE, 1, &[0; 8]);
    c.client(CONN, &api_versions(9));
    assert_eq!(client_read(CONN), ok(&[9]));
}

#[test]
fn connections_are_ordered_independently() {
    let _g = mock_reset();
    let mut c = Codec::new();
    let mut a = produce(1, -1);
    a.extend(fetch(2));
    c.client(1, &a);
    c.client(2, &fetch(1));
    c.session(1, FETCH, 2, &[0; 8]);
    // Connection 2's answer is not held behind connection 1's Produce.
    c.session(2, FETCH, 1, &[0; 8]);
    let bytes = mock().fifos.remove(&FRAMES).unwrap_or_default();
    let (_, len) = wire::decode_header(&bytes);
    assert_eq!(
        usize::from(len) + wire::ENVELOPE_HDR,
        bytes.len(),
        "one frame"
    );
    assert_eq!(u16::from_le_bytes([bytes[3], bytes[4]]), 2);
}

impl Codec {
    fn k_pool_in_use(&self) -> u16 {
        self.k.pool.in_use
    }
}

// ── session_processor's half ─────────────────────────────────────────────

/// The source between `from` and the next item at column 0.
fn item<'a>(src: &'a str, from: &str) -> &'a str {
    let at = src.find(from).unwrap_or_else(|| panic!("{from} is gone"));
    let rest = &src[at..];
    let end = rest[1..].find("\n}\n").map_or(rest.len(), |e| e + 3);
    &rest[..end]
}

#[test]
fn session_processor_says_when_a_kafka_request_will_never_be_answered() {
    let dispatch = SESSION_PROCESSOR
        .find("wire::MSG_SESSION_PROPOSAL if proto == PROTO_KAFKA")
        .expect("the Kafka dispatch");
    let arm = &SESSION_PROCESSOR[dispatch..dispatch + 4000];
    let cleared = arm
        .find("s.kafka_settled = false;")
        .expect("cleared per request");
    let handled = arm.find("handle_kafka_produce(").expect("the handlers");
    let marked = arm
        .find("if !s.kafka_settled {")
        .expect("an unsettled request is answered");
    assert!(cleared < handled && handled < marked);
    assert!(arm[marked..].contains("wire::KAFKA_API_CLOSE"));
    // Every way a request is settled marks it: an answer taken, an
    // inflight or cold slot that answers it later, acks = 0.
    for f in [
        "unsafe fn emit_kafka_response(",
        "unsafe fn kafka_outbuf_reply(",
        "unsafe fn try_park_cold_fetch(",
        "unsafe fn handle_kafka_produce(",
        "unsafe fn handle_kafka_offset_commit(",
    ] {
        assert!(item(SESSION_PROCESSOR, f).contains("kafka_settled"), "{f}");
    }
}

#[test]
fn session_processor_answers_the_kafka_requests_its_sweep_expires() {
    let sweep = SESSION_PROCESSOR
        .find("KAFKA_INFLIGHT_TIMEOUT_MS) {")
        .expect("the expiry sweep");
    let body = &SESSION_PROCESSOR[sweep..sweep + 4000];
    let expire = body
        .find("expire_kafka_inflight(s, sys, i, e)")
        .expect("Kafka inflights are answered");
    let free = body.find("store::inflight_free(").expect("the free");
    assert!(expire < free, "answered before any silent free");
    let f = item(SESSION_PROCESSOR, "unsafe fn expire_kafka_inflight(");
    assert!(f.contains("KERR_REQUEST_TIMED_OUT"));
    assert!(f.contains("ANSWER_PRODUCE") && f.contains("ANSWER_OFFSET"));
}

#[test]
fn the_codec_drops_no_request_it_reads() {
    // The old codec dropped a request the session edge refused and one of
    // an API it did not serve; either left a request unanswered.
    let f = item(KAFKA, "unsafe fn handle_request(");
    assert!(!f.contains("drop; producer retries"));
    assert!(f.matches("fail_conn(s, ci)").count() >= 4);
}

// ── The coordinator ──────────────────────────────────────────────────────

/// The correlation ids handed to the coordinator, in order, drained.
fn coord_forwarded() -> Vec<i32> {
    std::mem::take(&mut mock().coord_forwarded)
        .into_iter()
        .map(|p| i32::from_le_bytes([p[7], p[8], p[9], p[10]]))
        .collect()
}

#[test]
fn group_and_offset_calls_go_to_the_coordinator_and_log_calls_to_the_bound_worker() {
    let _g = mock_reset();
    let mut c = Codec::new();
    // Produce and Fetch read and write the log every worker holds;
    // OffsetCommit, OffsetFetch, JoinGroup, Heartbeat, LeaveGroup and
    // SyncGroup reach state one worker owns for the pair. Eight: the
    // connection's depth.
    let mut chunk = produce(1, -1);
    chunk.extend(fetch(2));
    for (corr, api) in [(3, 8i16), (4, 9), (5, 11), (6, 12), (7, 13), (8, 14)] {
        chunk.extend(request(api, 0, corr, &[0; 8]));
    }
    assert!(!c.client(CONN, &chunk));
    assert_eq!(
        forwarded(),
        (1..=8).collect::<Vec<_>>(),
        "every request forwarded, in order"
    );
    assert_eq!(
        coord_forwarded(),
        vec![3, 4, 5, 6, 7, 8],
        "only the group and offset calls"
    );
    // Answers from either worker, in any order, leave in request order.
    for corr in (1..=8).rev() {
        let api = [PRODUCE, FETCH, 8, 9, 11, 12, 13, 14][corr as usize - 1];
        assert!(c.session(CONN, api, corr, &[0; 8]));
    }
    assert_eq!(client_read(CONN), ok(&(1..=8).collect::<Vec<_>>()));
}

#[test]
fn the_codec_routes_by_the_anchor_cores_rule() {
    // The routing decision is the core's (`kafka_coordinator_api`), not a
    // second list in the codec.
    let at = KAFKA
        .find("kafka_coordinator_api(api_key)")
        .expect("the codec asks the core");
    assert!(KAFKA[at..].contains("forward_coord("));
    for api in [8i16, 9, 11, 12, 13, 14] {
        assert!(
            anchor::kafka_coordinator_api(api),
            "api {api} is the coordinator's"
        );
    }
    for api in [0i16, 1, 2, 3, 10, 18, 22] {
        assert!(
            !anchor::kafka_coordinator_api(api),
            "api {api} is not the coordinator's"
        );
    }
}
