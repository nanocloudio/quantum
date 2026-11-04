//! What `session_processor` does with a client response the codec bus
//! refuses: its response hold (`modules/app/session_processor/reply_hold.rs`)
//! mounted verbatim and driven against a mock `SyscallTable` whose output
//! channels refuse writes until told to accept them.
//!
//! The defect these pin: a PUBACK, a DISCONNECT failing a refused or
//! superseded flow, a Kafka response, an AMQP Basic.Ack or the expiry
//! sweep's Nack was written once, and lost when the bus refused it — the
//! client heard nothing about work it had handed over. Now a refused
//! response is held, per output, and written in order as the bus takes
//! it; every input that can answer a client is read only while the hold
//! has room for its answers, so the hold never overflows and the pressure
//! goes back to the clients instead; an input that cannot wait (a
//! committed entry, a Clustor refusal, a cold read, the expiry sweep)
//! keeps its operation with the answer owed and answers it later.
//!
//! What these pin:
//!
//! - every response arrives once, in the order it was made, however long
//!   the bus refuses and wherever the ring wraps;
//! - nothing overtakes a held response: a later response, a delivery
//!   offered while one is held, a resend the CONNECT owes;
//! - the reserve the input gates read is a guarantee: whatever one input
//!   answers fits once it is granted;
//! - a full bus stops only the sessions of its own output;
//! - a closed connection's held responses are never written (its id is
//!   the next connection's); a DISCONNECT with no room — a placement
//!   update's "server moved" for every session it releases, a failed
//!   flow's — is owed one per connection;
//! - an answer whose input cannot wait is refused, not lost, on a full
//!   ring, and kept by its caller;
//! - in `session_processor`, every response leaves through the hold;
//!   every input that answers a client and can wait is gated on it, and
//!   every one that cannot keeps its answer owed in its own slot.

#![allow(
    dead_code,
    reason = "host test crate; the mounted table and SDK surface are a superset of what the tests touch"
)]
#![allow(
    unsafe_code,
    reason = "drives the mounted table's unsafe writes against a mock SyscallTable; the unsafety is confined to the module ABI boundary"
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
    reason = "a sequence that finds the table or the module source not as it must be fails the test, which is what expect and panic say"
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
#[path = "../../../modules/app/session_processor/reply_hold.rs"]
mod reply_hold;

use reply_hold::{Reply, Sent};
use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, MutexGuard};

const SESSION_PROCESSOR: &str = include_str!("../../../modules/app/session_processor/mod.rs");
const WORKER: &str = include_str!("../../../modules/app/session_processor/worker.rs");

// ── Mock kernel: byte-FIFO channels, each refusing writes while closed ──

#[derive(Default)]
struct Mock {
    fifos: HashMap<i32, Vec<u8>>,
    closed: HashSet<i32>,
    /// Writes allowed on a channel before it closes again (none: open).
    budget: HashMap<i32, usize>,
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

unsafe extern "C" fn mock_channel_read(handle: i32, buf: *mut u8, len: usize) -> i32 {
    let mut m = mock();
    let Some(fifo) = m.fifos.get_mut(&handle) else {
        return 0;
    };
    let n = len.min(fifo.len());
    for (i, b) in fifo.drain(..n).enumerate() {
        // SAFETY: the caller's buffer holds `len >= n` bytes.
        unsafe { *buf.add(i) = b };
    }
    n as i32
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

unsafe extern "C" fn mock_channel_discard(handle: i32, len: usize) -> i32 {
    let mut m = mock();
    let fifo = m.fifos.entry(handle).or_default();
    let n = len.min(fifo.len());
    fifo.drain(..n);
    len as i32
}

unsafe extern "C" fn mock_channel_poll(handle: i32, events: u32) -> i32 {
    let m = mock();
    let mut ready: u32 = 0;
    if !m.closed.contains(&handle) {
        ready |= 0x02;
    }
    if m.fifos.get(&handle).is_some_and(|f| !f.is_empty()) {
        ready |= 0x01;
    }
    (ready & events) as i32
}

unsafe extern "C" fn mock_channel_peek(handle: i32, buf: *mut u8, len: usize) -> i32 {
    let m = mock();
    let Some(fifo) = m.fifos.get(&handle) else {
        return 0;
    };
    let n = len.min(fifo.len());
    for (i, b) in fifo.iter().take(n).enumerate() {
        // SAFETY: the caller's buffer holds `len >= n` bytes.
        unsafe { *buf.add(i) = *b };
    }
    n as i32
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
unsafe extern "C" fn mock_provider_call(_h: i32, _op: u32, _a: *mut u8, _l: usize) -> i32 {
    -38
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

fn refuse(chan: i32) {
    mock().closed.insert(chan);
}

fn accept(chan: i32) {
    let mut m = mock();
    m.closed.remove(&chan);
    m.budget.remove(&chan);
}

/// The channel takes `n` more writes, then refuses.
fn accept_only(chan: i32, n: usize) {
    let mut m = mock();
    m.closed.remove(&chan);
    m.budget.insert(chan, n);
}

/// Every envelope written to `chan`, as `(msg_type, payload)`, drained.
fn take_written(chan: i32) -> Vec<(u8, Vec<u8>)> {
    let bytes = mock().fifos.remove(&chan).unwrap_or_default();
    let mut out = Vec::new();
    let mut at = 0usize;
    while at + wire::ENVELOPE_HDR <= bytes.len() {
        let (mt, len) = wire::decode_header(&bytes[at..]);
        let start = at + wire::ENVELOPE_HDR;
        let end = start + usize::from(len);
        assert!(end <= bytes.len(), "a torn envelope on the channel");
        out.push((mt, bytes[start..end].to_vec()));
        at = end;
    }
    assert_eq!(at, bytes.len(), "trailing bytes on the channel");
    out
}

// ── Harness ──────────────────────────────────────────────────────────────

const CODEC_OUT: i32 = 11;
const CODEC2_OUT: i32 = 12;
const FRONT_OUT: i32 = 13;
const RESP: u8 = wire::MSG_SESSION_RESPONSE;

fn hold() -> Box<reply_hold::Hold> {
    let layout = std::alloc::Layout::new::<reply_hold::Hold>();
    // SAFETY: the hold is plain integers, for which all-zero is a valid
    // value; the layout is non-zero-sized and the pointer is checked.
    let mut h = unsafe {
        let p = std::alloc::alloc_zeroed(layout).cast::<reply_hold::Hold>();
        assert!(!p.is_null(), "allocation failed");
        Box::from_raw(p)
    };
    reply_hold::init(&mut h);
    h
}

fn owed() -> Box<reply_hold::Owed> {
    let layout = std::alloc::Layout::new::<reply_hold::Owed>();
    // SAFETY: as for `hold`.
    let mut o = unsafe {
        let p = std::alloc::alloc_zeroed(layout).cast::<reply_hold::Owed>();
        assert!(!p.is_null(), "allocation failed");
        Box::from_raw(p)
    };
    reply_hold::owed_init(&mut o);
    o
}

/// A codec response payload as `session_processor` builds one:
/// `[conn_id:u16 LE][proto][pkt_type][flags][body]`.
fn mqtt(conn: u16, pkt_type: u8, body: &[u8]) -> Vec<u8> {
    let mut v = conn.to_le_bytes().to_vec();
    v.extend_from_slice(&[0, pkt_type, 0]);
    v.extend_from_slice(body);
    v
}

/// A payload of `len` bytes for `conn`, numbered `n` so its place in the
/// order can be read back.
fn numbered(conn: u16, n: u32, len: usize) -> Vec<u8> {
    assert!(len >= 6);
    let mut v = vec![0xA5; len];
    v[0..2].copy_from_slice(&conn.to_le_bytes());
    v[2..6].copy_from_slice(&n.to_le_bytes());
    v
}

fn number_of(p: &[u8]) -> u32 {
    u32::from_le_bytes([p[2], p[3], p[4], p[5]])
}

const PKT_PUBACK: u8 = 4;
const PKT_PUBREC: u8 = 5;
const PKT_PUBCOMP: u8 = 7;
const PKT_SUBACK: u8 = 9;
const PKT_PUBLISH: u8 = 3;
const PKT_DISCONNECT: u8 = 14;

/// Who heard of each write, as `session_processor`'s delivery cursor does.
#[derive(Default)]
struct Cursor {
    writes: Vec<(u16, usize, bool)>,
}

impl Cursor {
    fn counted_bytes(&self, conn: u16) -> usize {
        self.writes
            .iter()
            .filter(|w| w.0 == conn && w.2)
            .map(|w| w.1)
            .sum()
    }
}

fn send(
    r: &mut reply_hold::Ring,
    sys: &SyscallTable,
    chan: i32,
    payload: &[u8],
    reply: Reply,
    cur: &mut Cursor,
) -> Sent {
    let conn = u16::from_le_bytes([payload[0], payload[1]]);
    // SAFETY: the mock table is live for the test.
    unsafe {
        reply_hold::respond(r, sys, chan, conn, RESP, payload, reply, true, |c, b, k| {
            cur.writes.push((c, b, k));
        })
    }
}

fn flush(r: &mut reply_hold::Ring, sys: &SyscallTable, chan: i32, cur: &mut Cursor) -> u32 {
    // SAFETY: the mock table is live for the test.
    unsafe {
        reply_hold::flush(r, sys, chan, |c, b, k| {
            cur.writes.push((c, b, k));
        })
    }
}

// ── The responses QT found lost, end to end ──────────────────────────────

/// A PUBACK, a DISCONNECT failing a refused flow and a SUBACK the bus
/// refuses are held, and arrive once each, in order, when it takes them.
#[test]
fn refused_responses_arrive_once_in_order() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    let r = &mut h.rings[0];

    refuse(CODEC_OUT);
    let puback = mqtt(7, PKT_PUBACK, &[0, 1]);
    let disconnect = mqtt(7, PKT_DISCONNECT, &[0x97]);
    let suback = mqtt(9, PKT_SUBACK, &[0, 2, 1]);
    for p in [&puback, &disconnect, &suback] {
        assert_eq!(
            send(r, &sys, CODEC_OUT, p, Reply::Must, &mut cur),
            Sent::Held
        );
    }
    // Refused for a while: steps flush and nothing is lost or written.
    for _ in 0..5 {
        assert_eq!(flush(r, &sys, CODEC_OUT, &mut cur), 0);
    }
    assert!(take_written(CODEC_OUT).is_empty());
    assert_eq!(reply_hold::len(r), 3);

    accept(CODEC_OUT);
    assert_eq!(flush(r, &sys, CODEC_OUT, &mut cur), 3);
    assert!(reply_hold::is_empty(r));
    let got = take_written(CODEC_OUT);
    assert_eq!(
        got,
        vec![
            (RESP, puback.clone()),
            (RESP, disconnect.clone()),
            (RESP, suback.clone())
        ]
    );
    // Flushing again writes nothing twice.
    assert_eq!(flush(r, &sys, CODEC_OUT, &mut cur), 0);
    assert!(take_written(CODEC_OUT).is_empty());
    // The delivery cursor counted each write once, at the write, with its
    // envelope header.
    let hdr = wire::ENVELOPE_HDR;
    assert_eq!(
        cur.counted_bytes(7),
        puback.len() + disconnect.len() + 2 * hdr
    );
    assert_eq!(cur.counted_bytes(9), suback.len() + hdr);
}

/// A response made while one is held goes behind it, even when the bus
/// would have taken it first: the bus opening for one write takes the
/// held one, and the new one is held behind.
#[test]
fn nothing_overtakes_a_held_response() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    let r = &mut h.rings[0];

    refuse(CODEC_OUT);
    let pubrec = mqtt(3, PKT_PUBREC, &[0, 5]);
    assert_eq!(
        send(r, &sys, CODEC_OUT, &pubrec, Reply::Must, &mut cur),
        Sent::Held
    );
    // One write's room: it goes to the held PUBREC.
    accept_only(CODEC_OUT, 1);
    let pubcomp = mqtt(3, PKT_PUBCOMP, &[0, 5]);
    assert_eq!(
        send(r, &sys, CODEC_OUT, &pubcomp, Reply::Must, &mut cur),
        Sent::Held
    );
    // A delivery offered now is refused, not written ahead of the PUBCOMP.
    accept_only(CODEC_OUT, 0);
    let delivery = mqtt(3, PKT_PUBLISH, b"\x00\x01tdata");
    assert_eq!(
        send(r, &sys, CODEC_OUT, &delivery, Reply::Offer, &mut cur),
        Sent::Refused
    );
    accept(CODEC_OUT);
    // Offered again once the bus has room: the held PUBCOMP first.
    assert_eq!(
        send(r, &sys, CODEC_OUT, &delivery, Reply::Offer, &mut cur),
        Sent::Written
    );
    assert_eq!(
        take_written(CODEC_OUT),
        vec![(RESP, pubrec), (RESP, pubcomp), (RESP, delivery)]
    );
}

/// Offered work with nothing held and the bus open is written at once;
/// with the bus refusing it is refused (the caller keeps it), never held.
#[test]
fn an_offer_is_never_held() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    let r = &mut h.rings[0];
    let p = mqtt(1, PKT_PUBLISH, b"\x00\x01tx");
    assert_eq!(
        send(r, &sys, CODEC_OUT, &p, Reply::Offer, &mut cur),
        Sent::Written
    );
    refuse(CODEC_OUT);
    assert_eq!(
        send(r, &sys, CODEC_OUT, &p, Reply::Offer, &mut cur),
        Sent::Refused
    );
    assert!(reply_hold::is_empty(r));
    assert_eq!(take_written(CODEC_OUT), vec![(RESP, p)]);
}

/// A resend the CONNECT owes is held behind its CONNACK while the ring
/// keeps its reserve, and refused (left owed in the log) once it would
/// not — the reserve stays for the next input's answers.
#[test]
fn connect_resends_hold_only_beyond_the_reserve() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    let r = &mut h.rings[0];
    refuse(CODEC_OUT);
    let connack = mqtt(4, 2, &[1, 0]);
    assert_eq!(
        send(r, &sys, CODEC_OUT, &connack, Reply::Must, &mut cur),
        Sent::Held
    );
    let mut held = vec![connack];
    let mut n = 0u32;
    loop {
        let resend = numbered(4, n, 1500);
        match send(r, &sys, CODEC_OUT, &resend, Reply::Spare, &mut cur) {
            Sent::Held => held.push(resend),
            Sent::Refused => break,
            other => panic!("a resend was {other:?}"),
        }
        n += 1;
        assert!(n < 1000, "the spare room never ran out");
    }
    assert!(n > 0, "a resend fits behind the CONNACK");
    assert!(reply_hold::has_reserve(r), "the resends left the reserve");
    accept(CODEC_OUT);
    flush(r, &sys, CODEC_OUT, &mut cur);
    let got: Vec<Vec<u8>> = take_written(CODEC_OUT)
        .into_iter()
        .map(|(_, p)| p)
        .collect();
    assert_eq!(got, held);
}

/// A bus refusing for a long stretch, then taking a few writes at a time,
/// under a steady load of mixed-size responses gated as the module gates
/// its inputs: every response arrives exactly once, in order, the ring
/// wraps many times, and a `Must` is never refused.
#[test]
fn gated_load_through_a_stuttering_bus_is_lossless_and_ordered() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    let r = &mut h.rings[0];
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut made = 0u32;
    let mut gated_steps = 0u32;
    for step in 0..4000u32 {
        // The bus: shut for the first stretch, then a few writes a step.
        if step < 300 {
            refuse(CODEC_OUT);
        } else {
            accept_only(CODEC_OUT, (next() % 4) as usize);
        }
        flush(r, &sys, CODEC_OUT, &mut cur);
        // Inputs, each read only with the reserve, each answering up to
        // RESERVE_ENTRIES responses within RESERVE_BYTES.
        for _ in 0..8 {
            if !reply_hold::has_reserve(r) {
                gated_steps += 1;
                break;
            }
            let answers = 1 + (next() % reply_hold::RESERVE_ENTRIES as u64) as usize;
            let mut budget = reply_hold::RESERVE_BYTES;
            for _ in 0..answers {
                if budget < 6 {
                    break;
                }
                let len = 6 + (next() as usize % (reply_hold::FRAME_MAX - 6 + 1)).min(budget - 6);
                budget -= len;
                let p = numbered(1, made, len);
                let sent = send(r, &sys, CODEC_OUT, &p, Reply::Must, &mut cur);
                assert!(sent.taken(), "response {made} was {sent:?}");
                made += 1;
            }
        }
    }
    accept(CODEC_OUT);
    flush(r, &sys, CODEC_OUT, &mut cur);
    assert!(reply_hold::is_empty(r));
    let got: Vec<u32> = take_written(CODEC_OUT)
        .iter()
        .map(|(_, p)| number_of(p))
        .collect();
    assert_eq!(got.len() as u32, made, "every response arrives once");
    assert!(
        got.iter().copied().eq(0..made),
        "in the order they were made"
    );
    assert!(
        made > 1000 && gated_steps > 0,
        "the load filled the ring and was gated"
    );
}

/// The reserve the gates read is a guarantee, whatever the ring's state:
/// from any reachable state with `has_reserve`, any run of up to
/// RESERVE_ENTRIES responses within RESERVE_BYTES (each up to FRAME_MAX)
/// is taken.
#[test]
fn the_reserve_always_fits_one_inputs_answers() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    let r = &mut h.rings[0];
    let mut rng: u64 = 0xD1B5_4A32_D192_ED03;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    refuse(CODEC_OUT);
    let mut n = 0u32;
    for _ in 0..20_000 {
        match next() % 3 {
            // Drain some.
            0 => {
                accept_only(CODEC_OUT, (next() % 3) as usize);
                flush(r, &sys, CODEC_OUT, &mut cur);
                refuse(CODEC_OUT);
                take_written(CODEC_OUT);
            }
            // Fill while the gate grants.
            _ => {
                if !reply_hold::has_reserve(r) {
                    continue;
                }
                let k = 1 + (next() % reply_hold::RESERVE_ENTRIES as u64) as usize;
                let mut left = reply_hold::RESERVE_BYTES;
                for i in 0..k {
                    // The last answer takes the rest of the budget at worst.
                    let cap = left.min(reply_hold::FRAME_MAX);
                    if cap < 6 {
                        break;
                    }
                    let len = if i + 1 == k {
                        cap
                    } else {
                        6 + next() as usize % (cap - 5)
                    };
                    left -= len;
                    let p = numbered(2, n, len);
                    n += 1;
                    assert_eq!(
                        send(r, &sys, CODEC_OUT, &p, Reply::Must, &mut cur),
                        Sent::Held,
                        "a granted input's answer of {len} bytes was not taken"
                    );
                }
            }
        }
    }
}

/// Each output has its own ring: a refusing `front_out` holds the front's
/// sessions' responses and nothing of `codec_out`'s, which go straight
/// out; the gate on one output's inputs reads only its ring.
#[test]
fn a_full_output_holds_only_its_own_sessions() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    refuse(FRONT_OUT);
    let mut k = 0u32;
    while reply_hold::has_reserve(&h.rings[2]) {
        let p = numbered(0xC000, k, 4000);
        assert_eq!(
            send(&mut h.rings[2], &sys, FRONT_OUT, &p, Reply::Must, &mut cur),
            Sent::Held
        );
        k += 1;
    }
    assert!(!reply_hold::has_reserve(&h.rings[2]));
    assert!(reply_hold::has_reserve(&h.rings[0]) && reply_hold::has_reserve(&h.rings[1]));
    let a = mqtt(5, PKT_PUBACK, &[0, 9]);
    assert_eq!(
        send(&mut h.rings[0], &sys, CODEC_OUT, &a, Reply::Must, &mut cur),
        Sent::Written
    );
    let b = mqtt(6, PKT_PUBACK, &[0, 9]);
    assert_eq!(
        send(&mut h.rings[1], &sys, CODEC2_OUT, &b, Reply::Must, &mut cur),
        Sent::Written
    );
    assert_eq!(take_written(CODEC_OUT), vec![(RESP, a)]);
    assert_eq!(take_written(CODEC2_OUT), vec![(RESP, b)]);
    accept(FRONT_OUT);
    assert_eq!(flush(&mut h.rings[2], &sys, FRONT_OUT, &mut cur), k);
    let got: Vec<u32> = take_written(FRONT_OUT)
        .iter()
        .map(|(_, p)| number_of(p))
        .collect();
    assert!(got.iter().copied().eq(0..k));
}

/// A connection that closes while its responses are held: none of them is
/// written (its id may already be the next connection's), the others'
/// are, in order, and the drain no longer waits on it.
#[test]
fn a_closed_connections_held_responses_are_never_written() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    refuse(CODEC_OUT);
    let mut want = Vec::new();
    for n in 0..6u32 {
        let conn = if n % 2 == 0 { 20 } else { 21 };
        let p = numbered(conn, n, 64);
        assert_eq!(
            send(&mut h.rings[0], &sys, CODEC_OUT, &p, Reply::Must, &mut cur),
            Sent::Held
        );
        if conn == 21 {
            want.push(p);
        }
    }
    assert!(reply_hold::holds_for(&h, 20) && reply_hold::holds_for(&h, 21));
    reply_hold::forget_conn(&mut h, 20);
    assert!(!reply_hold::holds_for(&h, 20));
    assert!(reply_hold::holds_for(&h, 21));
    accept(CODEC_OUT);
    assert_eq!(flush(&mut h.rings[0], &sys, CODEC_OUT, &mut cur), 3);
    assert!(reply_hold::is_empty(&h.rings[0]));
    let got: Vec<Vec<u8>> = take_written(CODEC_OUT)
        .into_iter()
        .map(|(_, p)| p)
        .collect();
    assert_eq!(got, want);
    assert_eq!(cur.counted_bytes(20), 0);
}

/// A payload no channel message can carry is refused outright, never
/// held: held, it would stop its ring for good.
#[test]
fn an_unwritable_payload_is_never_held() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    refuse(CODEC_OUT);
    let big = numbered(1, 0, reply_hold::FRAME_MAX + 1);
    assert_eq!(
        send(
            &mut h.rings[0],
            &sys,
            CODEC_OUT,
            &big,
            Reply::Must,
            &mut cur
        ),
        Sent::Refused
    );
    let max = numbered(1, 1, reply_hold::FRAME_MAX);
    assert_eq!(
        send(
            &mut h.rings[0],
            &sys,
            CODEC_OUT,
            &max,
            Reply::Must,
            &mut cur
        ),
        Sent::Held
    );
    accept(CODEC_OUT);
    assert_eq!(flush(&mut h.rings[0], &sys, CODEC_OUT, &mut cur), 1);
    assert_eq!(take_written(CODEC_OUT), vec![(RESP, max)]);
    assert_eq!(
        reply_hold::FRAME_MAX + wire::ENVELOPE_HDR,
        abi::CHANNEL_BUFFER_SIZE,
        "the largest held payload is the largest a channel message carries"
    );
}

/// The DISCONNECTs owed — a placement update's "server moved" for every
/// session it releases, more than any ring holds, and a failed flow's
/// with no room — are one per connection with the first reason owed,
/// listed from where the last pass stopped so every one has its turn,
/// and settled once each.
#[test]
fn owed_disconnects_are_one_per_connection() {
    let mut o = owed();
    for c in (0..u16::MAX).step_by(3) {
        reply_hold::owe(&mut o, c, 0x9D);
        // A second reason does not replace the one the flow failed on.
        reply_hold::owe(&mut o, c, 0x97);
    }
    let total = (0..u16::MAX).step_by(3).count() as u32;
    assert_eq!(o.count, total, "owing twice owes once");
    assert_eq!(reply_hold::owed(&o, 3), Some(0x9D));
    assert_eq!(reply_hold::owed(&o, 4), None);
    // Passes that settle nothing still move on: from 1000, wrapping.
    let mut batch = [0u16; 64];
    let n = reply_hold::owed_conns(&o, 1000, &mut batch);
    assert_eq!(n, 64);
    assert_eq!(batch[0], 1002, "the first owed at or past the start");
    assert!(batch[..n].windows(2).all(|w| w[0] < w[1]));
    // Near the top it wraps to the lowest ids.
    let n = reply_hold::owed_conns(&o, u16::MAX - 4, &mut batch);
    assert_eq!(n, 64);
    assert!(batch[..n].iter().any(|&c| c < 1000), "the pass wraps");
    // Settling as a flush does, pass after pass, reaches every one once.
    let mut seen = std::collections::HashSet::new();
    let mut from = 0u16;
    loop {
        let n = reply_hold::owed_conns(&o, from, &mut batch);
        if n == 0 {
            break;
        }
        from = batch[n - 1].wrapping_add(1);
        for &c in &batch[..n] {
            assert!(reply_hold::owed(&o, c).is_some());
            reply_hold::settle(&mut o, c);
            reply_hold::settle(&mut o, c);
            assert!(seen.insert(c), "settled twice");
        }
    }
    assert_eq!(seen.len() as u32, total);
    assert_eq!(o.count, 0);
}

/// An answer decided where its input cannot wait ([`Reply::Kept`]) is
/// held while the ring has room and refused — kept by its caller, never
/// counted lost — when it has none; a [`Reply::Must`] meeting a full ring
/// is the one loss, which the gates make unreachable.
#[test]
fn a_kept_answer_is_refused_not_lost_on_a_full_ring() {
    let _g = mock_reset();
    let sys = sys_table();
    let mut h = hold();
    let mut cur = Cursor::default();
    let r = &mut h.rings[0];
    refuse(CODEC_OUT);
    let mut n = 0u32;
    loop {
        let p = numbered(8, n, 4000);
        match send(r, &sys, CODEC_OUT, &p, Reply::Kept, &mut cur) {
            Sent::Held => n += 1,
            Sent::Refused => break,
            other => panic!("a kept answer was {other:?}"),
        }
    }
    assert!(n >= 7, "the ring held {n} answers of 4000 bytes");
    let p = numbered(8, n, 4000);
    assert_eq!(
        send(r, &sys, CODEC_OUT, &p, Reply::Must, &mut cur),
        Sent::Full
    );
    // Answered again once the bus has room: behind what was held.
    accept(CODEC_OUT);
    assert_eq!(
        send(r, &sys, CODEC_OUT, &p, Reply::Kept, &mut cur),
        Sent::Written
    );
    let got: Vec<u32> = take_written(CODEC_OUT)
        .iter()
        .map(|(_, p)| number_of(p))
        .collect();
    assert!(got.iter().copied().eq(0..=n));
}

// ── session_processor: every response through the hold, every input gated

/// The body of `fn name` in `src`, to its closing brace at column 0.
fn fn_body<'a>(src: &'a str, name: &str) -> &'a str {
    let start = src
        .find(&format!("fn {name}("))
        .unwrap_or_else(|| panic!("fn {name} is gone"));
    let end = src[start..].find("\n}\n").map_or(src.len(), |e| start + e);
    &src[start..end]
}

/// No response reaches a codec output but through `reply_hold::respond`:
/// the module writes `MSG_SESSION_RESPONSE` nowhere itself, and the
/// emitter that wrote it unheld is gone.
#[test]
fn every_response_leaves_through_the_hold() {
    let code: String = SESSION_PROCESSOR
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    let writes = code.matches("channel_write_msg(").count();
    // The one direct write is an owed DISCONNECT, made only when its
    // output holds nothing for the connection (`flush_holds`).
    let owed_write = {
        let f = fn_body(&code, "flush_holds");
        assert!(f.contains("if reply_hold::ring_holds_for(&s.hold.rings[k], conn) {"));
        let at = code.find(f).expect("flush_holds");
        at..at + f.len()
    };
    let unheld = code
        .match_indices("channel_write_msg(")
        .filter(|(i, _)| !owed_write.contains(i))
        .filter(|(i, _)| {
            code[*i..]
                .lines()
                .take(6)
                .any(|l| l.contains("MSG_SESSION_RESPONSE"))
        })
        .count();
    assert!(writes > 0);
    assert_eq!(unheld, 0, "a MSG_SESSION_RESPONSE written past the hold");
    assert!(!code.contains("emit_codec_response_raw"));
    for emitter in [
        "fn emit_codec_reply(",
        "fn emit_kafka_response(",
        "fn kafka_outbuf_reply(",
        "fn emit_amqp_reply(",
    ] {
        let body = fn_body(
            &code,
            emitter.trim_start_matches("fn ").trim_end_matches('('),
        );
        assert!(
            body.contains("respond("),
            "{emitter} does not go through respond"
        );
    }
    assert!(fn_body(&code, "respond").contains("reply_hold::respond("));
}

/// Every input that answers a client and whose sender holds what it has
/// not delivered (a client's codec envelopes, `flow`'s completions) is read
/// only while the hold has room for its answers. One that cannot wait — a
/// committed entry (no commit waits on a client), Clustor's assignments
/// and refusals (consensus drops what a full edge refuses), a cold read —
/// is never gated: its answers are kept, owed in their operation's slot.
/// The hold drains first in every step, and owed answers next.
#[test]
fn every_answering_input_is_gated_or_keeps_its_answer() {
    let src = SESSION_PROCESSOR;
    // Codec envelopes: per output.
    assert!(fn_body(src, "codec_waits").contains("hold_ready_for("));
    // Completions: the gate precedes the read.
    let at = src
        .find("wire::channel_read_msg(sys, s.in_ack")
        .expect("ack_in is read");
    assert!(src[at.saturating_sub(900)..at].contains("if !hold_ready(s) {"));
    // Never gated on a client.
    assert!(!fn_body(src, "outputs_ready_for_apply").contains("hold_ready"));
    for read in [
        "wire::channel_read_msg(sys, s.in_assigned",
        "wire::channel_read_partitioned(sys, s.in_wal_reply",
    ] {
        let at = src.find(read).expect("the input is read");
        assert!(
            !src[at.saturating_sub(900)..at].contains("hold_ready"),
            "{read} waits on the codec bus"
        );
    }
    // Their answers are kept: the Kafka / AMQP inflight answers through
    // `answer_inflight` (owed on refusal), the cold fetch's slot owes it.
    let answer = fn_body(src, "answer_inflight");
    assert!(answer.contains("store::inflight_owe(&mut s.store, ki, answer);"));
    assert!(answer.contains("Reply::Kept"));
    assert!(fn_body(src, "emit_kafka_produce_response_multi").contains("Reply::Kept"));
    let fail_kin = fn_body(src, "fail_kin");
    assert!(fail_kin.contains("answer_inflight(s, sys, ki, store::ANSWER_NACK)"));
    assert!(fail_kin.contains("answer_inflight(s, sys, ki, store::ANSWER_OFFSET)"));
    assert!(fail_kin.contains("answer_inflight(s, sys, ki, store::ANSWER_PRODUCE)"));
    assert!(!fail_kin.contains("emit_amqp_response("));
    assert!(fn_body(src, "apply_kafka_produce")
        .contains("answer_inflight(s, sys, ki, store::ANSWER_PRODUCE)"));
    assert!(fn_body(src, "emit_cold_fetch_response").contains("Reply::Kept"));
    assert!(src.contains("s.cold[slot].owed = if err == KERR_NONE {"));
    // The expiry sweep answers (and never expires) owed slots.
    let sweep = src
        .find("inflight_expired(&s.store, i, now, KAFKA_INFLIGHT_TIMEOUT_MS)")
        .expect("the expiry sweep");
    let sweep = &src[sweep..sweep + 2000];
    assert!(sweep.contains("store::inflight_owed(&s.store, i) != store::ANSWER_NONE"));
    assert!(sweep.contains("answer_inflight(s, sys, i, store::ANSWER_NACK)"));
    // A DISCONNECT with no room is owed, and nothing follows it.
    let reply = fn_body(src, "emit_codec_reply");
    assert!(reply.contains("reply_hold::owed(&s.disc_owed, conn_id).is_some()"));
    let squashed: String = reply.split_whitespace().collect();
    assert_eq!(
        squashed
            .matches("reply_hold::owe(&muts.disc_owed,conn_id,")
            .count(),
        2,
        "a DISCONNECT the seat ring or the hold refuses is owed"
    );
    // The step drains the hold, then the owed answers, before any input.
    let step = fn_body(src, "module_step");
    let flushed = step
        .find("flush_holds(s, sys);")
        .expect("the step flushes the hold");
    let owed = step
        .find("answer_owed_inflights(s, sys);")
        .expect("the step answers what is owed");
    let phase1 = step.find("Phase 1:").expect("phase 1");
    assert!(flushed < owed && owed < phase1);
    // A closed connection's held responses and owed DISCONNECT are forgotten.
    assert!(src.contains("reply_hold::forget_conn(&mut s.hold, c);"));
    assert!(src.contains("reply_hold::settle(&mut s.disc_owed, c);"));
    // A session with anything held or owed is not quiescent for a handoff.
    let quiet = fn_body(WORKER, "quiescent");
    assert!(quiet.contains("reply_hold::holds_for(&s.hold, conn_id)"));
    assert!(quiet.contains("reply_hold::owed(&s.disc_owed, conn_id).is_some()"));
}

/// The deliveries and resends `session_processor` keeps and offers again
/// are offered (refused while anything is held ahead), never held, so
/// they never overtake a held response nor fill the reserve.
#[test]
fn deliveries_are_offered_not_held() {
    let src = SESSION_PROCESSOR;
    assert!(fn_body(src, "try_deliver").contains("offer_codec_response("));
    let redeliver = fn_body(src, "redeliver_owed");
    assert!(redeliver.contains("Reply::Spare"));
    assert!(!redeliver.contains("emit_codec_response("));
    assert!(fn_body(src, "amqp_delivery_pump").contains("offer_amqp_response("));
}
