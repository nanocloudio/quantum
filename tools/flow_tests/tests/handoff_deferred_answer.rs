//! A Kafka connection moved between session workers while a request of
//! its is still owed an answer: `session_processor`'s inflight table
//! (`modules/app/session_processor/store.rs`) and the codec's order core
//! (`modules/common/cores/kafka_order_core.rs`) mounted verbatim, the
//! anchor's swap and the two workers played by the test.
//!
//! The defect these pin: the worker's drain gate (`worker::quiescent`)
//! found no MQTT session on a Kafka connection and declared it
//! quiescent, so an anchor swap exported it while a Produce (or an
//! OffsetCommit, or a cold Fetch) was still awaiting durability. The
//! answer exists only on the exporting worker — its correlation, its
//! place in the connection's request order, the partition outcome — and
//! once the connection moved that worker was frozen and fenced by the
//! anchor, so the answer never reached the client; the codec's sequencer
//! then held every later answer behind it and closed the connection at
//! its 15 s backstop.
//!
//! What these pin:
//!
//! - a connection with a live inflight slot (Kafka Produce or
//!   OffsetCommit, AMQP confirm, decided or not, owed or not) is not
//!   quiescent, and only that connection;
//! - with the gate, a swap under a Produce in flight answers every
//!   request in order — the exporting worker's owed answer first, the
//!   requests the anchor held during the drain by the worker it moved
//!   to — and never closes the connection;
//! - without it (the old gate) the same sequence loses the answer and
//!   closes the connection at the backstop, which is the bug;
//! - a durability that outlasts the drain deadline refuses the handoff:
//!   the same worker keeps the connection and answers in turn;
//! - the module wires the gate: `quiescent` asks `answer_deferred_for`
//!   before the session check, and that covers inflight and cold-fetch
//!   slots, each of which records the connection it answers.

#![allow(
    dead_code,
    reason = "host test crate; the mounted tables and SDK surface are a superset of what the tests touch"
)]
#![allow(
    unsafe_code,
    reason = "the mounted SDK is unsafe by its ABI, and the tables are allocated zeroed on the heap as the module's state is"
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
    reason = "a table with no slot where the sequence needs one, or a module source not as it must be, fails the test, which is what expect and panic say"
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
#[path = "../../../modules/common/cores/kafka_log_core.rs"]
mod kafka_log;
#[rustfmt::skip]
#[path = "../../../modules/common/cores/kafka_order_core.rs"]
mod kafka_order;
#[rustfmt::skip]
#[path = "../../../modules/app/session_processor/store.rs"]
mod store;

// The constants the mounted table reads from its parent, as
// `session_processor/mod.rs` defines them (`failed_proposal_release.rs`
// guards the drift).
const KAFKA_SLOT_BASE: u32 = 0x4000_0000;
const KAFKA_INFLIGHT: usize = 256;
const KIN_KI_MASK: u32 = 0xFF;
const KIN_EPOCH_SHIFT: u32 = 8;
const KIN_EPOCH_MASK: u32 = 0x3F_FFFF;
const KAFKA_MAX_TOPIC: usize = 64;
const KIN_PROTO_KAFKA: u8 = 1;
const KIN_PROTO_AMQP: u8 = 2;
const KIN_MAX_PARTS: usize = 16;
const KSTORE_PARTS: usize = 24;
const KSTORE_BYTES: usize = 16384;
const KENTRY_HDR: usize = 2 + 1 + 8 + 4 + 8;
const KFLAG_BATCH: u8 = 1;
const KWRAP: u16 = 0xFFFF;

const SESSION_PROCESSOR: &str = include_str!("../../../modules/app/session_processor/mod.rs");
const WORKER: &str = include_str!("../../../modules/app/session_processor/worker.rs");

/// `worker::QUIET_STEPS`: quiet steps before a draining attachment may
/// export.
const QUIET_STEPS: u32 = 8;
/// The codec's backstop for an answer that never comes (`protocol`'s
/// Kafka head timeout).
const HEAD_TIMEOUT_MS: u64 = 15_000;
/// `session_drain_ms`: the anchor refuses a handoff that has not
/// exported by then.
const DRAIN_MS: u64 = 500;

const CONN: u16 = 7;

/// A zeroed heap allocation of `T`, as the module's state starts.
fn zeroed<T>() -> Box<T> {
    let layout = std::alloc::Layout::new::<T>();
    // SAFETY: both tables are plain integer arrays, for which all-zero is a
    // valid value; the layout is non-zero-sized and the pointer is checked.
    unsafe {
        let p = std::alloc::alloc_zeroed(layout).cast::<T>();
        assert!(!p.is_null(), "allocation failed");
        Box::from_raw(p)
    }
}

fn store_table() -> Box<store::Store> {
    let mut s = zeroed::<store::Store>();
    store::init(&mut s);
    s
}

/// `handle_kafka_produce` / `handle_amqp_publish` / OffsetCommit opening
/// a durability-gated slot for `conn`.
fn open(s: &mut store::Store, conn: u16, proto: u8, corr: i32) -> usize {
    let (ki, _) = store::inflight_alloc(s).expect("a slot");
    store::inflight_open(
        s,
        ki,
        store::InflightOpen {
            conn_id: conn,
            proto,
            topic_len: 1,
            n_parts: 1,
            n_done: 0,
            api_ver: 3,
            acks: -1,
            kafka_corr: corr,
            channel: 1,
            delivery_tag: 1,
            ts_ms: 0,
        },
        b"t",
    );
    store::inflight_set_part(s, ki, 0, 0, 0);
    ki
}

#[test]
fn a_live_slot_keeps_only_its_own_connection_from_quiescence() {
    let mut s = store_table();
    assert!(!store::inflight_for_conn(&s, CONN));
    let produce = open(&mut s, CONN, KIN_PROTO_KAFKA, 1);
    let other = open(&mut s, CONN + 1, KIN_PROTO_AMQP, 1);
    assert!(store::inflight_for_conn(&s, CONN));
    assert!(store::inflight_for_conn(&s, CONN + 1));
    assert!(!store::inflight_for_conn(&s, CONN + 2));

    // Durable and applied: the answer is decided, and the bus refused
    // it — still owed, still this worker's to give.
    store::inflight_complete_part(&mut s, produce);
    store::inflight_apply_part(&mut s, produce, 0);
    assert!(store::inflight_ready(&s, produce));
    store::inflight_owe(&mut s, produce, store::ANSWER_PRODUCE);
    assert!(store::inflight_for_conn(&s, CONN));

    // Written: the slot frees, and so does the connection.
    store::inflight_free(&mut s, produce);
    assert!(!store::inflight_for_conn(&s, CONN));
    assert!(
        store::inflight_for_conn(&s, CONN + 1),
        "another connection's slot is its own"
    );
    store::inflight_free(&mut s, other);
    assert!(!store::inflight_for_conn(&s, CONN + 1));
}

#[test]
fn every_slot_counts_not_just_the_first() {
    let mut s = store_table();
    let mut last = 0;
    for i in 0..KAFKA_INFLIGHT {
        last = open(&mut s, 100 + (i as u16 % 3), KIN_PROTO_KAFKA, i as i32);
    }
    for i in 0..KAFKA_INFLIGHT - 1 {
        store::inflight_free(&mut s, i);
    }
    // Only the last slot is live, for the connection it was opened for.
    let conn = 100 + ((KAFKA_INFLIGHT - 1) as u16 % 3);
    for c in 100..103 {
        assert_eq!(store::inflight_for_conn(&s, c), c == conn, "conn {c}");
    }
    store::inflight_free(&mut s, last);
    assert!((100..103).all(|c| !store::inflight_for_conn(&s, c)));
}

// ── The swap, played out ─────────────────────────────────────────────

/// Which worker the anchor binds the connection to, and where its
/// handoff stands.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bound {
    /// Worker 0, the drain started at this time (ms).
    Draining(u64),
    /// Worker 0, attached (the handoff was refused, or never ran).
    Old,
    /// Worker 1, after the export and RESUMED.
    New,
}

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    /// The correlation ids the client read, in order.
    read: Vec<i32>,
    /// The connection was closed by the codec (the 15 s backstop).
    closed: bool,
    /// Where the connection ended up.
    bound: Bound,
}

/// One connection through the codec's sequencer while the anchor swaps
/// its worker. The client pipelines a Produce (1, durability-gated on
/// worker 0) and a Fetch (2, answered at once by worker 0); the swap
/// starts; the client sends a ListOffsets (3), which the anchor holds
/// while the connection is not live. Worker 0's Produce turns durable
/// and applies at `durable_at` ms. `gate` is the drain gate: whether a
/// draining connection with no MQTT session may export, given worker
/// 0's inflight table.
fn swap_under_produce(durable_at: u64, gate: impl Fn(&store::Store, u16) -> bool) -> Outcome {
    let mut seq = kafka_order::Seq::zero();
    let mut pool = zeroed::<kafka_order::Pool>();
    kafka_order::pool_init(&mut pool);
    let mut old = store_table();
    let mut read = Vec::new();
    let mut closed = false;

    let write = |read: &mut Vec<i32>, frame: &[u8]| {
        read.push(i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]));
        true
    };
    let frame = |corr: i32| corr.to_be_bytes().to_vec();

    // Request 1: Produce. The oldest takes no slot.
    let slot = kafka_order::reserve(&seq, &mut pool, kafka_order::class_for(0)).expect("room");
    kafka_order::push(&mut seq, 1, slot, 0);
    let produce = open(&mut old, CONN, KIN_PROTO_KAFKA, 1);
    // Request 2: Fetch, answered at once by worker 0; it waits its turn.
    let slot = kafka_order::reserve(&seq, &mut pool, kafka_order::class_for(1)).expect("room");
    kafka_order::push(&mut seq, 2, slot, 0);
    let r = kafka_order::answer(&mut seq, &mut pool, 2, Some(&frame(2)), 0, |f| {
        write(&mut read, f)
    });
    assert_eq!(r, kafka_order::Answered::Kept);

    // The swap starts at 1 ms. Request 3 arrives at 2 ms and is held by
    // the anchor (the connection is not live while it drains).
    let mut bound = Bound::Draining(1);
    let slot = kafka_order::reserve(&seq, &mut pool, kafka_order::class_for(2)).expect("room");
    kafka_order::push(&mut seq, 3, slot, 2);
    let mut held = vec![3];
    let mut quiet = 0u32;
    let mut produce_answered = false;

    for now in 1..=(HEAD_TIMEOUT_MS + 1_000) {
        // Worker 0's Produce resolves: durable, applied, answered by it.
        // The anchor relays it only while worker 0 is the one bound;
        // after the move it is a stale emitter and is fenced.
        if now == durable_at && !produce_answered {
            store::inflight_complete_part(&mut old, produce);
            store::inflight_apply_part(&mut old, produce, 0);
            assert!(store::inflight_ready(&old, produce));
            if bound != Bound::New {
                kafka_order::answer(&mut seq, &mut pool, 1, Some(&frame(1)), now, |f| {
                    write(&mut read, f)
                });
            }
            store::inflight_free(&mut old, produce);
            produce_answered = true;
            quiet = 0;
        }
        match bound {
            Bound::Draining(since) => {
                quiet += 1;
                if quiet >= QUIET_STEPS && gate(&old, CONN) {
                    // DRAINED + export; RESUMED on worker 1; the anchor
                    // releases what it held to worker 1, which answers.
                    bound = Bound::New;
                    for corr in held.drain(..) {
                        kafka_order::answer(
                            &mut seq,
                            &mut pool,
                            corr,
                            Some(&frame(corr)),
                            now,
                            |f| write(&mut read, f),
                        );
                    }
                } else if now.saturating_sub(since) > DRAIN_MS {
                    // Refused: worker 0 is resumed at the current epoch
                    // and gets what the anchor held.
                    bound = Bound::Old;
                    for corr in held.drain(..) {
                        kafka_order::answer(
                            &mut seq,
                            &mut pool,
                            corr,
                            Some(&frame(corr)),
                            now,
                            |f| write(&mut read, f),
                        );
                    }
                }
            }
            Bound::Old | Bound::New => {}
        }
        match kafka_order::flush(&mut seq, &mut pool, now, |f| write(&mut read, f)) {
            kafka_order::Front::Close => {
                closed = true;
                break;
            }
            _ => {}
        }
        if kafka_order::overdue(&seq, now, HEAD_TIMEOUT_MS) {
            closed = true;
            break;
        }
        if kafka_order::is_idle(&seq) && produce_answered && held.is_empty() {
            break;
        }
    }
    Outcome {
        read,
        closed,
        bound,
    }
}

/// The gate `quiescent` applies to a connection with no MQTT session.
fn gate(s: &store::Store, conn: u16) -> bool {
    !store::inflight_for_conn(s, conn)
}

#[test]
fn a_swap_under_a_produce_answers_every_request_in_order() {
    let o = swap_under_produce(40, gate);
    assert_eq!(
        o,
        Outcome {
            read: vec![1, 2, 3],
            closed: false,
            bound: Bound::New
        }
    );
}

#[test]
fn without_the_gate_the_answer_is_fenced_and_the_connection_closes() {
    // The old gate: no MQTT session, so quiescent after QUIET_STEPS.
    let o = swap_under_produce(40, |_, _| true);
    assert!(o.closed, "the head waited past the backstop: {o:?}");
    assert!(o.read.is_empty(), "nothing overtook the lost answer: {o:?}");
    assert_eq!(o.bound, Bound::New);
}

#[test]
fn a_durability_past_the_drain_deadline_keeps_the_worker_and_answers() {
    let o = swap_under_produce(DRAIN_MS + 300, gate);
    assert_eq!(
        o,
        Outcome {
            read: vec![1, 2, 3],
            closed: false,
            bound: Bound::Old
        }
    );
}

#[test]
fn a_produce_resolved_before_the_drain_moves_at_once() {
    let o = swap_under_produce(1, gate);
    assert_eq!(o.read, vec![1, 2, 3]);
    assert!(!o.closed);
    assert_eq!(o.bound, Bound::New);
}

// ── The module wires it ──────────────────────────────────────────────

fn body_of<'a>(src: &'a str, sig: &str) -> &'a str {
    let start = src.find(sig).unwrap_or_else(|| panic!("`{sig}` not found"));
    let rest = &src[start..];
    let end = rest.find("\n}\n").unwrap_or(rest.len());
    &rest[..end]
}

#[test]
fn quiescent_waits_for_deferred_answers_before_the_session_check() {
    let q = body_of(WORKER, "fn quiescent(");
    let gate = q
        .find("super::answer_deferred_for(s, conn_id)")
        .unwrap_or_else(|| panic!("quiescent no longer asks answer_deferred_for"));
    let session = q
        .find("sessions::find_by_conn")
        .unwrap_or_else(|| panic!("quiescent no longer checks the session"));
    assert!(
        gate < session,
        "the deferred-answer gate must hold a session-less connection too"
    );
}

#[test]
fn the_gate_covers_inflight_and_cold_fetch_slots() {
    let g = body_of(SESSION_PROCESSOR, "fn answer_deferred_for(");
    assert!(g.contains("store::inflight_for_conn(&s.store, conn)"));
    assert!(g.contains("s.cold.iter().any(|c| c.active == 1 && c.conn_id == conn)"));
    // Every slot records the connection it answers.
    let park = body_of(SESSION_PROCESSOR, "unsafe fn try_park_cold_fetch(");
    assert!(park.contains("c.conn_id = conn_id;"));
    let open = body_of(
        include_str!("../../../modules/app/session_processor/store.rs"),
        "pub fn inflight_open(",
    );
    assert!(open.contains("e.conn_id = o.conn_id;"));
}
