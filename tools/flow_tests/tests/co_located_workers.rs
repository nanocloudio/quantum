//! Two `session_processor` workers on one node, both serving Kafka and
//! AMQP: what keeps one worker's answers off the other's clients.
//!
//! Both workers apply every op that names no session (the Kafka log,
//! committed offsets, group records, AMQP messages), and Clustor's
//! `proposal_assigned` / `MSG_PROPOSAL_REFUSED`, flow's `MSG_ACK_EMIT` and
//! durability's `cold_read_reply` are fanned to both. So every id a
//! worker matches an answer by must be its own:
//!
//! - correlation ids (`correlate.rs`): a namespace byte per worker, apart
//!   from `session_directory`'s and `operations`';
//! - durable-publish slots (`store.rs`): the worker's id in the epoch's top
//!   bits, so `inflight_valid` fails for the other worker's slot at the
//!   same index and reuse count;
//! - the produce origin (`wire::QOP_KAFKA_PRODUCE`): node AND worker;
//! - cold-read request ids: a namespace per worker.
//!
//! And state one worker owns for the pair (the coordinator's: a Kafka
//! group, committed offsets, an AMQP queue's cursor and consumers) is
//! answered as coordinator answers (`wire::MSG_COORD_RESPONSE`), which
//! are in no delivery cursor: they never hold a handoff, and a close
//! forgets them without touching the session's own held responses.
//!
//! The tables are mounted verbatim; what lives in `mod.rs` is pinned by
//! source guards.

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
    reason = "a table that has no slot where the sequence needs one, or a source not as it must be, fails the test, which is what expect says"
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
#[path = "../../../modules/app/session_processor/correlate.rs"]
mod correlate;
#[rustfmt::skip]
#[path = "../../../modules/app/session_processor/store.rs"]
mod store;
#[rustfmt::skip]
#[path = "../../../modules/app/session_processor/reply_hold.rs"]
mod reply_hold;

// The constants the mounted tables read from their parent, as
// `session_processor/mod.rs` defines them (`failed_proposal_release.rs`
// guards the same mirror against drift).
const MAX_PENDING_CORRELATIONS: usize = 1024;
const STASH_SLOTS: usize = 32;
const MAX_STASH_ENV: usize = wire::QOS1_OP_MAX;
const STASH_DEDUP_PENDING: u8 = 0;
const PENDING_DLV_SLOTS: usize = 8;
const MAX_PENDING_DLV: usize = wire::DELIVERY_ENVELOPE_MAX;
const PENDING_ACK_SLOTS: usize = MAX_PENDING_CORRELATIONS;
const KAFKA_SLOT_BASE: u32 = 0x4000_0000;
const KAFKA_INFLIGHT: usize = 256;
const KIN_KI_MASK: u32 = 0xFF;
const KIN_EPOCH_SHIFT: u32 = 8;
const KIN_EPOCH_MASK: u32 = 0x3F_FFFF;
const KAFKA_MAX_TOPIC: usize = 64;
const KIN_PROTO_KAFKA: u8 = 1;
const KIN_MAX_PARTS: usize = 16;
const KSTORE_PARTS: usize = 24;
const KSTORE_BYTES: usize = 16384;
const KENTRY_HDR: usize = 2 + 1 + 8 + 4 + 8;
const KFLAG_BATCH: u8 = 1;
const KWRAP: u16 = 0xFFFF;

const SESSION_PROCESSOR: &str = include_str!("../../../modules/app/session_processor/mod.rs");
const WORKER: &str = include_str!("../../../modules/app/session_processor/worker.rs");

/// A zeroed heap allocation of `T`, as the module's state starts.
fn zeroed<T>() -> Box<T> {
    let layout = std::alloc::Layout::new::<T>();
    // SAFETY: the tables are plain integer arrays, for which all-zero is a
    // valid value; the layout is non-zero-sized and the pointer is checked.
    unsafe {
        let p = std::alloc::alloc_zeroed(layout).cast::<T>();
        assert!(!p.is_null(), "allocation failed");
        Box::from_raw(p)
    }
}

/// Worker `w`'s durable-publish table, as `module_new` builds it.
fn store_of(w: u8) -> Box<store::Store> {
    let mut s = zeroed::<store::Store>();
    store::init(&mut s);
    store::set_worker(&mut s, w);
    s
}

/// Worker `w`'s correlation table, as `module_new` builds it.
fn correlate_of(w: u8) -> Box<correlate::Correlate> {
    let mut c = zeroed::<correlate::Correlate>();
    correlate::init(&mut c);
    correlate::set_namespace(&mut c, w);
    c
}

fn open(s: &mut store::Store, ki: usize, conn: u16) {
    store::inflight_open(
        s,
        ki,
        store::InflightOpen {
            conn_id: conn,
            proto: KIN_PROTO_KAFKA,
            topic_len: 1,
            n_parts: 1,
            n_done: 0,
            api_ver: 3,
            acks: -1,
            kafka_corr: 7,
            channel: 0,
            delivery_tag: 0,
            ts_ms: 0,
        },
        b"t",
    );
}

/// The item starting at `from` in `src`, to its closing brace at column 0.
fn item<'a>(src: &'a str, from: &str) -> &'a str {
    let at = src.find(from).expect("the item is in the source");
    let rest = &src[at..];
    let end = rest.find("\n}\n").map_or(rest.len(), |e| e + 2);
    &rest[..end]
}

// ── Durable-publish slots ────────────────────────────────────────────────

#[test]
fn the_same_slot_on_two_workers_is_two_wire_slots() {
    let mut w0 = store_of(0);
    let mut w1 = store_of(1);
    let (k0, ss0) = store::inflight_alloc(&mut w0).expect("a free slot");
    let (k1, ss1) = store::inflight_alloc(&mut w1).expect("a free slot");
    // Same index, same reuse count: only the worker tells them apart.
    assert_eq!(k0, k1);
    assert_ne!(ss0, ss1, "one wire slot for two workers' requests");
    assert_eq!(store::kin_slot_worker(ss0), 0);
    assert_eq!(store::kin_slot_worker(ss1), 1);
    open(&mut w0, k0, 3);
    open(&mut w1, k1, 4);
    let (_, e0) = store::kin_decode_slot(ss0);
    let (_, e1) = store::kin_decode_slot(ss1);
    // A completion is fanned to both; each resolves only its own.
    assert!(store::inflight_valid(&w0, k0, e0));
    assert!(
        !store::inflight_valid(&w0, k0, e1),
        "worker 0 took worker 1's completion"
    );
    assert!(store::inflight_valid(&w1, k1, e1));
    assert!(
        !store::inflight_valid(&w1, k1, e0),
        "worker 1 took worker 0's completion"
    );
    // Still in the durable-publish namespace, below bit 30's routing.
    for ss in [ss0, ss1] {
        assert!(ss >= KAFKA_SLOT_BASE && ss < 2 * KAFKA_SLOT_BASE);
    }
}

#[test]
fn a_slots_reuse_count_wraps_below_the_worker_id() {
    let mut w = store_of(5);
    let top = (5u32 << store::KIN_EPOCH_WORKER_SHIFT) | store::KIN_EPOCH_COUNT_MASK;
    w.inflight[0].epoch = top;
    let (ki, ss) = store::inflight_alloc(&mut w).expect("a free slot");
    assert_eq!(ki, 0);
    let (_, epoch) = store::kin_decode_slot(ss);
    assert_eq!(
        epoch >> store::KIN_EPOCH_WORKER_SHIFT,
        5,
        "the wrap reached the worker id"
    );
    assert_eq!(epoch & store::KIN_EPOCH_COUNT_MASK, 0);
    assert_eq!(store::kin_slot_worker(ss), 5);
    // The 22-bit epoch field holds both: the worker field is 3 bits.
    assert_eq!(
        store::KIN_EPOCH_WORKER_SHIFT + 3,
        KIN_EPOCH_MASK.count_ones()
    );
}

#[test]
fn a_coordinator_calls_slot_never_holds_a_handoff() {
    let mut w = store_of(0);
    let (ki, _) = store::inflight_alloc(&mut w).expect("a free slot");
    open(&mut w, ki, 9);
    assert!(
        store::inflight_for_conn(&w, 9),
        "a session's own slot holds its handoff"
    );
    store::inflight_set_coord(&mut w, ki);
    assert!(store::inflight_coord(&w, ki));
    assert!(
        !store::inflight_for_conn(&w, 9),
        "the coordinator's answer does not move"
    );
    // Reopened for the next request, the mark is gone.
    store::inflight_free(&mut w, ki);
    let (kj, _) = store::inflight_alloc(&mut w).expect("a free slot");
    open(&mut w, kj, 9);
    assert!(!store::inflight_coord(&w, kj));
    assert!(store::inflight_for_conn(&w, 9));
}

// ── Correlation ids ──────────────────────────────────────────────────────

#[test]
fn correlation_ids_are_disjoint_across_workers_and_the_other_proposers() {
    let mut c0 = correlate_of(0);
    let mut c1 = correlate_of(1);
    let a = correlate::allocate(&mut c0, 1, 1, 0, 0).expect("room");
    let b = correlate::allocate(&mut c1, 2, 1, 0, 0).expect("room");
    assert_ne!(
        a, b,
        "both workers counting from 1 bound each other's assignments"
    );
    assert_eq!(correlate::cid_namespace(a), correlate::CID_NAMESPACE);
    assert_eq!(correlate::cid_namespace(b), correlate::CID_NAMESPACE | 1);
    // The assignment for worker 0's proposal reaches worker 1 too: it is
    // not worker 1's to take.
    assert!(correlate::take(&mut c1, a).is_none());
    assert!(correlate::take(&mut c0, a).is_some());
    // session_directory issues `replica_id << 56`, operations sets bit 63.
    for cid in [a, b] {
        assert!(cid >> 56 > 3, "inside a replica id's namespace");
        assert_eq!(cid >> 63, 0, "inside operations' namespace");
    }
    // A reset keeps the worker's identity.
    correlate::reset(&mut c1);
    let c = correlate::allocate(&mut c1, 2, 1, 0, 0).expect("room");
    assert_eq!(correlate::cid_namespace(c), correlate::CID_NAMESPACE | 1);
}

#[test]
fn consecutive_ids_stay_in_the_workers_namespace() {
    let mut c = correlate_of(2);
    let mut last = 0u64;
    for _ in 0..4 {
        let cid = correlate::allocate(&mut c, 1, 1, 0, 0).expect("room");
        assert_eq!(correlate::cid_namespace(cid), correlate::CID_NAMESPACE | 2);
        assert!(cid & correlate::CID_COUNTER_MASK != 0, "counter 0 issued");
        assert_ne!(cid, last);
        last = cid;
        assert!(correlate::take(&mut c, cid).is_some());
    }
}

// ── The produce origin ───────────────────────────────────────────────────

#[test]
fn a_produce_names_the_node_and_the_worker_that_accepted_it() {
    let mut buf = [0u8; 64];
    let n = wire::encode_produce_body_hdr(&mut buf, 2, 1, 7, b"orders");
    assert_eq!(n, wire::PRODUCE_BODY_HDR + 6);
    buf[n..n + 3].copy_from_slice(b"rec");
    let h = wire::decode_produce_body_hdr(&buf[..n + 3]).expect("a header");
    assert_eq!(
        h,
        wire::ProduceBodyHdr {
            origin: 2,
            origin_worker: 1,
            partition: 7,
            topic_len: 6,
            records_at: n,
        }
    );
    assert_eq!(&buf[wire::PRODUCE_BODY_HDR..h.records_at], b"orders");
    // A body shorter than the topic it declares is not one.
    assert!(wire::decode_produce_body_hdr(&buf[..n - 1]).is_none());
    assert!(wire::decode_produce_body_hdr(&buf[..3]).is_none());
}

#[test]
fn only_the_accepting_worker_of_the_accepting_node_resolves_a_produce() {
    let f = item(SESSION_PROCESSOR, "unsafe fn apply_kafka_produce(");
    assert!(f.contains(
        "origin != wire::PRODUCE_ORIGIN_NONE && origin == s.self_id && origin_worker == s.worker_id"
    ));
    let p = item(SESSION_PROCESSOR, "unsafe fn handle_kafka_produce(");
    assert!(
        p.contains("s.self_id,\n            s.worker_id,"),
        "the propose side stamps both"
    );
    // The cold read finds the records past the same header.
    assert!(SESSION_PROCESSOR.contains("wire::decode_produce_body_hdr(&body[h..])"));
}

// ── Apply ownership ──────────────────────────────────────────────────────

#[test]
fn every_worker_applies_the_ops_that_name_no_session() {
    let f = item(SESSION_PROCESSOR, "fn op_names_no_session(");
    for op in [
        "QOP_KAFKA_PRODUCE",
        "QOP_KAFKA_OFFSET",
        "QOP_KAFKA_GROUP_GEN",
        "QOP_KAFKA_GROUP_MEMBER",
        "QOP_AMQP_PUBLISH",
    ] {
        assert!(f.contains(op), "{op} is not applied by every worker");
    }
    for op in ["QOP_CONNECT", "QOP_PUBLISH", "QOP_SUBSCRIBE", "QOP_PUBREL"] {
        assert!(!f.contains(&format!("wire::{op}\n")) && !f.contains(&format!("wire::{op} ")));
    }
    let mine = item(SESSION_PROCESSOR, "fn entry_is_mine(");
    assert!(mine.contains("s.co_located_workers == 0 || op_names_no_session(op)"));
    // The apply loop takes them before the slot ownership, skip marks and
    // import floors: no session's slot filters the log.
    let at = SESSION_PROCESSOR
        .find("} else if op_names_no_session(p.op) {")
        .expect("the loop routes the log's ops first");
    let own = SESSION_PROCESSOR
        .find("} else if !entry_is_mine(s, p.op, p.session_slot) {")
        .expect("then ownership");
    let floor = SESSION_PROCESSOR[own..]
        .find("replay_floor_filters(")
        .expect("then floors")
        + own;
    assert!(at < own && own < floor);
}

#[test]
fn every_id_a_worker_matches_by_is_its_own() {
    assert!(SESSION_PROCESSOR.contains("correlate::set_namespace(&mut s.correlate, s.worker_id);"));
    assert!(SESSION_PROCESSOR.contains("store::set_worker(&mut s.store, s.worker_id);"));
    assert!(SESSION_PROCESSOR.contains("s.next_cold_id = cold_id_base(s.worker_id) | 1;"));
    assert!(SESSION_PROCESSOR.contains(
        "cold_id_base(s.worker_id) | (s.next_cold_id.wrapping_add(1) & COLD_ID_MASK).max(1);"
    ));
}

// ── Coordinator answers ──────────────────────────────────────────────────

#[test]
fn a_coordinator_answer_is_not_the_sessions_and_a_close_forgets_only_it() {
    let mut h = zeroed::<reply_hold::Hold>();
    reply_hold::init(&mut h);
    let conn = 5u16;
    assert!(reply_hold::push(
        &mut h.rings[0],
        conn,
        wire::MSG_COORD_RESPONSE,
        b"join-ok",
        false
    ));
    assert!(
        !reply_hold::holds_for(&h, conn),
        "a held coordinator answer held the session's handoff"
    );
    assert!(reply_hold::push(
        &mut h.rings[0],
        conn,
        wire::MSG_SESSION_RESPONSE,
        b"produce-ok",
        true
    ));
    assert!(
        reply_hold::holds_for(&h, conn),
        "the session's own response holds it"
    );
    // The coordinator hears the close: its answers go, the session's stay
    // for the session's own close notice.
    reply_hold::forget_coord(&mut h, conn);
    assert!(reply_hold::holds_for(&h, conn));
    assert!(reply_hold::ring_holds_for(&h.rings[0], conn));
    reply_hold::forget_conn(&mut h, conn);
    assert!(!reply_hold::holds_for(&h, conn));
}

#[test]
fn the_worker_answers_a_coordinator_call_as_one_and_counts_every_session_response() {
    let r = item(SESSION_PROCESSOR, "unsafe fn respond(");
    assert!(r.contains("let (msg_type, counted) = if s.coord_reply {"));
    assert!(r.contains("(wire::MSG_COORD_RESPONSE, false)"));
    // Kafka and AMQP responses are counted: the anchor counts every one it
    // relays, and a handoff admits only equal counts.
    assert_eq!(
        SESSION_PROCESSOR.matches(", false).taken()").count(),
        0,
        "an uncounted session response refuses every handoff of its connection"
    );
    // A coordinator call is taken whatever the binding: unwrapped before
    // the frozen check and the cursor, its close releasing only what the
    // coordinator holds.
    let call = SESSION_PROCESSOR
        .find("if mt == wire::MSG_COORD_CALL {")
        .expect("the drain takes coordinator calls");
    let frozen = SESSION_PROCESSOR
        .find("if plen >= 2 && !s.coord_reply {")
        .expect("the cursor skips them");
    assert!(call < frozen);
    let body = &SESSION_PROCESSOR[call..frozen];
    assert!(body.contains("reply_hold::forget_coord(&mut s.hold, c);"));
    assert!(body.contains("handle_conn_disconnect(s, c);"));
    assert!(
        !body.contains("client_gone"),
        "a coordinator close ended a binding"
    );
    // Later answers to a coordinator call go back as one.
    let a = item(SESSION_PROCESSOR, "unsafe fn answer_inflight(");
    assert!(a.contains("s.coord_reply = e.coord == 1;"));
    assert!(SESSION_PROCESSOR.contains("s.coord_reply = view.coord;"));
    // The drain gate reads the session's holds only.
    assert!(WORKER.contains("super::reply_hold::holds_for(&s.hold, conn_id)"));
}
