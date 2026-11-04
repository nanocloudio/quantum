//! What a failed proposal frees in `session_processor`: its correlation
//! table (`modules/app/session_processor/correlate.rs`) and its durable-
//! publish inflight table (`store.rs`), both mounted verbatim and driven
//! through the call sequence `session_processor` makes on a refusal
//! (`MSG_PROPOSAL_REFUSED`) or a failed verdict (`MSG_ACK_EMIT` with a
//! superseded / unknown status).
//!
//! What these pin:
//!
//! - MQTT: a refused or superseded PUBLISH leaves its correlation free and
//!   owns no publish stash. The stash is the apply path's, for an entry
//!   that committed, and frees only on its dedup verdict and the topic bus
//!   (`finalise_stash`) — never on the client's flow failing. Releasing it
//!   there lost the message: its dedup key is filed, so the resend reads
//!   DUPLICATE and is never fanned out.
//! - Kafka produce: a partition failed before it was proposed counts as
//!   applied (it has nothing to apply), so a produce mixing failed and
//!   proposed partitions is answered instead of waiting for the 8 s sweep;
//!   and a partition's apply counts once, so a refused partition whose
//!   `OUTCOME_UNKNOWN` entry later commits does not release the response
//!   before another partition's offset is known.

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
    reason = "a table that has no slot where the sequence needs one fails the test, which is what expect says"
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

// The constants the mounted tables read from their parent, as
// `session_processor/mod.rs` defines them. `the_mirrored_constants_match`
// fails the moment one drifts.
const MAX_PENDING_CORRELATIONS: usize = 1024;
const STASH_SLOTS: usize = 32;
const MAX_STASH_ENV: usize = wire::QOS1_OP_MAX;
const STASH_DEDUP_PENDING: u8 = 0;
const STASH_DEDUP_OK: u8 = 1;
const STASH_DEDUP_DUPLICATE: u8 = 2;
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

/// `session_processor` op codes the correlation carries.
const OP_PUBLISH: u8 = 0;

const SESSION_PROCESSOR: &str = include_str!("../../../modules/app/session_processor/mod.rs");

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

fn correlate_table() -> Box<correlate::Correlate> {
    let mut c = zeroed::<correlate::Correlate>();
    correlate::init(&mut c);
    c
}

fn store_table() -> Box<store::Store> {
    let mut s = zeroed::<store::Store>();
    store::init(&mut s);
    s
}

fn dedup_key(n: u8) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[0] = 0xD5;
    k[19] = n;
    k
}

/// `apply_qop_publish` for a committed QoS 1 entry whose flow's inflight
/// carries `cid`: open a stash, store the op-body, mark it durable.
fn apply_committed(c: &mut correlate::Correlate, cid: u64, key: &[u8; 20], env: &[u8]) -> usize {
    let slot = correlate::stash_alloc(c, cid, key).expect("a stash slot is free");
    assert!(correlate::stash_set_env(c, slot, env));
    correlate::stash_mark_durable(c, slot);
    slot
}

/// `MSG_DEDUP_RESULT` then `release_ready_stashes` with the topic bus
/// open: returns the envelope each released stash fanned out, in order.
fn verdict(c: &mut correlate::Correlate, key: &[u8; 20], state: u8) -> Vec<Vec<u8>> {
    let slot = correlate::stash_by_dedup_key(c, key).expect("the verdict finds its stash");
    correlate::stash_set_dedup_state(c, slot, state);
    let mut fanned = Vec::new();
    while let Some(ready) = correlate::stash_oldest_ready(c) {
        if correlate::stash_dedup_state(c, ready) == STASH_DEDUP_OK {
            fanned.push(correlate::stash_env(c, ready).to_vec());
        }
        correlate::stash_release(c, ready);
    }
    fanned
}

// ── MQTT: the correlation and the publish stash ──────────────────────────

#[test]
fn a_refused_publish_frees_its_correlation_and_owns_no_stash() {
    let mut c = correlate_table();
    // Fill the table: every slot of capacity held by a proposal in flight.
    let mut cids = Vec::new();
    while let Some(cid) = correlate::allocate(&mut c, 7, cids.len() as u16, OP_PUBLISH, 0) {
        cids.push(cid);
    }
    assert_eq!(cids.len(), PENDING_ACK_SLOTS);
    assert!(correlate::allocate(&mut c, 7, 0, OP_PUBLISH, 0).is_none());
    // Clustor refuses every one: `on_proposal_refused` takes each.
    for (packet, &cid) in cids.iter().enumerate() {
        assert_eq!(
            correlate::take(&mut c, cid),
            Some((7, packet as u16, OP_PUBLISH))
        );
        // A refused proposal is in no log, so nothing applied it and no
        // stash names it.
        assert!(correlate::stash_by_correlation(&c, cid).is_none());
    }
    assert_eq!(correlate::stash_free(&c), STASH_SLOTS);
    // The whole capacity is the clients' again.
    for i in 0..PENDING_ACK_SLOTS {
        assert!(correlate::allocate(&mut c, 7, i as u16, OP_PUBLISH, 0).is_some());
    }
}

#[test]
fn a_committed_entry_under_a_failed_flows_correlation_still_fans_out() {
    let mut c = correlate_table();
    // The resend of packet 5 after a reconnect is proposed under `cid`;
    // the earlier attempt commits late and applies while the resend's
    // inflight carries `cid`, so its stash is filed under it.
    let cid = correlate::allocate(&mut c, 3, 5, OP_PUBLISH, 0).expect("capacity");
    let key = dedup_key(5);
    let env = b"QOP_PUBLISH op-body of packet 5".to_vec();
    apply_committed(&mut c, cid, &key, &env);
    // The resend is refused (or its verdict comes back superseded /
    // unknown): its correlation goes, the flow fails, and the stash is
    // left alone — it is a committed entry.
    assert!(correlate::take(&mut c, cid).is_some());
    assert!(correlate::stash_by_correlation(&c, cid).is_some());
    // Its verdict delivers it and frees the slot.
    assert_eq!(verdict(&mut c, &key, STASH_DEDUP_OK), vec![env]);
    assert!(correlate::stash_by_correlation(&c, cid).is_none());
    assert_eq!(correlate::stash_free(&c), STASH_SLOTS);
    // The client's next resend commits too and is the duplicate the
    // dedup table says it is: freed, not delivered twice.
    let again = correlate::allocate(&mut c, 3, 5, OP_PUBLISH, 0).expect("capacity");
    apply_committed(&mut c, again, &key, b"QOP_PUBLISH op-body of packet 5");
    assert!(verdict(&mut c, &key, STASH_DEDUP_DUPLICATE).is_empty());
    assert_eq!(correlate::stash_free(&c), STASH_SLOTS);
}

#[test]
fn every_stash_frees_on_its_verdict_after_its_flows_fail_and_is_reused() {
    let mut c = correlate_table();
    // Every stash slot held by a committed entry whose flow then fails.
    let mut keys = Vec::new();
    for n in 0..STASH_SLOTS {
        let cid = correlate::allocate(&mut c, 1, n as u16, OP_PUBLISH, 0).expect("capacity");
        let key = dedup_key(n as u8);
        apply_committed(&mut c, cid, &key, &[n as u8; 24]);
        assert!(correlate::take(&mut c, cid).is_some());
        keys.push(key);
    }
    assert_eq!(correlate::stash_free(&c), 0);
    // The verdicts free every one, in the order they became ready.
    let mut fanned = Vec::new();
    for (n, key) in keys.iter().enumerate() {
        let state = if n % 3 == 0 {
            STASH_DEDUP_DUPLICATE
        } else {
            STASH_DEDUP_OK
        };
        fanned.extend(verdict(&mut c, key, state));
    }
    let expect: Vec<Vec<u8>> = (0..STASH_SLOTS)
        .filter(|n| n % 3 != 0)
        .map(|n| vec![n as u8; 24])
        .collect();
    assert_eq!(fanned, expect);
    assert_eq!(correlate::stash_free(&c), STASH_SLOTS);
    // And the slots take the next entries.
    for n in 0..STASH_SLOTS {
        assert!(correlate::stash_alloc(&mut c, 0, &dedup_key(100 + n as u8)).is_some());
    }
}

/// The invariant the two tests above rely on, held at its source: in
/// `session_processor`, a publish stash is released only by
/// `finalise_stash` — its verdict and the topic bus — never by a refusal,
/// a failed verdict or any other client-side path.
#[test]
fn only_finalise_stash_releases_a_publish_stash() {
    let start = SESSION_PROCESSOR
        .find("unsafe fn finalise_stash(")
        .expect("finalise_stash is defined");
    let end = start
        + SESSION_PROCESSOR[start..]
            .find("\n}\n")
            .expect("finalise_stash ends");
    let mut at = 0;
    let mut n = 0;
    while let Some(i) = SESSION_PROCESSOR[at..].find("correlate::stash_release(") {
        let pos = at + i;
        assert!(
            (start..end).contains(&pos),
            "a stash is released outside finalise_stash at byte {pos}: {}",
            &SESSION_PROCESSOR[SESSION_PROCESSOR[..pos].rfind('\n').unwrap_or(0)..pos + 40]
        );
        n += 1;
        at = pos + 1;
    }
    assert!(n > 0, "finalise_stash releases its stash");
}

// ── Kafka produce: per-partition resolution ──────────────────────────────

fn open_produce(s: &mut store::Store, n_parts: usize, failed: &[(usize, i16)]) -> usize {
    let (ki, _) = store::inflight_alloc(s).expect("a slot");
    store::inflight_open(
        s,
        ki,
        store::InflightOpen {
            conn_id: 9,
            proto: KIN_PROTO_KAFKA,
            topic_len: 1,
            n_parts: n_parts as u8,
            // As `handle_kafka_produce` opens it: the partitions failed
            // before proposing are already resolved on durability.
            n_done: failed.len() as u8,
            api_ver: 3,
            acks: -1,
            kafka_corr: 1,
            channel: 0,
            delivery_tag: 0,
            ts_ms: 0,
        },
        b"t",
    );
    for pi in 0..n_parts {
        let err = failed.iter().find(|(p, _)| *p == pi).map_or(0, |(_, e)| *e);
        store::inflight_set_part(s, ki, pi, pi as i32, err);
    }
    ki
}

/// A partition's quorum durability (`MSG_ACK_EMIT`, committed).
fn durable(s: &mut store::Store, ki: usize) {
    store::inflight_complete_part(s, ki);
}

/// `fail_kin` for a produce partition: refused, superseded or unknown.
fn fail_part(s: &mut store::Store, ki: usize, pidx: usize, err: i16) {
    store::inflight_set_part_err(s, ki, pidx, err);
    store::inflight_complete_part(s, ki);
    store::inflight_apply_part(s, ki, pidx);
}

#[test]
fn a_produce_mixing_failed_and_proposed_partitions_is_answered() {
    let mut s = store_table();
    // Partition 1 was refused before proposing (not this node's).
    let ki = open_produce(&mut s, 3, &[(1, 6)]);
    assert!(!store::inflight_ready(&s, ki));
    for pidx in [0, 2] {
        durable(&mut s, ki);
        store::inflight_apply_part(&mut s, ki, pidx);
    }
    assert!(store::inflight_ready(&s, ki), "every partition resolved");
}

#[test]
fn a_failed_partition_whose_entry_applies_anyway_counts_once() {
    let mut s = store_table();
    let ki = open_produce(&mut s, 2, &[]);
    // Partition 0's proposal is refused OUTCOME_UNKNOWN …
    fail_part(&mut s, ki, 0, 7);
    // … and its entry commits after all and applies.
    store::inflight_apply_part(&mut s, ki, 0);
    // Partition 1 is durable but its offset is not known yet: the response
    // must wait for it.
    durable(&mut s, ki);
    assert!(!store::inflight_ready(&s, ki));
    store::inflight_apply_part(&mut s, ki, 1);
    assert!(store::inflight_ready(&s, ki));
}

#[test]
fn an_applied_partition_failed_by_its_verdict_counts_once() {
    let mut s = store_table();
    let ki = open_produce(&mut s, 2, &[]);
    // Partition 0 applies, then its verdict comes back unknown.
    store::inflight_apply_part(&mut s, ki, 0);
    fail_part(&mut s, ki, 0, 7);
    durable(&mut s, ki);
    assert!(
        !store::inflight_ready(&s, ki),
        "partition 1 has not applied"
    );
    store::inflight_apply_part(&mut s, ki, 1);
    assert!(store::inflight_ready(&s, ki));
    store::inflight_free(&mut s, ki);
    // The slot is reusable at once, under a new epoch.
    let (again, slot) = store::inflight_alloc(&mut s).expect("the slot is free");
    assert_eq!(again, ki);
    assert_eq!(store::kin_decode_slot(slot).0, ki);
}

// ── Drift guard ──────────────────────────────────────────────────────────

#[test]
fn the_mirrored_constants_match() {
    for line in [
        "const MAX_PENDING_CORRELATIONS: usize = 1024;",
        "const STASH_SLOTS: usize = 32;",
        "const MAX_STASH_ENV: usize = wire::QOS1_OP_MAX;",
        "const STASH_DEDUP_PENDING: u8 = 0;",
        "const STASH_DEDUP_OK: u8 = 1;",
        "const STASH_DEDUP_DUPLICATE: u8 = 2;",
        "const PENDING_DLV_SLOTS: usize = 8;",
        "const MAX_PENDING_DLV: usize = wire::DELIVERY_ENVELOPE_MAX;",
        "const PENDING_ACK_SLOTS: usize = MAX_PENDING_CORRELATIONS;",
        "const KAFKA_SLOT_BASE: u32 = 0x4000_0000;",
        "const KAFKA_INFLIGHT: usize = 256;",
        "const KIN_KI_MASK: u32 = 0xFF;",
        "const KIN_EPOCH_SHIFT: u32 = 8;",
        "const KIN_EPOCH_MASK: u32 = 0x3F_FFFF;",
        "const KAFKA_MAX_TOPIC: usize = 64;",
        "const KIN_PROTO_KAFKA: u8 = 1;",
        "const KIN_MAX_PARTS: usize = 16;",
        "const KSTORE_PARTS: usize = 24;",
        "const KSTORE_BYTES: usize = 16384;",
        "const KENTRY_HDR: usize = 2 + 1 + 8 + 4 + 8;",
        "const KFLAG_BATCH: u8 = 1;",
        "const KWRAP: u16 = 0xFFFF;",
        "const OP_PUBLISH: u8 = 0;",
    ] {
        assert!(
            SESSION_PROCESSOR.lines().any(|l| l.trim() == line),
            "session_processor/mod.rs no longer defines `{line}`"
        );
    }
}

/// A Kafka / AMQP answer the codec bus had no room for when it was
/// decided is owed in its inflight slot (`answer_inflight`): the slot
/// stays live and carries the answer until it is written, and a reused
/// slot starts owing nothing — a stale answer never reaches the next
/// request.
#[test]
fn an_owed_answer_keeps_its_slot_and_dies_with_it() {
    let mut s = store_table();
    let ki = open_produce(&mut s, 1, &[]);
    assert_eq!(store::inflight_owed(&s, ki), store::ANSWER_NONE);
    store::inflight_complete_part(&mut s, ki);
    store::inflight_apply_part(&mut s, ki, 0);
    assert!(store::inflight_ready(&s, ki));
    // The response is decided; the bus refused it.
    store::inflight_owe(&mut s, ki, store::ANSWER_PRODUCE);
    assert_eq!(store::inflight_owed(&s, ki), store::ANSWER_PRODUCE);
    assert!(store::inflight_get(&s, ki).is_some(), "the slot is kept");
    // Written later: freed, and its answer goes with it.
    store::inflight_free(&mut s, ki);
    assert_eq!(store::inflight_owed(&s, ki), store::ANSWER_NONE);
    // The slot's next occupant owes nothing.
    let mut reused = None;
    for _ in 0..KAFKA_INFLIGHT {
        let k = open_produce(&mut s, 1, &[]);
        if k == ki {
            reused = Some(k);
            break;
        }
    }
    let k = reused.expect("the slot is reused");
    assert_eq!(store::inflight_owed(&s, k), store::ANSWER_NONE);
    // The answer kinds are distinct, and none reads as "nothing owed".
    let kinds = [
        store::ANSWER_PRODUCE,
        store::ANSWER_OFFSET,
        store::ANSWER_ACK,
        store::ANSWER_NACK,
    ];
    for (i, a) in kinds.iter().enumerate() {
        assert_ne!(*a, store::ANSWER_NONE);
        assert!(!kinds[i + 1..].contains(a));
    }
}
