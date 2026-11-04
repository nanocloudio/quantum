//! `flow`'s ack component (`modules/app/flow/ack.rs`), mounted as the
//! composite mounts it, driven against a mock `SyscallTable` with
//! byte-FIFO channels.
//!
//! The defect these pin: an entry appended at `(index, term)` that a new
//! leader overwrote before it committed was acknowledged — PUBACK, Kafka
//! `ProduceResponse`, AMQP confirm — once the new log's commit horizon
//! passed its index, because the ack table matched on the index alone.
//! Now a registration carries its term; the component acks only the entry
//! committed at `(index, term)` and answers an overwritten one with a
//! failure status the session state machine fails the operation on.

#![allow(
    dead_code,
    reason = "host test crate; the mounted module and SDK surface is a superset of what the tests touch"
)]
#![allow(
    unsafe_code,
    reason = "drives the PIC component's unsafe entry points against a mock SyscallTable; the unsafety is confined to the module ABI boundary"
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
#[path = "../../../modules/common/types.rs"]
mod types;
mod term {
    include!("../../../modules/common/cores/ack_term_core.rs");
}
#[rustfmt::skip]
#[path = "../../../modules/app/flow/ack.rs"]
mod ack;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, MutexGuard};

// ── Mock kernel ──────────────────────────────────────────────────────────

static MOCK: LazyLock<Mutex<HashMap<i32, Vec<u8>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn mock_reset() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    MOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    guard
}

fn mock() -> MutexGuard<'static, HashMap<i32, Vec<u8>>> {
    MOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

unsafe extern "C" fn mock_channel_read(handle: i32, buf: *mut u8, len: usize) -> i32 {
    let mut m = mock();
    let Some(fifo) = m.get_mut(&handle) else {
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
    let fifo = m.entry(handle).or_default();
    for i in 0..len {
        // SAFETY: the caller's buffer holds `len` bytes.
        fifo.push(unsafe { *data.add(i) });
    }
    len as i32
}

unsafe extern "C" fn mock_channel_discard(handle: i32, len: usize) -> i32 {
    let mut m = mock();
    let fifo = m.entry(handle).or_default();
    let n = len.min(fifo.len());
    fifo.drain(..n);
    len as i32
}

unsafe extern "C" fn mock_channel_poll(handle: i32, events: u32) -> i32 {
    let m = mock();
    let mut ready: u32 = 0x02;
    if m.get(&handle).is_some_and(|f| !f.is_empty()) {
        ready |= 0x01;
    }
    (ready & events) as i32
}

unsafe extern "C" fn mock_channel_peek(handle: i32, buf: *mut u8, len: usize) -> i32 {
    let m = mock();
    let Some(fifo) = m.get(&handle) else {
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

// ── Harness ──────────────────────────────────────────────────────────────

const CH_ACK: i32 = 1;
const CH_REDELIVER: i32 = 2;

/// The ack component, initialised and wired as `flow`'s `module_new`
/// wires it (both outputs on their own channel here, to tell them apart).
struct Flow {
    ack: Box<ack::Ack>,
    sys: SyscallTable,
}

/// One MSG_ACK_EMIT the component wrote.
#[derive(Debug, PartialEq, Eq)]
struct Emit {
    session_slot: u32,
    message_id: u32,
    generation: u32,
    wal_index: u64,
    status: u8,
}

impl Flow {
    fn new() -> Self {
        let layout = std::alloc::Layout::new::<ack::Ack>();
        // SAFETY: `Ack` is plain old data (integers and arrays of them) for
        // which all-zero is a valid value; `init` then sets it up.
        let mut ack = unsafe {
            let p = std::alloc::alloc_zeroed(layout).cast::<ack::Ack>();
            assert!(!p.is_null(), "allocation");
            Box::from_raw(p)
        };
        ack::init(&mut ack);
        ack.out_ack = CH_ACK;
        ack.out_redeliver = CH_REDELIVER;
        Self {
            ack,
            sys: sys_table(),
        }
    }

    /// MSG_ACK_REGISTER, as `session_processor` sends it on an assignment.
    fn register(&mut self, slot: u32, msg: u32, pid: u16, index: u64, entry_term: u64) -> bool {
        let body = term::encode_register(&term::Registration {
            session_slot: slot,
            message_id: msg,
            partition_id: pid,
            wal_index: index,
            session_generation: 9,
            term: entry_term,
        });
        ack::on_register(&mut self.ack, &body, 0)
    }

    /// MSG_DURABILITY_PROOF from `consensus.durable_horizon`.
    fn horizon(&mut self, pid: u16, index: u64, current_term: u64) {
        let mut p = [0u8; wire::DURABILITY_PROOF_LEN];
        p[0..2].copy_from_slice(&pid.to_le_bytes());
        p[2..10].copy_from_slice(&current_term.to_le_bytes());
        p[10..18].copy_from_slice(&index.to_le_bytes());
        p[18] = 0;
        ack::on_durability(&mut self.ack, &p);
    }

    /// MSG_ACK_APPLIED, as `session_processor` reports a run.
    fn applied(&mut self, pid: u16, first: u64, last: u64, run_term: u64) {
        let body = term::encode_applied(
            pid,
            term::Run {
                first,
                last,
                term: run_term,
            },
        );
        ack::on_applied(&mut self.ack, &body);
    }

    /// One `step_acks` sweep, and the completions it wrote.
    fn sweep(&mut self) -> Vec<Emit> {
        // SAFETY: `sys` is the mock table, live for the test.
        unsafe { ack::step_acks(&mut self.ack, &self.sys) };
        let mut out = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            // SAFETY: as above.
            let (mt, plen) = unsafe { wire::channel_read_msg(&self.sys, CH_ACK, &mut buf) };
            if plen <= 0 {
                break;
            }
            assert_eq!(mt, wire::MSG_ACK_EMIT);
            assert_eq!(plen as usize, wire::ACK_EMIT_LEN);
            let b = &buf[..wire::ACK_EMIT_LEN];
            out.push(Emit {
                session_slot: u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
                message_id: u32::from_le_bytes([b[4], b[5], b[6], b[7]]),
                generation: u32::from_le_bytes([b[8], b[9], b[10], b[11]]),
                wal_index: u64::from_le_bytes([
                    b[12], b[13], b[14], b[15], b[16], b[17], b[18], b[19],
                ]),
                status: b[20],
            });
        }
        out
    }

    fn metrics(&self) -> [u8; 36] {
        let mut m = [0u8; 36];
        assert_eq!(ack::metrics(&self.ack, &mut m), 36);
        m
    }
}

fn emit(slot: u32, msg: u32, wal_index: u64, status: u8) -> Emit {
    Emit {
        session_slot: slot,
        message_id: msg,
        generation: 9,
        wal_index,
        status,
    }
}

fn counter(m: &[u8; 36], at: usize) -> u32 {
    u32::from_le_bytes([m[at], m[at + 1], m[at + 2], m[at + 3]])
}

// ── Tests ────────────────────────────────────────────────────────────────

/// Term 2's leader appended the publish at index 5 and was deposed; term
/// 3's leader wrote its own 3..=6 and committed them. The new horizon
/// passes 5 — the term-blind table acked here — but the entry at 5 is term
/// 3's: the component holds it, then fails it once the applied runs name
/// the term there.
#[test]
fn an_entry_a_new_leader_overwrote_is_failed_not_acknowledged() {
    let _g = mock_reset();
    let mut f = Flow::new();
    assert!(f.register(1, 100, 0, 5, 2));
    assert!(f.register(2, 200, 0, 6, 3));
    f.horizon(0, 2, 2);
    assert_eq!(f.sweep(), vec![]);

    f.horizon(0, 6, 3);
    // Term 3's own entry acks on the horizon; the overwritten one does not.
    assert_eq!(f.sweep(), vec![emit(2, 200, 6, term::STATUS_COMMITTED)]);
    assert_eq!(f.sweep(), vec![], "still unresolved, never acked");

    f.applied(0, 1, 2, 2);
    f.applied(0, 3, 6, 3);
    assert_eq!(f.sweep(), vec![emit(1, 100, 5, term::STATUS_SUPERSEDED)]);
    let m = f.metrics();
    assert_eq!(counter(&m, 12), 0, "no inflight left");
    assert_eq!(counter(&m, 28), 1, "one superseded");
    assert_eq!(counter(&m, 32), 0);
}

/// An entry from before the leader change that the new leader carried and
/// committed: the horizon's term cannot vouch for it, the applied run can.
#[test]
fn an_older_terms_entry_committed_by_the_next_leader_is_acknowledged() {
    let _g = mock_reset();
    let mut f = Flow::new();
    assert!(f.register(1, 7, 3, 4, 2));
    f.horizon(3, 6, 3);
    assert_eq!(f.sweep(), vec![]);
    f.applied(3, 1, 4, 2);
    f.applied(3, 5, 6, 3);
    assert_eq!(f.sweep(), vec![emit(1, 7, 4, term::STATUS_COMMITTED)]);
}

/// A run of another term at the index fails the entry at once, before the
/// horizon gets there: the index is another entry's for good.
#[test]
fn an_overwrite_seen_on_apply_fails_before_the_horizon_arrives() {
    let _g = mock_reset();
    let mut f = Flow::new();
    assert!(f.register(4, 1, 0, 8, 5));
    f.applied(0, 7, 9, 6);
    assert_eq!(f.sweep(), vec![emit(4, 1, 8, term::STATUS_SUPERSEDED)]);
}

/// The node applied past the index but holds no run naming its term (the
/// notice was lost): the entry fails as unknown rather than acks.
#[test]
fn an_entry_the_node_cannot_place_fails_as_unknown() {
    let _g = mock_reset();
    let mut f = Flow::new();
    assert!(f.register(4, 2, 0, 5, 1));
    f.applied(0, 1, 3, 1);
    f.applied(0, 8, 9, 2);
    f.horizon(0, 9, 2);
    assert_eq!(f.sweep(), vec![emit(4, 2, 5, term::STATUS_UNKNOWN)]);
    assert_eq!(counter(&f.metrics(), 32), 1);
}

/// The steady state is unchanged: a registration in the current term acks
/// the moment the horizon reaches it, and not a moment before.
#[test]
fn the_steady_state_acks_on_the_horizon_alone() {
    let _g = mock_reset();
    let mut f = Flow::new();
    assert!(f.register(1, 1, 0, 10, 4));
    assert!(f.register(1, 2, 0, 11, 4));
    f.horizon(0, 10, 4);
    assert_eq!(f.sweep(), vec![emit(1, 1, 10, term::STATUS_COMMITTED)]);
    f.horizon(0, 11, 4);
    assert_eq!(f.sweep(), vec![emit(1, 2, 11, term::STATUS_COMMITTED)]);
}

/// A registration that names no entry (no term) is refused, not tracked:
/// nothing could ever prove it committed.
#[test]
fn a_registration_without_a_term_is_refused() {
    let _g = mock_reset();
    let mut f = Flow::new();
    assert!(!f.register(1, 1, 0, 10, 0));
    assert!(!f.register(1, 1, 0, 0, 3));
    f.horizon(0, 10, 0);
    assert_eq!(f.sweep(), vec![]);
    assert_eq!(counter(&f.metrics(), 12), 0);
}
