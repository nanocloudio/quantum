//! ack — durable-publish inflight tracking, ack emission and redelivery.
//!
//! Inflight entries are keyed by `(partition_id, wal_index)` so durability
//! proofs from different partitions can't alias each other, and carry the
//! term their entry was appended in: an index names an entry only with its
//! term, and a leader change can commit another entry at the same index.
//! An entry acks once its partition's commit horizon has reached its
//! `wal_index` AND the entry committed there is provably its own — the
//! horizon's term equals the entry's, or the applied runs
//! `session_processor` reports name that term at that index
//! (`cores/ack_term_core.rs` says why each is proof). An entry another
//! term's entry superseded, or whose term this node cannot name once it has
//! applied past it, is answered with a failure status instead, and the
//! session state machine fails the operation so the client retries.
//! Entries that age past their backoff are redelivered with exponential
//! backoff, and abandoned after `max_attempts`.
//!
//! ## Per-step bound
//!
//! `on_register`, `on_applied` and `on_durability` each handle ONE frame
//! per call ([`REGISTER_BUDGET`] / [`APPLIED_BUDGET`] /
//! [`DURABILITY_BUDGET`] per step). `step_acks` and
//! `step_timeouts` each sweep all [`MAX_INFLIGHT`] slots; `step_timeouts`
//! runs at most once per `scan_interval_ms`.

use super::abi::SyscallTable;
use super::term;
use super::wire;
use super::{dev_log, dev_millis, fmt_u32_raw};

const MAX_INFLIGHT: usize = 2048;

/// Maximum number of raft partitions we track a durable mark for.
/// Matches the engine's `K_MAX` (64) and `session_processor`'s
/// `MAX_APPLY_PARTITIONS`: a partition above this never gets a durable
/// mark, so every QoS 1 write routed to it waits for the redelivery
/// scan instead of its ack — measured at K=64 as an 8 s p50 the moment
/// load spread past sixteen topics.
const MAX_PARTITIONS: usize = 64;

/// Registrations admitted per step. Matches the standalone drain bound.
pub const REGISTER_BUDGET: u8 = 16;
/// Applied-run notices admitted per step. The session state machine sends
/// at most one per partition per flush, plus one per term change.
pub const APPLIED_BUDGET: u8 = 16;
/// Durability proofs admitted per step. Proofs are monotone high-water
/// marks, so deferring the tail to the next step loses nothing and
/// keeps the composite step bounded (standards fluxor-modules.md §8
/// rule 5).
pub const DURABILITY_BUDGET: u8 = 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct InflightEntry {
    session_slot: u32,
    message_id: u32,
    /// Session generation the publish was accepted under; carried back
    /// on MSG_ACK_EMIT so a completion cannot land on a reused slot.
    session_generation: u32,
    partition_id: u16,
    wal_index: u64,
    /// The term the entry was appended in (MSG_PROPOSAL_ASSIGNED).
    term: u64,
    sent_ms: u64,
    backoff_ms: u32,
    attempts: u8,
    active: u8,
}

impl InflightEntry {
    const fn zero() -> Self {
        Self {
            session_slot: 0,
            message_id: 0,
            session_generation: 0,
            partition_id: 0,
            wal_index: 0,
            term: 0,
            sent_ms: 0,
            backoff_ms: 1000,
            attempts: 0,
            active: 0,
        }
    }
}

#[repr(C)]
pub struct Ack {
    pub out_ack: i32,
    pub out_redeliver: i32,

    backoff_base_ms: u32,
    backoff_max_ms: u32,
    max_attempts: u8,
    scan_interval_ms: u32,
    last_scan_ms: u64,

    entries: [InflightEntry; MAX_INFLIGHT],
    acks_emitted: u32,
    /// ACKs whose write did not land in full, leaving the entry active
    /// for a later step. Output-side saturation, the counterpart to
    /// `register_refused` / `full_stalls` on the input side.
    emit_retries: u32,
    redelivers: u32,
    abandoned: u32,
    /// Registrations that reached this component with no slot to hold
    /// them, and steps whose drain stopped because the table was full.
    /// Both are the saturation signal an operator needs: the first must
    /// stay at zero, the second is the bounded backpressure that keeps
    /// it there.
    register_refused: u32,
    full_stalls: u32,
    /// Entries answered as not committed: another term's entry took the
    /// index, or this node applied past it without a run naming its term.
    /// Each is an operation failed for the client to retry; `superseded`
    /// rising follows leader changes, `term_unknown` should stay near 0.
    superseded: u32,
    term_unknown: u32,

    /// Per-partition commit horizon and applied term runs, indexed by
    /// partition_id; out-of-range frames are dropped. An inflight entry
    /// resolves against its partition's view (`term::PartitionTerms::
    /// resolve`).
    parts: [term::PartitionTerms; MAX_PARTITIONS],
}

pub fn init(a: &mut Ack) {
    a.out_ack = -1;
    a.out_redeliver = -1;
    a.backoff_base_ms = 1000;
    a.backoff_max_ms = 60_000;
    a.max_attempts = 10;
    a.scan_interval_ms = 100;
    a.last_scan_ms = 0;
    a.acks_emitted = 0;
    a.emit_retries = 0;
    a.redelivers = 0;
    a.abandoned = 0;
    a.register_refused = 0;
    a.full_stalls = 0;
    a.superseded = 0;
    a.term_unknown = 0;
    for i in 0..MAX_INFLIGHT {
        a.entries[i] = InflightEntry::zero();
    }
    for i in 0..MAX_PARTITIONS {
        a.parts[i] = term::PartitionTerms::new();
    }
}

/// Count one step whose registration drain stopped on a full table.
pub fn note_full_stall(a: &mut Ack) {
    a.full_stalls = a.full_stalls.wrapping_add(1);
}

/// True when every inflight slot is taken. The dispatch table checks
/// this before consuming a record: a registration this component
/// cannot hold must stay in the channel, where it backpressures the
/// session state machine, rather than being read and discarded.
pub fn is_full(a: &Ack) -> bool {
    !(0..MAX_INFLIGHT).any(|i| a.entries[i].active == 0)
}

/// MSG_ACK_REGISTER (`wire::ACK_REGISTER_LEN`, encoded by
/// `term::encode_register`): `[session_slot:u32][message_id:u32]
/// [partition_id:u16][wal_index:u64][session_generation:u32][term:u64]`.
///
/// The session state machine receives MSG_PROPOSAL_ASSIGNED from
/// consensus carrying `(correlation_id, partition_id, wal_index, term)`
/// and forwards `(session_slot, packet_id, partition_id, wal_index, term)`
/// here.
///
/// A proposal Clustor REFUSED (MSG_PROPOSAL_REFUSED on the same port) is
/// never registered: no assignment follows a refusal, so the session state
/// machine frees its correlation and fails the operation itself
/// (`session_processor::on_proposal_refused`) and nothing reaches this
/// table to wait for a durability proof that cannot come. Registration
/// therefore stays exactly "one per assignment", and a refusal costs this
/// component no slot and no redelivery.
///
/// Returns false when the registration was not recorded. A malformed
/// or unroutable payload is refused (there is nothing to track) — index 0
/// is reserved as "the proposer never received an assignment back", and
/// no Raft entry has term 0, so neither can name an entry to wait for and
/// acking one against partition 0's first proof would be a phantom; the
/// proposer times out and retries. A full table is refused too, and
/// counted, though the dispatch gate above means a caller should never
/// see it.
pub fn on_register(a: &mut Ack, payload: &[u8], now: u64) -> bool {
    let Some(r) = term::decode_register(payload) else {
        return false;
    };
    if (r.partition_id as usize) >= MAX_PARTITIONS {
        return false;
    }

    for i in 0..MAX_INFLIGHT {
        if a.entries[i].active == 0 {
            a.entries[i] = InflightEntry {
                session_slot: r.session_slot,
                message_id: r.message_id,
                session_generation: r.session_generation,
                partition_id: r.partition_id,
                wal_index: r.wal_index,
                term: r.term,
                sent_ms: now,
                backoff_ms: a.backoff_base_ms,
                attempts: 1,
                active: 1,
            };
            return true;
        }
    }
    a.register_refused = a.register_refused.wrapping_add(1);
    false
}

/// MSG_ACK_APPLIED (`wire::ACK_APPLIED_LEN`) from the session state
/// machine: the partition's committed entries `first..=last` carry `term`
/// and have applied. Recorded in that partition's term runs; a malformed
/// or out-of-range frame is dropped.
pub fn on_applied(a: &mut Ack, payload: &[u8]) {
    let Some((pid, run)) = term::decode_applied(payload) else {
        return;
    };
    if (pid as usize) < MAX_PARTITIONS {
        a.parts[pid as usize].note_applied(run);
    }
}

/// MSG_DURABILITY_PROOF (19 bytes) carrying a partition's COMMIT horizon
/// — `consensus.durable_horizon`: `[partition_id][current_term]
/// [commit_index][replica]`, one per partition on this node. Advances that
/// partition's horizon. A quorum-durability proof from the ledger is not
/// a commit and must not be wired here (`cores/ack_term_core.rs`).
pub fn on_durability(a: &mut Ack, payload: &[u8]) {
    if payload.len() < wire::DURABILITY_PROOF_LEN {
        return;
    }
    let (partition_id, term, index, _replica) =
        wire::decode_durability_proof(&payload[..wire::DURABILITY_PROOF_LEN]);
    if (partition_id as usize) >= MAX_PARTITIONS {
        return;
    }
    a.parts[partition_id as usize].note_horizon(index, term);
}

/// Emit MSG_ACK_EMIT for every inflight entry its partition's view has
/// resolved: status `term::STATUS_COMMITTED` for an entry committed at its
/// `(wal_index, term)`, a failure status for one superseded or unknown.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn step_acks(a: &mut Ack, sys: &SyscallTable) {
    for i in 0..MAX_INFLIGHT {
        if a.entries[i].active != 1 {
            continue;
        }
        let pid = a.entries[i].partition_id as usize;
        if pid >= MAX_PARTITIONS {
            continue;
        }
        let verdict = a.parts[pid].resolve(a.entries[i].wal_index, a.entries[i].term);
        let Some(status) = verdict.status() else {
            continue;
        };

        let ack = term::encode_emit(
            a.entries[i].session_slot,
            a.entries[i].message_id,
            a.entries[i].session_generation,
            a.entries[i].wal_index,
            status,
        );
        if a.out_ack >= 0 {
            // The entry is released only once the ACK is provably on the
            // channel. `channel_poll` reports writability, not success —
            // a short or refused write with the entry already cleared
            // would drop the client's only completion. On failure the
            // entry stays active and the next step retries it.
            // SAFETY: caller guarantees `sys` is live.
            unsafe {
                let poll_out = (sys.channel_poll)(a.out_ack, 0x02);
                if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                    let want = (wire::ENVELOPE_HDR + ack.len()) as i32;
                    let wrote = wire::channel_write_msg(sys, a.out_ack, wire::MSG_ACK_EMIT, &ack);
                    if wrote == want {
                        a.entries[i].active = 0;
                        a.acks_emitted = a.acks_emitted.wrapping_add(1);
                        match verdict {
                            term::Verdict::Superseded => {
                                a.superseded = a.superseded.wrapping_add(1);
                                dev_log(sys, 2, b"[flow] ack superseded".as_ptr(), 21);
                            }
                            term::Verdict::Unknown => {
                                a.term_unknown = a.term_unknown.wrapping_add(1);
                                dev_log(sys, 2, b"[flow] ack term unknown".as_ptr(), 23);
                            }
                            _ => {}
                        }
                        // One in 32: register-to-ack age, the wait for the
                        // durable mark to pass the entry's index.
                        if a.acks_emitted & 31 == 0 {
                            let age = dev_millis(sys).wrapping_sub(a.entries[i].sent_ms);
                            let mut line = [0u8; 48];
                            let mut pos = 0usize;
                            for &b in b"[flow] ack age ms=" {
                                line[pos] = b;
                                pos += 1;
                            }
                            pos += fmt_u32_raw(
                                line.as_mut_ptr().add(pos),
                                age.min(u32::MAX as u64) as u32,
                            );
                            dev_log(sys, 3, line.as_ptr(), pos);
                        }
                    } else {
                        a.emit_retries = a.emit_retries.wrapping_add(1);
                    }
                }
            }
        } else {
            a.entries[i].active = 0;
        }
    }
}

/// Periodic timeout scan: redeliver aged entries with exponential
/// backoff, abandon past `max_attempts`.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn step_timeouts(a: &mut Ack, sys: &SyscallTable, now: u64) {
    if now.wrapping_sub(a.last_scan_ms) < a.scan_interval_ms as u64 {
        return;
    }
    a.last_scan_ms = now;
    for i in 0..MAX_INFLIGHT {
        if a.entries[i].active == 0 {
            continue;
        }
        let age = now.wrapping_sub(a.entries[i].sent_ms);
        if age < a.entries[i].backoff_ms as u64 {
            continue;
        }
        if a.entries[i].attempts >= a.max_attempts {
            a.entries[i].active = 0;
            a.abandoned = a.abandoned.wrapping_add(1);
            continue;
        }
        let mut r = [0u8; 8];
        r[0..4].copy_from_slice(&a.entries[i].session_slot.to_le_bytes());
        r[4..8].copy_from_slice(&a.entries[i].message_id.to_le_bytes());
        if a.out_redeliver >= 0 {
            // SAFETY: caller guarantees `sys` is live.
            unsafe {
                let poll_out = (sys.channel_poll)(a.out_redeliver, 0x02);
                if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                    wire::channel_write_msg(sys, a.out_redeliver, wire::MSG_ACK_REDELIVER, &r);
                    a.redelivers = a.redelivers.wrapping_add(1);
                }
            }
        }
        a.entries[i].sent_ms = now;
        a.entries[i].attempts = a.entries[i].attempts.wrapping_add(1);
        a.entries[i].backoff_ms = a.entries[i]
            .backoff_ms
            .saturating_mul(2)
            .min(a.backoff_max_ms);
    }
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(a: &Ack, m: &mut [u8; 36]) -> usize {
    m[0..4].copy_from_slice(&a.acks_emitted.to_le_bytes());
    m[4..8].copy_from_slice(&a.redelivers.to_le_bytes());
    m[8..12].copy_from_slice(&a.abandoned.to_le_bytes());
    let mut inflight = 0u32;
    for i in 0..MAX_INFLIGHT {
        if a.entries[i].active == 1 {
            inflight += 1;
        }
    }
    m[12..16].copy_from_slice(&inflight.to_le_bytes());
    m[16..20].copy_from_slice(&a.register_refused.to_le_bytes());
    m[20..24].copy_from_slice(&a.full_stalls.to_le_bytes());
    m[24..28].copy_from_slice(&a.emit_retries.to_le_bytes());
    m[28..32].copy_from_slice(&a.superseded.to_le_bytes());
    m[32..36].copy_from_slice(&a.term_unknown.to_le_bytes());
    36
}
