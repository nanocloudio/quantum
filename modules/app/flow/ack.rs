//! ack — durable-publish inflight tracking, ack emission and redelivery.
//!
//! Inflight entries are keyed by `(partition_id, wal_index)` so durability
//! proofs from different partitions can't alias each other. An entry acks
//! once its partition's durable high-water mark reaches its `wal_index`;
//! entries that age past their backoff are redelivered with exponential
//! backoff, and abandoned after `max_attempts`.
//!
//! ## Per-step bound
//!
//! `on_register` and `on_durability` each handle ONE frame per call
//! ([`REGISTER_BUDGET`] / [`DURABILITY_BUDGET`] per step). `step_acks` and
//! `step_timeouts` each sweep all [`MAX_INFLIGHT`] slots; `step_timeouts`
//! runs at most once per `scan_interval_ms`.

use super::abi::SyscallTable;
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

    /// Per-partition high-water mark of durable wal_index. An inflight
    /// entry acks when `durable_per_partition[entry.partition_id] >=
    /// entry.wal_index`. Indexed by partition_id; out-of-range proofs
    /// are dropped.
    durable_per_partition: [u64; MAX_PARTITIONS],
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
    for i in 0..MAX_INFLIGHT {
        a.entries[i] = InflightEntry::zero();
    }
    for i in 0..MAX_PARTITIONS {
        a.durable_per_partition[i] = 0;
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

/// MSG_ACK_REGISTER: `[session_slot:u32][message_id:u32]`
/// `[partition_id:u16][wal_index:u64]` (18 bytes).
///
/// The session state machine receives MSG_PROPOSAL_ASSIGNED from
/// consensus carrying `(correlation_id, partition_id, wal_index)` and
/// forwards `(session_slot, packet_id, partition_id, wal_index)` here.
///
/// Returns false when the registration was not recorded. A malformed
/// or unroutable payload is refused (there is nothing to track); a
/// full table is refused too, and counted, though the dispatch gate
/// above means a caller should never see it.
pub fn on_register(a: &mut Ack, payload: &[u8], now: u64) -> bool {
    if payload.len() < wire::ACK_REGISTER_LEN {
        return false;
    }
    let session_slot = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let message_id = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
    let partition_id = u16::from_le_bytes([payload[8], payload[9]]);
    let wal_index = u64::from_le_bytes([
        payload[10],
        payload[11],
        payload[12],
        payload[13],
        payload[14],
        payload[15],
        payload[16],
        payload[17],
    ]);
    let session_generation =
        u32::from_le_bytes([payload[18], payload[19], payload[20], payload[21]]);
    if (partition_id as usize) >= MAX_PARTITIONS {
        return false;
    }
    // wal_index == 0 is reserved as "the proposer never received an
    // assignment back" — drop the register rather than create a phantom
    // inflight that could ack against partition 0's first proof. The
    // proposer will time out and retry.
    if wal_index == 0 {
        return false;
    }

    for i in 0..MAX_INFLIGHT {
        if a.entries[i].active == 0 {
            a.entries[i] = InflightEntry {
                session_slot,
                message_id,
                session_generation,
                partition_id,
                wal_index,
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

/// MSG_DURABILITY_PROOF (19 bytes), fanned in from every partition's
/// durability instance. Advances that partition's high-water mark.
pub fn on_durability(a: &mut Ack, payload: &[u8]) {
    if payload.len() < wire::DURABILITY_PROOF_LEN {
        return;
    }
    let (partition_id, _term, index, _replica) =
        wire::decode_durability_proof(&payload[..wire::DURABILITY_PROOF_LEN]);
    if (partition_id as usize) >= MAX_PARTITIONS {
        return;
    }
    if index > a.durable_per_partition[partition_id as usize] {
        a.durable_per_partition[partition_id as usize] = index;
    }
}

/// Emit MSG_ACK_EMIT for every inflight entry whose partition has reached
/// or surpassed its `wal_index`.
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
        if a.entries[i].wal_index > a.durable_per_partition[pid] {
            continue;
        }

        let mut ack = [0u8; wire::ACK_EMIT_LEN];
        ack[0..4].copy_from_slice(&a.entries[i].session_slot.to_le_bytes());
        ack[4..8].copy_from_slice(&a.entries[i].message_id.to_le_bytes());
        ack[8..12].copy_from_slice(&a.entries[i].session_generation.to_le_bytes());
        ack[12..20].copy_from_slice(&a.entries[i].wal_index.to_le_bytes());
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
pub fn metrics(a: &Ack, m: &mut [u8; 28]) -> usize {
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
    28
}
