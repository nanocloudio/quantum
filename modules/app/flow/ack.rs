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

const MAX_INFLIGHT: usize = 2048;

/// Maximum number of partitions we can track durability for. Matches
/// `partition_router::MAX_LOCAL_PARTITIONS` for now; if the router grows
/// beyond that (e.g. via tree composition or multi-tenant raft), bump
/// this in lockstep.
const MAX_PARTITIONS: usize = 16;

/// Registrations admitted per step. Matches the standalone drain bound.
pub const REGISTER_BUDGET: u8 = 16;
/// Durability proofs admitted per step. The standalone module drained
/// these in an unbounded loop; proofs are monotone high-water marks, so
/// deferring the tail to the next step loses nothing and gives the
/// composite a bounded step (standards fluxor-modules.md §8 rule 5).
pub const DURABILITY_BUDGET: u8 = 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct InflightEntry {
    session_slot: u32,
    message_id: u32,
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
    redelivers: u32,
    abandoned: u32,

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
    a.redelivers = 0;
    a.abandoned = 0;
    for i in 0..MAX_INFLIGHT {
        a.entries[i] = InflightEntry::zero();
    }
    for i in 0..MAX_PARTITIONS {
        a.durable_per_partition[i] = 0;
    }
}

/// MSG_ACK_REGISTER: `[session_slot:u32][message_id:u32]`
/// `[partition_id:u16][wal_index:u64]` (18 bytes).
///
/// The session state machine receives MSG_PROPOSAL_ASSIGNED from
/// consensus carrying `(correlation_id, partition_id, wal_index)` and
/// forwards `(session_slot, packet_id, partition_id, wal_index)` here.
pub fn on_register(a: &mut Ack, payload: &[u8], now: u64) {
    if payload.len() < 18 {
        return;
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
    if (partition_id as usize) >= MAX_PARTITIONS {
        return;
    }
    // wal_index == 0 is reserved as "the proposer never received an
    // assignment back" — drop the register rather than create a phantom
    // inflight that could ack against partition 0's first proof. The
    // proposer will time out and retry.
    if wal_index == 0 {
        return;
    }

    for i in 0..MAX_INFLIGHT {
        if a.entries[i].active == 0 {
            a.entries[i] = InflightEntry {
                session_slot,
                message_id,
                partition_id,
                wal_index,
                sent_ms: now,
                backoff_ms: a.backoff_base_ms,
                attempts: 1,
                active: 1,
            };
            return;
        }
    }
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

        let mut ack = [0u8; 8];
        ack[0..4].copy_from_slice(&a.entries[i].session_slot.to_le_bytes());
        ack[4..8].copy_from_slice(&a.entries[i].message_id.to_le_bytes());
        if a.out_ack >= 0 {
            // SAFETY: caller guarantees `sys` is live.
            unsafe {
                let poll_out = (sys.channel_poll)(a.out_ack, 0x02);
                if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                    wire::channel_write_msg(sys, a.out_ack, wire::MSG_ACK_EMIT, &ack);
                    a.entries[i].active = 0;
                    a.acks_emitted = a.acks_emitted.wrapping_add(1);
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
pub fn metrics(a: &Ack, m: &mut [u8; 24]) -> usize {
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
    16
}
