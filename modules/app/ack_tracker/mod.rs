//! ACK Tracker — Per-message inflight tracking with exponential-backoff redelivery.
//!
//! Consumes DurabilityProof from durability_ledger (cross-core) and
//! inflight registrations from session_processor. Maps durability proofs
//! to protocol ACKs and emits them back to session_processor. Periodic
//! timer scan detects timeouts and triggers redelivery.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK is include!'d wholesale and each module consumes only a subset; pending upstream allow attributes in target/fluxor/fluxor-abi/sdk/"
)]

use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "see file-level allow: SDK surface is shared across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

const MAX_INFLIGHT: usize = 2048;

/// Maximum number of partitions we can track durability for. Matches
/// `partition_router::MAX_LOCAL_PARTITIONS` for now; if the router
/// grows beyond 4 (e.g. via tree composition or multi-tenant raft),
/// bump this in lockstep.
const MAX_PARTITIONS: usize = 16;

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
            session_slot: 0, message_id: 0, partition_id: 0, wal_index: 0,
            sent_ms: 0, backoff_ms: 1000, attempts: 0, active: 0,
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_durability: i32,   // in[0]: 19-byte DurabilityProof, fan-in from every partition's durability_ledger
    in_register: i32,     // in[1]: InflightRegister from session_processor
    out_ack: i32,         // out[0]: ack emit to session_processor
    out_redeliver: i32,   // out[1]: redeliver signal to session_processor
    out_metrics: i32,     // out[2]: metrics to metrics_aggregator

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

    buf: [u8; 256],
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 { core::mem::size_of::<ModuleState>() as u32 }

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32, out_chan: i32, _ctrl_chan: i32,
    _params: *const u8, _params_len: usize,
    state: *mut u8, state_size: usize, syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() { return -1; }
        if state_size < core::mem::size_of::<ModuleState>() { return -2; }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_durability = in_chan;
        s.out_ack = out_chan;
        s.in_register = dev_channel_port(sys, 0, 1);
        s.out_redeliver = dev_channel_port(sys, 1, 1);
        s.out_metrics = dev_channel_port(sys, 1, 2);
        s.backoff_base_ms = 1000;
        s.backoff_max_ms = 60_000;
        s.max_attempts = 10;
        s.scan_interval_ms = 100;

        for i in 0..MAX_INFLIGHT {
            s.entries[i] = InflightEntry::zero();
        }
        for i in 0..MAX_PARTITIONS {
            s.durable_per_partition[i] = 0;
        }

        dev_log(sys, 3, b"[ack] init".as_ptr(), 9);
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // Drain inflight registrations.
        //
        // session_processor receives MSG_PROPOSAL_ASSIGNED from
        // raft_engine carrying `(correlation_id, partition_id,
        // wal_index)` and forwards `(session_slot, packet_id,
        // partition_id, wal_index)` here as MSG_ACK_REGISTER (18
        // bytes). Inflight entries are keyed by `(partition_id,
        // wal_index)` so cross-partition durability proofs don't
        // alias each other.
        if s.in_register >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_register, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_register, &mut s.buf);
                // register_in shares the session_processor.forward_out bus
                // with MSG_LAG_SIGNAL; ignore anything that isn't an ack
                // registration.
                if mt != wire::MSG_ACK_REGISTER { continue; }
                let plen = plen as usize;
                if plen < 18 { continue; }

                let session_slot = u32::from_le_bytes([
                    s.buf[0], s.buf[1], s.buf[2], s.buf[3],
                ]);
                let message_id = u32::from_le_bytes([
                    s.buf[4], s.buf[5], s.buf[6], s.buf[7],
                ]);
                let partition_id = u16::from_le_bytes([s.buf[8], s.buf[9]]);
                let wal_index = u64::from_le_bytes([
                    s.buf[10], s.buf[11], s.buf[12], s.buf[13],
                    s.buf[14], s.buf[15], s.buf[16], s.buf[17],
                ]);
                if (partition_id as usize) >= MAX_PARTITIONS { continue; }
                // wal_index == 0 is reserved as "the proposer never
                // received an assignment back" — drop the register
                // rather than create a phantom inflight that could
                // ack against partition 0's first proof. The proposer
                // will time out and retry.
                if wal_index == 0 { continue; }

                for i in 0..MAX_INFLIGHT {
                    if s.entries[i].active == 0 {
                        s.entries[i] = InflightEntry {
                            session_slot, message_id, partition_id, wal_index,
                            sent_ms: now, backoff_ms: s.backoff_base_ms,
                            attempts: 1, active: 1,
                        };
                        break;
                    }
                }
            }
        }

        // Drain durability proofs (19 bytes each, fan-in from every
        // per-partition durability_ledger) and advance the
        // partition's high-water mark.
        if s.in_durability >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_durability, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, plen) = wire::channel_read_msg(sys, s.in_durability, &mut s.buf);
                let plen = plen as usize;
                if plen < wire::DURABILITY_PROOF_LEN { continue; }

                let (partition_id, _term, index, _replica) =
                    wire::decode_durability_proof(&s.buf[..wire::DURABILITY_PROOF_LEN]);
                if (partition_id as usize) >= MAX_PARTITIONS { continue; }
                if index > s.durable_per_partition[partition_id as usize] {
                    s.durable_per_partition[partition_id as usize] = index;
                }
            }

            // Emit ACK for every inflight entry whose partition has
            // reached or surpassed its wal_index.
            for i in 0..MAX_INFLIGHT {
                if s.entries[i].active != 1 { continue; }
                let pid = s.entries[i].partition_id as usize;
                if pid >= MAX_PARTITIONS { continue; }
                if s.entries[i].wal_index > s.durable_per_partition[pid] { continue; }

                let mut ack = [0u8; 8];
                ack[0..4].copy_from_slice(&s.entries[i].session_slot.to_le_bytes());
                ack[4..8].copy_from_slice(&s.entries[i].message_id.to_le_bytes());
                if s.out_ack >= 0 {
                    let poll_out = (sys.channel_poll)(s.out_ack, 0x02);
                    if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                        wire::channel_write_msg(sys, s.out_ack, wire::MSG_ACK_EMIT, &ack);
                        s.entries[i].active = 0;
                        s.acks_emitted = s.acks_emitted.wrapping_add(1);
                    }
                } else {
                    s.entries[i].active = 0;
                }
            }
        }

        // Periodic timeout scan
        if now.wrapping_sub(s.last_scan_ms) >= s.scan_interval_ms as u64 {
            s.last_scan_ms = now;
            for i in 0..MAX_INFLIGHT {
                if s.entries[i].active == 0 { continue; }
                let age = now.wrapping_sub(s.entries[i].sent_ms);
                if age >= s.entries[i].backoff_ms as u64 {
                    if s.entries[i].attempts >= s.max_attempts {
                        s.entries[i].active = 0;
                        s.abandoned = s.abandoned.wrapping_add(1);
                        continue;
                    }
                    // Emit redeliver signal
                    let mut r = [0u8; 8];
                    r[0..4].copy_from_slice(&s.entries[i].session_slot.to_le_bytes());
                    r[4..8].copy_from_slice(&s.entries[i].message_id.to_le_bytes());
                    if s.out_redeliver >= 0 {
                        let poll_out = (sys.channel_poll)(s.out_redeliver, 0x02);
                        if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                            wire::channel_write_msg(sys, s.out_redeliver, wire::MSG_ACK_REDELIVER, &r);
                            s.redelivers = s.redelivers.wrapping_add(1);
                        }
                    }
                    s.entries[i].sent_ms = now;
                    s.entries[i].attempts = s.entries[i].attempts.wrapping_add(1);
                    s.entries[i].backoff_ms = (s.entries[i].backoff_ms.saturating_mul(2)).min(s.backoff_max_ms);
                }
            }
        }

        0
    }
}
