//! Transaction Coordinator — Two-phase commit for Kafka + AMQP transactions.
//!
//! Tracks transaction state (Begin/Prepare/Commit/Abort) with timeouts.
//! Commit is recorded only after all involved PRGs reach wal_committed_index.

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

#[path = "../../common/types.rs"]
mod types;

use types::*;

const MAX_TXNS: usize = 256;

#[repr(C)]
#[derive(Clone, Copy)]
struct Txn {
    txn_id: u64,
    tenant: u32,
    session_slot: u32,
    state: u8,
    started_ms: u64,
    prg_mask: u32,        // bitmap of involved PRGs
    prg_ready: u32,       // bitmap of PRGs that reached commit index
    active: u8,
}

impl Txn {
    const fn zero() -> Self {
        Self {
            txn_id: 0, tenant: 0, session_slot: 0, state: TXN_BEGIN,
            started_ms: 0, prg_mask: 0, prg_ready: 0, active: 0,
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_op: i32,
    in_durability: i32,   // in[1]: durability proofs from durability_ledger
    out_result: i32,
    out_metrics: i32,

    timeout_ms: u32,
    next_txn_id: u64,

    txns: [Txn; MAX_TXNS],
    begins: u32,
    commits: u32,
    aborts: u32,
    timeouts: u32,
    last_scan_ms: u64,
    last_durable_index: u64,
    buf: [u8; 128],
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
        s.in_op = in_chan;
        s.in_durability = dev_channel_port(sys, 0, 1);
        s.out_result = out_chan;
        s.out_metrics = dev_channel_port(sys, 1, 1);
        s.timeout_ms = 60_000;
        s.next_txn_id = 1;
        for i in 0..MAX_TXNS {
            s.txns[i] = Txn::zero();
        }
        dev_log(sys, 3, b"[txn] init".as_ptr(), 9);
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

        // Drain txn ops: [op_type:u8][tenant:u32][session:u32][txn_id:u64][prg_mask:u32]
        if s.in_op >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_op, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_op, &mut s.buf);
                // op_in is a shared bus from session_processor.messaging_out;
                // ignore any message that isn't a txn op.
                if mt != wire::MSG_TXN_OP { continue; }
                if plen < 21 { continue; }

                let op_type = s.buf[0];
                let tenant = u32::from_le_bytes([s.buf[1], s.buf[2], s.buf[3], s.buf[4]]);
                let session = u32::from_le_bytes([s.buf[5], s.buf[6], s.buf[7], s.buf[8]]);
                let txn_id = u64::from_le_bytes([s.buf[9], s.buf[10], s.buf[11], s.buf[12], s.buf[13], s.buf[14], s.buf[15], s.buf[16]]);
                let prg_mask = u32::from_le_bytes([s.buf[17], s.buf[18], s.buf[19], s.buf[20]]);

                match op_type {
                    TXN_BEGIN => {
                        for i in 0..MAX_TXNS {
                            if s.txns[i].active == 0 {
                                let id = if txn_id == 0 { s.next_txn_id } else { txn_id };
                                if txn_id == 0 { s.next_txn_id = s.next_txn_id.wrapping_add(1); }
                                s.txns[i] = Txn {
                                    txn_id: id, tenant, session_slot: session,
                                    state: TXN_BEGIN, started_ms: now,
                                    prg_mask, prg_ready: 0, active: 1,
                                };
                                s.begins = s.begins.wrapping_add(1);
                                break;
                            }
                        }
                    }
                    TXN_COMMIT => {
                        // Advance to PREPARE and wait for all PRGs to confirm durability.
                        // The actual commit emission happens below in the durability scan.
                        for i in 0..MAX_TXNS {
                            if s.txns[i].active == 1 && s.txns[i].txn_id == txn_id {
                                s.txns[i].state = TXN_PREPARE;
                                break;
                            }
                        }
                    }
                    TXN_ABORT => {
                        for i in 0..MAX_TXNS {
                            if s.txns[i].active == 1 && s.txns[i].txn_id == txn_id {
                                s.txns[i].state = TXN_ABORT;
                                s.txns[i].active = 0;
                                s.aborts = s.aborts.wrapping_add(1);
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Drain durability proofs: advance prg_ready for PREPARE-phase transactions.
        // Proof payload: [term:u64][index:u64][replica:u8]
        if s.in_durability >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_durability, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, plen) = wire::channel_read_msg(sys, s.in_durability, &mut s.buf);
                if plen < 17 { continue; }
                let index = u64::from_le_bytes([s.buf[8], s.buf[9], s.buf[10], s.buf[11], s.buf[12], s.buf[13], s.buf[14], s.buf[15]]);
                let replica = s.buf[16];
                if index > s.last_durable_index { s.last_durable_index = index; }

                // Mark replica's PRG bit for all PREPARE transactions awaiting it.
                // Simplification: replica_id is the PRG id here (one-node-per-PRG assumption).
                let bit = 1u32 << (replica & 0x1F);
                for i in 0..MAX_TXNS {
                    if s.txns[i].active == 1 && s.txns[i].state == TXN_PREPARE {
                        s.txns[i].prg_ready |= bit;
                        if (s.txns[i].prg_ready & s.txns[i].prg_mask) == s.txns[i].prg_mask {
                            // All involved PRGs confirmed — commit
                            s.txns[i].state = TXN_COMMIT;
                            s.txns[i].active = 0;
                            s.commits = s.commits.wrapping_add(1);
                            let mut r = [0u8; 17];
                            r[0] = TXN_COMMIT;
                            r[1..9].copy_from_slice(&s.txns[i].txn_id.to_le_bytes());
                            r[9..13].copy_from_slice(&s.txns[i].tenant.to_le_bytes());
                            r[13..17].copy_from_slice(&s.txns[i].session_slot.to_le_bytes());
                            if s.out_result >= 0 {
                                let poll_out = (sys.channel_poll)(s.out_result, 0x02);
                                if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                                    wire::channel_write_msg(sys, s.out_result, wire::MSG_TXN_RESULT, &r);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Timeout scan
        if now.wrapping_sub(s.last_scan_ms) >= 1000 {
            s.last_scan_ms = now;
            for i in 0..MAX_TXNS {
                if s.txns[i].active == 1 && now.wrapping_sub(s.txns[i].started_ms) >= s.timeout_ms as u64 {
                    s.txns[i].active = 0;
                    s.timeouts = s.timeouts.wrapping_add(1);
                }
            }

            if s.out_metrics >= 0 {
                let mut m = [0u8; 16];
                m[0..4].copy_from_slice(&s.begins.to_le_bytes());
                m[4..8].copy_from_slice(&s.commits.to_le_bytes());
                m[8..12].copy_from_slice(&s.aborts.to_le_bytes());
                m[12..16].copy_from_slice(&s.timeouts.to_le_bytes());
                let poll = (sys.channel_poll)(s.out_metrics, 0x02);
                if poll > 0 && (poll as u32 & 0x02) != 0 {
                    wire::channel_write_msg(sys, s.out_metrics, wire::MSG_METRICS, &m);
                }
            }
        }

        0
    }
}
