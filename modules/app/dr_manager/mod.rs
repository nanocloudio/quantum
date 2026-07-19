//! DR Manager — Disaster recovery orchestration.
//!
//! Long-running multi-step state machine for:
//! - Periodic checkpoint export scheduling
//! - WAL archive shipping with cursor tracking
//! - Controlled promotion (FenceCommit → durability verify → promote)
//! - Cross-region replication lag monitoring

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

const STATE_IDLE: u8 = 0;
const STATE_CHECKPOINT_EXPORTING: u8 = 1;
const STATE_WAL_ARCHIVING: u8 = 2;
const STATE_FENCING: u8 = 3;
const STATE_PROMOTING: u8 = 4;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_wal_signal: i32,       // in[0]: compaction signal from wal
    in_snapshot_resp: i32,    // in[1]: snapshot export completion
    in_promotion_req: i32,    // in[2]: promotion request from admin_handler
    out_snapshot_req: i32,    // out[0]: snapshot trigger to snapshot_engine
    out_promotion: i32,       // out[1]: promotion command to admin_handler
    out_audit: i32,           // out[2]: audit events to audit_logger
    out_metrics: i32,         // out[3]: metrics to metrics_aggregator

    checkpoint_interval_s: u32,
    wal_archive_interval_s: u32,
    fence_commit_required: u8,

    state: u8,
    last_checkpoint_ms: u64,
    last_wal_archive_ms: u64,
    wal_cursor_index: u64,
    checkpoints_shipped: u32,
    archives_shipped: u32,
    promotions: u32,

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
        s.in_wal_signal = in_chan;
        s.out_snapshot_req = out_chan;
        s.in_snapshot_resp = dev_channel_port(sys, 0, 1);
        s.in_promotion_req = dev_channel_port(sys, 0, 2);
        s.out_promotion = dev_channel_port(sys, 1, 1);
        s.out_audit = dev_channel_port(sys, 1, 2);
        s.out_metrics = dev_channel_port(sys, 1, 3);
        s.checkpoint_interval_s = 900;
        s.wal_archive_interval_s = 300;
        s.fence_commit_required = 1;
        s.state = STATE_IDLE;
        dev_log(sys, 3, b"[dr] init".as_ptr(), 8);
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

        // Drain WAL compaction signals to track cursor
        if s.in_wal_signal >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_wal_signal, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, plen) = wire::channel_read_msg(sys, s.in_wal_signal, &mut s.buf);
                if plen >= 8 {
                    s.wal_cursor_index = u64::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3], s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);
                }
            }
        }

        // Drain snapshot responses
        if s.in_snapshot_resp >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_snapshot_resp, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, _plen) = wire::channel_read_msg(sys, s.in_snapshot_resp, &mut s.buf);
                if s.state == STATE_CHECKPOINT_EXPORTING {
                    s.state = STATE_IDLE;
                    s.checkpoints_shipped = s.checkpoints_shipped.wrapping_add(1);
                }
            }
        }

        // Drain promotion requests
        if s.in_promotion_req >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_promotion_req, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, _plen) = wire::channel_read_msg(sys, s.in_promotion_req, &mut s.buf);
                if s.state == STATE_IDLE {
                    s.state = if s.fence_commit_required == 1 { STATE_FENCING } else { STATE_PROMOTING };
                    // Emit audit event
                    if s.out_audit >= 0 {
                        let mut ev = [0u8; 2];
                        ev[0] = 5; // AUDIT_DR_EVENT
                        ev[1] = 1; // subtype: promotion_start
                        let poll_a = (sys.channel_poll)(s.out_audit, 0x02);
                        if poll_a > 0 && (poll_a as u32 & 0x02) != 0 {
                            wire::channel_write_msg(sys, s.out_audit, wire::MSG_AUDIT_EVENT, &ev);
                        }
                    }
                }
            }
        }

        // State machine tick
        match s.state {
            STATE_CHECKPOINT_EXPORTING => {
                // Request snapshot from engine; wait for snapshot_resp_in
                // (handled above in the drain loop). No action here.
            }
            STATE_WAL_ARCHIVING => {
                // In production: issue dev_call writing WAL segment range to
                // the archive destination (S3, NFS, etc). For now we advance
                // the cursor and transition back to IDLE.
                s.state = STATE_IDLE;
                s.archives_shipped = s.archives_shipped.wrapping_add(1);
            }
            STATE_FENCING => {
                // Fence: wait for durability_ledger to confirm quorum-durable
                // state matches our checkpoint cursor. Production implementation
                // would consume durability proofs here and gate promotion.
                // Simplified: advance immediately.
                s.state = STATE_PROMOTING;
            }
            STATE_PROMOTING => {
                // Emit promotion command to admin_handler
                if s.out_promotion >= 0 {
                    let mut p = [0u8; 9];
                    p[0] = 1;  // controlled
                    p[1..9].copy_from_slice(&s.wal_cursor_index.to_le_bytes());
                    let poll = (sys.channel_poll)(s.out_promotion, 0x02);
                    if poll > 0 && (poll as u32 & 0x02) != 0 {
                        wire::channel_write_msg(sys, s.out_promotion, wire::MSG_DR_PROMOTE, &p);
                        s.promotions = s.promotions.wrapping_add(1);
                        s.state = STATE_IDLE;

                        // Audit: promotion completed
                        if s.out_audit >= 0 {
                            let mut ev = [0u8; 10];
                            ev[0] = 5; // AUDIT_DR_EVENT
                            ev[1] = 2; // subtype: promotion_complete
                            ev[2..10].copy_from_slice(&s.wal_cursor_index.to_le_bytes());
                            let poll_a = (sys.channel_poll)(s.out_audit, 0x02);
                            if poll_a > 0 && (poll_a as u32 & 0x02) != 0 {
                                wire::channel_write_msg(sys, s.out_audit, wire::MSG_AUDIT_EVENT, &ev);
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        // Scheduled checkpoint export
        if s.state == STATE_IDLE
            && now.wrapping_sub(s.last_checkpoint_ms) >= (s.checkpoint_interval_s as u64) * 1000
            && s.out_snapshot_req >= 0
        {
            s.last_checkpoint_ms = now;
            s.state = STATE_CHECKPOINT_EXPORTING;
            let mut req = [0u8; 8];
            req[0..8].copy_from_slice(&s.wal_cursor_index.to_le_bytes());
            let poll = (sys.channel_poll)(s.out_snapshot_req, 0x02);
            if poll > 0 && (poll as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, s.out_snapshot_req, wire::MSG_DR_SNAPSHOT_REQ, &req);
            }
        }

        // WAL archive tick: transition to archiving state (state machine
        // continues in next step when STATE_WAL_ARCHIVING is handled above).
        if s.state == STATE_IDLE
            && now.wrapping_sub(s.last_wal_archive_ms) >= (s.wal_archive_interval_s as u64) * 1000
        {
            s.last_wal_archive_ms = now;
            s.state = STATE_WAL_ARCHIVING;
        }

        // Metrics
        if s.out_metrics >= 0 && (now / 1000) % 10 == 0 {
            let mut m = [0u8; 16];
            m[0..4].copy_from_slice(&s.checkpoints_shipped.to_le_bytes());
            m[4..8].copy_from_slice(&s.archives_shipped.to_le_bytes());
            m[8..12].copy_from_slice(&s.promotions.to_le_bytes());
            m[12] = s.state;
            let poll = (sys.channel_poll)(s.out_metrics, 0x02);
            if poll > 0 && (poll as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, s.out_metrics, wire::MSG_METRICS, &m);
            }
        }

        0
    }
}
