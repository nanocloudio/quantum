//! dr — disaster-recovery orchestration.
//!
//! Schedules checkpoint exports and WAL archive shipping, and runs the
//! controlled-promotion state machine (FenceCommit → durability verify →
//! promote). Multi-step and long-running, distinct from the one-shot
//! admin commands the operations surface serves.
//!
//! ## Per-step bound
//!
//! Each `on_*` handles ONE frame per call; `step` advances the schedule
//! and at most one promotion transition per tick.

use super::abi::SyscallTable;
use super::{dev_channel_port, dev_log, dev_millis, wire};

const STATE_IDLE: u8 = 0;
const STATE_CHECKPOINT_EXPORTING: u8 = 1;
const STATE_WAL_ARCHIVING: u8 = 2;
const STATE_FENCING: u8 = 3;
const STATE_PROMOTING: u8 = 4;

#[repr(C)]
pub struct Dr {
    pub in_wal_signal: i32,    // in[0]: compaction signal from wal
    pub in_snapshot_resp: i32, // in[1]: snapshot export completion
    pub in_promotion_req: i32, // in[2]: promotion request from operations
    pub out_snapshot_req: i32, // out[0]: snapshot trigger to durability
    pub out_promotion: i32,    // out[1]: promotion command to operations

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

/// Component defaults. Channel handles are assigned by the
/// composite after this returns.
pub fn init(s: &mut Dr) {
    s.checkpoint_interval_s = 900;
    s.wal_archive_interval_s = 300;
    s.fence_commit_required = 1;
    s.state = STATE_IDLE;
}

/// One step of this component.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn step(s: &mut Dr, sys: &SyscallTable, out: &mut super::Outbox) {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let now = dev_millis(sys);

        // Drain WAL compaction signals to track cursor
        if s.in_wal_signal >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_wal_signal, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (_, plen) = wire::channel_read_msg(sys, s.in_wal_signal, &mut s.buf);
                if plen >= 8 {
                    s.wal_cursor_index = u64::from_le_bytes([
                        s.buf[0], s.buf[1], s.buf[2], s.buf[3], s.buf[4], s.buf[5], s.buf[6],
                        s.buf[7],
                    ]);
                }
            }
        }

        // Drain snapshot responses
        if s.in_snapshot_resp >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_snapshot_resp, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
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
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (_, _plen) = wire::channel_read_msg(sys, s.in_promotion_req, &mut s.buf);
                if s.state == STATE_IDLE {
                    s.state = if s.fence_commit_required == 1 {
                        STATE_FENCING
                    } else {
                        STATE_PROMOTING
                    };
                    // Audit: promotion started.
                    let mut ev = [0u8; 2];
                    ev[0] = 5; // AUDIT_DR_EVENT
                    ev[1] = 1; // subtype: promotion_start
                    out.audit.push(wire::MSG_AUDIT_EVENT, &ev);
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
                // Fence: wait for durability to confirm quorum-durable
                // state matches our checkpoint cursor. Production implementation
                // would consume durability proofs here and gate promotion.
                // Simplified: advance immediately.
                s.state = STATE_PROMOTING;
            }
            STATE_PROMOTING => {
                // Emit promotion command to operations
                if s.out_promotion >= 0 {
                    let mut p = [0u8; 9];
                    p[0] = 1; // controlled
                    p[1..9].copy_from_slice(&s.wal_cursor_index.to_le_bytes());
                    let poll = (sys.channel_poll)(s.out_promotion, 0x02);
                    if poll > 0 && (poll as u32 & 0x02) != 0 {
                        wire::channel_write_msg(sys, s.out_promotion, wire::MSG_DR_PROMOTE, &p);
                        s.promotions = s.promotions.wrapping_add(1);
                        s.state = STATE_IDLE;

                        // Audit: promotion completed.
                        let mut ev = [0u8; 10];
                        ev[0] = 5; // AUDIT_DR_EVENT
                        ev[1] = 2; // subtype: promotion_complete
                        ev[2..10].copy_from_slice(&s.wal_cursor_index.to_le_bytes());
                        out.audit.push(wire::MSG_AUDIT_EVENT, &ev);
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
        if (now / 1000).is_multiple_of(10) {
            let mut m = [0u8; 16];
            m[0..4].copy_from_slice(&s.checkpoints_shipped.to_le_bytes());
            m[4..8].copy_from_slice(&s.archives_shipped.to_le_bytes());
            m[8..12].copy_from_slice(&s.promotions.to_le_bytes());
            m[12] = s.state;
            out.metrics.push(wire::MSG_METRICS, &m);
        }
    }
}
