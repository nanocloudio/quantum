//! anchor — the SessionCtrlV1 **transport anchor** role
//! (docs/architecture/session_continuity.md).
//!
//! The client's connection is owned here, not by the session worker
//! behind it: `protocol` classifies the connection, pins its codec
//! state and multiplexes its responses by `conn_id`, and none of that
//! moves. What this component adds is the attachment: each classified
//! connection is minted a Fluxor `session_id`
//! (`[anchor_id:8][conn_id:2][attach_gen:4][0:2]`,
//! `session_identity_core`) and attached to a session worker with
//! `CMD_SC_ATTACH` at class `edge_anchored`; every proposal the codecs
//! decode is forwarded to the worker the connection is bound to, and
//! the binding can be moved to the other worker while the connection
//! stays open.
//!
//! ## The seam
//!
//! Workers are addressed by slot: 0 on `proposals_out` / `responses_in`
//! / `ctrl_out` / `ctrl_in`, 1 on the `proposals2_out` / `responses2_in`
//! / `ctrl2_*` set. Every codec hands its decoded envelopes to
//! [`forward_env`] instead of writing the proposal edge itself; the
//! composite drains both response ports and both control ports.
//!
//! ## Delivery cursors
//!
//! Per connection, in envelope bytes: `forwarded` advances when an
//! envelope is accepted onto the bound worker's proposal channel,
//! `relayed` when a response envelope from that worker is accepted by
//! the codec onto the frame edge — never when it is merely read, since
//! a refused response is offered again. The worker counts the same
//! unit; `EXPORT_BEGIN` must match both or the handoff is refused.
//!
//! ## The swap
//!
//! Every `handoff_after_records` forwarded envelopes (0 = never; the
//! gates' trigger) every connection bound to the current default
//! worker is moved to the other one, and new connections attach there
//! from that moment. Per connection: `DRAIN` → the worker's `DRAINED`
//! and export, relayed verbatim to the standby after the cursors are
//! admitted → the standby's `IMPORT_END` → `RESUME` at epoch + 1 →
//! `RESUMED`, at which the forwarding target flips, the held ingress is
//! released and the exporting worker is detached. Cursors that disagree, an
//! import the standby refuses, a worker error or a window past
//! `session_drain_ms` refuse the handoff: the standby is detached and
//! the exporting worker returned to service with `RESUME` at the
//! current epoch; the held ingress goes back to it on `RESUMED`.
//!
//! ## The hold
//!
//! Envelopes decoded for a connection whose attachment is not live —
//! attaching, or mid-swap — are held in one bounded FIFO (`hold_bytes`)
//! and released to the bound worker in order when it is. The buffer
//! and its overflow policy are declared configuration: an overflow
//! CLOSES the connection by default (`hold_overflow = 0`), because a
//! silently dropped publish is a QoS promise broken; `1` drops the
//! envelope and counts it.

use super::wire;
use super::{dev_log, SyscallTable};

// ── SessionCtrlV1 (contracts/net/session_ctrl.rs) ────────────────────

pub const SC_CMD_HELLO: u8 = 0x70;
pub const SC_CMD_ATTACH: u8 = 0x71;
pub const SC_CMD_DETACH: u8 = 0x72;
pub const SC_CMD_DRAIN: u8 = 0x73;
pub const SC_CMD_EXPORT_BEGIN: u8 = 0x74;
pub const SC_CMD_EXPORT_CHUNK: u8 = 0x75;
pub const SC_CMD_EXPORT_END: u8 = 0x76;
pub const SC_CMD_RESUME: u8 = 0x77;

pub const SC_MSG_HELLO_ACK: u8 = 0x90;
pub const SC_MSG_ATTACHED: u8 = 0x91;
pub const SC_MSG_DETACHED: u8 = 0x92;
pub const SC_MSG_DRAINED: u8 = 0x93;
pub const SC_MSG_IMPORT_BEGIN: u8 = 0x94;
pub const SC_MSG_IMPORT_END: u8 = 0x96;
pub const SC_MSG_RESUMED: u8 = 0x97;
pub const SC_MSG_ERROR: u8 = 0x9F;

pub const SC_ROLE_ANCHOR: u8 = 1;
pub const SC_CC_EDGE_ANCHORED: u8 = 4;
pub const SC_DETACH_NORMAL: u8 = 0;
pub const SC_DETACH_CLIENT_GONE: u8 = 4;
pub const SC_STATUS_OK: u8 = 0;

// ── Session directory (clustor session_registry, MSG_SR_*) ───────────
//
// The directory is the single-writer authority per (session_id,
// epoch). The anchor mirrors every binding change to it and reads its
// verdicts: a BIND or EPOCH_BUMP the directory refuses as stale is the
// fencing signal that this anchor's view of a session's generation has
// fallen behind the cluster's. Requests carry `[request_id:u64 LE]`
// then the op body; the request id is `(conn_id << 32) | epoch` so a
// reply can be matched without a table.
pub const MSG_SR_REQUEST: u8 = 0x90;
pub const MSG_SR_REPLY: u8 = 0x91;
const SR_OP_BIND: u8 = 1;
const SR_OP_EPOCH_BUMP: u8 = 2;
const SR_OP_UNBIND: u8 = 9;
const SR_ST_OK: u8 = 0;
const SR_ST_STALE_EPOCH: u8 = 1;
const SR_REPLY_LEN: usize = 1 + 1 + 16 + 4 + 8 + 8;

const SID: usize = 16;
const SID_EPOCH: usize = SID + 4;
const ATTACH_LEN: usize = SID + 8 + 4 + 1 + 8;

const CONN_ID_SPACE: usize = 1 << 16;
/// Largest hold buffer a graph may declare.
pub const HOLD_MAX: usize = 1 << 17;
/// Hold entry header: `[conn:2][mtype:1][len:2]`.
const HOLD_HDR: usize = 5;
/// Control frame buffer: an export chunk with its session header.
pub const CTRL_BUF: usize = 1100;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Idle = 0,
    /// ATTACH sent; records held until ATTACHED.
    WaitAttached = 1,
    Active = 2,
    /// DRAIN sent to the bound worker.
    DrainWait = 3,
    /// Export relayed; waiting for the standby's IMPORT_END.
    ImportWait = 4,
    /// RESUME sent to the standby.
    ResumeWait = 5,
    /// Handoff refused; RESUME at the current epoch sent to the bound
    /// worker, waiting for its RESUMED before the hold is released.
    RefuseWait = 6,
    /// Close requested; waiting for the worker's DETACHED.
    Closing = 7,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Conn {
    session_id: [u8; SID],
    epoch: u32,
    forwarded: u64,
    relayed: u64,
    /// When the current handoff's DRAIN went out.
    started_ms: u64,
    phase: Phase,
    /// Bound worker slot.
    worker: u8,
    /// A post-swap DETACH to the exporting worker is outstanding.
    detach_old: u8,
    /// A refusal's RESUME could not be written; retried each step.
    resume_pending: u8,
}

impl Conn {
    const fn zero() -> Self {
        Self {
            session_id: [0; SID],
            epoch: 0,
            forwarded: 0,
            relayed: 0,
            started_ms: 0,
            phase: Phase::Idle,
            worker: 0,
            detach_old: 0,
            resume_pending: 0,
        }
    }
}

#[repr(C)]
pub struct Anchor {
    pub ctrl_in: [i32; 2],
    pub ctrl_out: [i32; 2],
    pub prop_out: [i32; 2],
    pub resp_in: [i32; 2],
    pub out_frames: i32,
    /// Session directory (`dir_out` → requests, replies → `dir_in`).
    /// Unwired on a graph without a directory member.
    pub dir_out: i32,
    pub dir_in: i32,
    // Declared configuration.
    pub anchor_id: [u8; 8],
    pub handoff_after_records: u32,
    /// Swaps the record trigger may start in this anchor's life (0 =
    /// unlimited). The gates set 1 so a session moves exactly once.
    pub handoff_max_swaps: u32,
    pub session_drain_ms: u32,
    pub hold_bytes: u32,
    pub hold_overflow: u8,
    pub default_worker: u8,
    hello_sent: u8,
    self_idx: u8,
    attach_gen: u32,
    records_since_swap: u32,
    /// Connections still short of RESUMED in the current swap.
    swap_pending: u32,
    conns: [Conn; CONN_ID_SPACE],
    hold: [u8; HOLD_MAX],
    hold_len: u32,
    ctrl_buf: [u8; CTRL_BUF],
    mon_buf: [u8; 192],
    // Accounting.
    pub attached: u32,
    pub swaps: u32,
    pub relocated: u32,
    pub refused: u32,
    pub cursor_mismatch: u32,
    pub hold_overflows: u32,
    pub held_records: u32,
    /// Directory verdicts: bindings confirmed, refused as stale, and
    /// requests the edge refused.
    pub dir_ok: u32,
    pub dir_stale: u32,
    pub dir_refused: u32,
}

include!("../../../target/fluxor/fluxor-abi/sdk/cores/session_handoff.rs");
mod identity {
    include!("../../common/cores/session_identity_core.rs");
}
use identity::{mint_session_id, session_id_conn, FIRST_EPOCH};

pub fn init(a: &mut Anchor) {
    a.ctrl_in = [-1; 2];
    a.ctrl_out = [-1; 2];
    a.prop_out = [-1; 2];
    a.resp_in = [-1; 2];
    a.out_frames = -1;
    a.dir_out = -1;
    a.dir_in = -1;
    a.anchor_id = *b"QANCHOR0";
    a.handoff_after_records = 0;
    a.handoff_max_swaps = 0;
    a.session_drain_ms = 500;
    a.hold_bytes = 65536;
    a.hold_overflow = 0;
    a.default_worker = 0;
    a.hello_sent = 0;
    a.self_idx = 0xFF;
    a.attach_gen = 0;
    a.records_since_swap = 0;
    a.swap_pending = 0;
    for c in a.conns.iter_mut() {
        *c = Conn::zero();
    }
    a.hold_len = 0;
    a.attached = 0;
    a.swaps = 0;
    a.relocated = 0;
    a.refused = 0;
    a.cursor_mismatch = 0;
    a.hold_overflows = 0;
    a.held_records = 0;
    a.dir_ok = 0;
    a.dir_stale = 0;
    a.dir_refused = 0;
}

// ── Directory mirror ─────────────────────────────────────────────────

fn worker_id_bytes(w: u8) -> [u8; 8] {
    let mut b = *b"QWORKER0";
    b[7] = b'0' + (w % 10);
    b
}

/// BIND `[sid:16][epoch:4][anchor:8][worker:8][flags:1]`.
unsafe fn dir_bind(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    if a.dir_out < 0 {
        return;
    }
    let c = a.conns[usize::from(conn)];
    let mut p = [0u8; 8 + 1 + SID + 4 + 8 + 8 + 1];
    let rid = (u64::from(conn) << 32) | u64::from(c.epoch);
    p[..8].copy_from_slice(&rid.to_le_bytes());
    p[8] = SR_OP_BIND;
    p[9..9 + SID].copy_from_slice(&c.session_id);
    p[25..29].copy_from_slice(&c.epoch.to_le_bytes());
    p[29..37].copy_from_slice(&a.anchor_id);
    p[37..45].copy_from_slice(&worker_id_bytes(c.worker));
    p[45] = 0;
    if wire::channel_write_msg(sys, a.dir_out, MSG_SR_REQUEST, &p) <= 0 {
        a.dir_refused = a.dir_refused.wrapping_add(1);
    }
}

/// EPOCH_BUMP `[sid:16][old:4][new:4]`.
unsafe fn dir_epoch_bump(a: &mut Anchor, sys: &SyscallTable, conn: u16, old: u32, new: u32) {
    if a.dir_out < 0 {
        return;
    }
    let c = a.conns[usize::from(conn)];
    let mut p = [0u8; 8 + 1 + SID + 4 + 4];
    let rid = (u64::from(conn) << 32) | u64::from(new);
    p[..8].copy_from_slice(&rid.to_le_bytes());
    p[8] = SR_OP_EPOCH_BUMP;
    p[9..9 + SID].copy_from_slice(&c.session_id);
    p[25..29].copy_from_slice(&old.to_le_bytes());
    p[29..33].copy_from_slice(&new.to_le_bytes());
    if wire::channel_write_msg(sys, a.dir_out, MSG_SR_REQUEST, &p) <= 0 {
        a.dir_refused = a.dir_refused.wrapping_add(1);
    }
}

/// UNBIND `[sid:16][epoch:4]`.
unsafe fn dir_unbind(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    if a.dir_out < 0 {
        return;
    }
    let c = a.conns[usize::from(conn)];
    let mut p = [0u8; 8 + 1 + SID + 4];
    let rid = (u64::from(conn) << 32) | u64::from(c.epoch);
    p[..8].copy_from_slice(&rid.to_le_bytes());
    p[8] = SR_OP_UNBIND;
    p[9..9 + SID].copy_from_slice(&c.session_id);
    p[25..29].copy_from_slice(&c.epoch.to_le_bytes());
    if wire::channel_write_msg(sys, a.dir_out, MSG_SR_REQUEST, &p) <= 0 {
        a.dir_refused = a.dir_refused.wrapping_add(1);
    }
}

/// Read the directory's verdicts. A stale refusal is counted and said:
/// it means the cluster holds a newer generation of the binding than
/// this anchor believes it owns.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn handle_dir(a: &mut Anchor, sys: &SyscallTable) -> u32 {
    if a.dir_in < 0 {
        return 0;
    }
    let mut worked = 0u32;
    for _ in 0..16 {
        let poll = (sys.channel_poll)(a.dir_in, 0x01);
        if poll <= 0 || (poll as u32 & 0x01) == 0 {
            break;
        }
        let mut buf = [0u8; 64];
        let (mt, plen) = wire::channel_read_msg(sys, a.dir_in, &mut buf);
        worked += 1;
        if mt != MSG_SR_REPLY || (plen as usize) < 8 + SR_REPLY_LEN {
            continue;
        }
        let status = buf[9];
        if status == SR_ST_OK {
            a.dir_ok = a.dir_ok.wrapping_add(1);
        } else if status == SR_ST_STALE_EPOCH {
            a.dir_stale = a.dir_stale.wrapping_add(1);
            dev_log(sys, 1, b"[anchor] directory: stale epoch".as_ptr(), 31);
        } else {
            a.dir_refused = a.dir_refused.wrapping_add(1);
        }
    }
    worked
}

/// Set `anchor_id` from a parameter string (up to 8 bytes, zero padded).
pub fn set_anchor_id(a: &mut Anchor, d: *const u8, len: usize) {
    if d.is_null() || len == 0 {
        return;
    }
    let n = len.min(8);
    a.anchor_id = [0; 8];
    // SAFETY: the caller hands a TLV value of `len` readable bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(d, a.anchor_id.as_mut_ptr(), n);
    }
}

#[inline]
fn standby_wired(a: &Anchor, w: usize) -> bool {
    a.ctrl_out[w] >= 0 && a.prop_out[w] >= 0
}

/// True when this anchor has a worker to attach to at all.
#[inline]
pub fn enabled(a: &Anchor) -> bool {
    a.ctrl_out[0] >= 0 || a.ctrl_out[1] >= 0
}

// ── Emitters ─────────────────────────────────────────────────────────

unsafe fn write_ctrl(a: &Anchor, sys: &SyscallTable, w: usize, mt: u8, payload: &[u8]) -> bool {
    let ch = a.ctrl_out[w];
    ch >= 0 && wire::channel_write_msg(sys, ch, mt, payload) > 0
}

unsafe fn send_attach(a: &Anchor, sys: &SyscallTable, w: usize, conn: u16) -> bool {
    let c = &a.conns[usize::from(conn)];
    let mut p = [0u8; ATTACH_LEN];
    p[..SID].copy_from_slice(&c.session_id);
    p[SID..SID + 8].copy_from_slice(&a.anchor_id);
    p[SID + 8..SID + 12].copy_from_slice(&c.epoch.to_le_bytes());
    p[SID + 12] = SC_CC_EDGE_ANCHORED;
    write_ctrl(a, sys, w, SC_CMD_ATTACH, &p)
}

unsafe fn send_detach(a: &Anchor, sys: &SyscallTable, w: usize, conn: u16, reason: u8) -> bool {
    let c = &a.conns[usize::from(conn)];
    let mut p = [0u8; SID_EPOCH + 1];
    p[..SID].copy_from_slice(&c.session_id);
    p[SID..SID_EPOCH].copy_from_slice(&c.epoch.to_le_bytes());
    p[SID_EPOCH] = reason;
    write_ctrl(a, sys, w, SC_CMD_DETACH, &p)
}

unsafe fn send_drain(a: &Anchor, sys: &SyscallTable, w: usize, conn: u16) -> bool {
    let c = &a.conns[usize::from(conn)];
    let mut p = [0u8; SID_EPOCH + 4];
    p[..SID].copy_from_slice(&c.session_id);
    p[SID..SID_EPOCH].copy_from_slice(&c.epoch.to_le_bytes());
    p[SID_EPOCH..].copy_from_slice(&a.session_drain_ms.to_le_bytes());
    write_ctrl(a, sys, w, SC_CMD_DRAIN, &p)
}

unsafe fn send_resume(a: &Anchor, sys: &SyscallTable, w: usize, conn: u16, epoch: u32) -> bool {
    let c = &a.conns[usize::from(conn)];
    let mut p = [0u8; SID_EPOCH];
    p[..SID].copy_from_slice(&c.session_id);
    p[SID..].copy_from_slice(&epoch.to_le_bytes());
    write_ctrl(a, sys, w, SC_CMD_RESUME, &p)
}

unsafe fn send_close(a: &Anchor, sys: &SyscallTable, conn: u16) {
    if a.out_frames < 0 {
        return;
    }
    let cb = conn.to_le_bytes();
    wire::channel_write_msg(sys, a.out_frames, wire::MSG_CONN_CLOSE_REQUEST, &cb);
}

unsafe fn mon(
    a: &mut Anchor,
    sys: &SyscallTable,
    conn: u16,
    event: u8,
    reason: &[u8],
    status: &[u8],
) {
    if a.self_idx == 0xFF {
        let idx = super::dev_self_index(sys);
        if idx >= 0 {
            a.self_idx = idx as u8;
        }
    }
    let c = a.conns[usize::from(conn)];
    let _ = super::dev_mon_session(
        sys,
        a.self_idx,
        event,
        c.session_id.as_ptr(),
        c.epoch,
        a.anchor_id.as_ptr(),
        core::ptr::null(),
        reason,
        status,
        a.mon_buf.as_mut_ptr(),
        a.mon_buf.len(),
    );
}

// ── Hold buffer ──────────────────────────────────────────────────────

unsafe fn hold_push(a: &mut Anchor, sys: &SyscallTable, conn: u16, mt: u8, payload: &[u8]) -> bool {
    let need = HOLD_HDR + payload.len();
    let cap = (a.hold_bytes as usize).min(HOLD_MAX);
    if a.hold_len as usize + need > cap {
        a.hold_overflows = a.hold_overflows.wrapping_add(1);
        if a.hold_overflow == 0 {
            // Declared policy: a publish we cannot hold is one the
            // client must resend, so the connection is closed rather
            // than the envelope dropped.
            dev_log(sys, 1, b"[anchor] hold overflow: closing conn".as_ptr(), 36);
            close_conn(a, sys, conn);
        } else {
            dev_log(sys, 2, b"[anchor] hold overflow: dropped".as_ptr(), 31);
        }
        return true;
    }
    let at = a.hold_len as usize;
    a.hold[at..at + 2].copy_from_slice(&conn.to_le_bytes());
    a.hold[at + 2] = mt;
    a.hold[at + 3..at + 5].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    a.hold[at + 5..at + need].copy_from_slice(payload);
    a.hold_len += need as u32;
    a.held_records = a.held_records.wrapping_add(1);
    true
}

/// Release held envelopes whose connection is live again, in order.
/// An envelope the worker's channel refuses stays, with everything
/// after it, for the next step.
unsafe fn flush_hold(a: &mut Anchor, sys: &SyscallTable) {
    let total = a.hold_len as usize;
    if total == 0 {
        return;
    }
    let mut rd = 0usize;
    let mut wr = 0usize;
    let mut stalled = false;
    while rd + HOLD_HDR <= total {
        let conn = u16::from_le_bytes([a.hold[rd], a.hold[rd + 1]]);
        let mt = a.hold[rd + 2];
        let len = usize::from(u16::from_le_bytes([a.hold[rd + 3], a.hold[rd + 4]]));
        let entry = HOLD_HDR + len;
        if rd + entry > total {
            break;
        }
        let c = a.conns[usize::from(conn)];
        let mut keep = true;
        if !stalled {
            match c.phase {
                Phase::Active => {
                    let ch = a.prop_out[usize::from(c.worker)];
                    let w = if ch >= 0 {
                        wire::channel_write_msg(sys, ch, mt, &a.hold[rd + HOLD_HDR..rd + entry])
                    } else {
                        -1
                    };
                    if w > 0 {
                        let cm = &mut a.conns[usize::from(conn)];
                        cm.forwarded = cm.forwarded.wrapping_add((wire::ENVELOPE_HDR + len) as u64);
                        a.records_since_swap = a.records_since_swap.wrapping_add(1);
                        keep = false;
                    } else {
                        stalled = true;
                    }
                }
                Phase::Idle | Phase::Closing => keep = false,
                _ => {}
            }
        }
        if keep {
            if wr != rd {
                // Byte loop: the PIC build links no panic machinery
                // for a bounds-checked `copy_within`.
                let mut k = 0;
                while k < entry {
                    a.hold[wr + k] = a.hold[rd + k];
                    k += 1;
                }
            }
            wr += entry;
        }
        rd += entry;
    }
    a.hold_len = wr as u32;
}

/// Drop every held envelope of one connection.
fn hold_drop_conn(a: &mut Anchor, conn: u16) {
    let total = a.hold_len as usize;
    let mut rd = 0usize;
    let mut wr = 0usize;
    while rd + HOLD_HDR <= total {
        let c = u16::from_le_bytes([a.hold[rd], a.hold[rd + 1]]);
        let len = usize::from(u16::from_le_bytes([a.hold[rd + 3], a.hold[rd + 4]]));
        let entry = HOLD_HDR + len;
        if rd + entry > total {
            break;
        }
        if c != conn {
            if wr != rd {
                // Byte loop: the PIC build links no panic machinery
                // for a bounds-checked `copy_within`.
                let mut k = 0;
                while k < entry {
                    a.hold[wr + k] = a.hold[rd + k];
                    k += 1;
                }
            }
            wr += entry;
        }
        rd += entry;
    }
    a.hold_len = wr as u32;
}

// ── Data plane ───────────────────────────────────────────────────────

/// Forward one decoded envelope `[conn_id:u16 LE][...]` to the worker
/// the connection is bound to, attaching first if it is new and holding
/// it while the attachment is not live. `false` only when the live
/// worker's channel refused it — the codec keeps the packet and offers
/// it again.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn forward_env(a: &mut Anchor, sys: &SyscallTable, mt: u8, payload: &[u8]) -> bool {
    if payload.len() < 2 {
        return true;
    }
    let conn = u16::from_le_bytes([payload[0], payload[1]]);
    if !enabled(a) {
        // No worker wired: the seam is the plain proposal edge, and
        // the anchor is transparent.
        let ch = a.prop_out[0];
        return ch >= 0 && wire::channel_write_msg(sys, ch, mt, payload) > 0;
    }
    let phase = a.conns[usize::from(conn)].phase;
    match phase {
        Phase::Idle => {
            attach(a, sys, conn);
            hold_push(a, sys, conn, mt, payload)
        }
        Phase::Active => {
            let w = usize::from(a.conns[usize::from(conn)].worker);
            let ch = a.prop_out[w];
            if ch < 0 {
                return true;
            }
            let wrote = wire::channel_write_msg(sys, ch, mt, payload);
            if wrote > 0 {
                let c = &mut a.conns[usize::from(conn)];
                c.forwarded = c
                    .forwarded
                    .wrapping_add((wire::ENVELOPE_HDR + payload.len()) as u64);
                a.records_since_swap = a.records_since_swap.wrapping_add(1);
                true
            } else {
                false
            }
        }
        Phase::Closing => true,
        _ => hold_push(a, sys, conn, mt, payload),
    }
}

/// A response envelope from worker `w` for `conn` was accepted onto the
/// frame edge.
pub fn note_relayed(a: &mut Anchor, w: usize, conn: u16, envelope_bytes: usize) {
    let c = &mut a.conns[usize::from(conn)];
    if c.phase != Phase::Idle && usize::from(c.worker) == w {
        c.relayed = c.relayed.wrapping_add(envelope_bytes as u64);
    }
}

/// Mint the attachment and ask the default worker to take it.
unsafe fn attach(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    let w = usize::from(a.default_worker);
    a.attach_gen = a.attach_gen.wrapping_add(1);
    let c = &mut a.conns[usize::from(conn)];
    *c = Conn::zero();
    c.session_id = mint_session_id(&a.anchor_id, conn, a.attach_gen);
    c.epoch = FIRST_EPOCH;
    c.worker = w as u8;
    c.phase = Phase::WaitAttached;
    if send_attach(a, sys, w, conn) {
        mon(a, sys, conn, super::MON_EV_ATTACH_REQ, b"", b"");
    } else {
        // ctrl edge full: the step loop retries from WaitAttached.
        a.conns[usize::from(conn)].resume_pending = 1;
    }
}

/// The transport reported the connection closed.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn conn_closed(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    let c = a.conns[usize::from(conn)];
    if c.phase == Phase::Idle {
        return;
    }
    hold_drop_conn(a, conn);
    dir_unbind(a, sys, conn);
    let w = usize::from(c.worker);
    let mid_swap = matches!(
        c.phase,
        Phase::DrainWait | Phase::ImportWait | Phase::ResumeWait
    );
    if mid_swap && standby_wired(a, w ^ 1) {
        send_detach(a, sys, w ^ 1, conn, SC_DETACH_CLIENT_GONE);
        if a.swap_pending > 0 {
            a.swap_pending -= 1;
        }
    }
    send_detach(a, sys, w, conn, SC_DETACH_CLIENT_GONE);
    mon(a, sys, conn, super::MON_EV_DETACH_REQ, b"client_gone", b"");
    let cm = &mut a.conns[usize::from(conn)];
    cm.phase = Phase::Closing;
    cm.detach_old = 0;
}

unsafe fn close_conn(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    send_close(a, sys, conn);
    conn_closed(a, sys, conn);
}

// ── The swap ─────────────────────────────────────────────────────────

/// Refuse the handoff in flight for `conn`: the standby discards its
/// half-import and the exporting worker is returned to service at the
/// session's current epoch. The hold is released on its RESUMED.
unsafe fn refuse(a: &mut Anchor, sys: &SyscallTable, conn: u16, status: &[u8]) {
    let c = a.conns[usize::from(conn)];
    let w = usize::from(c.worker);
    if standby_wired(a, w ^ 1) {
        send_detach(a, sys, w ^ 1, conn, SC_DETACH_NORMAL);
    }
    a.refused = a.refused.wrapping_add(1);
    mon(a, sys, conn, super::MON_EV_ERROR, b"", status);
    let epoch = c.epoch;
    let sent = send_resume(a, sys, w, conn, epoch);
    let cm = &mut a.conns[usize::from(conn)];
    cm.phase = Phase::RefuseWait;
    cm.resume_pending = u8::from(!sent);
    if sent {
        mon(a, sys, conn, super::MON_EV_RESUME_REQ, b"", b"");
    }
    if a.swap_pending > 0 {
        a.swap_pending -= 1;
    }
    dev_log(sys, 1, b"[anchor] handoff refused".as_ptr(), 24);
}

/// Move every connection bound to `from` onto the other worker, and
/// bind new connections there from now on.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn start_swap(a: &mut Anchor, sys: &SyscallTable, now: u64) -> bool {
    let from = usize::from(a.default_worker);
    let to = 1 - from;
    if a.swap_pending != 0 || !standby_wired(a, to) {
        return false;
    }
    a.default_worker = to as u8;
    a.records_since_swap = 0;
    a.swaps = a.swaps.wrapping_add(1);
    let mut started = 0u32;
    for i in 0..CONN_ID_SPACE {
        let c = a.conns[i];
        if c.phase != Phase::Active || usize::from(c.worker) != from || c.detach_old != 0 {
            continue;
        }
        if send_drain(a, sys, from, i as u16) {
            let cm = &mut a.conns[i];
            cm.phase = Phase::DrainWait;
            cm.started_ms = now;
            started += 1;
            mon(a, sys, i as u16, super::MON_EV_EXPORT_REQ, b"", b"");
        }
    }
    a.swap_pending = started;
    dev_log(sys, 3, b"[anchor] swap started".as_ptr(), 21);
    true
}

/// Relay the control frame in `ctrl_buf` verbatim to worker `w`.
unsafe fn relay(a: &mut Anchor, sys: &SyscallTable, w: usize, mt: u8, n: usize) {
    let mut f = [0u8; CTRL_BUF];
    f[..n].copy_from_slice(&a.ctrl_buf[..n]);
    if !write_ctrl(a, sys, w, mt, &f[..n]) {
        // A partial relay cannot be admitted: the standby refuses at
        // IMPORT_END (a gap), which refuses the handoff.
        dev_log(sys, 1, b"[anchor] relay refused".as_ptr(), 22);
    }
}

/// Drain worker `w`'s control channel. Its response channel is drained
/// first by the composite, and only read here when it polls empty, so
/// everything the worker emitted before DRAINED has reached the client
/// side and been counted before its export is admitted.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn handle_ctrl(a: &mut Anchor, sys: &SyscallTable, w: usize, now: u64) -> u32 {
    let ch = a.ctrl_in[w];
    if ch < 0 {
        return 0;
    }
    let mut worked = 0u32;
    for _ in 0..32 {
        if a.resp_in[w] >= 0 {
            let rp = (sys.channel_poll)(a.resp_in[w], 0x01);
            if rp > 0 && (rp as u32 & 0x01) != 0 {
                break;
            }
        }
        let poll = (sys.channel_poll)(ch, 0x01);
        if poll <= 0 || (poll as u32 & 0x01) == 0 {
            break;
        }
        let (mt, plen) = {
            let mut buf = [0u8; CTRL_BUF];
            let r = wire::channel_read_msg(sys, ch, &mut buf);
            a.ctrl_buf = buf;
            r
        };
        worked += 1;
        let n = plen as usize;
        if !(SID..=CTRL_BUF).contains(&n) {
            continue;
        }
        let frame = a.ctrl_buf;
        let p = &frame[..n];
        let conn = if mt == SC_MSG_HELLO_ACK {
            0
        } else {
            session_id_conn(&sid_of(p))
        };
        let c = a.conns[usize::from(conn)];
        let is_bound = usize::from(c.worker) == w;
        if mt != SC_MSG_HELLO_ACK && (c.phase == Phase::Idle || c.session_id != p[..SID]) {
            // A reply for an attachment that no longer exists.
            continue;
        }
        match mt {
            SC_MSG_HELLO_ACK => {
                dev_log(sys, 3, b"[anchor] worker hello".as_ptr(), 21);
            }
            SC_MSG_ATTACHED => {
                if c.phase == Phase::WaitAttached && is_bound && n > SID_EPOCH {
                    if p[SID_EPOCH] == SC_STATUS_OK {
                        a.conns[usize::from(conn)].phase = Phase::Active;
                        a.attached = a.attached.wrapping_add(1);
                        mon(a, sys, conn, super::MON_EV_ATTACHED, b"", b"ok");
                        dir_bind(a, sys, conn);
                        flush_hold(a, sys);
                    } else {
                        mon(a, sys, conn, super::MON_EV_ATTACH_FAILED, b"", b"refused");
                        close_conn(a, sys, conn);
                    }
                }
            }
            SC_MSG_DRAINED => {
                if is_bound && c.phase == Phase::DrainWait {
                    a.conns[usize::from(conn)].phase = Phase::ImportWait;
                }
            }
            SC_CMD_EXPORT_BEGIN | SC_CMD_EXPORT_CHUNK | SC_CMD_EXPORT_END => {
                if !is_bound || !matches!(c.phase, Phase::DrainWait | Phase::ImportWait) {
                    continue;
                }
                if mt == SC_CMD_EXPORT_BEGIN {
                    let off = SID_EPOCH + 4;
                    let admit = match SessionCursors::decode(&p[off.min(n)..]) {
                        Some(cur) => cursors_admit(&cur, c.forwarded, c.relayed),
                        None => HANDOFF_CURSOR_MISMATCH,
                    };
                    if admit != HANDOFF_OK {
                        a.cursor_mismatch = a.cursor_mismatch.wrapping_add(1);
                        dev_log(sys, 1, b"[anchor] export cursors disagree".as_ptr(), 32);
                        refuse(a, sys, conn, b"cursor_mismatch");
                        continue;
                    }
                    a.conns[usize::from(conn)].phase = Phase::ImportWait;
                }
                relay(a, sys, w ^ 1, mt, n);
            }
            SC_MSG_IMPORT_BEGIN => {
                if !is_bound
                    && c.phase == Phase::ImportWait
                    && n > SID_EPOCH
                    && p[SID_EPOCH] != SC_STATUS_OK
                {
                    refuse(a, sys, conn, b"no_capacity");
                }
            }
            SC_MSG_IMPORT_END => {
                if !is_bound && c.phase == Phase::ImportWait && n > SID_EPOCH {
                    if p[SID_EPOCH] == SC_STATUS_OK {
                        let new_epoch = c.epoch + 1;
                        if send_resume(a, sys, w, conn, new_epoch) {
                            a.conns[usize::from(conn)].phase = Phase::ResumeWait;
                            mon(a, sys, conn, super::MON_EV_RESUME_REQ, b"", b"");
                        } else {
                            refuse(a, sys, conn, b"not_ready");
                        }
                    } else {
                        let why: &[u8] = if p[SID_EPOCH] == 1 {
                            b"stale_epoch"
                        } else {
                            b"corrupt"
                        };
                        refuse(a, sys, conn, why);
                    }
                }
            }
            SC_MSG_RESUMED => {
                if is_bound && c.phase == Phase::RefuseWait {
                    // Back on the exporting worker at the unchanged epoch.
                    a.conns[usize::from(conn)].phase = Phase::Active;
                    mon(a, sys, conn, super::MON_EV_RESUMED, b"", b"kept");
                    flush_hold(a, sys);
                } else if !is_bound && c.phase == Phase::ResumeWait {
                    let old = usize::from(c.worker);
                    {
                        let cm = &mut a.conns[usize::from(conn)];
                        cm.epoch += 1;
                        cm.worker = w as u8;
                        cm.phase = Phase::Active;
                        cm.detach_old = 1;
                    }
                    send_detach(a, sys, old, conn, SC_DETACH_NORMAL);
                    let new_epoch = a.conns[usize::from(conn)].epoch;
                    dir_epoch_bump(a, sys, conn, new_epoch - 1, new_epoch);
                    dir_bind(a, sys, conn);
                    a.relocated = a.relocated.wrapping_add(1);
                    if a.swap_pending > 0 {
                        a.swap_pending -= 1;
                    }
                    mon(a, sys, conn, super::MON_EV_EPOCH_BUMP, b"", b"ok");
                    mon(a, sys, conn, super::MON_EV_RELOCATED, b"", b"ok");
                    flush_hold(a, sys);
                }
            }
            SC_MSG_DETACHED => {
                if !is_bound && c.detach_old != 0 {
                    a.conns[usize::from(conn)].detach_old = 0;
                } else if c.phase == Phase::Closing && is_bound {
                    a.conns[usize::from(conn)] = Conn::zero();
                }
                // A standby acknowledging a refusal's detach: nothing.
            }
            SC_MSG_ERROR => {
                if matches!(
                    c.phase,
                    Phase::DrainWait | Phase::ImportWait | Phase::ResumeWait
                ) {
                    refuse(a, sys, conn, b"error");
                } else if c.phase == Phase::RefuseWait && is_bound {
                    // The exporting worker will not resume: the
                    // session is over on both sides.
                    close_conn(a, sys, conn);
                }
            }
            _ => {}
        }
        let _ = now;
    }
    worked
}

#[inline]
fn sid_of(p: &[u8]) -> [u8; SID] {
    let mut s = [0u8; SID];
    s.copy_from_slice(&p[..SID]);
    s
}

/// Per-step housekeeping: the HELLO handshake, retried control writes,
/// the drain deadline and the swap trigger.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn step(a: &mut Anchor, sys: &SyscallTable, now: u64) {
    if !enabled(a) {
        return;
    }
    if a.hello_sent == 0 {
        let mut hello = [0u8; 10];
        hello[0] = SC_ROLE_ANCHOR;
        hello[1..9].copy_from_slice(&a.anchor_id);
        let mut all = true;
        for w in 0..2 {
            if a.ctrl_out[w] >= 0 && !write_ctrl(a, sys, w, SC_CMD_HELLO, &hello) {
                all = false;
            }
        }
        if all {
            a.hello_sent = 1;
        }
    }
    let deadline = u64::from(a.session_drain_ms);
    // Bounded walk: only connections mid-attach or mid-handoff need a
    // look, and they are rare relative to the table; the walk itself
    // is 64 Ki cheap loads.
    for i in 0..CONN_ID_SPACE {
        let c = a.conns[i];
        match c.phase {
            Phase::Idle | Phase::Active | Phase::Closing => {}
            Phase::WaitAttached => {
                if c.resume_pending != 0 && send_attach(a, sys, usize::from(c.worker), i as u16) {
                    a.conns[i].resume_pending = 0;
                }
            }
            Phase::RefuseWait => {
                if c.resume_pending != 0
                    && send_resume(a, sys, usize::from(c.worker), i as u16, c.epoch)
                {
                    a.conns[i].resume_pending = 0;
                }
            }
            Phase::DrainWait | Phase::ImportWait | Phase::ResumeWait => {
                if now.saturating_sub(c.started_ms) > deadline {
                    dev_log(sys, 1, b"[anchor] handoff deadline".as_ptr(), 25);
                    refuse(a, sys, i as u16, b"drain_timeout");
                }
            }
        }
    }
    if a.hold_len > 0 {
        flush_hold(a, sys);
    }
    if a.handoff_after_records != 0
        && a.records_since_swap >= a.handoff_after_records
        && (a.handoff_max_swaps == 0 || a.swaps < a.handoff_max_swaps)
    {
        start_swap(a, sys, now);
    }
}

/// Fill the component's metric payload.
pub fn metrics(a: &Anchor, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&a.attached.to_le_bytes());
    m[4..8].copy_from_slice(&a.swaps.to_le_bytes());
    m[8..12].copy_from_slice(&a.relocated.to_le_bytes());
    m[12..16].copy_from_slice(&a.refused.to_le_bytes());
    m[16..20].copy_from_slice(&a.cursor_mismatch.to_le_bytes());
    m[20..24].copy_from_slice(&a.hold_overflows.to_le_bytes());
    24
}

pub fn hb_counters(a: &Anchor) -> (u32, u32, u32, u32) {
    (a.attached, a.relocated, a.refused, a.held_records)
}

#[inline]
pub fn phase(a: &Anchor, conn: u16) -> Phase {
    a.conns[usize::from(conn)].phase
}
