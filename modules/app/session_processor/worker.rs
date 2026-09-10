//! worker — the SessionCtrlV1 **session worker** role
//! (docs/architecture/session_continuity.md).
//!
//! `protocol` is the transport anchor: it owns the client's connection
//! and mints a Fluxor `session_id` per attachment. This component is
//! what makes `session_processor` the movable half behind it. It
//! answers the control plane on `ctrl_in` / `ctrl_out`:
//!
//!   HELLO → HELLO_ACK, ATTACH → ATTACHED, DETACH → DETACHED,
//!   DRAIN → (quiescent) DRAINED + EXPORT_BEGIN / CHUNK… / END,
//!   EXPORT_* (relayed from the other worker) → IMPORT_BEGIN / IMPORT_END,
//!   RESUME → RESUMED (the rebind after an import, or the
//!   return-to-service of a refused handoff).
//!
//! ## What moves
//!
//! The record `sessions::export_record` builds: identity, generation,
//! negotiated parameters, Will, the flow windows and the QoS in-flight
//! ledger — everything the importing worker needs that the committed
//! log does not hand it. Subscriptions stay in `topic_engine`, keyed by
//! the slot index the session keeps across the move; the importer
//! re-points them with `MSG_TOPIC_MOVE` once RESUME admits the rebind.
//!
//! ## Delivery cursors
//!
//! Per attachment, in envelope bytes on the anchor↔worker seam: every
//! record read off `codec_in` for the connection advances `in_consumed`
//! by its envelope size (header included); every `MSG_SESSION_RESPONSE`
//! written for it advances `out_produced` the same way. The anchor
//! counts the same unit on its side, and `EXPORT_BEGIN` carries the
//! pair for it to admit.
//!
//! ## Drain to quiescent
//!
//! `DRAINED` is declared only when the session is at a protocol-coherent
//! point: no proposal of its awaits its Raft assignment, no delivery or
//! ack registration of its is parked, and nothing has moved for it for
//! [`QUIET_STEPS`] consecutive steps. A QoS 2 flow between PUBREC and
//! PUBREL is such a point — the ledger records it durably and the
//! record carries it; a flow whose PUBLISH is still in Raft is not, and
//! the drain waits. Past the anchor's deadline it refuses the handoff
//! and returns this worker to service.
//!
//! ## Frozen sessions
//!
//! Between export and DETACH the session is `Exported`: this worker
//! still holds it but must neither consume nor emit for it. A topic
//! delivery that still reaches it is parked in the offline queue —
//! keyed by the slot, which the importer drains once, in queue order,
//! [`RESUME_SETTLE_MS`] after RESUME, parking its own deliveries until
//! then so nothing overtakes — and an ack completion is dropped: the
//! importer re-registers every in-flight publish it received, and the
//! flow ledger answers a registration it already holds durable at once.

use super::abi::SyscallTable;
use super::wire;
use super::{correlate, dev_log, sessions, ModuleState, MAX_SESSIONS};

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

pub const SC_ROLE_WORKER: u8 = 2;
pub const SC_DETACH_CLIENT_GONE: u8 = 4;

pub const SC_STATUS_OK: u8 = 0;
pub const SC_STATUS_STALE_EPOCH: u8 = 1;
pub const SC_STATUS_NO_CAPACITY: u8 = 3;
pub const SC_STATUS_CORRUPT: u8 = 4;
pub const SC_STATUS_NOT_READY: u8 = 5;

const SID: usize = 16;
const SID_EPOCH: usize = SID + 4;
const ATTACH_LEN: usize = SID + 8 + 4 + 1 + 8;
const EXPORT_BEGIN_LEN: usize = SID_EPOCH + 4 + 16;

/// Every value a conn id can take: bindings are a direct index.
const CONN_ID_SPACE: usize = 1 << 16;
/// Attachments mid-handoff at once. The anchor swaps every session it
/// serves under one window, so this bounds the swap, not the table.
const MAX_HANDOFFS: usize = 1024;
/// Concurrent imports, each with its own reassembly buffer.
const IMPORT_SLOTS: usize = 256;
/// Steps with no traffic for the attachment before it is quiescent.
/// Long enough for an in-process messaging round trip (a retained read
/// answering a SUBSCRIBE) to have come back: a reply that lands after
/// the export is dropped by the frozen exporter and by the importer
/// until it resumes.
pub const QUIET_STEPS: u8 = 8;
/// Export chunk size. Under the ctrl edge's record cap with the
/// session header in front.
const EXPORT_CHUNK: usize = 512;
/// After RESUME the session stays "settling" for this long: deliveries
/// reaching this worker are parked in the offline queue beside any the
/// exporting worker parked before it saw DETACH (a step or two after
/// RESUMED), and ONE drain at the end replays everything in queue
/// order. Draining earlier would let a direct delivery overtake a
/// parked one. The exporter's last park lands within a few ticks of
/// RESUMED; the window is generous.
pub const RESUME_SETTLE_MS: u64 = 25;
/// Frame buffer for one control message.
pub const CTRL_BUF: usize = 1100;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Idle = 0,
    Attached = 1,
    Draining = 2,
    Exported = 3,
    Importing = 4,
    Imported = 5,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Attachment {
    session_id: [u8; SID],
    epoch: u32,
    phase: Phase,
    /// Steps with no in/out traffic (saturating).
    quiet: u8,
    /// Traffic seen this step.
    dirty: u8,
    /// `1 + index` into `imports` while importing; 0 = none.
    import: u16,
    in_consumed: u64,
    out_produced: u64,
    /// Slot the record was placed at, so RESUME can address it.
    slot: u32,
    /// A second `MSG_OFFLINE_RECONNECT` is due at this time (0 = none).
    settle_at_ms: u64,
}

impl Attachment {
    const fn zero() -> Self {
        Self {
            session_id: [0; SID],
            epoch: 0,
            phase: Phase::Idle,
            quiet: 0,
            dirty: 0,
            import: 0,
            in_consumed: 0,
            out_produced: 0,
            slot: 0,
            settle_at_ms: 0,
        }
    }
}

#[repr(C)]
pub struct Worker {
    pub in_ctrl: i32,
    pub out_ctrl: i32,
    pub worker_id: u8,
    /// Test-only skew added to the exported inbound cursor (param 8).
    pub cursor_skew: u32,
    /// 8-byte identity on the wire: `QWORKER<n>`.
    id_bytes: [u8; 8],
    self_idx: u8,
    bindings: [Attachment; CONN_ID_SPACE],
    /// Conns whose binding is past `Attached`, walked each step.
    busy: [u16; MAX_HANDOFFS],
    busy_len: u32,
    imports: [HandoffImport; IMPORT_SLOTS],
    import_buf: [[u8; sessions::RECORD_MAX]; IMPORT_SLOTS],
    import_used: [u8; IMPORT_SLOTS],
    export_buf: [u8; sessions::RECORD_MAX],
    ctrl_buf: [u8; CTRL_BUF],
    mon_buf: [u8; 192],
    // Accounting.
    pub attached: u32,
    pub drained: u32,
    pub exported: u32,
    pub imported: u32,
    pub resumed: u32,
    pub returned: u32,
    pub refused: u32,
    pub frozen_drops: u32,
    pub frozen_parked: u32,
    /// Attachments the step list could not take because it was full.
    /// Non-zero means a swap larger than [`MAX_HANDOFFS`] was asked
    /// for: the attachments past the bound are never stepped, so their
    /// drains never reach `DRAINED` and the anchor refuses those
    /// handoffs on its deadline. The sessions keep serving — a refused
    /// handoff returns them — but the swap only half happened, and the
    /// count is what says so.
    pub busy_overflow: u32,
}

include!("../../../target/fluxor/fluxor-abi/sdk/cores/session_handoff.rs");

pub fn init(w: &mut Worker, worker_id: u8) {
    w.in_ctrl = -1;
    w.out_ctrl = -1;
    w.worker_id = worker_id;
    w.id_bytes = *b"QWORKER0";
    w.id_bytes[7] = b'0' + (worker_id % 10);
    w.self_idx = 0xFF;
    for b in w.bindings.iter_mut() {
        *b = Attachment::zero();
    }
    w.busy_len = 0;
    for i in 0..IMPORT_SLOTS {
        w.imports[i] = HandoffImport::new();
        w.import_used[i] = 0;
    }
    w.attached = 0;
    w.drained = 0;
    w.exported = 0;
    w.imported = 0;
    w.resumed = 0;
    w.returned = 0;
    w.refused = 0;
    w.frozen_drops = 0;
    w.frozen_parked = 0;
}

#[inline]
pub fn phase(w: &Worker, conn_id: u16) -> Phase {
    w.bindings[usize::from(conn_id)].phase
}

/// True while the attachment must neither consume nor emit.
#[inline]
pub fn is_frozen(w: &Worker, conn_id: u16) -> bool {
    matches!(
        w.bindings[usize::from(conn_id)].phase,
        Phase::Exported | Phase::Importing | Phase::Imported
    )
}

/// True while a handoff is in progress for the attachment, in either
/// direction — the keep-alive sweep leaves such a session alone.
#[inline]
pub fn is_held(w: &Worker, conn_id: u16) -> bool {
    w.bindings[usize::from(conn_id)].phase != Phase::Attached
        && w.bindings[usize::from(conn_id)].phase != Phase::Idle
}

/// An envelope of `bytes` (header included) was read for `conn_id`.
pub fn note_in(w: &mut Worker, conn_id: u16, bytes: usize) {
    let b = &mut w.bindings[usize::from(conn_id)];
    if b.phase == Phase::Attached || b.phase == Phase::Draining {
        b.in_consumed = b.in_consumed.wrapping_add(bytes as u64);
        b.dirty = 1;
    }
}

/// An envelope of `bytes` (header included) was written for `conn_id`.
pub fn note_out(w: &mut Worker, conn_id: u16, bytes: usize) {
    let b = &mut w.bindings[usize::from(conn_id)];
    if b.phase == Phase::Attached || b.phase == Phase::Draining {
        b.out_produced = b.out_produced.wrapping_add(bytes as u64);
        b.dirty = 1;
    }
}

/// Put an attachment on the per-step walk list. Idempotent.
///
/// Returns false when the list is full, which is a refusal rather than
/// a silent drop: the caller counts it, because an attachment that is
/// not walked never advances its drain.
fn busy_add(w: &mut Worker, conn_id: u16) -> bool {
    for i in 0..w.busy_len as usize {
        if w.busy[i] == conn_id {
            return true;
        }
    }
    if (w.busy_len as usize) >= MAX_HANDOFFS {
        w.busy_overflow = w.busy_overflow.wrapping_add(1);
        return false;
    }
    w.busy[w.busy_len as usize] = conn_id;
    w.busy_len += 1;
    true
}

fn busy_remove(w: &mut Worker, conn_id: u16) {
    let n = w.busy_len as usize;
    for i in 0..n {
        if w.busy[i] == conn_id {
            w.busy[i] = w.busy[n - 1];
            w.busy_len -= 1;
            return;
        }
    }
}

fn import_alloc(w: &mut Worker) -> Option<usize> {
    (0..IMPORT_SLOTS)
        .find(|&i| w.import_used[i] == 0)
        .inspect(|&i| {
            w.import_used[i] = 1;
            w.imports[i] = HandoffImport::new();
        })
}

fn import_free(w: &mut Worker, conn_id: u16) {
    let b = &mut w.bindings[usize::from(conn_id)];
    if b.import != 0 {
        let i = usize::from(b.import - 1);
        w.import_used[i] = 0;
        w.imports[i].reset();
        b.import = 0;
    }
}

// ── Emitters ─────────────────────────────────────────────────────────

unsafe fn write_ctrl(w: &mut Worker, sys: &SyscallTable, mt: u8, payload: &[u8]) -> bool {
    if w.out_ctrl < 0 {
        return false;
    }
    wire::channel_write_msg(sys, w.out_ctrl, mt, payload) > 0
}

unsafe fn send_sid_epoch(w: &mut Worker, sys: &SyscallTable, mt: u8, conn_id: u16) -> bool {
    let b = w.bindings[usize::from(conn_id)];
    let mut p = [0u8; SID_EPOCH];
    p[..SID].copy_from_slice(&b.session_id);
    p[SID..].copy_from_slice(&b.epoch.to_le_bytes());
    write_ctrl(w, sys, mt, &p)
}

unsafe fn send_status(
    w: &mut Worker,
    sys: &SyscallTable,
    mt: u8,
    sid: &[u8; SID],
    epoch: u32,
    status: u8,
) -> bool {
    let mut p = [0u8; SID_EPOCH + 1];
    p[..SID].copy_from_slice(sid);
    p[SID..SID_EPOCH].copy_from_slice(&epoch.to_le_bytes());
    p[SID_EPOCH] = status;
    write_ctrl(w, sys, mt, &p)
}

unsafe fn mon(
    w: &mut Worker,
    sys: &SyscallTable,
    conn_id: u16,
    event: u8,
    reason: &[u8],
    status: &[u8],
) {
    if w.self_idx == 0xFF {
        let idx = super::dev_self_index(sys);
        if idx >= 0 {
            w.self_idx = idx as u8;
        }
    }
    let b = w.bindings[usize::from(conn_id)];
    let _ = super::dev_mon_session(
        sys,
        w.self_idx,
        event,
        b.session_id.as_ptr(),
        b.epoch,
        core::ptr::null(),
        w.id_bytes.as_ptr(),
        reason,
        status,
        w.mon_buf.as_mut_ptr(),
        w.mon_buf.len(),
    );
}

// ── Session-scoped helpers over the parent state ─────────────────────

#[inline]
fn sid_conn(sid: &[u8]) -> u16 {
    u16::from_be_bytes([sid[8], sid[9]])
}

#[inline]
fn epoch_at(p: &[u8]) -> u32 {
    u32::from_le_bytes([p[SID], p[SID + 1], p[SID + 2], p[SID + 3]])
}

/// The attachment is at a protocol-coherent point: nothing of its is in
/// flight anywhere this worker can see, and it has been quiet.
fn quiescent(s: &ModuleState, conn_id: u16) -> bool {
    let b = &s.worker.bindings[usize::from(conn_id)];
    if b.quiet < QUIET_STEPS {
        return false;
    }
    match sessions::find_by_conn(&s.sessions, conn_id) {
        Some(si) => {
            let slot = si as u32;
            if correlate::pending_for_slot(&s.correlate, slot)
                || correlate::dlv_parked_for_slot(&s.correlate, slot)
                || correlate::ack_parked_for_slot(&s.correlate, slot)
            {
                return false;
            }
            // A publish of this session still in the commit-gating
            // stash — assigned, but its dedup verdict or durability
            // proof not yet back — is this worker's to release: the
            // topic publish is emitted from the stash when both land,
            // and a worker that exported (and was detached) before then
            // never emits it. The drain waits for the release.
            let mut views = [sessions::InflightView::zero(); super::MAX_INFLIGHT_PER_SESSION];
            let n = sessions::inflight_entries(&s.sessions, si, &mut views);
            !views.iter().take(n).any(|v| {
                v.direction == super::INFLIGHT_PUB
                    && v.correlation_id != 0
                    && correlate::stash_by_correlation(&s.correlate, v.correlation_id).is_some()
            })
        }
        // No MQTT session yet (a CONNECT is still in the anchor's
        // hold, or the client never sent one): nothing to carry.
        None => true,
    }
}

/// DRAINED, then the record as EXPORT_BEGIN / CHUNK… / END.
unsafe fn export(s: &mut ModuleState, sys: &SyscallTable, conn_id: u16, now: u64) {
    let (slot, len) = match sessions::find_by_conn(&s.sessions, conn_id) {
        Some(si) => {
            let n = sessions::export_record(&s.sessions, si, now, &mut s.worker.export_buf);
            (si as u32, n)
        }
        None => (u32::MAX, 0),
    };
    let b = s.worker.bindings[usize::from(conn_id)];
    let w = &mut s.worker;
    if !send_sid_epoch(w, sys, SC_MSG_DRAINED, conn_id) {
        return;
    }
    w.drained = w.drained.wrapping_add(1);
    mon(w, sys, conn_id, super::MON_EV_DRAINED, b"", b"");

    // EXPORT_BEGIN: [sid][epoch][total_len][in_consumed][out_produced]
    let mut begin = [0u8; EXPORT_BEGIN_LEN];
    begin[..SID].copy_from_slice(&b.session_id);
    begin[SID..SID_EPOCH].copy_from_slice(&b.epoch.to_le_bytes());
    begin[SID_EPOCH..SID_EPOCH + 4].copy_from_slice(&(len as u32).to_le_bytes());
    let mut cur = [0u8; CURSOR_PAIR_LEN];
    SessionCursors::new(
        b.in_consumed.wrapping_add(u64::from(w.cursor_skew)),
        b.out_produced,
    )
    .encode(&mut cur);
    begin[SID_EPOCH + 4..].copy_from_slice(&cur);
    let mut ok = write_ctrl(w, sys, SC_CMD_EXPORT_BEGIN, &begin);

    let mut exp = HandoffExport::new(len as u32);
    let mut chunk = [0u8; SID_EPOCH + 4 + EXPORT_CHUNK];
    chunk[..SID].copy_from_slice(&b.session_id);
    chunk[SID..SID_EPOCH].copy_from_slice(&b.epoch.to_le_bytes());
    while let Some((off, n)) = exp.next_chunk(EXPORT_CHUNK as u32) {
        chunk[SID_EPOCH..SID_EPOCH + 4].copy_from_slice(&off.to_le_bytes());
        let base = SID_EPOCH + 4;
        let (o, n) = (off as usize, n as usize);
        chunk[base..base + n].copy_from_slice(&w.export_buf[o..o + n]);
        ok &= write_ctrl(w, sys, SC_CMD_EXPORT_CHUNK, &chunk[..base + n]);
        exp.advance(n as u32);
    }
    let mut end = [0u8; SID_EPOCH + 4];
    end[..SID].copy_from_slice(&b.session_id);
    end[SID..SID_EPOCH].copy_from_slice(&b.epoch.to_le_bytes());
    end[SID_EPOCH..].copy_from_slice(&handoff_crc32(&w.export_buf[..len]).to_le_bytes());
    ok &= write_ctrl(w, sys, SC_CMD_EXPORT_END, &end);

    let bm = &mut w.bindings[usize::from(conn_id)];
    bm.slot = slot;
    bm.phase = Phase::Exported;
    if ok {
        w.exported = w.exported.wrapping_add(1);
        mon(w, sys, conn_id, super::MON_EV_EXPORTED, b"", b"");
    } else {
        // The ctrl edge refused part of the export: the anchor cannot
        // admit a partial blob. Say so; it refuses and resumes us.
        let sid = b.session_id;
        send_status(w, sys, SC_MSG_ERROR, &sid, b.epoch, SC_STATUS_NOT_READY);
        mon(w, sys, conn_id, super::MON_EV_ERROR, b"", b"not_ready");
    }
}

/// Post-import work once RESUME admits the rebind: the session is live
/// on this worker. Re-register every in-flight publish with `flow` (a
/// completion the exporting worker dropped after its export is
/// answered again from the durable mark) and drain what was parked
/// offline while the session was frozen.
unsafe fn after_resume(s: &mut ModuleState, sys: &SyscallTable, conn_id: u16, now: u64) {
    let slot = s.worker.bindings[usize::from(conn_id)].slot;
    if slot as usize >= MAX_SESSIONS {
        return;
    }
    let si = slot as usize;
    // Re-point the session's subscriptions here only now that the
    // rebind is admitted: a handoff refused after the import would
    // otherwise leave them delivering to a worker that gave the
    // session back.
    let mut mv = [0u8; 5];
    mv[..4].copy_from_slice(&slot.to_le_bytes());
    mv[4] = s.worker.worker_id;
    super::try_emit(sys, s.out_topic, wire::MSG_TOPIC_MOVE, &mv);
    let gen = sessions::generation(&s.sessions, si);
    let mut views = [sessions::InflightView::zero(); super::MAX_INFLIGHT_PER_SESSION];
    let n = sessions::inflight_entries(&s.sessions, si, &mut views);
    for v in views.iter().take(n) {
        if v.direction != super::INFLIGHT_PUB || v.wal_index == 0 {
            continue;
        }
        // A QoS 2 flow whose PUBREC already went out owes the client
        // nothing until its PUBREL: registering it again would answer
        // as if the PUBREL had committed.
        if v.qos == 2 && v.phase == super::QOS2_PUBLISH && v.rec_sent {
            continue;
        }
        let mut reg = [0u8; wire::ACK_REGISTER_LEN];
        reg[0..4].copy_from_slice(&slot.to_le_bytes());
        reg[4..8].copy_from_slice(&u32::from(v.packet_id).to_le_bytes());
        reg[8..10].copy_from_slice(&v.partition_id.to_le_bytes());
        reg[10..18].copy_from_slice(&v.wal_index.to_le_bytes());
        reg[18..22].copy_from_slice(&gen.to_le_bytes());
        if !super::try_emit(sys, s.out_forward, wire::MSG_ACK_REGISTER, &reg) {
            let _ = correlate::ack_stash(&mut s.correlate, reg);
        }
    }
    s.worker.bindings[usize::from(conn_id)].settle_at_ms = now.saturating_add(RESUME_SETTLE_MS);
    if !busy_add(&mut s.worker, conn_id) {
        // Nothing will fire the settle drain, so drain now rather than
        // leave what the exporting worker parked sitting in the queue.
        let body = s.worker.bindings[usize::from(conn_id)].slot.to_le_bytes();
        super::try_emit(sys, s.out_messaging, wire::MSG_OFFLINE_RECONNECT, &body);
        s.worker.bindings[usize::from(conn_id)].settle_at_ms = 0;
    }
}

/// True while the attachment is settling after a RESUME: deliveries
/// park until the settle drain.
#[inline]
pub fn is_settling(w: &Worker, conn_id: u16) -> bool {
    let b = &w.bindings[usize::from(conn_id)];
    b.phase == Phase::Attached && b.settle_at_ms != 0
}

// ── Control frames ───────────────────────────────────────────────────

/// Drain `ctrl_in`. Bounded per step.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn handle_ctrl(s: &mut ModuleState, sys: &SyscallTable, now: u64) -> u32 {
    let chan = s.worker.in_ctrl;
    if chan < 0 {
        return 0;
    }
    let mut worked = 0u32;
    for _ in 0..32 {
        let poll = (sys.channel_poll)(chan, 0x01);
        if poll <= 0 || (poll as u32 & 0x01) == 0 {
            break;
        }
        let (mt, plen) = {
            let mut buf = [0u8; CTRL_BUF];
            let r = wire::channel_read_msg(sys, chan, &mut buf);
            s.worker.ctrl_buf = buf;
            r
        };
        worked += 1;
        let n = plen as usize;
        if n > CTRL_BUF {
            continue;
        }
        let frame = s.worker.ctrl_buf;
        let p = &frame[..n];
        match mt {
            SC_CMD_HELLO => {
                if n >= 9 {
                    let mut ack = [0u8; 9];
                    ack[0] = SC_ROLE_WORKER;
                    ack[1..].copy_from_slice(&s.worker.id_bytes);
                    write_ctrl(&mut s.worker, sys, SC_MSG_HELLO_ACK, &ack);
                    dev_log(sys, 3, b"[sess] anchor hello".as_ptr(), 19);
                }
            }
            SC_CMD_ATTACH => {
                if n < ATTACH_LEN {
                    continue;
                }
                let conn = sid_conn(p);
                let mut sid = [0u8; SID];
                sid.copy_from_slice(&p[..SID]);
                let epoch = u32::from_le_bytes([p[SID + 8], p[SID + 9], p[SID + 10], p[SID + 11]]);
                let b = &mut s.worker.bindings[usize::from(conn)];
                // A new attachment on a recycled conn id replaces a
                // stale one; an attachment mid-handoff is refused.
                let status = if b.phase == Phase::Idle || b.phase == Phase::Attached {
                    *b = Attachment::zero();
                    b.session_id = sid;
                    b.epoch = epoch;
                    b.phase = Phase::Attached;
                    SC_STATUS_OK
                } else {
                    SC_STATUS_NOT_READY
                };
                send_status(&mut s.worker, sys, SC_MSG_ATTACHED, &sid, epoch, status);
                if status == SC_STATUS_OK {
                    s.worker.attached = s.worker.attached.wrapping_add(1);
                    mon(&mut s.worker, sys, conn, super::MON_EV_ATTACHED, b"", b"ok");
                }
            }
            SC_CMD_DETACH => {
                if n < SID_EPOCH + 1 {
                    continue;
                }
                let conn = sid_conn(p);
                let reason = p[SID_EPOCH];
                let b = s.worker.bindings[usize::from(conn)];
                if b.session_id != p[..SID] {
                    continue;
                }
                match b.phase {
                    Phase::Exported => {
                        // The session lives on the other worker now: give
                        // up the record but keep the index reserved.
                        if (b.slot as usize) < MAX_SESSIONS {
                            sessions::clear_flow(&mut s.sessions, b.slot as usize);
                            sessions::lend(&mut s.sessions, b.slot as usize);
                        }
                    }
                    Phase::Importing | Phase::Imported => {
                        // A refused handoff: discard the half-import. A
                        // committed-but-unresumed record is dropped the
                        // same way; the exporter still holds its own.
                        if b.phase == Phase::Imported && (b.slot as usize) < MAX_SESSIONS {
                            sessions::clear_flow(&mut s.sessions, b.slot as usize);
                            sessions::lend(&mut s.sessions, b.slot as usize);
                        }
                    }
                    Phase::Attached | Phase::Draining => {
                        if reason == SC_DETACH_CLIENT_GONE {
                            super::handle_conn_disconnect(s, conn);
                        }
                    }
                    Phase::Idle => {}
                }
                import_free(&mut s.worker, conn);
                send_sid_epoch(&mut s.worker, sys, SC_MSG_DETACHED, conn);
                mon(&mut s.worker, sys, conn, super::MON_EV_DETACHED, b"", b"");
                s.worker.bindings[usize::from(conn)] = Attachment::zero();
                busy_remove(&mut s.worker, conn);
            }
            SC_CMD_DRAIN => {
                if n < SID_EPOCH + 4 {
                    continue;
                }
                let conn = sid_conn(p);
                let b = &mut s.worker.bindings[usize::from(conn)];
                if b.session_id != p[..SID] || b.epoch != epoch_at(p) {
                    let mut sid = [0u8; SID];
                    sid.copy_from_slice(&p[..SID]);
                    send_status(
                        &mut s.worker,
                        sys,
                        SC_MSG_ERROR,
                        &sid,
                        epoch_at(p),
                        SC_STATUS_STALE_EPOCH,
                    );
                    continue;
                }
                if b.phase == Phase::Attached {
                    let sid = b.session_id;
                    let epoch = b.epoch;
                    b.phase = Phase::Draining;
                    b.quiet = 0;
                    if !busy_add(&mut s.worker, conn) {
                        // Nothing would ever step this drain, so say no
                        // now instead of letting the anchor discover it
                        // on the deadline.
                        s.worker.bindings[usize::from(conn)].phase = Phase::Attached;
                        send_status(
                            &mut s.worker,
                            sys,
                            SC_MSG_ERROR,
                            &sid,
                            epoch,
                            SC_STATUS_NO_CAPACITY,
                        );
                    }
                }
            }
            SC_CMD_EXPORT_BEGIN => {
                if n < EXPORT_BEGIN_LEN {
                    continue;
                }
                let conn = sid_conn(p);
                let mut sid = [0u8; SID];
                sid.copy_from_slice(&p[..SID]);
                let epoch = epoch_at(p);
                let total = u32::from_le_bytes([
                    p[SID_EPOCH],
                    p[SID_EPOCH + 1],
                    p[SID_EPOCH + 2],
                    p[SID_EPOCH + 3],
                ]);
                let cursors = SessionCursors::decode(&p[SID_EPOCH + 4..]);
                let b = s.worker.bindings[usize::from(conn)];
                let status = if b.phase != Phase::Idle {
                    SC_STATUS_NOT_READY
                } else if let Some(i) = import_alloc(&mut s.worker) {
                    let mut st = s.worker.imports[i].begin(total, sessions::RECORD_MAX as u32);
                    if st == HANDOFF_OK {
                        // The settle drain after RESUME is stepped from
                        // the walk list; take the slot before admitting
                        // the import, so a full list refuses here
                        // rather than half-importing.
                        if busy_add(&mut s.worker, conn) {
                            let b = &mut s.worker.bindings[usize::from(conn)];
                            *b = Attachment::zero();
                            b.session_id = sid;
                            b.epoch = epoch;
                            b.phase = Phase::Importing;
                            b.import = (i as u16) + 1;
                            if let Some(c) = cursors {
                                b.in_consumed = c.in_consumed;
                                b.out_produced = c.out_produced;
                            }
                        } else {
                            s.worker.import_used[i] = 0;
                            st = SC_STATUS_NO_CAPACITY;
                        }
                    } else {
                        s.worker.import_used[i] = 0;
                    }
                    st
                } else {
                    SC_STATUS_NO_CAPACITY
                };
                send_status(&mut s.worker, sys, SC_MSG_IMPORT_BEGIN, &sid, epoch, status);
            }
            SC_CMD_EXPORT_CHUNK => {
                let hdr = SID_EPOCH + 4;
                if n <= hdr {
                    continue;
                }
                let conn = sid_conn(p);
                let b = s.worker.bindings[usize::from(conn)];
                if b.phase != Phase::Importing || b.import == 0 {
                    continue;
                }
                let i = usize::from(b.import - 1);
                let off = u32::from_le_bytes([
                    p[SID_EPOCH],
                    p[SID_EPOCH + 1],
                    p[SID_EPOCH + 2],
                    p[SID_EPOCH + 3],
                ]);
                let mut dest = s.worker.import_buf[i];
                let st = s.worker.imports[i].chunk(off, &p[hdr..], &mut dest);
                s.worker.import_buf[i] = dest;
                if st != HANDOFF_OK {
                    // Offset gap, overrun or a chunk before BEGIN: the
                    // relay lost or reordered a frame.
                    let mut line = [0u8; 96];
                    let mut pos = 0usize;
                    for &c in b"[sess] import chunk refused off=" {
                        line[pos] = c;
                        pos += 1;
                    }
                    pos += super::fmt_u32_raw(line.as_mut_ptr().add(pos), off);
                    for &c in b" have=" {
                        line[pos] = c;
                        pos += 1;
                    }
                    pos += super::fmt_u32_raw(
                        line.as_mut_ptr().add(pos),
                        s.worker.imports[i].received(),
                    );
                    for &c in b" total=" {
                        line[pos] = c;
                        pos += 1;
                    }
                    pos += super::fmt_u32_raw(
                        line.as_mut_ptr().add(pos),
                        s.worker.imports[i].total_len(),
                    );
                    dev_log(sys, 1, line.as_ptr(), pos);
                    let sid = b.session_id;
                    send_status(&mut s.worker, sys, SC_MSG_IMPORT_END, &sid, b.epoch, st);
                    import_free(&mut s.worker, conn);
                    s.worker.bindings[usize::from(conn)] = Attachment::zero();
                    busy_remove(&mut s.worker, conn);
                    s.worker.refused = s.worker.refused.wrapping_add(1);
                }
            }
            SC_CMD_EXPORT_END => {
                if n < SID_EPOCH + 4 {
                    continue;
                }
                let conn = sid_conn(p);
                let b = s.worker.bindings[usize::from(conn)];
                if b.phase != Phase::Importing || b.import == 0 {
                    continue;
                }
                let i = usize::from(b.import - 1);
                let crc = u32::from_le_bytes([
                    p[SID_EPOCH],
                    p[SID_EPOCH + 1],
                    p[SID_EPOCH + 2],
                    p[SID_EPOCH + 3],
                ]);
                let mut st = s.worker.imports[i].end(crc);
                let len = s.worker.imports[i].total_len() as usize;
                let mut slot = u32::MAX;
                if st != HANDOFF_OK {
                    dev_log(sys, 1, b"[sess] import: bad crc or length".as_ptr(), 31);
                } else if len > 0 {
                    let blob = s.worker.import_buf[i];
                    st = commit_import(s, sys, &blob[..len], conn, now, &mut slot);
                    if st == SC_STATUS_CORRUPT {
                        dev_log(sys, 1, b"[sess] import: bad record".as_ptr(), 25);
                    }
                }
                let sid = b.session_id;
                let epoch = b.epoch;
                if st == HANDOFF_OK {
                    let bm = &mut s.worker.bindings[usize::from(conn)];
                    bm.phase = Phase::Imported;
                    bm.slot = slot;
                    s.worker.imported = s.worker.imported.wrapping_add(1);
                    send_status(
                        &mut s.worker,
                        sys,
                        SC_MSG_IMPORT_END,
                        &sid,
                        epoch,
                        SC_STATUS_OK,
                    );
                    mon(&mut s.worker, sys, conn, super::MON_EV_IMPORTED, b"", b"ok");
                } else {
                    send_status(&mut s.worker, sys, SC_MSG_IMPORT_END, &sid, epoch, st);
                    let status_name: &[u8] = if st == SC_STATUS_STALE_EPOCH {
                        b"stale_epoch"
                    } else {
                        b"corrupt"
                    };
                    mon(
                        &mut s.worker,
                        sys,
                        conn,
                        super::MON_EV_IMPORTED,
                        b"",
                        status_name,
                    );
                    s.worker.bindings[usize::from(conn)] = Attachment::zero();
                    busy_remove(&mut s.worker, conn);
                    s.worker.refused = s.worker.refused.wrapping_add(1);
                }
                import_free(&mut s.worker, conn);
            }
            SC_CMD_RESUME => {
                if n < SID_EPOCH {
                    continue;
                }
                let conn = sid_conn(p);
                let new_epoch = epoch_at(p);
                let b = s.worker.bindings[usize::from(conn)];
                if b.session_id != p[..SID] {
                    continue;
                }
                let sid = b.session_id;
                // session_identity_core rule 4.
                let admitted = match b.phase {
                    Phase::Imported => new_epoch > b.epoch,
                    Phase::Draining | Phase::Exported => new_epoch == b.epoch,
                    _ => false,
                };
                if !admitted {
                    send_status(
                        &mut s.worker,
                        sys,
                        SC_MSG_ERROR,
                        &sid,
                        new_epoch,
                        SC_STATUS_STALE_EPOCH,
                    );
                    mon(
                        &mut s.worker,
                        sys,
                        conn,
                        super::MON_EV_REJECTED,
                        b"stale_epoch",
                        b"",
                    );
                    continue;
                }
                let was_imported = b.phase == Phase::Imported;
                {
                    let bm = &mut s.worker.bindings[usize::from(conn)];
                    bm.epoch = new_epoch;
                    bm.phase = Phase::Attached;
                    bm.quiet = 0;
                }
                busy_remove(&mut s.worker, conn);
                if was_imported {
                    after_resume(s, sys, conn, now);
                    s.worker.resumed = s.worker.resumed.wrapping_add(1);
                } else {
                    s.worker.returned = s.worker.returned.wrapping_add(1);
                }
                send_sid_epoch(&mut s.worker, sys, SC_MSG_RESUMED, conn);
                mon(&mut s.worker, sys, conn, super::MON_EV_RESUMED, b"", b"ok");
            }
            _ => {}
        }
    }
    worked
}

/// Place a verified record: reconcile its generation against any
/// replicated record this worker holds (`session_identity_core` rule
/// 3), take the index it names, re-point its subscriptions here.
unsafe fn commit_import(
    s: &mut ModuleState,
    sys: &SyscallTable,
    blob: &[u8],
    conn: u16,
    now: u64,
    slot_out: &mut u32,
) -> u8 {
    let Some((tenant, stream_hash, blob_gen, slot)) = sessions::record_identity(blob) else {
        return SC_STATUS_CORRUPT;
    };
    let si = slot as usize;
    if si >= MAX_SESSIONS {
        return SC_STATUS_CORRUPT;
    }
    let local = sessions::find_by_stream(&s.sessions, tenant, stream_hash)
        .map(|li| sessions::generation(&s.sessions, li));
    let (st, gen) = reconcile_generation(blob_gen, local);
    if st != SI_STATUS_OK {
        return SC_STATUS_STALE_EPOCH;
    }
    if let Some(li) = sessions::find_by_stream(&s.sessions, tenant, stream_hash) {
        if li != si {
            // This worker already holds a record of the same MQTT
            // session at another index: an earlier life of the client
            // here (a clean session whose disconnect has not applied
            // yet, or a parked one). The imported record is the live
            // one — the generation reconciled above says the log
            // agrees — so a copy with no connection gives way to it. A
            // copy with a live connection is a takeover the anchor did
            // not mediate; refuse rather than guess.
            if sessions::is_active(&s.sessions, li) {
                return SC_STATUS_NO_CAPACITY;
            }
            sessions::clear_flow(&mut s.sessions, li);
            sessions::clear(&mut s.sessions, li);
        }
    }
    sessions::unlend(&mut s.sessions, si);
    if !sessions::import_record(&mut s.sessions, si, blob, conn, gen, now) {
        return SC_STATUS_NO_CAPACITY;
    }
    let _ = sys;
    *slot_out = slot;
    HANDOFF_OK
}

// The identity core's `Binding` models the same phases for the host
// tests; here the attachment table above is the live state and only the
// reconciliation rule is used.
mod identity {
    include!("../../common/cores/session_identity_core.rs");
}
use identity::{reconcile_generation, SI_STATUS_OK};

/// Per-step work: advance quiet counters, declare DRAINED + export the
/// attachments that reached quiescence, and fire settle drains.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn step(s: &mut ModuleState, sys: &SyscallTable, now: u64) {
    // `busy_len` is re-read every iteration: a settle drain below
    // removes its entry by swapping the last one into `i`, so a bound
    // snapshotted here would walk past the live end and re-process the
    // entry that moved.
    let mut i = 0usize;
    while i < (s.worker.busy_len as usize).min(MAX_HANDOFFS) {
        let conn = s.worker.busy[i];
        let b = s.worker.bindings[usize::from(conn)];
        match b.phase {
            Phase::Draining => {
                {
                    let bm = &mut s.worker.bindings[usize::from(conn)];
                    if bm.dirty != 0 {
                        bm.quiet = 0;
                        bm.dirty = 0;
                    } else {
                        bm.quiet = bm.quiet.saturating_add(1);
                    }
                }
                if quiescent(s, conn) {
                    // The whole export goes out in one step; a control
                    // edge with no room would lose a chunk and refuse
                    // the handoff as corrupt. Wait for room instead.
                    let ch = s.worker.out_ctrl;
                    let poll = if ch >= 0 {
                        (sys.channel_poll)(ch, 0x02)
                    } else {
                        0
                    };
                    if poll > 0 && (poll as u32 & 0x02) != 0 {
                        export(s, sys, conn, now);
                    }
                }
            }
            Phase::Attached => {
                if b.settle_at_ms != 0 && now >= b.settle_at_ms {
                    let body = b.slot.to_le_bytes();
                    if (b.slot as usize) < MAX_SESSIONS
                        && super::try_emit(sys, s.out_messaging, wire::MSG_OFFLINE_RECONNECT, &body)
                    {
                        s.worker.bindings[usize::from(conn)].settle_at_ms = 0;
                        busy_remove(&mut s.worker, conn);
                        continue; // the swap-in moved the last entry here
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    // Every attachment starts the next step clean.
    for j in 0..(s.worker.busy_len as usize).min(MAX_HANDOFFS) {
        let conn = s.worker.busy[j];
        let bm = &mut s.worker.bindings[usize::from(conn)];
        if bm.phase != Phase::Draining {
            bm.dirty = 0;
        }
    }
}

/// Fill the component's metric payload.
pub fn metrics(w: &Worker, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&w.attached.to_le_bytes());
    m[4..8].copy_from_slice(&w.exported.to_le_bytes());
    m[8..12].copy_from_slice(&w.imported.to_le_bytes());
    m[12..16].copy_from_slice(&w.resumed.to_le_bytes());
    m[16..20].copy_from_slice(&w.refused.to_le_bytes());
    m[20..24].copy_from_slice(&w.frozen_parked.to_le_bytes());
    24
}
