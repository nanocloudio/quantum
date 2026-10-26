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

// ── SessionCtrlV1: the SDK contract itself, not a local copy ─────────
use super::abi::contracts::net::session_ctrl as sc;

const SID: usize = sc::SESSION_ID_BYTES;
const SID_EPOCH: usize = sc::SESSION_HEADER;
const ATTACH_LEN: usize = sc::ATTACH_PAYLOAD_LEN;
const EXPORT_BEGIN_LEN: usize = sc::EXPORT_BEGIN_LEN;

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
    /// The anchor seam the attachment was made on (0: `ctrl_in` /
    /// `codec_in`, 1: `ctrl2_in` / `codec2_in`): where the session's
    /// responses and control frames go.
    seam: u8,
    /// `1 + index` into `imports` while importing; 0 = none.
    import: u16,
    in_consumed: u64,
    out_produced: u64,
    /// Slot the record was placed at, so RESUME can address it.
    slot: u32,
    /// A second `MSG_OFFLINE_RECONNECT` is due at this time (0 = none).
    settle_at_ms: u64,
    /// Highest delivery id consumed for the session: a delivery envelope
    /// at or below it is a retry (docs/architecture/mqtt_anchor.md).
    delivery_hw: u64,
    /// Delivery high-water of the last checkpoint sent for the session.
    ckpt_hw: u64,
}

impl Attachment {
    const fn zero() -> Self {
        Self {
            session_id: [0; SID],
            epoch: 0,
            phase: Phase::Idle,
            quiet: 0,
            dirty: 0,
            seam: 0,
            import: 0,
            in_consumed: 0,
            out_produced: 0,
            slot: 0,
            settle_at_ms: 0,
            delivery_hw: 0,
            ckpt_hw: 0,
        }
    }
}

/// Checkpoints this worker holds for sessions the OTHER worker serves,
/// as a standby: the shadows a worker loss adopts from. Bounds how many
/// of the other worker's sessions a loss can keep open; past it the
/// anchor closes the rest (no acknowledged checkpoint).
const SHADOW_SLOTS: usize = 256;

#[repr(C)]
#[derive(Clone, Copy)]
struct Shadow {
    /// 0 = free, otherwise `1 + conn`.
    conn: u32,
    /// The checkpoint being reassembled.
    asm: HandoffImport,
    asm_sid: [u8; SID],
    asm_epoch: u32,
    /// The last complete checkpoint, adoptable.
    ready: u8,
    ready_sid: [u8; SID],
    ready_epoch: u32,
    ready_len: u32,
}

impl Shadow {
    const fn zero() -> Self {
        Self {
            conn: 0,
            asm: HandoffImport::new(),
            asm_sid: [0; SID],
            asm_epoch: 0,
            ready: 0,
            ready_sid: [0; SID],
            ready_epoch: 0,
            ready_len: 0,
        }
    }
}

#[repr(C)]
pub struct Worker {
    pub in_ctrl: i32,
    pub out_ctrl: i32,
    /// The second anchor seam's control pair (`ctrl2_in` / `ctrl2_out`):
    /// a worker behind two anchors — a paired ingress host's — serves each
    /// session on the seam its attachment came from.
    pub in_ctrl2: i32,
    pub out_ctrl2: i32,
    /// While a control frame is handled, the seam it came from: answers go
    /// back to the anchor that asked.
    reply_seam: Option<u8>,
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
    /// PINGRESP productions a full seat ring refused (the client pings
    /// again; nothing else is lost).
    pub pingresp_refused: u32,
    pub frozen_parked: u32,
    /// Attachments the step list could not take because it was full.
    /// Non-zero means a swap larger than [`MAX_HANDOFFS`] was asked
    /// for: the attachments past the bound are never stepped, so their
    /// drains never reach `DRAINED` and the anchor refuses those
    /// handoffs on its deadline. The sessions keep serving — a refused
    /// handoff returns them — but the swap only half happened, and the
    /// count is what says so.
    pub busy_overflow: u32,
    /// Delivery envelopes refused, by reason: an epoch the session has
    /// left (a stale anchor binding), an epoch it has not been resumed
    /// at, a retried delivery id, and no live binding for the session.
    pub delivery_stale: u32,
    pub delivery_future: u32,
    pub delivery_dup: u32,
    pub delivery_unknown: u32,
    /// Committed swap records forwarded to the anchor.
    pub swaps_forwarded: u32,
    /// Checkpoint period (param 12; 0 = no checkpoints) and when the last
    /// pass ran, and where the next one resumes its walk.
    pub checkpoint_ms: u32,
    last_ckpt_ms: u64,
    ckpt_next: u32,
    shadows: [Shadow; SHADOW_SLOTS],
    shadow_asm: [[u8; sessions::RECORD_MAX]; SHADOW_SLOTS],
    shadow_ready: [[u8; sessions::RECORD_MAX]; SHADOW_SLOTS],
    /// Checkpoints sent, kept as a standby, sessions adopted after a
    /// worker loss, and adoptions refused.
    pub ckpt_sent: u32,
    pub ckpt_kept: u32,
    pub adopted: u32,
    pub adopt_refused: u32,
    /// Anchor continuity (docs/architecture/anchor_continuity.md): the
    /// sessions attached as transport-migratable, each with its retained
    /// productions.
    pub seats: anchor_core::ProductionSeats,
    /// Per seat, the `(conn, epoch, delivery_hw)` last reported to the
    /// anchor (`MSG_ANCHOR_CONSUMED`).
    consumed_told: [(u16, u32, u64); anchor_core::SEAT_COUNT],
    /// Consumption reports the control edge took.
    pub consumed_reports: u32,
}

include!("../../../target/fluxor/fluxor-abi/sdk/cores/session_handoff.rs");
pub mod anchor_core {
    include!("../../common/cores/mqtt_anchor_core.rs");
    include!("../../common/cores/anchor_continuity_core.rs");
    include!("../../common/cores/production_seat_core.rs");
}
use anchor_core::{
    admit_delivery, decode_delivery, swap_seq, unwrap_delivery_in_place, DeliveryVerdict,
};

/// Reset the worker's runtime state. The fields set from module
/// parameters (`cursor_skew`, `checkpoint_ms`) are parsed before this
/// runs and are left as configured.
pub fn init(w: &mut Worker, worker_id: u8) {
    w.in_ctrl = -1;
    w.out_ctrl = -1;
    w.in_ctrl2 = -1;
    w.out_ctrl2 = -1;
    w.reply_seam = None;
    w.seats = anchor_core::ProductionSeats::new();
    w.consumed_told = [(0, 0, 0); anchor_core::SEAT_COUNT];
    w.consumed_reports = 0;
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
    w.pingresp_refused = 0;
    w.frozen_parked = 0;
    w.delivery_stale = 0;
    w.delivery_future = 0;
    w.delivery_dup = 0;
    w.delivery_unknown = 0;
    w.swaps_forwarded = 0;
    w.last_ckpt_ms = 0;
    w.ckpt_next = 0;
    for sh in w.shadows.iter_mut() {
        *sh = Shadow::zero();
    }
    w.ckpt_sent = 0;
    w.ckpt_kept = 0;
    w.adopted = 0;
    w.adopt_refused = 0;
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

/// A `MSG_ANCHOR_DELIVERY` envelope of `bytes` (header included) was
/// read: counted as [`note_in`] when it is at its binding's epoch.
pub fn note_in_delivery(w: &mut Worker, frame: &[u8], bytes: usize) {
    if let Some(h) = decode_delivery(frame) {
        if w.bindings[usize::from(h.conn)].epoch == h.epoch {
            note_in(w, h.conn, bytes);
        }
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

/// Admit the `MSG_ANCHOR_DELIVERY` envelope in `buf[..n]` against the
/// binding it names, and unwrap it in place. Returns the inner envelope's
/// message type and length when it is new work at the session's current
/// epoch; `None` when it is refused — a stale or future epoch, a retried
/// delivery id, or no live binding — which is counted by reason.
///
/// A retry is refused here rather than consumed again: the id names a
/// frame the session already took, and what that frame changed is in
/// the session's state (and, past a handoff, in the record it carried).
pub fn admit(w: &mut Worker, buf: &mut [u8], n: usize) -> Option<(u8, usize)> {
    let Some(h) = decode_delivery(&buf[..n.min(buf.len())]) else {
        w.delivery_unknown = w.delivery_unknown.wrapping_add(1);
        return None;
    };
    let b = &mut w.bindings[usize::from(h.conn)];
    let verdict = if matches!(b.phase, Phase::Attached | Phase::Draining) {
        admit_delivery(&b.session_id, b.epoch, b.delivery_hw, &h)
    } else {
        DeliveryVerdict::UnknownSession
    };
    match verdict {
        DeliveryVerdict::Accept => {
            b.delivery_hw = h.delivery_id;
            let (_, m) = unwrap_delivery_in_place(buf, n)?;
            Some((h.inner_msg_type, m))
        }
        DeliveryVerdict::StaleEpoch => {
            w.delivery_stale = w.delivery_stale.wrapping_add(1);
            None
        }
        DeliveryVerdict::FutureEpoch => {
            w.delivery_future = w.delivery_future.wrapping_add(1);
            None
        }
        DeliveryVerdict::Duplicate => {
            w.delivery_dup = w.delivery_dup.wrapping_add(1);
            None
        }
        DeliveryVerdict::UnknownSession => {
            w.delivery_unknown = w.delivery_unknown.wrapping_add(1);
            None
        }
    }
}

/// The delivery high-water of the attachment on `conn_id`.
pub fn delivery_hw(w: &Worker, conn_id: u16) -> u64 {
    w.bindings[usize::from(conn_id)].delivery_hw
}

/// Forward a committed `QASW` swap record to the anchor as
/// `MSG_ANCHOR_SWAP`, sequenced by its place in the log so the anchor
/// executes it once however many workers forward it.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn forward_swap_record(
    w: &mut Worker,
    sys: &SyscallTable,
    body: &[u8],
    partition: u16,
    index: u64,
) -> bool {
    if body.len() < wire::QA_SWAP_RECORD_LEN || body[..4] != wire::QA_SWAP_RECORD_MAGIC {
        return false;
    }
    let mut cmd = [0u8; anchor_core::SWAP_CMD_LEN];
    cmd[..9].copy_from_slice(&body[4..13]);
    cmd[9..17].copy_from_slice(&swap_seq(partition, index).to_le_bytes());
    // Addressed by anchor id: every anchor seam hears it, and the anchor
    // it names executes it.
    let mut sent = false;
    for ch in [w.out_ctrl, w.out_ctrl2] {
        sent |= ch >= 0 && wire::channel_write_msg(sys, ch, wire::MSG_ANCHOR_SWAP, &cmd) > 0;
    }
    if sent {
        w.swaps_forwarded = w.swaps_forwarded.wrapping_add(1);
    }
    true
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

/// The anchor seam `conn`'s attachment was made on.
pub fn seam(w: &Worker, conn: u16) -> u8 {
    w.bindings[usize::from(conn)].seam
}

/// The control channel to the anchor `conn`'s attachment was made on.
pub fn ctrl_out_for(w: &Worker, conn: u16) -> i32 {
    if seam(w, conn) == 1 {
        w.out_ctrl2
    } else {
        w.out_ctrl
    }
}

/// Write a control frame to an anchor: back to the one being answered
/// while a control frame is handled; otherwise to the seam the session
/// the payload's SessionCtrlV1 header names is attached on.
unsafe fn write_ctrl(w: &mut Worker, sys: &SyscallTable, mt: u8, payload: &[u8]) -> bool {
    let seam = match w.reply_seam {
        Some(k) => k,
        None if payload.len() >= SID => {
            let b = &w.bindings[usize::from(sid_conn(payload))];
            if b.session_id[..] == payload[..SID] {
                b.seam
            } else {
                0
            }
        }
        None => 0,
    };
    let ch = if seam == 1 { w.out_ctrl2 } else { w.out_ctrl };
    ch >= 0 && wire::channel_write_msg(sys, ch, mt, payload) > 0
}

/// Tell the anchor how far seat `i`'s session has consumed
/// (`MSG_ANCHOR_CONSUMED`) when its high-water moved since the last
/// report: the anchor acknowledges to the transport only what this
/// covers, so a delivery lost between the two is still the transport's to
/// hand again.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn report_consumed(w: &mut Worker, sys: &SyscallTable, i: usize) {
    let (used, seat_conn, seat_sid) = {
        let seat = &w.seats.seats[i];
        (seat.used, seat.conn, seat.session_id)
    };
    let b = w.bindings[usize::from(seat_conn)];
    if !used || b.session_id != seat_sid || !matches!(b.phase, Phase::Attached | Phase::Draining) {
        return;
    }
    let (conn, epoch, hw) = w.consumed_told[i];
    if conn == seat_conn && epoch == b.epoch && hw >= b.delivery_hw {
        return;
    }
    let mut p = [0u8; wire::ANCHOR_CONSUMED_LEN];
    p[..SID].copy_from_slice(&b.session_id);
    p[SID..SID_EPOCH].copy_from_slice(&b.epoch.to_le_bytes());
    p[SID_EPOCH..].copy_from_slice(&b.delivery_hw.to_le_bytes());
    if write_ctrl(w, sys, wire::MSG_ANCHOR_CONSUMED, &p) {
        w.consumed_told[i] = (seat_conn, b.epoch, b.delivery_hw);
        w.consumed_reports = w.consumed_reports.wrapping_add(1);
    }
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

/// The anchor's production gate position a RESUME or ADOPT carries for a
/// session under anchor continuity, after the session header:
/// `[next_production:u64 LE][bytes_written:u64 LE]`. `None` without one.
fn seat_tail(p: &[u8]) -> Option<(u64, u64)> {
    let t = p.get(SID_EPOCH..SID_EPOCH + 16)?;
    let mut a = [0u8; 8];
    let mut b = [0u8; 8];
    a.copy_from_slice(&t[..8]);
    b.copy_from_slice(&t[8..]);
    Some((u64::from_le_bytes(a), u64::from_le_bytes(b)))
}

/// The attachment is at a protocol-coherent point: nothing of its is in
/// flight anywhere this worker can see, and it has been quiet.
fn quiescent(s: &ModuleState, conn_id: u16) -> bool {
    let b = &s.worker.bindings[usize::from(conn_id)];
    // A seated session moves only once every production it made is inside
    // the transport's horizon: the worker it moves to cannot re-offer one
    // it never retained.
    if b.quiet < QUIET_STEPS || !s.worker.seats.settled(conn_id) {
        return false;
    }
    match sessions::find_by_conn(&s.sessions, conn_id) {
        Some(si) => coherent(s, si),
        // No MQTT session yet (a CONNECT is still in the anchor's
        // hold, or the client never sent one): nothing to carry.
        None => true,
    }
}

/// Session `si` has nothing of its in flight anywhere this worker can
/// see: no proposal awaiting its Raft assignment, no delivery or ack
/// registration parked, no publish still in the commit-gating stash.
/// A record taken now reflects a protocol-coherent point.
fn coherent(s: &ModuleState, si: usize) -> bool {
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

// ── Checkpoints and adoption (docs/architecture/mqtt_anchor.md, "Worker loss") ──

/// Checkpoints at most this many sessions per pass, so a pass is bounded
/// in work and in control-edge bytes.
const CKPT_PER_PASS: u32 = 4;
/// Session slots one pass looks at.
const CKPT_SCAN: u32 = 2048;

/// Checkpoint the sessions this worker serves whose consumed delivery
/// high-water moved since their last checkpoint and which are at a
/// coherent point, through the anchor to the standby. Bounded per pass.
unsafe fn checkpoint_pass(s: &mut ModuleState, sys: &SyscallTable, now: u64) {
    if s.worker.checkpoint_ms == 0
        || now.saturating_sub(s.worker.last_ckpt_ms) < u64::from(s.worker.checkpoint_ms)
    {
        return;
    }
    s.worker.last_ckpt_ms = now;
    let mut sent = 0u32;
    let mut k = 0u32;
    while k < CKPT_SCAN && sent < CKPT_PER_PASS {
        let si = (s.worker.ckpt_next as usize) % MAX_SESSIONS;
        s.worker.ckpt_next = s.worker.ckpt_next.wrapping_add(1);
        k += 1;
        // Durable, live and connected here: a transient session's
        // CONNECT has not applied, so a record of it would precede it.
        let Some(v) = sessions::view(&s.sessions, si) else {
            continue;
        };
        if !v.active || v.transient {
            continue;
        }
        let conn = v.conn_id;
        let b = s.worker.bindings[usize::from(conn)];
        if b.phase != Phase::Attached || b.delivery_hw <= b.ckpt_hw || !coherent(s, si) {
            continue;
        }
        // Sent only when the control edge has room. A checkpoint the edge
        // refuses part of is discarded by the standby (its END never
        // arrives, and the next BEGIN restarts the assembly), and the
        // session stays due, so the next pass sends it again.
        let ch = ctrl_out_for(&s.worker, conn);
        let poll = if ch >= 0 {
            (sys.channel_poll)(ch, 0x02)
        } else {
            0
        };
        if poll <= 0 || (poll as u32 & 0x02) == 0 {
            break;
        }
        let tail = super::replay::HandoffTail::from_apply(b.delivery_hw, &s.apply_index);
        let mut tb = [0u8; super::replay::TAIL_MAX];
        let tn = tail.encode(&mut tb);
        let bn = s.sub_book.export_section(si as u32, &mut s.book_buf);
        let len = sessions::export_record(
            &s.sessions,
            si,
            now,
            &mut s.worker.export_buf,
            &s.book_buf[..bn],
            &tb[..tn],
        );
        if len == 0 {
            continue;
        }
        let cursors = SessionCursors::new(b.in_consumed, b.out_produced);
        if send_record_frames(
            &mut s.worker,
            sys,
            &b.session_id,
            b.epoch,
            len,
            &cursors,
            [
                wire::MSG_ANCHOR_CKPT_BEGIN,
                wire::MSG_ANCHOR_CKPT_CHUNK,
                wire::MSG_ANCHOR_CKPT_END,
            ],
        ) {
            s.worker.bindings[usize::from(conn)].ckpt_hw = b.delivery_hw;
            s.worker.ckpt_sent = s.worker.ckpt_sent.wrapping_add(1);
            sent += 1;
        }
    }
}

/// The shadow slot holding `conn`'s checkpoints, or a free one for it.
fn shadow_for(w: &mut Worker, conn: u16, alloc: bool) -> Option<usize> {
    let key = u32::from(conn) + 1;
    if let Some(i) = w.shadows.iter().position(|sh| sh.conn == key) {
        return Some(i);
    }
    if !alloc {
        return None;
    }
    let i = w.shadows.iter().position(|sh| sh.conn == 0)?;
    w.shadows[i] = Shadow::zero();
    w.shadows[i].conn = key;
    Some(i)
}

/// `MSG_ANCHOR_CKPT_ACK [sid][epoch][status][delivery_hw]`.
unsafe fn send_ckpt_ack(
    w: &mut Worker,
    sys: &SyscallTable,
    sid: &[u8; SID],
    epoch: u32,
    status: u8,
    hw: u64,
) {
    let mut p = [0u8; wire::ANCHOR_CKPT_ACK_LEN];
    p[..SID].copy_from_slice(sid);
    p[SID..SID_EPOCH].copy_from_slice(&epoch.to_le_bytes());
    p[SID_EPOCH] = status;
    p[SID_EPOCH + 1..SID_EPOCH + 9].copy_from_slice(&hw.to_le_bytes());
    write_ctrl(w, sys, wire::MSG_ANCHOR_CKPT_ACK, &p);
}

/// A checkpoint frame relayed from the other worker: reassemble it into
/// the session's shadow, and on a complete, verified record make it the
/// adoptable one and acknowledge it with the delivery high-water it
/// covers. A checkpoint is kept, never imported: the session is the
/// other worker's until a loss says otherwise.
unsafe fn on_checkpoint(s: &mut ModuleState, sys: &SyscallTable, mt: u8, p: &[u8]) {
    let n = p.len();
    if n < SID_EPOCH + 4 {
        return;
    }
    let conn = sid_conn(p);
    let mut sid = [0u8; SID];
    sid.copy_from_slice(&p[..SID]);
    let epoch = epoch_at(p);
    let w = &mut s.worker;
    match mt {
        wire::MSG_ANCHOR_CKPT_BEGIN => {
            if n < EXPORT_BEGIN_LEN {
                return;
            }
            let Some(i) = shadow_for(w, conn, true) else {
                // Every shadow in use: this session stays unadoptable
                // until one frees, and the anchor closes it on a loss.
                send_ckpt_ack(w, sys, &sid, epoch, sc::STATUS_NO_CAPACITY, 0);
                return;
            };
            let total = u32::from_le_bytes([
                p[SID_EPOCH],
                p[SID_EPOCH + 1],
                p[SID_EPOCH + 2],
                p[SID_EPOCH + 3],
            ]);
            let sh = &mut w.shadows[i];
            sh.asm = HandoffImport::new();
            sh.asm_sid = sid;
            sh.asm_epoch = epoch;
            let st = sh.asm.begin(total, sessions::RECORD_MAX as u32);
            if st != HANDOFF_OK {
                send_ckpt_ack(w, sys, &sid, epoch, st, 0);
            }
        }
        wire::MSG_ANCHOR_CKPT_CHUNK => {
            let Some(i) = shadow_for(w, conn, false) else {
                return;
            };
            if w.shadows[i].asm_sid != sid {
                return;
            }
            let off = u32::from_le_bytes([
                p[SID_EPOCH],
                p[SID_EPOCH + 1],
                p[SID_EPOCH + 2],
                p[SID_EPOCH + 3],
            ]);
            let st = w.shadows[i]
                .asm
                .chunk(off, &p[SID_EPOCH + 4..], &mut w.shadow_asm[i]);
            if st != HANDOFF_OK {
                w.shadows[i].asm.reset();
            }
        }
        wire::MSG_ANCHOR_CKPT_END => {
            let Some(i) = shadow_for(w, conn, false) else {
                return;
            };
            if w.shadows[i].asm_sid != sid {
                return;
            }
            let crc = u32::from_le_bytes([
                p[SID_EPOCH],
                p[SID_EPOCH + 1],
                p[SID_EPOCH + 2],
                p[SID_EPOCH + 3],
            ]);
            let st = w.shadows[i].asm.end(crc);
            let len = (w.shadows[i].asm.total_len() as usize).min(sessions::RECORD_MAX);
            let record = &w.shadow_asm[i][..len];
            // A checkpoint is adoptable only with the delivery high-water
            // its tail states: replay after a loss starts above it.
            let hw = sessions::record_tail(record)
                .and_then(super::replay::HandoffTail::decode)
                .map(|t| t.delivery_hw)
                .filter(|_| st == HANDOFF_OK && sessions::record_identity(record).is_some());
            if let Some(hw) = hw {
                w.shadow_ready[i] = w.shadow_asm[i];
                let sh = &mut w.shadows[i];
                sh.ready = 1;
                sh.ready_sid = sid;
                sh.ready_epoch = epoch;
                sh.ready_len = len as u32;
                w.ckpt_kept = w.ckpt_kept.wrapping_add(1);
                send_ckpt_ack(w, sys, &sid, epoch, sc::STATUS_OK, hw);
            } else {
                send_ckpt_ack(w, sys, &sid, epoch, sc::STATUS_CORRUPT, 0);
            }
            w.shadows[i].asm.reset();
        }
        _ => {}
    }
}

/// `MSG_ANCHOR_ADOPT [sid][new_epoch]`: the other worker was lost, take
/// the session from its last checkpoint. The record is committed as an
/// import (identity, generation and the replay rules of an import), the
/// session is bound here at `new_epoch` with cursors counted afresh, its
/// subscriptions and in-flight registrations are re-pointed here, and
/// what the log says it is owed is redelivered. Frames after the
/// checkpoint are the anchor's to replay; their ops are idempotent at
/// apply (the dedupe ledger for PUBLISH and PUBREL, set semantics for
/// SUBSCRIBE and UNSUBSCRIBE), so the entries this worker skipped past
/// the checkpoint while the session was not its own are re-derived
/// rather than refused.
unsafe fn adopt(s: &mut ModuleState, sys: &SyscallTable, p: &[u8], seam: u8, now: u64) {
    if p.len() < SID_EPOCH {
        return;
    }
    let conn = sid_conn(p);
    let mut sid = [0u8; SID];
    sid.copy_from_slice(&p[..SID]);
    let new_epoch = epoch_at(p);
    let refuse = |s: &mut ModuleState, status: u8| {
        s.worker.adopt_refused = s.worker.adopt_refused.wrapping_add(1);
        send_status(
            &mut s.worker,
            sys,
            sc::MSG_SC_ERROR,
            &sid,
            new_epoch,
            status,
        );
    };
    let Some(i) = shadow_for(&mut s.worker, conn, false) else {
        refuse(s, sc::STATUS_UNKNOWN_SESSION);
        return;
    };
    let sh = s.worker.shadows[i];
    if sh.ready == 0 || sh.ready_sid != sid || new_epoch <= sh.ready_epoch {
        refuse(s, sc::STATUS_STALE_EPOCH);
        return;
    }
    if s.worker.bindings[usize::from(conn)].phase != Phase::Idle {
        refuse(s, sc::STATUS_NOT_READY);
        return;
    }
    // Under anchor continuity the session's responses carry on in the
    // anchor's numbering, from a seat of this worker's.
    let tail = seat_tail(p);
    if tail.is_some() && !s.worker.seats.can_open(conn, &sid) {
        s.worker.seats.refused = s.worker.seats.refused.wrapping_add(1);
        refuse(s, sc::STATUS_NO_CAPACITY);
        return;
    }
    let blob = s.worker.shadow_ready[i];
    let len = sh.ready_len as usize;
    let mut slot = u32::MAX;
    let st = commit_import(s, sys, &blob[..len], conn, now, &mut slot, true);
    if st != HANDOFF_OK || slot as usize >= MAX_SESSIONS {
        refuse(s, st);
        return;
    }
    let hw = s.worker.bindings[usize::from(conn)].delivery_hw;
    {
        let b = &mut s.worker.bindings[usize::from(conn)];
        *b = Attachment::zero();
        b.session_id = sid;
        b.epoch = new_epoch;
        b.phase = Phase::Attached;
        b.slot = slot;
        b.delivery_hw = hw;
        b.ckpt_hw = hw;
        b.seam = seam;
    }
    s.worker.shadows[i] = Shadow::zero();
    if let Some((next, bytes)) = tail {
        s.worker
            .seats
            .open_resumed(conn, &sid, new_epoch, next, bytes);
    }
    after_resume(s, sys, conn, now);
    super::redeliver_owed(s, sys, slot as usize);
    s.worker.adopted = s.worker.adopted.wrapping_add(1);
    send_sid_epoch(&mut s.worker, sys, sc::MSG_SC_RESUMED, conn);
    mon(
        &mut s.worker,
        sys,
        conn,
        super::MON_EV_RESUMED,
        b"",
        b"adopted",
    );
}

/// The record in `export_buf[..len]` as three-frame transfer `types`
/// (`[begin, chunk, end]`): EXPORT_* for a handoff, `MSG_ANCHOR_CKPT_*`
/// for a checkpoint. Same layouts either way:
/// BEGIN `[sid][epoch][total_len][in_consumed][out_produced]`,
/// CHUNK `[sid][epoch][offset][data]`, END `[sid][epoch][crc32]`.
/// `false` when the control edge refused any frame.
unsafe fn send_record_frames(
    w: &mut Worker,
    sys: &SyscallTable,
    sid: &[u8; SID],
    epoch: u32,
    len: usize,
    cursors: &SessionCursors,
    types: [u8; 3],
) -> bool {
    let mut begin = [0u8; EXPORT_BEGIN_LEN];
    begin[..SID].copy_from_slice(sid);
    begin[SID..SID_EPOCH].copy_from_slice(&epoch.to_le_bytes());
    begin[SID_EPOCH..SID_EPOCH + 4].copy_from_slice(&(len as u32).to_le_bytes());
    let mut cur = [0u8; CURSOR_PAIR_LEN];
    cursors.encode(&mut cur);
    begin[SID_EPOCH + 4..].copy_from_slice(&cur);
    let mut ok = write_ctrl(w, sys, types[0], &begin);

    let mut exp = HandoffExport::new(len as u32);
    let mut chunk = [0u8; SID_EPOCH + 4 + EXPORT_CHUNK];
    chunk[..SID].copy_from_slice(sid);
    chunk[SID..SID_EPOCH].copy_from_slice(&epoch.to_le_bytes());
    while let Some((off, n)) = exp.next_chunk(EXPORT_CHUNK as u32) {
        chunk[SID_EPOCH..SID_EPOCH + 4].copy_from_slice(&off.to_le_bytes());
        let base = SID_EPOCH + 4;
        let (o, n) = (off as usize, n as usize);
        chunk[base..base + n].copy_from_slice(&w.export_buf[o..o + n]);
        ok &= write_ctrl(w, sys, types[1], &chunk[..base + n]);
        exp.advance(n as u32);
    }
    let mut end = [0u8; SID_EPOCH + 4];
    end[..SID].copy_from_slice(sid);
    end[SID..SID_EPOCH].copy_from_slice(&epoch.to_le_bytes());
    end[SID_EPOCH..].copy_from_slice(&handoff_crc32(&w.export_buf[..len]).to_le_bytes());
    ok &= write_ctrl(w, sys, types[2], &end);
    ok
}

/// DRAINED, then the record as EXPORT_BEGIN / CHUNK… / END.
unsafe fn export(s: &mut ModuleState, sys: &SyscallTable, conn_id: u16, now: u64) {
    let (slot, len) = match sessions::find_by_conn(&s.sessions, conn_id) {
        Some(si) => {
            // Where the state sits: the apply cursor it reflects and the
            // highest delivery id the session consumed (QSR2 tail).
            let hw = s.worker.bindings[usize::from(conn_id)].delivery_hw;
            let tail = super::replay::HandoffTail::from_apply(hw, &s.apply_index);
            let mut tb = [0u8; super::replay::TAIL_MAX];
            let tn = tail.encode(&mut tb);
            let bn = s.sub_book.export_section(si as u32, &mut s.book_buf);
            let n = sessions::export_record(
                &s.sessions,
                si,
                now,
                &mut s.worker.export_buf,
                &s.book_buf[..bn],
                &tb[..tn],
            );
            (si as u32, n)
        }
        None => (u32::MAX, 0),
    };
    let b = s.worker.bindings[usize::from(conn_id)];
    let w = &mut s.worker;
    if !send_sid_epoch(w, sys, sc::MSG_SC_DRAINED, conn_id) {
        return;
    }
    w.drained = w.drained.wrapping_add(1);
    mon(w, sys, conn_id, super::MON_EV_DRAINED, b"", b"");

    let cursors = SessionCursors::new(
        b.in_consumed.wrapping_add(u64::from(w.cursor_skew)),
        b.out_produced,
    );
    let ok = send_record_frames(
        w,
        sys,
        &b.session_id,
        b.epoch,
        len,
        &cursors,
        [
            sc::CMD_SC_EXPORT_BEGIN,
            sc::CMD_SC_EXPORT_CHUNK,
            sc::CMD_SC_EXPORT_END,
        ],
    );

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
        send_status(
            w,
            sys,
            sc::MSG_SC_ERROR,
            &sid,
            b.epoch,
            sc::STATUS_NOT_READY,
        );
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

/// End the binding on `conn`: what the phase it is in holds is released,
/// `DETACHED` is answered and the slot is free for the next attachment.
/// `reason` is the anchor's (`sc::DETACH_*`); a client that went away
/// also releases the connection's non-MQTT state.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn detach(s: &mut ModuleState, sys: &SyscallTable, conn: u16, reason: u8) {
    let b = s.worker.bindings[usize::from(conn)];
    match b.phase {
        Phase::Exported => {
            // The session lives on the other worker now: give up the
            // record but keep the index reserved.
            if (b.slot as usize) < MAX_SESSIONS {
                sessions::clear_flow(&mut s.sessions, b.slot as usize);
                sessions::lend(&mut s.sessions, b.slot as usize);
                // Its book entries went with its record.
                s.sub_book.clear(b.slot);
            }
        }
        Phase::Importing | Phase::Imported => {
            // A refused handoff: discard the half-import. A
            // committed-but-unresumed record is dropped the same way; the
            // exporter still holds its own.
            if b.phase == Phase::Imported && (b.slot as usize) < MAX_SESSIONS {
                sessions::clear_flow(&mut s.sessions, b.slot as usize);
                sessions::lend(&mut s.sessions, b.slot as usize);
            }
        }
        Phase::Attached | Phase::Draining => {
            if reason == sc::DETACH_CLIENT_GONE {
                super::handle_conn_disconnect(s, conn);
            }
        }
        Phase::Idle => {}
    }
    import_free(&mut s.worker, conn);
    s.worker.seats.close(conn);
    send_sid_epoch(&mut s.worker, sys, sc::MSG_SC_DETACHED, conn);
    mon(&mut s.worker, sys, conn, super::MON_EV_DETACHED, b"", b"");
    s.worker.bindings[usize::from(conn)] = Attachment::zero();
    busy_remove(&mut s.worker, conn);
}

/// The anchor's in-band notice that the client on `conn` went away: an
/// admitted `MSG_CONN_CLOSED` delivery. It travels on the data channel
/// behind the client's last frames, so the session consumes its
/// DISCONNECT, say, before the binding ends — a `DETACH` on the control
/// channel could overtake it, since a step reads control first. Returns
/// `true` when it ended an anchored binding, which also released the
/// connection's non-MQTT state.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn client_gone(s: &mut ModuleState, sys: &SyscallTable, conn: u16) -> bool {
    if !matches!(
        s.worker.bindings[usize::from(conn)].phase,
        Phase::Attached | Phase::Draining
    ) {
        return false;
    }
    detach(s, sys, conn, sc::DETACH_CLIENT_GONE);
    true
}

/// Drain each anchor seam's control input (`ctrl_in`, `ctrl2_in`),
/// answering on the seam a frame came from. Bounded per step.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn handle_ctrl(s: &mut ModuleState, sys: &SyscallTable, now: u64) -> u32 {
    let mut worked = 0u32;
    for seam in 0..2u8 {
        let chan = if seam == 0 {
            s.worker.in_ctrl
        } else {
            s.worker.in_ctrl2
        };
        if chan < 0 {
            continue;
        }
        s.worker.reply_seam = Some(seam);
        worked += handle_ctrl_from(s, sys, chan, seam, now);
    }
    s.worker.reply_seam = None;
    worked
}

/// Drain one anchor seam's control input `chan`.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn handle_ctrl_from(
    s: &mut ModuleState,
    sys: &SyscallTable,
    chan: i32,
    seam: u8,
    now: u64,
) -> u32 {
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
            wire::MSG_ANCHOR_PROBE => {
                write_ctrl(&mut s.worker, sys, wire::MSG_ANCHOR_PROBE_ACK, p);
            }
            wire::MSG_ANCHOR_CKPT_BEGIN
            | wire::MSG_ANCHOR_CKPT_CHUNK
            | wire::MSG_ANCHOR_CKPT_END => {
                on_checkpoint(s, sys, mt, p);
            }
            wire::MSG_ANCHOR_ADOPT => adopt(s, sys, p, seam, now),
            wire::MSG_ANCHOR_HORIZON => {
                s.worker.seats.horizon(p);
            }
            wire::MSG_ANCHOR_REANCHOR => reanchor(s, sys, p, seam),
            wire::MSG_SITE_EXPORT => super::site::export(s, sys, p),
            sc::CMD_SC_HELLO => {
                if n >= 9 {
                    let mut ack = [0u8; 9];
                    ack[0] = sc::ROLE_WORKER;
                    ack[1..].copy_from_slice(&s.worker.id_bytes);
                    write_ctrl(&mut s.worker, sys, sc::MSG_SC_HELLO_ACK, &ack);
                    dev_log(sys, 3, b"[sess] anchor hello".as_ptr(), 19);
                }
            }
            sc::CMD_SC_ATTACH => {
                if n < ATTACH_LEN {
                    continue;
                }
                let conn = sid_conn(p);
                let mut sid = [0u8; SID];
                sid.copy_from_slice(&p[..SID]);
                let epoch = u32::from_le_bytes([p[SID + 8], p[SID + 9], p[SID + 10], p[SID + 11]]);
                // Transport-migratable: the session's responses become
                // productions retained until the anchor's horizon covers
                // them; no seat free refuses the attachment.
                let migratable = p[SID + 12] == sc::CC_TRANSPORT_MIGRATABLE;
                let idle = matches!(
                    s.worker.bindings[usize::from(conn)].phase,
                    Phase::Idle | Phase::Attached
                );
                let seated = !migratable || !idle || s.worker.seats.open(conn, &sid, epoch);
                if !migratable {
                    s.worker.seats.close(conn);
                }
                let b = &mut s.worker.bindings[usize::from(conn)];
                // A new attachment on a recycled conn id replaces a
                // stale one; an attachment mid-handoff is refused.
                let status = if idle && seated {
                    *b = Attachment::zero();
                    b.session_id = sid;
                    b.epoch = epoch;
                    b.phase = Phase::Attached;
                    b.seam = seam;
                    sc::STATUS_OK
                } else {
                    sc::STATUS_NOT_READY
                };
                send_status(&mut s.worker, sys, sc::MSG_SC_ATTACHED, &sid, epoch, status);
                if status == sc::STATUS_OK {
                    s.worker.attached = s.worker.attached.wrapping_add(1);
                    mon(&mut s.worker, sys, conn, super::MON_EV_ATTACHED, b"", b"ok");
                }
            }
            sc::CMD_SC_DETACH => {
                if n < SID_EPOCH + 1 {
                    continue;
                }
                let conn = sid_conn(p);
                let reason = p[SID_EPOCH];
                // A standby told the session is gone frees its checkpoint.
                if let Some(i) = shadow_for(&mut s.worker, conn, false) {
                    if s.worker.shadows[i].ready_sid == p[..SID]
                        || s.worker.shadows[i].asm_sid == p[..SID]
                    {
                        s.worker.shadows[i] = Shadow::zero();
                    }
                }
                if s.worker.bindings[usize::from(conn)].session_id != p[..SID] {
                    continue;
                }
                detach(s, sys, conn, reason);
            }
            sc::CMD_SC_DRAIN => {
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
                        sc::MSG_SC_ERROR,
                        &sid,
                        epoch_at(p),
                        sc::STATUS_STALE_EPOCH,
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
                            sc::MSG_SC_ERROR,
                            &sid,
                            epoch,
                            sc::STATUS_NO_CAPACITY,
                        );
                    }
                }
            }
            sc::CMD_SC_EXPORT_BEGIN => {
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
                    sc::STATUS_NOT_READY
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
                            st = sc::STATUS_NO_CAPACITY;
                        }
                    } else {
                        s.worker.import_used[i] = 0;
                    }
                    st
                } else {
                    sc::STATUS_NO_CAPACITY
                };
                send_status(
                    &mut s.worker,
                    sys,
                    sc::MSG_SC_IMPORT_BEGIN,
                    &sid,
                    epoch,
                    status,
                );
            }
            sc::CMD_SC_EXPORT_CHUNK => {
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
                    send_status(&mut s.worker, sys, sc::MSG_SC_IMPORT_END, &sid, b.epoch, st);
                    import_free(&mut s.worker, conn);
                    s.worker.bindings[usize::from(conn)] = Attachment::zero();
                    busy_remove(&mut s.worker, conn);
                    s.worker.refused = s.worker.refused.wrapping_add(1);
                }
            }
            sc::CMD_SC_EXPORT_END => {
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
                    st = commit_import(s, sys, &blob[..len], conn, now, &mut slot, false);
                    if st == sc::STATUS_CORRUPT {
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
                        sc::MSG_SC_IMPORT_END,
                        &sid,
                        epoch,
                        sc::STATUS_OK,
                    );
                    mon(&mut s.worker, sys, conn, super::MON_EV_IMPORTED, b"", b"ok");
                } else {
                    send_status(&mut s.worker, sys, sc::MSG_SC_IMPORT_END, &sid, epoch, st);
                    let status_name: &[u8] = if st == sc::STATUS_STALE_EPOCH {
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
            sc::CMD_SC_RESUME => {
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
                        sc::MSG_SC_ERROR,
                        &sid,
                        new_epoch,
                        sc::STATUS_STALE_EPOCH,
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
                // An imported session under anchor continuity takes a seat
                // where the anchor's gate stands; with none free the
                // handoff is refused and the session stays where it was.
                let tail = if was_imported { seat_tail(p) } else { None };
                if let Some((next, bytes)) = tail {
                    if !s
                        .worker
                        .seats
                        .open_resumed(conn, &sid, new_epoch, next, bytes)
                    {
                        send_status(
                            &mut s.worker,
                            sys,
                            sc::MSG_SC_ERROR,
                            &sid,
                            new_epoch,
                            sc::STATUS_NO_CAPACITY,
                        );
                        continue;
                    }
                }
                {
                    let bm = &mut s.worker.bindings[usize::from(conn)];
                    bm.epoch = new_epoch;
                    bm.phase = Phase::Attached;
                    bm.quiet = 0;
                    bm.seam = seam;
                }
                busy_remove(&mut s.worker, conn);
                if was_imported {
                    after_resume(s, sys, conn, now);
                    s.worker.resumed = s.worker.resumed.wrapping_add(1);
                } else {
                    s.worker.returned = s.worker.returned.wrapping_add(1);
                }
                send_sid_epoch(&mut s.worker, sys, sc::MSG_SC_RESUMED, conn);
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
    adopting: bool,
) -> u8 {
    let Some((tenant, stream_hash, blob_gen, slot)) = sessions::record_identity(blob) else {
        return sc::STATUS_CORRUPT;
    };
    let si = slot as usize;
    if si >= MAX_SESSIONS {
        return sc::STATUS_CORRUPT;
    }
    let local = sessions::find_by_stream(&s.sessions, tenant, stream_hash)
        .map(|li| sessions::generation(&s.sessions, li));
    let (st, gen) = reconcile_generation(blob_gen, local);
    if st != SI_STATUS_OK {
        return sc::STATUS_STALE_EPOCH;
    }
    // The record's cursor against this worker's own apply position
    // (`session_replay_core`): an entry of the session this worker
    // skipped past the record's cursor is in neither, so the import is
    // refused; a record ahead of this worker is admitted with a floor so
    // what it already contains is not applied twice. A record whose tail
    // cannot be read states no cursor and is refused as corrupt.
    let Some(tail) = sessions::record_tail(blob).and_then(super::replay::HandoffTail::decode)
    else {
        return sc::STATUS_CORRUPT;
    };
    // An adoption does not check what this worker skipped: the anchor
    // replays the frames past the checkpoint, which re-derive those
    // entries (see `adopt`). It still takes a floor when this worker is
    // behind the checkpoint, so nothing in the record is applied twice.
    let last_skip = if adopting {
        None
    } else {
        super::replay_last_skip(s, si)
    };
    let floor = match super::replay::cursor_verdict(&tail, &s.apply_index, last_skip) {
        super::replay::CursorVerdict::Mismatch => {
            s.replay_cursor_refused = s.replay_cursor_refused.wrapping_add(1);
            dev_log(
                sys,
                1,
                b"[sess] import: cursor behind a skipped entry".as_ptr(),
                43,
            );
            return sc::STATUS_CURSOR_MISMATCH;
        }
        super::replay::CursorVerdict::AdmitWithFloor => {
            if !super::replay_floor_available(s, slot) {
                return sc::STATUS_NOT_READY;
            }
            true
        }
        super::replay::CursorVerdict::Admit => false,
    };
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
                return sc::STATUS_NO_CAPACITY;
            }
            sessions::clear_flow(&mut s.sessions, li);
            sessions::clear(&mut s.sessions, li);
            s.sub_book.clear(li as u32);
        }
    }
    sessions::unlend(&mut s.sessions, si);
    if !sessions::import_record(&mut s.sessions, si, blob, conn, gen, now) {
        return sc::STATUS_NO_CAPACITY;
    }
    // Its subscriptions as the worker it came from booked them: the
    // source of a site export of it from here.
    match sessions::record_book(blob) {
        Some(book) => {
            s.sub_book.import_section(si as u32, book);
        }
        None => {
            s.sub_book.clear(si as u32);
            s.sub_book.lose(si as u32);
        }
    }
    if floor {
        super::replay_install_floor(s, slot, &tail);
    }
    // Retried delivery ids keep being refused on this worker too.
    s.worker.bindings[usize::from(conn)].delivery_hw = tail.delivery_hw;
    super::replay_clear_skip(s, si);
    // The session is this worker's now: a checkpoint of it this worker
    // held as a standby is of a life it has taken over.
    if let Some(i) = shadow_for(&mut s.worker, conn, false) {
        s.worker.shadows[i] = Shadow::zero();
    }
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
    checkpoint_pass(s, sys, now);
    super::seat_step(s, sys);
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
                    let ch = ctrl_out_for(&s.worker, conn);
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

// ── Anchor continuity: productions and the re-anchor exchange ───────

/// Wrap the response envelope `inner` for `conn` as a production when its
/// session is seated. `Unseated`: emit `inner` as it is.
pub fn produce(w: &mut Worker, conn: u16, inner: &[u8], out: &mut [u8]) -> anchor_core::Produced {
    w.seats
        .produce(conn, wire::MSG_SESSION_RESPONSE, inner, out)
}

/// `MSG_ANCHOR_REANCHOR` from a replacement anchor: decided from the
/// seat and the binding's consumed delivery high-water
/// (`reanchor_verdict`); on OK the binding moves to the new epoch and
/// every production the seat retains is offered again under its
/// original id (`reoffer_productions`, continued by the step until the
/// channel has taken them all).
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn reanchor(s: &mut ModuleState, sys: &SyscallTable, p: &[u8], seam: u8) {
    let Some(req) = anchor_core::Reanchor::decode(p) else {
        return;
    };
    let conn = sid_conn(p);
    let b = s.worker.bindings[usize::from(conn)];
    let bound = b.session_id == req.session_id && b.phase == Phase::Attached;
    // A repeat of a request already answered OK finds the binding at its
    // epoch: answered again, and nothing here moves a second time.
    let repeat = bound && b.epoch == req.new_epoch;
    let hw = if bound { b.delivery_hw } else { 0 };
    let ans = if bound {
        s.worker.seats.reanchor(conn, &req, hw)
    } else {
        anchor_core::Reanchored {
            session_id: req.session_id,
            new_epoch: req.new_epoch,
            status: anchor_core::AC_UNKNOWN_SESSION,
            delivery_hw: hw,
            next_production: 1,
            released: 0,
            released_bytes: 0,
        }
    };
    let mut out = [0u8; anchor_core::REANCHORED_LEN];
    let n = ans.encode(&mut out);
    write_ctrl(&mut s.worker, sys, wire::MSG_ANCHOR_REANCHORED, &out[..n]);
    if ans.status != anchor_core::AC_OK {
        mon(
            &mut s.worker,
            sys,
            conn,
            super::MON_EV_ERROR,
            b"",
            b"reanchor_refused",
        );
        return;
    }
    if !repeat {
        // The replacement anchor counts the seam afresh from the answer it
        // applies: so does this side, so the next handoff's cursors
        // compare.
        let b = &mut s.worker.bindings[usize::from(conn)];
        b.epoch = req.new_epoch;
        b.in_consumed = 0;
        b.out_produced = 0;
    }
    // The session is served from the replacement's seam from here.
    s.worker.bindings[usize::from(conn)].seam = seam;
    mon(
        &mut s.worker,
        sys,
        conn,
        super::MON_EV_RESUMED,
        b"",
        b"reanchored",
    );
    super::reoffer_productions(s, sys, conn);
}

/// The connection a session id names (the anchor's minting).
pub fn sid_conn_of(sid: &[u8; SID]) -> u16 {
    u16::from_be_bytes([sid[8], sid[9]])
}

/// `(epoch, delivery_hw)` of the binding of `conn` when it is `sid`'s
/// and attached here.
pub fn bound(w: &Worker, conn: u16, sid: &[u8; SID]) -> Option<(u32, u64)> {
    let b = &w.bindings[usize::from(conn)];
    (b.session_id == *sid && matches!(b.phase, Phase::Attached | Phase::Draining))
        .then_some((b.epoch, b.delivery_hw))
}
