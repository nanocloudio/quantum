//! anchor — the SessionCtrlV1 **transport anchor** role
//! (docs/architecture/mqtt_anchor.md, docs/architecture/session_continuity.md).
//!
//! The client's connection is owned here, not by the session worker
//! behind it. This file is `quantum_mqtt_anchor`'s, and `protocol`
//! mounts the same source (`#[path]`), so the multi-protocol front door
//! and the MQTT anchor are one implementation. The per-connection state
//! and every decision that needs no syscall — the delivery envelope, the
//! worker admission rule, liveness-only mode, read-pause backpressure,
//! the framed hold — are `cores/mqtt_anchor_core.rs`, host-tested.
//!
//! Each classified connection is minted a Fluxor `session_id`
//! (`[anchor_id:8][conn_id:2][attach_gen:4][0:2]`,
//! `session_identity_core`) and attached to a session worker with
//! `CMD_SC_ATTACH` at class `edge_anchored`; every envelope the codecs
//! decode is forwarded to the worker the connection is bound to, and the
//! binding can be moved to the other worker while the connection stays
//! open.
//!
//! ## The seam
//!
//! Workers are addressed by slot: 0 on `proposals_out` / `responses_in`
//! / `ctrl_out` / `ctrl_in`, 1 on the `proposals2_out` / `responses2_in`
//! / `ctrl2_*` set. Every codec hands its decoded envelopes to
//! [`forward_env`] instead of writing the proposal edge itself. While a
//! worker is attached, every envelope crosses the seam wrapped in
//! `MSG_ANCHOR_DELIVERY`, which names the session, the epoch it was
//! decided under and its delivery id; with no worker attached (the plain
//! seam) the anchor is transparent.
//!
//! ## Delivery cursors
//!
//! Per connection, in envelope bytes as written: `forwarded` advances
//! when an envelope is accepted onto the bound worker's proposal channel,
//! `relayed` when a response envelope from that worker is accepted by
//! the codec onto the frame edge — never when it is merely read, since a
//! refused response is offered again. The worker counts the same unit;
//! `EXPORT_BEGIN` must match both or the handoff is refused.
//!
//! ## The swap
//!
//! A handoff moves every connection bound to the current default worker
//! to the other one, and new connections attach there from that moment.
//! It is started by an operator or control plane with `MSG_ANCHOR_SWAP`
//! (on `cmd_in`, or forwarded by a worker that applied a committed
//! `QASW` record) — the primary trigger — or, for the gates, every
//! `handoff_after_records` forwarded envelopes. Per connection: `DRAIN`
//! → the worker's `DRAINED` and export, relayed verbatim to the standby
//! after the cursors are admitted → the standby's `IMPORT_END` → `RESUME`
//! at epoch + 1 → `RESUMED`, at which the forwarding target flips, the
//! held ingress is released and the exporting worker is detached.
//! Cursors that disagree, an import the standby refuses, a worker error
//! or a window past `session_drain_ms` refuse the handoff: the standby is
//! detached and the exporting worker returned to service with `RESUME`
//! at the current epoch; the held ingress goes back to it on `RESUMED`.
//!
//! ## The hold and liveness
//!
//! Envelopes decoded for a connection whose worker is not authoritative
//! — attaching, mid-swap — are held in one bounded FIFO (`hold_bytes`)
//! with their delivery ids and released in order when it is. Reading
//! from the transport stops at `hold_high_water_pct` of the hold, so the
//! client's TCP window closes before the hold can fill; an overflow
//! still CLOSES the connection by default (`hold_overflow = 0`), because
//! a silently dropped publish is a QoS promise broken. A PINGREQ while
//! no worker is authoritative is answered only under `liveness_only`;
//! otherwise its PINGRESP is owed until one is. PUBLISH, SUBSCRIBE,
//! UNSUBSCRIBE and AUTH are never acknowledged here.
//!
//! ## Worker loss
//!
//! The bound worker checkpoints each session it serves through the
//! anchor to the standby (`MSG_ANCHOR_CKPT_*`, a QSR2 record kept there
//! as a non-live shadow), and the anchor retains every frame a loss would
//! have to replay (`replay_bytes`) until the standby acknowledges a
//! checkpoint that covers it. A worker that answers no liveness probe for
//! `worker_loss_ms` — or that an operator declares lost — loses its
//! sessions to the standby: each one with an acknowledged checkpoint and
//! no replay gap is adopted at the next epoch (`MSG_ANCHOR_ADOPT`), the
//! retained frames above the checkpoint are replayed to it under their
//! original delivery ids, and the connection never closes; any other is
//! closed so its client resumes from committed state. A lost worker is
//! fenced for good: nothing it emits is relayed.

use super::abi::contracts::net::session_ctrl as sc;
use super::wire;
use super::{dev_log, SyscallTable};

include!("../../common/cores/mqtt_anchor_core.rs");
include!("../../common/cores/anchor_continuity_core.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/cores/session_handoff.rs");
mod identity {
    include!("../../common/cores/session_identity_core.rs");
}
use identity::{mint_session_id, session_id_conn, FIRST_EPOCH};

const SID: usize = sc::SESSION_ID_BYTES;
const SID_EPOCH: usize = sc::SESSION_HEADER;

const CONN_ID_SPACE: usize = 1 << 16;
/// Largest hold buffer a graph may declare.
pub const HOLD_MAX: usize = 1 << 17;
/// Control frame buffer: an export chunk with its session header.
pub const CTRL_BUF: usize = 1100;
/// Largest wrapped envelope payload: one channel record less its header.
pub const WRAP_MAX: usize = 8192 - wire::ENVELOPE_HDR;
/// Largest replay window a graph may declare.
pub const REPLAY_MAX: usize = 1 << 17;

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
    /// Control-plane commands (`MSG_ANCHOR_SWAP`) from a module wired
    /// directly. Unwired, the commands arrive through the workers.
    pub cmd_in: i32,
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
    /// Route policy: 1 admits liveness-only mode — the anchor answers a
    /// PINGREQ itself while no worker is authoritative.
    pub liveness_only: u8,
    /// Hold fill, in percent, at which reading from the transport stops
    /// (0 = never; overflow is then the only limit).
    pub hold_high_water_pct: u8,
    /// Listener identity stamped on every connection this anchor
    /// accepts. The tenant is the listener's, and a listener carries
    /// none yet (docs/architecture/multi_tenancy.md), so every
    /// connection's tenant is `TENANT_UNRESOLVED`.
    pub listener_id: u16,
    /// Liveness probe period (0 = no probes) and the silence after which
    /// a worker is declared lost (0 = only an operator declares it).
    pub worker_probe_ms: u32,
    pub worker_loss_ms: u32,
    /// Bytes of frames retained for replay after a worker loss (0 = none:
    /// a loss then closes every connection of the lost worker).
    pub replay_bytes: u32,
    /// `continuity`: 1 runs every connection under anchor continuity
    /// (docs/architecture/anchor_continuity.md) — attached as
    /// `CC_TRANSPORT_MIGRATABLE`, its responses productions through a
    /// production gate, its `QAC1` record exported on `state_out`, and a
    /// connection the transport moved here imported from `state_in` and
    /// re-anchored with its worker. 0 is the edge-anchored anchor.
    pub continuity: u8,
    /// `QAC1` records out, imports in (unwired: none).
    pub state_out: i32,
    pub state_in: i32,
    gates: [GateSeat; GATE_SEATS],
    /// The paired anchor's records, held for a takeover of their
    /// connections.
    staged: [StagedImport; STAGED_IMPORTS],
    /// The clients connections were accepted from (`MSG_CLIENT_PEER`),
    /// most recent last, for the record each attachment exports.
    peers: [Option<(u16, Peer)>; PEERS_SEEN],
    peers_next: usize,
    /// Imported connections whose transport id is not the one their
    /// session id names: `(session id, conn)`.
    /// `(named, transport)`: an imported connection the transport
    /// presented under another id than its session id names.
    aliases: [Option<(u16, u16)>; GATE_SEATS],
    /// Workers declared lost: fenced for good.
    lost: [u8; 2],
    last_ack_ms: [u64; 2],
    probe_seq: u32,
    last_probe_ms: u64,
    replay: [u8; REPLAY_MAX],
    replay_len: usize,
    hello_sent: u8,
    self_idx: u8,
    attach_gen: u32,
    records_since_swap: u32,
    /// Connections still short of RESUMED in the current swap.
    swap_pending: u32,
    /// `MSG_ANCHOR_SWAP` sequences executed, per partition.
    swap_seqs: SwapSeqs,
    conns: [AnchorConn; CONN_ID_SPACE],
    hold: [u8; HOLD_MAX],
    hold_len: usize,
    ctrl_buf: [u8; CTRL_BUF],
    wrap_buf: [u8; WRAP_MAX],
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
    /// PINGREQs answered here, and ones whose PINGRESP was owed.
    pub pings_answered: u32,
    pub pings_deferred: u32,
    /// Responses from a worker the connection is not bound to: a stale
    /// emitter, fenced here rather than relayed.
    pub stale_responses: u32,
    /// Connections closed for client silence past the keep-alive budget
    /// while no worker was watching it.
    pub keepalive_closed: u32,
    /// Connections closed because an envelope would not fit the seam.
    pub oversize_closed: u32,
    /// Operator swap commands executed.
    pub swap_cmds: u32,
    /// Steps on which reading from the transport was paused.
    pub read_pauses: u32,
    /// Workers declared lost; sessions adopted by the standby, closed
    /// because they could not be adopted exactly, and frames replayed.
    pub losses: u32,
    pub adopted: u32,
    pub loss_closed: u32,
    pub replayed: u32,
    /// Checkpoints relayed to the standby and acknowledged by it, and
    /// frames that needed retaining for replay and could not be.
    pub ckpt_relayed: u32,
    pub ckpt_acked: u32,
    pub replay_gaps: u32,
    /// Continuity: productions given (inside the horizon), re-offers of
    /// an id already given, gaps refused, `QAC1` records exported,
    /// imports staged and adopted, re-anchors accepted and refused, and
    /// connections refused for want of a gate seat.
    pub prod_given: u32,
    /// Acknowledgements the transport took (`MSG_CLIENT_DELIVERED`).
    pub deliveries_acked: u32,
    /// Staged records dropped: evicted from a full stage, or named by a
    /// client whose new connection was not a resumed one.
    pub staged_dropped: u32,
    /// Resumed connections no staged record claimed: closed.
    pub resumed_unknown: u32,
    /// Attachments whose record could not be exported because the
    /// transport never reported their client.
    pub qac_unpeered: u32,
    /// Output horizon reports the transport sent (`MSG_CLIENT_HORIZON`).
    pub horizon_reports: u32,
    pub prod_dup: u32,
    pub prod_gap: u32,
    pub qac_exported: u32,
    pub imports_staged: u32,
    pub imports_adopted: u32,
    pub reanchored: u32,
    pub reanchor_refused: u32,
    pub gate_full: u32,
}

/// Connections one anchor runs under continuity at once.
pub const GATE_SEATS: usize = 16;
/// The paired anchor's records held here: one per connection it can run
/// under continuity.
pub const STAGED_IMPORTS: usize = GATE_SEATS;
/// Clients remembered between a connection's `MSG_CLIENT_PEER` and its
/// attachment.
pub const PEERS_SEEN: usize = 64;
/// While an imported connection waits for its worker's answer, the
/// request is sent again this often: a request or an answer the control
/// edge refused is not lost, and the worker answers a repeat as it
/// answered the first.
pub const REANCHOR_RETRY_MS: u64 = 250;
/// An imported connection whose worker has not answered after this is
/// closed: the client reconnects and resumes from committed state.
pub const REANCHOR_GIVE_UP_MS: u64 = 10_000;
/// One connection's production gate.
#[derive(Clone, Copy)]
struct GateSeat {
    used: bool,
    conn: u16,
    gate: ProductionGate,
    /// The transport's output horizon the gate reads.
    horizon: TransportHorizon,
    /// An imported connection waiting for its worker's
    /// `MSG_ANCHOR_REANCHORED`, since `reanchor_ms`; the request last went
    /// at `reanchor_sent_ms` (0: not yet taken by the control edge).
    reanchor: Option<AnchorReanchor>,
    reanchor_ms: u64,
    reanchor_sent_ms: u64,
    /// What of the client's bytes may be acknowledged to the transport.
    inbound: DeliveryCursor,
    /// The client, for the record this connection exports.
    peer: Option<Peer>,
}

impl GateSeat {
    const fn empty() -> Self {
        Self {
            used: false,
            conn: 0,
            gate: ProductionGate::new(),
            horizon: TransportHorizon::new(),
            reanchor: None,
            reanchor_ms: 0,
            reanchor_sent_ms: 0,
            inbound: DeliveryCursor::new(),
            peer: None,
        }
    }
}

/// A client as the transport reports it (`MSG_CLIENT_PEER`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    pub family: u8,
    pub addr: [u8; 16],
    pub port: u16,
}

/// The paired anchor's record of one of its connections, held until a
/// takeover moves that connection here, the record is replaced or
/// retired, or a full stage evicts it.
#[derive(Clone, Copy)]
struct StagedImport {
    used: bool,
    peer: Peer,
    rec: AnchorRecord,
    staged_ms: u64,
    /// The connection that presented this record's client, waiting for
    /// the transport's `MSG_CLIENT_RESUMED`.
    claimed: Option<u16>,
}

pub fn init(a: &mut Anchor) {
    a.ctrl_in = [-1; 2];
    a.ctrl_out = [-1; 2];
    a.prop_out = [-1; 2];
    a.resp_in = [-1; 2];
    a.out_frames = -1;
    a.dir_out = -1;
    a.dir_in = -1;
    a.cmd_in = -1;
    a.anchor_id = *b"QANCHOR0";
    a.handoff_after_records = 0;
    a.handoff_max_swaps = 0;
    a.session_drain_ms = 500;
    a.hold_bytes = 65536;
    a.hold_overflow = 0;
    a.default_worker = 0;
    a.liveness_only = 0;
    a.hold_high_water_pct = 75;
    a.listener_id = 0;
    a.worker_probe_ms = 0;
    a.worker_loss_ms = 0;
    a.replay_bytes = 0;
    a.lost = [0; 2];
    a.last_ack_ms = [0; 2];
    a.probe_seq = 0;
    a.last_probe_ms = 0;
    a.replay_len = 0;
    a.losses = 0;
    a.adopted = 0;
    a.loss_closed = 0;
    a.replayed = 0;
    a.ckpt_relayed = 0;
    a.ckpt_acked = 0;
    a.replay_gaps = 0;
    a.continuity = 0;
    a.state_out = -1;
    a.state_in = -1;
    a.gates = [GateSeat::empty(); GATE_SEATS];
    a.staged = [StagedImport {
        used: false,
        peer: Peer {
            family: 0,
            addr: [0; 16],
            port: 0,
        },
        rec: AnchorRecord::zero(),
        staged_ms: 0,
        claimed: None,
    }; STAGED_IMPORTS];
    a.peers = [None; PEERS_SEEN];
    a.peers_next = 0;
    a.deliveries_acked = 0;
    a.staged_dropped = 0;
    a.resumed_unknown = 0;
    a.qac_unpeered = 0;
    a.aliases = [None; GATE_SEATS];
    a.prod_given = 0;
    a.horizon_reports = 0;
    a.prod_dup = 0;
    a.prod_gap = 0;
    a.qac_exported = 0;
    a.imports_staged = 0;
    a.imports_adopted = 0;
    a.reanchored = 0;
    a.reanchor_refused = 0;
    a.gate_full = 0;
    a.hello_sent = 0;
    a.self_idx = 0xFF;
    a.attach_gen = 0;
    a.records_since_swap = 0;
    a.swap_pending = 0;
    a.swap_seqs = SwapSeqs::new();
    for c in a.conns.iter_mut() {
        *c = AnchorConn::zero();
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
    a.pings_answered = 0;
    a.pings_deferred = 0;
    a.stale_responses = 0;
    a.keepalive_closed = 0;
    a.oversize_closed = 0;
    a.swap_cmds = 0;
    a.read_pauses = 0;
}

// ── Directory mirror ─────────────────────────────────────────────────

fn worker_id_bytes(w: u8) -> [u8; 8] {
    let mut b = *b"QWORKER0";
    b[7] = b'0' + (w % 10);
    b
}

/// ATTACH `[sid:16][anchor_id:8][epoch:4][cc:1][worker_id:8]`.
unsafe fn dir_attach(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    if a.dir_out < 0 {
        return;
    }
    let c = a.conns[usize::from(conn)];
    let mut p = [0u8; sc::ATTACH_PAYLOAD_LEN];
    sc::put_attach(
        &mut p,
        &c.session_id,
        &a.anchor_id,
        c.epoch,
        attach_class(a),
        &worker_id_bytes(c.worker),
    );
    if wire::channel_write_msg(sys, a.dir_out, sc::CMD_SC_ATTACH, &p) <= 0 {
        a.dir_refused = a.dir_refused.wrapping_add(1);
    }
}

/// DETACH `[sid:16][epoch:4][reason:1]`.
unsafe fn dir_detach(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    if a.dir_out < 0 {
        return;
    }
    let c = a.conns[usize::from(conn)];
    let mut p = [0u8; sc::DETACH_PAYLOAD_LEN];
    sc::put_session_header(&mut p, &c.session_id, c.epoch);
    p[SID_EPOCH] = sc::DETACH_NORMAL;
    if wire::channel_write_msg(sys, a.dir_out, sc::CMD_SC_DETACH, &p) <= 0 {
        a.dir_refused = a.dir_refused.wrapping_add(1);
    }
}

/// Read the directory's verdicts. A stale refusal is counted and said:
/// it means the cluster holds a newer generation of the binding than
/// this anchor believes it owns. Grants and HELLO_ACK are the
/// transport's and the handshake's; neither is a verdict.
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
        let p = &buf[..(plen as usize).min(buf.len())];
        let status = match mt {
            sc::MSG_SC_ATTACHED | sc::MSG_SC_ERROR if p.len() > SID_EPOCH => sc::status(p),
            sc::MSG_SC_DETACHED if p.len() >= SID_EPOCH => sc::STATUS_OK,
            _ => continue,
        };
        if status == sc::STATUS_OK {
            a.dir_ok = a.dir_ok.wrapping_add(1);
        } else if status == sc::STATUS_STALE_EPOCH {
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

/// True when worker `w` can still be told anything: wired and not lost.
#[inline]
fn reachable(a: &Anchor, w: usize) -> bool {
    a.ctrl_out[w] >= 0 && a.lost[w] == 0
}

/// True when a loss of worker `w` could be adopted by the other one:
/// only then is anything worth retaining for replay.
#[inline]
fn adoptable_from(a: &Anchor, w: usize) -> bool {
    standby_wired(a, w ^ 1) && a.lost[w ^ 1] == 0
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
    let mut p = [0u8; sc::ATTACH_PAYLOAD_LEN];
    sc::put_attach(
        &mut p,
        &c.session_id,
        &a.anchor_id,
        c.epoch,
        attach_class(a),
        &[0u8; sc::WORKER_ID_BYTES],
    );
    write_ctrl(a, sys, w, sc::CMD_SC_ATTACH, &p)
}

unsafe fn send_detach(a: &Anchor, sys: &SyscallTable, w: usize, conn: u16, reason: u8) -> bool {
    let c = &a.conns[usize::from(conn)];
    let mut p = [0u8; sc::DETACH_PAYLOAD_LEN];
    sc::put_session_header(&mut p, &c.session_id, c.epoch);
    p[SID_EPOCH] = reason;
    write_ctrl(a, sys, w, sc::CMD_SC_DETACH, &p)
}

unsafe fn send_drain(a: &Anchor, sys: &SyscallTable, w: usize, conn: u16) -> bool {
    let c = &a.conns[usize::from(conn)];
    let mut p = [0u8; sc::DRAIN_PAYLOAD_LEN];
    sc::put_session_header(&mut p, &c.session_id, c.epoch);
    sc::put_u32_after_header(&mut p, a.session_drain_ms);
    write_ctrl(a, sys, w, sc::CMD_SC_DRAIN, &p)
}

/// The session header at `epoch`, then — for a connection under a
/// production gate — where the gate stands
/// (`[next_production:u64 LE][bytes_written:u64 LE]`): a worker the
/// session moves to numbers its responses on from there. Returns the
/// payload length.
fn session_payload(
    a: &Anchor,
    conn: u16,
    epoch: u32,
    p: &mut [u8; sc::SESSION_HEADER + 16],
) -> usize {
    let c = &a.conns[usize::from(conn)];
    sc::put_session_header(&mut p[..sc::SESSION_HEADER], &c.session_id, epoch);
    match gate_seat(a, conn) {
        Some(i) => {
            let at = sc::SESSION_HEADER;
            p[at..at + 8].copy_from_slice(&a.gates[i].gate.next_submit.to_le_bytes());
            p[at + 8..at + 16].copy_from_slice(&a.gates[i].horizon.written.to_le_bytes());
            sc::SESSION_HEADER + 16
        }
        None => sc::SESSION_HEADER,
    }
}

unsafe fn send_resume(a: &Anchor, sys: &SyscallTable, w: usize, conn: u16, epoch: u32) -> bool {
    let mut p = [0u8; sc::SESSION_HEADER + 16];
    let n = session_payload(a, conn, epoch, &mut p);
    write_ctrl(a, sys, w, sc::CMD_SC_RESUME, &p[..n])
}

unsafe fn send_close(a: &Anchor, sys: &SyscallTable, conn: u16) -> bool {
    if a.out_frames < 0 {
        return true;
    }
    let cb = conn.to_le_bytes();
    wire::channel_write_msg(sys, a.out_frames, wire::MSG_CONN_CLOSE_REQUEST, &cb) > 0
}

/// PINGRESP straight onto the frame edge as `MSG_CLIENT_FRAME`.
unsafe fn send_pingresp(a: &Anchor, sys: &SyscallTable, conn: u16) -> bool {
    if a.out_frames < 0 {
        return false;
    }
    let cb = conn.to_le_bytes();
    let f = [cb[0], cb[1], MQTT_PINGRESP << 4, 0];
    wire::channel_write_msg(sys, a.out_frames, wire::MSG_CLIENT_FRAME, &f) > 0
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

// ── The seam write ───────────────────────────────────────────────────

/// What became of one attempt to hand an envelope to a worker.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Wrote {
    Written,
    /// The channel refused it; offer it again later.
    Refused,
    /// It can never cross the seam (no channel, or too large).
    Never,
}

/// Write `payload` of type `mt` for `conn` to the worker it is bound to, wrapped in
/// `MSG_ANCHOR_DELIVERY` under the connection's current session, epoch
/// and the given delivery id, and advance the inbound cursor by what was
/// written.
unsafe fn write_seam(
    a: &mut Anchor,
    sys: &SyscallTable,
    conn: u16,
    mt: u8,
    id: u64,
    payload: &[u8],
    retain: bool,
) -> Wrote {
    let c = a.conns[usize::from(conn)];
    let ch = a.prop_out[usize::from(c.worker)];
    if ch < 0 {
        return Wrote::Never;
    }
    let n = encode_delivery(&mut a.wrap_buf, mt, payload, &c.session_id, c.epoch, id);
    if n == 0 {
        return Wrote::Never;
    }
    // The worker knows the connection by the id its session id names; an
    // imported connection the transport presented under another id is
    // addressed by that one.
    let named = session_id_conn(&c.session_id);
    a.wrap_buf[..2].copy_from_slice(&named.to_le_bytes());
    if wire::channel_write_msg(sys, ch, wire::MSG_ANCHOR_DELIVERY, &a.wrap_buf[..n]) <= 0 {
        return Wrote::Refused;
    }
    let cm = &mut a.conns[usize::from(conn)];
    cm.forwarded = cm.forwarded.wrapping_add((wire::ENVELOPE_HDR + n) as u64);
    a.records_since_swap = a.records_since_swap.wrapping_add(1);
    // Retained until a checkpoint the standby holds covers it: a loss of
    // this worker replays it to the replacement under the same id. With
    // no standby to adopt from there is nothing to replay to.
    if retain
        && a.replay_bytes != 0
        && adoptable_from(a, usize::from(c.worker))
        && needs_replay(payload)
    {
        let cap = (a.replay_bytes as usize).min(REPLAY_MAX);
        if !fifo_push(&mut a.replay, &mut a.replay_len, cap, conn, mt, id, payload) {
            a.conns[usize::from(conn)].note_replay_gap(id);
            a.replay_gaps = a.replay_gaps.wrapping_add(1);
        }
    }
    Wrote::Written
}

// ── Hold buffer ──────────────────────────────────────────────────────

unsafe fn hold_push(
    a: &mut Anchor,
    sys: &SyscallTable,
    conn: u16,
    mt: u8,
    id: u64,
    payload: &[u8],
) -> bool {
    let cap = (a.hold_bytes as usize).min(HOLD_MAX);
    if !fifo_push(&mut a.hold, &mut a.hold_len, cap, conn, mt, id, payload) {
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
    a.conns[usize::from(conn)].commit_delivery();
    a.held_records = a.held_records.wrapping_add(1);
    true
}

/// Release held envelopes whose connection is live again, in order, each
/// under the delivery id it was held with. An envelope the worker's
/// channel refuses stays, with everything after it, for the next step.
unsafe fn flush_hold(a: &mut Anchor, sys: &SyscallTable) {
    if a.hold_len == 0 {
        return;
    }
    let total = a.hold_len;
    let mut at = 0usize;
    let mut stalled = false;
    let mut any_released = false;
    while let Some(e) = fifo_entry(&a.hold, total, at) {
        let c = a.conns[usize::from(e.conn)];
        let release = if !stalled && c.phase == Phase::Active && c.replay_from == 0 {
            // Staged out of the hold so the write can borrow the anchor.
            let mut payload = [0u8; WRAP_MAX];
            let plen = e.payload_len.min(WRAP_MAX);
            payload[..plen].copy_from_slice(&a.hold[e.payload_at..e.payload_at + plen]);
            match write_seam(
                a,
                sys,
                e.conn,
                e.msg_type,
                e.delivery_id,
                &payload[..plen],
                true,
            ) {
                Wrote::Written | Wrote::Never => true,
                Wrote::Refused => {
                    // Everything after it stays too: releasing a later
                    // frame first would reorder the session.
                    stalled = true;
                    false
                }
            }
        } else {
            matches!(c.phase, Phase::Idle | Phase::Closing)
        };
        if release {
            // Delivery id 0 is never issued (`commit_delivery`), so it
            // is an unambiguous released mark for the compaction below.
            a.hold[e.payload_at - 8..e.payload_at].copy_from_slice(&0u64.to_le_bytes());
            any_released = true;
        }
        at = e.next;
    }
    if any_released {
        fifo_remove(&mut a.hold, &mut a.hold_len, |_, id| id == 0);
    }
}

/// Drop every held envelope of one connection.
fn hold_drop_conn(a: &mut Anchor, conn: u16) {
    fifo_remove(&mut a.hold, &mut a.hold_len, |c, _| c == conn);
}

/// True when the codecs should take no raw bytes this step: the hold is
/// at its high-water mark. Not reading is the stream contract's only
/// backpressure, and it closes the client's TCP window before the hold
/// can overflow.
pub fn read_paused(a: &mut Anchor) -> bool {
    let cap = (a.hold_bytes as usize).min(HOLD_MAX);
    let paused = read_pause(a.hold_len, cap, a.hold_high_water_pct);
    if paused {
        a.read_pauses = a.read_pauses.wrapping_add(1);
    }
    paused
}

// ── Data plane ───────────────────────────────────────────────────────

/// Forward one decoded envelope `[conn_id:u16 LE][...]` to the worker
/// the connection is bound to, attaching first if it is new and holding
/// it while the worker is not authoritative. `false` only when the live
/// worker's channel refused it — the codec keeps the packet and offers
/// it again, and it keeps its delivery id.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn forward_env(
    a: &mut Anchor,
    sys: &SyscallTable,
    now: u64,
    mt: u8,
    payload: &[u8],
) -> bool {
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
    if payload.len() + DELIVERY_INSERT > WRAP_MAX {
        // Bounded outcome for a frame the seam cannot carry: the codec
        // already refuses packets past its own maximum, so this is a
        // frame the worker could not have read either.
        a.oversize_closed = a.oversize_closed.wrapping_add(1);
        dev_log(sys, 1, b"[anchor] envelope too large: closing".as_ptr(), 36);
        close_conn(a, sys, conn);
        return true;
    }
    a.conns[usize::from(conn)].last_in_ms = now;
    let phase = a.conns[usize::from(conn)].phase;
    let id = a.conns[usize::from(conn)].peek_delivery();
    match phase {
        Phase::Idle => {
            if a.aliases.iter().flatten().any(|&(named, _)| named == conn) {
                // The worker knows a session imported here by this id: a
                // new session on it would replace that binding. The
                // client reconnects on another id.
                a.gate_full = a.gate_full.wrapping_add(1);
                close_conn(a, sys, conn);
                return true;
            }
            attach(a, sys, conn, now);
            hold_push(a, sys, conn, mt, id, payload)
        }
        Phase::Active => {
            // Anything still held, or still being replayed, goes first.
            if fifo_count(&a.hold, a.hold_len, conn) != 0
                || a.conns[usize::from(conn)].replay_from != 0
            {
                return hold_push(a, sys, conn, mt, id, payload);
            }
            match write_seam(a, sys, conn, mt, id, payload, true) {
                Wrote::Written => {
                    a.conns[usize::from(conn)].commit_delivery();
                    true
                }
                Wrote::Refused => false,
                Wrote::Never => true,
            }
        }
        Phase::Closing => true,
        _ => hold_push(a, sys, conn, mt, id, payload),
    }
}

/// A response envelope from worker `w` for `conn` was accepted onto the
/// frame edge: the inbound cursor advances, and so does the production
/// id the response was relayed under.
pub fn note_relayed(a: &mut Anchor, w: usize, conn: u16, envelope_bytes: usize, now: u64) {
    let c = &mut a.conns[usize::from(conn)];
    if c.phase != Phase::Idle && (usize::from(c.worker) == w || taking_over(c, w)) {
        c.relayed = c.relayed.wrapping_add(envelope_bytes as u64);
        c.accept_production(now);
    }
}

/// Whether worker `w` is the one a session is moving to (resumed after a
/// handoff, or adopting it after a loss). It answers RESUMED on control
/// and responds on its data channel at once, and the anchor reads
/// responses first, so what it sends can arrive before the RESUMED that
/// binds it.
fn taking_over(c: &AnchorConn, w: usize) -> bool {
    matches!(c.phase, Phase::ResumeWait | Phase::AdoptWait) && usize::from(c.worker) != w
}

/// Whether a response worker `w` produced for `conn` may reach the
/// client. Only the worker the connection is bound to may emit for it:
/// after a rebind the other one is a stale emitter, and what it emits is
/// dropped here and counted rather than relayed.
pub fn response_admitted(a: &mut Anchor, w: usize, conn: u16) -> bool {
    if !enabled(a) {
        return true;
    }
    let c = &a.conns[usize::from(conn)];
    if a.lost[w] == 0 && (c.phase == Phase::Idle || usize::from(c.worker) == w || taking_over(c, w))
    {
        return true;
    }
    a.stale_responses = a.stale_responses.wrapping_add(1);
    false
}

/// The codec decoded the client's CONNECT: record what the anchor owns
/// of it — version, keep-alive, the client-id hash.
pub fn note_connect(a: &mut Anchor, conn: u16, version: u8, keepalive_s: u16, client_id: &[u8]) {
    let listener = a.listener_id;
    let c = &mut a.conns[usize::from(conn)];
    c.on_connect(version, keepalive_s, client_id);
    c.listener = listener;
}

/// The codec decoded a syntactically valid PINGREQ. Returns `true` when
/// the codec should answer PINGRESP now. With a worker authoritative the
/// ping is also forwarded as a liveness mark so its keep-alive sweep
/// sees the client; with none, the answer depends on the route's
/// liveness-only policy and an unanswered ping is owed.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn pingreq(a: &mut Anchor, sys: &SyscallTable, now: u64, conn: u16) -> bool {
    let mark = {
        let cb = conn.to_le_bytes();
        [cb[0], cb[1], ENV_PROTO_MQTT, MQTT_PINGREQ, 0]
    };
    if !enabled(a) {
        // The plain seam: the worker is always there to be told.
        let ch = a.prop_out[0];
        if ch >= 0 {
            wire::channel_write_msg(sys, ch, wire::MSG_SESSION_LIVENESS, &mark);
        }
        a.pings_answered = a.pings_answered.wrapping_add(1);
        return true;
    }
    let c = a.conns[usize::from(conn)];
    if c.phase == Phase::Closing {
        // The anchor is closing this connection; nothing is answered.
        return false;
    }
    a.conns[usize::from(conn)].last_in_ms = now;
    if a.continuity != 0 && gate_seat(a, conn).is_some() {
        // Under continuity every byte after the attachment is a
        // production, so the worker answers the PINGREQ (its PINGRESP is
        // a production like any response): the anchor forwards the mark.
        // The mark takes its delivery id here, in decode order like every
        // packet, so an anchor the transport hands these bytes again
        // after a takeover numbers them as this one did: written now, or
        // held behind whatever is held until the worker is authoritative.
        let id = c.peek_delivery();
        if ping_action(&c, false) == PingAction::AnswerAndMark && ping_mark(a, sys, conn) {
            a.pings_answered = a.pings_answered.wrapping_add(1);
        } else {
            hold_push(a, sys, conn, wire::MSG_SESSION_LIVENESS, id, &mark);
            a.pings_deferred = a.pings_deferred.wrapping_add(1);
        }
        return false;
    }
    match ping_action(&c, a.liveness_only != 0) {
        PingAction::AnswerAndMark => {
            // Best effort: a refused mark costs the worker one refresh,
            // and a mark is not work, so it is never held.
            if fifo_count(&a.hold, a.hold_len, conn) == 0 && c.replay_from == 0 {
                let id = c.peek_delivery();
                if write_seam(a, sys, conn, wire::MSG_SESSION_LIVENESS, id, &mark, true)
                    == Wrote::Written
                {
                    a.conns[usize::from(conn)].commit_delivery();
                }
            }
            a.pings_answered = a.pings_answered.wrapping_add(1);
            true
        }
        PingAction::Answer => {
            a.conns[usize::from(conn)].set(FLAG_LIVENESS_ONLY, true);
            a.pings_answered = a.pings_answered.wrapping_add(1);
            true
        }
        PingAction::Defer => {
            a.conns[usize::from(conn)].set(FLAG_PING_OWED, true);
            a.pings_deferred = a.pings_deferred.wrapping_add(1);
            false
        }
    }
}

/// Forward a PINGREQ's liveness mark to `conn`'s worker. `true` when it
/// went (nothing held ahead of it).
unsafe fn ping_mark(a: &mut Anchor, sys: &SyscallTable, conn: u16) -> bool {
    let mark = {
        let cb = conn.to_le_bytes();
        [cb[0], cb[1], ENV_PROTO_MQTT, MQTT_PINGREQ, 0]
    };
    let c = a.conns[usize::from(conn)];
    if fifo_count(&a.hold, a.hold_len, conn) != 0 || c.replay_from != 0 {
        return false;
    }
    let id = c.peek_delivery();
    if write_seam(a, sys, conn, wire::MSG_SESSION_LIVENESS, id, &mark, true) == Wrote::Written {
        a.conns[usize::from(conn)].commit_delivery();
        return true;
    }
    false
}

/// A worker became authoritative for `conn` again: leave liveness-only
/// mode and pay any PINGRESP that was owed. Under continuity none is:
/// the PINGREQ's mark waits in the hold for the worker to answer.
unsafe fn became_live(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    let c = &mut a.conns[usize::from(conn)];
    c.set(FLAG_LIVENESS_ONLY, false);
    c.set(FLAG_DRAINING, false);
    if c.has(FLAG_PING_OWED) && send_pingresp(a, sys, conn) {
        a.conns[usize::from(conn)].set(FLAG_PING_OWED, false);
    }
}

/// Mint the attachment and ask the default worker to take it.
unsafe fn attach(a: &mut Anchor, sys: &SyscallTable, conn: u16, now: u64) {
    let w = usize::from(a.default_worker);
    a.attach_gen = a.attach_gen.wrapping_add(1);
    let listener = a.listener_id;
    let c = &mut a.conns[usize::from(conn)];
    // What the codec recorded from the CONNECT that opened the
    // attachment survives the reset (`note_connect` runs first).
    let (version, keepalive_s, client_hash) = (c.mqtt_version, c.keepalive_s, c.client_hash);
    let admitted = c.has(FLAG_CONNECT_ADMITTED);
    *c = AnchorConn::zero();
    c.mqtt_version = version;
    c.keepalive_s = keepalive_s;
    c.client_hash = client_hash;
    c.set(FLAG_CONNECT_ADMITTED, admitted);
    c.session_id = mint_session_id(&a.anchor_id, conn, a.attach_gen);
    c.epoch = FIRST_EPOCH;
    c.worker = w as u8;
    c.phase = Phase::WaitAttached;
    c.listener = listener;
    c.last_in_ms = now;
    if send_attach(a, sys, w, conn) {
        mon(a, sys, conn, super::MON_EV_ATTACH_REQ, b"", b"");
    } else {
        // ctrl edge full: the step loop retries from WaitAttached.
        a.conns[usize::from(conn)].resume_pending = 1;
    }
}

/// End the attachment of a connection whose client is going away: drop
/// what is held or retained for it, and detach it from every worker that
/// may hold any of it — the bound worker, and the other one, which may
/// be mid-import or holding a checkpoint of it.
///
/// A bound worker that is live is told in band: a `MSG_CONN_CLOSED`
/// delivery on its data channel, behind the frames the client sent last.
/// A `DETACH` on the control channel would race them — the worker reads
/// control first in a step, and a DISCONNECT still unread would then
/// find its binding gone. The worker ends the binding on the in-band
/// notice exactly as on `DETACH` (`DETACH_CLIENT_GONE`). Any other case
/// has nothing in flight on the data channel and uses `DETACH`.
unsafe fn release_attachment(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    let c = a.conns[usize::from(conn)];
    let w = usize::from(c.worker);
    let in_band = c.phase == Phase::Active
        && c.replay_from == 0
        && reachable(a, w)
        && fifo_count(&a.hold, a.hold_len, conn) == 0
        && write_seam(
            a,
            sys,
            conn,
            wire::MSG_CONN_CLOSED,
            c.peek_delivery(),
            &conn.to_le_bytes(),
            false,
        ) == Wrote::Written;
    hold_drop_conn(a, conn);
    fifo_remove(&mut a.replay, &mut a.replay_len, |c, _| c == conn);
    dir_detach(a, sys, conn);
    if matches!(
        c.phase,
        Phase::DrainWait | Phase::ImportWait | Phase::ResumeWait
    ) && a.swap_pending > 0
    {
        a.swap_pending -= 1;
    }
    if reachable(a, w ^ 1) {
        send_detach(a, sys, w ^ 1, conn, sc::DETACH_CLIENT_GONE);
    }
    if !in_band && reachable(a, w) {
        send_detach(a, sys, w, conn, sc::DETACH_CLIENT_GONE);
    }
    mon(a, sys, conn, super::MON_EV_DETACH_REQ, b"client_gone", b"");
}

/// The transport reported the connection closed. The slot returns to
/// `Idle` at once: the transport sends nothing more for this connection,
/// and the next client it assigns the id to starts a fresh attachment.
/// A connection this anchor was already closing was released then.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn conn_closed(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    match a.conns[usize::from(conn)].phase {
        Phase::Idle => return,
        Phase::Closing => {}
        _ => release_attachment(a, sys, conn),
    }
    a.conns[usize::from(conn)] = AnchorConn::zero();
}

/// Close a connection from this side: release the attachment and ask the
/// transport to close the socket. The slot stays `Closing`, dropping any
/// frame still decoded for it, until the transport's close notice
/// arrives (`conn_closed`); a close request the frame edge refuses is
/// retried each step.
/// The codec could not take an imported connection: closed, and the
/// client reconnects.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn refuse_import(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    a.reanchor_refused = a.reanchor_refused.wrapping_add(1);
    close_conn(a, sys, conn);
}

unsafe fn close_conn(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    let phase = a.conns[usize::from(conn)].phase;
    if phase == Phase::Closing {
        return;
    }
    if phase != Phase::Idle {
        release_attachment(a, sys, conn);
    }
    let sent = send_close(a, sys, conn);
    let cm = &mut a.conns[usize::from(conn)];
    cm.phase = Phase::Closing;
    cm.detach_old = 0;
    cm.resume_pending = u8::from(!sent);
}

// ── The swap ─────────────────────────────────────────────────────────

/// Refuse the handoff in flight for `conn`: the standby discards its
/// half-import and the exporting worker is returned to service at the
/// session's current epoch. The hold is released on its RESUMED.
unsafe fn refuse(a: &mut Anchor, sys: &SyscallTable, conn: u16, status: &[u8]) {
    let c = a.conns[usize::from(conn)];
    let w = usize::from(c.worker);
    if reachable(a, w ^ 1) {
        send_detach(a, sys, w ^ 1, conn, sc::DETACH_NORMAL);
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

/// Move every connection bound to the default worker onto the other
/// one, and bind new connections there from now on. Returns how many
/// connections the swap started for; `None` when no swap could start
/// (one in flight, or no standby wired).
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn start_swap(a: &mut Anchor, sys: &SyscallTable, now: u64) -> Option<u32> {
    let from = usize::from(a.default_worker);
    let to = 1 - from;
    if a.swap_pending != 0 || !standby_wired(a, to) {
        return None;
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
            cm.set(FLAG_DRAINING, true);
            started += 1;
            mon(a, sys, i as u16, super::MON_EV_EXPORT_REQ, b"", b"");
        }
    }
    a.swap_pending = started;
    dev_log(sys, 3, b"[anchor] swap started".as_ptr(), 21);
    Some(started)
}

/// Execute an operator `MSG_ANCHOR_SWAP` (`[anchor_id:8][mode][seq]`).
///
/// A command acts on the sessions bound to the default worker when it
/// executes; with none bound it changes nothing — not even which worker
/// new connections attach to, and not whether a worker is lost. Every
/// committed `QASW` record is applied again when a node replays its log
/// at boot, before any client is attached, and must be inert then.
unsafe fn swap_command(a: &mut Anchor, sys: &SyscallTable, p: &[u8], now: u64) {
    let Some(cmd) = decode_swap(p) else {
        return;
    };
    if !a.swap_seqs.admit(&cmd, &a.anchor_id) {
        return;
    }
    if cmd.mode != SWAP_PLANNED && cmd.mode != SWAP_WORKER_LOST {
        dev_log(sys, 2, b"[anchor] swap: unsupported mode".as_ptr(), 31);
        return;
    }
    let from = a.default_worker;
    let bound = a
        .conns
        .iter()
        .any(|c| c.phase == Phase::Active && c.worker == from && c.detach_old == 0);
    if !bound {
        dev_log(sys, 3, b"[anchor] swap: nothing bound".as_ptr(), 28);
        return;
    }
    if cmd.mode == SWAP_WORKER_LOST {
        // The operator says the default worker is gone: fail over now
        // rather than wait out `worker_loss_ms`.
        let w = usize::from(from);
        if a.lost[w] == 0 {
            a.swap_cmds = a.swap_cmds.wrapping_add(1);
            declare_lost(a, sys, w, now);
        }
        return;
    }
    if start_swap(a, sys, now).is_some() {
        a.swap_cmds = a.swap_cmds.wrapping_add(1);
        dev_log(sys, 3, b"[anchor] swap: operator".as_ptr(), 23);
    }
}

// ── Worker loss ──────────────────────────────────────────────────────

/// `MSG_ANCHOR_ADOPT [sid:16][new_epoch:4]` to worker `w`.
unsafe fn send_adopt(a: &Anchor, sys: &SyscallTable, w: usize, conn: u16, new_epoch: u32) -> bool {
    let mut p = [0u8; sc::SESSION_HEADER + 16];
    let n = session_payload(a, conn, new_epoch, &mut p);
    write_ctrl(a, sys, w, wire::MSG_ANCHOR_ADOPT, &p[..n])
}

/// Declare worker `w` lost: it is fenced for good, new connections go to
/// the other worker, and every session bound to it is adopted by the
/// standby from its checkpoint or, when that cannot be exact, closed.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn declare_lost(a: &mut Anchor, sys: &SyscallTable, w: usize, now: u64) {
    if a.lost[w] != 0 {
        return;
    }
    a.lost[w] = 1;
    a.losses = a.losses.wrapping_add(1);
    let other = w ^ 1;
    let standby_ok = standby_wired(a, other) && a.lost[other] == 0;
    if usize::from(a.default_worker) == w && standby_ok {
        a.default_worker = other as u8;
    }
    dev_log(sys, 1, b"[anchor] worker lost".as_ptr(), 20);
    for i in 0..CONN_ID_SPACE {
        let c = a.conns[i];
        match loss_action(&c, w as u8, standby_ok) {
            LossAction::Unaffected => {}
            LossAction::RefuseHandoff => refuse(a, sys, i as u16, b"worker_lost"),
            LossAction::Close => {
                a.loss_closed = a.loss_closed.wrapping_add(1);
                mon(a, sys, i as u16, super::MON_EV_ERROR, b"", b"worker_lost");
                close_conn(a, sys, i as u16);
            }
            LossAction::Adopt => {
                // Everything the checkpoint covers is in the standby's
                // shadow; only what came after it is replayed.
                let hw = c.checkpoint_delivery;
                let conn = i as u16;
                fifo_remove(&mut a.replay, &mut a.replay_len, |k, id| {
                    k == conn && id <= hw
                });
                if send_adopt(a, sys, other, conn, c.epoch + 1) {
                    let cm = &mut a.conns[i];
                    cm.phase = Phase::AdoptWait;
                    cm.started_ms = now;
                    // Both sides count afresh from the adoption: the dead
                    // worker's share of the old counts is not the
                    // replacement's to match. Reset here, not at RESUMED,
                    // because the adopting worker's first responses can
                    // arrive before its RESUMED.
                    cm.forwarded = 0;
                    cm.relayed = 0;
                    mon(a, sys, conn, super::MON_EV_RESUME_REQ, b"", b"adopt");
                } else {
                    a.loss_closed = a.loss_closed.wrapping_add(1);
                    close_conn(a, sys, conn);
                }
            }
        }
    }
}

/// The standby resumed an adopted session at `c.epoch + 1`: flip the
/// binding and start replaying the retained frames above the checkpoint.
unsafe fn adopted(a: &mut Anchor, sys: &SyscallTable, w: usize, conn: u16) {
    {
        let cm = &mut a.conns[usize::from(conn)];
        cm.epoch += 1;
        cm.worker = w as u8;
        cm.phase = Phase::Active;
        cm.detach_old = 0;
        cm.replay_from = cm.checkpoint_delivery.wrapping_add(1);
        // The standby's shadow was consumed; the replacement checkpoints
        // anew to the worker it replaced only if that one returns, which
        // a lost worker does not.
        cm.checkpoint_reset();
    }
    dir_attach(a, sys, conn);
    a.adopted = a.adopted.wrapping_add(1);
    mon(a, sys, conn, super::MON_EV_RELOCATED, b"", b"adopted");
    rebound(a, sys, conn);
    became_live(a, sys, conn);
    pump_replay(a, sys, conn);
}

/// `conn`'s session moved to another worker at a new epoch: its record is
/// exported again, and nothing more is acknowledged to the transport
/// until the new worker reports what it consumed.
unsafe fn rebound(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    if let Some(i) = gate_seat(a, conn) {
        a.gates[i].inbound.rebound();
        export_qac(a, sys, conn);
    }
}

/// Replay the retained frames of `conn` from `replay_from` on, in order,
/// to its (new) worker; stop at the first refusal and resume next step.
/// When nothing is left, frames held behind the replay are released.
unsafe fn pump_replay(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    let from = a.conns[usize::from(conn)].replay_from;
    if from == 0 {
        return;
    }
    let mut at = 0usize;
    let mut next = from;
    let mut done = true;
    while let Some(e) = fifo_entry(&a.replay, a.replay_len, at) {
        at = e.next;
        if e.conn != conn || e.delivery_id < next {
            continue;
        }
        let mut payload = [0u8; WRAP_MAX];
        let plen = e.payload_len.min(WRAP_MAX);
        payload[..plen].copy_from_slice(&a.replay[e.payload_at..e.payload_at + plen]);
        match write_seam(
            a,
            sys,
            conn,
            e.msg_type,
            e.delivery_id,
            &payload[..plen],
            false,
        ) {
            Wrote::Written | Wrote::Never => {
                next = e.delivery_id + 1;
                a.replayed = a.replayed.wrapping_add(1);
            }
            Wrote::Refused => {
                done = false;
                break;
            }
        }
    }
    let cm = &mut a.conns[usize::from(conn)];
    cm.replay_from = if done { 0 } else { next };
    if done {
        flush_hold(a, sys);
    }
}

/// Drain `cmd_in`: control-plane commands wired straight to the anchor.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn handle_cmd(a: &mut Anchor, sys: &SyscallTable, now: u64) -> u32 {
    if a.cmd_in < 0 {
        return 0;
    }
    let mut worked = 0u32;
    for _ in 0..8 {
        let poll = (sys.channel_poll)(a.cmd_in, 0x01);
        if poll <= 0 || (poll as u32 & 0x01) == 0 {
            break;
        }
        let mut buf = [0u8; 64];
        let (mt, plen) = wire::channel_read_msg(sys, a.cmd_in, &mut buf);
        worked += 1;
        let n = (plen as usize).min(buf.len());
        if mt == wire::MSG_ANCHOR_SWAP {
            swap_command(a, sys, &buf[..n], now);
        } else if mt == wire::MSG_ANCHOR_QAC {
            qac_request(a, sys, &buf[..n]);
        }
    }
    worked
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
        if n > CTRL_BUF {
            continue;
        }
        let frame = a.ctrl_buf;
        let p = &frame[..n];
        if mt == wire::MSG_ANCHOR_SWAP {
            // A worker forwarding a committed swap record.
            swap_command(a, sys, p, now);
            continue;
        }
        if mt == wire::MSG_ANCHOR_PROBE_ACK {
            a.last_ack_ms[w] = now;
            continue;
        }
        if a.lost[w] != 0 {
            // A lost worker is fenced for good: whatever it still says is
            // about bindings it no longer holds.
            continue;
        }
        if n < SID {
            continue;
        }
        if matches!(
            mt,
            wire::MSG_ANCHOR_CKPT_BEGIN | wire::MSG_ANCHOR_CKPT_CHUNK | wire::MSG_ANCHOR_CKPT_END
        ) {
            // A checkpoint from the bound worker, relayed to the standby.
            let conn = conn_of_sid(a, &sid_of(p));
            let c = a.conns[usize::from(conn)];
            let other = w ^ 1;
            if c.phase == Phase::Active
                && usize::from(c.worker) == w
                && c.session_id == p[..SID]
                && standby_wired(a, other)
                && a.lost[other] == 0
            {
                if mt == wire::MSG_ANCHOR_CKPT_BEGIN {
                    a.ckpt_relayed = a.ckpt_relayed.wrapping_add(1);
                }
                relay(a, sys, other, mt, n);
            }
            continue;
        }
        if mt == wire::MSG_ANCHOR_CKPT_ACK {
            // The standby kept a checkpoint: replay need not reach behind it.
            if n >= wire::ANCHOR_CKPT_ACK_LEN {
                let conn = conn_of_sid(a, &sid_of(p));
                let c = a.conns[usize::from(conn)];
                let mut hw = [0u8; 8];
                hw.copy_from_slice(&p[SID_EPOCH + 1..SID_EPOCH + 9]);
                let hw = u64::from_le_bytes(hw);
                if c.phase == Phase::Active
                    && usize::from(c.worker) != w
                    && c.session_id == p[..SID]
                    && sc::epoch(p) == c.epoch
                    && sc::status(p) == sc::STATUS_OK
                {
                    a.conns[usize::from(conn)].checkpoint_acked(hw);
                    let hw = a.conns[usize::from(conn)].checkpoint_delivery;
                    fifo_remove(&mut a.replay, &mut a.replay_len, |k, id| {
                        k == conn && id <= hw
                    });
                    a.ckpt_acked = a.ckpt_acked.wrapping_add(1);
                }
            }
            continue;
        }
        if mt == wire::MSG_ANCHOR_REANCHORED {
            reanchored(a, sys, w, p);
            continue;
        }
        if mt == wire::MSG_ANCHOR_CONSUMED {
            if n == wire::ANCHOR_CONSUMED_LEN {
                consumed(a, w, p);
            }
            continue;
        }
        let conn = if mt == sc::MSG_SC_HELLO_ACK {
            0
        } else {
            conn_of_sid(a, &sid_of(p))
        };
        let c = a.conns[usize::from(conn)];
        let is_bound = usize::from(c.worker) == w;
        if mt != sc::MSG_SC_HELLO_ACK && (c.phase == Phase::Idle || c.session_id != p[..SID]) {
            // A reply for an attachment that no longer exists.
            continue;
        }
        match mt {
            sc::MSG_SC_HELLO_ACK => {
                dev_log(sys, 3, b"[anchor] worker hello".as_ptr(), 21);
            }
            sc::MSG_SC_ATTACHED => {
                if c.phase == Phase::WaitAttached && is_bound && n > SID_EPOCH {
                    if sc::status(p) == sc::STATUS_OK {
                        if a.continuity != 0 && !gate_open(a, conn) {
                            a.gate_full = a.gate_full.wrapping_add(1);
                            mon(a, sys, conn, super::MON_EV_ATTACH_FAILED, b"", b"no_gate");
                            close_conn(a, sys, conn);
                            continue;
                        }
                        a.conns[usize::from(conn)].phase = Phase::Active;
                        a.attached = a.attached.wrapping_add(1);
                        export_qac(a, sys, conn);
                        mon(a, sys, conn, super::MON_EV_ATTACHED, b"", b"ok");
                        dir_attach(a, sys, conn);
                        became_live(a, sys, conn);
                        flush_hold(a, sys);
                    } else {
                        mon(a, sys, conn, super::MON_EV_ATTACH_FAILED, b"", b"refused");
                        close_conn(a, sys, conn);
                    }
                }
            }
            sc::MSG_SC_DRAINED => {
                if is_bound && c.phase == Phase::DrainWait {
                    a.conns[usize::from(conn)].phase = Phase::ImportWait;
                }
            }
            sc::CMD_SC_EXPORT_BEGIN | sc::CMD_SC_EXPORT_CHUNK | sc::CMD_SC_EXPORT_END => {
                if !is_bound || !matches!(c.phase, Phase::DrainWait | Phase::ImportWait) {
                    continue;
                }
                if mt == sc::CMD_SC_EXPORT_BEGIN {
                    let admit = match sc::export_cursors(p).and_then(SessionCursors::decode) {
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
            sc::MSG_SC_IMPORT_BEGIN => {
                if !is_bound
                    && c.phase == Phase::ImportWait
                    && n > SID_EPOCH
                    && sc::status(p) != sc::STATUS_OK
                {
                    refuse(a, sys, conn, b"no_capacity");
                }
            }
            sc::MSG_SC_IMPORT_END => {
                if !is_bound && c.phase == Phase::ImportWait && n > SID_EPOCH {
                    let st = sc::status(p);
                    if st == sc::STATUS_OK {
                        let new_epoch = c.epoch + 1;
                        if send_resume(a, sys, w, conn, new_epoch) {
                            a.conns[usize::from(conn)].phase = Phase::ResumeWait;
                            mon(a, sys, conn, super::MON_EV_RESUME_REQ, b"", b"");
                        } else {
                            refuse(a, sys, conn, b"not_ready");
                        }
                    } else {
                        let why: &[u8] = match st {
                            sc::STATUS_STALE_EPOCH => b"stale_epoch",
                            sc::STATUS_CURSOR_MISMATCH => b"cursor_mismatch",
                            _ => b"corrupt",
                        };
                        refuse(a, sys, conn, why);
                    }
                }
            }
            sc::MSG_SC_RESUMED => {
                if is_bound && c.phase == Phase::RefuseWait {
                    // Back on the exporting worker at the unchanged epoch.
                    a.conns[usize::from(conn)].phase = Phase::Active;
                    mon(a, sys, conn, super::MON_EV_RESUMED, b"", b"kept");
                    became_live(a, sys, conn);
                    flush_hold(a, sys);
                } else if !is_bound && c.phase == Phase::AdoptWait {
                    adopted(a, sys, w, conn);
                } else if !is_bound && c.phase == Phase::ResumeWait {
                    let old = usize::from(c.worker);
                    {
                        let cm = &mut a.conns[usize::from(conn)];
                        cm.epoch += 1;
                        cm.worker = w as u8;
                        cm.phase = Phase::Active;
                        cm.detach_old = 1;
                        // The export carried everything forwarded; the
                        // standby's checkpoint (if any) was of the worker
                        // the session just left.
                        cm.checkpoint_reset();
                    }
                    fifo_remove(&mut a.replay, &mut a.replay_len, |k, _| k == conn);
                    send_detach(a, sys, old, conn, sc::DETACH_NORMAL);
                    // One ATTACH at the next epoch naming the new worker is
                    // the whole rebind: the directory records it against
                    // that epoch and answers ATTACHED, so there is nothing
                    // for a separate epoch verb to add.
                    dir_attach(a, sys, conn);
                    mon(a, sys, conn, super::MON_EV_ATTACH_REQ, b"", b"");
                    a.relocated = a.relocated.wrapping_add(1);
                    if a.swap_pending > 0 {
                        a.swap_pending -= 1;
                    }
                    mon(a, sys, conn, super::MON_EV_RELOCATED, b"", b"ok");
                    rebound(a, sys, conn);
                    became_live(a, sys, conn);
                    flush_hold(a, sys);
                }
            }
            sc::MSG_SC_DETACHED => {
                // The exporting worker let the session go after a
                // relocation. Any other DETACHED acknowledges a release
                // or a refusal's detach, and changes nothing here.
                if !is_bound && c.detach_old != 0 {
                    a.conns[usize::from(conn)].detach_old = 0;
                }
            }
            sc::MSG_SC_ERROR => {
                if c.phase == Phase::AdoptWait && !is_bound {
                    // The standby could not adopt it: close, honestly.
                    a.loss_closed = a.loss_closed.wrapping_add(1);
                    mon(a, sys, conn, super::MON_EV_ERROR, b"", b"adopt_refused");
                    close_conn(a, sys, conn);
                } else if matches!(
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
/// the drain deadline, the keep-alive watch while no worker is, the hold
/// release and the record-count swap trigger.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn step(a: &mut Anchor, sys: &SyscallTable, now: u64) {
    if a.continuity != 0 {
        reanchor_step(a, sys, now);
    }
    if !enabled(a) {
        return;
    }
    if a.hello_sent == 0 {
        let mut hello = [0u8; 10];
        hello[0] = sc::ROLE_ANCHOR;
        hello[1..9].copy_from_slice(&a.anchor_id);
        let mut all = true;
        for w in 0..2 {
            if a.ctrl_out[w] >= 0 && !write_ctrl(a, sys, w, sc::CMD_SC_HELLO, &hello) {
                all = false;
            }
        }
        if all {
            a.hello_sent = 1;
            // Silence is measured from the handshake, not from boot.
            a.last_ack_ms = [now; 2];
        }
    }
    // Liveness probes, and the loss of a worker that stopped answering.
    if a.worker_probe_ms != 0 && now.saturating_sub(a.last_probe_ms) >= u64::from(a.worker_probe_ms)
    {
        a.last_probe_ms = now;
        a.probe_seq = a.probe_seq.wrapping_add(1);
        let seq = a.probe_seq.to_le_bytes();
        for w in 0..2 {
            if a.ctrl_out[w] >= 0 && a.lost[w] == 0 {
                write_ctrl(a, sys, w, wire::MSG_ANCHOR_PROBE, &seq);
            }
        }
    }
    if a.hello_sent != 0 {
        for w in 0..2 {
            if a.ctrl_out[w] >= 0
                && a.lost[w] == 0
                && worker_lost(a.last_ack_ms[w], now, a.worker_probe_ms, a.worker_loss_ms)
            {
                declare_lost(a, sys, w, now);
            }
        }
    }
    let deadline = u64::from(a.session_drain_ms);
    // Bounded walk: only connections mid-attach or mid-handoff need a
    // look, and they are rare relative to the table; the walk itself
    // is 64 Ki cheap loads.
    for i in 0..CONN_ID_SPACE {
        let c = a.conns[i];
        match c.phase {
            Phase::Active => {
                if c.replay_from != 0 {
                    pump_replay(a, sys, i as u16);
                }
                continue;
            }
            Phase::Idle => continue,
            Phase::Closing => {
                if c.resume_pending != 0 && send_close(a, sys, i as u16) {
                    a.conns[i].resume_pending = 0;
                }
                continue;
            }
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
            Phase::AdoptWait => {
                if now.saturating_sub(c.started_ms) > deadline {
                    a.loss_closed = a.loss_closed.wrapping_add(1);
                    mon(a, sys, i as u16, super::MON_EV_ERROR, b"", b"adopt_timeout");
                    close_conn(a, sys, i as u16);
                }
            }
        }
        // No worker is watching this client's keep-alive while it is
        // not authoritative (the worker's sweep exempts a held session),
        // so the anchor does: a client silent past its budget is gone,
        // and holding its connection open would be a lie to no one.
        let c = a.conns[i];
        if c.phase != Phase::Closing && keepalive_expired(&c, now) {
            a.keepalive_closed = a.keepalive_closed.wrapping_add(1);
            dev_log(sys, 2, b"[anchor] keep-alive expired".as_ptr(), 27);
            close_conn(a, sys, i as u16);
        }
    }
    if a.hold_len > 0 {
        flush_hold(a, sys);
    }
    if a.continuity != 0 {
        ack_step(a, sys);
    }
    if a.handoff_after_records != 0
        && a.records_since_swap >= a.handoff_after_records
        && (a.handoff_max_swaps == 0 || a.swaps < a.handoff_max_swaps)
    {
        let _ = start_swap(a, sys, now);
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

/// Log the anchor's `[anchor] hb` line: attachments and relocations,
/// refused handoffs, records held; pings answered and owed, stale
/// responses fenced, operator swaps, read pauses and keep-alive closes;
/// worker losses, sessions adopted and closed for want of an exact
/// adoption, frames replayed, checkpoints relayed and acknowledged, and
/// replay gaps. Every counter is cumulative.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn log_hb(a: &Anchor, sys: &SyscallTable) {
    let mut line = [0u8; 384];
    let mut pos = 0usize;
    for (label, v) in [
        (&b"[anchor] hb att="[..], a.attached),
        (&b" reloc="[..], a.relocated),
        (&b" hoffref="[..], a.refused),
        (&b" heldrec="[..], a.held_records),
        (&b" ping="[..], a.pings_answered),
        (&b" pingowed="[..], a.pings_deferred),
        (&b" stale="[..], a.stale_responses),
        (&b" swcmd="[..], a.swap_cmds),
        (&b" rpause="[..], a.read_pauses),
        (&b" kaclose="[..], a.keepalive_closed),
        (&b" lost="[..], a.losses),
        (&b" adopted="[..], a.adopted),
        (&b" lclosed="[..], a.loss_closed),
        (&b" replayed="[..], a.replayed),
        (&b" ckrel="[..], a.ckpt_relayed),
        (&b" ckack="[..], a.ckpt_acked),
        (&b" rgap="[..], a.replay_gaps),
    ] {
        for &b in label {
            line[pos] = b;
            pos += 1;
        }
        pos = super::fmt_u32(&mut line, pos, v);
    }
    dev_log(sys, 3, line.as_ptr(), pos);
    if a.continuity == 0 {
        return;
    }
    // Anchor continuity: productions given, re-offers answered without
    // the transport, gaps refused, horizon reports, records exported,
    // imports staged / re-anchored / refused, connections without a gate,
    // acknowledgements the transport took, staged records dropped,
    // resumed connections no record claimed, attachments without a
    // reported client.
    let mut pos = 0usize;
    for (label, v) in [
        (&b"[anchor] cont given="[..], a.prod_given),
        (&b" dup="[..], a.prod_dup),
        (&b" gap="[..], a.prod_gap),
        (&b" hzrep="[..], a.horizon_reports),
        (&b" qac="[..], a.qac_exported),
        (&b" staged="[..], a.imports_staged),
        (&b" reanch="[..], a.reanchored),
        (&b" reref="[..], a.reanchor_refused),
        (&b" gatefull="[..], a.gate_full),
        (&b" acked="[..], a.deliveries_acked),
        (&b" stdrop="[..], a.staged_dropped),
        (&b" runknown="[..], a.resumed_unknown),
        (&b" unpeered="[..], a.qac_unpeered),
    ] {
        for &b in label {
            line[pos] = b;
            pos += 1;
        }
        pos = super::fmt_u32(&mut line, pos, v);
    }
    dev_log(sys, 3, line.as_ptr(), pos);
}

#[inline]
pub fn phase(a: &Anchor, conn: u16) -> Phase {
    a.conns[usize::from(conn)].phase
}

// ── Anchor continuity (docs/architecture/anchor_continuity.md) ───────

/// The class an attachment is made under: transport-migratable when this
/// anchor runs continuity, so the worker seats the session.
fn attach_class(a: &Anchor) -> u8 {
    if a.continuity != 0 {
        sc::CC_TRANSPORT_MIGRATABLE
    } else {
        sc::CC_EDGE_ANCHORED
    }
}

fn gate_seat(a: &Anchor, conn: u16) -> Option<usize> {
    a.gates.iter().position(|g| g.used && g.conn == conn)
}

/// Whether `conn` runs under a production gate.
pub fn gated(a: &Anchor, conn: u16) -> bool {
    gate_seat(a, conn).is_some()
}

/// A plain response arrived for gated `conn`: a DISCONNECT closes the
/// connection, anything else is dropped as a stale emitter's.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
pub unsafe fn plain_on_gated(a: &mut Anchor, sys: &SyscallTable, conn: u16, disconnect: bool) {
    if disconnect {
        close_conn(a, sys, conn);
    } else {
        a.stale_responses = a.stale_responses.wrapping_add(1);
    }
}

/// A gate for `conn`'s new attachment: productions from 1, its output
/// from the first byte. A gate left on the id by an earlier connection is
/// that connection's, and is replaced.
fn gate_open(a: &mut Anchor, conn: u16) -> bool {
    let Some(i) = gate_seat(a, conn).or_else(|| a.gates.iter().position(|g| !g.used)) else {
        return false;
    };
    a.gates[i] = GateSeat::empty();
    a.gates[i].used = true;
    a.gates[i].conn = conn;
    a.gates[i].peer = a
        .peers
        .iter()
        .flatten()
        .find(|(c, _)| *c == conn)
        .map(|&(_, p)| p);
    true
}

/// `conn` is gone: its gate and alias with it.
pub fn gate_close(a: &mut Anchor, conn: u16) {
    if let Some(i) = gate_seat(a, conn) {
        a.gates[i] = GateSeat::empty();
    }
    for al in a.aliases.iter_mut() {
        if matches!(al, Some((_, to)) if *to == conn) {
            *al = None;
        }
    }
}

/// The connection a session id is served on here: the one it names,
/// unless the transport presented an imported connection under another
/// id.
fn conn_of_sid(a: &Anchor, sid: &[u8; SID]) -> u16 {
    let named = session_id_conn(sid);
    for &(from, to) in a.aliases.iter().flatten() {
        if from == named && a.conns[usize::from(to)].session_id == *sid {
            return to;
        }
    }
    named
}

/// Export `conn`'s `QAC1` record and its client on `state_out`. A
/// connection whose client the transport never reported cannot be
/// claimed by the paired anchor, so nothing is exported for it.
unsafe fn export_qac(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    if a.continuity == 0 || a.state_out < 0 {
        return;
    }
    let Some(i) = gate_seat(a, conn) else {
        return;
    };
    let Some(peer) = a.gates[i].peer else {
        a.qac_unpeered = a.qac_unpeered.wrapping_add(1);
        return;
    };
    let accepted = a.gates[i].gate.accepted;
    let rec = AnchorRecord::of(&a.conns[usize::from(conn)], &a.anchor_id, accepted);
    let mut b = [0u8; wire::QAC_EXPORT_LEN];
    if rec.encode(&mut b) != QAC_LEN {
        return;
    }
    b[QAC_LEN] = peer.family;
    b[QAC_LEN + 1..QAC_LEN + 17].copy_from_slice(&peer.addr);
    b[QAC_LEN + 17..QAC_LEN + 19].copy_from_slice(&peer.port.to_le_bytes());
    if wire::channel_write_msg(sys, a.state_out, wire::MSG_ANCHOR_QAC, &b) > 0 {
        a.qac_exported = a.qac_exported.wrapping_add(1);
    }
}

/// `conn` closed here: the paired anchor drops the record it holds of
/// it (`MSG_ANCHOR_QAC_RETIRE`).
pub unsafe fn retire_qac(a: &mut Anchor, sys: &SyscallTable, conn: u16) {
    if a.continuity == 0 || a.state_out < 0 || gate_seat(a, conn).is_none() {
        return;
    }
    let sid = a.conns[usize::from(conn)].session_id;
    wire::channel_write_msg(sys, a.state_out, wire::MSG_ANCHOR_QAC_RETIRE, &sid);
}

/// `MSG_ANCHOR_QAC [session_id]` on `cmd_in`: export that session's
/// record now (a planned cut asks after its quiesce).
pub unsafe fn qac_request(a: &mut Anchor, sys: &SyscallTable, p: &[u8]) {
    if p.len() < SID {
        return;
    }
    let conn = conn_of_sid(a, &sid_of(p));
    if a.conns[usize::from(conn)].session_id[..] == p[..SID] {
        export_qac(a, sys, conn);
    }
}

/// What the module does with a production from worker `w`.
pub enum ProductionStep {
    /// Dropped: a stale emitter's, or not this connection's binding.
    Done,
    /// Answered without the transport — a re-offer the gate already
    /// took, or a gap refused — for the bound worker, which counted it as
    /// written, so the seam's cursor counts it too.
    Answered,
    /// Production `id` for `conn` is the next the transport takes: hand
    /// its response to the codec, writing only what the transport does
    /// not hold yet — `written` and `through` are where the connection's
    /// output stands and how far the standby holds it — then
    /// `production_taken`.
    Offer {
        conn: u16,
        id: u64,
        written: u64,
        through: u64,
    },
    /// More productions are beyond the horizon than it tracks, or the
    /// worker re-offers ahead of its REANCHORED: hold this one and offer
    /// it again.
    Hold,
}

/// Admit a `MSG_ANCHOR_PRODUCTION` from worker `w` through `conn`'s gate.
pub unsafe fn production_step(
    a: &mut Anchor,
    sys: &SyscallTable,
    w: usize,
    frame: &[u8],
) -> ProductionStep {
    let Some(h) = decode_production(frame) else {
        return ProductionStep::Done;
    };
    let conn = conn_of_sid(a, &h.session_id);
    // The worker re-offers right after its REANCHORED, which this step
    // reads only after the responses: a re-offer at the epoch the import
    // claims is held until the answer makes it the connection's.
    if let Some(i) = gate_seat(a, conn) {
        if let Some(imp) = a.gates[i].reanchor {
            if imp.conn.session_id == h.session_id
                && imp.new_epoch == h.epoch
                && usize::from(imp.conn.worker) & 1 == w
            {
                return ProductionStep::Hold;
            }
        }
    }
    {
        // The same for the worker a handoff or an adoption moves the
        // session to: its productions at the next epoch wait for its
        // RESUMED.
        let c = &a.conns[usize::from(conn)];
        if taking_over(c, w)
            && a.lost[w] == 0
            && c.session_id == h.session_id
            && h.epoch == c.epoch.wrapping_add(1)
        {
            return ProductionStep::Hold;
        }
    }
    if !response_admitted(a, w, conn) {
        return ProductionStep::Done;
    }
    let c = a.conns[usize::from(conn)];
    if c.session_id != h.session_id
        || c.epoch != h.epoch
        || matches!(c.phase, Phase::Idle | Phase::Closing)
    {
        a.stale_responses = a.stale_responses.wrapping_add(1);
        return ProductionStep::Done;
    }
    let Some(i) = gate_seat(a, conn) else {
        a.stale_responses = a.stale_responses.wrapping_add(1);
        return ProductionStep::Done;
    };
    let g = a.gates[i].gate;
    if h.production_id == g.next_submit {
        let hz = a.gates[i].horizon;
        if !hz.ready() {
            return ProductionStep::Hold;
        }
        return ProductionStep::Offer {
            conn,
            id: h.production_id,
            written: hz.written,
            through: hz.through,
        };
    }
    // A re-offer of what the transport already holds, or a gap: the gate
    // answers without the transport.
    let seat = &mut a.gates[i];
    match seat.gate.offer(&mut seat.horizon, h.production_id, 0) {
        Offer::Accepted | Offer::Pending => a.prod_dup = a.prod_dup.wrapping_add(1),
        Offer::Gap | Offer::NotReady => a.prod_gap = a.prod_gap.wrapping_add(1),
    }
    report_horizon(a, sys, w, conn);
    ProductionStep::Answered
}

/// The connection a production envelope is for here.
pub fn production_conn(a: &Anchor, frame: &[u8]) -> u16 {
    decode_production(frame).map_or(0, |h| conn_of_sid(a, &h.session_id))
}

/// The codec took production `id` for `conn` onto the transport as `len`
/// bytes of its output (some of which the standby may already have held):
/// it is inside the horizon once the transport's report reaches its last
/// byte, and the worker is told then.
pub unsafe fn production_taken(
    a: &mut Anchor,
    sys: &SyscallTable,
    w: usize,
    conn: u16,
    id: u64,
    len: u64,
) {
    let Some(i) = gate_seat(a, conn) else {
        return;
    };
    let seat = &mut a.gates[i];
    if seat.gate.offer(&mut seat.horizon, id, len) == Offer::Accepted {
        a.prod_given = a.prod_given.wrapping_add(1);
    }
    report_horizon(a, sys, w, conn);
}

/// `MSG_CLIENT_HORIZON [conn][bytes_through]` on `raw_in`: the transport's
/// mirrored output horizon for `conn` moved. Every production whose last
/// byte it reached is accepted, and the worker is told.
pub unsafe fn transport_horizon(a: &mut Anchor, sys: &SyscallTable, p: &[u8]) {
    if p.len() < wire::CLIENT_HORIZON_LEN {
        return;
    }
    let conn = u16::from_le_bytes([p[0], p[1]]);
    let mut b = [0u8; 8];
    b.copy_from_slice(&p[2..10]);
    let through = u64::from_le_bytes(b);
    let Some(i) = gate_seat(a, conn) else {
        return;
    };
    a.horizon_reports = a.horizon_reports.wrapping_add(1);
    let seat = &mut a.gates[i];
    if !seat.horizon.report(through) {
        return;
    }
    let before = seat.gate.accepted;
    seat.gate.observe(&seat.horizon);
    let given = seat.gate.accepted.saturating_sub(before);
    a.prod_given = a.prod_given.wrapping_add(given as u32);
    if a.gates[i].reanchor.is_none() {
        let w = usize::from(a.conns[usize::from(conn)].worker) & 1;
        report_horizon(a, sys, w, conn);
    }
}

/// Tell the bound worker the horizon `conn`'s gate reached, once per
/// advance.
unsafe fn report_horizon(a: &mut Anchor, sys: &SyscallTable, w: usize, conn: u16) {
    let Some(i) = gate_seat(a, conn) else {
        return;
    };
    let g = a.gates[i].gate;
    let hz = a.gates[i].horizon;
    if g.accepted <= g.reported || hz.accepted != g.accepted {
        return;
    }
    let c = &a.conns[usize::from(conn)];
    let rep = HorizonReport {
        session_id: c.session_id,
        epoch: c.epoch,
        accepted_through: g.accepted,
        bytes_through: hz.accepted_bytes,
    };
    let mut b = [0u8; HORIZON_LEN];
    rep.encode(&mut b);
    if write_ctrl(a, sys, w, wire::MSG_ANCHOR_HORIZON, &b) {
        a.gates[i].gate.reported = g.accepted;
    }
}

/// The paired anchor's records from `state_in`: each `MSG_ANCHOR_QAC`
/// is staged under its session — replacing an older record of it, or,
/// on a full stage, the record staged longest ago — and a
/// `MSG_ANCHOR_QAC_RETIRE` drops the session's. A record that does not
/// decode is dropped and counted. Without continuity there is nothing to
/// import.
pub unsafe fn handle_state(a: &mut Anchor, sys: &SyscallTable, now: u64) -> u32 {
    if a.state_in < 0 || a.continuity == 0 {
        return 0;
    }
    let mut worked = 0;
    for _ in 0..4 {
        let poll = (sys.channel_poll)(a.state_in, 0x01);
        if poll <= 0 || (poll as u32 & 0x01) == 0 {
            break;
        }
        let mut buf = [0u8; 256];
        let (mt, plen) = wire::channel_read_msg(sys, a.state_in, &mut buf);
        worked += 1;
        let n = usize::from(plen);
        if mt == wire::MSG_ANCHOR_QAC_RETIRE && n == SID {
            for st in a.staged.iter_mut() {
                if st.used && st.rec.session_id[..] == buf[..SID] {
                    st.used = false;
                }
            }
            continue;
        }
        if mt != wire::MSG_ANCHOR_QAC || n != wire::QAC_EXPORT_LEN {
            continue;
        }
        let Ok(rec) = AnchorRecord::decode(&buf[..QAC_LEN]) else {
            a.reanchor_refused = a.reanchor_refused.wrapping_add(1);
            continue;
        };
        let at = QAC_LEN;
        let mut addr = [0u8; 16];
        addr.copy_from_slice(&buf[at + 1..at + 17]);
        let peer = Peer {
            family: buf[at],
            addr,
            port: u16::from_le_bytes([buf[at + 17], buf[at + 18]]),
        };
        let same = a
            .staged
            .iter()
            .position(|x| x.used && x.rec.session_id == rec.session_id);
        let k = match same {
            Some(k) if a.staged[k].rec.epoch > rec.epoch => continue,
            Some(k) => k,
            None => match a.staged.iter().position(|x| !x.used) {
                Some(k) => k,
                None => {
                    let mut oldest = 0;
                    for (k, x) in a.staged.iter().enumerate() {
                        if x.staged_ms < a.staged[oldest].staged_ms {
                            oldest = k;
                        }
                    }
                    a.staged_dropped = a.staged_dropped.wrapping_add(1);
                    oldest
                }
            },
        };
        let st = &mut a.staged[k];
        if same.is_none() || st.peer != peer {
            st.claimed = None;
        }
        st.used = true;
        st.rec = rec;
        st.peer = peer;
        st.staged_ms = now;
        a.imports_staged = a.imports_staged.wrapping_add(1);
    }
    worked
}

/// The transport reported the client of `conn` (`MSG_CLIENT_PEER`). A
/// connection a takeover moved here presents the client of a staged
/// record: it claims that record, imported when the transport's
/// `MSG_CLIENT_RESUMED` follows. Every connection's client is
/// remembered for the record its attachment exports.
pub fn on_peer(a: &mut Anchor, conn: u16, peer: Peer) {
    if a.continuity == 0 {
        return;
    }
    for st in a.staged.iter_mut() {
        if st.claimed == Some(conn) {
            st.claimed = None;
        }
    }
    if let Some(k) = a.staged.iter().position(|x| x.used && x.peer == peer) {
        a.staged[k].claimed = Some(conn);
    }
    let slot = match a
        .peers
        .iter()
        .position(|p| matches!(p, Some((c, _)) if *c == conn))
    {
        Some(k) => k,
        None => {
            let k = a.peers_next;
            a.peers_next = (k + 1) % PEERS_SEEN;
            k
        }
    };
    a.peers[slot] = Some((conn, peer));
}

/// Something other than `MSG_CLIENT_RESUMED` followed `conn`'s client
/// report: the transport sends a resumed connection's ahead of its data,
/// so this connection is a new one from the client a staged record
/// names. That record's connection is gone; it is dropped.
pub fn on_data(a: &mut Anchor, conn: u16) {
    if a.continuity == 0 {
        return;
    }
    if let Some(k) = a
        .staged
        .iter()
        .position(|x| x.used && x.claimed == Some(conn))
    {
        a.staged[k].used = false;
        a.staged[k].claimed = None;
        a.staged_dropped = a.staged_dropped.wrapping_add(1);
    }
}

/// `MSG_CLIENT_RESUMED [conn][consumer_cursor][in_bytes][out_bytes]`: a
/// takeover moved `conn` here. The record its client claimed is imported
/// over the transport's cursors (`anchor_import`): the bytes handed from
/// here start at `in_bytes`, the first delivery among them takes
/// `consumer_cursor`, and the standby holds `out_bytes` of the output.
/// The connection is held until its worker answers `MSG_ANCHOR_REANCHOR`.
/// A resumed connection no record claimed, or one whose cursors cannot
/// be imported, is closed and its client reconnects. Returns the MQTT
/// version the codec decodes the connection under and the byte its
/// input resumes at.
pub unsafe fn on_resumed(
    a: &mut Anchor,
    sys: &SyscallTable,
    p: &[u8],
    now: u64,
) -> Option<(u8, u64)> {
    if a.continuity == 0 || p.len() < wire::CLIENT_RESUMED_LEN {
        return None;
    }
    let conn = u16::from_le_bytes([p[0], p[1]]);
    let u = |at: usize| {
        let mut x = [0u8; 8];
        x.copy_from_slice(&p[at..at + 8]);
        u64::from_le_bytes(x)
    };
    let (cursor, in_bytes, out_bytes) = (u(2), u(10), u(18));
    let Some(k) = a
        .staged
        .iter()
        .position(|x| x.used && x.claimed == Some(conn))
    else {
        a.resumed_unknown = a.resumed_unknown.wrapping_add(1);
        close_conn(a, sys, conn);
        return None;
    };
    let st = a.staged[k];
    a.staged[k].used = false;
    a.staged[k].claimed = None;
    let cursors = TransportCursors {
        next_delivery: cursor,
        delivery_acked: cursor.saturating_sub(1),
        out_bytes,
    };
    let imp = match anchor_import(&st.rec, &cursors) {
        Ok(r) => r,
        Err(_) => {
            a.reanchor_refused = a.reanchor_refused.wrapping_add(1);
            close_conn(a, sys, conn);
            return None;
        }
    };
    let named = session_id_conn(&imp.conn.session_id);
    let alias = if named == conn {
        None
    } else {
        match a.aliases.iter().position(|al| al.is_none()) {
            Some(k) => Some(k),
            None => {
                a.gate_full = a.gate_full.wrapping_add(1);
                close_conn(a, sys, conn);
                return None;
            }
        }
    };
    let Some(slot) = gate_seat(a, conn).or_else(|| a.gates.iter().position(|g| !g.used)) else {
        a.gate_full = a.gate_full.wrapping_add(1);
        close_conn(a, sys, conn);
        return None;
    };
    if let Some(k) = alias {
        a.aliases[k] = Some((named, conn));
    }
    a.conns[usize::from(conn)] = imp.conn;
    a.gates[slot] = GateSeat::empty();
    a.gates[slot].used = true;
    a.gates[slot].conn = conn;
    a.gates[slot].gate = imp.gate;
    a.gates[slot].horizon = TransportHorizon::resumed(imp.bytes_held, 0, 0);
    a.gates[slot].inbound = DeliveryCursor::resumed(cursor, in_bytes);
    a.gates[slot].peer = Some(st.peer);
    a.gates[slot].reanchor = Some(imp);
    a.gates[slot].reanchor_ms = now;
    a.gates[slot].reanchor_sent_ms = 0;
    send_reanchor(a, sys, slot, now);
    a.imports_adopted = a.imports_adopted.wrapping_add(1);
    mon(a, sys, conn, super::MON_EV_ATTACH_REQ, b"", b"reanchor");
    Some((imp.conn.mqtt_version, in_bytes))
}

/// The codec has given a delivery id to every packet in `conn`'s first
/// `at` clear bytes: a boundary the transport may later be told is
/// consumed.
pub fn boundary(a: &mut Anchor, conn: u16, at: u64) {
    if let Some(i) = gate_seat(a, conn) {
        let next = a.conns[usize::from(conn)].peek_delivery();
        a.gates[i].inbound.boundary(next, at);
    }
}

/// `MSG_ANCHOR_CONSUMED` from worker `w`: counted for the connection the
/// session is served on when `w` is its bound worker at its epoch.
fn consumed(a: &mut Anchor, w: usize, p: &[u8]) {
    let conn = conn_of_sid(a, &sid_of(p));
    let c = a.conns[usize::from(conn)];
    if c.session_id[..] != p[..SID] || usize::from(c.worker) != w || sc::epoch(p) != c.epoch {
        return;
    }
    let mut hw = [0u8; 8];
    hw.copy_from_slice(&p[SID_EPOCH..SID_EPOCH + 8]);
    if let Some(i) = gate_seat(a, conn) {
        if a.gates[i].reanchor.is_none() {
            a.gates[i].inbound.consumed(u64::from_le_bytes(hw));
        }
    }
}

/// Tell the transport what each gated connection's worker has consumed
/// (`MSG_CLIENT_DELIVERED`), in order with the connection's output.
unsafe fn ack_step(a: &mut Anchor, sys: &SyscallTable) {
    if a.out_frames < 0 {
        return;
    }
    for i in 0..GATE_SEATS {
        let g = a.gates[i];
        if !g.used
            || g.reanchor.is_some()
            || matches!(
                a.conns[usize::from(g.conn)].phase,
                Phase::Idle | Phase::Closing
            )
        {
            continue;
        }
        let Some((cursor, at)) = a.gates[i].inbound.due() else {
            continue;
        };
        let mut b = [0u8; wire::CLIENT_DELIVERED_LEN];
        b[..2].copy_from_slice(&g.conn.to_le_bytes());
        b[2..10].copy_from_slice(&cursor.to_le_bytes());
        b[10..18].copy_from_slice(&at.to_le_bytes());
        if wire::channel_write_msg(sys, a.out_frames, wire::MSG_CLIENT_DELIVERED, &b) <= 0 {
            return;
        }
        a.gates[i].inbound.acked();
        a.deliveries_acked = a.deliveries_acked.wrapping_add(1);
    }
}

/// Send gate `i`'s `MSG_ANCHOR_REANCHOR` to its worker, noting when the
/// control edge took it.
unsafe fn send_reanchor(a: &mut Anchor, sys: &SyscallTable, i: usize, now: u64) {
    let Some(imp) = a.gates[i].reanchor else {
        return;
    };
    let req = imp.request(&a.anchor_id);
    let mut b = [0u8; REANCHOR_LEN];
    let n = req.encode(&mut b);
    let w = usize::from(imp.conn.worker) & 1;
    if write_ctrl(a, sys, w, wire::MSG_ANCHOR_REANCHOR, &b[..n]) {
        a.gates[i].reanchor_sent_ms = now.max(1);
    }
}

/// Imported connections still waiting for their worker's answer: the
/// request again every [`REANCHOR_RETRY_MS`], and the connection closed
/// past [`REANCHOR_GIVE_UP_MS`].
unsafe fn reanchor_step(a: &mut Anchor, sys: &SyscallTable, now: u64) {
    for i in 0..GATE_SEATS {
        let g = a.gates[i];
        if !g.used || g.reanchor.is_none() {
            continue;
        }
        if now.saturating_sub(g.reanchor_ms) >= REANCHOR_GIVE_UP_MS {
            a.gates[i].reanchor = None;
            a.reanchor_refused = a.reanchor_refused.wrapping_add(1);
            mon(
                a,
                sys,
                g.conn,
                super::MON_EV_ATTACH_FAILED,
                b"",
                b"reanchor_timeout",
            );
            close_conn(a, sys, g.conn);
        } else if g.reanchor_sent_ms == 0
            || now.saturating_sub(g.reanchor_sent_ms) >= REANCHOR_RETRY_MS
        {
            send_reanchor(a, sys, i, now);
        }
    }
}

/// `MSG_ANCHOR_REANCHORED` from worker `w`: an import whose worker
/// accepts it is live at the new epoch — the directory told, the held
/// frames released (a delivery the worker already consumed it refuses
/// as a duplicate); any other answer closes the connection and the
/// client reconnects.
unsafe fn reanchored(a: &mut Anchor, sys: &SyscallTable, w: usize, p: &[u8]) {
    let Some(ans) = Reanchored::decode(p) else {
        return;
    };
    let conn = conn_of_sid(a, &ans.session_id);
    let Some(i) = gate_seat(a, conn) else {
        return;
    };
    let Some(mut imp) = a.gates[i].reanchor else {
        return;
    };
    if usize::from(imp.conn.worker) & 1 != w {
        return;
    }
    if imp.apply(&ans) != AC_OK {
        a.gates[i].reanchor = None;
        a.reanchor_refused = a.reanchor_refused.wrapping_add(1);
        mon(
            a,
            sys,
            conn,
            super::MON_EV_ATTACH_FAILED,
            b"",
            b"reanchor_refused",
        );
        close_conn(a, sys, conn);
        return;
    }
    a.gates[i].reanchor = None;
    a.gates[i].gate = imp.gate;
    // The horizon the transport reported meanwhile only ever moves on.
    let mut hz = imp.horizon;
    hz.report(a.gates[i].horizon.through);
    a.gates[i].horizon = hz;
    {
        // Only the binding moves: what the connection did while it
        // waited (frames held, ids taken, a ping owed) stands. The seam's
        // cursors count afresh from here, as the worker's do from its
        // answer.
        let c = &mut a.conns[usize::from(conn)];
        c.epoch = imp.new_epoch;
        c.phase = Phase::Active;
        c.forwarded = 0;
        c.relayed = 0;
    }
    a.reanchored = a.reanchored.wrapping_add(1);
    a.attached = a.attached.wrapping_add(1);
    dir_attach(a, sys, conn);
    export_qac(a, sys, conn);
    mon(a, sys, conn, super::MON_EV_ATTACHED, b"", b"reanchored");
    became_live(a, sys, conn);
    flush_hold(a, sys);
}
