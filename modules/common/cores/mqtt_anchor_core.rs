// Bounded, no_std, no-alloc MQTT ANCHOR core — the decisions and state
// `quantum_mqtt_anchor` (and `protocol`, which mounts the same anchor)
// keeps for a client transport it holds open while the session worker
// behind it moves, stalls or is replaced.
// `include!`d by the host test crate and by the anchor.
//
// ## What the anchor owns
//
// Only the minimum needed to route, preserve framing and keep the
// connection alive — [`AnchorConn`] is that list, field for field:
// tenant/listener identity, MQTT version, client-id hash, session epoch,
// negotiated keep-alive and the last safe inbound/outbound activity, the
// delivery ids of frames waiting for a worker, the production ids of
// worker responses waiting for the transport, the worker binding and
// the handoff phase, and the CONNECT / AUTH / drain / liveness-only
// flags. The incremental fixed-header parser and its partial bytes are
// the codec's reassembly buffer, which lives in the same module.
// Subscriptions, retained messages, offline queues, packet-id/QoS state
// and dedupe stay with the worker and the committed log: the anchor is
// not a second MQTT database.
//
// ## Delivery ids
//
// Every framed request the anchor hands a worker carries
// `(session_id, session_epoch, delivery_id)` in a delivery envelope
// ([`encode_delivery`]). The id is assigned when the frame is first
// taken from the client and never changes after — not when it is held,
// not when a refused write is retried, not when it is replayed to a
// replacement — so the worker can tell a retry from new work by the id
// alone ([`admit_delivery`]). Ids are per session, start at 1 and only
// increase; 0 is never issued.
//
// ## Production ids
//
// The response the anchor is about to give the transport is
// `next_production`; the counter advances only when the transport
// ACCEPTS it. A response the frame edge refuses is offered again under
// the same id, which is what makes the id stable until accepted.

/// MQTT control packet types the anchor distinguishes.
pub const MQTT_CONNECT: u8 = 1;
pub const MQTT_PUBLISH: u8 = 3;
pub const MQTT_SUBSCRIBE: u8 = 8;
pub const MQTT_UNSUBSCRIBE: u8 = 10;
pub const MQTT_PINGREQ: u8 = 12;
pub const MQTT_PINGRESP: u8 = 13;
pub const MQTT_DISCONNECT: u8 = 14;
pub const MQTT_AUTH: u8 = 15;

/// Session-envelope protocol tag for MQTT (`payload[2]`).
pub const ENV_PROTO_MQTT: u8 = 0;

/// Bytes of a Fluxor `session_id`.
pub const SESSION_ID_LEN: usize = 16;
/// Bytes of the connection id every envelope leads with.
pub const CONN_ID_LEN: usize = 2;

// ── Delivery envelope ───────────────────────────────────────────────────────

/// Bytes the delivery envelope inserts after the connection id:
/// `[session_id:16][session_epoch:u32 LE][delivery_id:u64 LE]
/// [inner_msg_type:u8]`.
pub const DELIVERY_INSERT: usize = SESSION_ID_LEN + 4 + 8 + 1;

/// The fields a delivery envelope carries in front of the frame.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DeliveryHeader {
    pub conn: u16,
    pub session_id: [u8; SESSION_ID_LEN],
    pub epoch: u32,
    pub delivery_id: u64,
    pub inner_msg_type: u8,
}

/// Wrap the envelope `inner = [conn_id:2][rest]` of type `inner_mt` as a
/// delivery envelope payload into `out`:
/// `[conn_id:2][session_id:16][epoch:4][delivery_id:8][inner_mt:1][rest]`.
/// Returns the bytes written, or 0 when `inner` has no connection id or
/// `out` is too short — never a truncated frame.
pub fn encode_delivery(
    out: &mut [u8],
    inner_mt: u8,
    inner: &[u8],
    session_id: &[u8; SESSION_ID_LEN],
    epoch: u32,
    delivery_id: u64,
) -> usize {
    if inner.len() < CONN_ID_LEN {
        return 0;
    }
    let total = inner.len() + DELIVERY_INSERT;
    if out.len() < total {
        return 0;
    }
    out[0] = inner[0];
    out[1] = inner[1];
    let mut at = CONN_ID_LEN;
    out[at..at + SESSION_ID_LEN].copy_from_slice(session_id);
    at += SESSION_ID_LEN;
    out[at..at + 4].copy_from_slice(&epoch.to_le_bytes());
    at += 4;
    out[at..at + 8].copy_from_slice(&delivery_id.to_le_bytes());
    at += 8;
    out[at] = inner_mt;
    at += 1;
    let rest = &inner[CONN_ID_LEN..];
    out[at..at + rest.len()].copy_from_slice(rest);
    total
}

/// Read a delivery envelope's header, or `None` when it is too short to
/// carry one.
pub fn decode_delivery(p: &[u8]) -> Option<DeliveryHeader> {
    if p.len() < CONN_ID_LEN + DELIVERY_INSERT {
        return None;
    }
    let mut sid = [0u8; SESSION_ID_LEN];
    sid.copy_from_slice(&p[CONN_ID_LEN..CONN_ID_LEN + SESSION_ID_LEN]);
    let e = CONN_ID_LEN + SESSION_ID_LEN;
    let mut id = [0u8; 8];
    id.copy_from_slice(&p[e + 4..e + 12]);
    Some(DeliveryHeader {
        conn: u16::from_le_bytes([p[0], p[1]]),
        session_id: sid,
        epoch: u32::from_le_bytes([p[e], p[e + 1], p[e + 2], p[e + 3]]),
        delivery_id: u64::from_le_bytes(id),
        inner_msg_type: p[e + 12],
    })
}

/// Unwrap the delivery envelope in `buf[..n]` in place, leaving the inner
/// envelope `[conn_id:2][rest]` at the head of `buf`. Returns the header
/// and the inner length, or `None` when `buf[..n]` is not one.
pub fn unwrap_delivery_in_place(buf: &mut [u8], n: usize) -> Option<(DeliveryHeader, usize)> {
    if n > buf.len() {
        return None;
    }
    let h = decode_delivery(&buf[..n])?;
    let from = CONN_ID_LEN + DELIVERY_INSERT;
    let mut k = 0usize;
    // Byte loop: the PIC build links no panic machinery for a
    // bounds-checked `copy_within`.
    while from + k < n {
        buf[CONN_ID_LEN + k] = buf[from + k];
        k += 1;
    }
    Some((h, n - DELIVERY_INSERT))
}

/// What a worker does with a delivery envelope.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeliveryVerdict {
    /// New work for the session at its current epoch: consume it.
    Accept,
    /// The envelope names an epoch the session has left behind — a
    /// stale anchor binding or a frame replayed from before a rebind.
    StaleEpoch,
    /// The envelope names an epoch this worker has not been resumed at:
    /// the anchor is ahead of it, so it is not authoritative yet.
    FutureEpoch,
    /// A delivery id at or below the high-water this session has
    /// already consumed: a retry, answered by the state it left.
    Duplicate,
    /// No live binding for that session id on this connection.
    UnknownSession,
}

/// The worker's admission rule for a delivery envelope against the
/// binding it holds: `(session_id, epoch)` and the highest delivery id
/// already consumed, `hw`.
pub fn admit_delivery(
    bound_sid: &[u8; SESSION_ID_LEN],
    bound_epoch: u32,
    hw: u64,
    h: &DeliveryHeader,
) -> DeliveryVerdict {
    if &h.session_id != bound_sid || bound_epoch == 0 {
        return DeliveryVerdict::UnknownSession;
    }
    if h.epoch < bound_epoch {
        return DeliveryVerdict::StaleEpoch;
    }
    if h.epoch > bound_epoch {
        return DeliveryVerdict::FutureEpoch;
    }
    if h.delivery_id == 0 || h.delivery_id <= hw {
        return DeliveryVerdict::Duplicate;
    }
    DeliveryVerdict::Accept
}

// ── The per-connection record ───────────────────────────────────────────────

/// Where a connection's attachment stands.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
    /// The anchor asked the transport to close the connection and has
    /// released the attachment; waiting for the transport's close
    /// notice, which returns the slot to `Idle`. Frames still decoded
    /// for the connection meanwhile are dropped.
    Closing = 7,
    /// The bound worker was declared lost; a replacement is adopting
    /// the session from its last acknowledged checkpoint.
    AdoptWait = 8,
}

/// CONNECT was decoded and handed to a worker for admission.
pub const FLAG_CONNECT_ADMITTED: u8 = 1 << 0;
/// An MQTT 5 AUTH exchange is in progress on the connection.
pub const FLAG_AUTH_EXCHANGE: u8 = 1 << 1;
/// The bound worker has been asked to drain.
pub const FLAG_DRAINING: u8 = 1 << 2;
/// The anchor is answering liveness alone: no worker is authoritative.
pub const FLAG_LIVENESS_ONLY: u8 = 1 << 3;
/// A PINGREQ arrived while no worker was authoritative and policy does
/// not admit liveness-only mode: PINGRESP is owed when one is.
pub const FLAG_PING_OWED: u8 = 1 << 4;
/// A frame that would have to be replayed after a worker loss could not
/// be retained: the session cannot be adopted exactly, so a loss closes
/// it instead of resuming it.
pub const FLAG_REPLAY_GAP: u8 = 1 << 5;
/// The standby holds an acknowledged checkpoint of the session, at
/// `checkpoint_delivery`: a loss of the bound worker can be adopted.
pub const FLAG_CKPT_VALID: u8 = 1 << 6;

/// Everything the anchor keeps for one client connection.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AnchorConn {
    pub session_id: [u8; SESSION_ID_LEN],
    pub epoch: u32,
    /// Tenant the listener resolved, and the listener that accepted.
    pub tenant: u32,
    pub listener: u16,
    pub phase: Phase,
    /// Bound worker slot.
    pub worker: u8,
    /// MQTT protocol level from CONNECT (3, 4 or 5; 0 until CONNECT).
    pub mqtt_version: u8,
    /// `FLAG_*`.
    pub flags: u8,
    /// A post-swap DETACH to the exporting worker is outstanding.
    pub detach_old: u8,
    /// A control write (ATTACH, RESUME, or in `Closing` the close
    /// request) could not be made; retried each step.
    pub resume_pending: u8,
    /// Negotiated keep-alive, seconds (0 = none).
    pub keepalive_s: u16,
    /// `fnv1a_64(client_id)` — the worker's session identity.
    pub client_hash: u64,
    /// Envelope bytes forwarded to / relayed from the bound worker.
    pub forwarded: u64,
    pub relayed: u64,
    /// When the current handoff started.
    pub started_ms: u64,
    /// Last inbound frame from the client, and last outbound frame the
    /// transport accepted — both "safe" activity: neither acknowledges
    /// application work the worker has not.
    pub last_in_ms: u64,
    pub last_out_ms: u64,
    /// The id the next frame taken from the client will carry.
    pub next_delivery: u64,
    /// The id of the next worker response the transport accepts.
    pub next_production: u64,
    /// Highest delivery id a checkpoint the standby holds accounts for.
    pub checkpoint_delivery: u64,
    /// The highest delivery id that needed retaining for replay and
    /// could not be (0 = none). Only a checkpoint at or past it covers
    /// every frame the replay window is missing.
    pub replay_gap_hw: u64,
    /// While non-zero, the replacement worker is being replayed the
    /// retained frames from this delivery id on, and new frames queue
    /// behind them in the hold.
    pub replay_from: u64,
}

impl AnchorConn {
    pub const fn zero() -> Self {
        Self {
            session_id: [0; SESSION_ID_LEN],
            epoch: 0,
            tenant: 0,
            listener: 0,
            phase: Phase::Idle,
            worker: 0,
            mqtt_version: 0,
            flags: 0,
            detach_old: 0,
            resume_pending: 0,
            keepalive_s: 0,
            client_hash: 0,
            forwarded: 0,
            relayed: 0,
            started_ms: 0,
            last_in_ms: 0,
            last_out_ms: 0,
            next_delivery: 1,
            next_production: 1,
            checkpoint_delivery: 0,
            replay_gap_hw: 0,
            replay_from: 0,
        }
    }

    /// True while a worker is authoritative for the session.
    pub fn worker_live(&self) -> bool {
        self.phase == Phase::Active
    }

    pub fn has(&self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    pub fn set(&mut self, flag: u8, on: bool) {
        if on {
            self.flags |= flag;
        } else {
            self.flags &= !flag;
        }
    }

    /// The delivery id the next frame would take. Committed with
    /// [`AnchorConn::commit_delivery`] only once the frame is written or
    /// held, so a refused write retried later keeps its id.
    pub fn peek_delivery(&self) -> u64 {
        self.next_delivery
    }

    pub fn commit_delivery(&mut self) {
        self.next_delivery = self.next_delivery.wrapping_add(1).max(1);
    }

    /// The transport accepted the response under `next_production`.
    pub fn accept_production(&mut self, now: u64) {
        self.next_production = self.next_production.wrapping_add(1).max(1);
        self.last_out_ms = now;
    }

    /// A frame that needed retaining for replay could not be. The gap
    /// is remembered by its highest id: a checkpoint below it still
    /// leaves a frame the replacement could never be replayed.
    pub fn note_replay_gap(&mut self, id: u64) {
        self.replay_gap_hw = self.replay_gap_hw.max(id);
        self.set(FLAG_REPLAY_GAP, true);
    }

    /// The standby acknowledged a checkpoint at delivery high-water `hw`:
    /// replay may start after it, and a gap it covers entirely is closed.
    pub fn checkpoint_acked(&mut self, hw: u64) {
        if hw < self.checkpoint_delivery && self.has(FLAG_CKPT_VALID) {
            return;
        }
        self.checkpoint_delivery = hw;
        self.set(FLAG_CKPT_VALID, true);
        if self.replay_gap_hw != 0 && hw >= self.replay_gap_hw {
            self.replay_gap_hw = 0;
            self.set(FLAG_REPLAY_GAP, false);
        }
    }

    /// The session moved by a planned handoff: the standby's checkpoint
    /// (if any) is of the worker the session just left, so a loss of the
    /// new one cannot be adopted until it checkpoints.
    pub fn checkpoint_reset(&mut self) {
        self.checkpoint_delivery = 0;
        self.replay_gap_hw = 0;
        self.set(FLAG_CKPT_VALID, false);
        self.set(FLAG_REPLAY_GAP, false);
    }

    /// The client's CONNECT: version, keep-alive and identity. The
    /// tenant and listener are the listener's, set on the record before.
    pub fn on_connect(&mut self, version: u8, keepalive_s: u16, client_id: &[u8]) {
        self.mqtt_version = version;
        self.keepalive_s = keepalive_s;
        self.client_hash = fnv1a_64(client_id);
        self.set(FLAG_CONNECT_ADMITTED, true);
    }
}

/// FNV-1a 64 — the same hash `session_processor` keys a session by.
pub fn fnv1a_64(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

// ── Liveness ────────────────────────────────────────────────────────────────

/// What the anchor does with a syntactically valid PINGREQ.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PingAction {
    /// A worker is authoritative: answer PINGRESP and forward a liveness
    /// mark, so the worker's keep-alive sweep sees the client alive.
    AnswerAndMark,
    /// No worker is authoritative and the route admits liveness-only
    /// mode: answer PINGRESP and nothing else. A PINGRESP acknowledges
    /// no application work, which is why this one frame is safe.
    Answer,
    /// No worker is authoritative and the route does not admit
    /// liveness-only mode: owe the PINGRESP until one is.
    Defer,
}

pub fn ping_action(c: &AnchorConn, liveness_only: bool) -> PingAction {
    if c.worker_live() {
        PingAction::AnswerAndMark
    } else if liveness_only {
        PingAction::Answer
    } else {
        PingAction::Defer
    }
}

/// MQTT's keep-alive budget: one and a half keep-alive periods with no
/// control packet from the client (3.1.1 §3.1.2.10, 5.0 §3.1.2.10).
/// 0 means the client asked for no keep-alive.
pub fn keepalive_budget_ms(keepalive_s: u16) -> u64 {
    u64::from(keepalive_s) * 1500
}

/// True when the client has been silent past its keep-alive budget.
pub fn keepalive_expired(c: &AnchorConn, now: u64) -> bool {
    let budget = keepalive_budget_ms(c.keepalive_s);
    budget != 0 && c.last_in_ms != 0 && now.saturating_sub(c.last_in_ms) > budget
}

/// True when the envelope payload (`[conn][proto][pkt][flags]...`) is
/// application work only a worker may acknowledge — the frames the
/// anchor holds rather than answers while no worker is authoritative.
pub fn is_application_work(payload: &[u8]) -> bool {
    if payload.len() < 4 || payload[2] != ENV_PROTO_MQTT {
        return payload.len() >= 3;
    }
    matches!(
        payload[3],
        MQTT_PUBLISH | MQTT_SUBSCRIBE | MQTT_UNSUBSCRIBE | MQTT_AUTH
    )
}

/// True when a frame must be retained for replay to a replacement
/// worker until a checkpoint accounts for it: MQTT work whose loss the
/// client would observe. QoS 0 PUBLISH is at-most-once and is not
/// retained (a replay could deliver it twice); liveness marks and
/// non-MQTT envelopes are not either.
pub fn needs_replay(payload: &[u8]) -> bool {
    if payload.len() < 5 || payload[2] != ENV_PROTO_MQTT {
        return false;
    }
    let pkt = payload[3];
    let qos = (payload[4] >> 1) & 0x03;
    match pkt {
        MQTT_PINGREQ | MQTT_CONNECT => false,
        MQTT_PUBLISH => qos > 0,
        _ => true,
    }
}

/// True when the anchor should stop reading from the transport: the
/// hold has reached `pct` percent of its capacity. The stream contract
/// has no receive-window verb, so not reading IS the backpressure — the
/// transport's own buffers fill and the client's TCP window closes —
/// and it starts before the hold is full so an overflow is never the
/// first signal. `pct == 0` disables it.
pub fn read_pause(hold_len: usize, cap: usize, pct: u8) -> bool {
    pct != 0 && cap != 0 && hold_len * 100 >= cap * usize::from(pct.min(100))
}

// ── Framed FIFO (the hold and the replay window) ────────────────────────────

/// Entry header: `[conn:2 LE][msg_type:1][len:2 LE][delivery_id:8 LE]`.
pub const FIFO_HDR: usize = 13;

/// One entry, as offsets into the FIFO buffer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FifoEntry {
    pub conn: u16,
    pub msg_type: u8,
    pub delivery_id: u64,
    /// Payload bytes are `buf[payload_at..payload_at + payload_len]`.
    pub payload_at: usize,
    pub payload_len: usize,
    /// Offset of the next entry.
    pub next: usize,
}

/// The entry at offset `at` of `buf[..len]`, or `None` past the end or
/// on a torn entry.
pub fn fifo_entry(buf: &[u8], len: usize, at: usize) -> Option<FifoEntry> {
    if at + FIFO_HDR > len || len > buf.len() {
        return None;
    }
    let plen = usize::from(u16::from_le_bytes([buf[at + 3], buf[at + 4]]));
    let next = at + FIFO_HDR + plen;
    if next > len {
        return None;
    }
    let mut id = [0u8; 8];
    id.copy_from_slice(&buf[at + 5..at + 13]);
    Some(FifoEntry {
        conn: u16::from_le_bytes([buf[at], buf[at + 1]]),
        msg_type: buf[at + 2],
        delivery_id: u64::from_le_bytes(id),
        payload_at: at + FIFO_HDR,
        payload_len: plen,
        next,
    })
}

/// Append an entry. `false` when it does not fit `cap` (or the buffer),
/// in which case nothing is written.
pub fn fifo_push(
    buf: &mut [u8],
    len: &mut usize,
    cap: usize,
    conn: u16,
    msg_type: u8,
    delivery_id: u64,
    payload: &[u8],
) -> bool {
    let need = FIFO_HDR + payload.len();
    let cap = cap.min(buf.len());
    if payload.len() > usize::from(u16::MAX) || *len + need > cap {
        return false;
    }
    let at = *len;
    buf[at..at + 2].copy_from_slice(&conn.to_le_bytes());
    buf[at + 2] = msg_type;
    buf[at + 3..at + 5].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    buf[at + 5..at + 13].copy_from_slice(&delivery_id.to_le_bytes());
    buf[at + FIFO_HDR..at + need].copy_from_slice(payload);
    *len += need;
    true
}

/// Move `n` bytes from `from` to `to` (`to <= from`) without
/// `copy_within`.
fn shift_down(buf: &mut [u8], to: usize, from: usize, n: usize) {
    if to == from {
        return;
    }
    let mut k = 0usize;
    while k < n {
        buf[to + k] = buf[from + k];
        k += 1;
    }
}

/// What a drain visitor decided for one entry.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Visit {
    /// Leave it in place and keep going.
    Keep,
    /// It has been dealt with: remove it.
    Release,
    /// Leave it, and every entry after it, unvisited — the order across
    /// the FIFO must not be broken by releasing a later entry first.
    Stop,
}

/// Walk the FIFO in order, letting `f` keep or release each entry, and
/// compact what is kept. Returns the entries released.
pub fn fifo_drain(
    buf: &mut [u8],
    len: &mut usize,
    mut f: impl FnMut(&FifoEntry, &[u8]) -> Visit,
) -> u32 {
    let total = *len;
    let mut rd = 0usize;
    let mut wr = 0usize;
    let mut stopped = false;
    let mut released = 0u32;
    while let Some(e) = fifo_entry(buf, total, rd) {
        let entry = e.next - rd;
        let keep = if stopped {
            true
        } else {
            match f(&e, &buf[e.payload_at..e.payload_at + e.payload_len]) {
                Visit::Keep => true,
                Visit::Release => false,
                Visit::Stop => {
                    stopped = true;
                    true
                }
            }
        };
        if keep {
            shift_down(buf, wr, rd, entry);
            wr += entry;
        } else {
            released += 1;
        }
        rd = e.next;
    }
    *len = wr;
    released
}

/// Remove every entry `pred` names, keeping the order of the rest.
/// Returns the entries removed.
pub fn fifo_remove(buf: &mut [u8], len: &mut usize, mut pred: impl FnMut(u16, u64) -> bool) -> u32 {
    fifo_drain(buf, len, |e, _| {
        if pred(e.conn, e.delivery_id) {
            Visit::Release
        } else {
            Visit::Keep
        }
    })
}

/// Entries of `conn` in the FIFO.
pub fn fifo_count(buf: &[u8], len: usize, conn: u16) -> u32 {
    let mut at = 0usize;
    let mut n = 0u32;
    while let Some(e) = fifo_entry(buf, len, at) {
        if e.conn == conn {
            n += 1;
        }
        at = e.next;
    }
    n
}

// ── Worker loss ─────────────────────────────────────────────────────────────

/// True when a worker that last answered a liveness probe at
/// `last_ack_ms` is declared lost at `now`. Silence means something
/// only while probes are sent: `probe_ms == 0` or `loss_ms == 0`
/// disables detection (only an operator command declares a loss).
pub fn worker_lost(last_ack_ms: u64, now: u64, probe_ms: u32, loss_ms: u32) -> bool {
    probe_ms != 0 && loss_ms != 0 && now.saturating_sub(last_ack_ms) > u64::from(loss_ms)
}

/// What a worker loss does to one connection.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LossAction {
    /// Not bound to the lost worker, and not mid-handoff with it.
    Unaffected,
    /// Bound to the lost worker with an acknowledged checkpoint and no
    /// replay gap: the standby adopts it, the connection stays open.
    Adopt,
    /// Bound to the lost worker and not adoptable exactly (no checkpoint
    /// yet, or a frame that could not be retained): close it, honestly,
    /// so the client reconnects and resumes from committed state.
    Close,
    /// Mid-handoff toward the lost worker: refuse the handoff, the
    /// session stays on the worker it is bound to.
    RefuseHandoff,
}

pub fn loss_action(c: &AnchorConn, lost: u8, standby_ok: bool) -> LossAction {
    match c.phase {
        Phase::Idle | Phase::Closing | Phase::AdoptWait => LossAction::Unaffected,
        _ if c.worker != lost => {
            if matches!(
                c.phase,
                Phase::DrainWait | Phase::ImportWait | Phase::ResumeWait
            ) {
                LossAction::RefuseHandoff
            } else {
                LossAction::Unaffected
            }
        }
        _ if standby_ok && c.has(FLAG_CKPT_VALID) && !c.has(FLAG_REPLAY_GAP) => LossAction::Adopt,
        _ => LossAction::Close,
    }
}

// ── Operator swap command ───────────────────────────────────────────────────

/// `MSG_ANCHOR_SWAP` mode: move the sessions to the standby through the
/// planned export/import handoff.
pub const SWAP_PLANNED: u8 = 0;
/// `MSG_ANCHOR_SWAP` mode: declare the default worker lost and adopt its
/// sessions on the standby from their last checkpoint.
pub const SWAP_WORKER_LOST: u8 = 1;
/// Payload bytes of `MSG_ANCHOR_SWAP`: `[anchor_id:8][mode:1][seq:u64 LE]`.
pub const SWAP_CMD_LEN: usize = 8 + 1 + 8;

/// A decoded swap command.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SwapCmd {
    pub anchor_id: [u8; 8],
    pub mode: u8,
    pub seq: u64,
}

pub fn decode_swap(p: &[u8]) -> Option<SwapCmd> {
    if p.len() < SWAP_CMD_LEN {
        return None;
    }
    let mut id = [0u8; 8];
    id.copy_from_slice(&p[..8]);
    let mut seq = [0u8; 8];
    seq.copy_from_slice(&p[9..17]);
    Some(SwapCmd {
        anchor_id: id,
        mode: p[8],
        seq: u64::from_le_bytes(seq),
    })
}

pub fn encode_swap(out: &mut [u8], cmd: &SwapCmd) -> usize {
    if out.len() < SWAP_CMD_LEN {
        return 0;
    }
    out[..8].copy_from_slice(&cmd.anchor_id);
    out[8] = cmd.mode;
    out[9..17].copy_from_slice(&cmd.seq.to_le_bytes());
    SWAP_CMD_LEN
}

/// The sequence a worker stamps on a swap record it forwards: the
/// record's place in the log, `(partition << 48) | raft index`.
pub fn swap_seq(partition: u16, index: u64) -> u64 {
    (u64::from(partition) << 48) | (index & ((1u64 << 48) - 1))
}

/// Partitions whose swap sequences an anchor tracks at once: one
/// consensus engine's hosted groups.
pub const SWAP_SEQ_SLOTS: usize = 64;

/// The swap sequences already executed, per partition.
///
/// Raft indices order records within a partition and say nothing across
/// partitions, so the high-water is kept per partition: a single one
/// would refuse every later command committed on a partition whose
/// indices happen to sort below another's. A partition maps to slot
/// `partition % SWAP_SEQ_SLOTS`, which is exact for any run of
/// `SWAP_SEQ_SLOTS` consecutive partition ids; a slot taken over by
/// another partition forgets the first one's high-water, so a copy of
/// one of its records arriving after that executes again.
#[derive(Clone, Copy)]
pub struct SwapSeqs {
    seen: [u64; SWAP_SEQ_SLOTS],
}

impl SwapSeqs {
    pub const fn new() -> Self {
        Self {
            seen: [0; SWAP_SEQ_SLOTS],
        }
    }

    /// Whether this anchor executes `cmd`, recording it if so: it names
    /// this anchor (or all of them, with an all-zero id), and its
    /// sequence is above the last one executed on its partition. A
    /// command carried through the log reaches the anchor once per
    /// worker that forwards it, so a sequence already executed is the
    /// same command again. Sequence 0 is unsequenced and always
    /// executes.
    pub fn admit(&mut self, cmd: &SwapCmd, my_id: &[u8; 8]) -> bool {
        let named = cmd.anchor_id == *my_id || cmd.anchor_id == [0u8; 8];
        if !named {
            return false;
        }
        if cmd.seq == 0 {
            return true;
        }
        let partition = cmd.seq >> 48;
        let slot = (partition as usize) % SWAP_SEQ_SLOTS;
        let last = self.seen[slot];
        if last != 0 && last >> 48 == partition && cmd.seq <= last {
            return false;
        }
        self.seen[slot] = cmd.seq;
        true
    }
}

impl Default for SwapSeqs {
    fn default() -> Self {
        Self::new()
    }
}
