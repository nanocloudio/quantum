// Bounded, no_std, no-alloc SESSION REPLAY core — what makes a session
// worker's replacement resume EXACTLY where the committed log and the
// anchor left off. `include!`d by the host test crate and by
// `session_processor`.
//
// Three pieces, all pure:
//
// ## The handoff tail (QSR2)
//
// A handoff record states WHERE its state sits: the per-partition apply
// cursor the exporting worker had reached, and the highest anchor
// delivery id the session consumed. Layout, appended to the QSR record:
//
//   [delivery_hw:u64 LE][count:u8] then count × [partition:u16 LE][index:u64 LE]
//
// ## The import cursor rule
//
// The importer consumes the same committed stream, but while the session
// was the other worker's it SKIPPED the session's entries. So two things
// can be wrong with a record's cursor E against the importer's own apply
// position I:
//
//   - The importer skipped an entry for the session past E: the record
//     does not contain it, and the importer will never see it again.
//     The import is refused `STATUS_CURSOR_MISMATCH` (`Mismatch`).
//   - The importer is BEHIND E on some partition: entries up to E are
//     already folded into the record, and applying them again once the
//     session is the importer's would apply them twice (a CONNECT bumping
//     the generation twice, say). The import is admitted with a replay
//     FLOOR: the importer skips the session's entries at or below E until
//     its own cursor passes E (`AdmitWithFloor`).
//
// Skipped entries are tracked per slot as the most recent one, which is
// exact for a session whose entries ride one partition (every session-
// keyed op rides its session shard's partition) and checks only the
// latest skip otherwise.
//
// ## The outbound delivery ledger
//
// What a subscriber is owed is in the log, not only in the memory of the
// worker that sent it. `QOP_DELIVER` logs a QoS 1/2 delivery (session,
// generation, packet id, QoS and the encoded PUBLISH body) and
// `QOP_DELIVER_ACK` its progress: `ACK_RELEASED` when a QoS 2 subscriber's
// PUBREC arrived and the PUBREL went out, `ACK_COMPLETE` at PUBACK or
// PUBCOMP. Every worker applies both into this ledger whatever slot they
// name — the ledger has no side effects, so applying it everywhere is
// safe. A replacement, or a restarted node replaying its WAL, rebuilds
// from the log alone what a session is still owed and resends it in the
// order it was first sent: a PUBLISH with DUP, or the PUBREL of a QoS 2
// delivery already released.

/// The handoff tail's fixed part: delivery high-water and cursor count.
pub const TAIL_FIXED: usize = 8 + 1;
/// One cursor entry: partition and index.
pub const TAIL_CURSOR: usize = 2 + 8;
/// Cursors a tail can carry: one per hosted raft partition.
pub const TAIL_MAX_CURSORS: usize = 64;
/// Largest tail.
pub const TAIL_MAX: usize = TAIL_FIXED + TAIL_MAX_CURSORS * TAIL_CURSOR;

/// Where a handoff record sits in the committed log and the anchor seam.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HandoffTail {
    pub delivery_hw: u64,
    pub count: u8,
    pub cursors: [(u16, u64); TAIL_MAX_CURSORS],
}

impl HandoffTail {
    pub const fn empty() -> Self {
        Self {
            delivery_hw: 0,
            count: 0,
            cursors: [(0, 0); TAIL_MAX_CURSORS],
        }
    }

    /// Build from a dense per-partition cursor array (index = partition),
    /// carrying only the partitions that have applied anything.
    pub fn from_apply(delivery_hw: u64, apply: &[u64]) -> Self {
        let mut t = Self::empty();
        t.delivery_hw = delivery_hw;
        let mut p = 0usize;
        while p < apply.len() && (t.count as usize) < TAIL_MAX_CURSORS {
            if apply[p] != 0 {
                t.cursors[t.count as usize] = (p as u16, apply[p]);
                t.count += 1;
            }
            p += 1;
        }
        t
    }

    /// The record's cursor on `partition` (0 = it had applied nothing
    /// there).
    pub fn cursor(&self, partition: u16) -> u64 {
        let mut i = 0usize;
        while i < self.count as usize {
            if self.cursors[i].0 == partition {
                return self.cursors[i].1;
            }
            i += 1;
        }
        0
    }

    pub fn encoded_len(&self) -> usize {
        TAIL_FIXED + self.count as usize * TAIL_CURSOR
    }

    /// Write the tail into `out`. Returns the bytes written, or 0 when
    /// `out` is too short.
    pub fn encode(&self, out: &mut [u8]) -> usize {
        let n = self.encoded_len();
        if out.len() < n {
            return 0;
        }
        out[..8].copy_from_slice(&self.delivery_hw.to_le_bytes());
        out[8] = self.count;
        let mut at = TAIL_FIXED;
        let mut i = 0usize;
        while i < self.count as usize {
            out[at..at + 2].copy_from_slice(&self.cursors[i].0.to_le_bytes());
            out[at + 2..at + 10].copy_from_slice(&self.cursors[i].1.to_le_bytes());
            at += TAIL_CURSOR;
            i += 1;
        }
        n
    }

    /// Read a tail from `b`, or `None` when it is short or names more
    /// cursors than a tail can carry.
    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < TAIL_FIXED {
            return None;
        }
        let mut hw = [0u8; 8];
        hw.copy_from_slice(&b[..8]);
        let count = b[8];
        if count as usize > TAIL_MAX_CURSORS || b.len() < TAIL_FIXED + count as usize * TAIL_CURSOR
        {
            return None;
        }
        let mut t = Self::empty();
        t.delivery_hw = u64::from_le_bytes(hw);
        t.count = count;
        let mut at = TAIL_FIXED;
        let mut i = 0usize;
        while i < count as usize {
            let p = u16::from_le_bytes([b[at], b[at + 1]]);
            let mut ix = [0u8; 8];
            ix.copy_from_slice(&b[at + 2..at + 10]);
            t.cursors[i] = (p, u64::from_le_bytes(ix));
            at += TAIL_CURSOR;
            i += 1;
        }
        Some(t)
    }
}

/// What an importer does with a record, by its cursor.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CursorVerdict {
    /// The importer has applied at least as far as the record on every
    /// partition and skipped nothing of the session past it.
    Admit,
    /// The importer is behind the record somewhere: admit, and skip the
    /// session's entries at or below the record's cursor until caught up.
    AdmitWithFloor,
    /// The importer skipped an entry of the session that the record does
    /// not contain: refuse (`STATUS_CURSOR_MISMATCH`).
    Mismatch,
}

/// Decide an import. `apply[p]` is the importer's last applied index on
/// partition `p`; `last_skip` the most recent entry of this session's
/// slot the importer skipped as not its own, `(partition, index)`, or
/// `None`.
pub fn cursor_verdict(
    tail: &HandoffTail,
    apply: &[u64],
    last_skip: Option<(u16, u64)>,
) -> CursorVerdict {
    if let Some((p, ix)) = last_skip {
        if ix > tail.cursor(p) {
            return CursorVerdict::Mismatch;
        }
    }
    let mut i = 0usize;
    while i < tail.count as usize {
        let (p, ix) = tail.cursors[i];
        let mine = apply.get(usize::from(p)).copied().unwrap_or(0);
        if mine < ix {
            return CursorVerdict::AdmitWithFloor;
        }
        i += 1;
    }
    CursorVerdict::Admit
}

/// True when an entry at `(partition, index)` for a session imported
/// under `tail` is already folded into the record and must be skipped.
pub fn below_floor(tail: &HandoffTail, partition: u16, index: u64) -> bool {
    index <= tail.cursor(partition)
}

/// True when the importer has caught up with every cursor of `tail`, so
/// the floor no longer filters anything and can be released.
pub fn floor_passed(tail: &HandoffTail, apply: &[u64]) -> bool {
    let mut i = 0usize;
    while i < tail.count as usize {
        let (p, ix) = tail.cursors[i];
        if apply.get(usize::from(p)).copied().unwrap_or(0) < ix {
            return false;
        }
        i += 1;
    }
    true
}

// ── QOP_DELIVER / QOP_DELIVER_ACK op bodies ─────────────────────────────────

/// `QOP_DELIVER` body before the PUBLISH body:
/// `[stream_hash:u64 LE][generation:u32 LE][packet_id:u16 BE][qos:u8]
///  [body_len:u16 LE]`.
pub const DELIVER_FIXED: usize = 8 + 4 + 2 + 1 + 2;
/// `QOP_DELIVER_ACK` body: `[stream_hash:u64 LE][generation:u32 LE]
/// [packet_id:u16 BE][stage:u8]`.
pub const DELIVER_ACK_LEN: usize = 8 + 4 + 2 + 1;
/// `QOP_DELIVER_ACK` stage: a QoS 2 subscriber's PUBREC arrived and the
/// PUBREL went out. What the session is still owed is that PUBREL.
pub const ACK_RELEASED: u8 = 1;
/// `QOP_DELIVER_ACK` stage: the delivery is complete (PUBACK for QoS 1,
/// PUBCOMP for QoS 2).
pub const ACK_COMPLETE: u8 = 2;
/// Largest PUBLISH body a ledger entry holds. A delivery past it is not
/// logged (and is counted by the caller), so it is not reconstructible —
/// the honest bound of a ledger that keeps bodies inline.
pub const DELIVER_BODY_MAX: usize = 1920;

/// A decoded `QOP_DELIVER`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Deliver<'a> {
    pub stream_hash: u64,
    pub generation: u32,
    pub packet_id: u16,
    pub qos: u8,
    pub body: &'a [u8],
}

pub fn encode_deliver(out: &mut [u8], d: &Deliver<'_>) -> usize {
    let n = DELIVER_FIXED + d.body.len();
    if d.body.len() > DELIVER_BODY_MAX || out.len() < n {
        return 0;
    }
    out[..8].copy_from_slice(&d.stream_hash.to_le_bytes());
    out[8..12].copy_from_slice(&d.generation.to_le_bytes());
    out[12..14].copy_from_slice(&d.packet_id.to_be_bytes());
    out[14] = d.qos;
    out[15..17].copy_from_slice(&(d.body.len() as u16).to_le_bytes());
    out[DELIVER_FIXED..n].copy_from_slice(d.body);
    n
}

pub fn decode_deliver(b: &[u8]) -> Option<Deliver<'_>> {
    if b.len() < DELIVER_FIXED {
        return None;
    }
    let mut h = [0u8; 8];
    h.copy_from_slice(&b[..8]);
    let len = usize::from(u16::from_le_bytes([b[15], b[16]]));
    if len > DELIVER_BODY_MAX || b.len() < DELIVER_FIXED + len {
        return None;
    }
    Some(Deliver {
        stream_hash: u64::from_le_bytes(h),
        generation: u32::from_le_bytes([b[8], b[9], b[10], b[11]]),
        packet_id: u16::from_be_bytes([b[12], b[13]]),
        qos: b[14],
        body: &b[DELIVER_FIXED..DELIVER_FIXED + len],
    })
}

/// A decoded `QOP_DELIVER_ACK`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DeliverAck {
    pub stream_hash: u64,
    pub generation: u32,
    pub packet_id: u16,
    /// `ACK_RELEASED` or `ACK_COMPLETE`.
    pub stage: u8,
}

pub fn encode_deliver_ack(out: &mut [u8], a: &DeliverAck) -> usize {
    if out.len() < DELIVER_ACK_LEN {
        return 0;
    }
    out[..8].copy_from_slice(&a.stream_hash.to_le_bytes());
    out[8..12].copy_from_slice(&a.generation.to_le_bytes());
    out[12..14].copy_from_slice(&a.packet_id.to_be_bytes());
    out[14] = a.stage;
    DELIVER_ACK_LEN
}

/// Read a `QOP_DELIVER_ACK`, or `None` when it is short or names a stage
/// that is not one.
pub fn decode_deliver_ack(b: &[u8]) -> Option<DeliverAck> {
    if b.len() < DELIVER_ACK_LEN || !matches!(b[14], ACK_RELEASED | ACK_COMPLETE) {
        return None;
    }
    let mut h = [0u8; 8];
    h.copy_from_slice(&b[..8]);
    Some(DeliverAck {
        stream_hash: u64::from_le_bytes(h),
        generation: u32::from_le_bytes([b[8], b[9], b[10], b[11]]),
        packet_id: u16::from_be_bytes([b[12], b[13]]),
        stage: b[14],
    })
}

// ── The ledger ──────────────────────────────────────────────────────────────

/// Owed deliveries a worker's ledger holds at once, across sessions.
pub const LEDGER_SLOTS: usize = 256;

#[derive(Clone, Copy)]
pub struct LedgerEntry {
    pub tenant: u32,
    pub stream_hash: u64,
    pub generation: u32,
    /// Order the delivery was logged in: redelivery follows it, because
    /// MQTT requires a resent PUBLISH to keep its original order.
    pub seq: u64,
    pub packet_id: u16,
    pub qos: u8,
    pub live: u8,
    /// 1 once a QoS 2 delivery's PUBREL went out: what is owed is the
    /// PUBREL, never the PUBLISH again.
    pub released: u8,
    pub body_len: u16,
    pub body: [u8; DELIVER_BODY_MAX],
}

impl LedgerEntry {
    pub const fn zero() -> Self {
        Self {
            tenant: 0,
            stream_hash: 0,
            generation: 0,
            seq: 0,
            packet_id: 0,
            qos: 0,
            live: 0,
            released: 0,
            body_len: 0,
            body: [0; DELIVER_BODY_MAX],
        }
    }

    pub fn body(&self) -> &[u8] {
        &self.body[..usize::from(self.body_len).min(DELIVER_BODY_MAX)]
    }
}

/// Deliveries the log says are owed an acknowledgement.
pub struct DeliveryLedger {
    pub entries: [LedgerEntry; LEDGER_SLOTS],
    /// Deliveries refused for want of a free entry. Non-zero means some
    /// owed delivery is not reconstructible; it is counted, never
    /// silently evicted (evicting would forget an owed delivery while
    /// claiming to hold them all).
    pub refused: u32,
    /// The `seq` the next recorded delivery takes.
    next_seq: u64,
}

impl DeliveryLedger {
    pub const fn new() -> Self {
        Self {
            entries: [LedgerEntry::zero(); LEDGER_SLOTS],
            refused: 0,
            next_seq: 1,
        }
    }

    fn find(&self, tenant: u32, stream: u64, pid: u16) -> Option<usize> {
        let mut i = 0usize;
        while i < LEDGER_SLOTS {
            let e = &self.entries[i];
            if e.live != 0 && e.tenant == tenant && e.stream_hash == stream && e.packet_id == pid {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    /// Apply a committed `QOP_DELIVER`. A delivery under a packet id the
    /// session already owes replaces it (the id was reused after its
    /// acknowledgement applied, or this is a replay of the same entry).
    pub fn record(&mut self, tenant: u32, d: &Deliver<'_>) -> bool {
        if d.body.len() > DELIVER_BODY_MAX {
            self.refused = self.refused.wrapping_add(1);
            return false;
        }
        let at = match self.find(tenant, d.stream_hash, d.packet_id) {
            Some(i) => i,
            None => match self.entries.iter().position(|e| e.live == 0) {
                Some(i) => i,
                None => {
                    self.refused = self.refused.wrapping_add(1);
                    return false;
                }
            },
        };
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let e = &mut self.entries[at];
        e.tenant = tenant;
        e.stream_hash = d.stream_hash;
        e.generation = d.generation;
        e.seq = seq;
        e.packet_id = d.packet_id;
        e.qos = d.qos;
        e.live = 1;
        e.released = 0;
        e.body_len = d.body.len() as u16;
        e.body[..d.body.len()].copy_from_slice(d.body);
        true
    }

    /// Apply a committed `QOP_DELIVER_ACK` to the delivery under its
    /// packet id: `ACK_RELEASED` marks a QoS 2 delivery released,
    /// `ACK_COMPLETE` ends it. An acknowledgement applies to a delivery
    /// logged at its own generation or an earlier one — a delivery owed
    /// across a resume is redelivered under the same packet id, and the
    /// client's acknowledgement then carries the resumed generation — but
    /// never to one logged at a LATER generation, which is a new delivery
    /// that reused the id.
    pub fn ack(&mut self, tenant: u32, a: &DeliverAck) -> bool {
        let Some(i) = self.find(tenant, a.stream_hash, a.packet_id) else {
            return false;
        };
        let e = &mut self.entries[i];
        if e.generation > a.generation {
            return false;
        }
        match a.stage {
            ACK_RELEASED if e.qos == 2 => e.released = 1,
            ACK_COMPLETE => e.live = 0,
            _ => return false,
        }
        true
    }

    /// Forget every delivery the session owes: a clean start, or the
    /// session's expiry.
    pub fn drop_stream(&mut self, tenant: u32, stream: u64) -> u32 {
        let mut n = 0u32;
        for e in self.entries.iter_mut() {
            if e.live != 0 && e.tenant == tenant && e.stream_hash == stream {
                e.live = 0;
                n += 1;
            }
        }
        n
    }

    /// Indices of the deliveries `(tenant, stream)` owes, oldest first,
    /// into `out`; when the session owes more than `out` holds, the
    /// oldest `out.len()`. Returns how many.
    pub fn owed(&self, tenant: u32, stream: u64, out: &mut [usize]) -> usize {
        let mut n = 0usize;
        for (i, e) in self.entries.iter().enumerate() {
            if e.live == 0 || e.tenant != tenant || e.stream_hash != stream {
                continue;
            }
            // Insertion into `out[..n]` by `seq`: the list is a session's
            // in-flight window, so it is short.
            let mut at = n;
            while at > 0 && self.entries[out[at - 1]].seq > e.seq {
                at -= 1;
            }
            if at == out.len() {
                continue;
            }
            let end = if n == out.len() { n - 1 } else { n };
            let mut k = end;
            while k > at {
                out[k] = out[k - 1];
                k -= 1;
            }
            out[at] = i;
            n = (n + 1).min(out.len());
        }
        n
    }

    pub fn live(&self) -> usize {
        self.entries.iter().filter(|e| e.live != 0).count()
    }
}

impl Default for DeliveryLedger {
    fn default() -> Self {
        Self::new()
    }
}
