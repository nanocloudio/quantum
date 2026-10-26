// Bounded, no_std, no-alloc SESSION SITE core — a worker's session state
// exported to a surviving site and imported there, so a site-loss RESUME
// is the application's own resume rather than a fresh session.
// `include!`d AFTER `session_replay_core.rs` and
// `session_identity_core.rs`, in the same module: it names `HandoffTail`,
// `cursor_verdict`, `CursorVerdict`, `DELIVER_BODY_MAX` and
// `SESSION_ID_LEN` by bare name.
// The host test crate and the Wormhole site-loss harness mount it that
// way; see docs/architecture/anchor_continuity.md.
//
// `session_processor` exports it on `MSG_SITE_EXPORT` (its `site.rs`) to
// `site_out`, for the cross-site carrier; the import rule is the
// surviving site's.
//
// ## What the record is
//
// A planned handoff moves a session between two workers that read the
// same node's `topic_engine` and the same committed stream, so its QSR2
// record leaves out what the importer already has. A surviving SITE has
// neither the lost site's `topic_engine` nor a worker that held the
// session, so the site record ([`SiteSession`], `QSS1`) carries what the
// session's delivery state is made of, in the same vocabulary:
//
//   - identity, generation, slot and placement shard, protocol,
//     keep-alive, the next packet id and the receive maximum, as QSR2;
//   - the clean-start flag and the WILL (present, QoS, retain, delay,
//     topic, payload), as QSR2 carries them;
//   - SUBSCRIPTIONS — filter, QoS and an options byte per entry (0: the
//     broker applies no MQTT 5 subscription options);
//   - the QoS 1/2 IN-FLIGHT ledger, entry for entry in QSR2's 28-byte
//     layout (packet id, QoS, phase, direction, generation, wal index,
//     correlation, partition, PUBREC-sent);
//   - the OWED outbound deliveries (`QOP_DELIVER` entries not yet
//     complete: packet id, QoS, released, generation, PUBLISH body), so a
//     redelivery keeps its packet id and a released QoS 2 delivery is
//     owed its PUBREL, never a second PUBLISH;
//   - the handoff tail (`session_replay_core`): the DEDUPE cursor (the
//     delivery-id high-water the session consumed) and the per-partition
//     APPLY cursor the state reflects.
//
// It is bounded ([`SITE_RECORD_MAX`]), CRC-checked, and refused whole
// when anything in it is out of bounds — never truncated. It does not
// carry flow-control credit (prefetch and outstanding subscription
// deliveries), which the resumed session's in-flight window and owed
// deliveries re-establish, nor the activity clock, which restarts at the
// resume.
//
// ## The import rule
//
// [`site_admit`] decides at the surviving site, against the epoch the
// session directory committed for the resume and the site's own replica
// of the committed stream:
//
//   - the resume epoch must be above the record's (`STALE_EPOCH`);
//   - a site whose replica holds the session at a LATER generation saw a
//     CONNECT the record does not reflect: the record is stale. An
//     earlier generation is a site that has not yet applied the record's
//     CONNECT — behind it, so the cursor rule below gives it a floor;
//   - the apply cursor follows `session_replay_core::cursor_verdict`: an
//     entry of the session the site applied past the record's cursor and
//     the record does not contain refuses the import
//     (`CURSOR_MISMATCH`); a site behind the record imports it with a
//     replay floor, so nothing is applied twice.
//
// A refused record is a RECONNECT, honestly: the client starts the
// session again from what the surviving site's log holds.

/// Statuses (mirror session_ctrl::STATUS_*).
pub const SS_OK: u8 = 0;
pub const SS_STALE_EPOCH: u8 = 1;
pub const SS_UNKNOWN_SESSION: u8 = 2;
pub const SS_NO_CAPACITY: u8 = 3;
pub const SS_CORRUPT: u8 = 4;
pub const SS_CURSOR_MISMATCH: u8 = 7;

/// Magic of the site record.
pub const SITE_MAGIC: [u8; 4] = *b"QSS1";
/// Subscriptions a record carries.
pub const SITE_SUBS_MAX: usize = 32;
/// Bytes of one subscription filter.
pub const SITE_FILTER_MAX: usize = 256;
/// In-flight entries (`MAX_INFLIGHT_PER_SESSION`).
pub const SITE_INFLIGHT_MAX: usize = 16;
/// Owed outbound deliveries (a session's in-flight window).
pub const SITE_OWED_MAX: usize = 16;
/// Bytes of the Will topic and payload (QSR2's bounds).
pub const SITE_WILL_TOPIC_MAX: usize = 256;
pub const SITE_WILL_PAYLOAD_MAX: usize = 512;
/// `flags` bit: the session was opened with clean start.
pub const SITE_FLAG_CLEAN_START: u8 = 1 << 0;
/// One in-flight entry, QSR2's layout.
pub const SITE_INFLIGHT_LEN: usize = 2 + 1 + 1 + 1 + 4 + 8 + 8 + 2 + 1;
/// In-flight direction: the client's own PUBLISH flow (QSR2's
/// `INFLIGHT_PUB`); 1 is a delivery to it.
pub const SITE_DIR_PUBLISH: u8 = 0;
/// QoS 2 phase after PUBREC (QSR2's `QOS2_PUBREC`).
pub const SITE_QOS2_PUBREC: u8 = 1;

/// Fixed part: magic, session id, epoch, source site, export sequence,
/// tenant, stream hash, generation, slot, shard, protocol, keep-alive,
/// next packet id, expiry, receive maximum.
const SITE_FIXED: usize = 4 + 16 + 4 + 4 + 8 + 4 + 8 + 4 + 4 + 4 + 1 + 4 + 4 + 4 + 2;
const SITE_SUB_HDR: usize = 1 + 1 + 2;
/// Flags, then the Will: present, QoS, retain, delay, and the topic and
/// payload lengths.
const SITE_WILL_HDR: usize = 1 + 1 + 1 + 1 + 4 + 2 + 2;
const SITE_OWED_HDR: usize = 2 + 1 + 1 + 4 + 2;
/// Largest site record.
pub const SITE_RECORD_MAX: usize = SITE_FIXED
    + SITE_WILL_HDR
    + SITE_WILL_TOPIC_MAX
    + SITE_WILL_PAYLOAD_MAX
    + 1
    + SITE_SUBS_MAX * (SITE_SUB_HDR + SITE_FILTER_MAX)
    + 1
    + SITE_INFLIGHT_MAX * SITE_INFLIGHT_LEN
    + 1
    + SITE_OWED_MAX * (SITE_OWED_HDR + DELIVER_BODY_MAX)
    + TAIL_MAX
    + 4;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SiteSub {
    pub qos: u8,
    /// Subscription options byte, carried as recorded. The broker applies
    /// no MQTT 5 subscription options, so the worker records 0.
    pub options: u8,
    pub len: u16,
    pub filter: [u8; SITE_FILTER_MAX],
}

impl SiteSub {
    pub const fn zero() -> Self {
        Self {
            qos: 0,
            options: 0,
            len: 0,
            filter: [0; SITE_FILTER_MAX],
        }
    }

    pub fn filter(&self) -> &[u8] {
        &self.filter[..usize::from(self.len).min(SITE_FILTER_MAX)]
    }
}

/// One QoS 1/2 in-flight flow, as QSR2 carries it.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct SiteInflight {
    pub packet_id: u16,
    pub qos: u8,
    pub phase: u8,
    pub direction: u8,
    pub generation: u32,
    pub wal_index: u64,
    pub correlation_id: u64,
    pub partition_id: u16,
    pub rec_sent: u8,
}

/// One owed outbound delivery, as the delivery ledger holds it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SiteOwed {
    pub packet_id: u16,
    pub qos: u8,
    pub released: u8,
    pub generation: u32,
    pub len: u16,
    pub body: [u8; DELIVER_BODY_MAX],
}

impl SiteOwed {
    pub const fn zero() -> Self {
        Self {
            packet_id: 0,
            qos: 0,
            released: 0,
            generation: 0,
            len: 0,
            body: [0; DELIVER_BODY_MAX],
        }
    }

    pub fn body(&self) -> &[u8] {
        &self.body[..usize::from(self.len).min(DELIVER_BODY_MAX)]
    }
}

/// A session, whole, for a surviving site.
#[derive(Clone, Copy)]
pub struct SiteSession {
    pub session_id: [u8; SESSION_ID_LEN],
    /// The SessionCtrlV1 epoch the session was bound at when exported.
    pub epoch: u32,
    /// The site that exported it.
    pub source_site: u32,
    /// Per-session export sequence: a site keeps only the newest.
    pub export_seq: u64,
    pub tenant: u32,
    pub stream_hash: u64,
    pub generation: u32,
    pub slot: u32,
    pub shard: u32,
    pub protocol: u8,
    pub keep_alive_ms: u32,
    /// The packet id the session's next delivery takes.
    pub next_msg_id: u32,
    pub session_expiry_s: u32,
    pub receive_maximum: u16,
    /// `SITE_FLAG_*`.
    pub flags: u8,
    /// The Will the session's CONNECT registered, fired by the surviving
    /// site as it would have been here.
    pub will_present: u8,
    pub will_qos: u8,
    pub will_retain: u8,
    pub will_delay_ms: u32,
    pub will_topic_len: u16,
    pub will_topic: [u8; SITE_WILL_TOPIC_MAX],
    pub will_payload_len: u16,
    pub will_payload: [u8; SITE_WILL_PAYLOAD_MAX],
    pub sub_count: u8,
    pub subs: [SiteSub; SITE_SUBS_MAX],
    pub inflight_count: u8,
    pub inflight: [SiteInflight; SITE_INFLIGHT_MAX],
    pub owed_count: u8,
    pub owed: [SiteOwed; SITE_OWED_MAX],
    /// Dedupe cursor (`delivery_hw`) and apply cursor.
    pub tail: HandoffTail,
}

impl SiteSession {
    pub const fn zero() -> Self {
        Self {
            session_id: [0; SESSION_ID_LEN],
            epoch: 0,
            source_site: 0,
            export_seq: 0,
            tenant: 0,
            stream_hash: 0,
            generation: 0,
            slot: 0,
            shard: 0,
            protocol: 0,
            keep_alive_ms: 0,
            next_msg_id: 1,
            session_expiry_s: 0,
            receive_maximum: 0,
            flags: 0,
            will_present: 0,
            will_qos: 0,
            will_retain: 0,
            will_delay_ms: 0,
            will_topic_len: 0,
            will_topic: [0; SITE_WILL_TOPIC_MAX],
            will_payload_len: 0,
            will_payload: [0; SITE_WILL_PAYLOAD_MAX],
            sub_count: 0,
            subs: [SiteSub::zero(); SITE_SUBS_MAX],
            inflight_count: 0,
            inflight: [SiteInflight {
                packet_id: 0,
                qos: 0,
                phase: 0,
                direction: 0,
                generation: 0,
                wal_index: 0,
                correlation_id: 0,
                partition_id: 0,
                rec_sent: 0,
            }; SITE_INFLIGHT_MAX],
            owed_count: 0,
            owed: [SiteOwed::zero(); SITE_OWED_MAX],
            tail: HandoffTail::empty(),
        }
    }

    /// Add a subscription. `false` when the record is full or the filter
    /// is past its bound: the export is then refused, never truncated.
    pub fn subscribe(&mut self, filter: &[u8], qos: u8, options: u8) -> bool {
        if usize::from(self.sub_count) == SITE_SUBS_MAX
            || filter.len() > SITE_FILTER_MAX
            || filter.is_empty()
        {
            return false;
        }
        let s = &mut self.subs[usize::from(self.sub_count)];
        s.qos = qos;
        s.options = options;
        s.len = filter.len() as u16;
        s.filter[..filter.len()].copy_from_slice(filter);
        self.sub_count += 1;
        true
    }

    pub fn add_inflight(&mut self, e: SiteInflight) -> bool {
        if usize::from(self.inflight_count) == SITE_INFLIGHT_MAX {
            return false;
        }
        self.inflight[usize::from(self.inflight_count)] = e;
        self.inflight_count += 1;
        true
    }

    pub fn add_owed(
        &mut self,
        packet_id: u16,
        qos: u8,
        released: u8,
        generation: u32,
        body: &[u8],
    ) -> bool {
        if usize::from(self.owed_count) == SITE_OWED_MAX || body.len() > DELIVER_BODY_MAX {
            return false;
        }
        let o = &mut self.owed[usize::from(self.owed_count)];
        o.packet_id = packet_id;
        o.qos = qos;
        o.released = released;
        o.generation = generation;
        o.len = body.len() as u16;
        o.body[..body.len()].copy_from_slice(body);
        self.owed_count += 1;
        true
    }

    /// A PUBLISH the client sends again under `packet_id` after the
    /// resume is a retry of a QoS 2 flow the session already recorded: it
    /// is answered PUBREC, never applied a second time. A recorded inbound
    /// QoS 2 flow is the PUBLISH's own, whether or not its PUBREC went
    /// out — the original was proposed, and applying the retry too would
    /// apply the message twice.
    pub fn publish_is_retry(&self, packet_id: u16) -> bool {
        let mut i = 0usize;
        while i < usize::from(self.inflight_count) {
            let e = &self.inflight[i];
            if e.packet_id == packet_id && e.direction == SITE_DIR_PUBLISH && e.qos == 2 {
                return true;
            }
            i += 1;
        }
        false
    }

    /// The bytes the record encodes to.
    pub fn encoded_len(&self) -> usize {
        let mut n = SITE_FIXED + SITE_WILL_HDR + 1 + 1 + 1 + 4;
        n += usize::from(self.will_topic_len).min(SITE_WILL_TOPIC_MAX);
        n += usize::from(self.will_payload_len).min(SITE_WILL_PAYLOAD_MAX);
        let mut i = 0usize;
        while i < usize::from(self.sub_count).min(SITE_SUBS_MAX) {
            n += SITE_SUB_HDR + usize::from(self.subs[i].len);
            i += 1;
        }
        n += usize::from(self.inflight_count) * SITE_INFLIGHT_LEN;
        let mut k = 0usize;
        while k < usize::from(self.owed_count).min(SITE_OWED_MAX) {
            n += SITE_OWED_HDR + usize::from(self.owed[k].len);
            k += 1;
        }
        n + self.tail.encoded_len()
    }

    /// Encode into `out`. Returns the bytes written, or 0 when `out` is
    /// too short or the record is out of its bounds.
    pub fn encode(&self, out: &mut [u8]) -> usize {
        if usize::from(self.sub_count) > SITE_SUBS_MAX
            || usize::from(self.inflight_count) > SITE_INFLIGHT_MAX
            || usize::from(self.owed_count) > SITE_OWED_MAX
            || usize::from(self.tail.count) > TAIL_MAX_CURSORS
            || usize::from(self.will_topic_len) > SITE_WILL_TOPIC_MAX
            || usize::from(self.will_payload_len) > SITE_WILL_PAYLOAD_MAX
        {
            return 0;
        }
        let n = self.encoded_len();
        if out.len() < n {
            return 0;
        }
        let mut w = SsWriter { b: out, at: 0 };
        w.put(&SITE_MAGIC);
        w.put(&self.session_id);
        w.put(&self.epoch.to_le_bytes());
        w.put(&self.source_site.to_le_bytes());
        w.put(&self.export_seq.to_le_bytes());
        w.put(&self.tenant.to_le_bytes());
        w.put(&self.stream_hash.to_le_bytes());
        w.put(&self.generation.to_le_bytes());
        w.put(&self.slot.to_le_bytes());
        w.put(&self.shard.to_le_bytes());
        w.put(&[self.protocol]);
        w.put(&self.keep_alive_ms.to_le_bytes());
        w.put(&self.next_msg_id.to_le_bytes());
        w.put(&self.session_expiry_s.to_le_bytes());
        w.put(&self.receive_maximum.to_le_bytes());
        w.put(&[
            self.flags,
            self.will_present,
            self.will_qos,
            self.will_retain,
        ]);
        w.put(&self.will_delay_ms.to_le_bytes());
        w.put(&self.will_topic_len.to_le_bytes());
        w.put(&self.will_topic[..usize::from(self.will_topic_len)]);
        w.put(&self.will_payload_len.to_le_bytes());
        w.put(&self.will_payload[..usize::from(self.will_payload_len)]);
        w.put(&[self.sub_count]);
        let mut i = 0usize;
        while i < usize::from(self.sub_count) {
            let s = &self.subs[i];
            if s.len == 0 || usize::from(s.len) > SITE_FILTER_MAX {
                return 0;
            }
            w.put(&[s.qos, s.options]);
            w.put(&s.len.to_le_bytes());
            w.put(s.filter());
            i += 1;
        }
        w.put(&[self.inflight_count]);
        let mut j = 0usize;
        while j < usize::from(self.inflight_count) {
            let e = &self.inflight[j];
            w.put(&e.packet_id.to_le_bytes());
            w.put(&[e.qos, e.phase, e.direction]);
            w.put(&e.generation.to_le_bytes());
            w.put(&e.wal_index.to_le_bytes());
            w.put(&e.correlation_id.to_le_bytes());
            w.put(&e.partition_id.to_le_bytes());
            w.put(&[e.rec_sent]);
            j += 1;
        }
        w.put(&[self.owed_count]);
        let mut k = 0usize;
        while k < usize::from(self.owed_count) {
            let o = &self.owed[k];
            if usize::from(o.len) > DELIVER_BODY_MAX {
                return 0;
            }
            w.put(&o.packet_id.to_be_bytes());
            w.put(&[o.qos, o.released]);
            w.put(&o.generation.to_le_bytes());
            w.put(&o.len.to_le_bytes());
            w.put(o.body());
            k += 1;
        }
        let at = w.at;
        let t = self.tail.encode(&mut w.b[at..]);
        if t == 0 {
            return 0;
        }
        w.at += t;
        let crc = ss_crc32(&w.b[..w.at]);
        w.put(&crc.to_le_bytes());
        w.at
    }

    /// Decode a record, whole and exact: magic, CRC, every count and
    /// length inside its bound, the tail present, nothing left over.
    pub fn decode(b: &[u8]) -> Result<Self, u8> {
        if b.len() < SITE_FIXED + 4 || b.len() > SITE_RECORD_MAX || b[..4] != SITE_MAGIC {
            return Err(SS_CORRUPT);
        }
        let body = b.len() - 4;
        let crc = u32::from_le_bytes([b[body], b[body + 1], b[body + 2], b[body + 3]]);
        if crc != ss_crc32(&b[..body]) {
            return Err(SS_CORRUPT);
        }
        let mut r = SsReader {
            b: &b[..body],
            at: 4,
        };
        let mut s = Self::zero();
        s.session_id.copy_from_slice(r.take(16)?);
        s.epoch = r.u32()?;
        s.source_site = r.u32()?;
        s.export_seq = r.u64()?;
        s.tenant = r.u32()?;
        s.stream_hash = r.u64()?;
        s.generation = r.u32()?;
        s.slot = r.u32()?;
        s.shard = r.u32()?;
        s.protocol = r.u8()?;
        s.keep_alive_ms = r.u32()?;
        s.next_msg_id = r.u32()?;
        s.session_expiry_s = r.u32()?;
        s.receive_maximum = r.u16()?;
        s.flags = r.u8()?;
        s.will_present = r.u8()?;
        s.will_qos = r.u8()?;
        s.will_retain = r.u8()?;
        if s.flags & !SITE_FLAG_CLEAN_START != 0
            || s.will_present > 1
            || s.will_qos > 2
            || s.will_retain > 1
        {
            return Err(SS_CORRUPT);
        }
        s.will_delay_ms = r.u32()?;
        s.will_topic_len = r.u16()?;
        if usize::from(s.will_topic_len) > SITE_WILL_TOPIC_MAX {
            return Err(SS_CORRUPT);
        }
        let t = r.take(usize::from(s.will_topic_len))?;
        s.will_topic[..t.len()].copy_from_slice(t);
        s.will_payload_len = r.u16()?;
        if usize::from(s.will_payload_len) > SITE_WILL_PAYLOAD_MAX {
            return Err(SS_CORRUPT);
        }
        let pl = r.take(usize::from(s.will_payload_len))?;
        s.will_payload[..pl.len()].copy_from_slice(pl);
        s.sub_count = r.u8()?;
        if usize::from(s.sub_count) > SITE_SUBS_MAX {
            return Err(SS_CORRUPT);
        }
        let mut i = 0usize;
        while i < usize::from(s.sub_count) {
            let qos = r.u8()?;
            let options = r.u8()?;
            let len = r.u16()?;
            if len == 0 || usize::from(len) > SITE_FILTER_MAX || qos > 2 {
                return Err(SS_CORRUPT);
            }
            let f = r.take(usize::from(len))?;
            let e = &mut s.subs[i];
            e.qos = qos;
            e.options = options;
            e.len = len;
            e.filter[..f.len()].copy_from_slice(f);
            i += 1;
        }
        s.inflight_count = r.u8()?;
        if usize::from(s.inflight_count) > SITE_INFLIGHT_MAX {
            return Err(SS_CORRUPT);
        }
        let mut j = 0usize;
        while j < usize::from(s.inflight_count) {
            let e = SiteInflight {
                packet_id: r.u16()?,
                qos: r.u8()?,
                phase: r.u8()?,
                direction: r.u8()?,
                generation: r.u32()?,
                wal_index: r.u64()?,
                correlation_id: r.u64()?,
                partition_id: r.u16()?,
                rec_sent: r.u8()?,
            };
            if e.qos == 0 || e.qos > 2 {
                return Err(SS_CORRUPT);
            }
            s.inflight[j] = e;
            j += 1;
        }
        s.owed_count = r.u8()?;
        if usize::from(s.owed_count) > SITE_OWED_MAX {
            return Err(SS_CORRUPT);
        }
        let mut k = 0usize;
        while k < usize::from(s.owed_count) {
            let pid = r.take(2)?;
            let packet_id = u16::from_be_bytes([pid[0], pid[1]]);
            let qos = r.u8()?;
            let released = r.u8()?;
            let generation = r.u32()?;
            let len = r.u16()?;
            if usize::from(len) > DELIVER_BODY_MAX || qos == 0 || qos > 2 || released > 1 {
                return Err(SS_CORRUPT);
            }
            let body = r.take(usize::from(len))?;
            let o = &mut s.owed[k];
            o.packet_id = packet_id;
            o.qos = qos;
            o.released = released;
            o.generation = generation;
            o.len = len;
            o.body[..body.len()].copy_from_slice(body);
            k += 1;
        }
        let rest = &r.b[r.at..];
        let Some(tail) = HandoffTail::decode(rest) else {
            return Err(SS_CORRUPT);
        };
        if tail.encoded_len() != rest.len() {
            return Err(SS_CORRUPT);
        }
        s.tail = tail;
        if s.epoch == 0 || s.session_id == [0u8; SESSION_ID_LEN] {
            return Err(SS_CORRUPT);
        }
        Ok(s)
    }
}

/// The surviving site's view, for an import decision.
#[derive(Clone, Copy)]
pub struct SiteImporter<'a> {
    /// The site's apply cursor over its replica of the committed stream,
    /// per partition.
    pub apply: &'a [u64],
    /// The most recent entry of the session's slot the site skipped as
    /// not its own, `(partition, index)`.
    pub last_skip: Option<(u16, u64)>,
    /// The generation of a replicated record of the session the site
    /// already holds, if any.
    pub local_generation: Option<u32>,
}

/// What [`site_admit`] decided.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SiteVerdict {
    pub status: u8,
    /// The generation the resumed session takes.
    pub generation: u32,
    /// The site is behind the record: skip the session's entries at or
    /// below the record's cursor until caught up.
    pub floor: bool,
}

/// Decide the import of `rec` at a surviving site, resumed at
/// `resume_epoch` (the epoch the session directory committed).
pub fn site_admit(rec: &SiteSession, resume_epoch: u32, imp: &SiteImporter<'_>) -> SiteVerdict {
    let refuse = |status| SiteVerdict {
        status,
        generation: rec.generation,
        floor: false,
    };
    if resume_epoch <= rec.epoch {
        return refuse(SS_STALE_EPOCH);
    }
    // A later generation at the site is a CONNECT the record missed; an
    // earlier one is a site behind the record, which the cursor rule
    // floors.
    if imp.local_generation.is_some_and(|g| g > rec.generation) {
        return refuse(SS_STALE_EPOCH);
    }
    let generation = rec.generation;
    match cursor_verdict(&rec.tail, imp.apply, imp.last_skip) {
        CursorVerdict::Mismatch => refuse(SS_CURSOR_MISMATCH),
        CursorVerdict::AdmitWithFloor => SiteVerdict {
            status: SS_OK,
            generation,
            floor: true,
        },
        CursorVerdict::Admit => SiteVerdict {
            status: SS_OK,
            generation,
            floor: false,
        },
    }
}

/// Whether a site holding the record `(epoch, export_seq)` of a session
/// keeps a newly arrived one: only a newer epoch, or a later export at
/// the same epoch. An older or repeated record changes nothing.
pub fn site_keeps(held: Option<(u32, u64)>, rec: &SiteSession) -> bool {
    match held {
        None => true,
        Some((e, q)) => rec.epoch > e || (rec.epoch == e && rec.export_seq > q),
    }
}

// ── The worker's subscription book ──────────────────────────────────────────
//
// Subscriptions live in `topic_engine`, keyed by the session's slot; a site
// record carries them whole, so the worker keeps its own book of them,
// filled on the apply side from the same committed `QOP_SUBSCRIBE` /
// `QOP_UNSUBSCRIBE` entries every replica applies, and cleared whenever
// the session's subscriptions end (`MSG_SESSION_DROP`). A session whose
// record could not be whole here is marked LOST — a subscription the book
// cannot hold, a session that arrived by handoff (its subscriptions were
// booked by the worker it left), or an owed delivery the delivery ledger
// had no room for — and its export is refused, never sent incomplete.

/// Subscriptions the book holds across every session this worker serves.
pub const SITE_SUB_BOOK: usize = 256;
/// Bytes of a session's book section in a handoff record (QSR2): its
/// subscriptions within the site record's own bounds, so a session that
/// moves between workers stays exportable.
/// `[flags:1][count:1] count × [qos:1][options:1][len:2 LE][filter]`.
pub const SITE_BOOK_SECTION_MAX: usize = 2 + SITE_SUBS_MAX * (SITE_SUB_HDR + SITE_FILTER_MAX);
/// Book section flag: the exporting worker's book did not hold the
/// session's subscriptions whole.
const BOOK_SECTION_LOST: u8 = 1 << 0;
/// Session slots the book tracks LOST individually; a slot past it is
/// tracked by `lost_beyond`, which only a full reset clears.
pub const SITE_BOOK_SLOTS: usize = 16384;

#[derive(Clone, Copy)]
pub struct SiteSubEntry {
    pub used: u8,
    pub qos: u8,
    pub options: u8,
    pub len: u16,
    pub slot: u32,
    pub filter: [u8; SITE_FILTER_MAX],
}

impl SiteSubEntry {
    pub const fn zero() -> Self {
        Self {
            used: 0,
            qos: 0,
            options: 0,
            len: 0,
            slot: 0,
            filter: [0; SITE_FILTER_MAX],
        }
    }
}

/// The worker's book of its sessions' subscriptions, by session slot.
pub struct SiteSubBook {
    pub entries: [SiteSubEntry; SITE_SUB_BOOK],
    /// Session slots whose record cannot be whole here, one bit per
    /// slot: their export is refused until the session's subscriptions
    /// are cleared.
    pub lost: [u64; SITE_BOOK_SLOTS / 64],
    /// A slot past `SITE_BOOK_SLOTS` was lost.
    pub lost_beyond: bool,
}

impl SiteSubBook {
    /// The record of the session at `slot` cannot be whole here: its
    /// export is refused until its subscriptions are cleared.
    pub fn lose(&mut self, slot: u32) {
        let i = slot as usize;
        if i < SITE_BOOK_SLOTS {
            self.lost[i / 64] |= 1 << (i % 64);
        } else {
            self.lost_beyond = true;
        }
    }

    /// Whether the record of the session at `slot` cannot be whole here:
    /// its export must be refused.
    pub fn is_lost(&self, slot: u32) -> bool {
        let i = slot as usize;
        if i < SITE_BOOK_SLOTS {
            self.lost[i / 64] & (1 << (i % 64)) != 0
        } else {
            self.lost_beyond
        }
    }

    /// A committed subscription of the session at `slot`: added, or its
    /// QoS replaced when the filter is already booked.
    pub fn subscribe(&mut self, slot: u32, qos: u8, filter: &[u8]) {
        self.subscribe_with(slot, qos, 0, filter);
    }

    /// [`Self::subscribe`] with the subscription's options byte.
    pub fn subscribe_with(&mut self, slot: u32, qos: u8, options: u8, filter: &[u8]) {
        if filter.is_empty() || filter.len() > SITE_FILTER_MAX {
            self.lose(slot);
            return;
        }
        if let Some(e) = self.entries.iter_mut().find(|e| {
            e.used != 0
                && e.slot == slot
                && usize::from(e.len) == filter.len()
                && e.filter[..filter.len()] == *filter
        }) {
            e.qos = qos;
            e.options = options;
            return;
        }
        let Some(e) = self.entries.iter_mut().find(|e| e.used == 0) else {
            self.lose(slot);
            return;
        };
        *e = SiteSubEntry::zero();
        e.used = 1;
        e.slot = slot;
        e.qos = qos;
        e.options = options;
        e.len = filter.len() as u16;
        e.filter[..filter.len()].copy_from_slice(filter);
    }

    /// A committed unsubscription.
    pub fn unsubscribe(&mut self, slot: u32, filter: &[u8]) {
        for e in self.entries.iter_mut() {
            if e.used != 0
                && e.slot == slot
                && usize::from(e.len) == filter.len()
                && e.filter[..filter.len()] == *filter
            {
                *e = SiteSubEntry::zero();
            }
        }
    }

    /// The session at `slot` is gone: its subscriptions with it.
    pub fn clear(&mut self, slot: u32) {
        for e in self.entries.iter_mut() {
            if e.used != 0 && e.slot == slot {
                *e = SiteSubEntry::zero();
            }
        }
        let i = slot as usize;
        if i < SITE_BOOK_SLOTS {
            self.lost[i / 64] &= !(1 << (i % 64));
        }
    }
}

impl SiteSubBook {
    pub const fn new() -> Self {
        Self {
            entries: [SiteSubEntry::zero(); SITE_SUB_BOOK],
            lost: [0; SITE_BOOK_SLOTS / 64],
            lost_beyond: false,
        }
    }

    /// The session at `slot`'s book section for a handoff record, into
    /// `out` (at least [`SITE_BOOK_SECTION_MAX`] bytes). A session whose
    /// subscriptions the book does not hold whole, or holds past the site
    /// record's bounds, is sent as lost — the importer refuses its export
    /// just as this worker would. Returns the bytes written, 0 when `out`
    /// is short.
    pub fn export_section(&self, slot: u32, out: &mut [u8]) -> usize {
        if out.len() < SITE_BOOK_SECTION_MAX {
            return 0;
        }
        let lost = self.is_lost(slot) || self.of(slot).count() > SITE_SUBS_MAX;
        if lost {
            out[0] = BOOK_SECTION_LOST;
            out[1] = 0;
            return 2;
        }
        let mut at = 2usize;
        let mut n = 0u8;
        for e in self.of(slot) {
            let len = usize::from(e.len);
            out[at] = e.qos;
            out[at + 1] = e.options;
            out[at + 2..at + 4].copy_from_slice(&e.len.to_le_bytes());
            out[at + 4..at + 4 + len].copy_from_slice(&e.filter[..len]);
            at += SITE_SUB_HDR + len;
            n += 1;
        }
        out[0] = 0;
        out[1] = n;
        at
    }

    /// Seed the session at `slot` from a handoff record's book section:
    /// what the book held for the slot is replaced by what the session
    /// brought. A section that does not parse, says lost, or does not fit
    /// leaves the session lost. Returns whether the section parsed.
    pub fn import_section(&mut self, slot: u32, b: &[u8]) -> bool {
        self.clear(slot);
        let (Some(&flags), Some(&count)) = (b.first(), b.get(1)) else {
            self.lose(slot);
            return false;
        };
        if flags & !BOOK_SECTION_LOST != 0 || usize::from(count) > SITE_SUBS_MAX {
            self.lose(slot);
            return false;
        }
        if flags & BOOK_SECTION_LOST != 0 {
            self.lose(slot);
            return count == 0 && b.len() == 2;
        }
        // Parse whole before booking any of it.
        let mut at = 2usize;
        for _ in 0..count {
            let Some(h) = b.get(at..at + SITE_SUB_HDR) else {
                self.lose(slot);
                return false;
            };
            let len = usize::from(u16::from_le_bytes([h[2], h[3]]));
            if h[0] > 2 || len == 0 || len > SITE_FILTER_MAX || b.len() < at + SITE_SUB_HDR + len {
                self.lose(slot);
                return false;
            }
            at += SITE_SUB_HDR + len;
        }
        if at != b.len() {
            self.lose(slot);
            return false;
        }
        let mut at = 2usize;
        for _ in 0..count {
            let len = usize::from(u16::from_le_bytes([b[at + 2], b[at + 3]]));
            let f = &b[at + SITE_SUB_HDR..at + SITE_SUB_HDR + len];
            self.subscribe_with(slot, b[at], b[at + 1], f);
            at += SITE_SUB_HDR + len;
        }
        true
    }

    /// The subscriptions of the session at `slot`, in book order.
    pub fn of(&self, slot: u32) -> impl Iterator<Item = &SiteSubEntry> {
        self.entries
            .iter()
            .filter(move |e| e.used != 0 && e.slot == slot)
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

struct SsWriter<'a> {
    b: &'a mut [u8],
    at: usize,
}

impl SsWriter<'_> {
    fn put(&mut self, x: &[u8]) {
        self.b[self.at..self.at + x.len()].copy_from_slice(x);
        self.at += x.len();
    }
}

struct SsReader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> SsReader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], u8> {
        if self.at + n > self.b.len() {
            return Err(SS_CORRUPT);
        }
        let s = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, u8> {
        let x = self.take(2)?;
        Ok(u16::from_le_bytes([x[0], x[1]]))
    }

    fn u32(&mut self) -> Result<u32, u8> {
        let x = self.take(4)?;
        Ok(u32::from_le_bytes([x[0], x[1], x[2], x[3]]))
    }

    fn u64(&mut self) -> Result<u64, u8> {
        let x = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(x);
        Ok(u64::from_le_bytes(a))
    }
}

/// CRC32 (IEEE 802.3, reflected 0xEDB88320), the handoff frames' own.
pub fn ss_crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= u32::from(b);
        let mut i = 0;
        while i < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            i += 1;
        }
    }
    !crc
}
