// Bounded, no_std, no-alloc KAFKA protocol core — wire framing, the
// consumer-group request/response codecs, and the group-membership state
// machine. `include!`d by the host crate (tests) and the `kafka` .fmod.
//
// The codec-vs-protocol proof here is different from Postgres's (crypto): a
// Kafka consumer cannot read a single byte until it has completed a FIVE-STEP
// multi-round-trip membership handshake —
//   ApiVersions -> FindCoordinator -> JoinGroup -> SyncGroup -> (Heartbeat)*
// — where each request is built from fields the PREVIOUS response returned
// (coordinator host/port, member_id, generation_id, assignment), the client must
// then heartbeat on a TIMER to retain membership, and a heartbeat answering
// REBALANCE_IN_PROGRESS forces an immediate re-JoinGroup. Reply-dependent
// control flow, cross-request state, and timers: none of it expressible in a
// stateless encode/decode program.

/// Kafka API keys used by the consumer-group flow.
pub mod api {
    pub const PRODUCE: i16 = 0;
    pub const METADATA: i16 = 3;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const JOIN_GROUP: i16 = 11;
    pub const HEARTBEAT: i16 = 12;
    pub const SYNC_GROUP: i16 = 14;
    pub const API_VERSIONS: i16 = 18;
}

/// The error codes the membership state machine reacts to.
pub mod kerr {
    pub const NONE: i16 = 0;
    pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const NOT_COORDINATOR: i16 = 16;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const MEMBER_ID_REQUIRED: i16 = 79;
}

// ---- writers ----------------------------------------------------------------

fn kput(out: &mut [u8], pos: &mut usize, b: &[u8]) -> Option<()> {
    if *pos + b.len() > out.len() {
        return None;
    }
    out[*pos..*pos + b.len()].copy_from_slice(b);
    *pos += b.len();
    Some(())
}
fn kput_i16(out: &mut [u8], pos: &mut usize, v: i16) -> Option<()> {
    kput(out, pos, &v.to_be_bytes())
}
fn kput_i32(out: &mut [u8], pos: &mut usize, v: i32) -> Option<()> {
    kput(out, pos, &v.to_be_bytes())
}
/// Kafka STRING: `[len:i16 BE][bytes]`.
fn kput_str(out: &mut [u8], pos: &mut usize, s: &[u8]) -> Option<()> {
    kput_i16(out, pos, s.len() as i16)?;
    kput(out, pos, s)
}
/// Kafka BYTES: `[len:i32 BE][bytes]`.
fn kput_bytes(out: &mut [u8], pos: &mut usize, b: &[u8]) -> Option<()> {
    kput_i32(out, pos, b.len() as i32)?;
    kput(out, pos, b)
}
fn kput_i8(out: &mut [u8], pos: &mut usize, v: i8) -> Option<()> {
    if *pos >= out.len() {
        return None;
    }
    out[*pos] = v as u8;
    *pos += 1;
    Some(())
}
fn kput_i64(out: &mut [u8], pos: &mut usize, v: i64) -> Option<()> {
    kput(out, pos, &v.to_be_bytes())
}
/// Kafka signed (zig-zag) varint — the RecordBatch record encoding.
fn kput_varint(out: &mut [u8], pos: &mut usize, v: i64) -> Option<()> {
    let mut zz = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let mut byte = (zz & 0x7f) as u8;
        zz >>= 7;
        if zz != 0 {
            byte |= 0x80;
        }
        if *pos >= out.len() {
            return None;
        }
        out[*pos] = byte;
        *pos += 1;
        if zz == 0 {
            break;
        }
    }
    Some(())
}

/// CRC-32C (Castagnoli, reflected) — the RecordBatch v2 checksum. Bit-reflected
/// polynomial 0x82F63B78. KAT: `crc32c(b"123456789") == 0xE306_9283`.
pub fn kafka_crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = !0;
    for &b in data {
        crc ^= b as u32;
        let mut i = 0;
        while i < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            i += 1;
        }
    }
    !crc
}

/// Build a Produce v7 request BODY (everything after the request header's
/// client_id) for `topic`/partition 0 carrying one record with `value` (null
/// key), stamped `timestamp` (Unix ms). `kafka_request(api::PRODUCE, 7, …)`
/// frames it. `acks = 1` (leader), 30 s timeout. Returns the body length.
///
/// This is a real per-protocol connector capability: a Kafka producer must
/// build a CRC-checksummed RecordBatch v2 with zig-zag varint record framing —
/// not something a stateless byte codec expresses.
pub fn kafka_produce_body(
    topic: &[u8],
    value: &[u8],
    timestamp: i64,
    out: &mut [u8],
) -> Option<usize> {
    let mut p = 0usize;
    kput_i16(out, &mut p, -1)?; // transactional_id = null
    kput_i16(out, &mut p, 1)?; // acks = leader
    kput_i32(out, &mut p, 30_000)?; // timeout_ms
    kput_i32(out, &mut p, 1)?; // topic count
    kput_str(out, &mut p, topic)?;
    kput_i32(out, &mut p, 1)?; // partition count
    kput_i32(out, &mut p, 0)?; // partition index
    let rec_len_pos = p;
    kput_i32(out, &mut p, 0)?; // records length (patched)
    // ---- RecordBatch v2 ----
    kput_i64(out, &mut p, 0)?; // baseOffset
    let batch_len_pos = p;
    kput_i32(out, &mut p, 0)?; // batchLength (patched)
    kput_i32(out, &mut p, -1)?; // partitionLeaderEpoch
    kput_i8(out, &mut p, 2)?; // magic v2
    let crc_pos = p;
    kput_i32(out, &mut p, 0)?; // crc (patched)
    let body_start = p; // CRC covers from here
    kput_i16(out, &mut p, 0)?; // attributes
    kput_i32(out, &mut p, 0)?; // lastOffsetDelta
    kput_i64(out, &mut p, timestamp)?; // firstTimestamp
    kput_i64(out, &mut p, timestamp)?; // maxTimestamp
    kput_i64(out, &mut p, -1)?; // producerId
    kput_i16(out, &mut p, -1)?; // producerEpoch
    kput_i32(out, &mut p, -1)?; // baseSequence
    kput_i32(out, &mut p, 1)?; // record count
    // One record, varint-length-prefixed. Build it into scratch to know its
    // length before writing the prefix.
    let mut rec = [0u8; 512];
    let mut rp = 0usize;
    kput_i8(&mut rec, &mut rp, 0)?; // attributes
    kput_varint(&mut rec, &mut rp, 0)?; // timestampDelta
    kput_varint(&mut rec, &mut rp, 0)?; // offsetDelta
    kput_varint(&mut rec, &mut rp, -1)?; // keyLength = null
    kput_varint(&mut rec, &mut rp, value.len() as i64)?;
    kput(&mut rec, &mut rp, value)?;
    kput_varint(&mut rec, &mut rp, 0)?; // header count
    kput_varint(out, &mut p, rp as i64)?; // record length
    kput(out, &mut p, &rec[..rp])?;
    let body_end = p;
    // Back-patch crc, batchLength, records length.
    let crc = kafka_crc32c(&out[body_start..body_end]);
    out[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
    let batch_len = (body_end - (batch_len_pos + 4)) as i32;
    out[batch_len_pos..batch_len_pos + 4].copy_from_slice(&batch_len.to_be_bytes());
    let records_len = (body_end - (rec_len_pos + 4)) as i32;
    out[rec_len_pos..rec_len_pos + 4].copy_from_slice(&records_len.to_be_bytes());
    Some(p)
}

/// Parse a Produce v7 response body (header already stripped): returns the
/// first partition's `(error_code, base_offset)`. `None` if truncated.
pub fn kafka_parse_produce_response(body: &[u8]) -> Option<(i16, i64)> {
    let mut r = KReader::new(body);
    let responses = r.i32()?;
    if responses < 1 {
        return None;
    }
    let _topic = r.string()?;
    let partitions = r.i32()?;
    if partitions < 1 {
        return None;
    }
    let _index = r.i32()?;
    let error = r.i16()?;
    let offset = r.i64()?;
    Some((error, offset))
}

/// Frame a full request: `[size:i32][api_key:i16][api_version:i16]
/// [correlation_id:i32][client_id:STRING][body]`. `size` counts everything
/// after itself. Returns the total length.
pub fn kafka_request(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &[u8],
    body: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let mut p = 4; // reserve the size prefix
    kput_i16(out, &mut p, api_key)?;
    kput_i16(out, &mut p, api_version)?;
    kput_i32(out, &mut p, correlation_id)?;
    kput_str(out, &mut p, client_id)?;
    kput(out, &mut p, body)?;
    let size = (p - 4) as i32;
    out[0..4].copy_from_slice(&size.to_be_bytes());
    Some(p)
}

// ---- readers ----------------------------------------------------------------

/// A bounds-checked big-endian cursor over a response.
pub struct KReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> KReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    pub fn at(buf: &'a [u8], pos: usize) -> Self {
        Self { buf, pos }
    }
    pub fn i16(&mut self) -> Option<i16> {
        let b = self.buf.get(self.pos..self.pos + 2)?;
        self.pos += 2;
        Some(i16::from_be_bytes([b[0], b[1]]))
    }
    pub fn i32(&mut self) -> Option<i32> {
        let b = self.buf.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    pub fn i64(&mut self) -> Option<i64> {
        let b = self.buf.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    /// Kafka STRING; a -1 length yields an empty slice (null).
    pub fn string(&mut self) -> Option<&'a [u8]> {
        let n = self.i16()?;
        if n < 0 {
            return Some(&[]);
        }
        let s = self.buf.get(self.pos..self.pos + n as usize)?;
        self.pos += n as usize;
        Some(s)
    }
    /// Kafka BYTES; a -1 length yields an empty slice (null).
    pub fn bytes(&mut self) -> Option<&'a [u8]> {
        let n = self.i32()?;
        if n < 0 {
            return Some(&[]);
        }
        let s = self.buf.get(self.pos..self.pos + n as usize)?;
        self.pos += n as usize;
        Some(s)
    }
    pub fn pos(&self) -> usize {
        self.pos
    }
}

/// Length of the first complete size-prefixed response in `buf`, or `None` if
/// more bytes are needed. (`[size:i32][payload]`, so total = 4 + size.)
pub fn kafka_response_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < 4 {
        return None;
    }
    let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if size < 4 {
        return None;
    }
    let total = 4 + size as usize;
    if buf.len() < total {
        return None;
    }
    Some(total)
}

/// `(correlation_id, body_offset)` of a complete response.
pub fn kafka_response_header(buf: &[u8]) -> Option<(i32, usize)> {
    let _ = kafka_response_len(buf)?;
    let mut r = KReader::at(buf, 4);
    let corr = r.i32()?;
    Some((corr, r.pos()))
}

// ---- consumer-group requests ------------------------------------------------

/// FindCoordinator v0 body: `group_id:STRING`.
pub fn kafka_find_coordinator_body(group_id: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut p = 0;
    kput_str(out, &mut p, group_id)?;
    Some(p)
}

/// JoinGroup v0 body: `group_id, session_timeout_ms, member_id, protocol_type,
/// [protocols: (name, metadata)]`. `member_id` is empty on the first join — the
/// coordinator assigns one, and the NEXT join must echo it back.
pub fn kafka_join_group_body(
    group_id: &[u8],
    session_timeout_ms: i32,
    member_id: &[u8],
    protocol_name: &[u8],
    protocol_metadata: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let mut p = 0;
    kput_str(out, &mut p, group_id)?;
    kput_i32(out, &mut p, session_timeout_ms)?;
    kput_str(out, &mut p, member_id)?;
    kput_str(out, &mut p, b"consumer")?; // protocol_type
    kput_i32(out, &mut p, 1)?; // one protocol
    kput_str(out, &mut p, protocol_name)?;
    kput_bytes(out, &mut p, protocol_metadata)?;
    Some(p)
}

/// SyncGroup v0 body: `group_id, generation_id, member_id, [assignments]`. A
/// non-leader sends an empty assignment list.
pub fn kafka_sync_group_body(
    group_id: &[u8],
    generation_id: i32,
    member_id: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let mut p = 0;
    kput_str(out, &mut p, group_id)?;
    kput_i32(out, &mut p, generation_id)?;
    kput_str(out, &mut p, member_id)?;
    kput_i32(out, &mut p, 0)?; // no assignments (follower)
    Some(p)
}

/// Heartbeat v0 body: `group_id, generation_id, member_id`. Must be re-sent on a
/// timer or the coordinator evicts the member.
pub fn kafka_heartbeat_body(
    group_id: &[u8],
    generation_id: i32,
    member_id: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let mut p = 0;
    kput_str(out, &mut p, group_id)?;
    kput_i32(out, &mut p, generation_id)?;
    kput_str(out, &mut p, member_id)?;
    Some(p)
}

// ---- consumer-group responses -----------------------------------------------

/// FindCoordinator v0 response: `error_code, node_id, host, port`.
pub struct Coordinator<'a> {
    pub error_code: i16,
    pub node_id: i32,
    pub host: &'a [u8],
    pub port: i32,
}

pub fn kafka_parse_find_coordinator(body: &[u8]) -> Option<Coordinator<'_>> {
    let mut r = KReader::new(body);
    let error_code = r.i16()?;
    let node_id = r.i32()?;
    let host = r.string()?;
    let port = r.i32()?;
    Some(Coordinator { error_code, node_id, host, port })
}

/// JoinGroup v0 response: `error_code, generation_id, group_protocol, leader_id,
/// member_id, [members]`. The coordinator-assigned `member_id` and
/// `generation_id` are what every later request must carry.
pub struct JoinGroup<'a> {
    pub error_code: i16,
    pub generation_id: i32,
    pub leader_id: &'a [u8],
    pub member_id: &'a [u8],
    pub is_leader: bool,
}

pub fn kafka_parse_join_group(body: &[u8]) -> Option<JoinGroup<'_>> {
    let mut r = KReader::new(body);
    let error_code = r.i16()?;
    let generation_id = r.i32()?;
    let _protocol = r.string()?;
    let leader_id = r.string()?;
    let member_id = r.string()?;
    Some(JoinGroup {
        error_code,
        generation_id,
        leader_id,
        member_id,
        is_leader: !leader_id.is_empty() && leader_id == member_id,
    })
}

/// SyncGroup v0 response: `error_code, member_assignment`.
pub fn kafka_parse_sync_group(body: &[u8]) -> Option<(i16, &[u8])> {
    let mut r = KReader::new(body);
    let error_code = r.i16()?;
    let assignment = r.bytes()?;
    Some((error_code, assignment))
}

/// Heartbeat v0 / any error-code-first response.
pub fn kafka_parse_error_code(body: &[u8]) -> Option<i16> {
    KReader::new(body).i16()
}

// ---- group-membership state machine -----------------------------------------

/// Where the consumer is in the membership lifecycle. `#[repr(u8)]` so it can
/// live in a module's `#[repr(C)]` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KPhase {
    Disconnected = 0,
    Connecting = 1,
    AwaitApiVersions = 2,
    AwaitCoordinator = 3,
    AwaitJoin = 4,
    AwaitSync = 5,
    /// A full member of the group: assignment held, heartbeating.
    Stable = 6,
    AwaitHeartbeat = 7,
    /// PRODUCER mode: connected, waiting for the next message on `request_in`.
    ProduceIdle = 8,
    /// PRODUCER mode: a Produce request is in flight, awaiting the ack.
    ProduceWait = 9,
}

/// Events the I/O layer feeds the membership machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KEv {
    Start,
    Connected,
    RespOk,
    /// The group is rebalancing (or our member/generation was invalidated) — the
    /// consumer must re-join. This is the reply-dependent branch.
    RespRebalance,
    /// Transient: the coordinator moved or is unavailable — retry discovery.
    RespRetryable,
    RespFatal,
    /// The heartbeat interval elapsed (timer-driven — a codec has no timers).
    HeartbeatDue,
    PeerClosed,
    NetError,
}

/// Actions the I/O layer performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KAct {
    None,
    Connect,
    SendApiVersions,
    SendFindCoordinator,
    SendJoinGroup,
    SendSyncGroup,
    SendHeartbeat,
    Fail,
}

/// Map a Kafka error code to the membership event it implies.
pub fn kafka_classify_error(code: i16) -> KEv {
    match code {
        kerr::NONE => KEv::RespOk,
        kerr::REBALANCE_IN_PROGRESS
        | kerr::ILLEGAL_GENERATION
        | kerr::UNKNOWN_MEMBER_ID
        | kerr::MEMBER_ID_REQUIRED => KEv::RespRebalance,
        kerr::COORDINATOR_NOT_AVAILABLE | kerr::NOT_COORDINATOR => KEv::RespRetryable,
        _ => KEv::RespFatal,
    }
}

/// The consumer-group membership state machine — pure and host-testable.
///
/// The five-step handshake (ApiVersions -> FindCoordinator -> JoinGroup ->
/// SyncGroup -> Stable) must complete before any data flows, each step consuming
/// the previous response; `Stable` then heartbeats on a timer, and a heartbeat
/// answered with REBALANCE_IN_PROGRESS drops straight back to JoinGroup.
pub fn kafka_transition(phase: KPhase, ev: KEv) -> (KAct, KPhase) {
    use KAct::*;
    use KEv::*;
    use KPhase::*;
    match (phase, ev) {
        (Disconnected, Start) => (Connect, Connecting),
        (Connecting, Connected) => (SendApiVersions, AwaitApiVersions),
        (AwaitApiVersions, RespOk) => (SendFindCoordinator, AwaitCoordinator),
        // Coordinator discovery retries until the coordinator is available.
        (AwaitCoordinator, RespOk) => (SendJoinGroup, AwaitJoin),
        (AwaitCoordinator, RespRetryable) => (SendFindCoordinator, AwaitCoordinator),
        // The first join is often answered MEMBER_ID_REQUIRED: re-join echoing
        // the assigned member_id.
        (AwaitJoin, RespOk) => (SendSyncGroup, AwaitSync),
        (AwaitJoin, RespRebalance) => (SendJoinGroup, AwaitJoin),
        (AwaitSync, RespOk) => (None, Stable),
        (AwaitSync, RespRebalance) => (SendJoinGroup, AwaitJoin),
        // Membership upkeep: heartbeat on the timer; a rebalance answer rejoins.
        (Stable, HeartbeatDue) => (SendHeartbeat, AwaitHeartbeat),
        (AwaitHeartbeat, RespOk) => (None, Stable),
        (AwaitHeartbeat, RespRebalance) => (SendJoinGroup, AwaitJoin),
        // A coordinator move mid-membership sends us back to discovery.
        (AwaitHeartbeat, RespRetryable) => (SendFindCoordinator, AwaitCoordinator),
        // Anything fatal or transport-level resets the whole membership.
        (_, RespFatal) | (_, PeerClosed) | (_, NetError) => (Fail, Disconnected),
        _ => (None, phase),
    }
}
