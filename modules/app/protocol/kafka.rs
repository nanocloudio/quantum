//! kafka — Kafka binary-protocol codec.
//!
//! Reassembles size-delimited requests, decodes the supported API
//! surface into session proposals, and encodes responses back onto the
//! wire. Holds per-connection reassembly state and the advertised
//! host/port used in Metadata responses.
//!
//! ## Per-step bound
//!
//! `RX_QUOTA` records off `in_raw` and `RESP_QUOTA` off `in_responses`.

use super::abi::SyscallTable;
use super::{dev_log, dev_millis, dev_channel_port, wire};


/// Per-connection reassembly capacity. Bounds the largest single Kafka
/// request we accept; a request whose `size` field exceeds this closes
/// the conn's reassembly state (fail-closed, client reconnects). Sized
/// so a max request still fits the 8 KiB channel envelope after the
/// session-proposal header is prepended.
const RASM: usize = 8192;
/// Largest accepted value of the Kafka `size` field. Leaves room for
/// the session envelope (10 bytes) + conn_id + tagged-proposal (18
/// bytes) + wire envelope (3 bytes) downstream of the 8 KiB channel cap.
const MAX_KREQ: usize = 8100;
const KCONNS: usize = 32;
/// Frame/scratch buffer size (responses, outbound envelopes).
const OUT_BUF: usize = 8192;
/// Per-tick drain quotas. Ingress must outrun the router component's 64
/// records/tick; response encode matches the session side's burst.
const RX_QUOTA: usize = 64;
const RESP_QUOTA: usize = 64;

const API_PRODUCE: i16 = 0;
const API_FETCH: i16 = 1;
const API_LIST_OFFSETS: i16 = 2;
const API_METADATA: i16 = 3;
const API_OFFSET_COMMIT: i16 = 8;
const API_OFFSET_FETCH: i16 = 9;
const API_FIND_COORDINATOR: i16 = 10;
const API_JOIN_GROUP: i16 = 11;
const API_HEARTBEAT: i16 = 12;
const API_LEAVE_GROUP: i16 = 13;
const API_SYNC_GROUP: i16 = 14;
const API_API_VERSIONS: i16 = 18;
const API_INIT_PRODUCER_ID: i16 = 22;

const MAX_TOPICS_SEEN: usize = 8;
const MAX_TOPIC_LEN: usize = 64;
const MAX_HOST_LEN: usize = 48;

#[repr(C)]
#[derive(Clone, Copy)]
struct KConn {
    conn_id: u8,
    active: u8,
    len: u16,
    buf: [u8; RASM],
}

impl KConn {
    const fn zero() -> Self {
        Self { conn_id: 0, active: 0, len: 0, buf: [0; RASM] }
    }
}

#[repr(C)]
pub struct Kafka {
    pub out_proposals: i32,
    pub out_frames: i32,

    // Params
    advertised_host: [u8; MAX_HOST_LEN],
    advertised_host_len: u8,
    pub advertised_port: u16,
    pub partitions: u16,

    requests_decoded: u32,
    responses_encoded: u32,
    parse_errors: u32,
    resync_resets: u32,
    unsupported_dropped: u32,
    /// InitProducerId issuance counter (monotonic; broker-local).
    next_producer_id: u64,

    topics_seen: [[u8; MAX_TOPIC_LEN]; MAX_TOPICS_SEEN],
    topics_seen_len: [u8; MAX_TOPICS_SEEN],
    topics_seen_count: u8,

    conns: [KConn; KCONNS],
    frame: [u8; OUT_BUF],
    scratch: [u8; OUT_BUF],
}

/// Set the broker address advertised in Metadata responses. Must be the
/// address CLIENTS can reach (e.g. the rig's LAN IP), not the bind
/// address. Bounded and non-empty only; oversize or empty is ignored so
/// the default survives.
///
/// # Safety
///
/// `d` must be valid for `len` reads (the param-macro contract).
pub unsafe fn set_advertised_host(s: &mut Kafka, d: *const u8, len: usize) {
    if d.is_null() || len == 0 || len > MAX_HOST_LEN {
        return;
    }
    // SAFETY: caller guarantees `d` is valid for `len` reads, and `len`
    // is bounded by MAX_HOST_LEN above.
    unsafe {
        core::ptr::copy_nonoverlapping(d, s.advertised_host.as_mut_ptr(), len);
    }
    s.advertised_host_len = len as u8;
}



/// Component defaults. Channel handles are assigned by the
/// composite after this returns.
pub fn init(s: &mut Kafka) {
    for c in s.conns.iter_mut() { *c = KConn::zero(); }
    s.topics_seen_count = 0;
    s.next_producer_id = 0;
    s.advertised_host_len = 0;
    s.advertised_port = 9090;
    s.partitions = 1;
}

/// Apply the advertised-host default if the graph supplied none. Called
/// by the composite after param parsing.
pub fn finish_init(s: &mut Kafka) {
    if s.advertised_host_len == 0 {
        let def = b"127.0.0.1";
        s.advertised_host[..def.len()].copy_from_slice(def);
        s.advertised_host_len = def.len() as u8;
    }
}

/// Emit a `[conn_id][bytes]` payload as a `MSG_CLIENT_FRAME` envelope
/// toward peer_router. See `the mqtt component::write_conn_frame` for the
/// rationale.
/// # Safety
unsafe fn write_conn(sys: &SyscallTable, chan: i32, conn_id: u8, bytes: &[u8]) -> bool {
    if chan < 0 { return false; }
    let total = 1 + bytes.len();
    if total > OUT_BUF { return false; }
    let mut out = [0u8; OUT_BUF];
    out[0] = conn_id;
    out[1..total].copy_from_slice(bytes);
    let w = wire::channel_write_msg(sys, chan, wire::MSG_CLIENT_FRAME, &out[..total]);
    w > 0
}

fn find_conn(s: &mut Kafka, conn_id: u8) -> usize {
    for i in 0..KCONNS {
        if s.conns[i].active == 1 && s.conns[i].conn_id == conn_id { return i; }
    }
    for i in 0..KCONNS {
        if s.conns[i].active == 0 {
            s.conns[i].conn_id = conn_id;
            s.conns[i].active = 1;
            s.conns[i].len = 0;
            return i;
        }
    }
    // Table full: evict slot 0. peer_router caps client conns at 64 but
    // only a fraction speak Kafka; the bench profile stays well inside 32.
    s.conns[0].conn_id = conn_id;
    s.conns[0].active = 1;
    s.conns[0].len = 0;
    0
}

fn remember_topic(s: &mut Kafka, name: &[u8]) {
    if name.is_empty() || name.len() > MAX_TOPIC_LEN { return; }
    let n = s.topics_seen_count as usize;
    for i in 0..n {
        if &s.topics_seen[i][..s.topics_seen_len[i] as usize] == name { return; }
    }
    if n < MAX_TOPICS_SEEN {
        s.topics_seen[n][..name.len()].copy_from_slice(name);
        s.topics_seen_len[n] = name.len() as u8;
        s.topics_seen_count += 1;
    }
}

// ── Response builders ───────────────────────────────────────────────────────

/// Frame `[size][corr][body]` into `s.frame` and send to the client.
/// # Safety
unsafe fn send_response(
    s: &mut Kafka, sys: &SyscallTable, conn_id: u8, corr: i32, body_len: usize,
) {
    // Body was built in s.scratch[..body_len].
    let total = 8 + body_len;
    if total > OUT_BUF { return; }
    s.frame[0..4].copy_from_slice(&((4 + body_len) as i32).to_be_bytes());
    s.frame[4..8].copy_from_slice(&corr.to_be_bytes());
    s.frame[8..total].copy_from_slice(&s.scratch[..body_len]);
    let out_frames = s.out_frames;
    if write_conn(sys, out_frames, conn_id, &s.frame[..total]) {
        s.responses_encoded = s.responses_encoded.wrapping_add(1);
    }
}

/// ApiVersions response body into `s.scratch`. Returns body length.
///
/// v0-v2 non-flexible: [error:i16][api_keys: i32 count, per key
/// i16×3][throttle:i32 (v1+)]. For v3+ requests we answer the KIP-511
/// downgrade: a v0-shaped body with error 35 and the full key list.
fn build_api_versions(s: &mut Kafka, api_ver: i16) -> usize {
    let downgrade = api_ver >= 3;
    let error: i16 = if downgrade { 35 } else { 0 };
    const KEYS: [(i16, i16, i16); 13] = [
        (API_PRODUCE, 2, 8),
        (API_FETCH, 0, 5),
        (API_LIST_OFFSETS, 0, 2),
        (API_METADATA, 0, 7),
        (API_OFFSET_COMMIT, 0, 2),
        (API_OFFSET_FETCH, 0, 3),
        (API_FIND_COORDINATOR, 0, 1),
        (API_JOIN_GROUP, 0, 2),
        (API_HEARTBEAT, 0, 1),
        (API_LEAVE_GROUP, 0, 1),
        (API_SYNC_GROUP, 0, 1),
        (API_API_VERSIONS, 0, 2),
        (API_INIT_PRODUCER_ID, 0, 1),
    ];
    let mut p = 0usize;
    s.scratch[p..p + 2].copy_from_slice(&error.to_be_bytes()); p += 2;
    s.scratch[p..p + 4].copy_from_slice(&(KEYS.len() as i32).to_be_bytes()); p += 4;
    for &(k, mn, mx) in KEYS.iter() {
        s.scratch[p..p + 2].copy_from_slice(&k.to_be_bytes()); p += 2;
        s.scratch[p..p + 2].copy_from_slice(&mn.to_be_bytes()); p += 2;
        s.scratch[p..p + 2].copy_from_slice(&mx.to_be_bytes()); p += 2;
    }
    if !downgrade && api_ver >= 1 {
        s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    }
    p
}

/// Append one topic block (error 0, `partitions` partitions, leader 0)
/// to the Metadata response being built in `s.scratch` at offset `p`.
/// Returns the new offset. Capacity is guaranteed by the caller: with
/// `partitions` clamped to 16 and MAX_TOPICS_SEEN = 8, the worst-case
/// response (8 × (77 + 16×34) = 4.9 KiB) fits OUT_BUF.
fn put_metadata_topic(s: &mut Kafka, mut p: usize, name_idx: usize, v: i16) -> usize {
    let nlen = s.topics_seen_len[name_idx] as usize;
    let parts = s.partitions.clamp(1, 16) as usize;
    s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2; // error
    s.scratch[p..p + 2].copy_from_slice(&(nlen as i16).to_be_bytes()); p += 2;
    let name = s.topics_seen[name_idx];
    s.scratch[p..p + nlen].copy_from_slice(&name[..nlen]); p += nlen;
    if v >= 1 { s.scratch[p] = 0; p += 1; } // is_internal = false
    s.scratch[p..p + 4].copy_from_slice(&(parts as i32).to_be_bytes()); p += 4;
    for part in 0..parts {
        s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2; // error
        s.scratch[p..p + 4].copy_from_slice(&(part as i32).to_be_bytes()); p += 4;
        s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // leader
        if v >= 7 {
            s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // leader_epoch
        }
        s.scratch[p..p + 4].copy_from_slice(&1i32.to_be_bytes()); p += 4; // replicas: [0]
        s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
        s.scratch[p..p + 4].copy_from_slice(&1i32.to_be_bytes()); p += 4; // isr: [0]
        s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
        if v >= 5 {
            s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // offline: []
        }
    }
    p
}

/// Metadata response body (non-flexible v0-v7) into `s.scratch`.
/// `req` is the request body after client_id. Returns body length.
fn build_metadata(s: &mut Kafka, v: i16, req: &[u8]) -> usize {
    // Parse requested topic names first (also refreshes topics_seen).
    // Request: [topics: i32 count][string...]  (count -1 = all, v1+)
    let mut want: [usize; MAX_TOPICS_SEEN] = [0; MAX_TOPICS_SEEN];
    let mut want_n = 0usize;
    let mut all = true;
    if req.len() >= 4 {
        let count = i32::from_be_bytes([req[0], req[1], req[2], req[3]]);
        if count > 0 {
            all = false;
            let mut off = 4usize;
            for _ in 0..count.min(MAX_TOPICS_SEEN as i32) {
                if off + 2 > req.len() { break; }
                let nlen = i16::from_be_bytes([req[off], req[off + 1]]) as usize;
                off += 2;
                if off + nlen > req.len() || nlen == 0 || nlen > MAX_TOPIC_LEN { break; }
                let mut name = [0u8; MAX_TOPIC_LEN];
                name[..nlen].copy_from_slice(&req[off..off + nlen]);
                off += nlen;
                remember_topic(s, &name[..nlen]);
                // Locate index in topics_seen (remember_topic guarantees
                // presence unless the table is full).
                for i in 0..s.topics_seen_count as usize {
                    if &s.topics_seen[i][..s.topics_seen_len[i] as usize] == &name[..nlen]
                        && want_n < MAX_TOPICS_SEEN
                    {
                        want[want_n] = i;
                        want_n += 1;
                        break;
                    }
                }
            }
        } else if count == 0 {
            // v0: empty array means "all topics"; keep `all`.
        }
    }
    if all {
        want_n = s.topics_seen_count as usize;
        for (i, w) in want.iter_mut().enumerate().take(want_n) { *w = i; }
    }

    let mut p = 0usize;
    if v >= 3 {
        s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // throttle
    }
    // brokers: [1 broker: node 0, advertised host:port, rack null (v1+)]
    s.scratch[p..p + 4].copy_from_slice(&1i32.to_be_bytes()); p += 4;
    s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // node_id
    let hlen = s.advertised_host_len as usize;
    s.scratch[p..p + 2].copy_from_slice(&(hlen as i16).to_be_bytes()); p += 2;
    let host = s.advertised_host;
    s.scratch[p..p + hlen].copy_from_slice(&host[..hlen]); p += hlen;
    s.scratch[p..p + 4].copy_from_slice(&(s.advertised_port as i32).to_be_bytes()); p += 4;
    if v >= 1 {
        s.scratch[p..p + 2].copy_from_slice(&(-1i16).to_be_bytes()); p += 2; // rack null
    }
    if v >= 2 {
        s.scratch[p..p + 2].copy_from_slice(&(-1i16).to_be_bytes()); p += 2; // cluster_id null
    }
    if v >= 1 {
        s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // controller_id
    }
    s.scratch[p..p + 4].copy_from_slice(&(want_n as i32).to_be_bytes()); p += 4;
    for i in 0..want_n {
        p = put_metadata_topic(s, p, want[i], v);
    }
    p
}

// ── Step ────────────────────────────────────────────────────────────────────

/// Process one complete Kafka request sitting at `conn.buf[off..off+total]`.
/// # Safety
unsafe fn handle_request(
    s: &mut Kafka, sys: &SyscallTable, conn_id: u8, ci: usize, off: usize, total: usize,
) {
    let req = &s.conns[ci].buf[off..off + total];
    if total < 12 { s.parse_errors = s.parse_errors.wrapping_add(1); return; }
    let api_key = i16::from_be_bytes([req[4], req[5]]);
    let api_ver = i16::from_be_bytes([req[6], req[7]]);
    let corr = i32::from_be_bytes([req[8], req[9], req[10], req[11]]);
    // client_id: nullable string right after the header (all non-flexible
    // request header versions we advertise).
    let mut body_off = 12usize;
    if total >= body_off + 2 {
        let cl = i16::from_be_bytes([req[body_off], req[body_off + 1]]);
        body_off += 2;
        if cl > 0 {
            body_off += cl as usize;
            if body_off > total { s.parse_errors = s.parse_errors.wrapping_add(1); return; }
        }
    } else {
        s.parse_errors = s.parse_errors.wrapping_add(1);
        return;
    }
    s.requests_decoded = s.requests_decoded.wrapping_add(1);

    match api_key {
        API_API_VERSIONS => {
            let blen = build_api_versions(s, api_ver);
            send_response(s, sys, conn_id, corr, blen);
        }
        API_METADATA => {
            // Copy the request body into `frame` (as scratch input) so
            // build_metadata can borrow it while writing s.scratch.
            let body_len = total - body_off;
            if body_len > OUT_BUF { s.parse_errors = s.parse_errors.wrapping_add(1); return; }
            core::ptr::copy_nonoverlapping(
                s.conns[ci].buf.as_ptr().add(off + body_off),
                s.frame.as_mut_ptr(),
                body_len,
            );
            // SAFETY: build_metadata writes only s.scratch; req_copy
            // aliases s.frame, which build_metadata never touches.
            let req_copy: &[u8] = core::slice::from_raw_parts(s.frame.as_ptr(), body_len);
            let blen = build_metadata(s, api_ver, req_copy);
            send_response(s, sys, conn_id, corr, blen);
        }
        API_FIND_COORDINATOR => {
            // Single logical broker: the coordinator is always us.
            // v0: [err][node_id][host][port]
            // v1: [throttle][err][error_message null][node_id][host][port]
            let hlen = s.advertised_host_len as usize;
            let host = s.advertised_host;
            let port = s.advertised_port as i32;
            let mut p = 0usize;
            if api_ver >= 1 {
                s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
            }
            s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
            if api_ver >= 1 {
                s.scratch[p..p + 2].copy_from_slice(&(-1i16).to_be_bytes()); p += 2;
            }
            s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // node 0
            s.scratch[p..p + 2].copy_from_slice(&(hlen as i16).to_be_bytes()); p += 2;
            s.scratch[p..p + hlen].copy_from_slice(&host[..hlen]); p += hlen;
            s.scratch[p..p + 4].copy_from_slice(&port.to_be_bytes()); p += 4;
            send_response(s, sys, conn_id, corr, p);
        }
        API_INIT_PRODUCER_ID => {
            // Producer id issuance. Sequence-number dedup is NOT enforced
            // yet — idempotent producers connect and function, with
            // at-least-once (not exactly-once) semantics on retry.
            // v0/v1: [throttle][err][producer_id i64][producer_epoch i16]
            s.next_producer_id = s.next_producer_id.wrapping_add(1);
            let pid = s.next_producer_id as i64;
            let mut p = 0usize;
            s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
            s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
            s.scratch[p..p + 8].copy_from_slice(&pid.to_be_bytes()); p += 8;
            s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
            send_response(s, sys, conn_id, corr, p);
        }
        API_PRODUCE | API_FETCH | API_LIST_OFFSETS | API_OFFSET_COMMIT
        | API_OFFSET_FETCH | API_JOIN_GROUP | API_HEARTBEAT
        | API_LEAVE_GROUP | API_SYNC_GROUP => {
            // Envelope: [conn_id][proto=1][api_key LE][api_ver LE][corr LE][body]
            let body_len = total - body_off;
            let env_len = 10 + body_len;
            if env_len + 1 > OUT_BUF {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                return;
            }
            s.scratch[0] = conn_id;
            s.scratch[1] = 1; // PROTO_KAFKA
            s.scratch[2..4].copy_from_slice(&api_key.to_le_bytes());
            s.scratch[4..6].copy_from_slice(&api_ver.to_le_bytes());
            s.scratch[6..10].copy_from_slice(&corr.to_le_bytes());
            core::ptr::copy_nonoverlapping(
                s.conns[ci].buf.as_ptr().add(off + body_off),
                s.scratch.as_mut_ptr().add(10),
                body_len,
            );
            let out = s.out_proposals;
            let w = wire::channel_write_msg(
                sys, out, wire::MSG_SESSION_PROPOSAL, &s.scratch[..10 + body_len],
            );
            if w <= 0 {
                // codec_in saturated — drop; producer retries on timeout.
                s.parse_errors = s.parse_errors.wrapping_add(1);
            }
        }
        _ => {
            s.unsupported_dropped = s.unsupported_dropped.wrapping_add(1);
        }
    }
}

/// One step of this component.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
/// Handle one client record: `[conn_id][tcp chunk]` framed as
/// MSG_CLIENT_FRAME, or MSG_CONN_CLOSED carrying just the conn id.
/// Appends to the connection's reassembly buffer and drains every
/// complete size-delimited request from it.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_frame(s: &mut Kafka, sys: &SyscallTable, mtype: u8, payload: &[u8]) {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {

        // Stage the record exactly as the channel read did:
        // `frame` holds `[conn_id][tcp chunk]`.
        let n = payload.len();
        if n > s.frame.len() { return; }
        s.frame[..n].copy_from_slice(payload);
        if n < 1 { return; }
        let conn_id = s.frame[0];

        // Connection closed: release this conn's reassembly slot and
        // tell session_processor to drop conn-keyed state (groups).
        if mtype == wire::MSG_CONN_CLOSED {
            for i in 0..KCONNS {
                if s.conns[i].active == 1 && s.conns[i].conn_id == conn_id {
                    s.conns[i] = KConn::zero();
                    break;
                }
            }
            // Forward as MSG_CONN_CLOSED (NOT MSG_SESSION_DISCONNECT —
            // that type already means "MQTT DISCONNECT packet" on this
            // fan-in lane and is claimed by session_processor's MQTT
            // path). session_processor releases conn-keyed state on it.
            let mut cb = [conn_id];
            wire::channel_write_msg(
                sys, s.out_proposals, wire::MSG_CONN_CLOSED, &mut cb,
            );
            return;
        }
        if n <= 1 { return; }
        let data_len = n - 1;
        let ci = find_conn(s, conn_id);

        // Append to the conn's reassembly buffer.
        let have = s.conns[ci].len as usize;
        if have + data_len > RASM {
            // Overflow — protocol desync or oversize request.
            // Fail closed: drop buffered bytes, resync from empty.
            s.conns[ci].len = 0;
            s.resync_resets = s.resync_resets.wrapping_add(1);
            return;
        }
        core::ptr::copy_nonoverlapping(
            s.frame.as_ptr().add(1),
            s.conns[ci].buf.as_mut_ptr().add(have),
            data_len,
        );
        s.conns[ci].len = (have + data_len) as u16;

        // Extract every complete frame currently buffered.
        let mut off = 0usize;
        let mut avail = s.conns[ci].len as usize;
        loop {
            if avail < 4 { break; }
            let size = i32::from_be_bytes([
                s.conns[ci].buf[off],
                s.conns[ci].buf[off + 1],
                s.conns[ci].buf[off + 2],
                s.conns[ci].buf[off + 3],
            ]);
            if size <= 0 || size as usize > MAX_KREQ {
                // Unrecoverable desync for a stream protocol.
                s.conns[ci].len = 0;
                s.resync_resets = s.resync_resets.wrapping_add(1);
                off = 0;
                avail = 0;
                break;
            }
            let total = 4 + size as usize;
            if avail < total { break; }
            handle_request(s, sys, conn_id, ci, off, total);
            off += total;
            avail -= total;
        }
        // Compact leftover fragment to the front.
        if off > 0 && avail > 0 {
            core::ptr::copy(
                s.conns[ci].buf.as_ptr().add(off),
                s.conns[ci].buf.as_mut_ptr(),
                avail,
            );
        }
        if off > 0 { s.conns[ci].len = avail as u16; }
    }
}

/// Encode one session response into a Kafka response frame.
///
/// Payload is the session envelope
/// `[conn_id:u8][proto=1][api_key:i16 LE][api_ver:i16 LE][corr_id:i32 LE][body]`
/// (see session_processor's `emit_kafka_response`). The body is already a
/// complete, correctly-versioned response body for `api_key`/`api_ver`;
/// this only reframes it as the wire form `[size:i32 BE][corr:i32 BE][body]`
/// and writes it to the originating connection.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_response(s: &mut Kafka, sys: &SyscallTable, payload: &[u8]) {
    let plen = payload.len();
    // conn(1) + proto(1) + api_key(2) + api_ver(2) + corr(4)
    if plen < 10 {
        return;
    }
    let conn_id = payload[0];
    // payload[1] is the PROTO_KAFKA discriminator; the dispatch table has
    // already matched it.
    let corr = i32::from_le_bytes([payload[6], payload[7], payload[8], payload[9]]);
    let body_len = plen - 10;
    if body_len > s.scratch.len() {
        s.parse_errors = s.parse_errors.wrapping_add(1);
        return;
    }
    s.scratch[..body_len].copy_from_slice(&payload[10..plen]);
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        send_response(s, sys, conn_id, corr, body_len);
    }
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(s: &Kafka, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&s.requests_decoded.to_le_bytes());
    m[4..8].copy_from_slice(&s.responses_encoded.to_le_bytes());
    m[8..12].copy_from_slice(&s.parse_errors.to_le_bytes());
    12
}
