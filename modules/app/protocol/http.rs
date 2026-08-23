//! http — request parsing and response framing for the shared client
//! port's diagnostic leg.
//!
//! The router pins a connection to PROTO_HTTP from its first bytes.
//! Records on such a connection are parsed here into the admin
//! surface's message form — MSG_HTTP_REQUEST
//! `[conn_id][method][path_len][path][body]` — and emitted on
//! `http_out`. Replies return message-shaped as MSG_HTTP_RESPONSE
//! `[conn_id][status:u16 LE][body_len:u16 LE][body]` on
//! `http_responses_in`; the wire-level HTTP/1.1 response is framed
//! HERE and egresses on the shared `frames_out` handle as a
//! MSG_CLIENT_FRAME. The consumer stays message-shaped; the module
//! that owns the client port owns the wire format.
//!
//! One record = one request: a request must arrive within a single
//! client record (peer_router emits TCP segments up to 4 KiB, far
//! above any diagnostic request). A request split across records
//! parses as malformed and answers 400 on each fragment.

use super::abi::SyscallTable;
use super::wire;

/// Path and body bounds, matching the admin surface's `request`-port
/// parser (`operations`: MAX_EXT_PATH / MAX_EXT_BODY). A request the
/// consumer would truncate is refused here instead.
pub const MAX_PATH: usize = 64;
pub const MAX_BODY: usize = 1024;

/// Response staging buffer: `[header reserve][response payload]`. The
/// reserve takes the conn_id byte plus the longest status line +
/// header block (≤ 110 bytes); the payload area takes the largest
/// MSG_HTTP_RESPONSE a channel can carry (`operations`' `/metrics`
/// export caps the body at `SAFE_EXPORT_MAX` = 7,400 bytes, plus the
/// 5-byte response header).
pub const RESP_HDR_RESERVE: usize = 128;
pub const RESP_BUF: usize = 8192;
/// Offset of the response BODY inside the staging buffer: the
/// MSG_HTTP_RESPONSE payload is read at `RESP_HDR_RESERVE`, so its
/// 5-byte `[conn_id][status][body_len]` header sits just below the
/// body and is overwritten by the tail of the HTTP header block.
const BODY_START: usize = RESP_HDR_RESERVE + 5;

/// MSG_HTTP_RESPONSE frames drained per step.
const RESP_DRAIN_BUDGET: usize = 8;

/// Reason phrase for the status codes the admin surface emits; any
/// other code renders a generic phrase (the code itself is what
/// clients dispatch on).
fn reason(status: u16) -> &'static [u8] {
    match status {
        200 => b"OK",
        202 => b"Accepted",
        400 => b"Bad Request",
        404 => b"Not Found",
        503 => b"Service Unavailable",
        _ => b"Status",
    }
}

/// Render `HTTP/1.1 <status> <reason>` + headers into `hdr`,
/// returning the header block's length. `hdr` must hold
/// `RESP_HDR_RESERVE` bytes.
fn render_header(hdr: &mut [u8], status: u16, body_len: usize) -> usize {
    fn put(hdr: &mut [u8], n: usize, s: &[u8]) -> usize {
        hdr[n..n + s.len()].copy_from_slice(s);
        n + s.len()
    }
    let status = status.clamp(100, 999);
    let code = [
        b'0' + (status / 100) as u8,
        b'0' + (status / 10 % 10) as u8,
        b'0' + (status % 10) as u8,
    ];
    let mut n = put(hdr, 0, b"HTTP/1.1 ");
    n = put(hdr, n, &code);
    n = put(hdr, n, b" ");
    n = put(hdr, n, reason(status));
    n = put(hdr, n, b"\r\nContent-Type: text/plain\r\nContent-Length: ");
    let mut digits = [0u8; 5];
    let mut d = 0;
    let mut v = body_len;
    loop {
        digits[d] = b'0' + (v % 10) as u8;
        d += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    for i in (0..d).rev() {
        hdr[n] = digits[i];
        n += 1;
    }
    put(hdr, n, b"\r\nConnection: close\r\n\r\n")
}

/// Frame the response body staged at `buf[BODY_START..BODY_START+len]`
/// as one MSG_CLIENT_FRAME `[conn_id][HTTP/1.1 bytes]` on
/// `frames_out`. Best-effort: an unwritable port drops the reply, the
/// client's request timeout is the backstop.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
unsafe fn emit_prepared(
    sys: &SyscallTable,
    out_frames: i32,
    buf: &mut [u8; RESP_BUF],
    conn_id: u8,
    status: u16,
    body_len: usize,
) {
    if out_frames < 0 {
        return;
    }
    let mut hdr = [0u8; RESP_HDR_RESERVE];
    let hlen = render_header(&mut hdr, status, body_len);
    let start = BODY_START - 1 - hlen;
    buf[start] = conn_id;
    buf[start + 1..BODY_START].copy_from_slice(&hdr[..hlen]);
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        wire::channel_write_msg(
            sys,
            out_frames,
            wire::MSG_CLIENT_FRAME,
            &buf[start..BODY_START + body_len],
        );
    }
}

/// Answer `conn_id` directly with `status` and a short static body —
/// the local-verdict path (malformed request, consumer unwritable).
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
unsafe fn respond(
    sys: &SyscallTable,
    out_frames: i32,
    buf: &mut [u8; RESP_BUF],
    conn_id: u8,
    status: u16,
    body: &[u8],
) {
    let n = body.len().min(RESP_BUF - BODY_START);
    buf[BODY_START..BODY_START + n].copy_from_slice(&body[..n]);
    // SAFETY: caller guarantees `sys` is live.
    unsafe { emit_prepared(sys, out_frames, buf, conn_id, status, n) };
}

/// Parse one HTTP-classified client record `[conn_id][raw bytes]` and
/// emit it as MSG_HTTP_REQUEST on `http_out`. A record that does not
/// parse as a complete request within the consumer's path/body bounds
/// answers 400 on `frames_out` directly; a consumer port with no room
/// answers 503.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn forward_request(
    sys: &SyscallTable,
    out_http: i32,
    out_frames: i32,
    resp_buf: &mut [u8; RESP_BUF],
    payload: &[u8],
) {
    if payload.len() < 2 {
        return;
    }
    let conn_id = payload[0];
    let raw = &payload[1..];

    // Request line: `<METHOD> <path> HTTP/1.1`. The method token is at
    // most 7 bytes (OPTIONS / CONNECT); the method byte forwarded is
    // its first character, per the MSG_HTTP_REQUEST contract.
    let Some(m_end) = raw.iter().take(8).position(|&b| b == b' ') else {
        // SAFETY: `sys` is live per this function's contract.
        unsafe { respond(sys, out_frames, resp_buf, conn_id, 400, b"bad request") };
        return;
    };
    let p_start = m_end + 1;
    let Some(p_len) = raw[p_start..]
        .iter()
        .take(MAX_PATH + 1)
        .position(|&b| b == b' ')
        .filter(|&l| l > 0 && l <= MAX_PATH)
    else {
        // SAFETY: `sys` is live per this function's contract.
        unsafe { respond(sys, out_frames, resp_buf, conn_id, 400, b"bad request") };
        return;
    };
    let p_end = p_start + p_len;

    // Body: everything after the header terminator, within THIS
    // record. No terminator means no body (a GET's headers always
    // terminate in-record; see the module comment's one-record bound).
    let body = raw[p_end..]
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| &raw[p_end + p + 4..])
        .unwrap_or(&[]);
    if body.len() > MAX_BODY {
        // SAFETY: `sys` is live per this function's contract.
        unsafe { respond(sys, out_frames, resp_buf, conn_id, 400, b"body too large") };
        return;
    }

    if out_http < 0 {
        return;
    }
    // SAFETY: `sys` is live per this function's contract.
    unsafe {
        let poll = (sys.channel_poll)(out_http, 0x02);
        if poll <= 0 || (poll as u32 & 0x02) == 0 {
            respond(sys, out_frames, resp_buf, conn_id, 503, b"busy");
            return;
        }
        let mut env = [0u8; 3 + MAX_PATH + MAX_BODY];
        env[0] = conn_id;
        env[1] = raw[0];
        env[2] = p_len as u8;
        env[3..3 + p_len].copy_from_slice(&raw[p_start..p_end]);
        env[3 + p_len..3 + p_len + body.len()].copy_from_slice(body);
        wire::channel_write_msg(
            sys,
            out_http,
            wire::MSG_HTTP_REQUEST,
            &env[..3 + p_len + body.len()],
        );
    }
}

/// One drain of `http_responses_in`: each MSG_HTTP_RESPONSE is framed
/// as a wire-level HTTP/1.1 response on `frames_out`. Egress is gated
/// BEFORE consuming, so a response is never read off the channel with
/// nowhere to go. Returns the number of frames forwarded.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn drain_responses(
    sys: &SyscallTable,
    in_responses: i32,
    out_frames: i32,
    buf: &mut [u8; RESP_BUF],
) -> u32 {
    if in_responses < 0 || out_frames < 0 {
        return 0;
    }
    let mut worked = 0;
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        for _ in 0..RESP_DRAIN_BUDGET {
            let poll_out = (sys.channel_poll)(out_frames, 0x02);
            if poll_out <= 0 || (poll_out as u32 & 0x02) == 0 {
                break;
            }
            let poll = (sys.channel_poll)(in_responses, 0x01);
            if poll <= 0 || (poll as u32 & 0x01) == 0 {
                break;
            }
            let (mtype, plen) =
                wire::channel_read_msg(sys, in_responses, &mut buf[RESP_HDR_RESERVE..]);
            let n = plen as usize;
            if mtype != wire::MSG_HTTP_RESPONSE || n < 5 {
                continue;
            }
            let conn_id = buf[RESP_HDR_RESERVE];
            let status = u16::from_le_bytes([buf[RESP_HDR_RESERVE + 1], buf[RESP_HDR_RESERVE + 2]]);
            let body_len =
                u16::from_le_bytes([buf[RESP_HDR_RESERVE + 3], buf[RESP_HDR_RESERVE + 4]]) as usize;
            let body_len = body_len.min(n - 5);
            emit_prepared(sys, out_frames, buf, conn_id, status, body_len);
            worked += 1;
        }
    }
    worked
}
