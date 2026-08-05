// Bounded, no_std, no-alloc NATS protocol core — control-line framing, the
// client command builders, and the subscriber state machine. `include!`d by the
// host crate (tests) and the `nats` .fmod.
//
// NATS covers a protocol class the other connectors don't: SERVER-PUSHED pub/sub
// streaming. After the INFO/CONNECT/SUB handshake the client does not poll — the
// server pushes `MSG` frames whenever anyone publishes to a subscribed subject,
// indefinitely, interleaved with `PING` keepalives the client must answer with
// `PONG` or be disconnected. A request/reply codec cannot model an endpoint that
// emits unsolicited frames on its own schedule and demands liveness responses;
// that is a stateful, bidirectional session, so it is a compiled module.
//
// Wire form is text, CRLF-terminated control lines:
//   server: INFO {json}\r\n | MSG <subj> <sid> [reply] <#bytes>\r\n<payload>\r\n
//           | PING\r\n | PONG\r\n | +OK\r\n | -ERR <msg>\r\n
//   client: CONNECT {json}\r\n | SUB <subj> <sid>\r\n
//           | PUB <subj> <#bytes>\r\n<payload>\r\n | PONG\r\n

/// Find the index of the `\r` of the first CRLF at/after `from`, or `None`.
fn nats_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn nput(out: &mut [u8], pos: &mut usize, b: &[u8]) -> Option<()> {
    if *pos + b.len() > out.len() {
        return None;
    }
    out[*pos..*pos + b.len()].copy_from_slice(b);
    *pos += b.len();
    Some(())
}

/// Parse a decimal integer from ASCII bytes.
fn nats_atoi(b: &[u8]) -> Option<usize> {
    if b.is_empty() {
        return None;
    }
    let mut v = 0usize;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.wrapping_mul(10).wrapping_add((c - b'0') as usize);
    }
    Some(v)
}

// ---- client command builders ------------------------------------------------

/// `CONNECT {json}\r\n`. `verbose=false` keeps the server from echoing `+OK` on
/// every command. When `user`/`pass` are non-empty, they authenticate.
pub fn nats_connect(verbose: bool, user: &[u8], pass: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut p = 0;
    nput(out, &mut p, b"CONNECT {\"verbose\":")?;
    nput(out, &mut p, if verbose { b"true" } else { b"false" })?;
    nput(out, &mut p, b",\"pedantic\":false")?;
    if !user.is_empty() {
        nput(out, &mut p, b",\"user\":\"")?;
        nput(out, &mut p, user)?;
        nput(out, &mut p, b"\",\"pass\":\"")?;
        nput(out, &mut p, pass)?;
        nput(out, &mut p, b"\"")?;
    }
    nput(out, &mut p, b"}\r\n")?;
    Some(p)
}

/// `SUB <subject> <sid>\r\n`.
pub fn nats_sub(subject: &[u8], sid: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut p = 0;
    nput(out, &mut p, b"SUB ")?;
    nput(out, &mut p, subject)?;
    nput(out, &mut p, b" ")?;
    nput(out, &mut p, sid)?;
    nput(out, &mut p, b"\r\n")?;
    Some(p)
}

/// `PUB <subject> <#bytes>\r\n<payload>\r\n`.
pub fn nats_pub(subject: &[u8], payload: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut p = 0;
    nput(out, &mut p, b"PUB ")?;
    nput(out, &mut p, subject)?;
    nput(out, &mut p, b" ")?;
    let mut num = [0u8; 20];
    let n = itoa(payload.len() as i64, &mut num)?;
    nput(out, &mut p, &num[..n])?;
    nput(out, &mut p, b"\r\n")?;
    nput(out, &mut p, payload)?;
    nput(out, &mut p, b"\r\n")?;
    Some(p)
}

/// `PONG\r\n` — the keepalive answer.
pub fn nats_pong(out: &mut [u8]) -> Option<usize> {
    if out.len() < 6 {
        return None;
    }
    out[..6].copy_from_slice(b"PONG\r\n");
    Some(6)
}

// ---- server frame parsing ---------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NatsKind {
    Info,
    Msg,
    Ping,
    Pong,
    Ok,
    Err,
    Incomplete,
    Unknown,
}

/// One parsed server frame. For `Msg`, `subject`/`payload` are byte ranges into
/// the input; `total` is the whole frame length (header line + payload + CRLF).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NatsFrame {
    pub kind: NatsKind,
    pub subject_start: usize,
    pub subject_end: usize,
    pub payload_start: usize,
    pub payload_end: usize,
    pub total: usize,
}

impl NatsFrame {
    fn simple(kind: NatsKind, total: usize) -> Self {
        Self { kind, subject_start: 0, subject_end: 0, payload_start: 0, payload_end: 0, total }
    }
    fn incomplete() -> Self {
        Self::simple(NatsKind::Incomplete, 0)
    }
}

/// Parse the first complete server frame in `buf`. `Incomplete` when more bytes
/// are needed (control line not terminated, or a `MSG` payload not fully
/// arrived). Never panics.
pub fn nats_parse(buf: &[u8]) -> NatsFrame {
    let line_end = match nats_crlf(buf, 0) {
        Some(e) => e,
        None => return NatsFrame::incomplete(),
    };
    let line = &buf[..line_end];
    let after = line_end + 2;

    // Classify by the leading verb (case-insensitive for the keywords).
    if starts_ci(line, b"MSG ") {
        // MSG <subject> <sid> [reply] <#bytes>
        let mut it = TokenIter::new(&line[4..], 4);
        let (ss, se) = match it.next() {
            Some(t) => t,
            None => return NatsFrame::simple(NatsKind::Unknown, after),
        };
        // Remaining tokens: sid, [reply], nbytes — the LAST is the byte count.
        let mut last: Option<(usize, usize)> = None;
        while let Some(t) = it.next() {
            last = Some(t);
        }
        let (bs, be) = match last {
            Some(t) => t,
            None => return NatsFrame::simple(NatsKind::Unknown, after),
        };
        let nbytes = match nats_atoi(&buf[bs..be]) {
            Some(n) => n,
            None => return NatsFrame::simple(NatsKind::Unknown, after),
        };
        let payload_start = after;
        let total = payload_start + nbytes + 2; // payload + trailing CRLF
        if buf.len() < total {
            return NatsFrame::incomplete();
        }
        NatsFrame {
            kind: NatsKind::Msg,
            subject_start: ss,
            subject_end: se,
            payload_start,
            payload_end: payload_start + nbytes,
            total,
        }
    } else if starts_ci(line, b"INFO") {
        NatsFrame::simple(NatsKind::Info, after)
    } else if line == b"PING" {
        NatsFrame::simple(NatsKind::Ping, after)
    } else if line == b"PONG" {
        NatsFrame::simple(NatsKind::Pong, after)
    } else if line == b"+OK" {
        NatsFrame::simple(NatsKind::Ok, after)
    } else if starts_ci(line, b"-ERR") {
        NatsFrame::simple(NatsKind::Err, after)
    } else {
        NatsFrame::simple(NatsKind::Unknown, after)
    }
}

fn starts_ci(line: &[u8], prefix: &[u8]) -> bool {
    line.len() >= prefix.len()
        && line[..prefix.len()]
            .iter()
            .zip(prefix)
            .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// Iterate whitespace-separated tokens, yielding `(start, end)` byte ranges
/// relative to the ORIGINAL line (offset by `base`).
struct TokenIter<'a> {
    buf: &'a [u8],
    pos: usize,
    base: usize,
}
impl<'a> TokenIter<'a> {
    fn new(buf: &'a [u8], base: usize) -> Self {
        Self { buf, pos: 0, base }
    }
    fn next(&mut self) -> Option<(usize, usize)> {
        while self.pos < self.buf.len() && self.buf[self.pos] == b' ' {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            return None;
        }
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != b' ' {
            self.pos += 1;
        }
        Some((self.base + start, self.base + self.pos))
    }
}

// ---- subscriber state machine -----------------------------------------------

/// The subscriber lifecycle. `#[repr(u8)]` for `#[repr(C)]` module state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NPhase {
    Disconnected = 0,
    Connecting = 1,
    /// Connected; awaiting the server's opening `INFO`.
    AwaitInfo = 2,
    /// Subscribed; receiving server-pushed `MSG` frames.
    Ready = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NEv {
    Start,
    Connected,
    GotInfo,
    GotMsg,
    GotPing,
    GotErr,
    PeerClosed,
    NetError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NAct {
    None,
    Connect,
    /// Send CONNECT then the SUB — the client half of the opening handshake.
    SendConnectSub,
    /// Answer a server PING with PONG (liveness — miss it and get dropped).
    SendPong,
    /// Emit a pushed message's payload downstream.
    DeliverMsg,
    Fail,
}

/// The NATS subscriber state machine — pure and host-testable. The distinctive
/// part is `Ready`: it is a long-lived state that reacts to SERVER-INITIATED
/// events (pushed `MSG`, keepalive `PING`) rather than driving request/reply.
pub fn nats_transition(phase: NPhase, ev: NEv) -> (NAct, NPhase) {
    use NAct::*;
    use NEv::*;
    use NPhase::*;
    match (phase, ev) {
        (Disconnected, Start) => (Connect, Connecting),
        (Connecting, Connected) => (None, AwaitInfo),
        (AwaitInfo, GotInfo) => (SendConnectSub, Ready),
        // Long-lived subscription: server pushes messages and pings.
        (Ready, GotMsg) => (DeliverMsg, Ready),
        (Ready, GotPing) => (SendPong, Ready),
        (Ready, GotErr) => (Fail, Disconnected),
        (_, PeerClosed) | (_, NetError) => (Fail, Disconnected),
        _ => (None, phase),
    }
}
