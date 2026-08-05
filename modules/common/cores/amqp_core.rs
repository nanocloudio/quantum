// Bounded, no_std, no-alloc AMQP 0-9-1 protocol core — frame framing, the
// connection-handshake method builders/parsers, and the handshake state machine.
// `include!`d by the host crate (tests) and the `amqp` .fmod.
//
// AMQP adds a structural class the other connectors don't: CHANNEL MULTIPLEXING.
// After a multi-step connection handshake (protocol-header -> Start/Start-Ok ->
// Tune/Tune-Ok -> Open/Open-Ok) the client opens logical CHANNELS over the single
// TCP connection, and every frame is tagged with a channel number so many
// independent conversations share one socket. Modelling a connection whose frames
// belong to different logical streams, negotiated across a five-message
// handshake, is not something a stateless request/reply codec can express.
//
// Frame: [type:u8][channel:u16 BE][size:u32 BE][payload][frame-end:0xCE]
// Method payload: [class-id:u16 BE][method-id:u16 BE][args]

pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
pub const FRAME_END: u8 = 0xCE;

/// Class/method identifiers used by the connection handshake.
pub mod method {
    pub const CONNECTION: u16 = 10;
    pub const CHANNEL: u16 = 20;
    pub const CONN_START: u16 = 10;
    pub const CONN_START_OK: u16 = 11;
    pub const CONN_TUNE: u16 = 30;
    pub const CONN_TUNE_OK: u16 = 31;
    pub const CONN_OPEN: u16 = 40;
    pub const CONN_OPEN_OK: u16 = 41;
    pub const CONN_CLOSE: u16 = 50;
    pub const CHANNEL_OPEN: u16 = 10;
    pub const CHANNEL_OPEN_OK: u16 = 11;
    pub const CHANNEL_CLOSE: u16 = 40;
}

fn aput(out: &mut [u8], pos: &mut usize, b: &[u8]) -> Option<()> {
    if *pos + b.len() > out.len() {
        return None;
    }
    out[*pos..*pos + b.len()].copy_from_slice(b);
    *pos += b.len();
    Some(())
}
fn au16(out: &mut [u8], pos: &mut usize, v: u16) -> Option<()> {
    aput(out, pos, &v.to_be_bytes())
}
fn au32(out: &mut [u8], pos: &mut usize, v: u32) -> Option<()> {
    aput(out, pos, &v.to_be_bytes())
}
/// AMQP short-string: `[len:u8][bytes]`.
fn ashortstr(out: &mut [u8], pos: &mut usize, s: &[u8]) -> Option<()> {
    if s.len() > 255 {
        return None;
    }
    aput(out, pos, &[s.len() as u8])?;
    aput(out, pos, s)
}
/// AMQP long-string: `[len:u32 BE][bytes]`.
fn alongstr(out: &mut [u8], pos: &mut usize, s: &[u8]) -> Option<()> {
    au32(out, pos, s.len() as u32)?;
    aput(out, pos, s)
}

/// The 8-byte protocol header the client sends first: `AMQP\0\0\x09\x01`.
pub fn amqp_protocol_header(out: &mut [u8]) -> Option<usize> {
    if out.len() < 8 {
        return None;
    }
    out[..8].copy_from_slice(b"AMQP\x00\x00\x09\x01");
    Some(8)
}

/// Wrap a method (`class`,`method`,`args`) as a complete frame on `channel`.
fn amqp_method_frame(
    channel: u16,
    class: u16,
    method: u16,
    args: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let size = 4 + args.len(); // class(2) + method(2) + args
    let total = 1 + 2 + 4 + size + 1;
    if total > out.len() {
        return None;
    }
    let mut p = 0;
    aput(out, &mut p, &[FRAME_METHOD])?;
    au16(out, &mut p, channel)?;
    au32(out, &mut p, size as u32)?;
    au16(out, &mut p, class)?;
    au16(out, &mut p, method)?;
    aput(out, &mut p, args)?;
    aput(out, &mut p, &[FRAME_END])?;
    Some(p)
}

/// Connection.Start-Ok: empty client-properties, `PLAIN` mechanism, the
/// `\0user\0pass` response as a long-string, `en_US` locale.
pub fn amqp_start_ok(user: &[u8], pass: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut args = [0u8; 256];
    let mut p = 0;
    au32(&mut args, &mut p, 0)?; // client-properties: empty field-table
    ashortstr(&mut args, &mut p, b"PLAIN")?;
    // response = \0 user \0 pass, as a long-string
    let mut resp = [0u8; 130];
    let mut rp = 0;
    aput(&mut resp, &mut rp, &[0])?;
    aput(&mut resp, &mut rp, user)?;
    aput(&mut resp, &mut rp, &[0])?;
    aput(&mut resp, &mut rp, pass)?;
    alongstr(&mut args, &mut p, &resp[..rp])?;
    ashortstr(&mut args, &mut p, b"en_US")?;
    amqp_method_frame(0, method::CONNECTION, method::CONN_START_OK, &args[..p], out)
}

/// Connection.Tune-Ok: echo the server's negotiated `channel-max`, `frame-max`,
/// `heartbeat`.
pub fn amqp_tune_ok(channel_max: u16, frame_max: u32, heartbeat: u16, out: &mut [u8]) -> Option<usize> {
    let mut args = [0u8; 8];
    let mut p = 0;
    au16(&mut args, &mut p, channel_max)?;
    au32(&mut args, &mut p, frame_max)?;
    au16(&mut args, &mut p, heartbeat)?;
    amqp_method_frame(0, method::CONNECTION, method::CONN_TUNE_OK, &args[..p], out)
}

/// Connection.Open: `virtual-host` short-string, empty reserved-1, reserved-2 bit.
pub fn amqp_open(vhost: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut args = [0u8; 64];
    let mut p = 0;
    ashortstr(&mut args, &mut p, vhost)?;
    ashortstr(&mut args, &mut p, b"")?; // reserved-1
    aput(&mut args, &mut p, &[0])?; // reserved-2 (bit)
    amqp_method_frame(0, method::CONNECTION, method::CONN_OPEN, &args[..p], out)
}

/// Channel.Open on `channel`: a single empty reserved-1 short-string.
pub fn amqp_channel_open(channel: u16, out: &mut [u8]) -> Option<usize> {
    let mut args = [0u8; 8];
    let mut p = 0;
    ashortstr(&mut args, &mut p, b"")?;
    amqp_method_frame(channel, method::CHANNEL, method::CHANNEL_OPEN, &args[..p], out)
}

// ---- frame parsing ----------------------------------------------------------

/// A parsed frame view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmqpFrame {
    pub ftype: u8,
    pub channel: u16,
    pub payload_start: usize,
    pub payload_end: usize,
    pub total: usize,
}

/// Frame the first complete AMQP frame in `buf`, or `None` if truncated or the
/// frame-end octet is wrong. Never panics.
pub fn amqp_parse_frame(buf: &[u8]) -> Option<AmqpFrame> {
    if buf.len() < 7 {
        return None;
    }
    let ftype = buf[0];
    let channel = u16::from_be_bytes([buf[1], buf[2]]);
    let size = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as usize;
    let total = 7 + size + 1; // header + payload + frame-end
    if buf.len() < total {
        return None;
    }
    if buf[total - 1] != FRAME_END {
        return None;
    }
    Some(AmqpFrame { ftype, channel, payload_start: 7, payload_end: 7 + size, total })
}

/// `(class_id, method_id)` of a METHOD frame's payload, or `None`.
pub fn amqp_method_id(payload: &[u8]) -> Option<(u16, u16)> {
    if payload.len() < 4 {
        return None;
    }
    Some((
        u16::from_be_bytes([payload[0], payload[1]]),
        u16::from_be_bytes([payload[2], payload[3]]),
    ))
}

/// Parse a Connection.Tune method payload's `(channel-max, frame-max, heartbeat)`
/// (the args follow the 4-byte class/method id).
pub fn amqp_parse_tune(payload: &[u8]) -> Option<(u16, u32, u16)> {
    if payload.len() < 4 + 2 + 4 + 2 {
        return None;
    }
    let a = &payload[4..];
    let channel_max = u16::from_be_bytes([a[0], a[1]]);
    let frame_max = u32::from_be_bytes([a[2], a[3], a[4], a[5]]);
    let heartbeat = u16::from_be_bytes([a[6], a[7]]);
    Some((channel_max, frame_max, heartbeat))
}

// ---- handshake state machine ------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum APhase {
    Disconnected = 0,
    Connecting = 1,
    AwaitStart = 2,
    AwaitTune = 3,
    AwaitOpenOk = 4,
    AwaitChannelOk = 5,
    /// Connection open and channel 1 established — ready to multiplex.
    Ready = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AEv {
    Start,
    Connected,
    GotStart,
    GotTune,
    GotOpenOk,
    GotChannelOk,
    GotClose,
    PeerClosed,
    NetError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AAct {
    None,
    Connect,
    SendProtocolHeader,
    SendStartOk,
    /// Tune-Ok immediately followed by Connection.Open.
    SendTuneOkOpen,
    SendChannelOpen,
    Fail,
}

/// Map a received method `(class, method)` to the handshake event it drives.
pub fn amqp_classify(class: u16, meth: u16) -> Option<AEv> {
    use method::*;
    match (class, meth) {
        (CONNECTION, CONN_START) => Some(AEv::GotStart),
        (CONNECTION, CONN_TUNE) => Some(AEv::GotTune),
        (CONNECTION, CONN_OPEN_OK) => Some(AEv::GotOpenOk),
        (CHANNEL, CHANNEL_OPEN_OK) => Some(AEv::GotChannelOk),
        (CONNECTION, CONN_CLOSE) | (CHANNEL, CHANNEL_CLOSE) => Some(AEv::GotClose),
        _ => None,
    }
}

/// The AMQP connection-handshake state machine — pure and host-testable. Five
/// negotiated messages bring the connection up; then a Channel.Open establishes
/// the first logical channel for multiplexed traffic.
pub fn amqp_transition(phase: APhase, ev: AEv) -> (AAct, APhase) {
    use AAct::*;
    use AEv::*;
    use APhase::*;
    match (phase, ev) {
        (Disconnected, Start) => (Connect, Connecting),
        (Connecting, Connected) => (SendProtocolHeader, AwaitStart),
        (AwaitStart, GotStart) => (SendStartOk, AwaitTune),
        (AwaitTune, GotTune) => (SendTuneOkOpen, AwaitOpenOk),
        (AwaitOpenOk, GotOpenOk) => (SendChannelOpen, AwaitChannelOk),
        (AwaitChannelOk, GotChannelOk) => (None, Ready),
        (_, GotClose) | (_, PeerClosed) | (_, NetError) => (Fail, Disconnected),
        _ => (None, phase),
    }
}
