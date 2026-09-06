//! `quantum-mqtt-loadgen` — off-DUT open-loop MQTT load generator.
//!
//! Drives the broker over the real MQTT codec path (plaintext TCP today; TLS
//! is a follow-up for the Pi 5 ingress). One TCP connection per shard, each
//! issuing a fixed-interval (Poisson-free) arrival stream at its share of the
//! offered rate, with its own latency histogram merged at the end.
//!
//! Latency is **coordinated-omission corrected**: each PUBLISH's latency is
//! measured from its *intended* send time, so a stalled broker shows a growing
//! tail instead of pacing our loop. The headroom verdict flags a run the
//! generator (not the DUT) bottlenecked as `HARNESS_BOUND`.
//!
//! QoS 1: send PUBLISH, await matching PUBACK, record the round-trip.
//! Closed loop by default (one PUBLISH outstanding per connection), which
//! makes throughput a function of the round trip: 64 connections at 50 ms
//! cannot exceed 1280/s however much is offered. `--inflight N` opens the
//! loop — each connection keeps up to N PUBLISHes outstanding, paced at its
//! share of `--rate`, with PUBACKs matched in order off a reader thread —
//! so the offered rate, not the ack latency, sets the load.
//! QoS 0: fire-and-forget; latency is the send-call cost only, and
//! offered==accepted accounting must be cross-checked against broker /metrics
//! (the broker has no ack to confirm delivery).
//!
//! Usage:
//!   quantum-mqtt-loadgen --host 127.0.0.1:9090 --rate 2000 --duration 10 \
//!       --conns 8 --qos 1 --size 128 --topic bench/t [--inflight 32]
//!       [--fleet 4]   (worker threads multiplexing every connection)

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use quantum_bench::{JsonObj, LatencyHist};

struct Args {
    host: String,
    rate: u64,
    duration_secs: u64,
    conns: u64,
    qos: u8,
    size: usize,
    topic: String,
    /// Spread connections over this many `<topic>/<n>` topics (1 = one topic).
    topics: u64,
    /// QoS 1 PUBLISHes a connection may have outstanding. 1 is the
    /// closed loop; more opens it.
    inflight: u32,
    /// Fleet mode: this many worker threads multiplex every connection
    /// (0 = one thread per connection). For thousands of connections
    /// each publishing rarely.
    fleet: u64,
}

fn usage() -> ! {
    eprintln!(
        "quantum-mqtt-loadgen --host <addr:port> --rate <msg/s> [--duration N]\n\
         \x20  [--conns N] [--qos 0|1|2] [--size <bytes>] [--topic <name>] [--topics N]\n\
         \x20  [--inflight N]   (QoS 1: PUBLISHes outstanding per connection; 1 = closed loop)\n\
         \x20  [--fleet W]      (W worker threads multiplex all connections; QoS 0/1 only)"
    );
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut a = Args {
        host: String::new(),
        rate: 1000,
        duration_secs: 10,
        conns: 4,
        qos: 1,
        size: 128,
        topic: "bench/t".to_string(),
        topics: 1,
        inflight: 1,
        fleet: 0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut next = || it.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--host" => a.host = next(),
            "--rate" => a.rate = next().parse().unwrap_or_else(|_| usage()),
            "--duration" => a.duration_secs = next().parse().unwrap_or_else(|_| usage()),
            "--conns" => a.conns = next().parse().unwrap_or_else(|_| usage()),
            "--qos" => a.qos = next().parse().unwrap_or_else(|_| usage()),
            "--size" => a.size = next().parse().unwrap_or_else(|_| usage()),
            "--topic" => a.topic = next(),
            "--topics" => a.topics = next().parse().unwrap_or_else(|_| usage()),
            "--inflight" => a.inflight = next().parse().unwrap_or_else(|_| usage()),
            "--fleet" => a.fleet = next().parse().unwrap_or_else(|_| usage()),
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown flag: {other}");
                usage();
            }
        }
    }
    if a.host.is_empty() || a.rate == 0 || a.conns == 0 || a.qos > 2 || a.inflight == 0 {
        usage();
    }
    if a.inflight > 1 && a.qos != 1 {
        eprintln!("--inflight applies to QoS 1 only");
        usage();
    }
    if a.fleet > 0 && a.qos == 2 {
        eprintln!("--fleet supports QoS 0 and 1 only");
        usage();
    }
    a
}

/// The topic connection `idx` publishes to. With `--topics N` (N > 1)
/// each connection takes `<topic>/<idx mod N>`, so the offered load
/// spreads over N shards — and at K>1 over up to N raft groups —
/// instead of serialising through one. One topic is one log.
fn shard_topic(base: &str, topics: u64, idx: u64) -> String {
    if topics <= 1 {
        base.to_string()
    } else {
        format!("{base}/{}", idx % topics)
    }
}

// ── MQTT 3.1.1 framing helpers ──────────────────────────────────────────

fn encode_remaining_len(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (n & 0x7f) as u8;
        n >>= 7;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
}

fn connect_packet(client_id: &str) -> Vec<u8> {
    let mut var = Vec::new();
    var.extend_from_slice(&[0x00, 0x04]);
    var.extend_from_slice(b"MQTT");
    var.push(0x04); // protocol level 4 (3.1.1)
    var.push(0x02); // connect flags: clean session
    var.extend_from_slice(&[0x00, 0x3c]); // keep-alive 60s
    var.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    var.extend_from_slice(client_id.as_bytes());
    let mut pkt = vec![0x10u8];
    encode_remaining_len(var.len(), &mut pkt);
    pkt.extend_from_slice(&var);
    pkt
}

fn publish_packet(topic: &str, payload: &[u8], qos: u8, packet_id: u16) -> Vec<u8> {
    let mut rem = Vec::new();
    rem.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    rem.extend_from_slice(topic.as_bytes());
    if qos > 0 {
        rem.extend_from_slice(&packet_id.to_be_bytes());
    }
    rem.extend_from_slice(payload);
    let hdr = 0x30u8 | (qos << 1);
    let mut pkt = vec![hdr];
    encode_remaining_len(rem.len(), &mut pkt);
    pkt.extend_from_slice(&rem);
    pkt
}

/// PUBREL (QoS 2 phase 3): fixed header 0x62, remaining length 2, packet id.
fn pubrel_packet(packet_id: u16) -> [u8; 4] {
    let pid = packet_id.to_be_bytes();
    [0x62, 0x02, pid[0], pid[1]]
}

/// Read one whole MQTT control packet (fixed header + remaining-length varint +
/// body). Returns the first header byte and the body, or an error string.
fn read_packet(s: &mut TcpStream) -> Result<(u8, Vec<u8>), String> {
    let mut h = [0u8; 1];
    s.read_exact(&mut h).map_err(|e| format!("hdr: {e}"))?;
    let mut mult = 1usize;
    let mut len = 0usize;
    for _ in 0..4 {
        let mut b = [0u8; 1];
        s.read_exact(&mut b).map_err(|e| format!("rl: {e}"))?;
        len += (b[0] & 0x7f) as usize * mult;
        if b[0] & 0x80 == 0 {
            break;
        }
        mult *= 128;
    }
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).map_err(|e| format!("body: {e}"))?;
    Ok((h[0], body))
}

struct ShardResult {
    hist: LatencyHist,
    sent: u64,
    ok: u64,
    errors: u64,
    /// Open loop only: acks that arrived for a PUBLISH other than the
    /// oldest outstanding one. Matched by id, so nothing is lost to
    /// reordering; the count says the broker acked out of order.
    reordered: u64,
    /// Open loop only: PUBLISHes still unacked when the connection's
    /// reader gave up (read timeout or close), or at the end of the
    /// drain window.
    unacked: u64,
}

/// One connection's share of the run.
struct ShardSpec {
    idx: u64,
    per_shard_rate: f64,
    duration: Duration,
    qos: u8,
    inflight: u32,
    payload: Vec<u8>,
    topic: String,
}

/// Read PUBACK packet ids off a connection until it closes or times out.
/// The writer matches them in order: MQTT delivers one connection's acks
/// in the order the PUBLISHes were sent.
fn puback_reader(mut s: TcpStream, tx: mpsc::Sender<Option<u16>>) {
    loop {
        match read_packet(&mut s) {
            Ok((0x40, body)) if body.len() >= 2 => {
                if tx
                    .send(Some(u16::from_be_bytes([body[0], body[1]])))
                    .is_err()
                {
                    return;
                }
            }
            Ok(_) => {}
            Err(_) => {
                let _ = tx.send(None);
                return;
            }
        }
    }
}

/// Open-loop QoS 1: PUBLISHes go out on the pace regardless of acks, up
/// to `inflight` outstanding; each ack retires the oldest outstanding
/// PUBLISH and its latency is measured from that PUBLISH's intended send
/// time. When the window is full the pace waits for an ack — that wait
/// counts against the latency of everything behind it, as with any
/// coordinated-omission-corrected measurement. After the run, acks still
/// owed are given a drain window; whatever is unacked then is an error.
fn run_shard_open(mut s: TcpStream, spec: ShardSpec, mut hist: LatencyHist) -> ShardResult {
    let ShardSpec {
        idx,
        per_shard_rate,
        duration,
        inflight,
        payload,
        topic,
        ..
    } = spec;
    let (mut sent, mut ok, mut errors) = (0u64, 0u64, 0u64);
    let (mut unacked, mut reordered) = (0u64, 0u64);
    let reader = match s.try_clone() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[shard {idx}] clone failed: {e}");
            return ShardResult {
                hist,
                sent,
                ok,
                errors,
                unacked: 0,
                reordered: 0,
            };
        }
    };
    let (tx, rx) = mpsc::channel::<Option<u16>>();
    let reader = std::thread::spawn(move || puback_reader(reader, tx));
    let mut outstanding: std::collections::VecDeque<(u16, Instant)> =
        std::collections::VecDeque::with_capacity(inflight as usize);
    let mut closed = false;
    // Retire one ack. `None` from the reader means the connection is
    // gone: everything outstanding is lost.
    let take = |ack: Option<u16>,
                outstanding: &mut std::collections::VecDeque<(u16, Instant)>,
                hist: &mut LatencyHist,
                ok: &mut u64,
                unacked: &mut u64,
                reordered: &mut u64,
                closed: &mut bool| {
        match ack {
            Some(pid) => {
                // Match by id anywhere in the window: an ack out of send
                // order is counted, not treated as loss. An id not in the
                // window at all is a stray (a duplicate, or an ack for a
                // PUBLISH already retired) and is ignored.
                let Some(at) = outstanding.iter().position(|&(id, _)| id == pid) else {
                    return;
                };
                if at != 0 {
                    *reordered += 1;
                }
                if let Some((_, intended)) = outstanding.remove(at) {
                    let lat = Instant::now().saturating_duration_since(intended);
                    hist.record(lat.as_micros() as u64);
                    *ok += 1;
                }
            }
            None => {
                *unacked += outstanding.len() as u64;
                outstanding.clear();
                *closed = true;
            }
        }
    };

    let interval = Duration::from_secs_f64(1.0 / per_shard_rate);
    let start = Instant::now();
    let mut i: u64 = 0;
    while !closed {
        let intended = start + interval * (i as u32);
        let now = Instant::now();
        if now >= start + duration {
            break;
        }
        if intended > now {
            std::thread::sleep(intended - now);
        }
        while let Ok(ack) = rx.try_recv() {
            take(
                ack,
                &mut outstanding,
                &mut hist,
                &mut ok,
                &mut unacked,
                &mut reordered,
                &mut closed,
            );
        }
        while !closed && outstanding.len() >= inflight as usize {
            match rx.recv() {
                Ok(ack) => take(
                    ack,
                    &mut outstanding,
                    &mut hist,
                    &mut ok,
                    &mut unacked,
                    &mut reordered,
                    &mut closed,
                ),
                Err(_) => closed = true,
            }
        }
        if closed {
            break;
        }
        let packet_id = ((i % 65535) + 1) as u16;
        let pkt = publish_packet(&topic, &payload, 1, packet_id);
        sent += 1;
        i += 1;
        if s.write_all(&pkt).is_err() {
            errors += 1;
            continue;
        }
        outstanding.push_back((packet_id, intended));
    }
    // Drain: acks for the tail of the run are still in flight.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !closed && !outstanding.is_empty() {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        match rx.recv_timeout(left) {
            Ok(ack) => take(
                ack,
                &mut outstanding,
                &mut hist,
                &mut ok,
                &mut unacked,
                &mut reordered,
                &mut closed,
            ),
            Err(_) => break,
        }
    }
    unacked += outstanding.len() as u64;
    errors += unacked;
    let _ = s.shutdown(std::net::Shutdown::Both);
    let _ = reader.join();
    ShardResult {
        hist,
        sent,
        ok,
        errors,
        unacked,
        reordered,
    }
}

fn run_shard(host: String, spec: ShardSpec) -> ShardResult {
    let idx = spec.idx;
    let mut hist = LatencyHist::new();
    let (mut sent, mut ok, mut errors) = (0u64, 0u64, 0u64);

    let mut s = match TcpStream::connect(&host) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[shard {idx}] connect failed: {e}");
            return ShardResult {
                hist,
                sent,
                ok,
                errors,
                unacked: 0,
                reordered: 0,
            };
        }
    };
    let _ = s.set_nodelay(true);
    let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = s.set_write_timeout(Some(Duration::from_secs(5)));

    // CONNECT → CONNACK handshake.
    if s.write_all(&connect_packet(&format!("ldg-{idx}"))).is_err() {
        return ShardResult {
            hist,
            sent,
            ok,
            errors,
            unacked: 0,
            reordered: 0,
        };
    }
    match read_packet(&mut s) {
        Ok((0x20, body)) if body.len() >= 2 && body[1] == 0 => {}
        _ => {
            eprintln!("[shard {idx}] CONNACK failed");
            return ShardResult {
                hist,
                sent,
                ok,
                errors,
                unacked: 0,
                reordered: 0,
            };
        }
    }

    if spec.qos == 1 && spec.inflight > 1 {
        return run_shard_open(s, spec, hist);
    }
    let ShardSpec {
        per_shard_rate,
        duration,
        qos,
        payload,
        topic,
        ..
    } = spec;

    let interval = Duration::from_secs_f64(1.0 / per_shard_rate);
    let start = Instant::now();
    let mut i: u64 = 0;
    loop {
        let intended = start + interval * (i as u32);
        let now = Instant::now();
        if now >= start + duration {
            break;
        }
        if intended > now {
            std::thread::sleep(intended - now);
        }
        let packet_id = ((i % 65535) + 1) as u16;
        let pkt = publish_packet(&topic, &payload, qos, packet_id);
        if s.write_all(&pkt).is_err() {
            errors += 1;
            i += 1;
            sent += 1;
            continue;
        }
        sent += 1;
        match qos {
            1 => match read_packet(&mut s) {
                // PUBACK
                Ok((0x40, _)) => {
                    let lat = Instant::now().saturating_duration_since(intended);
                    hist.record(lat.as_micros() as u64);
                    ok += 1;
                }
                _ => errors += 1,
            },
            2 => {
                // Four-phase exactly-once: PUBLISH → PUBREC → PUBREL → PUBCOMP.
                // Latency spans the whole exchange (two durable round-trips).
                match read_packet(&mut s) {
                    Ok((0x50, _)) => {
                        if s.write_all(&pubrel_packet(packet_id)).is_ok() {
                            match read_packet(&mut s) {
                                Ok((0x70, _)) => {
                                    let lat = Instant::now().saturating_duration_since(intended);
                                    hist.record(lat.as_micros() as u64);
                                    ok += 1;
                                }
                                _ => errors += 1,
                            }
                        } else {
                            errors += 1;
                        }
                    }
                    _ => errors += 1,
                }
            }
            _ => {
                // QoS 0: record the send-call cost; delivery is confirmed via
                // broker /metrics, not an ack.
                let lat = Instant::now().saturating_duration_since(intended);
                hist.record(lat.as_micros() as u64);
                ok += 1;
            }
        }
        i += 1;
    }
    ShardResult {
        hist,
        sent,
        ok,
        errors,
        unacked: 0,
        reordered: 0,
    }
}

// ── Fleet mode: many connections, few threads ───────────────────────────
//
// A thread per connection stops scaling at a few thousand sockets. Fleet
// mode gives each worker thread a slice of the connections on
// non-blocking sockets and drives them from one loop: a publish goes out
// when its connection's pace says so, and only connections with an ack
// outstanding are read at all. An idle connection therefore costs no
// system call, which is what makes ten thousand of them affordable on
// the generator host.

/// One connection in a fleet worker.
struct FleetConn {
    stream: TcpStream,
    idx: u64,
    topic: String,
    /// Publishes sent so far; the next one is number `seq`.
    seq: u64,
    /// Oldest first: `(packet_id, intended send instant)`.
    outstanding: VecDeque<(u16, Instant)>,
    rx: Vec<u8>,
    sent: u64,
    ok: u64,
    errors: u64,
    reordered: u64,
    unacked: u64,
    /// Socket failed; no further sends.
    dead: bool,
}

/// How long an ack may stay owed before the publish is written off.
const FLEET_ACK_TIMEOUT: Duration = Duration::from_secs(5);
/// Connections opened per second across the whole fleet, so the broker's
/// per-tick accept budget is not the first thing measured.
const FLEET_CONNECT_RATE: f64 = 400.0;
/// Upper bound on one fleet connect (SYN to established).
const FLEET_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Connect and handshake one fleet connection. Blocking with timeouts
/// for the handshake, non-blocking afterwards. The connect itself is
/// bounded too: a broker whose connection table is full drops the SYN,
/// and the host's own retransmit ladder would otherwise hold each such
/// connect for two minutes, serialised behind the worker's remaining
/// ramp.
fn fleet_connect(host: &str, idx: u64, topic: String) -> Option<FleetConn> {
    let addr = match host.to_socket_addrs().ok().and_then(|mut a| a.next()) {
        Some(a) => a,
        None => {
            eprintln!("[fleet {idx}] unresolvable host: {host}");
            return None;
        }
    };
    let mut s = match TcpStream::connect_timeout(&addr, FLEET_CONNECT_TIMEOUT) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[fleet {idx}] connect failed: {e}");
            return None;
        }
    };
    let _ = s.set_nodelay(true);
    let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = s.set_write_timeout(Some(Duration::from_secs(5)));
    if s.write_all(&connect_packet(&format!("ldg-{idx}"))).is_err() {
        return None;
    }
    match read_packet(&mut s) {
        Ok((0x20, body)) if body.len() >= 2 && body[1] == 0 => {}
        _ => {
            eprintln!("[fleet {idx}] CONNACK failed");
            return None;
        }
    }
    if s.set_nonblocking(true).is_err() {
        return None;
    }
    Some(FleetConn {
        stream: s,
        idx,
        topic,
        seq: 0,
        outstanding: VecDeque::new(),
        rx: Vec::with_capacity(256),
        sent: 0,
        ok: 0,
        errors: 0,
        reordered: 0,
        unacked: 0,
        dead: false,
    })
}

/// Take every complete packet out of `c.rx`, retiring PUBACKs.
fn fleet_drain_rx(c: &mut FleetConn, hist: &mut LatencyHist) {
    let mut off = 0usize;
    loop {
        let buf = &c.rx[off..];
        if buf.len() < 2 {
            break;
        }
        let mut mult = 1usize;
        let mut len = 0usize;
        let mut hdr = 1usize;
        let mut complete = false;
        for k in 0..4 {
            if 1 + k >= buf.len() {
                break;
            }
            let b = buf[1 + k];
            len += (b & 0x7f) as usize * mult;
            hdr = 2 + k;
            if b & 0x80 == 0 {
                complete = true;
                break;
            }
            mult *= 128;
        }
        if !complete || buf.len() < hdr + len {
            break;
        }
        if buf[0] == 0x40 && len >= 2 {
            let pid = u16::from_be_bytes([buf[hdr], buf[hdr + 1]]);
            let now = Instant::now();
            if let Some(pos) = c.outstanding.iter().position(|&(id, _)| id == pid) {
                if pos != 0 {
                    c.reordered += 1;
                }
                let (_, intended) = c.outstanding.remove(pos).unwrap();
                hist.record(now.saturating_duration_since(intended).as_micros() as u64);
                c.ok += 1;
            }
        }
        off += hdr + len;
    }
    if off > 0 {
        c.rx.drain(..off);
    }
}

/// Read whatever the socket holds without blocking. False when the
/// socket is gone.
fn fleet_read(c: &mut FleetConn, hist: &mut LatencyHist) -> bool {
    let mut tmp = [0u8; 512];
    loop {
        match c.stream.read(&mut tmp) {
            Ok(0) => return false,
            Ok(n) => c.rx.extend_from_slice(&tmp[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return false,
        }
    }
    fleet_drain_rx(c, hist);
    true
}

/// Write off every ack owed longer than `FLEET_ACK_TIMEOUT`.
fn fleet_expire(c: &mut FleetConn, now: Instant) {
    while let Some(&(_, intended)) = c.outstanding.front() {
        if now.saturating_duration_since(intended) < FLEET_ACK_TIMEOUT {
            break;
        }
        c.outstanding.pop_front();
        c.unacked += 1;
        c.errors += 1;
    }
}

struct FleetSpec {
    host: String,
    /// Connection indices this worker owns.
    first: u64,
    count: u64,
    conns_total: u64,
    per_conn_interval: Duration,
    duration: Duration,
    qos: u8,
    inflight: u32,
    payload: Vec<u8>,
    topic: String,
    topics: u64,
    /// Fleet-wide connect pacing: this worker opens one connection every
    /// `connect_gap`, offset by `connect_offset`.
    connect_gap: Duration,
    connect_offset: Duration,
}

fn run_fleet_worker(spec: FleetSpec) -> ShardResult {
    let mut hist = LatencyHist::new();
    let mut conns: Vec<FleetConn> = Vec::with_capacity(spec.count as usize);
    let mut errors = 0u64;
    let t0 = Instant::now() + spec.connect_offset;
    for k in 0..spec.count {
        let at = t0 + spec.connect_gap * (k as u32);
        let now = Instant::now();
        if at > now {
            std::thread::sleep(at - now);
        }
        let idx = spec.first + k;
        match fleet_connect(&spec.host, idx, shard_topic(&spec.topic, spec.topics, idx)) {
            Some(c) => conns.push(c),
            None => errors += 1,
        }
    }
    let n = conns.len();
    // Every connection's first publish is staggered across one interval
    // so the fleet's offered rate is smooth from the first second.
    let start = Instant::now();
    let stagger = |c: &FleetConn| {
        spec.per_conn_interval
            .mul_f64((c.idx as f64) / (spec.conns_total.max(1) as f64))
    };
    let end = start + spec.duration;
    let mut pending: Vec<usize> = Vec::new();
    loop {
        let now = Instant::now();
        let sending = now < end;
        // Sends due now.
        if sending {
            for (i, c) in conns.iter_mut().enumerate() {
                if c.dead {
                    continue;
                }
                let due = start + stagger(c) + spec.per_conn_interval * (c.seq as u32);
                if due > now {
                    continue;
                }
                if spec.qos == 1 && c.outstanding.len() >= spec.inflight as usize {
                    continue;
                }
                let packet_id = ((c.seq % 65535) + 1) as u16;
                let pkt = publish_packet(&c.topic, &spec.payload, spec.qos, packet_id);
                c.seq += 1;
                c.sent += 1;
                match c.stream.write_all(&pkt) {
                    Ok(()) => {
                        if spec.qos == 1 {
                            c.outstanding.push_back((packet_id, due));
                            if c.outstanding.len() == 1 {
                                pending.push(i);
                            }
                        } else {
                            hist.record(now.saturating_duration_since(due).as_micros() as u64);
                            c.ok += 1;
                        }
                    }
                    Err(e) => {
                        c.errors += 1;
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            c.dead = true;
                        }
                    }
                }
            }
        }
        // Reads only where an ack is owed.
        let now = Instant::now();
        pending.retain(|&i| {
            let c = &mut conns[i];
            if !c.dead && !fleet_read(c, &mut hist) {
                c.dead = true;
                c.unacked += c.outstanding.len() as u64;
                c.errors += c.outstanding.len() as u64;
                c.outstanding.clear();
            }
            fleet_expire(c, now);
            !c.outstanding.is_empty()
        });
        if !sending && (pending.is_empty() || now >= end + FLEET_ACK_TIMEOUT) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut r = ShardResult {
        hist,
        sent: 0,
        ok: 0,
        errors,
        unacked: 0,
        reordered: 0,
    };
    for c in &mut conns {
        r.unacked += c.outstanding.len() as u64;
        r.errors += c.outstanding.len() as u64;
        r.sent += c.sent;
        r.ok += c.ok;
        r.errors += c.errors;
        r.unacked += c.unacked;
        r.reordered += c.reordered;
    }
    eprintln!(
        "[fleet] worker first={} live={n}/{}",
        spec.first, spec.count
    );
    r
}

fn run_fleet(
    a: &Args,
    tx: &mpsc::Sender<ShardResult>,
    handles: &mut Vec<std::thread::JoinHandle<()>>,
) {
    let workers = a.fleet.min(a.conns).max(1);
    let per_conn_interval = Duration::from_secs_f64(a.conns as f64 / a.rate as f64);
    let connect_gap = Duration::from_secs_f64(workers as f64 / FLEET_CONNECT_RATE);
    let base = a.conns / workers;
    let extra = a.conns % workers;
    let mut first = 0u64;
    for w in 0..workers {
        let count = base + u64::from(w < extra);
        let spec = FleetSpec {
            host: a.host.clone(),
            first,
            count,
            conns_total: a.conns,
            per_conn_interval,
            duration: Duration::from_secs(a.duration_secs),
            qos: a.qos,
            inflight: a.inflight,
            payload: vec![b'x'; a.size],
            topic: a.topic.clone(),
            topics: a.topics,
            connect_gap,
            connect_offset: Duration::from_secs_f64(w as f64 / FLEET_CONNECT_RATE),
        };
        first += count;
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            let r = run_fleet_worker(spec);
            let _ = tx.send(r);
        }));
    }
}

fn main() {
    let a = parse_args();
    let per_shard = a.rate as f64 / a.conns as f64;
    let duration = Duration::from_secs(a.duration_secs);
    let payload = vec![b'x'; a.size];

    eprintln!(
        "[loadgen] host={} offered={}/s conns={} qos={} inflight={} dur={}s size={}B topic={}",
        a.host, a.rate, a.conns, a.qos, a.inflight, a.duration_secs, a.size, a.topic
    );

    let (tx, rx) = mpsc::channel();
    let wall = Instant::now();
    let mut handles = Vec::new();
    if a.fleet > 0 {
        run_fleet(&a, &tx, &mut handles);
    }
    for idx in 0..if a.fleet > 0 { 0 } else { a.conns } {
        let (host, payload, topic, tx) = (
            a.host.clone(),
            payload.clone(),
            shard_topic(&a.topic, a.topics, idx),
            tx.clone(),
        );
        let spec = ShardSpec {
            idx,
            per_shard_rate: per_shard,
            duration,
            qos: a.qos,
            inflight: a.inflight,
            payload,
            topic,
        };
        handles.push(std::thread::spawn(move || {
            let r = run_shard(host, spec);
            let _ = tx.send(r);
        }));
    }
    drop(tx);

    let mut merged = LatencyHist::new();
    let (mut sent, mut ok, mut errors) = (0u64, 0u64, 0u64);
    let (mut unacked, mut reordered) = (0u64, 0u64);
    for r in rx {
        merged.merge(&r.hist);
        sent += r.sent;
        ok += r.ok;
        errors += r.errors;
        unacked += r.unacked;
        reordered += r.reordered;
    }
    for h in handles {
        let _ = h.join();
    }
    let elapsed = wall.elapsed().as_secs_f64().max(0.001);
    let achieved = sent as f64 / elapsed;
    let acked_rate = ok as f64 / elapsed;
    let ratio = achieved / a.rate as f64;
    let verdict = if ratio < 0.9 {
        "HARNESS_BOUND"
    } else {
        "DUT_ATTRIBUTABLE"
    };

    let report = JsonObj::new()
        .str("schema", "quantum-mqtt-loadgen/1")
        .str("host", &a.host)
        .str("protocol", "mqtt")
        .num("qos", a.qos)
        .num("offered_rate", a.rate)
        .num("achieved_rate", format!("{achieved:.1}"))
        .num("acked_rate", format!("{acked_rate:.1}"))
        .num("conns", a.conns)
        .num("inflight", a.inflight)
        .num("fleet", a.fleet)
        .num("size", a.size)
        .num("sent", sent)
        .num("ok", ok)
        .num("errors", errors)
        .num("unacked", unacked)
        .num("reordered", reordered)
        .num("p50_us", merged.percentile(50.0))
        .num("p99_us", merged.percentile(99.0))
        .num("p999_us", merged.percentile(99.9))
        .num("max_us", merged.max())
        .num("mean_us", merged.mean())
        .str("headroom_verdict", verdict)
        .render();
    println!("{report}");
    eprintln!(
        "[loadgen] sent={sent} ok={ok} err={errors} (unacked={unacked} reordered={reordered}) achieved={achieved:.0}/s acked={acked_rate:.0}/s verdict={verdict}"
    );
}
