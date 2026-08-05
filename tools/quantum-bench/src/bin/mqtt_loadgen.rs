//! `quantum-mqtt-loadgen` — off-DUT open-loop MQTT load generator.
//!
//! Drives the broker over the real MQTT codec path (plaintext TCP today; TLS
//! is a follow-up for the Pi 5 ingress). One TCP connection per shard, each
//! issuing a fixed-interval (Poisson-free) arrival stream at its share of the
//! offered rate, with its own latency histogram merged at the end.
//!
//! Latency is **coordinated-omission corrected**: each PUBLISH's latency is
//! measured from its *intended* send time, so a stalled broker shows a growing
//! tail instead of pacing our loop. The RFC §2.4 headroom verdict flags a run
//! the generator (not the DUT) bottlenecked as `HARNESS_BOUND`.
//!
//! QoS 1: send PUBLISH, await matching PUBACK, record the round-trip.
//! QoS 0: fire-and-forget; latency is the send-call cost only, and
//! offered==accepted accounting must be cross-checked against broker /metrics
//! (the broker has no ack to confirm delivery).
//!
//! Usage:
//!   quantum-mqtt-loadgen --host 127.0.0.1:9090 --rate 2000 --duration 10 \
//!       --conns 8 --qos 1 --size 128 --topic bench/t

use std::io::{Read, Write};
use std::net::TcpStream;
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
}

fn usage() -> ! {
    eprintln!(
        "quantum-mqtt-loadgen --host <addr:port> --rate <msg/s> [--duration N]\n\
         \x20  [--conns N] [--qos 0|1|2] [--size <bytes>] [--topic <name>]"
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
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown flag: {other}");
                usage();
            }
        }
    }
    if a.host.is_empty() || a.rate == 0 || a.conns == 0 || a.qos > 2 {
        usage();
    }
    a
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
}

fn run_shard(
    host: String,
    idx: u64,
    per_shard_rate: f64,
    duration: Duration,
    qos: u8,
    payload: Vec<u8>,
    topic: String,
) -> ShardResult {
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
            };
        }
    }

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
    }
}

fn main() {
    let a = parse_args();
    let per_shard = a.rate as f64 / a.conns as f64;
    let duration = Duration::from_secs(a.duration_secs);
    let payload = vec![b'x'; a.size];

    eprintln!(
        "[loadgen] host={} offered={}/s conns={} qos={} dur={}s size={}B topic={}",
        a.host, a.rate, a.conns, a.qos, a.duration_secs, a.size, a.topic
    );

    let (tx, rx) = mpsc::channel();
    let wall = Instant::now();
    let mut handles = Vec::new();
    for idx in 0..a.conns {
        let (host, payload, topic, tx) =
            (a.host.clone(), payload.clone(), a.topic.clone(), tx.clone());
        let qos = a.qos;
        handles.push(std::thread::spawn(move || {
            let r = run_shard(host, idx, per_shard, duration, qos, payload, topic);
            let _ = tx.send(r);
        }));
    }
    drop(tx);

    let mut merged = LatencyHist::new();
    let (mut sent, mut ok, mut errors) = (0u64, 0u64, 0u64);
    for r in rx {
        merged.merge(&r.hist);
        sent += r.sent;
        ok += r.ok;
        errors += r.errors;
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
        .num("size", a.size)
        .num("sent", sent)
        .num("ok", ok)
        .num("errors", errors)
        .num("p50_us", merged.percentile(50.0))
        .num("p99_us", merged.percentile(99.0))
        .num("p999_us", merged.percentile(99.9))
        .num("max_us", merged.max())
        .num("mean_us", merged.mean())
        .str("headroom_verdict", verdict)
        .render();
    println!("{report}");
    eprintln!(
        "[loadgen] sent={sent} ok={ok} err={errors} achieved={achieved:.0}/s acked={acked_rate:.0}/s verdict={verdict}"
    );
}
