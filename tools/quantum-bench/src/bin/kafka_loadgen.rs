//! `quantum-kafka-loadgen` — off-DUT open-loop Kafka Produce load generator.
//!
//! Drives the broker's Produce path over the real Kafka wire protocol
//! (non-flexible versions: ApiVersions v0, Metadata v1, Produce v3) on a
//! persistent TCP connection per shard, each issuing a fixed-interval arrival
//! stream at its share of the offered rate, with its own latency histogram
//! merged at the end.
//!
//! Latency is **coordinated-omission corrected**: each Produce request's
//! latency is measured from its *intended* send time (start + interval*i), so
//! a stalled broker shows a growing tail instead of pacing our loop. The
//! headroom verdict flags a run the generator (not the DUT) bottlenecked as
//! `HARNESS_BOUND`.
//!
//! Rate semantics: `--rate` is Produce REQUESTS per second PER CONNECTION.
//! Each request carries one record batch of `--batch` records, so the offered
//! record rate is `rate * conns * batch` msgs/s.
//!
//! Pipelining: up to `--pipeline` Produce requests are kept in flight per
//! connection. A dedicated reader thread per connection reaps responses the
//! moment they arrive (matched by correlation id), so recorded latency is the
//! true response time — never an artifact of the pacing loop. With `--acks 0`
//! the broker sends no response and latency is the send-call cost only.
//!
//! Usage:
//!   quantum-kafka-loadgen --host <dut-ip> [--port 9092] --rate 1000 \
//!       [--duration N] [--conns N] [--topic bench] [--batch N] \
//!       [--value-size B] [--acks -1|0|1] [--pipeline N] [--skip-metadata] \
//!       [--handshake-only]
//!
//!   --handshake-only  connect once, do ApiVersions (+ Metadata unless
//!                     --skip-metadata) and exit 0 — smoke test, no produce.
//!
//!   --consume         consume instead of producing: per connection, resolve
//!                     the earliest/latest offsets with ListOffsets v1, then
//!                     Fetch v4 from earliest to latest counting batches and
//!                     records. `--conns` defaults to 1 in this mode. Exits
//!                     non-zero on any protocol error_code != 0, or when
//!                     nothing was consumed despite latest > earliest.
//!
//! std-only: one OS thread per shard, hand-rolled frames + CRC-32C. Fine for
//! a Pi-class driver at the rates a 1 GbE link admits.

use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use quantum_bench::{JsonObj, LatencyHist};

// ── CLI ─────────────────────────────────────────────────────────────────

struct Args {
    host: String,
    port: u16,
    rate: u64, // Produce requests/s per connection
    duration_secs: u64,
    conns: u64,
    topic: String,
    batch: u32, // records per Produce request
    value_size: usize,
    acks: i16,
    pipeline: usize,
    skip_metadata: bool,
    handshake_only: bool,
    consume: bool,
}

fn usage() -> ! {
    eprintln!(
        "quantum-kafka-loadgen --host <addr> [--port 9092] --rate <req/s per conn>\n\
         \x20  [--duration N] [--conns N] [--topic bench] [--batch N]\n\
         \x20  [--value-size B] [--acks -1|0|1] [--pipeline N]\n\
         \x20  [--skip-metadata] [--handshake-only] [--consume]"
    );
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut a = Args {
        host: String::new(),
        port: 9092,
        rate: 1000,
        duration_secs: 10,
        conns: 4,
        topic: "bench".to_string(),
        batch: 1,
        value_size: 64,
        acks: -1,
        pipeline: 16,
        skip_metadata: false,
        handshake_only: false,
        consume: false,
    };
    let mut conns_set = false;
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut next = || it.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--host" => a.host = next(),
            "--port" => a.port = next().parse().unwrap_or_else(|_| usage()),
            "--rate" => a.rate = next().parse().unwrap_or_else(|_| usage()),
            "--duration" => a.duration_secs = next().parse().unwrap_or_else(|_| usage()),
            "--conns" => {
                a.conns = next().parse().unwrap_or_else(|_| usage());
                conns_set = true;
            }
            "--topic" => a.topic = next(),
            "--batch" => a.batch = next().parse().unwrap_or_else(|_| usage()),
            "--value-size" => a.value_size = next().parse().unwrap_or_else(|_| usage()),
            "--acks" => a.acks = next().parse().unwrap_or_else(|_| usage()),
            "--pipeline" => a.pipeline = next().parse().unwrap_or_else(|_| usage()),
            "--skip-metadata" => a.skip_metadata = true,
            "--handshake-only" => a.handshake_only = true,
            "--consume" => a.consume = true,
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown flag: {other}");
                usage();
            }
        }
    }
    if a.host.is_empty()
        || a.rate == 0
        || a.conns == 0
        || a.batch == 0
        || a.pipeline == 0
        || !matches!(a.acks, -1..=1)
    {
        usage();
    }
    if a.consume && !conns_set {
        a.conns = 1;
    }
    a
}

// ── CRC-32C (Castagnoli), table-driven ──────────────────────────────────

fn crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0x82F6_3B78 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

fn crc32c(table: &[u32; 256], data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc = table[((crc ^ u32::from(b)) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

// ── Wire encoding helpers (non-flexible / classic encoding) ─────────────

/// Kafka STRING: i16 length + UTF-8 bytes.
fn put_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as i16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// Kafka NULLABLE_STRING null: i16 -1.
fn put_null_string(out: &mut Vec<u8>) {
    out.extend_from_slice(&(-1i16).to_be_bytes());
}

/// Zigzag-encoded signed varint (Kafka record fields).
fn put_varint(out: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let mut byte = (z & 0x7F) as u8;
        z >>= 7;
        if z != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if z == 0 {
            break;
        }
    }
}

/// Request header (non-flexible): api_key, api_version, correlation_id,
/// client_id (nullable string).
fn put_header(out: &mut Vec<u8>, api_key: i16, api_version: i16, corr: i32, client_id: &str) {
    out.extend_from_slice(&api_key.to_be_bytes());
    out.extend_from_slice(&api_version.to_be_bytes());
    out.extend_from_slice(&corr.to_be_bytes());
    put_string(out, client_id);
}

/// Frame a request (4-byte big-endian size prefix) and write it.
fn write_frame(w: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    framed.extend_from_slice(payload);
    w.write_all(&framed)
}

/// Read one response frame: [size i32] then size bytes (starting with the
/// correlation id). Returns the frame body.
fn read_frame<R: Read>(r: &mut R) -> Result<Vec<u8>, String> {
    let mut szb = [0u8; 4];
    r.read_exact(&mut szb).map_err(|e| format!("size: {e}"))?;
    let size = i32::from_be_bytes(szb);
    if !(0..=100 * 1024 * 1024).contains(&size) {
        return Err(format!("bad frame size {size}"));
    }
    let mut body = vec![0u8; size as usize];
    r.read_exact(&mut body).map_err(|e| format!("body: {e}"))?;
    Ok(body)
}

// ── Wire decoding cursor ────────────────────────────────────────────────

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Cur<'a> {
    fn new(b: &'a [u8]) -> Self {
        Cur { b, p: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.p + n > self.b.len() {
            return Err(format!("short read at {}+{}", self.p, n));
        }
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    fn i16(&mut self) -> Result<i16, String> {
        let s = self.take(2)?;
        Ok(i16::from_be_bytes([s[0], s[1]]))
    }
    fn i32(&mut self) -> Result<i32, String> {
        let s = self.take(4)?;
        Ok(i32::from_be_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn i64(&mut self) -> Result<i64, String> {
        let s = self.take(8)?;
        Ok(i64::from_be_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }
    /// STRING / NULLABLE_STRING; returns owned text (empty for null).
    fn string(&mut self) -> Result<String, String> {
        let len = self.i16()?;
        if len < 0 {
            return Ok(String::new());
        }
        let s = self.take(len as usize)?;
        Ok(String::from_utf8_lossy(s).into_owned())
    }
}

// ── Request builders ────────────────────────────────────────────────────

fn api_versions_request(corr: i32, client_id: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    put_header(&mut out, 18, 0, corr, client_id); // ApiVersions v0, empty body
    out
}

fn metadata_request(corr: i32, client_id: &str, topic: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(48);
    put_header(&mut out, 3, 1, corr, client_id); // Metadata v1
    out.extend_from_slice(&1i32.to_be_bytes()); // topics array: 1 entry
    put_string(&mut out, topic);
    out
}

/// Record batch v2 with `n` records of `value` each (null keys, no headers).
fn record_batch(table: &[u32; 256], n: u32, value: &[u8], timestamp_ms: i64) -> Vec<u8> {
    // Records section (varint-encoded per record).
    let mut records = Vec::with_capacity(n as usize * (value.len() + 16));
    let mut body = Vec::with_capacity(value.len() + 16);
    for i in 0..n {
        body.clear();
        body.push(0); // record attributes
        put_varint(&mut body, 0); // timestampDelta
        put_varint(&mut body, i64::from(i)); // offsetDelta
        put_varint(&mut body, -1); // key length (null)
        put_varint(&mut body, value.len() as i64);
        body.extend_from_slice(value);
        put_varint(&mut body, 0); // headers count
        put_varint(&mut records, body.len() as i64); // record length
        records.extend_from_slice(&body);
    }

    // Everything after the crc field (crc input).
    let mut post_crc = Vec::with_capacity(records.len() + 40);
    post_crc.extend_from_slice(&0i16.to_be_bytes()); // attributes
    post_crc.extend_from_slice(&(n as i32 - 1).to_be_bytes()); // lastOffsetDelta
    post_crc.extend_from_slice(&timestamp_ms.to_be_bytes()); // baseTimestamp
    post_crc.extend_from_slice(&timestamp_ms.to_be_bytes()); // maxTimestamp
    post_crc.extend_from_slice(&(-1i64).to_be_bytes()); // producerId
    post_crc.extend_from_slice(&(-1i16).to_be_bytes()); // producerEpoch
    post_crc.extend_from_slice(&(-1i32).to_be_bytes()); // baseSequence
    post_crc.extend_from_slice(&(n as i32).to_be_bytes()); // records count
    post_crc.extend_from_slice(&records);
    let crc = crc32c(table, &post_crc);

    // batchLength counts everything after the batchLength field itself:
    // partitionLeaderEpoch(4) + magic(1) + crc(4) + post_crc.
    let batch_length = (4 + 1 + 4 + post_crc.len()) as i32;
    let mut out = Vec::with_capacity(12 + batch_length as usize);
    out.extend_from_slice(&0i64.to_be_bytes()); // baseOffset
    out.extend_from_slice(&batch_length.to_be_bytes());
    out.extend_from_slice(&(-1i32).to_be_bytes()); // partitionLeaderEpoch
    out.push(2); // magic
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&post_crc);
    out
}

/// Produce request v3: null transactional_id, one topic, partition 0.
fn produce_request(
    corr: i32,
    client_id: &str,
    topic: &str,
    acks: i16,
    timeout_ms: i32,
    batch: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + topic.len() + batch.len());
    put_header(&mut out, 0, 3, corr, client_id);
    put_null_string(&mut out); // transactional_id
    out.extend_from_slice(&acks.to_be_bytes());
    out.extend_from_slice(&timeout_ms.to_be_bytes());
    out.extend_from_slice(&1i32.to_be_bytes()); // topic_data: 1 topic
    put_string(&mut out, topic);
    out.extend_from_slice(&1i32.to_be_bytes()); // partition_data: 1 partition
    out.extend_from_slice(&0i32.to_be_bytes()); // partition index 0
    out.extend_from_slice(&(batch.len() as i32).to_be_bytes()); // records BYTES
    out.extend_from_slice(batch);
    out
}

/// ListOffsets request v1: one topic, partition 0, given timestamp
/// (-2 = earliest, -1 = latest).
fn list_offsets_request(corr: i32, client_id: &str, topic: &str, timestamp: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(48 + topic.len());
    put_header(&mut out, 2, 1, corr, client_id);
    out.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    out.extend_from_slice(&1i32.to_be_bytes()); // topics: 1
    put_string(&mut out, topic);
    out.extend_from_slice(&1i32.to_be_bytes()); // partitions: 1
    out.extend_from_slice(&0i32.to_be_bytes()); // partition 0
    out.extend_from_slice(&timestamp.to_be_bytes());
    out
}

/// Fetch request v4: one topic, partition 0, from `fetch_offset`.
fn fetch_request(corr: i32, client_id: &str, topic: &str, fetch_offset: i64) -> Vec<u8> {
    const MAX_BYTES: i32 = 1_048_576;
    let mut out = Vec::with_capacity(64 + topic.len());
    put_header(&mut out, 1, 4, corr, client_id);
    out.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    out.extend_from_slice(&100i32.to_be_bytes()); // max_wait_ms
    out.extend_from_slice(&1i32.to_be_bytes()); // min_bytes
    out.extend_from_slice(&MAX_BYTES.to_be_bytes()); // max_bytes
    out.push(0); // isolation_level (read_uncommitted)
    out.extend_from_slice(&1i32.to_be_bytes()); // topics: 1
    put_string(&mut out, topic);
    out.extend_from_slice(&1i32.to_be_bytes()); // partitions: 1
    out.extend_from_slice(&0i32.to_be_bytes()); // partition 0
    out.extend_from_slice(&fetch_offset.to_be_bytes());
    out.extend_from_slice(&MAX_BYTES.to_be_bytes()); // partition_max_bytes
    out
}

// ── Response parsers ────────────────────────────────────────────────────

/// ProduceResponse v3 body (after correlation id): first partition's
/// error_code. Body: [responses [topic string][partition_responses
/// [index i32][error_code i16][base_offset i64][log_append_time i64]]]
/// [throttle_time_ms i32].
fn parse_produce_error(body: &[u8]) -> Result<i16, String> {
    let mut c = Cur::new(body);
    let topics = c.i32()?;
    if topics < 1 {
        return Err("produce response: empty topics array".to_string());
    }
    let _topic = c.string()?;
    let parts = c.i32()?;
    if parts < 1 {
        return Err("produce response: empty partitions array".to_string());
    }
    let _index = c.i32()?;
    let error_code = c.i16()?;
    let _base_offset = c.i64()?;
    let _log_append_time = c.i64()?;
    Ok(error_code)
}

/// ListOffsets v1 response body (after correlation id): first topic /
/// partition. Body: [topics [name string][partitions [partition i32]
/// [error_code i16][timestamp i64][offset i64]]]. Returns the offset.
fn parse_list_offsets(body: &[u8]) -> Result<i64, String> {
    let mut c = Cur::new(body);
    let topics = c.i32()?;
    if topics < 1 {
        return Err("ListOffsets response: empty topics array".to_string());
    }
    let _topic = c.string()?;
    let parts = c.i32()?;
    if parts < 1 {
        return Err("ListOffsets response: empty partitions array".to_string());
    }
    let _partition = c.i32()?;
    let ec = c.i16()?;
    if ec != 0 {
        return Err(format!("ListOffsets error_code {ec}"));
    }
    let _timestamp = c.i64()?;
    c.i64()
}

/// One Fetch v4 response, decoded down to the partition's records section.
struct FetchPartition {
    high_watermark: i64,
    /// Record batches (concatenated batch-v2 wire bytes), possibly empty.
    records: Vec<u8>,
}

/// Fetch v4 response body (after correlation id): [throttle_time i32]
/// [topics [name string][partitions [partition i32][error_code i16]
/// [high_watermark i64][last_stable_offset i64][aborted_txns count i32,
/// entries (producer_id i64, first_offset i64)][records BYTES]]].
fn parse_fetch(body: &[u8]) -> Result<FetchPartition, String> {
    let mut c = Cur::new(body);
    let _throttle = c.i32()?;
    let topics = c.i32()?;
    if topics < 1 {
        return Err("Fetch response: empty topics array".to_string());
    }
    let _topic = c.string()?;
    let parts = c.i32()?;
    if parts < 1 {
        return Err("Fetch response: empty partitions array".to_string());
    }
    let _partition = c.i32()?;
    let ec = c.i16()?;
    if ec != 0 {
        return Err(format!("Fetch error_code {ec}"));
    }
    let high_watermark = c.i64()?;
    let _last_stable_offset = c.i64()?;
    let aborted = c.i32()?;
    for _ in 0..aborted.max(0) {
        let _producer_id = c.i64()?;
        let _first_offset = c.i64()?;
    }
    let records_len = c.i32()?;
    let records = if records_len > 0 {
        c.take(records_len as usize)?.to_vec()
    } else {
        Vec::new()
    };
    Ok(FetchPartition {
        high_watermark,
        records,
    })
}

/// Walk a records section of concatenated batch-v2 entries:
/// [baseOffset i64][batchLength i32][…], per-batch record count =
/// lastOffsetDelta (i32 at byte offset 23 of the batch) + 1. A trailing
/// truncated batch (the broker may cut at max_bytes) is ignored.
/// Returns (batches, records, next_offset_after_last_complete_batch).
fn scan_batches(records: &[u8]) -> (u64, u64, Option<i64>) {
    let (mut batches, mut recs) = (0u64, 0u64);
    let mut next_offset = None;
    let mut p = 0usize;
    while records.len() - p >= 12 {
        let base_offset = i64::from_be_bytes(records[p..p + 8].try_into().unwrap());
        let batch_len = i32::from_be_bytes(records[p + 8..p + 12].try_into().unwrap());
        if batch_len < 0 {
            break;
        }
        let total = 12 + batch_len as usize;
        if p + total > records.len() || total < 27 {
            break; // truncated batch
        }
        let last_offset_delta = i32::from_be_bytes(records[p + 23..p + 27].try_into().unwrap());
        let count = i64::from(last_offset_delta) + 1;
        batches += 1;
        recs += count.max(0) as u64;
        next_offset = Some(base_offset + count.max(0));
        p += total;
    }
    (batches, recs, next_offset)
}

/// Per-connection handshake: ApiVersions v0 (validate error_code 0), then
/// optionally Metadata v1 for the topic (validate topic error_code 0).
fn handshake(
    w: &mut TcpStream,
    r: &mut BufReader<TcpStream>,
    corr: &mut i32,
    client_id: &str,
    topic: &str,
    skip_metadata: bool,
) -> Result<(), String> {
    // ApiVersions v0.
    let av_corr = *corr;
    *corr += 1;
    write_frame(w, &api_versions_request(av_corr, client_id))
        .map_err(|e| format!("ApiVersions write: {e}"))?;
    let frame = read_frame(r).map_err(|e| format!("ApiVersions read: {e}"))?;
    let mut c = Cur::new(&frame);
    let got = c.i32()?;
    if got != av_corr {
        return Err(format!("ApiVersions correlation id {got} != {av_corr}"));
    }
    let ec = c.i16()?;
    if ec != 0 {
        return Err(format!("ApiVersions error_code {ec}"));
    }

    if skip_metadata {
        return Ok(());
    }

    // Metadata v1: [brokers [node_id i32][host string][port i32]
    // [rack nullable_string]][controller_id i32][topics [error_code i16]
    // [name string][is_internal bool][partitions ...]].
    let md_corr = *corr;
    *corr += 1;
    write_frame(w, &metadata_request(md_corr, client_id, topic))
        .map_err(|e| format!("Metadata write: {e}"))?;
    let frame = read_frame(r).map_err(|e| format!("Metadata read: {e}"))?;
    let mut c = Cur::new(&frame);
    let got = c.i32()?;
    if got != md_corr {
        return Err(format!("Metadata correlation id {got} != {md_corr}"));
    }
    let brokers = c.i32()?;
    for _ in 0..brokers.max(0) {
        let _node = c.i32()?;
        let _host = c.string()?;
        let _port = c.i32()?;
        let _rack = c.string()?;
    }
    let _controller = c.i32()?;
    let topics = c.i32()?;
    if topics < 1 {
        return Err(format!("Metadata: topic {topic:?} missing from response"));
    }
    let ec = c.i16()?;
    let name = c.string()?;
    if ec != 0 {
        return Err(format!("Metadata: topic {name:?} error_code {ec}"));
    }
    Ok(())
}

// ── Shard loop ──────────────────────────────────────────────────────────

struct ShardCfg {
    addr: String,
    idx: u64,
    per_shard_rate: f64,
    duration: Duration,
    topic: String,
    batch: u32,
    value_size: usize,
    acks: i16,
    pipeline: usize,
    skip_metadata: bool,
}

struct ShardResult {
    hist: LatencyHist,
    sent: u64,
    ok: u64,
    errors: u64,
}

const PRODUCE_TIMEOUT_MS: i32 = 30_000;

fn xorshift64star(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Reader half of a shard: for each `(corr, intended)` the sender hands
/// over, read one response frame, match the correlation id (responses
/// arrive in request order on a Kafka connection), and record latency from
/// the intended send time at the moment the response actually arrived.
///
/// Runs in its own thread so responses are reaped the instant they land.
/// Reaping only when the pipeline fills (the previous design) meant a
/// response sat unread until the *next* paced send needed its slot, which
/// inflated every latency sample to ≈ pipeline × interval regardless of
/// broker speed.
fn run_reader(
    mut r: BufReader<TcpStream>,
    rx: mpsc::Receiver<(i32, Instant)>,
) -> (LatencyHist, u64, u64) {
    let mut hist = LatencyHist::new();
    let (mut ok, mut errors) = (0u64, 0u64);
    while let Ok((corr, intended)) = rx.recv() {
        match read_frame(&mut r) {
            Ok(frame) => {
                let done = Instant::now();
                let mut c = Cur::new(&frame);
                match c.i32() {
                    Ok(rc) if rc == corr => match parse_produce_error(&frame[c.p..]) {
                        Ok(0) => {
                            hist.record(
                                done.saturating_duration_since(intended).as_micros() as u64,
                            );
                            ok += 1;
                        }
                        Ok(_) | Err(_) => errors += 1,
                    },
                    _ => errors += 1,
                }
            }
            Err(e) => {
                eprintln!("[reader] {e}");
                // This entry plus everything still queued is lost.
                errors += 1 + rx.try_iter().count() as u64;
                break;
            }
        }
    }
    (hist, ok, errors)
}

fn run_shard(cfg: &ShardCfg) -> ShardResult {
    let mut hist = LatencyHist::new();
    let (mut sent, mut ok, mut errors) = (0u64, 0u64, 0u64);
    let fail = |hist: LatencyHist, sent, ok, errors| ShardResult {
        hist,
        sent,
        ok,
        errors,
    };

    let stream = match TcpStream::connect(&cfg.addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[shard {}] connect {}: {e}", cfg.idx, cfg.addr);
            return fail(hist, sent, ok, errors + 1);
        }
    };
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let mut w = match stream.try_clone() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[shard {}] clone: {e}", cfg.idx);
            return fail(hist, sent, ok, errors + 1);
        }
    };
    let mut r = BufReader::new(stream);

    let client_id = format!("qkl-{}", cfg.idx);
    let mut corr: i32 = 0;
    if let Err(e) = handshake(
        &mut w,
        &mut r,
        &mut corr,
        &client_id,
        &cfg.topic,
        cfg.skip_metadata,
    ) {
        eprintln!("[shard {}] handshake: {e}", cfg.idx);
        return fail(hist, sent, ok, errors + 1);
    }

    let crc_table = crc32c_table();
    let mut value = vec![b'x'; cfg.value_size];
    let mut rng: u64 = cfg.idx.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);

    // Bounded handoff to the reader thread doubles as the pipeline-depth
    // limiter: when `pipeline` requests are unreaped, send() blocks and the
    // paced loop stalls — which the intended-time latency accounting then
    // charges to the broker, as coordinated-omission correction requires.
    let (tx, rx) = mpsc::sync_channel::<(i32, Instant)>(cfg.pipeline);
    let reader = std::thread::spawn(move || run_reader(r, rx));
    let finish = |hist: &mut LatencyHist,
                  ok: &mut u64,
                  errors: &mut u64,
                  reader: std::thread::JoinHandle<(LatencyHist, u64, u64)>| {
        if let Ok((rh, rok, rerr)) = reader.join() {
            hist.merge(&rh);
            *ok += rok;
            *errors += rerr;
        }
    };

    let interval = Duration::from_secs_f64(1.0 / cfg.per_shard_rate);
    let start = Instant::now();
    let deadline = start + cfg.duration;
    let mut i: u64 = 0;
    loop {
        let intended = start + interval.mul_f64(i as f64);
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        if intended > now {
            std::thread::sleep(intended - now);
        }
        // Vary the payload prefix so batches are not byte-identical.
        let stamp = xorshift64star(&mut rng).to_le_bytes();
        let n = stamp.len().min(value.len());
        value[..n].copy_from_slice(&stamp[..n]);

        let batch = record_batch(&crc_table, cfg.batch, &value, now_unix_ms());
        let req_corr = corr;
        corr = corr.wrapping_add(1);
        let req = produce_request(
            req_corr,
            &client_id,
            &cfg.topic,
            cfg.acks,
            PRODUCE_TIMEOUT_MS,
            &batch,
        );
        if let Err(e) = write_frame(&mut w, &req) {
            eprintln!("[shard {}] write: {e}", cfg.idx);
            errors += 1;
            sent += 1;
            drop(tx);
            finish(&mut hist, &mut ok, &mut errors, reader);
            return fail(hist, sent, ok, errors);
        }
        sent += 1;
        if cfg.acks == 0 {
            // No response with acks=0: record the send-call cost only.
            hist.record(
                Instant::now()
                    .saturating_duration_since(intended)
                    .as_micros() as u64,
            );
            ok += 1;
        } else if tx.send((req_corr, intended)).is_err() {
            // Reader died (read error already printed); stop the shard.
            drop(tx);
            finish(&mut hist, &mut ok, &mut errors, reader);
            return fail(hist, sent, ok, errors);
        }
        i += 1;
    }

    // Close the handoff; the reader drains every outstanding response
    // (or times out) and returns its histogram.
    drop(tx);
    finish(&mut hist, &mut ok, &mut errors, reader);
    ShardResult {
        hist,
        sent,
        ok,
        errors,
    }
}

// ── Consume mode ────────────────────────────────────────────────────────

#[derive(Default)]
struct ConsumeShard {
    earliest: i64,
    latest: i64,
    records: u64,
    batches: u64,
    bytes: u64,
    fetches: u64,
    errors: u64,
}

/// Synchronous request/response round-trip: write one frame, read one
/// frame, validate the correlation id, return the body after it.
fn round_trip(
    w: &mut TcpStream,
    r: &mut BufReader<TcpStream>,
    corr: i32,
    req: &[u8],
    what: &str,
) -> Result<Vec<u8>, String> {
    write_frame(w, req).map_err(|e| format!("{what} write: {e}"))?;
    let frame = read_frame(r).map_err(|e| format!("{what} read: {e}"))?;
    let mut c = Cur::new(&frame);
    let got = c.i32()?;
    if got != corr {
        return Err(format!("{what} correlation id {got} != {corr}"));
    }
    Ok(frame[c.p..].to_vec())
}

/// One consume connection: handshake, ListOffsets earliest/latest, then a
/// Fetch v4 loop from earliest until the high watermark.
fn run_consume_shard(
    addr: &str,
    idx: u64,
    topic: &str,
    skip_metadata: bool,
) -> Result<ConsumeShard, String> {
    let stream = TcpStream::connect(addr).map_err(|e| format!("connect {addr}: {e}"))?;
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let mut w = stream.try_clone().map_err(|e| format!("clone: {e}"))?;
    let mut r = BufReader::new(stream);

    let client_id = format!("qkl-consume-{idx}");
    let mut corr: i32 = 0;
    handshake(&mut w, &mut r, &mut corr, &client_id, topic, skip_metadata)
        .map_err(|e| format!("handshake: {e}"))?;

    let c = corr;
    corr = corr.wrapping_add(1);
    let earliest = parse_list_offsets(&round_trip(
        &mut w,
        &mut r,
        c,
        &list_offsets_request(c, &client_id, topic, -2),
        "ListOffsets(earliest)",
    )?)?;
    let c = corr;
    corr = corr.wrapping_add(1);
    let latest = parse_list_offsets(&round_trip(
        &mut w,
        &mut r,
        c,
        &list_offsets_request(c, &client_id, topic, -1),
        "ListOffsets(latest)",
    )?)?;

    let mut out = ConsumeShard {
        earliest,
        latest,
        ..ConsumeShard::default()
    };
    let mut fetch_offset = earliest;
    let mut consecutive_empty = 0u32;
    while fetch_offset < latest {
        let c = corr;
        corr = corr.wrapping_add(1);
        let body = round_trip(
            &mut w,
            &mut r,
            c,
            &fetch_request(c, &client_id, topic, fetch_offset),
            "Fetch",
        )?;
        out.fetches += 1;
        let part = parse_fetch(&body)?;
        out.bytes += part.records.len() as u64;
        let (batches, recs, next_offset) = scan_batches(&part.records);
        out.batches += batches;
        out.records += recs;
        match next_offset {
            Some(next) if next > fetch_offset => {
                fetch_offset = next;
                consecutive_empty = 0;
            }
            _ => {
                consecutive_empty += 1;
                if consecutive_empty >= 3 {
                    break;
                }
            }
        }
        if fetch_offset >= part.high_watermark {
            break;
        }
    }
    Ok(out)
}

fn run_consume(a: &Args) -> ! {
    let addr = format!("{}:{}", a.host, a.port);
    eprintln!("[consume] addr={addr} topic={} conns={}", a.topic, a.conns);

    let (tx, rx) = mpsc::channel();
    let wall = Instant::now();
    let mut handles = Vec::new();
    for idx in 0..a.conns {
        let addr = addr.clone();
        let topic = a.topic.clone();
        let skip_metadata = a.skip_metadata;
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            let r = run_consume_shard(&addr, idx, &topic, skip_metadata).unwrap_or_else(|e| {
                eprintln!("[consume {idx}] {e}");
                ConsumeShard {
                    errors: 1,
                    ..ConsumeShard::default()
                }
            });
            let _ = tx.send(r);
        }));
    }
    drop(tx);

    let mut agg = ConsumeShard::default();
    let (mut earliest, mut latest) = (i64::MAX, i64::MIN);
    for s in rx {
        earliest = earliest.min(s.earliest);
        latest = latest.max(s.latest);
        agg.records += s.records;
        agg.batches += s.batches;
        agg.bytes += s.bytes;
        agg.fetches += s.fetches;
        agg.errors += s.errors;
    }
    for h in handles {
        let _ = h.join();
    }
    if earliest == i64::MAX {
        earliest = 0;
    }
    if latest == i64::MIN {
        latest = 0;
    }

    let elapsed = wall.elapsed().as_secs_f64().max(0.001);
    let records_per_sec = agg.records as f64 / elapsed;
    let report = JsonObj::new()
        .str("schema", "quantum-kafka-loadgen/1")
        .str("mode", "consume")
        .str("host", &a.host)
        .num("port", a.port)
        .str("topic", &a.topic)
        .num("conns", a.conns)
        .num("earliest", earliest)
        .num("latest", latest)
        .num("records_consumed", agg.records)
        .num("batches", agg.batches)
        .num("bytes", agg.bytes)
        .num("fetches", agg.fetches)
        .num("duration_secs", format!("{elapsed:.3}"))
        .num("records_per_sec", format!("{records_per_sec:.1}"))
        .num("errors", agg.errors)
        .render();
    println!("{report}");
    eprintln!(
        "[consume] earliest={earliest} latest={latest} records={} batches={} \
         bytes={} fetches={} err={} ({records_per_sec:.0} rec/s)",
        agg.records, agg.batches, agg.bytes, agg.fetches, agg.errors
    );

    let nothing_consumed = agg.records == 0 && latest > earliest;
    if agg.errors > 0 || nothing_consumed {
        std::process::exit(1);
    }
    std::process::exit(0);
}

// ── Handshake-only smoke test ───────────────────────────────────────────

fn run_handshake_only(a: &Args) -> ! {
    let addr = format!("{}:{}", a.host, a.port);
    let stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[handshake] connect {addr}: {e}");
            std::process::exit(1);
        }
    };
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let mut w = stream.try_clone().expect("clone");
    let mut r = BufReader::new(stream);
    let mut corr = 0i32;
    match handshake(
        &mut w,
        &mut r,
        &mut corr,
        "qkl-smoke",
        &a.topic,
        a.skip_metadata,
    ) {
        Ok(()) => {
            let report = JsonObj::new()
                .str("schema", "quantum-kafka-loadgen/1")
                .str("mode", "handshake-only")
                .str("host", &a.host)
                .num("port", a.port)
                .str("topic", &a.topic)
                .num("metadata_checked", u8::from(!a.skip_metadata))
                .str("result", "ok")
                .render();
            println!("{report}");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("[handshake] {e}");
            std::process::exit(1);
        }
    }
}

// ── Main ────────────────────────────────────────────────────────────────

fn main() {
    let a = parse_args();
    if a.handshake_only {
        run_handshake_only(&a);
    }
    if a.consume {
        run_consume(&a);
    }
    let addr = format!("{}:{}", a.host, a.port);
    let duration = Duration::from_secs(a.duration_secs);
    let offered_reqs = a.rate * a.conns; // total Produce requests/s
    let offered_msgs = offered_reqs * u64::from(a.batch);

    eprintln!(
        "[loadgen] addr={} offered={offered_reqs} req/s ({offered_msgs} msg/s) conns={} \
         rate/conn={} batch={} val={}B acks={} pipeline={} dur={}s topic={}",
        addr, a.conns, a.rate, a.batch, a.value_size, a.acks, a.pipeline, a.duration_secs, a.topic
    );

    let (tx, rx) = mpsc::channel();
    let wall = Instant::now();
    let mut handles = Vec::new();
    for idx in 0..a.conns {
        let cfg = ShardCfg {
            addr: addr.clone(),
            idx,
            per_shard_rate: a.rate as f64,
            duration,
            topic: a.topic.clone(),
            batch: a.batch,
            value_size: a.value_size,
            acks: a.acks,
            pipeline: a.pipeline,
            skip_metadata: a.skip_metadata,
        };
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            let r = run_shard(&cfg);
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
    let achieved_reqs = sent as f64 / elapsed;
    let achieved_msgs = achieved_reqs * f64::from(a.batch);
    let acked_reqs = ok as f64 / elapsed;
    let ratio = achieved_reqs / offered_reqs as f64;
    let verdict = if ratio < 0.9 {
        "HARNESS_BOUND"
    } else {
        "DUT_ATTRIBUTABLE"
    };

    let produce_tail = JsonObj::new()
        .num("count", merged.count())
        .num("p50_us", merged.percentile(50.0))
        .num("p99_us", merged.percentile(99.0))
        .num("p999_us", merged.percentile(99.9))
        .num("max_us", merged.max())
        .num("mean_us", merged.mean())
        .render();

    let report = JsonObj::new()
        .str("schema", "quantum-kafka-loadgen/1")
        .str("protocol", "kafka-produce-v3")
        .str("host", &a.host)
        .num("port", a.port)
        .str("topic", &a.topic)
        .num("acks", a.acks)
        .num("conns", a.conns)
        .num("pipeline", a.pipeline)
        .num("batch", a.batch)
        .num("value_size", a.value_size)
        .num("rate_per_conn", a.rate)
        .num("offered_req_rate", offered_reqs)
        .num("offered_msg_rate", offered_msgs)
        .num("achieved_req_rate", format!("{achieved_reqs:.1}"))
        .num("achieved_msg_rate", format!("{achieved_msgs:.1}"))
        .num("acked_req_rate", format!("{acked_reqs:.1}"))
        .num("sent", sent)
        .num("records_sent", sent * u64::from(a.batch))
        .num("ok", ok)
        .num("errors", errors)
        .raw("produce_tail", produce_tail)
        .str("headroom_verdict", verdict)
        .render();
    println!("{report}");
    eprintln!(
        "[loadgen] sent={sent} ok={ok} err={errors} achieved={achieved_reqs:.0} req/s \
         ({achieved_msgs:.0} msg/s) verdict={verdict}"
    );
}
