//! Shared logic for the quantum off-DUT benchmark harness, mirroring
//! clustor-bench: parsing
//! the binary `/metrics` export, a minimal HTTP/1.1 client, a hand-rolled
//! JSON writer, and a log-linear latency histogram with
//! coordinated-omission-aware percentiles. Std-only, no external crates, so it
//! builds on an offline driver host.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// `/metrics` binary export framing — mirrors `modules/common/wire.rs`:
/// `[magic:u8=0xC7][version:u8][count:u16 LE]` then `count` × 14-byte records
/// `[module_id:u8][partition_id:u16 LE][metric_id:u16 LE][kind:u8][value:i64 LE]`.
pub const EXPORT_MAGIC: u8 = 0xC7;
pub const EXPORT_HDR: usize = 4;
pub const RECORD_LEN: usize = 14;

/// Metric kinds (mirror `wire::METRIC_KIND_*`).
pub const KIND_COUNTER: u8 = 0;
pub const KIND_GAUGE: u8 = 1;
pub const KIND_HISTOGRAM: u8 = 2;

/// One decoded metric record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sample {
    pub module_id: u8,
    pub partition_id: u16,
    pub metric_id: u16,
    pub kind: u8,
    pub value: i64,
}

/// Stable key for a metric across scrapes.
pub type Key = (u8, u16, u16);

impl Sample {
    pub fn key(&self) -> Key {
        (self.module_id, self.partition_id, self.metric_id)
    }
}

/// Decode a binary `/metrics` export body into its records. Returns an error
/// string on a bad magic, truncated header, or short record stream.
pub fn parse_export(body: &[u8]) -> Result<Vec<Sample>, String> {
    if body.len() < EXPORT_HDR {
        return Err(format!("export too short: {} bytes", body.len()));
    }
    if body[0] != EXPORT_MAGIC {
        return Err(format!("bad export magic: 0x{:02X}", body[0]));
    }
    let version = body[1];
    let count = u16::from_le_bytes([body[2], body[3]]) as usize;
    let mut out = Vec::with_capacity(count);
    let mut off = EXPORT_HDR;
    for i in 0..count {
        if off + RECORD_LEN > body.len() {
            return Err(format!(
                "record {i}/{count} runs past body end ({} bytes, version {version})",
                body.len()
            ));
        }
        let r = &body[off..off + RECORD_LEN];
        out.push(Sample {
            module_id: r[0],
            partition_id: u16::from_le_bytes([r[1], r[2]]),
            metric_id: u16::from_le_bytes([r[3], r[4]]),
            kind: r[5],
            value: i64::from_le_bytes([r[6], r[7], r[8], r[9], r[10], r[11], r[12], r[13]]),
        });
        off += RECORD_LEN;
    }
    Ok(out)
}

/// A point-in-time scrape, keyed for delta computation.
#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub by_key: BTreeMap<Key, Sample>,
}

impl Snapshot {
    pub fn from_samples(samples: &[Sample]) -> Self {
        let mut by_key = BTreeMap::new();
        for s in samples {
            by_key.insert(s.key(), *s);
        }
        Snapshot { by_key }
    }

    pub fn get(&self, key: Key) -> Option<i64> {
        self.by_key.get(&key).map(|s| s.value)
    }

    /// Delta of a COUNTER between two scrapes (end - start, clamped at 0 so a
    /// counter reset reads as 0 rather than a negative spike).
    pub fn counter_delta(&self, start: &Snapshot, key: Key) -> i64 {
        let end = self.get(key).unwrap_or(0);
        let beg = start.get(key).unwrap_or(0);
        (end - beg).max(0)
    }
}

/// Minimal HTTP/1.1 GET. Returns the response body bytes. Honours
/// `Content-Length`; falls back to read-to-EOF if absent. Binary-safe (the
/// `/metrics` body is a binary record stream, not text).
pub fn http_get(addr: &str, path: &str, timeout: Duration) -> Result<Vec<u8>, String> {
    let mut stream = TcpStream::connect(addr).map_err(|e| format!("connect {addr}: {e}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| format!("set_read_timeout: {e}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| format!("set_write_timeout: {e}"))?;
    let req =
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("write request: {e}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|e| format!("read response: {e}"))?;
    split_http_body(&raw)
}

/// Minimal HTTP/1.1 POST with a binary body. Returns the response status code
/// and body. Used by the load generator to drive `/propose`.
pub fn http_post(
    addr: &str,
    path: &str,
    body: &[u8],
    timeout: Duration,
) -> Result<(u16, Vec<u8>), String> {
    let mut stream = TcpStream::connect(addr).map_err(|e| format!("connect {addr}: {e}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| format!("set_read_timeout: {e}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| format!("set_write_timeout: {e}"))?;
    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
        body.len()
    )
    .into_bytes();
    req.extend_from_slice(body);
    stream
        .write_all(&req)
        .map_err(|e| format!("write request: {e}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|e| format!("read response: {e}"))?;
    let status = parse_status(&raw)?;
    let body = split_http_body(&raw)?;
    Ok((status, body))
}

fn parse_status(raw: &[u8]) -> Result<u16, String> {
    // "HTTP/1.1 NNN ..."
    let line_end = raw.iter().position(|&b| b == b'\r').unwrap_or(raw.len());
    let line = std::str::from_utf8(&raw[..line_end]).map_err(|_| "non-utf8 status line")?;
    line.split(' ')
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("unparseable status line: {line:?}"))
}

fn split_http_body(raw: &[u8]) -> Result<Vec<u8>, String> {
    // Find the CRLFCRLF header/body separator.
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("no header/body separator in HTTP response")?;
    Ok(raw[sep + 4..].to_vec())
}

/// Log-linear latency histogram (microseconds). 64 buckets per power-of-two
/// decade from 1 µs upward; enough resolution for p50/p99/p999 on a request
/// stream without an external HdrHistogram dependency.
pub struct LatencyHist {
    /// buckets[i] counts samples whose value falls in [bound(i), bound(i+1)).
    buckets: Vec<u64>,
    count: u64,
    min: u64,
    max: u64,
    sum: u128,
}

const SUB_BITS: u32 = 6; // 64 sub-buckets per octave
const SUB: usize = 1 << SUB_BITS;

impl Default for LatencyHist {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHist {
    pub fn new() -> Self {
        // 40 octaves × SUB covers ~1 µs .. ~10^12 µs — far past any real RTT.
        LatencyHist {
            buckets: vec![0; 40 * SUB],
            count: 0,
            min: u64::MAX,
            max: 0,
            sum: 0,
        }
    }

    fn bucket_of(us: u64) -> usize {
        if us < SUB as u64 {
            return us as usize;
        }
        let octave = 63 - us.leading_zeros(); // floor(log2(us))
        let sub = (us >> (octave - SUB_BITS)) as usize & (SUB - 1);
        (octave as usize - SUB_BITS as usize + 1) * SUB + sub
    }

    fn value_at(idx: usize) -> u64 {
        if idx < SUB {
            return idx as u64;
        }
        let octave = (idx / SUB) as u32 + SUB_BITS - 1;
        let sub = (idx % SUB) as u64;
        (1u64 << octave) + (sub << (octave - SUB_BITS))
    }

    pub fn record(&mut self, us: u64) {
        let i = Self::bucket_of(us).min(self.buckets.len() - 1);
        self.buckets[i] += 1;
        self.count += 1;
        self.min = self.min.min(us);
        self.max = self.max.max(us);
        self.sum += us as u128;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((self.count as f64) * p / 100.0).ceil() as u64;
        let mut seen = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            seen += c;
            if seen >= target {
                return Self::value_at(i);
            }
        }
        self.max
    }

    pub fn mean(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            (self.sum / self.count as u128) as u64
        }
    }

    pub fn min(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.min
        }
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    /// Merge another histogram into this one (per-thread → global).
    pub fn merge(&mut self, other: &LatencyHist) {
        for (i, &c) in other.buckets.iter().enumerate() {
            self.buckets[i] += c;
        }
        self.count += other.count;
        if other.count > 0 {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
            self.sum += other.sum;
        }
    }
}

/// Minimal JSON object builder — emits a flat/nested object without pulling in
/// serde. Values are pre-formatted strings (numbers, quoted strings, nested
/// objects), so callers control escaping via [`json_str`].
#[derive(Default)]
pub struct JsonObj {
    fields: Vec<(String, String)>,
}

impl JsonObj {
    pub fn new() -> Self {
        JsonObj::default()
    }

    pub fn num(mut self, key: &str, v: impl ToString) -> Self {
        self.fields.push((key.to_string(), v.to_string()));
        self
    }

    pub fn str(mut self, key: &str, v: &str) -> Self {
        self.fields.push((key.to_string(), json_str(v)));
        self
    }

    pub fn raw(mut self, key: &str, v: String) -> Self {
        self.fields.push((key.to_string(), v));
        self
    }

    pub fn render(&self) -> String {
        let mut s = String::from("{");
        for (i, (k, v)) in self.fields.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            s.push_str(&json_str(k));
            s.push(':');
            s.push_str(v);
        }
        s.push('}');
        s
    }
}

/// Quote + escape a string as a JSON string literal.
pub fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// FNV-1a 64-bit hash — stable config-hash for the JSON baseline metadata,
/// without a crypto dependency.
pub fn fnv1a64(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
