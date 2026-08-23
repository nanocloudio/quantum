//! router — per-connection protocol classification.
//!
//! Classifies each new client connection from its first bytes (up to 16)
//! and pins the verdict for the connection's lifetime, so subsequent
//! records route without re-sniffing. Unclassifiable connections fall
//! back to `default_protocol` unless `strict_classification` is set.
//!
//! ## Per-step bound
//!
//! `route` classifies ONE record per call and never blocks; the
//! composite's dispatch table owns the per-step record budget.

use super::wire;

const MAX_CONNS: usize = 64;
/// Sized to carry the largest client record peer_router can emit
/// (`peer_router BUF_SIZE = 4096` net bytes + 1 conn_id byte, rounded
/// up). Undersizing this is not a soft cap: `channel_read_msg` DISCARDS
/// oversize envelopes wholesale, so a 1 KiB buffer silently dropped any
/// TCP segment over 1023 bytes — e.g. every pipelined or batched Kafka
/// produce burst. The composite owns the buffer; this is the bound it
/// must satisfy.
pub const READ_BUF: usize = 4608;

pub const PROTO_UNKNOWN: u8 = 0;
pub const PROTO_MQTT: u8 = 1;
pub const PROTO_KAFKA: u8 = 2;
pub const PROTO_AMQP: u8 = 3;
pub const PROTO_UNSUPPORTED: u8 = 0xFF;

#[repr(C)]
#[derive(Clone, Copy)]
struct ConnState {
    conn_id: u8,
    protocol: u8,
    sniff_buf_len: u8,
    sniff_buf: [u8; 16],
    active: u8,
}

impl ConnState {
    const fn zero() -> Self {
        Self {
            conn_id: 0,
            protocol: PROTO_UNKNOWN,
            sniff_buf_len: 0,
            sniff_buf: [0; 16],
            active: 0,
        }
    }
}

#[repr(C)]
pub struct Router {
    conns: [ConnState; MAX_CONNS],

    // Params / stats
    default_protocol: u8,
    strict_classification: u8,
    classified_mqtt: u32,
    classified_kafka: u32,
    classified_amqp: u32,
    unclassified: u32,
}

impl Router {
    fn find_or_create(&mut self, conn_id: u8) -> usize {
        for i in 0..MAX_CONNS {
            if self.conns[i].active == 1 && self.conns[i].conn_id == conn_id {
                return i;
            }
        }
        for i in 0..MAX_CONNS {
            if self.conns[i].active == 0 {
                self.conns[i] = ConnState {
                    conn_id,
                    protocol: PROTO_UNKNOWN,
                    sniff_buf_len: 0,
                    sniff_buf: [0; 16],
                    active: 1,
                };
                return i;
            }
        }
        // Table full: reuse slot 0 (LRU would be nicer)
        self.conns[0] = ConnState {
            conn_id,
            protocol: PROTO_UNKNOWN,
            sniff_buf_len: 0,
            sniff_buf: [0; 16],
            active: 1,
        };
        0
    }
}

/// First-byte protocol classification. Returns (protocol, bytes_consumed).
/// `bytes_consumed` indicates how much of the sniff buffer was used for
/// classification (meaningful only for UNSUPPORTED / deep checks).
fn classify(bytes: &[u8]) -> u8 {
    if bytes.is_empty() {
        return PROTO_UNKNOWN;
    }

    // MQTT: CONNECT packet type nibble = 0x1, flags nibble = 0x0.
    // Full signature check: [0x10][varint len][0x00 0x04 'M' 'Q' 'T' 'T'] for MQTT 3.1.1/5
    // or [0x10][varint len][0x00 0x06 'M' 'Q' 'I' 's' 'd' 'p'] for MQTT 3.1.
    // Accept bare 0x10 + varint if protocol name not yet arrived (partial packet).
    if bytes[0] == 0x10 {
        if bytes.len() >= 8 && bytes[2] == 0x00 && bytes[3] == 0x04 && &bytes[4..8] == b"MQTT" {
            return PROTO_MQTT;
        }
        if bytes.len() >= 10 && bytes[2] == 0x00 && bytes[3] == 0x06 && &bytes[4..10] == b"MQIsdp" {
            return PROTO_MQTT;
        }
        // First byte matches CONNECT; let it through as MQTT even if we
        // haven't seen the protocol name yet — subsequent bytes will be
        // validated by the mqtt component itself.
        if bytes.len() < 8 {
            return PROTO_MQTT;
        }
    }

    // AMQP: protocol header is exactly "AMQP\x00\x00\x09\x01"
    if bytes.len() >= 8 && &bytes[..4] == b"AMQP" {
        return PROTO_AMQP;
    }

    // Kafka: first 4 bytes are request size (big-endian i32), must be positive
    // and < 16 MB. Bytes [4..6] are api_key (i16 BE, 0..=67).
    if bytes.len() >= 6 {
        let size = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let api_key = i16::from_be_bytes([bytes[4], bytes[5]]);
        if size > 0 && size < 16 * 1024 * 1024 && (0..=67).contains(&api_key) {
            return PROTO_KAFKA;
        }
    }

    PROTO_UNKNOWN
}

/// Component defaults. Channel handles are assigned by the
/// composite after this returns.
pub fn init(s: &mut Router) {
    s.default_protocol = PROTO_MQTT;
    s.strict_classification = 0; // if 1, unclassified → dropped; else → default
    for i in 0..MAX_CONNS {
        s.conns[i] = ConnState::zero();
    }
}

/// Classify one client record and name the protocol that owns it.
///
/// `MSG_CONN_CLOSED` returns the protocol the connection had been pinned
/// to (and releases the slot) so the dispatch table can forward the close
/// to that codec; a record whose connection is not yet classifiable
/// returns [`PROTO_UNKNOWN`] and is dropped by the caller — the sniff
/// bytes are retained until a verdict is reachable.
pub fn route(s: &mut Router, conn_id: u8, mtype: u8, data: &[u8]) -> u8 {
    if mtype == wire::MSG_CONN_CLOSED {
        for i in 0..MAX_CONNS {
            if s.conns[i].active == 1 && s.conns[i].conn_id == conn_id {
                let proto = s.conns[i].protocol;
                s.conns[i] = ConnState::zero();
                return proto;
            }
        }
        return PROTO_UNKNOWN;
    }
    if data.is_empty() {
        return PROTO_UNKNOWN;
    }

    let idx = s.find_or_create(conn_id);

    // On first bytes for this conn, classify from up to 16 bytes. Whether
    // classification succeeds or we fall back to the default, the caller
    // forwards the FULL record — the sniff buffer is only the classifier's
    // inspection window.
    if s.conns[idx].protocol == PROTO_UNKNOWN {
        let sniff_have = s.conns[idx].sniff_buf_len as usize;
        let take = core::cmp::min(16 - sniff_have, data.len());
        s.conns[idx].sniff_buf[sniff_have..(take + sniff_have)].copy_from_slice(&data[..take]);
        s.conns[idx].sniff_buf_len = (sniff_have + take) as u8;

        let sniff_len = s.conns[idx].sniff_buf_len as usize;
        let proto = classify(&s.conns[idx].sniff_buf[..sniff_len]);

        if proto != PROTO_UNKNOWN {
            s.conns[idx].protocol = proto;
        } else if sniff_len >= 16 {
            // 16 bytes and still unclassifiable.
            if s.strict_classification == 0 {
                s.conns[idx].protocol = s.default_protocol;
            } else {
                s.conns[idx].protocol = PROTO_UNSUPPORTED;
                s.unclassified = s.unclassified.wrapping_add(1);
                return PROTO_UNKNOWN;
            }
        } else {
            // Need more bytes to decide; the caller drops this record.
            return PROTO_UNKNOWN;
        }

        match s.conns[idx].protocol {
            PROTO_MQTT => s.classified_mqtt = s.classified_mqtt.wrapping_add(1),
            PROTO_KAFKA => s.classified_kafka = s.classified_kafka.wrapping_add(1),
            PROTO_AMQP => s.classified_amqp = s.classified_amqp.wrapping_add(1),
            _ => {}
        }
    }

    s.conns[idx].protocol
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(s: &Router, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&s.classified_mqtt.to_le_bytes());
    m[4..8].copy_from_slice(&s.classified_kafka.to_le_bytes());
    m[8..12].copy_from_slice(&s.classified_amqp.to_le_bytes());
    m[12..16].copy_from_slice(&s.unclassified.to_le_bytes());
    16
}
