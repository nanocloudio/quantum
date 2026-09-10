//! mqtt — MQTT 3.1 / 3.1.1 / 5.0 wire codec.
//!
//! Decodes client packets into session proposals and encodes session
//! responses back into MQTT frames, holding per-connection decode state
//! (partial packets, MQTT 5 topic aliases).
//!
//! ## Per-step bound
//!
//! `on_frame` handles ONE client record per call; `step` drains at most
//! the response quota from `in_responses`. The composite's dispatch
//! table owns the inbound record budget.

use super::abi::SyscallTable;
use super::{dev_channel_port, dev_log, dev_millis, wire};

// MQTT packet types
const PKT_CONNECT: u8 = 1;
const PKT_CONNACK: u8 = 2;
const PKT_PUBLISH: u8 = 3;
const PKT_PUBACK: u8 = 4;
const PKT_PUBREC: u8 = 5;
const PKT_PUBREL: u8 = 6;
const PKT_PUBCOMP: u8 = 7;
const PKT_SUBSCRIBE: u8 = 8;
const PKT_SUBACK: u8 = 9;
const PKT_UNSUBSCRIBE: u8 = 10;
const PKT_UNSUBACK: u8 = 11;
const PKT_PINGREQ: u8 = 12;
const PKT_PINGRESP: u8 = 13;
const PKT_DISCONNECT: u8 = 14;

// Raised 4096 -> 8192 for lattice CDC egress (CDC RFC C-1): a CDC
// envelope carries up to a 4 KiB row image plus identity framing, and
// the C-1 frame budget is envelope + MAX_VALUE_LEN.
const MAX_PACKET: usize = 8192;
/// Envelope-sized buffer for response messages from session_processor:
/// `[conn_id:u16 LE][proto][pkt_type][flags][body]` where body may be a
/// full MAX_PACKET-sized MQTT publish. Also sized to hold the encoded
/// MQTT frame (`[type][varint up to 4 B][body]`) for the largest body
/// without truncating during encode.
const RESP_BUF: usize = MAX_PACKET + 16;
/// Concurrent MQTT connections per node.
///
/// Each `ConnCtx` is 528 bytes resident (a `REASS_INLINE` reassembly
/// buffer plus counters and pool handles), so the table is
/// `MAX_CONNS * 528 B` = 8.25 MiB. Anything larger a connection needs
/// is lent from a shared pool only while it is in use: a `MAX_PACKET`
/// reassembly page (`BIG_POOL` = 128 pages, 1 MiB) while a packet
/// longer than the inline buffer is partial, and a topic-alias table
/// (`ALIAS_POOL` = 256 tables, 536 KiB) from the first alias an MQTT 5
/// connection binds in either direction. See `kafka.rs`'s `KCONNS`
/// for why these stay static arrays rather than moving to the
/// per-module heap.
const MAX_CONNS: usize = 16384;
/// Every value a [`wire::ConnId`] can take, so `slot_by_conn` needs no
/// bounds check on the id itself.
const CONN_ID_SPACE: usize = 1 << 16;
/// Bytes of conn-id prefix on every client record and session envelope.
const CID: usize = 2;
const MAX_TOPIC_ALIASES_PER_CONN: usize = 16;
const ALIAS_TOPIC_MAX: usize = 128;
/// Reassembly bytes resident in every `ConnCtx`. Covers every control
/// packet and the common small PUBLISH; a partial packet that outgrows
/// it moves the connection onto a pooled `MAX_PACKET` page.
const REASS_INLINE: usize = 512;
/// Pooled `MAX_PACKET` reassembly pages. A page is held only while a
/// connection has a partial packet longer than `REASS_INLINE`, so this
/// bounds the number of large packets in flight mid-reassembly, not
/// the number of connections.
const BIG_POOL: usize = 128;
/// Pooled topic-alias tables, shared by the inbound and outbound
/// directions. A connection borrows one per direction on the first
/// alias it binds and keeps it until close.
const ALIAS_POOL: usize = 256;

#[repr(C)]
#[derive(Clone, Copy)]
struct TopicAlias {
    alias: u16,
    topic_len: u16,
    topic: [u8; ALIAS_TOPIC_MAX],
    active: u8,
}

impl TopicAlias {
    const fn zero() -> Self {
        Self {
            alias: 0,
            topic_len: 0,
            topic: [0; ALIAS_TOPIC_MAX],
            active: 0,
        }
    }
}

/// One topic-alias table: the unit the alias pool lends out.
type AliasTable = [TopicAlias; MAX_TOPIC_ALIASES_PER_CONN];

#[repr(C)]
#[derive(Clone, Copy)]
struct ConnCtx {
    /// Reassembly bytes while no pooled page is held (`big == 0`):
    /// partial MQTT packet bytes that arrived split across TCP reads,
    /// or trailing bytes after a coalesced read consumed one packet
    /// but part of the next. `reass_len` counts the bytes in whichever
    /// buffer is current.
    reass_inline: [u8; REASS_INLINE],
    conn_id: wire::ConnId,
    /// `1 + index` of the `alias_pool` table holding this conn's
    /// inbound (publisher → broker) aliases — bound when an MQTT 5
    /// PUBLISH from this conn carries a `TopicAlias` property. 0 =
    /// none borrowed.
    alias_in: u16,
    /// `1 + index` of the `alias_pool` table holding this conn's
    /// outbound (broker → subscriber) aliases; 0 = none borrowed.
    /// Populated when this conn is a subscriber and we deliver an
    /// MQTT 5 PUBLISH on a new topic for the first time. Subsequent
    /// deliveries on the same topic encode an empty topic + the
    /// cached `TopicAlias` property per MQTT 5 §3.3.2.3.4 — a
    /// downstream-bandwidth win on hot topics.
    alias_out: u16,
    /// `TopicAliasMaximum` (CONNECT property 0x22) — the upper bound
    /// on alias values the server is permitted to send to this client.
    /// `0` (the spec default when the client omits the property) means
    /// the server MUST NOT send any `TopicAlias` properties.
    sub_topic_alias_max: u16,
    /// How many alias slots have been issued (1..=sub_topic_alias_max).
    /// Used as the next alias id to assign; we don't reuse retired
    /// slots in this first ship.
    sub_alias_next: u16,
    /// Number of bytes currently held in the reassembly buffer.
    reass_len: u16,
    /// `1 + index` of the `big_pool` page this conn reassembles into;
    /// 0 = the inline buffer.
    big: u16,
    protocol_version: u8,
    active: u8,
}

impl ConnCtx {
    const fn zero() -> Self {
        Self {
            reass_inline: [0u8; REASS_INLINE],
            conn_id: 0,
            alias_in: 0,
            alias_out: 0,
            sub_topic_alias_max: 0,
            sub_alias_next: 0,
            reass_len: 0,
            big: 0,
            protocol_version: 0,
            active: 0,
        }
    }
}

#[repr(C)]
pub struct Mqtt {
    /// The composite's anchor: every proposal goes through it
    /// ([`write_proposal`]), which is what binds the connection to a
    /// session worker and holds it across a handoff.
    pub anchor: *mut super::anchor::Anchor,
    pub out_frames: i32,

    packets_decoded: u32,
    packets_encoded: u32,
    parse_errors: u32,
    qos2_packets: u32,
    mqtt5_publishes: u32,
    topic_alias_hits: u32,
    /// `1 + index` of the connection whose front packet the proposal
    /// edge refused; 0 = none. While set, `flush_held` must land it
    /// before any further raw bytes are taken, or a later packet
    /// overtakes it and the session sees this connection out of order.
    held_ci: u16,
    /// Decoded packets the proposal edge refused and this codec held
    /// (retried, never dropped). Steady state 0.
    proposals_held: u32,
    /// Frames `out_frames` refused where nothing retries them
    /// (PINGRESP, a close request, a server DISCONNECT). Steady state 0.
    frames_refused: u32,
    /// Records dropped because a partial packet outgrew `REASS_INLINE`
    /// while every `big_pool` page was held. Steady state 0.
    reass_pool_full: u32,
    /// Alias bindings refused because every `alias_pool` table was
    /// held: an inbound one drops the PUBLISH, an outbound one
    /// delivers with the full topic. Steady state 0.
    alias_pool_full: u32,

    conns: [ConnCtx; MAX_CONNS],
    /// Direct index from conn id to its `conns` slot: `0` = no slot,
    /// otherwise `slot + 1`. Maintained on create and close.
    slot_by_conn: [u16; CONN_ID_SPACE],
    /// `MAX_PACKET` reassembly pages lent to connections whose partial
    /// packet outgrows the inline buffer. `big_owner[i]` is `1 + conn
    /// slot` while page `i` is held, 0 while free; `big_free` is a
    /// stack of the free page indices, `big_free_len` deep.
    big_pool: [[u8; MAX_PACKET]; BIG_POOL],
    big_owner: [u16; BIG_POOL],
    big_free: [u16; BIG_POOL],
    big_free_len: u16,
    /// Topic-alias tables lent to connections on their first binding
    /// in a direction. Same owner / free-stack shape as `big_pool`.
    alias_pool: [AliasTable; ALIAS_POOL],
    alias_owner: [u16; ALIAS_POOL],
    alias_free: [u16; ALIAS_POOL],
    alias_free_len: u16,
    /// Staging for one client record or one complete packet:
    /// `[conn_id:u16 LE][bytes]`, where bytes may be a full MAX_PACKET.
    in_buf: [u8; CID + MAX_PACKET],
    envelope: [u8; MAX_PACKET],
    /// Response read + frame encode buffer. Sized larger than MAX_PACKET
    /// to fit the session envelope header (5 B) and the MQTT frame
    /// encode header (1 B type + up to 4 B varint) without truncating a
    /// MAX_PACKET-sized body.
    resp_buf: [u8; RESP_BUF],
    frame: [u8; RESP_BUF],
}

impl Mqtt {
    fn find_or_create_conn(&mut self, conn_id: wire::ConnId) -> usize {
        let hit = self.slot_by_conn[conn_id as usize];
        if hit != 0 {
            return (hit - 1) as usize;
        }
        for i in 0..MAX_CONNS {
            if self.conns[i].active == 0 {
                self.conns[i] = ConnCtx {
                    conn_id,
                    active: 1,
                    ..ConnCtx::zero()
                };
                self.slot_by_conn[conn_id as usize] = (i as u16) + 1;
                return i;
            }
        }
        // Table full: slot 0 is shared, and the index is left clear so
        // the next record for this conn tries again for a free slot.
        0
    }

    /// Release `conn_id`'s slot, if it has one, returning any pooled
    /// page and alias tables it holds.
    fn release_conn(&mut self, conn_id: wire::ConnId) {
        let hit = self.slot_by_conn[conn_id as usize];
        if hit != 0 {
            let ci = (hit - 1) as usize;
            self.reass_release_page(ci);
            self.alias_release(ci);
            self.conns[ci] = ConnCtx::zero();
            self.slot_by_conn[conn_id as usize] = 0;
        }
    }

    // ── Reassembly buffer: inline bytes or a pooled page ──────────────

    /// Base pointer and capacity of connection `ci`'s current
    /// reassembly buffer: the inline bytes, or the pooled page while
    /// one is held.
    fn reass_ptr(&mut self, ci: usize) -> (*mut u8, usize) {
        let big = self.conns[ci].big;
        if big == 0 {
            (self.conns[ci].reass_inline.as_mut_ptr(), REASS_INLINE)
        } else {
            (self.big_pool[(big - 1) as usize].as_mut_ptr(), MAX_PACKET)
        }
    }

    /// The bytes held in connection `ci`'s reassembly buffer.
    fn reass_bytes(&self, ci: usize) -> &[u8] {
        let len = self.conns[ci].reass_len as usize;
        let big = self.conns[ci].big;
        if big == 0 {
            &self.conns[ci].reass_inline[..len]
        } else {
            &self.big_pool[(big - 1) as usize][..len]
        }
    }

    /// Make room for `need` bytes in connection `ci`'s reassembly
    /// buffer, moving it onto a pooled page (inline bytes copied
    /// across) when the inline buffer is too small. `false` when
    /// `need` exceeds `MAX_PACKET` or every page is held; the caller
    /// then drops the stream as it does an oversize packet.
    fn reass_reserve(&mut self, ci: usize, need: usize) -> bool {
        if need > MAX_PACKET {
            return false;
        }
        if need <= REASS_INLINE || self.conns[ci].big != 0 {
            return true;
        }
        if self.big_free_len == 0 {
            self.reass_pool_full = self.reass_pool_full.wrapping_add(1);
            return false;
        }
        self.big_free_len -= 1;
        let page = self.big_free[self.big_free_len as usize] as usize;
        self.big_owner[page] = (ci as u16) + 1;
        let len = self.conns[ci].reass_len as usize;
        self.big_pool[page][..len].copy_from_slice(&self.conns[ci].reass_inline[..len]);
        self.conns[ci].big = (page as u16) + 1;
        true
    }

    /// Return connection `ci`'s pooled page, if it holds one. Any bytes
    /// on the page are abandoned, so callers drain or reset first.
    fn reass_release_page(&mut self, ci: usize) {
        let big = self.conns[ci].big;
        if big == 0 {
            return;
        }
        let page = (big - 1) as usize;
        self.conns[ci].big = 0;
        // The owner record is the guard against pushing one page onto
        // the free stack twice: only the recorded holder returns it.
        if self.big_owner[page] != (ci as u16) + 1 {
            return;
        }
        self.big_owner[page] = 0;
        self.big_free[self.big_free_len as usize] = page as u16;
        self.big_free_len += 1;
    }

    /// Return the pooled page once a drain has emptied the buffer, so
    /// a page is held only while a packet is partial.
    fn reass_release_if_empty(&mut self, ci: usize) {
        if self.conns[ci].reass_len == 0 {
            self.reass_release_page(ci);
        }
    }

    /// Discard connection `ci`'s partial bytes and release its page:
    /// the oversize / malformed-stream reset.
    fn reass_reset(&mut self, ci: usize) {
        self.conns[ci].reass_len = 0;
        self.reass_release_page(ci);
    }

    // ── Topic-alias tables: pooled per direction ──────────────────────

    /// Take a free alias table for connection `ci`, returning its
    /// `1 + index` handle, or 0 (counted in `alias_pool_full`) when
    /// every table is held.
    fn alias_borrow(&mut self, ci: usize) -> u16 {
        if self.alias_free_len == 0 {
            self.alias_pool_full = self.alias_pool_full.wrapping_add(1);
            return 0;
        }
        self.alias_free_len -= 1;
        let t = self.alias_free[self.alias_free_len as usize] as usize;
        self.alias_owner[t] = (ci as u16) + 1;
        self.alias_pool[t] = [TopicAlias::zero(); MAX_TOPIC_ALIASES_PER_CONN];
        (t as u16) + 1
    }

    /// Return one of connection `ci`'s table handles (`1 + index`, 0 =
    /// nothing) to the pool. Only the recorded holder may return a
    /// table, so a stale handle cannot push it onto the free stack
    /// twice.
    fn alias_return(&mut self, ci: usize, handle: u16) {
        if handle == 0 {
            return;
        }
        let t = (handle - 1) as usize;
        if self.alias_owner[t] != (ci as u16) + 1 {
            return;
        }
        self.alias_owner[t] = 0;
        self.alias_free[self.alias_free_len as usize] = t as u16;
        self.alias_free_len += 1;
    }

    /// Return both of connection `ci`'s alias tables.
    fn alias_release(&mut self, ci: usize) {
        let (a_in, a_out) = (self.conns[ci].alias_in, self.conns[ci].alias_out);
        self.conns[ci].alias_in = 0;
        self.conns[ci].alias_out = 0;
        self.alias_return(ci, a_in);
        self.alias_return(ci, a_out);
    }

    /// Connection `ci`'s inbound alias table, borrowing one on first
    /// use. `None` when the pool is exhausted.
    fn alias_in_table(&mut self, ci: usize) -> Option<&mut AliasTable> {
        if self.conns[ci].alias_in == 0 {
            self.conns[ci].alias_in = self.alias_borrow(ci);
        }
        let h = self.conns[ci].alias_in;
        if h == 0 {
            None
        } else {
            Some(&mut self.alias_pool[(h - 1) as usize])
        }
    }

    /// Connection `ci`'s inbound alias table for lookup only; `None`
    /// when it has never bound an alias.
    fn alias_in_lookup(&self, ci: usize) -> Option<&AliasTable> {
        let h = self.conns[ci].alias_in;
        if h == 0 {
            None
        } else {
            Some(&self.alias_pool[(h - 1) as usize])
        }
    }

    /// Index into `alias_pool` of connection `ci`'s outbound table.
    /// Borrows one only when a delivery is about to assign an alias
    /// (`sub_alias_next < sub_topic_alias_max`); `None` when there is
    /// no table and none is needed, or the pool is exhausted — the
    /// delivery then goes out with its full topic.
    fn alias_out_index(&mut self, ci: usize) -> Option<usize> {
        let c = &self.conns[ci];
        if c.alias_out == 0 && c.sub_alias_next < c.sub_topic_alias_max {
            self.conns[ci].alias_out = self.alias_borrow(ci);
        }
        let h = self.conns[ci].alias_out;
        if h == 0 {
            None
        } else {
            Some((h - 1) as usize)
        }
    }
}

/// Every pool page and table is lent by `1 + conn slot`, and every
/// conn slot is addressed by `u16`, so both fit their handles.
const _: () = assert!(MAX_CONNS < u16::MAX as usize);
const _: () = assert!(BIG_POOL < u16::MAX as usize);
const _: () = assert!(ALIAS_POOL < u16::MAX as usize);
/// The codec's whole state stays under 12 MiB: ~10.4 MiB as built
/// (conns 8,650,752 + big_pool 1,048,576 + alias_pool 548,864 +
/// slot_by_conn 131,072 + the four staging buffers and counters). The
/// connection table is the only term that scales with `MAX_CONNS`, so
/// the bound makes the next doubling an explicit decision against the
/// state arena.
const _: () = assert!(core::mem::size_of::<Mqtt>() < 12 * 1024 * 1024);

// ── MQTT varint and UTF-8 string helpers ─────────────────────────────────

fn decode_varint(buf: &[u8]) -> (u32, usize) {
    let mut val = 0u32;
    let mut mult = 1u32;
    let mut i = 0;
    while i < buf.len() && i < 4 {
        let b = buf[i];
        val = val.wrapping_add((b & 0x7F) as u32 * mult);
        i += 1;
        if (b & 0x80) == 0 {
            return (val, i);
        }
        mult = mult.wrapping_mul(128);
    }
    (0, 0)
}

fn encode_varint(buf: &mut [u8], mut val: u32) -> usize {
    let mut i = 0;
    loop {
        if i >= buf.len() {
            return 0;
        }
        let mut b = (val & 0x7F) as u8;
        val >>= 7;
        if val > 0 {
            b |= 0x80;
        }
        buf[i] = b;
        i += 1;
        if val == 0 {
            break;
        }
    }
    i
}

fn read_utf8_string(buf: &[u8]) -> (&[u8], usize) {
    if buf.len() < 2 {
        return (&[], 0);
    }
    let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if buf.len() < 2 + len {
        return (&[], 0);
    }
    (&buf[2..2 + len], 2 + len)
}

// ── MQTT 5 property block ────────────────────────────────────────────────

const PROP_MESSAGE_EXPIRY_INTERVAL: u8 = 0x02;
const PROP_SUBSCRIPTION_IDENTIFIER: u8 = 0x0B;
const PROP_SESSION_EXPIRY_INTERVAL: u8 = 0x11;
const PROP_SERVER_KEEP_ALIVE: u8 = 0x13;
const PROP_RECEIVE_MAXIMUM: u8 = 0x21;
const PROP_TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
const PROP_TOPIC_ALIAS: u8 = 0x23;
const PROP_USER_PROPERTY: u8 = 0x26;
const PROP_WILL_DELAY_INTERVAL: u8 = 0x18;

#[derive(Clone, Copy, Default)]
struct Mqtt5Props {
    topic_alias: u16,
    /// CONNECT property 0x22 — upper bound on alias values the server
    /// is permitted to send to this client (MQTT 5 §3.1.2.11.5).
    topic_alias_maximum: u16,
    message_expiry_s: u32,
    session_expiry_s: u32,
    receive_maximum: u16,
    subscription_id: u32,
    will_delay_s: u32,
    has_will_delay: bool,
}

fn parse_properties(buf: &[u8]) -> (Mqtt5Props, usize) {
    let mut props = Mqtt5Props::default();
    let (prop_len, vlen) = decode_varint(buf);
    if vlen == 0 {
        return (props, 0);
    }
    let end = vlen + prop_len as usize;
    if end > buf.len() {
        return (props, 0);
    }
    let mut pos = vlen;
    while pos < end {
        let id = buf[pos];
        pos += 1;
        match id {
            PROP_MESSAGE_EXPIRY_INTERVAL
            | PROP_SESSION_EXPIRY_INTERVAL
            | PROP_WILL_DELAY_INTERVAL => {
                if pos + 4 > end {
                    break;
                }
                let v = u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
                match id {
                    PROP_MESSAGE_EXPIRY_INTERVAL => props.message_expiry_s = v,
                    PROP_SESSION_EXPIRY_INTERVAL => props.session_expiry_s = v,
                    PROP_WILL_DELAY_INTERVAL => {
                        props.will_delay_s = v;
                        props.has_will_delay = true;
                    }
                    _ => {}
                }
                pos += 4;
            }
            PROP_RECEIVE_MAXIMUM
            | PROP_TOPIC_ALIAS_MAXIMUM
            | PROP_TOPIC_ALIAS
            | PROP_SERVER_KEEP_ALIVE => {
                if pos + 2 > end {
                    break;
                }
                let v = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
                match id {
                    PROP_RECEIVE_MAXIMUM => props.receive_maximum = v,
                    PROP_TOPIC_ALIAS => props.topic_alias = v,
                    PROP_TOPIC_ALIAS_MAXIMUM => props.topic_alias_maximum = v,
                    _ => {}
                }
                pos += 2;
            }
            PROP_SUBSCRIPTION_IDENTIFIER => {
                let (v, vlen) = decode_varint(&buf[pos..end]);
                if vlen == 0 {
                    break;
                }
                props.subscription_id = v;
                pos += vlen;
            }
            PROP_USER_PROPERTY => {
                if pos + 2 > end {
                    break;
                }
                let klen = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
                if pos + 2 + klen > end {
                    break;
                }
                let vpos = pos + 2 + klen;
                if vpos + 2 > end {
                    break;
                }
                let vlen = u16::from_be_bytes([buf[vpos], buf[vpos + 1]]) as usize;
                if vpos + 2 + vlen > end {
                    break;
                }
                pos = vpos + 2 + vlen;
            }
            _ => {
                // Unknown property: for safety, skip to end of block
                if pos + 2 <= end {
                    let slen = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
                    if pos + 2 + slen <= end {
                        pos += 2 + slen;
                        continue;
                    }
                }
                break;
            }
        }
    }
    (props, end)
}

/// Caps shared with `session_processor`'s QOP_PUBLISH V2 body
/// limits. Keeping them defined here lets `the mqtt component` reject
/// oversized inputs before they reach the propose-side proposer —
/// matches the existing pattern for Will fields.
const MAX_USER_PROPS_COUNT: usize = 4;
const MAX_USER_PROP_KEY_LEN: usize = 64;
const MAX_USER_PROP_VAL_LEN: usize = 128;

/// Scan an MQTT 5 property block (same shape `parse_properties`
/// consumes) and emit a self-delimiting User Property block into
/// `out`:
///   `[count:u8][per prop: key_len:u16 BE, key, val_len:u16 BE, val]`
///
/// Skips other property ids — they were already consumed by
/// `parse_properties` for their typed fields. Returns the number of
/// bytes written, or 0 if `out` is too small or the block doesn't
/// parse. Properties beyond `MAX_USER_PROPS_COUNT` (or oversized
/// key/value pairs) are dropped silently — admission control rather
/// than apply-side state corruption.
fn extract_user_props_block(buf: &[u8], out: &mut [u8]) -> usize {
    if out.is_empty() {
        return 0;
    }
    let (prop_len, vlen) = decode_varint(buf);
    if vlen == 0 {
        out[0] = 0;
        return 1;
    }
    let end = vlen + prop_len as usize;
    if end > buf.len() {
        out[0] = 0;
        return 1;
    }

    let count_off = 0usize;
    let mut write_off = 1usize;
    let mut written_count = 0u8;
    out[count_off] = 0;
    let mut pos = vlen;
    while pos < end {
        let id = buf[pos];
        pos += 1;
        match id {
            PROP_MESSAGE_EXPIRY_INTERVAL
            | PROP_SESSION_EXPIRY_INTERVAL
            | PROP_WILL_DELAY_INTERVAL => {
                if pos + 4 > end {
                    break;
                }
                pos += 4;
            }
            PROP_RECEIVE_MAXIMUM
            | PROP_TOPIC_ALIAS_MAXIMUM
            | PROP_TOPIC_ALIAS
            | PROP_SERVER_KEEP_ALIVE => {
                if pos + 2 > end {
                    break;
                }
                pos += 2;
            }
            PROP_SUBSCRIPTION_IDENTIFIER => {
                let (_, sv) = decode_varint(&buf[pos..end]);
                if sv == 0 {
                    break;
                }
                pos += sv;
            }
            PROP_USER_PROPERTY => {
                if pos + 2 > end {
                    break;
                }
                let klen = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
                if pos + 2 + klen > end {
                    break;
                }
                let kstart = pos + 2;
                let vpos = kstart + klen;
                if vpos + 2 > end {
                    break;
                }
                let vlenv = u16::from_be_bytes([buf[vpos], buf[vpos + 1]]) as usize;
                if vpos + 2 + vlenv > end {
                    break;
                }
                let vstart = vpos + 2;
                pos = vstart + vlenv;
                // Admission control: drop overcap entries.
                if (written_count as usize) >= MAX_USER_PROPS_COUNT {
                    continue;
                }
                if klen > MAX_USER_PROP_KEY_LEN || vlenv > MAX_USER_PROP_VAL_LEN {
                    continue;
                }
                let need = 2 + klen + 2 + vlenv;
                if write_off + need > out.len() {
                    break;
                }
                out[write_off..write_off + 2].copy_from_slice(&(klen as u16).to_be_bytes());
                out[write_off + 2..write_off + 2 + klen]
                    .copy_from_slice(&buf[kstart..kstart + klen]);
                let vlen_off = write_off + 2 + klen;
                out[vlen_off..vlen_off + 2].copy_from_slice(&(vlenv as u16).to_be_bytes());
                out[vlen_off + 2..vlen_off + 2 + vlenv]
                    .copy_from_slice(&buf[vstart..vstart + vlenv]);
                write_off += need;
                written_count += 1;
            }
            _ => {
                if pos + 2 <= end {
                    let slen = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
                    if pos + 2 + slen <= end {
                        pos += 2 + slen;
                        continue;
                    }
                }
                break;
            }
        }
    }
    out[count_off] = written_count;
    write_off
}

// ── CONNECT fields ────────────────────────────────────────────────────────

const CONNECT_FLAG_WILL_RETAIN: u8 = 0x20;
const CONNECT_FLAG_WILL_QOS_MASK: u8 = 0x18;
const CONNECT_FLAG_WILL_FLAG: u8 = 0x04;
const CONNECT_FLAG_CLEAN_START: u8 = 0x02;
const CONNECT_FLAG_USERNAME: u8 = 0x80;

#[derive(Clone, Copy, Default)]
struct ConnectInfo<'a> {
    client_id: &'a [u8],
    protocol_version: u8,
    clean_start: u8,
    keep_alive: u16,
    username: &'a [u8],
    will: Option<WillInfo<'a>>,
    props: Mqtt5Props,
}

#[derive(Clone, Copy, Default)]
struct WillInfo<'a> {
    qos: u8,
    retain: u8,
    topic: &'a [u8],
    payload: &'a [u8],
    delay_s: u32,
}

fn parse_connect(body: &[u8]) -> ConnectInfo<'_> {
    let mut info = ConnectInfo::default();
    let (_name, pn_len) = read_utf8_string(body);
    if pn_len == 0 {
        return info;
    }
    let mut cur = pn_len;
    if cur + 4 > body.len() {
        return info;
    }
    info.protocol_version = body[cur];
    cur += 1;
    let flags = body[cur];
    cur += 1;
    info.clean_start = if (flags & CONNECT_FLAG_CLEAN_START) != 0 {
        1
    } else {
        0
    };
    info.keep_alive = u16::from_be_bytes([body[cur], body[cur + 1]]);
    cur += 2;

    if info.protocol_version >= 5 {
        let (props, plen) = parse_properties(&body[cur..]);
        info.props = props;
        cur += plen;
    }

    let (client_id, cid_len) = read_utf8_string(&body[cur..]);
    info.client_id = client_id;
    cur += cid_len;

    if (flags & CONNECT_FLAG_WILL_FLAG) != 0 && cur < body.len() {
        let mut will = WillInfo {
            qos: (flags & CONNECT_FLAG_WILL_QOS_MASK) >> 3,
            retain: u8::from((flags & CONNECT_FLAG_WILL_RETAIN) != 0),
            ..Default::default()
        };
        if info.protocol_version >= 5 {
            let (wprops, wlen) = parse_properties(&body[cur..]);
            will.delay_s = wprops.will_delay_s;
            cur += wlen;
        }
        let (wt, wtlen) = read_utf8_string(&body[cur..]);
        will.topic = wt;
        cur += wtlen;
        if cur + 2 <= body.len() {
            let wpl = u16::from_be_bytes([body[cur], body[cur + 1]]) as usize;
            cur += 2;
            if cur + wpl <= body.len() {
                will.payload = &body[cur..cur + wpl];
                cur += wpl;
            }
        }
        info.will = Some(will);
    }

    if (flags & CONNECT_FLAG_USERNAME) != 0 && cur < body.len() {
        let (un, un_len) = read_utf8_string(&body[cur..]);
        info.username = un;
        cur += un_len;
    }
    let _ = cur;
    info
}

// ── Module entrypoints ────────────────────────────────────────────────────

/// Component defaults. Channel handles are assigned by the
/// composite after this returns.
pub fn init(s: &mut Mqtt) {
    for i in 0..MAX_CONNS {
        s.conns[i] = ConnCtx::zero();
    }
    s.slot_by_conn = [0; CONN_ID_SPACE];
    for i in 0..BIG_POOL {
        s.big_owner[i] = 0;
        s.big_free[i] = i as u16;
    }
    s.big_free_len = BIG_POOL as u16;
    for i in 0..ALIAS_POOL {
        s.alias_owner[i] = 0;
        s.alias_free[i] = i as u16;
    }
    s.alias_free_len = ALIAS_POOL as u16;
}

/// Emit a `[conn_id:u16 LE][bytes]` payload as a `MSG_CLIENT_FRAME`
/// envelope-framed message to protocol. Framing matters because
/// `protocol` is a fan-in merge over the codec writers (mqtt/amqp/
/// kafka) and back-to-back raw writes coalesce on the merge
/// module's byte FIFO, mangling the next frame's `conn_id`. Same
/// pattern as the `codec_in` fan-in.
/// # Safety
unsafe fn write_conn_frame(
    sys: &SyscallTable,
    chan: i32,
    conn_id: wire::ConnId,
    bytes: &[u8],
) -> bool {
    if chan < 0 {
        return false;
    }
    const FRAME_BUF: usize = CID + MAX_PACKET;
    let total = CID + bytes.len();
    if total > FRAME_BUF {
        return false;
    }
    let mut out = [0u8; FRAME_BUF];
    out[..CID].copy_from_slice(&conn_id.to_le_bytes());
    out[CID..total].copy_from_slice(bytes);
    let w = wire::channel_write_msg(sys, chan, wire::MSG_CLIENT_FRAME, &out[..total]);
    w > 0
}

/// Encode an MQTT packet: [type|flags] [varint len] [body].
fn encode_mqtt_frame(out: &mut [u8], pkt_type: u8, flags: u8, body: &[u8]) -> usize {
    if out.is_empty() {
        return 0;
    }
    let mut vbuf = [0u8; 4];
    let vlen = encode_varint(&mut vbuf, body.len() as u32);
    if vlen == 0 {
        return 0;
    }
    let total = 1 + vlen + body.len();
    if total > out.len() {
        return 0;
    }
    out[0] = (pkt_type << 4) | (flags & 0x0F);
    out[1..1 + vlen].copy_from_slice(&vbuf[..vlen]);
    out[1 + vlen..total].copy_from_slice(body);
    total
}

/// Reshape an MQTT-3.1.1-style ack body for MQTT 5. Session_processor
/// emits these in the 3.1.1 shape (CONNACK: `[ack_flags][reason]`,
/// SUBACK: `[packet_id][reason_codes...]`, UNSUBACK: `[packet_id]`);
/// MQTT 5 §3.2.2.3 / §3.9.2 / §3.11.2 require an additional empty
/// properties varint, and §3.11.3 also adds a per-topic reason code to
/// UNSUBACK that the 3.1.1 wire shape doesn't have. Returns the bytes
/// written to `out`, or 0 on overflow / bad body.
fn splice_mqtt5_ack(out: &mut [u8], pkt_type: u8, body: &[u8]) -> usize {
    if body.len() < 2 {
        return 0;
    }
    // Common splice for CONNACK / SUBACK: insert `0x00` (props varint)
    // at body offset 2, shift the rest right.
    let total = body.len() + 1;
    if total + 1 > out.len() {
        return 0;
    }
    out[0..2].copy_from_slice(&body[..2]);
    out[2] = 0;
    if body.len() > 2 {
        let tail = body.len() - 2;
        out[3..3 + tail].copy_from_slice(&body[2..]);
    }
    if pkt_type == PKT_UNSUBACK {
        // MQTT 3.1.1 UNSUBACK has no reason codes — body is just
        // `[packet_id]`. MQTT 5 §3.11.3 requires one reason code per
        // unsubscribed topic. session_processor only ever issues
        // single-topic UNSUBSCRIBEs in the current MQTT path, so a
        // single 0x00 (Success) is the correct trailer.
        out[total] = 0;
        total + 1
    } else {
        total
    }
}

/// Length-of-block parser for the self-delimiting user_props format
/// (`[count:u8][per prop: key_len BE, key, val_len BE, val]`). Returns
/// `None` on malformed bytes; the encoder treats malformed as
/// "no user_props".
fn user_props_block_len(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }
    let count = buf[0] as usize;
    let mut off = 1usize;
    for _ in 0..count {
        if off + 2 > buf.len() {
            return None;
        }
        let klen = u16::from_be_bytes([buf[off], buf[off + 1]]) as usize;
        off += 2 + klen;
        if off + 2 > buf.len() {
            return None;
        }
        let vlen = u16::from_be_bytes([buf[off], buf[off + 1]]) as usize;
        off += 2 + vlen;
        if off > buf.len() {
            return None;
        }
    }
    Some(off)
}

/// Encode a PUBLISH frame for an MQTT 5 subscriber, optionally
/// substituting the topic with a cached `TopicAlias` property.
///
/// `body` carries the MQTT-3.1.1-shaped payload that session_processor
/// emits on `MSG_SESSION_RESPONSE`:
///   `[topic_len:u16 BE][topic][packet_id:u16 BE if qos>0][payload]`
///
/// On a fresh topic (no alias yet, slot count below
/// `sub_topic_alias_max`) this assigns the next alias id and writes
/// the full topic + a `TopicAlias` property — the subscriber must
/// remember the mapping. On a known topic it writes a zero-length
/// topic + the same property — the subscriber expands it back via
/// its remembered mapping. When `sub_topic_alias_max == 0` (the
/// MQTT-default no-alias case) the function falls back to a bare
/// MQTT 5 PUBLISH (empty property block). Returns 0 on overflow / bad
/// body — caller then skips the emit.
fn encode_mqtt5_publish_with_alias(
    out: &mut [u8],
    flags: u8,
    body: &[u8],
    sub_aliases: &mut [TopicAlias],
    sub_topic_alias_max: u16,
    sub_alias_next: &mut u16,
) -> usize {
    // Parse out the body components. session_processor's emit_codec_
    // response payload shape (PUBLISH only) is:
    //   `[topic_len BE][topic][packet_id BE if qos>0][user_props_block][payload]`
    if body.len() < 2 {
        return 0;
    }
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    if 2 + topic_len > body.len() {
        return 0;
    }
    let topic_off = 2;
    let mut cur = topic_off + topic_len;
    let qos = (flags >> 1) & 0x03;
    let packet_id_bytes: &[u8] = if qos > 0 {
        if cur + 2 > body.len() {
            return 0;
        }
        let s = &body[cur..cur + 2];
        cur += 2;
        s
    } else {
        &[]
    };
    let up_off = cur;
    let up_len = user_props_block_len(&body[up_off..]).unwrap_or(0);
    let payload_off = up_off + up_len;
    if payload_off > body.len() {
        return 0;
    }
    let payload_len = body.len() - payload_off;
    let up_count = if up_len > 0 { body[up_off] as usize } else { 0 };

    // Topic alias decision.
    let (alias_id, send_full_topic) = if sub_topic_alias_max == 0 || topic_len == 0 {
        (0u16, true)
    } else {
        // Existing alias?
        let mut found: Option<u16> = None;
        for a in sub_aliases.iter() {
            if a.active == 1
                && a.topic_len as usize == topic_len
                && a.topic[..topic_len] == body[topic_off..topic_off + topic_len]
            {
                found = Some(a.alias);
                break;
            }
        }
        match found {
            Some(id) => (id, false),
            None => {
                // Allocate a new alias if we can.
                if *sub_alias_next < sub_topic_alias_max && topic_len <= ALIAS_TOPIC_MAX {
                    let id = *sub_alias_next + 1; // alias ids start at 1
                    let mut placed = false;
                    for a in sub_aliases.iter_mut() {
                        if a.active == 0 {
                            a.alias = id;
                            a.topic_len = topic_len as u16;
                            a.topic[..topic_len]
                                .copy_from_slice(&body[topic_off..topic_off + topic_len]);
                            a.active = 1;
                            placed = true;
                            break;
                        }
                    }
                    if placed {
                        *sub_alias_next += 1;
                        (id, true) // first use: full topic + alias property
                    } else {
                        (0, true) // table full: no alias
                    }
                } else {
                    (0, true) // out of allowance: no alias
                }
            }
        }
    };

    // Compute MQTT 5 user_property bytes that we'll emit verbatim
    // (each property block prepended with id=0x26): for each pair the
    // wire bytes are `[0x26][klen BE][k][vlen BE][v]`. The
    // user_props_block layout already has [klen BE][k][vlen BE][v]
    // per pair (count byte at offset 0), so we just need to write
    // 0x26 before each pair while iterating.
    let user_prop_bytes_total: usize = if up_count > 0 {
        // up_len includes the 1-byte count; properties bytes = up_len - 1.
        // Plus 1 extra id byte (0x26) per pair.
        (up_len - 1) + up_count
    } else {
        0
    };

    // Properties block: optional TopicAlias property (id=0x23, u16 BE)
    // + zero or more UserProperty entries (id=0x26).
    let alias_bytes_total = if alias_id > 0 { 3 } else { 0 };
    let props_len = alias_bytes_total + user_prop_bytes_total;
    let mut props_vbuf = [0u8; 4];
    let props_vlen = encode_varint(&mut props_vbuf, props_len as u32);
    if props_vlen == 0 {
        return 0;
    }

    let emit_topic_len = if send_full_topic { topic_len } else { 0 };
    let body_total = 2                  // topic_len BE
        + emit_topic_len
        + packet_id_bytes.len()
        + props_vlen
        + props_len
        + payload_len;

    let mut fb_vbuf = [0u8; 4];
    let fb_vlen = encode_varint(&mut fb_vbuf, body_total as u32);
    if fb_vlen == 0 {
        return 0;
    }
    let total = 1 + fb_vlen + body_total;
    if total > out.len() {
        return 0;
    }

    let mut p = 0usize;
    out[p] = (PKT_PUBLISH << 4) | (flags & 0x0F);
    p += 1;
    out[p..p + fb_vlen].copy_from_slice(&fb_vbuf[..fb_vlen]);
    p += fb_vlen;
    out[p..p + 2].copy_from_slice(&(emit_topic_len as u16).to_be_bytes());
    p += 2;
    if send_full_topic && topic_len > 0 {
        out[p..p + topic_len].copy_from_slice(&body[topic_off..topic_off + topic_len]);
        p += topic_len;
    }
    if !packet_id_bytes.is_empty() {
        out[p..p + packet_id_bytes.len()].copy_from_slice(packet_id_bytes);
        p += packet_id_bytes.len();
    }
    out[p..p + props_vlen].copy_from_slice(&props_vbuf[..props_vlen]);
    p += props_vlen;
    // TopicAlias property (if any).
    if alias_id > 0 {
        out[p] = 0x23;
        out[p + 1..p + 3].copy_from_slice(&alias_id.to_be_bytes());
        p += 3;
    }
    // UserProperty entries (each prepended with id=0x26). The
    // user_props_block in `body` already has [klen BE][k][vlen BE][v]
    // per pair right after the count byte at `up_off`.
    if up_count > 0 {
        let mut up_cur = up_off + 1; // skip count byte
        for _ in 0..up_count {
            // Each pair: klen + key + vlen + val.
            let klen = u16::from_be_bytes([body[up_cur], body[up_cur + 1]]) as usize;
            let vlen_off = up_cur + 2 + klen;
            let vlen = u16::from_be_bytes([body[vlen_off], body[vlen_off + 1]]) as usize;
            let pair_total = 2 + klen + 2 + vlen;
            out[p] = 0x26;
            p += 1;
            out[p..p + pair_total].copy_from_slice(&body[up_cur..up_cur + pair_total]);
            p += pair_total;
            up_cur += pair_total;
        }
    }
    if payload_len > 0 {
        out[p..p + payload_len].copy_from_slice(&body[payload_off..payload_off + payload_len]);
        p += payload_len;
    }
    p
}

/// Strip the inline user_props block from an emit_codec_response
/// PUBLISH body so an MQTT 3.1.1 subscriber's wire stays
/// 3.1.1-shaped. The body shape coming in is
/// `[topic_len BE][topic][packet_id BE if qos>0][user_props_block][payload]`;
/// returns a slice covering `[topic_len BE][topic][packet_id?][payload]`
/// using a caller-provided scratch buffer. Returns 0 on overflow / bad
/// body.
fn strip_user_props_for_v311(out: &mut [u8], flags: u8, body: &[u8]) -> usize {
    if body.len() < 2 {
        return 0;
    }
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    if 2 + topic_len > body.len() {
        return 0;
    }
    let mut cur = 2 + topic_len;
    let qos = (flags >> 1) & 0x03;
    let packet_id_bytes: &[u8] = if qos > 0 {
        if cur + 2 > body.len() {
            return 0;
        }
        let s = &body[cur..cur + 2];
        cur += 2;
        s
    } else {
        &[]
    };
    let up_len = user_props_block_len(&body[cur..]).unwrap_or(0);
    let payload_off = cur + up_len;
    if payload_off > body.len() {
        return 0;
    }
    let payload_len = body.len() - payload_off;
    let total = 2 + topic_len + packet_id_bytes.len() + payload_len;
    if total > out.len() {
        return 0;
    }
    out[0..2 + topic_len].copy_from_slice(&body[0..2 + topic_len]);
    let mut p = 2 + topic_len;
    if !packet_id_bytes.is_empty() {
        out[p..p + packet_id_bytes.len()].copy_from_slice(packet_id_bytes);
        p += packet_id_bytes.len();
    }
    if payload_len > 0 {
        out[p..p + payload_len].copy_from_slice(&body[payload_off..payload_off + payload_len]);
        p += payload_len;
    }
    p
}

/// One step of this component.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
/// Handle one client record: `[conn_id:u16 LE][tcp chunk]` framed as
/// MSG_CLIENT_FRAME, or MSG_CONN_CLOSED carrying just the conn id.
///
/// TCP delivers byte streams, not packets — a single MQTT frame may
/// arrive across several records, or several frames in one. The
/// per-connection reassembly buffer tracks both cases: every record appends,
/// and an inner loop pops every complete packet, leaving trailing
/// partial bytes parked for the next call.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_frame(s: &mut Mqtt, sys: &SyscallTable, mtype: u8, payload: &[u8]) -> bool {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        // the router component frames each client record as a MSG_CLIENT_FRAME
        // envelope so records from different conn_ids don't coalesce on the
        // byte FIFO. Read one envelope per iteration; payload is
        // `[conn_id:u16 LE][tcp chunk]`.
        // Stage the record into the conn reassembly path exactly as the
        // channel read did: `in_buf` holds `[conn_id][tcp chunk]`.
        let n = payload.len();
        if n > s.in_buf.len() {
            return false;
        }
        s.in_buf[..n].copy_from_slice(payload);
        if n < CID {
            return false;
        }

        let conn_id = u16::from_le_bytes([s.in_buf[0], s.in_buf[1]]);

        // Connection closed: clear this conn's reassembly slot so a
        // reused conn_id can't inherit a stale partial packet. MQTT
        // session teardown (will firing, session expiry) stays on the
        // apply-side keep-alive path — this only resets codec framing.
        if mtype == wire::MSG_CONN_CLOSED {
            s.release_conn(conn_id);
            return false;
        }

        let chunk_len = n - CID;
        if chunk_len == 0 {
            return false;
        }
        let ci = s.find_or_create_conn(conn_id);

        // Append this chunk to the conn's reassembly buffer, borrowing
        // a pooled page when it outgrows the inline bytes. Bail on
        // overflow — MAX_PACKET is the largest packet we claim to
        // honour — or when no page is free (`reass_pool_full`); either
        // way the partial bytes are discarded rather than left
        // misaligned for the next record.
        let cur = s.conns[ci].reass_len as usize;
        if cur + chunk_len > MAX_PACKET {
            s.parse_errors += 1;
            s.reass_reset(ci);
            return false;
        }
        if !s.reass_reserve(ci, cur + chunk_len) {
            s.parse_errors += 1;
            s.reass_reset(ci);
            if s.reass_pool_full & 0x1F == 1 {
                dev_log(sys, 2, b"[mqtt] reass pool full".as_ptr(), 22);
            }
            return false;
        }
        let (reass, _) = s.reass_ptr(ci);
        core::ptr::copy_nonoverlapping(s.in_buf.as_ptr().add(CID), reass.add(cur), chunk_len);
        s.conns[ci].reass_len = (cur + chunk_len) as u16;

        // Inner loop: drain every complete packet from the buffer. The
        // packet bytes are staged into `in_buf` so the existing
        // decode body can address them through `pkt_start` /
        // `pkt_bytes` unchanged.
        drain_conn(s, sys, ci, conn_id)
    }
}

/// Retry the packet a refused proposal write left at the front of its
/// connection's reassembly buffer. `true` when nothing is held (the
/// caller may take new raw bytes); `false` while the edge still
/// refuses it.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn flush_held(s: &mut Mqtt, sys: &SyscallTable) -> bool {
    if s.held_ci == 0 {
        return true;
    }
    let ci = (s.held_ci - 1) as usize;
    s.held_ci = 0;
    if ci >= MAX_CONNS || s.conns[ci].active == 0 {
        return true;
    }
    let conn_id = s.conns[ci].conn_id;
    !drain_conn(s, sys, ci, conn_id)
}

/// Decode and emit every complete packet at the front of connection
/// `ci`'s reassembly buffer. Returns `true` when a packet is HELD: the
/// proposal edge refused it, it is back at the front of the buffer
/// unchanged, and the caller must stop taking raw bytes until
/// `flush_held` lands it.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
unsafe fn drain_conn(s: &mut Mqtt, sys: &SyscallTable, ci: usize, conn_id: wire::ConnId) -> bool {
    unsafe {
        loop {
            // A pooled page goes back once the buffer is empty. This
            // sits at the loop head rather than at the shift below so
            // a refused proposal can put its packet back on the page
            // it came from without borrowing again.
            s.reass_release_if_empty(ci);
            let avail = s.conns[ci].reass_len as usize;
            if avail < 2 {
                break;
            }
            let probe_end = (1 + 4).min(avail);
            let (rem_len_probe, vlen_probe) = decode_varint(&s.reass_bytes(ci)[1..probe_end]);
            if vlen_probe == 0 {
                // Either malformed (varint > 4 bytes) or incomplete.
                if avail >= 5 {
                    s.parse_errors += 1;
                    s.reass_reset(ci);
                }
                break;
            }
            let pkt_size = 1 + vlen_probe + rem_len_probe as usize;
            if pkt_size > MAX_PACKET {
                s.parse_errors += 1;
                s.reass_reset(ci);
                break;
            }
            if pkt_size > avail {
                break;
            }

            // Stage one full packet at in_buf[CID..CID+pkt_size] with
            // the conn_id (u16 LE) at in_buf[..CID].
            s.in_buf[..CID].copy_from_slice(&conn_id.to_le_bytes());
            let (reass, _) = s.reass_ptr(ci);
            core::ptr::copy_nonoverlapping(reass, s.in_buf.as_mut_ptr().add(CID), pkt_size);

            // Shift trailing bytes (possible next packet) down so
            // the buffer is ready for the next iteration even if
            // decode `continue`s on error.
            let remaining = avail - pkt_size;
            if remaining > 0 {
                core::ptr::copy(reass.add(pkt_size), reass, remaining);
            }
            s.conns[ci].reass_len = remaining as u16;

            let pkt_start = CID;
            let pkt_bytes = pkt_size;

            // Parse MQTT fixed header (same as legacy single-packet path).
            let pkt_type = s.in_buf[pkt_start] >> 4;
            let flags = s.in_buf[pkt_start] & 0x0F;
            let qos = (flags >> 1) & 0x03;
            let (rem_len, vlen) =
                decode_varint(&s.in_buf[pkt_start + 1..pkt_start + pkt_bytes.min(5)]);
            if vlen == 0 {
                s.parse_errors += 1;
                continue;
            }
            let body_start = pkt_start + 1 + vlen;
            if body_start + rem_len as usize > pkt_start + pkt_bytes {
                s.parse_errors += 1;
                continue;
            }
            let body_ptr = s.in_buf.as_ptr().add(body_start);
            let body = core::slice::from_raw_parts(body_ptr, rem_len as usize);

            let ci = s.find_or_create_conn(conn_id);

            // Build session envelope: [proto=MQTT=0][pkt_type][flags][fields][body]
            let mut pos = 0usize;
            s.envelope[pos] = 0;
            pos += 1; // PROTO_MQTT
            s.envelope[pos] = pkt_type;
            pos += 1;
            s.envelope[pos] = flags;
            pos += 1;

            let session_msg_type = match pkt_type {
                PKT_CONNECT => wire::MSG_SESSION_CONNECT,
                PKT_DISCONNECT => wire::MSG_SESSION_DISCONNECT,
                _ => wire::MSG_SESSION_PROPOSAL,
            };

            match pkt_type {
                PKT_CONNECT => {
                    let info = parse_connect(body);
                    s.conns[ci].protocol_version = info.protocol_version;
                    // Cap the outbound alias table at both the CONNECT-
                    // advertised TopicAliasMaximum and the per-conn
                    // table size — we never issue more aliases than
                    // the slot count regardless of what the client
                    // asked for.
                    s.conns[ci].sub_topic_alias_max = if info.protocol_version >= 5 {
                        let cap = MAX_TOPIC_ALIASES_PER_CONN as u16;
                        info.props.topic_alias_maximum.min(cap)
                    } else {
                        0
                    };
                    s.conns[ci].sub_alias_next = 0;
                    // A fresh CONNECT starts from no bindings in either
                    // direction; the tables go back to the pool and are
                    // borrowed again on first use.
                    s.alias_release(ci);

                    // Envelope: [proto_ver][clean_start][keep_alive BE][session_expiry LE]
                    //           [recv_max LE][cid_len BE][cid][un_len BE][un]
                    //           [will_flag][if will: qos,retain,delay LE,topic,payload]
                    if pos + 12 > s.envelope.len() {
                        continue;
                    }
                    s.envelope[pos] = info.protocol_version;
                    pos += 1;
                    s.envelope[pos] = info.clean_start;
                    pos += 1;
                    s.envelope[pos..pos + 2].copy_from_slice(&info.keep_alive.to_be_bytes());
                    pos += 2;
                    s.envelope[pos..pos + 4]
                        .copy_from_slice(&info.props.session_expiry_s.to_le_bytes());
                    pos += 4;
                    s.envelope[pos..pos + 2]
                        .copy_from_slice(&info.props.receive_maximum.to_le_bytes());
                    pos += 2;
                    let cid_len = info.client_id.len().min(256);
                    if pos + 2 + cid_len > s.envelope.len() {
                        continue;
                    }
                    s.envelope[pos..pos + 2].copy_from_slice(&(cid_len as u16).to_be_bytes());
                    pos += 2;
                    s.envelope[pos..pos + cid_len].copy_from_slice(&info.client_id[..cid_len]);
                    pos += cid_len;
                    let un_len = info.username.len().min(256);
                    if pos + 2 + un_len > s.envelope.len() {
                        continue;
                    }
                    s.envelope[pos..pos + 2].copy_from_slice(&(un_len as u16).to_be_bytes());
                    pos += 2;
                    s.envelope[pos..pos + un_len].copy_from_slice(&info.username[..un_len]);
                    pos += un_len;
                    match info.will {
                        Some(w) => {
                            if pos + 7 > s.envelope.len() {
                                continue;
                            }
                            s.envelope[pos] = 1;
                            pos += 1;
                            s.envelope[pos] = w.qos;
                            pos += 1;
                            s.envelope[pos] = w.retain;
                            pos += 1;
                            s.envelope[pos..pos + 4].copy_from_slice(&w.delay_s.to_le_bytes());
                            pos += 4;
                            let wt_len = w.topic.len().min(512);
                            if pos + 2 + wt_len > s.envelope.len() {
                                continue;
                            }
                            s.envelope[pos..pos + 2]
                                .copy_from_slice(&(wt_len as u16).to_be_bytes());
                            pos += 2;
                            s.envelope[pos..pos + wt_len].copy_from_slice(&w.topic[..wt_len]);
                            pos += wt_len;
                            let wp_len = w.payload.len().min(2048);
                            if pos + 2 + wp_len > s.envelope.len() {
                                continue;
                            }
                            s.envelope[pos..pos + 2]
                                .copy_from_slice(&(wp_len as u16).to_be_bytes());
                            pos += 2;
                            s.envelope[pos..pos + wp_len].copy_from_slice(&w.payload[..wp_len]);
                            pos += wp_len;
                        }
                        None => {
                            s.envelope[pos] = 0;
                            pos += 1;
                        }
                    }
                }

                PKT_PUBLISH => {
                    // [topic_str][if qos>0: packet_id BE][if v5: properties][payload]
                    let (topic_field, tn_len) = read_utf8_string(body);
                    if tn_len == 0 {
                        s.parse_errors += 1;
                        continue;
                    }
                    let mut cur = tn_len;
                    let packet_id = if qos > 0 && cur + 2 <= body.len() {
                        let pid = u16::from_be_bytes([body[cur], body[cur + 1]]);
                        cur += 2;
                        pid
                    } else {
                        0
                    };

                    let mut mprops = Mqtt5Props::default();
                    let props_start = cur;
                    if s.conns[ci].protocol_version >= 5 && cur <= body.len() {
                        let (props, plen) = parse_properties(&body[cur..]);
                        mprops = props;
                        cur += plen;
                    }
                    // Capture User Property pairs into a self-delimiting
                    // block that gets appended to the codec envelope.
                    // MQTT 3.1.1 has no properties — `up_buf[0] = 0` and
                    // the apply path sees no user_props (item 6's
                    // wire-format contract).
                    let mut up_buf = [0u8; 1 + MAX_USER_PROPS_COUNT
                        * (2 + MAX_USER_PROP_KEY_LEN + 2 + MAX_USER_PROP_VAL_LEN)];
                    let up_len = if s.conns[ci].protocol_version >= 5 && cur <= body.len() {
                        extract_user_props_block(&body[props_start..cur], &mut up_buf)
                    } else {
                        up_buf[0] = 0;
                        1
                    };

                    // Topic alias resolution (MQTT 5)
                    let topic_slice: &[u8] = if mprops.topic_alias != 0 {
                        if !topic_field.is_empty() {
                            // New binding: cache the mapping. With
                            // every pooled table held the binding
                            // cannot be honoured, and a later
                            // alias-only PUBLISH would resolve to
                            // nothing, so the packet is a protocol
                            // error here (`alias_pool_full`).
                            let Some(table) = s.alias_in_table(ci) else {
                                s.parse_errors += 1;
                                if s.alias_pool_full & 0x1F == 1 {
                                    dev_log(sys, 2, b"[mqtt] alias pool full".as_ptr(), 22);
                                }
                                continue;
                            };
                            let mut placed = false;
                            for a in table.iter_mut() {
                                if a.active == 0 {
                                    a.alias = mprops.topic_alias;
                                    let tl = topic_field.len().min(ALIAS_TOPIC_MAX);
                                    a.topic_len = tl as u16;
                                    a.topic[..tl].copy_from_slice(&topic_field[..tl]);
                                    a.active = 1;
                                    placed = true;
                                    break;
                                } else if a.alias == mprops.topic_alias {
                                    let tl = topic_field.len().min(ALIAS_TOPIC_MAX);
                                    a.topic_len = tl as u16;
                                    a.topic[..tl].copy_from_slice(&topic_field[..tl]);
                                    placed = true;
                                    break;
                                }
                            }
                            let _ = placed;
                            topic_field
                        } else {
                            // Lookup
                            let mut resolved: &[u8] = &[];
                            if let Some(table) = s.alias_in_lookup(ci) {
                                for a in table.iter() {
                                    if a.active == 1 && a.alias == mprops.topic_alias {
                                        let tl = a.topic_len as usize;
                                        let tp = a.topic.as_ptr();
                                        resolved = core::slice::from_raw_parts(tp, tl);
                                        break;
                                    }
                                }
                            }
                            if !resolved.is_empty() {
                                s.topic_alias_hits = s.topic_alias_hits.wrapping_add(1);
                            }
                            resolved
                        }
                    } else {
                        topic_field
                    };

                    // Envelope: [packet_id BE][topic_len BE][topic]
                    //           [user_props_block][payload]
                    // user_props_block is `[count:u8][per prop: key_len BE,
                    // key, val_len BE, val]`; the apply-side parses it
                    // back out before payload starts.
                    if pos + 4 + topic_slice.len() + up_len > s.envelope.len() {
                        continue;
                    }
                    s.envelope[pos..pos + 2].copy_from_slice(&packet_id.to_be_bytes());
                    pos += 2;
                    let tl = topic_slice.len().min(1024);
                    s.envelope[pos..pos + 2].copy_from_slice(&(tl as u16).to_be_bytes());
                    pos += 2;
                    s.envelope[pos..pos + tl].copy_from_slice(&topic_slice[..tl]);
                    pos += tl;
                    s.envelope[pos..pos + up_len].copy_from_slice(&up_buf[..up_len]);
                    pos += up_len;
                    let payload = if cur < body.len() {
                        &body[cur..]
                    } else {
                        &[][..]
                    };
                    let pe = pos + payload.len();
                    if pe > s.envelope.len() {
                        continue;
                    }
                    s.envelope[pos..pe].copy_from_slice(payload);
                    pos = pe;
                    if qos == 2 {
                        s.qos2_packets = s.qos2_packets.wrapping_add(1);
                    }
                    if s.conns[ci].protocol_version >= 5 {
                        s.mqtt5_publishes = s.mqtt5_publishes.wrapping_add(1);
                    }
                }

                PKT_SUBSCRIBE => {
                    if body.len() < 2 {
                        continue;
                    }
                    let packet_id = u16::from_be_bytes([body[0], body[1]]);
                    let mut cur = 2usize;
                    if s.conns[ci].protocol_version >= 5 {
                        let (_, plen) = parse_properties(&body[cur..]);
                        if plen == 0 {
                            s.parse_errors += 1;
                            continue;
                        }
                        cur += plen;
                    }
                    let (topic, tn_len) = read_utf8_string(&body[cur..]);
                    if tn_len == 0 {
                        continue;
                    }
                    cur += tn_len;
                    if cur >= body.len() {
                        continue;
                    }
                    let req_qos = body[cur] & 0x03;
                    if pos + 5 + topic.len() > s.envelope.len() {
                        continue;
                    }
                    s.envelope[pos..pos + 2].copy_from_slice(&packet_id.to_be_bytes());
                    pos += 2;
                    s.envelope[pos] = req_qos;
                    pos += 1;
                    let tl = topic.len().min(1024);
                    s.envelope[pos..pos + 2].copy_from_slice(&(tl as u16).to_be_bytes());
                    pos += 2;
                    s.envelope[pos..pos + tl].copy_from_slice(&topic[..tl]);
                    pos += tl;
                }

                PKT_UNSUBSCRIBE => {
                    if body.len() < 2 {
                        continue;
                    }
                    let packet_id = u16::from_be_bytes([body[0], body[1]]);
                    let mut cur = 2usize;
                    if s.conns[ci].protocol_version >= 5 {
                        let (_, plen) = parse_properties(&body[cur..]);
                        if plen == 0 {
                            s.parse_errors += 1;
                            continue;
                        }
                        cur += plen;
                    }
                    let (topic, tn_len) = read_utf8_string(&body[cur..]);
                    if tn_len == 0 {
                        continue;
                    }
                    if pos + 5 + topic.len() > s.envelope.len() {
                        continue;
                    }
                    s.envelope[pos..pos + 2].copy_from_slice(&packet_id.to_be_bytes());
                    pos += 2;
                    s.envelope[pos] = 0;
                    pos += 1;
                    let tl = topic.len().min(1024);
                    s.envelope[pos..pos + 2].copy_from_slice(&(tl as u16).to_be_bytes());
                    pos += 2;
                    s.envelope[pos..pos + tl].copy_from_slice(&topic[..tl]);
                    pos += tl;
                }

                PKT_PUBACK | PKT_PUBREC | PKT_PUBREL | PKT_PUBCOMP => {
                    if body.len() < 2 {
                        continue;
                    }
                    s.envelope[pos..pos + 2].copy_from_slice(&body[..2]);
                    pos += 2;
                }

                PKT_PINGREQ => {
                    // Synthesize PINGRESP immediately — no session processor involvement
                    let ping = [(PKT_PINGRESP << 4), 0u8];
                    if !write_conn_frame(sys, s.out_frames, conn_id, &ping) {
                        s.frames_refused = s.frames_refused.wrapping_add(1);
                    }
                    s.packets_encoded += 1;
                    continue;
                }

                _ => {
                    // Forward raw body for other packet types
                    let end = pos + body.len();
                    if end > s.envelope.len() {
                        continue;
                    }
                    s.envelope[pos..end].copy_from_slice(body);
                    pos = end;
                }
            }

            if !write_proposal(s, sys, conn_id, session_msg_type, pos) {
                // Refused. A dropped PUBLISH is one the client never
                // hears back about, so the packet goes back to the
                // front of the reassembly buffer — `in_buf` still holds
                // it verbatim — and the connection is marked held. The
                // packet and remainder were in this buffer together
                // before the drain and the page (if any) is still held,
                // so the reserve cannot fail; the guard only keeps a
                // capacity slip from writing past the inline bytes.
                let rem = s.conns[ci].reass_len as usize;
                if !s.reass_reserve(ci, rem + pkt_size) {
                    s.parse_errors += 1;
                    s.reass_reset(ci);
                    return false;
                }
                let (reass, _) = s.reass_ptr(ci);
                core::ptr::copy(reass, reass.add(pkt_size), rem);
                core::ptr::copy_nonoverlapping(s.in_buf.as_ptr().add(CID), reass, pkt_size);
                s.conns[ci].reass_len = (rem + pkt_size) as u16;
                s.held_ci = (ci + 1) as u16;
                s.proposals_held = s.proposals_held.wrapping_add(1);
                if s.proposals_held & 0x1F == 1 {
                    dev_log(sys, 2, b"[mqtt] proposal held".as_ptr(), 20);
                }
                return true;
            }
            s.packets_decoded += 1;
        } // end inner reassembly-drain loop
        s.reass_release_if_empty(ci);
        false
    }
}

/// Encode one session response into MQTT frames on `out_frames`.
/// Payload (post-envelope-strip) is
/// `[conn_id:u16 LE][proto][pkt_type][flags][body]`.
/// Returns `false` when the frame edge refused the encoded packet and
/// the response must be offered again; `true` when it was written or
/// there is nothing to write.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_response(s: &mut Mqtt, sys: &SyscallTable, payload: &[u8]) -> bool {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        // Stage the record exactly as the channel read did.
        let plen = payload.len();
        if plen > s.resp_buf.len() {
            return true;
        }
        s.resp_buf[..plen].copy_from_slice(payload);
        dev_log(sys, 3, b"[mqtt] resp rx".as_ptr(), 14);
        if plen < 5 {
            return true;
        }
        let n = plen;
        let conn_id = u16::from_le_bytes([s.resp_buf[0], s.resp_buf[1]]);
        let pkt_type = s.resp_buf[3];
        let flags = s.resp_buf[4];
        let body = core::slice::from_raw_parts(s.resp_buf.as_ptr().add(5), n - 5);

        // PUBLISH delivery routes through one of two encoders:
        //   * MQTT 5 subscriber → alias-aware encoder that
        //     also emits any user_property pairs the publisher
        //     attached (item 6).
        //   * MQTT 3.1.1 subscriber → bare 3.1.1 encoder; the
        //     inline user_props block (always present in the
        //     body since item 6) is stripped first so the
        //     wire stays 3.1.1-shaped (no properties varint).
        // Other packet types (CONNACK, SUBACK, PUBACK, PUBREC,
        // PUBREL, PUBCOMP, UNSUBACK) keep the bare 3.1.1
        // encoder unchanged.
        let ci_resp = s.find_or_create_conn(conn_id);
        let proto_v = s.conns[ci_resp].protocol_version;

        // Translate the placement refusals into this connection's
        // version. `session_processor` knows the session is served
        // elsewhere; only the codec knows how to say so, because the
        // same condition is `0x9D`/`0x9C` in v5 and `0x03` in 3.1.1 —
        // and `0x03` in v5 means "Malformed Packet", so passing one
        // literal through would misinform half the clients.
        // A server-initiated DISCONNECT exists ONLY in v5. It is how a
        // node tells a client its session has moved: the client
        // reconnects, meets the CONNECT redirect, and lands on the new
        // owner with `session_present = 1` — session state is
        // REPLICATED, so nothing is lost by reconnecting. 3.1.1 has no
        // such packet; its remedy is closing the socket, which needs a
        // primitive that does not exist yet, so the frame is DROPPED
        // rather than encoded as bytes the client would read as a
        // malformed packet.
        if pkt_type == PKT_DISCONNECT {
            if body.is_empty() {
                return true;
            }
            if proto_v < 5 {
                // 3.1.1 has no server DISCONNECT, so the only way to
                // tell this client its session moved is to close the
                // socket — it then reconnects and meets the CONNECT
                // redirect. Encoding a v5 DISCONNECT here instead would
                // put bytes on the wire a 3.1.1 client reads as a
                // malformed packet.
                if !write_conn_frame_with_mtype(
                    sys,
                    s.out_frames,
                    conn_id,
                    wire::MSG_CONN_CLOSE_REQUEST,
                    &[],
                ) {
                    s.frames_refused = s.frames_refused.wrapping_add(1);
                }
                return true;
            }
            let reason = match body[0] {
                wire::MQTT_REASON_MOVED => 0x9Du8,         // Server moved
                wire::MQTT_REASON_TRY_ELSEWHERE => 0x9Cu8, // Use another server
                other => other,
            };
            let n = encode_mqtt_frame(&mut s.frame, PKT_DISCONNECT, 0, &[reason]);
            if n > 0 && !write_conn_frame(sys, s.out_frames, conn_id, &s.frame[..n]) {
                s.frames_refused = s.frames_refused.wrapping_add(1);
            }
            return true;
        }

        let mut retarget = [0u8; 2];
        let body = if pkt_type == PKT_CONNACK
            && body.len() >= 2
            && matches!(
                body[1],
                wire::MQTT_REASON_MOVED | wire::MQTT_REASON_TRY_ELSEWHERE
            ) {
            retarget[0] = body[0];
            retarget[1] = if proto_v >= 5 {
                if body[1] == wire::MQTT_REASON_MOVED {
                    0x9D // Server moved
                } else {
                    0x9C // Use another server
                }
            } else {
                0x03 // Server unavailable — 3.1.1's only refusal
            };
            &retarget[..]
        } else {
            body
        };
        let needs_v5_props_splice =
            proto_v >= 5 && matches!(pkt_type, PKT_CONNACK | PKT_SUBACK | PKT_UNSUBACK,);
        let frame_len = if pkt_type == PKT_PUBLISH && proto_v >= 5 {
            let alias_max = s.conns[ci_resp].sub_topic_alias_max;
            // With no table (none needed, or the pool exhausted) the
            // encoder sees an empty table and sends the full topic —
            // spec-legal, the subscriber just learns no alias.
            let table: &mut [TopicAlias] = match s.alias_out_index(ci_resp) {
                Some(t) => &mut s.alias_pool[t],
                None => &mut [],
            };
            encode_mqtt5_publish_with_alias(
                &mut s.frame,
                flags,
                body,
                table,
                alias_max,
                &mut s.conns[ci_resp].sub_alias_next,
            )
        } else if pkt_type == PKT_PUBLISH {
            let mut stripped = [0u8; RESP_BUF];
            let n = strip_user_props_for_v311(&mut stripped, flags, body);
            if n == 0 {
                0
            } else {
                encode_mqtt_frame(&mut s.frame, pkt_type, flags, &stripped[..n])
            }
        } else if needs_v5_props_splice {
            let mut spliced = [0u8; RESP_BUF];
            let n = splice_mqtt5_ack(&mut spliced, pkt_type, body);
            if n == 0 {
                0
            } else {
                encode_mqtt_frame(&mut s.frame, pkt_type, flags, &spliced[..n])
            }
        } else {
            encode_mqtt_frame(&mut s.frame, pkt_type, flags, body)
        };
        if frame_len == 0 {
            return true;
        }

        // A refused frame is NOT consumed: the caller keeps the response
        // and offers it again next step, so a PUBACK the client is
        // waiting on is never lost to a full edge.
        if !write_conn_frame(sys, s.out_frames, conn_id, &s.frame[..frame_len]) {
            return false;
        }
        dev_log(sys, 3, b"[mqtt] resp -> peer".as_ptr(), 19);
        s.packets_encoded += 1;
        true
    }
}

/// Hand the proposal envelope staged in `s.envelope[..len]` to the
/// anchor, which forwards it to the worker the connection is bound to
/// or holds it across a handoff. `false` only when the live worker's
/// channel refused it.
///
/// # Safety
unsafe fn write_proposal(
    s: &mut Mqtt,
    sys: &SyscallTable,
    conn_id: wire::ConnId,
    msg_type: u8,
    len: usize,
) -> bool {
    const PAYLOAD_BUF: usize = CID + MAX_PACKET;
    let total = CID + len;
    if total > PAYLOAD_BUF || s.anchor.is_null() {
        return false;
    }
    let mut out = [0u8; PAYLOAD_BUF];
    out[..CID].copy_from_slice(&conn_id.to_le_bytes());
    out[CID..total].copy_from_slice(&s.envelope[..len]);
    super::anchor::forward_env(&mut *s.anchor, sys, msg_type, &out[..total])
}

/// Write a framed proposal envelope to session_processor.codec_in.
///
/// Format (wire envelope: `[msg_type:u8][len:u16 LE][payload]`):
///   msg_type = `session_msg_type` (e.g. MSG_SESSION_PROPOSAL)
///   payload  = [conn_id:u16 LE][envelope bytes from the per-packet builder]
///
/// Envelope framing matters because codec_in is a fan-in port and the
/// underlying channel is a byte FIFO — without per-message length, two
/// back-to-back writes (e.g. three PUBLISH packets coalesced in one TCP
/// recv) coalesce into one read on the consumer side and the trailing
/// messages are silently mis-parsed as the body of the first.
///
/// # Safety
unsafe fn write_conn_frame_with_mtype(
    sys: &SyscallTable,
    chan: i32,
    conn_id: wire::ConnId,
    msg_type: u8,
    bytes: &[u8],
) -> bool {
    if chan < 0 {
        return false;
    }
    const PAYLOAD_BUF: usize = CID + MAX_PACKET;
    let total = CID + bytes.len();
    if total > PAYLOAD_BUF {
        return false;
    }
    let mut out = [0u8; PAYLOAD_BUF];
    out[..CID].copy_from_slice(&conn_id.to_le_bytes());
    out[CID..total].copy_from_slice(bytes);
    let w = wire::channel_write_msg(sys, chan, msg_type, &out[..total]);
    w > 0
}

/// Counters for the module's per-second accounting line: packets
/// decoded and written to the session, packets held on a refused
/// proposal edge, frames encoded to clients, frames refused where
/// nothing retries them.
pub fn hb_counters(s: &Mqtt) -> (u32, u32, u32, u32) {
    (
        s.packets_decoded,
        s.proposals_held,
        s.packets_encoded,
        s.frames_refused,
    )
}

/// Fill the component's metric payload. Returns the byte count.
pub fn metrics(s: &Mqtt, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&s.packets_decoded.to_le_bytes());
    m[4..8].copy_from_slice(&s.packets_encoded.to_le_bytes());
    m[8..12].copy_from_slice(&s.parse_errors.to_le_bytes());
    m[12..16].copy_from_slice(&s.qos2_packets.to_le_bytes());
    m[16..20].copy_from_slice(&s.mqtt5_publishes.to_le_bytes());
    m[20..24].copy_from_slice(&s.topic_alias_hits.to_le_bytes());
    24
}
