//! Dial authority for Quantum's outbound connectors.
//!
//! A connector names its peer once, as `host[:port]`, and dials it with
//! a `CMD_CONNECT_TO` record: a literal travels as its address, a name
//! travels as a name for the network provider to resolve. The text is
//! parsed when the module is constructed, so a bad authority refuses
//! construction with a reason, and every dial composes its record from
//! the kept bytes.
//!
//! `#[path]`-mounted by each connector, like `wire.rs` and `types.rs`.

#![allow(
    dead_code,
    reason = "shared dial helper; each connector uses one protocol port and a subset of the surface"
)]

// The single mount of the SDK contract for this file. A connector that
// needs the contract's opcodes reaches them through its own `abi`
// mount; nothing of this module's `net_proto` crosses the boundary.
#[path = "../../target/fluxor/fluxor-abi/sdk/contracts/net/net_proto.rs"]
pub mod net_proto;

use net_proto::{write_connect_to, Target, SOCK_TYPE_STREAM};

/// Longest authority a connector keeps. A 63-byte label with a port, or
/// a bracketed IPv6 literal with a port, fits; a longer name is refused
/// at construction rather than truncated into a different host.
pub const AUTHORITY_MAX: usize = 64;

/// Protocol default ports, applied when an authority names none.
pub const PORT_MQTT: u16 = 1883;
pub const PORT_KAFKA: u16 = 9092;
pub const PORT_AMQP: u16 = 5672;
pub const PORT_NATS: u16 = 4222;
/// The port Quantum's own broker advertises (`protocol.advertised_port`),
/// which is what the sinks adopt when their authority names none.
pub const PORT_QUANTUM_BROKER: u16 = 9090;

/// One `host[:port]` as configured, plus the port it resolves to.
#[repr(C)]
pub struct Authority {
    text: [u8; AUTHORITY_MAX],
    len: u8,
    /// The text offered was longer than `AUTHORITY_MAX`; `adopt` refuses.
    overflow: u8,
    /// The authority's port or the protocol default; 0 until `adopt`.
    port: u16,
}

impl Authority {
    pub const fn empty() -> Self {
        Authority {
            text: [0; AUTHORITY_MAX],
            len: 0,
            overflow: 0,
            port: 0,
        }
    }

    pub fn clear(&mut self) {
        *self = Authority::empty();
    }

    /// Keep `text` as written. Longer than `AUTHORITY_MAX` is recorded
    /// as an overflow so `adopt` can refuse it by name.
    pub fn set(&mut self, text: &[u8]) {
        self.clear();
        if text.len() > AUTHORITY_MAX {
            self.overflow = 1;
            return;
        }
        self.text[..text.len()].copy_from_slice(text);
        self.len = text.len() as u8;
    }

    /// Parse the kept text as `host[:port]`; `default_port` applies when
    /// it names none. `false` when nothing was set, the text overflowed,
    /// or it is not an authority — the caller refuses to construct.
    pub fn adopt(&mut self, default_port: u16) -> bool {
        if self.overflow != 0 || self.len == 0 {
            return false;
        }
        let Some((_, port)) = Target::parse(self.text()) else {
            return false;
        };
        self.port = port.unwrap_or(default_port);
        true
    }

    /// Some text was offered, whether or not it fit: an optional
    /// authority that was given must still parse.
    pub fn offered(&self) -> bool {
        self.len > 0 || self.overflow != 0
    }

    /// Text was set and `adopt` accepted it.
    pub fn is_set(&self) -> bool {
        self.len > 0 && self.port != 0
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn text(&self) -> &[u8] {
        &self.text[..self.len as usize]
    }

    /// The host alone: the authority without its port, and without the
    /// brackets a v6 literal is written in. What a module advertises to a
    /// client that will resolve the name itself.
    pub fn host(&self) -> &[u8] {
        let text = self.text();
        if let Some(close) = text.iter().position(|&c| c == b']') {
            return &text[1..close.min(text.len())];
        }
        match text.iter().rposition(|&c| c == b':') {
            Some(at) => &text[..at],
            None => text,
        }
    }

    /// Compose the `CMD_CONNECT_TO` payload for this authority into
    /// `buf`, tagged with `tag` when given. `0` when the authority was
    /// never adopted or `buf` cannot hold the record.
    pub fn connect_record(&self, buf: &mut [u8], tag: Option<u8>) -> usize {
        if !self.is_set() {
            return 0;
        }
        let Some((target, _)) = Target::parse(self.text()) else {
            return 0;
        };
        write_connect_to(buf, SOCK_TYPE_STREAM, self.port, &target, tag)
    }
}
