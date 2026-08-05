//! seam — in-module message seams for the governance composite.
//!
//! [`SeamRing`] stands in for an internal channel edge: a producer-owned
//! wrapping byte ring carrying the channel layer's `[type:u8][len:u16 LE]`
//! record framing with the same all-or-nothing write contract (a record
//! that does not fit the free space is dropped whole — atomic single
//! frames, never a torn two-part write). The dispatch table drains it
//! each step at the consuming component's original per-step bound.
//!
//! `tenants` and `dr` produce audit events and metric samples this way;
//! `audit` and `telemetry` consume them through the same
//! message-shaped entry points the ports fed. Replacing a
//! `ring.push(MSG_AUDIT_EVENT, &ev)` with a `channel_write_msg` to an
//! `out_audit` port lifts either component back out unchanged.

/// Record header: `[msg_type:u8][len:u16 LE]`.
const HDR: usize = 3;

/// Producer-owned wrapping byte ring with channel-record framing.
#[repr(C)]
pub struct SeamRing<const N: usize> {
    buf: [u8; N],
    head: u16,
    used: u16,
}

impl<const N: usize> SeamRing<N> {
    /// Reset to empty. Ring bytes are dead while `used == 0`, so the
    /// buffer content is left as-is (module state arrives zeroed from the
    /// kernel).
    pub fn reset(&mut self) {
        self.head = 0;
        self.used = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.used == 0
    }

    /// All-or-nothing enqueue of one framed record. Returns `false` —
    /// dropping the record whole — when `3 + payload.len()` exceeds the
    /// free space, mirroring an atomic channel write against a full ring.
    pub fn push(&mut self, msg_type: u8, payload: &[u8]) -> bool {
        let total = HDR + payload.len();
        if total > N - self.used as usize {
            return false;
        }
        let len = payload.len() as u16;
        let hdr = [msg_type, (len & 0xFF) as u8, (len >> 8) as u8];
        let mut w = (self.head as usize + self.used as usize) % N;
        for &b in hdr.iter().chain(payload.iter()) {
            self.buf[w] = b;
            w = (w + 1) % N;
        }
        self.used += total as u16;
        true
    }

    /// Dequeue one record into `dst`. Returns `(msg_type, len)` where
    /// `len` is the record's stored payload length; bytes beyond
    /// `dst.len()` are consumed but not copied (the channel reader's
    /// truncating contract).
    pub fn pop(&mut self, dst: &mut [u8]) -> Option<(u8, u16)> {
        if (self.used as usize) < HDR {
            return None;
        }
        let mut r = self.head as usize;
        let msg_type = self.buf[r];
        r = (r + 1) % N;
        let lo = self.buf[r];
        r = (r + 1) % N;
        let hi = self.buf[r];
        r = (r + 1) % N;
        let len = u16::from_le_bytes([lo, hi]) as usize;
        if (self.used as usize) < HDR + len {
            return None;
        }
        for i in 0..len {
            let b = self.buf[r];
            if i < dst.len() {
                dst[i] = b;
            }
            r = (r + 1) % N;
        }
        self.head = r as u16;
        self.used -= (HDR + len) as u16;
        Some((msg_type, len as u16))
    }
}
