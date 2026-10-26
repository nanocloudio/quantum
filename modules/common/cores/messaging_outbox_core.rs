// Bounded, no_std, no-alloc MESSAGING OUTBOX core — the worker's ordered
// queue toward `messaging`. `include!`d by `session_processor` and by the
// host test crate; it names nothing outside this file.
//
// Everything the worker asks of `messaging` is asked once — a dedup check
// a committed publish's fan-out waits on, a retained write, an offline
// enqueue, a drain or a read — so a request the op edge refuses must not
// be dropped. It waits here, in order behind any already waiting, and is
// written when the edge has room. The queue is bounded; the worker reads
// an input that may file requests only while the room left covers the
// most that input's next record can file (`free`), so a push never meets
// a full queue.

/// Bytes the outbox holds, frames and their headers.
pub const OUTBOX_BYTES: usize = 256 * 1024;
/// A frame's header here: `[msg_type:u8][len:u16 LE]`.
pub const FRAME_HDR: usize = 3;
/// The largest payload one frame carries.
pub const FRAME_MAX: usize = 8192;

/// The ordered queue.
pub struct Outbox {
    buf: [u8; OUTBOX_BYTES],
    head: usize,
    used: usize,
    /// Frames that waited for the edge (each counted once).
    pub waited: u32,
    /// Frames refused for want of room — zero while every input is read
    /// only with room for what it files.
    pub refused: u32,
    /// The most bytes ever waiting.
    pub peak: usize,
}

impl Outbox {
    pub const fn new() -> Self {
        Self {
            buf: [0; OUTBOX_BYTES],
            head: 0,
            used: 0,
            waited: 0,
            refused: 0,
            peak: 0,
        }
    }

    /// Empty in place — a module's state is too large to build on the
    /// stack and move.
    pub fn init(&mut self) {
        self.head = 0;
        self.used = 0;
        self.waited = 0;
        self.refused = 0;
        self.peak = 0;
    }

    /// Bytes waiting.
    pub fn used(&self) -> usize {
        self.used
    }

    /// Bytes free for frames.
    pub fn free(&self) -> usize {
        OUTBOX_BYTES - self.used
    }

    pub fn is_empty(&self) -> bool {
        self.used == 0
    }

    /// Bytes a frame of `len` payload bytes takes here.
    pub const fn cost(len: usize) -> usize {
        FRAME_HDR + len
    }

    /// Write the waiting frames, oldest first, for as long as `write`
    /// takes them. Returns how many went.
    pub fn flush<F: FnMut(u8, &[u8]) -> bool>(&mut self, mut write: F) -> usize {
        let mut scratch = [0u8; FRAME_MAX];
        let mut n = 0;
        while self.used > 0 {
            let mt = self.byte(0);
            let len = usize::from(u16::from_le_bytes([self.byte(1), self.byte(2)]));
            for (k, b) in scratch[..len].iter_mut().enumerate() {
                *b = self.byte(FRAME_HDR + k);
            }
            if !write(mt, &scratch[..len]) {
                break;
            }
            let total = FRAME_HDR + len;
            self.head = (self.head + total) % OUTBOX_BYTES;
            self.used -= total;
            n += 1;
        }
        n
    }

    /// Send `(mt, payload)`: written at once when nothing waits ahead of
    /// it and `write` takes it, else queued behind what waits. `false`
    /// only when it is larger than a frame or the queue has no room for
    /// it (counted in `refused`).
    pub fn send<F: FnMut(u8, &[u8]) -> bool>(
        &mut self,
        mut write: F,
        mt: u8,
        payload: &[u8],
    ) -> bool {
        if payload.len() > FRAME_MAX {
            self.refused = self.refused.wrapping_add(1);
            return false;
        }
        if self.used > 0 {
            self.flush(&mut write);
        }
        if self.used == 0 && write(mt, payload) {
            return true;
        }
        let total = FRAME_HDR + payload.len();
        if total > self.free() {
            self.refused = self.refused.wrapping_add(1);
            return false;
        }
        let len = (payload.len() as u16).to_le_bytes();
        let at = self.used;
        self.put(at, mt);
        self.put(at + 1, len[0]);
        self.put(at + 2, len[1]);
        for (k, &b) in payload.iter().enumerate() {
            self.put(at + FRAME_HDR + k, b);
        }
        self.used += total;
        self.waited = self.waited.wrapping_add(1);
        if self.used > self.peak {
            self.peak = self.used;
        }
        true
    }

    fn byte(&self, off: usize) -> u8 {
        self.buf[(self.head + off) % OUTBOX_BYTES]
    }

    fn put(&mut self, off: usize, b: u8) {
        self.buf[(self.head + off) % OUTBOX_BYTES] = b;
    }
}

impl Default for Outbox {
    fn default() -> Self {
        Self::new()
    }
}
