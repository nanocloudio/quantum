// Bounded, no_std, no-alloc OFFLINE QUEUE core — the deliveries a session
// is owed while it cannot take them, held per session in arrival order.
// `include!`d by the `messaging` module and by the host test crate; it
// names nothing outside this file.
//
// An envelope is stored whole, whatever its size up to
// [`OFFLINE_ENVELOPE_MAX`], in fixed chunks of a shared arena, so a large
// delivery costs its own size rather than every entry costing the
// largest. Each session's envelopes form a doubly linked list oldest
// first, so draining, counting and releasing a session touch only its
// own entries, and an expired entry unlinks from anywhere in its list.
//
// Bounds are refusal, counted by the caller: an envelope too large, a
// session slot past [`OFFLINE_SESSIONS`], or no entry or chunks left.

/// Session slots a queue is kept for.
pub const OFFLINE_SESSIONS: usize = 16384;
/// Envelopes held at once, across every session.
pub const OFFLINE_ENTRIES: usize = 4096;
/// Bytes of one arena chunk.
pub const OFFLINE_CHUNK: usize = 256;
/// Arena chunks: 4 MiB.
pub const OFFLINE_CHUNKS: usize = 16384;
/// The largest envelope stored.
pub const OFFLINE_ENVELOPE_MAX: usize = 4112;

/// An entry or chunk index plus one; 0 ends a list.
type QLink = u16;

const _: () = assert!(OFFLINE_ENTRIES < QLink::MAX as usize);
const _: () = assert!(OFFLINE_CHUNKS < QLink::MAX as usize);
const _: () = assert!(OFFLINE_ENVELOPE_MAX <= u16::MAX as usize);

#[derive(Clone, Copy)]
struct Entry {
    session: u32,
    sequence: u64,
    expiry_ms: u64,
    len: u16,
    first_chunk: QLink,
    prev: QLink,
    next: QLink,
    active: bool,
}

impl Entry {
    const fn zero() -> Self {
        Self {
            session: 0,
            sequence: 0,
            expiry_ms: 0,
            len: 0,
            first_chunk: 0,
            prev: 0,
            next: 0,
            active: false,
        }
    }
}

/// Why an envelope was not queued.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Refused {
    /// Larger than [`OFFLINE_ENVELOPE_MAX`] or empty.
    Size,
    /// A slot past [`OFFLINE_SESSIONS`].
    Slot,
    /// No entry or not enough chunks left.
    Full,
}

/// The queue.
pub struct OfflineQueue {
    entries: [Entry; OFFLINE_ENTRIES],
    free_entries: [QLink; OFFLINE_ENTRIES],
    free_entries_len: usize,
    arena: [[u8; OFFLINE_CHUNK]; OFFLINE_CHUNKS],
    chunk_next: [QLink; OFFLINE_CHUNKS],
    free_chunk: QLink,
    free_chunks: usize,
    head: [QLink; OFFLINE_SESSIONS],
    tail: [QLink; OFFLINE_SESSIONS],
    count: [u16; OFFLINE_SESSIONS],
    next_sequence: u64,
    live: usize,
}

const fn chunks_for(len: usize) -> usize {
    len.div_ceil(OFFLINE_CHUNK)
}

impl OfflineQueue {
    /// An empty queue. A module empties its own in place with
    /// [`Self::init`] instead: the queue is too large for the stack.
    pub const fn new() -> Self {
        Self {
            entries: [Entry::zero(); OFFLINE_ENTRIES],
            free_entries: [0; OFFLINE_ENTRIES],
            free_entries_len: 0,
            arena: [[0; OFFLINE_CHUNK]; OFFLINE_CHUNKS],
            chunk_next: [0; OFFLINE_CHUNKS],
            free_chunk: 0,
            free_chunks: 0,
            head: [0; OFFLINE_SESSIONS],
            tail: [0; OFFLINE_SESSIONS],
            count: [0; OFFLINE_SESSIONS],
            next_sequence: 1,
            live: 0,
        }
    }

    /// Empty in place — the queue is too large to build on the stack and
    /// move.
    pub fn init(&mut self) {
        for e in self.entries.iter_mut() {
            *e = Entry::zero();
        }
        // Handed out lowest first.
        for (k, f) in self.free_entries.iter_mut().enumerate() {
            *f = (OFFLINE_ENTRIES - k) as QLink;
        }
        self.free_entries_len = OFFLINE_ENTRIES;
        for (c, n) in self.chunk_next.iter_mut().enumerate() {
            *n = if c + 1 < OFFLINE_CHUNKS {
                (c + 2) as QLink
            } else {
                0
            };
        }
        self.free_chunk = 1;
        self.free_chunks = OFFLINE_CHUNKS;
        self.head = [0; OFFLINE_SESSIONS];
        self.tail = [0; OFFLINE_SESSIONS];
        self.count = [0; OFFLINE_SESSIONS];
        self.next_sequence = 1;
        self.live = 0;
    }

    /// Envelopes queued, across every session.
    pub fn live(&self) -> usize {
        self.live
    }

    /// Envelopes queued for `session`.
    pub fn queued(&self, session: u32) -> u32 {
        let s = session as usize;
        if s < OFFLINE_SESSIONS {
            u32::from(self.count[s])
        } else {
            0
        }
    }

    /// Queue `env` behind `session`'s others, to expire at `expiry_ms`.
    pub fn push(&mut self, session: u32, env: &[u8], expiry_ms: u64) -> Result<(), Refused> {
        let s = session as usize;
        if env.is_empty() || env.len() > OFFLINE_ENVELOPE_MAX {
            return Err(Refused::Size);
        }
        if s >= OFFLINE_SESSIONS {
            return Err(Refused::Slot);
        }
        let need = chunks_for(env.len());
        if self.free_entries_len == 0 || self.free_chunks < need || self.count[s] == u16::MAX {
            return Err(Refused::Full);
        }
        self.free_entries_len -= 1;
        let link = self.free_entries[self.free_entries_len];
        // Take `need` chunks off the free list and fill them in order.
        let first = self.free_chunk;
        let mut c = first;
        for (k, part) in env.chunks(OFFLINE_CHUNK).enumerate() {
            let i = usize::from(c) - 1;
            self.arena[i][..part.len()].copy_from_slice(part);
            if k + 1 == need {
                self.free_chunk = self.chunk_next[i];
                self.chunk_next[i] = 0;
            } else {
                c = self.chunk_next[i];
            }
        }
        self.free_chunks -= need;
        let e = &mut self.entries[usize::from(link) - 1];
        *e = Entry {
            session,
            sequence: self.next_sequence,
            expiry_ms,
            len: env.len() as u16,
            first_chunk: first,
            prev: self.tail[s],
            next: 0,
            active: true,
        };
        self.next_sequence = self.next_sequence.wrapping_add(1);
        match self.tail[s] {
            0 => self.head[s] = link,
            t => self.entries[usize::from(t) - 1].next = link,
        }
        self.tail[s] = link;
        self.count[s] += 1;
        self.live += 1;
        Ok(())
    }

    /// Copy `session`'s oldest envelope into `out`; its length, or `None`
    /// with none queued or `out` too small.
    pub fn front(&self, session: u32, out: &mut [u8]) -> Option<usize> {
        let s = session as usize;
        if s >= OFFLINE_SESSIONS || self.head[s] == 0 {
            return None;
        }
        let e = &self.entries[usize::from(self.head[s]) - 1];
        let len = usize::from(e.len);
        if out.len() < len {
            return None;
        }
        let mut c = e.first_chunk;
        for part in out[..len].chunks_mut(OFFLINE_CHUNK) {
            let i = usize::from(c) - 1;
            part.copy_from_slice(&self.arena[i][..part.len()]);
            c = self.chunk_next[i];
        }
        Some(len)
    }

    /// Drop `session`'s oldest envelope; `false` with none queued.
    pub fn pop_front(&mut self, session: u32) -> bool {
        let s = session as usize;
        if s >= OFFLINE_SESSIONS || self.head[s] == 0 {
            return false;
        }
        self.remove(self.head[s]);
        true
    }

    /// Drop every envelope queued for `session`; how many went.
    pub fn release(&mut self, session: u32) -> u32 {
        let mut n = 0;
        while self.pop_front(session) {
            n += 1;
        }
        n
    }

    /// Drop every envelope expired by `now`; how many went.
    pub fn expire(&mut self, now: u64) -> u32 {
        let mut n = 0;
        for k in 0..OFFLINE_ENTRIES {
            let e = &self.entries[k];
            if e.active && e.expiry_ms <= now {
                self.remove((k + 1) as QLink);
                n += 1;
            }
        }
        n
    }

    /// The lowest sequence still queued: the retention floor.
    pub fn earliest(&self) -> Option<u64> {
        self.entries
            .iter()
            .filter(|e| e.active)
            .map(|e| e.sequence)
            .min()
    }

    /// Drop everything.
    pub fn clear(&mut self) {
        self.init();
    }

    fn remove(&mut self, link: QLink) {
        let i = usize::from(link) - 1;
        let e = self.entries[i];
        let s = e.session as usize;
        match e.prev {
            0 => self.head[s] = e.next,
            p => self.entries[usize::from(p) - 1].next = e.next,
        }
        match e.next {
            0 => self.tail[s] = e.prev,
            n => self.entries[usize::from(n) - 1].prev = e.prev,
        }
        self.count[s] -= 1;
        // Its chunks go back on the free list, as one run.
        let need = chunks_for(usize::from(e.len));
        let mut last = e.first_chunk;
        for _ in 1..need {
            last = self.chunk_next[usize::from(last) - 1];
        }
        self.chunk_next[usize::from(last) - 1] = self.free_chunk;
        self.free_chunk = e.first_chunk;
        self.free_chunks += need;
        self.entries[i] = Entry::zero();
        self.free_entries[self.free_entries_len] = link;
        self.free_entries_len += 1;
        self.live -= 1;
    }
}
