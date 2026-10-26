// Bounded, no_std, no-alloc PRODUCTION SEAT core — the worker's side of
// anchor continuity (docs/architecture/anchor_continuity.md), as
// `session_processor` holds it. `include!`d AFTER `mqtt_anchor_core.rs`
// and `anchor_continuity_core.rs`, in the same module: it names
// `ProductionRing`, `encode_production`, `HorizonReport`,
// `horizon_admitted`, `Reanchor`, `Reanchored`, `WorkerSeat`,
// `reanchor_verdict`, `AC_OK` and `SESSION_ID_LEN` by bare name.
//
// A session whose anchor attached it as transport-migratable
// (SessionCtrlV1 class `CC_TRANSPORT_MIGRATABLE`) takes a SEAT: from then
// on every response the worker emits for it is a PRODUCTION — numbered,
// wrapped in the production envelope and kept in the seat's ring until
// the anchor reports it inside the transport's output horizon — and a
// replacement anchor's MSG_ANCHOR_REANCHOR is answered from the seat.
// A session without a seat is answered unnumbered, as a plain response.
//
// Bounds are backpressure, never eviction: a full ring refuses the next
// production (the worker holds the response back and offers it again), a
// channel that refuses a wrapped production gives its id back
// (`unproduce`) so the anchor never sees a gap, and a full seat table
// refuses the attachment (the anchor closes the connection rather than
// run it without continuity it asked for).

/// Sessions under continuity one worker seats at once.
pub const SEAT_COUNT: usize = 16;
/// Productions one seat retains until the horizon covers them.
pub const SEAT_RING: usize = 16;

/// One seated session.
pub struct ProductionSeat {
    pub used: bool,
    pub conn: u16,
    pub session_id: [u8; SESSION_ID_LEN],
    pub epoch: u32,
    pub ring: ProductionRing<SEAT_RING>,
    /// The connection's output bytes through the last released
    /// production: where a replacement anchor places what it re-offers.
    pub released_bytes: u64,
    /// After a re-anchor, the next retained production still to be
    /// offered to the replacement again (0: none owed). New productions
    /// wait until every re-offer has gone, so the replacement's gate sees
    /// the ids in order.
    pub reoffer_next: u64,
    /// A PINGRESP the ring had no room for: answered on a later pass, so
    /// a client that only pings is never left unanswered.
    pub pingresp_owed: bool,
    /// The anchor the seat was last re-anchored to: a repeated request
    /// from it at the epoch it already holds is answered again.
    pub reanchored_by: [u8; 8],
}

impl ProductionSeat {
    pub const fn empty() -> Self {
        Self {
            used: false,
            conn: 0,
            session_id: [0; SESSION_ID_LEN],
            epoch: 0,
            ring: ProductionRing::new(),
            released_bytes: 0,
            reoffer_next: 0,
            pingresp_owed: false,
            reanchored_by: [0; 8],
        }
    }
}

/// What [`ProductionSeats::produce`] did with a response.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Produced {
    /// The connection has no seat: emit the response as it is.
    Unseated,
    /// Wrapped as production `id`, `len` bytes of envelope in `out`.
    Wrapped { id: u64, len: usize },
    /// The ring is full, or re-offers after a re-anchor are still owed:
    /// hold the response back and offer it again.
    Full,
    /// `inner` carries no connection id or `out` cannot hold the
    /// envelope: nothing was numbered.
    Malformed,
}

/// The worker's seats.
pub struct ProductionSeats {
    pub seats: [ProductionSeat; SEAT_COUNT],
    /// Attachments refused for want of a seat.
    pub refused: u32,
    /// Productions held back on a full ring.
    pub full: u32,
    /// Productions numbered without their bytes (larger than a slot).
    pub unretained: u32,
    /// Productions released by horizon reports.
    pub released: u32,
    /// Horizon reports ignored (another session, epoch or a stale
    /// anchor's).
    pub stale_reports: u32,
    /// Re-anchor requests answered, and how many were refused.
    pub reanchors: u32,
    pub reanchor_refused: u32,
}

impl ProductionSeats {
    pub const fn new() -> Self {
        const E: ProductionSeat = ProductionSeat::empty();
        Self {
            seats: [E; SEAT_COUNT],
            refused: 0,
            full: 0,
            unretained: 0,
            released: 0,
            stale_reports: 0,
            reanchors: 0,
            reanchor_refused: 0,
        }
    }

    /// The seat of connection `conn`.
    pub fn seat_of(&self, conn: u16) -> Option<usize> {
        self.seats.iter().position(|s| s.used && s.conn == conn)
    }

    /// The seat of session `sid`.
    pub fn seat_of_session(&self, sid: &[u8; SESSION_ID_LEN]) -> Option<usize> {
        self.seats
            .iter()
            .position(|s| s.used && &s.session_id == sid)
    }

    /// Seat `sid` on `conn` at `epoch` (its attachment). An attachment is
    /// a new connection's, and the anchor's gate for it starts at
    /// production 1, so the seat does too: whatever a seat on `conn` or of
    /// `sid` held before belongs to an attachment that ended. `false` when
    /// no seat is free.
    pub fn open(&mut self, conn: u16, sid: &[u8; SESSION_ID_LEN], epoch: u32) -> bool {
        for s in self.seats.iter_mut() {
            if s.used && (s.conn == conn || &s.session_id == sid) {
                *s = ProductionSeat::empty();
            }
        }
        let Some(i) = self.seats.iter().position(|s| !s.used) else {
            self.refused = self.refused.wrapping_add(1);
            return false;
        };
        let s = &mut self.seats[i];
        *s = ProductionSeat::empty();
        s.used = true;
        s.conn = conn;
        s.session_id = *sid;
        s.epoch = epoch;
        true
    }

    /// Seat `sid` on `conn` at `epoch` where another worker left off: the
    /// session moved here (a planned handoff or an adoption) and its
    /// anchor's gate expects production `next_production` next, placed at
    /// byte `bytes_written` of the connection's output. Nothing below it
    /// is this worker's to re-offer. `false` when no seat is free.
    pub fn open_resumed(
        &mut self,
        conn: u16,
        sid: &[u8; SESSION_ID_LEN],
        epoch: u32,
        next_production: u64,
        bytes_written: u64,
    ) -> bool {
        if !self.open(conn, sid, epoch) {
            return false;
        }
        let Some(i) = self.seat_of(conn) else {
            return false;
        };
        let s = &mut self.seats[i];
        s.ring = ProductionRing::resumed(next_production);
        s.released_bytes = bytes_written;
        true
    }

    /// Whether a seat can be opened for `sid` on `conn` now.
    pub fn can_open(&self, conn: u16, sid: &[u8; SESSION_ID_LEN]) -> bool {
        self.seats
            .iter()
            .any(|s| !s.used || s.conn == conn || &s.session_id == sid)
    }

    /// Whether `conn` has nothing of its productions in flight: every one
    /// is inside the horizon, no re-offer and no PINGRESP owed. A seated
    /// session is at a point it can move from only then. An unseated one
    /// always is.
    pub fn settled(&self, conn: u16) -> bool {
        match self.seat_of(conn) {
            Some(i) => {
                let s = &self.seats[i];
                s.ring.is_empty() && s.reoffer_next == 0 && !s.pingresp_owed
            }
            None => true,
        }
    }

    /// Take back production `id` of `conn`, wrapped by [`Self::produce`]
    /// but refused by the channel: the next production takes its id.
    pub fn unproduce(&mut self, conn: u16, id: u64) -> bool {
        match self.seat_of(conn) {
            Some(i) => self.seats[i].ring.unproduce(id),
            None => false,
        }
    }

    /// The session left this worker: its seat is free.
    pub fn close(&mut self, conn: u16) {
        if let Some(i) = self.seat_of(conn) {
            self.seats[i] = ProductionSeat::empty();
        }
    }

    /// Make the response envelope `inner` (`[conn_id:2][rest]`, message
    /// type `inner_mt`) for `conn` a production: number it, keep it, and
    /// wrap it into `out`.
    pub fn produce(&mut self, conn: u16, inner_mt: u8, inner: &[u8], out: &mut [u8]) -> Produced {
        let Some(i) = self.seat_of(conn) else {
            return Produced::Unseated;
        };
        if inner.len() < CONN_ID_LEN || out.len() < inner.len() + PRODUCTION_INSERT {
            return Produced::Malformed;
        }
        let s = &mut self.seats[i];
        if s.reoffer_next != 0 {
            self.full = self.full.wrapping_add(1);
            return Produced::Full;
        }
        let id = if inner.len() <= RETAIN_BYTES_MAX {
            s.ring.retain(inner)
        } else {
            let id = s.ring.number_unretained();
            if id.is_some() {
                self.unretained = self.unretained.wrapping_add(1);
            }
            id
        };
        let Some(id) = id else {
            self.full = self.full.wrapping_add(1);
            return Produced::Full;
        };
        let len = encode_production(out, inner_mt, inner, &s.session_id, s.epoch, id);
        Produced::Wrapped { id, len }
    }

    /// A `MSG_ANCHOR_HORIZON` payload: release what it covers on the seat
    /// it names, if it is that seat's anchor at its epoch, and keep the
    /// output bytes its last production ends at. Returns the productions
    /// released.
    pub fn horizon(&mut self, payload: &[u8]) -> usize {
        let Some(h) = HorizonReport::decode(payload) else {
            self.stale_reports = self.stale_reports.wrapping_add(1);
            return 0;
        };
        let Some(i) = self.seat_of_session(&h.session_id) else {
            self.stale_reports = self.stale_reports.wrapping_add(1);
            return 0;
        };
        let s = &mut self.seats[i];
        if !horizon_admitted(&s.session_id, s.epoch, &h) {
            self.stale_reports = self.stale_reports.wrapping_add(1);
            return 0;
        }
        if h.accepted_through <= s.ring.released() {
            return 0;
        }
        // A horizon past what was produced, or whose bytes do not move on
        // with its ids, is not this session's: taking either would place
        // a replacement's re-offers at the wrong bytes.
        if h.accepted_through >= s.ring.next_id() || h.bytes_through <= s.released_bytes {
            self.stale_reports = self.stale_reports.wrapping_add(1);
            return 0;
        }
        s.released_bytes = h.bytes_through;
        let n = s.ring.release_through(h.accepted_through);
        self.released = self.released.wrapping_add(n as u32);
        n
    }

    /// A replacement anchor's `MSG_ANCHOR_REANCHOR` for a seated session,
    /// with the worker's consumed delivery high-water `delivery_hw`. On
    /// `AC_OK` the seat is rebound at the new epoch, its connection the
    /// replacement's `conn`, and its retained productions above the
    /// replacement's horizon are to be offered again (`reoffer`).
    pub fn reanchor(&mut self, conn: u16, req: &Reanchor, delivery_hw: u64) -> Reanchored {
        self.reanchors = self.reanchors.wrapping_add(1);
        // The same replacement asking again (its first answer was lost, or
        // crossed its retry): it is already the seat's anchor, so it is
        // answered again with the seat's cursors, and the re-offers start
        // over — the gate answers what it already took without a send.
        if let Some(i) = self.seat_of_session(&req.session_id) {
            let s = &mut self.seats[i];
            if s.epoch == req.new_epoch && s.reanchored_by == req.anchor_id && s.conn == conn {
                s.reoffer_next = if s.ring.is_empty() {
                    0
                } else {
                    s.ring.first_retained()
                };
                return Reanchored {
                    session_id: req.session_id,
                    new_epoch: req.new_epoch,
                    status: AC_OK,
                    delivery_hw,
                    next_production: s.ring.next_id(),
                    released: s.ring.released(),
                    released_bytes: s.released_bytes,
                };
            }
        }
        let seat = match self.seat_of_session(&req.session_id) {
            Some(i) => {
                let s = &self.seats[i];
                WorkerSeat {
                    session_id: s.session_id,
                    epoch: s.epoch,
                    delivery_hw,
                    next_production: s.ring.next_id(),
                    released: s.ring.released(),
                    released_bytes: s.released_bytes,
                    first_offerable: s.ring.first_offerable(),
                }
            }
            None => WorkerSeat {
                session_id: req.session_id,
                epoch: 0,
                delivery_hw,
                next_production: 1,
                released: 0,
                released_bytes: 0,
                first_offerable: 1,
            },
        };
        let ans = reanchor_verdict(&seat, req);
        if ans.status == AC_OK {
            if let Some(i) = self.seat_of_session(&req.session_id) {
                for (j, other) in self.seats.iter_mut().enumerate() {
                    if j != i && other.used && other.conn == conn {
                        *other = ProductionSeat::empty();
                    }
                }
                let s = &mut self.seats[i];
                s.epoch = req.new_epoch;
                s.conn = conn;
                s.reanchored_by = req.anchor_id;
                // Every retained production goes to the replacement again,
                // oldest first; what its standby holds it places without
                // sending twice, and each one it finds held comes back in
                // its first horizon report.
                s.reoffer_next = if s.ring.is_empty() {
                    0
                } else {
                    s.ring.first_retained()
                };
            }
        } else {
            self.reanchor_refused = self.reanchor_refused.wrapping_add(1);
        }
        ans
    }

    /// The next production `conn` owes the replacement anchor after a
    /// re-anchor: `(id, response envelope)`, to offer again under its
    /// original id, then [`Self::reoffered`]. `None` when none is owed.
    /// A production numbered without its bytes is passed over: the
    /// verdict admitted the replacement only with its horizon past every
    /// such one.
    pub fn next_reoffer(&mut self, conn: u16) -> Option<(u64, &[u8])> {
        let i = self.seat_of(conn)?;
        let s = &mut self.seats[i];
        if s.reoffer_next == 0 {
            return None;
        }
        let mut k = 0;
        while let Some(r) = s.ring.get(k) {
            if r.id >= s.reoffer_next && !r.unretained {
                s.reoffer_next = r.id;
                return Some((r.id, r.body()));
            }
            k += 1;
        }
        s.reoffer_next = 0;
        None
    }

    /// Re-offer `id` of `conn` went to the anchor: the next is owed.
    pub fn reoffered(&mut self, conn: u16, id: u64) {
        if let Some(i) = self.seat_of(conn) {
            let s = &mut self.seats[i];
            if s.reoffer_next == id {
                s.reoffer_next = if id + 1 >= s.ring.next_id() {
                    0
                } else {
                    id + 1
                };
            }
        }
    }

    /// Whether `conn` still owes the replacement anchor a re-offer.
    pub fn reoffer_owed(&self, conn: u16) -> bool {
        self.seat_of(conn)
            .is_some_and(|i| self.seats[i].reoffer_next != 0)
    }

    /// Wrap the retained production `(id, inner)` of `conn` into `out`
    /// again, under its original id and the seat's current epoch.
    pub fn rewrap(&self, conn: u16, inner_mt: u8, id: u64, inner: &[u8], out: &mut [u8]) -> usize {
        let Some(i) = self.seat_of(conn) else {
            return 0;
        };
        let s = &self.seats[i];
        encode_production(out, inner_mt, inner, &s.session_id, s.epoch, id)
    }
}

impl Default for ProductionSeats {
    fn default() -> Self {
        Self::new()
    }
}
