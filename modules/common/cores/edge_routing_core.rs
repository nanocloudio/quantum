// Bounded, no_std, no-alloc EDGE ROUTING core — owner resolution and
// epoch disposition for a listener that accepts any connection.
// `include!`d by the host test crate and by the modules that terminate
// client connections.
//
// ## The rules this encodes
//
// A connection's fate is not entangled with where it landed. Any node
// may accept any connection; landing on a non-owner costs one forward
// hop, never an error. Four rules follow from that, and every
// disposition below is one of them:
//
//   EDGE-ANYNODE   a hop is not an error, and is never surfaced to the
//                  client;
//   EDGE-EPOCH     every routed frame carries the routing epoch it was
//                  decided under, so a decision made against a stale
//                  map is detectable rather than silently wrong;
//   EDGE-FENCE     on an epoch change the losing PRG stops accepting
//                  BEFORE the gaining one starts, so ownership is
//                  never split;
//   EDGE-NOAFFINITY an ESTABLISHED session-oriented connection is
//                  never told to reconnect because placement moved;
//                  Kafka clients are, through the error codes their
//                  drivers already implement.
//
// This core owns the decision — resolve, compare epochs, choose a
// disposition — not the transport. The forward itself belongs to the
// peer plane, and until that exists a listener expresses a non-local
// disposition in the client's own protocol: see [`redirect_style`] and
// [`kafka_error_for`].

/// What a listener should do with a frame it has just classified.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EdgeAction {
    /// This node owns the shard: handle it here.
    Local,
    /// Another node owns it: forward, then proxy the response back.
    /// The client is never told; a hop is not an error (EDGE-ANYNODE).
    Forward(u16),
    /// Our placement view is OLDER than the sender's. We cannot decide
    /// correctly yet, so refresh from the control plane and retry
    /// rather than answer from a stale map.
    RefreshThenRetry,
    /// Our placement view is NEWER than the sender's. The sender is
    /// acting on a revoked placement; tell it, with the new owner.
    Redirect(u16),
    /// The shard is mid-fence: the losing owner has stopped accepting
    /// and the gaining one has not started. Briefly unavailable, which
    /// is the correct answer — accepting at either end would split
    /// ownership.
    Fenced,
}

/// Placement view a listener decides against.
#[derive(Clone, Copy)]
pub struct EdgeView {
    /// This node's PRG.
    pub local_prg: u16,
    /// Epoch this view was published under.
    pub epoch: u32,
    /// PRG whose shards are mid-transfer at this epoch, and whether
    /// that fence is live.
    ///
    /// The unit is a PRG, not a shard, because that is the unit the
    /// migration state machine actually moves: it runs source_prg ->
    /// target_prg over a shard COUNT, never a named shard set, so a
    /// shard-granular fence field could not be populated correctly by
    /// the only component that raises fences. Fencing the source PRG
    /// briefly refuses more than strictly necessary; refusing too much
    /// for a moment is safe, and splitting ownership is not.
    pub fenced_prg: u16,
    pub fenced_active: bool,
    /// How many PRGs the shard space is spread over. Needed to resolve
    /// a shard to its owning PRG, which is the lookup every caller of
    /// [`classify`] must do first — so the view that answers "is this
    /// mine?" is also the one that answers "whose is it?", rather than
    /// each module keeping its own copy of the divisor and drifting.
    pub prg_count: u16,
}

/// PRG count assumed until the control plane states one.
///
/// ONE, not a speculative 16: a graph with no control plane wired has
/// exactly one PRG, so every shard is local and every subscription is
/// owned here. Defaulting higher would place exact-topic subscriptions
/// on PRGs that do not exist and the delivery filter would drop them —
/// a subscriber that silently receives nothing.
///
/// Runtime, not compile-time: the whole point of the placement work is
/// that this number changes.
pub const DEFAULT_PRG_COUNT: u16 = 1;

/// `local_prg` sentinel in `MSG_PLACEMENT_UPDATE` meaning "this sender
/// is not asserting a placement; leave the current one alone". Without
/// it a fence-only update would silently reassign the node to PRG 0.
pub const PLACEMENT_UNCHANGED: u16 = u16::MAX;

/// What `apply_placement_update` did with a frame.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PlacementUpdate {
    /// The view moved to a strictly newer epoch.
    Applied,
    /// At or behind the epoch held. Re-deliveries are the common case,
    /// so callers count these rather than dropping them silently.
    Stale,
    /// Too short to carry even an epoch.
    Malformed,
}

impl EdgeView {
    pub const fn new(local_prg: u16, epoch: u32) -> Self {
        Self {
            local_prg,
            epoch,
            fenced_prg: 0,
            fenced_active: false,
            prg_count: DEFAULT_PRG_COUNT,
        }
    }

    fn is_fenced(&self, owner_prg: u16) -> bool {
        self.fenced_active && self.fenced_prg == owner_prg
    }
}

/// Sparse per-shard ownership overrides — the control plane's
/// `shard_map`, naming the PRG that SERVES a shard.
///
/// Sparse over the baseline rather than a dense `VIRTUAL_SHARDS`
/// array, which would be 512 KiB. At steady state every shard uses its
/// baseline; overrides are what a MIGRATION creates, a bounded batch at
/// a time.
///
/// On overflow the override is REFUSED and counted, so the shard keeps
/// its baseline owner. That is safe rather than merely convenient:
/// every node receives the same map and applies the same bound, so all
/// of them refuse the same entry and agree on the old owner. Accepting
/// it on some nodes and not others is what would split ownership.
pub const MAX_SHARD_OVERRIDES: usize = 256;

/// A `prg` of this value clears an override rather than setting one.
pub const SHARD_OVERRIDE_CLEAR: u16 = u16::MAX;

#[derive(Clone, Copy)]
pub struct ShardOverrides {
    shard: [u32; MAX_SHARD_OVERRIDES],
    prg: [u16; MAX_SHARD_OVERRIDES],
    count: u16,
    /// Monotone epoch of the last applied map, so a stale replay cannot
    /// undo a newer placement.
    epoch: u64,
    pub refused: u32,
}

impl Default for ShardOverrides {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardOverrides {
    pub const fn new() -> Self {
        Self {
            shard: [0; MAX_SHARD_OVERRIDES],
            prg: [0; MAX_SHARD_OVERRIDES],
            count: 0,
            epoch: 0,
            refused: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The override for `shard`, if any.
    pub fn get(&self, shard: u32) -> Option<u16> {
        let mut i = 0usize;
        while i < self.count as usize {
            if self.shard[i] == shard {
                return Some(self.prg[i]);
            }
            i += 1;
        }
        None
    }

    /// Set, replace or clear one shard's owning PRG.
    pub fn set(&mut self, shard: u32, prg: u16) {
        let mut i = 0usize;
        while i < self.count as usize {
            if self.shard[i] == shard {
                if prg == SHARD_OVERRIDE_CLEAR {
                    // Compact into the hole so `get`'s scan stays dense.
                    let last = self.count as usize - 1;
                    self.shard[i] = self.shard[last];
                    self.prg[i] = self.prg[last];
                    self.count -= 1;
                } else {
                    self.prg[i] = prg;
                }
                return;
            }
            i += 1;
        }
        if prg == SHARD_OVERRIDE_CLEAR {
            return;
        }
        if (self.count as usize) < MAX_SHARD_OVERRIDES {
            let n = self.count as usize;
            self.shard[n] = shard;
            self.prg[n] = prg;
            self.count += 1;
        } else {
            self.refused = self.refused.wrapping_add(1);
        }
    }

    /// Admit a map at `epoch` only if it is newer. Returns false when
    /// the update is stale and must be ignored wholesale.
    pub fn admit_epoch(&mut self, epoch: u64) -> bool {
        if epoch <= self.epoch && self.epoch != 0 {
            return false;
        }
        self.epoch = epoch;
        true
    }
}

/// A placement view together with the overrides that refine it.
///
/// Every ownership question lives HERE and not on [`EdgeView`]. The
/// view is `Copy` and passed by value everywhere, so a resolver hung
/// off it could answer "who owns this shard?" while silently ignoring
/// the override table — a wrong answer that reads exactly like a right
/// one. Keeping the resolver on the type that holds the table makes
/// asking without it a COMPILE ERROR rather than a convention.
///
/// Not `Copy`, deliberately: it holds the override table, and a caller
/// that copies it per call pays for the whole table.
#[derive(Clone)]
pub struct EdgeMap {
    pub view: EdgeView,
    pub overrides: ShardOverrides,
}

impl EdgeMap {
    pub const fn new(local_prg: u16, epoch: u32) -> Self {
        Self {
            view: EdgeView::new(local_prg, epoch),
            overrides: ShardOverrides::new(),
        }
    }

    /// True when the fence covers `prg`.
    pub fn is_fenced(&self, prg: u16) -> bool {
        self.view.is_fenced(prg)
    }

    /// The PRG that owns `shard`: the control plane's override if one
    /// exists, otherwise the baseline.
    ///
    /// Every module that routes on ownership resolves through this one
    /// function. A module keeping its own copy of the rule answers a
    /// different question from its peers, and a publish then lands on
    /// the PRG one module names while another expects it elsewhere.
    pub fn owner_prg(&self, shard: u32) -> u16 {
        if let Some(prg) = self.overrides.get(shard) {
            return prg;
        }
        baseline_prg(shard, self.view.prg_count)
    }

    /// True when `shard` maps to this node's PRG, IGNORING the fence.
    ///
    /// "Is this mine at all?" — distinct from [`owns_shard`], which also
    /// asks "may I act on it right now?". The two must not be conflated,
    /// because they drive opposite responses to a fence:
    ///
    /// - a fenced shard is still MINE, so its state must be KEPT: the
    ///   migration can still abort, and ABORTED lifts the fence and
    ///   leaves ownership exactly where it was. Dropping state on a
    ///   fence would destroy sessions a merely-aborted transfer was
    ///   supposed to leave untouched.
    /// - a shard that has been REASSIGNED is no longer mine, so its
    ///   state must be RELEASED — otherwise this node keeps answering
    ///   from a copy the new owner is already diverging from.
    ///
    /// So: withhold service on `!owns_shard`, release state on
    /// `!is_local`.
    pub fn is_local(&self, shard: u32) -> bool {
        self.owner_prg(shard) == self.view.local_prg
    }

    /// True when `shard` is mid-transfer and must not be accepted at
    /// EITHER end.
    ///
    /// Separate from `owns_shard` because the two questions are
    /// different and only one applies to a message being ROUTED. A
    /// publish is not state this node owns — the shard key says where it
    /// lands — so ownership must not gate it. But a FENCED shard is
    /// frozen for everyone: the losing owner has stopped accepting and
    /// the gaining one has not started, and accepting at either end is
    /// precisely the split the fence exists to prevent.
    pub fn is_shard_fenced(&self, shard: u32) -> bool {
        self.is_fenced(self.owner_prg(shard))
    }

    /// True when `shard` is owned by this node's PRG and not fenced —
    /// the question every per-shard state store has to answer before it
    /// serves or holds an entry.
    pub fn owns_shard(&self, shard: u32) -> bool {
        let owner = self.owner_prg(shard);
        owner == self.view.local_prg && !self.is_fenced(owner)
    }
}

/// Header of a `MSG_SHARD_MAP_UPDATE` body: `[epoch:u64][count:u16]`,
/// then `count` entries of `[shard:u32][prg:u16]`. Mirrors clustor's
/// `wire::SHARD_MAP_HDR` / `SHARD_MAP_ENTRY`.
pub const SHARD_MAP_HDR: usize = 10;
pub const SHARD_MAP_ENTRY: usize = 6;

/// Apply a `MSG_SHARD_MAP_UPDATE` payload to an override table.
///
/// Lives here, beside `apply_placement_update`, for the reason the core
/// exists at all: `session_processor` and `topic_engine` both resolve
/// ownership, so they must read this frame identically or they will
/// disagree about who owns a shard — the exact failure a private copy
/// of the modulo once caused.
///
/// Monotone in epoch and STRICTLY so, matching the placement gate: a
/// replayed map at the epoch held would otherwise undo a newer
/// placement. A stale map is rejected WHOLESALE rather than per entry,
/// because a half-applied map is a placement no node ever published.
pub fn apply_shard_map_update(o: &mut ShardOverrides, payload: &[u8]) -> PlacementUpdate {
    if payload.len() < SHARD_MAP_HDR {
        return PlacementUpdate::Malformed;
    }
    let epoch = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let count = u16::from_le_bytes([payload[8], payload[9]]) as usize;
    if payload.len() < SHARD_MAP_HDR + count * SHARD_MAP_ENTRY {
        return PlacementUpdate::Malformed;
    }
    if !o.admit_epoch(epoch) {
        return PlacementUpdate::Stale;
    }
    for i in 0..count {
        let off = SHARD_MAP_HDR + i * SHARD_MAP_ENTRY;
        let shard = u32::from_le_bytes([
            payload[off],
            payload[off + 1],
            payload[off + 2],
            payload[off + 3],
        ]);
        let prg = u16::from_le_bytes([payload[off + 4], payload[off + 5]]);
        o.set(shard, prg);
    }
    PlacementUpdate::Applied
}

/// Apply a `MSG_PLACEMENT_UPDATE` payload to a view.
///
/// Body: `[epoch:u32le]`, then two independently optional tails —
/// `[prg_count:u16][local_prg:u16]` and `[fenced_prg:u16][fenced:u8]`.
/// Each field is optional so a sender that knows only the fence can say
/// so without claiming a placement it does not own.
///
/// Monotone in epoch, and STRICTLY so. Admitting an update at the epoch
/// already held would let two different placements published at one
/// epoch both apply, the second silently winning — at which point the
/// epoch no longer identifies a placement, and the fence that forwarded
/// frames are stamped with could be rewound by an out-of-order arrival.
pub fn apply_placement_update(view: &mut EdgeView, payload: &[u8]) -> PlacementUpdate {
    if payload.len() < 4 {
        return PlacementUpdate::Malformed;
    }
    let epoch = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    if !accept_epoch(view.epoch, epoch) {
        return PlacementUpdate::Stale;
    }
    view.epoch = epoch;
    if payload.len() >= 8 {
        let prg_count = u16::from_le_bytes([payload[4], payload[5]]);
        if prg_count > 0 {
            view.prg_count = prg_count;
        }
        let local_prg = u16::from_le_bytes([payload[6], payload[7]]);
        if local_prg != PLACEMENT_UNCHANGED {
            view.local_prg = local_prg;
        }
    }
    if payload.len() >= 11 {
        view.fenced_prg = u16::from_le_bytes([payload[8], payload[9]]);
        view.fenced_active = payload[10] != 0;
    } else {
        // An update that names no fence CLEARS one: the transfer that
        // raised it has completed.
        view.fenced_active = false;
    }
    PlacementUpdate::Applied
}

/// The PRG a shard belongs to when no override names another: jump
/// consistent hash over `prg_count` PRGs. Mirrors
/// `clustor/modules/common/wire.rs::baseline_prg` byte for byte — the
/// control plane resolves owners with it too, and the two are pinned to
/// the same vectors by test on both sides.
///
/// Chosen over `shard % prg_count` for what happens on a resize: growing
/// N -> N+1 moves exactly the 1/(N+1) of shards that land on the new PRG
/// and nothing else, so a cluster widens without re-homing the shard
/// space and no dense map is ever needed.
///
/// `prg_count == 0` is treated as 1 — the wire's "unchanged" sentinel
/// must never divide.
pub fn baseline_prg(shard: u32, prg_count: u16) -> u16 {
    let buckets = i64::from(prg_count.max(1));
    let mut key = u64::from(shard);
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < buckets {
        b = j;
        key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        let denom = ((key >> 33) + 1) as f64;
        j = ((b + 1) as f64 * (2_147_483_648.0_f64 / denom)) as i64;
    }
    b as u16
}

/// Decide what to do with a frame owned by `owner_prg` under this
/// node's view, when the sender claims `sender_epoch`.
///
/// The caller resolves the shard to its `owner_prg` first (that is the
/// placement lookup); every disposition here turns on the PRG, so the
/// shard itself is deliberately not a parameter — passing one would
/// suggest a shard-granular fence this cannot express.
///
/// `sender_epoch == 0` means "the sender did not state one" — a plain
/// client rather than a peer — and is never treated as stale.
///
/// The epoch comparison comes FIRST, before ownership: deciding
/// ownership from a view we already know is out of date is how a frame
/// gets routed to a group that no longer owns it.
pub fn classify(view: &EdgeView, owner_prg: u16, sender_epoch: u32) -> EdgeAction {
    if sender_epoch > view.epoch {
        // The sender has seen a placement we have not. Answering now
        // would answer from a map we know is behind.
        return EdgeAction::RefreshThenRetry;
    }
    if sender_epoch != 0 && sender_epoch < view.epoch {
        // The sender is acting on a placement that has been superseded.
        return EdgeAction::Redirect(owner_prg);
    }
    if view.is_fenced(owner_prg) {
        return EdgeAction::Fenced;
    }
    if owner_prg == view.local_prg {
        EdgeAction::Local
    } else {
        EdgeAction::Forward(owner_prg)
    }
}

/// How a non-local disposition is expressed to a client, per protocol
/// (EDGE-NOAFFINITY).
///
/// This is the style for a connection that is already established. At
/// CONNECT there is no session to lose, so a session-oriented protocol
/// with a redirect of its own may use it — see `session_processor`'s
/// placement gate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RedirectStyle {
    /// Forward transparently and proxy the reply. MQTT has no
    /// "try another broker" in its protocol, and a client told to
    /// reconnect mid-session loses its subscriptions and inflight QoS
    /// state — so a rebalance must be invisible to it.
    Transparent,
    /// Answer with a retryable error so the driver refreshes metadata.
    /// Kafka clients already implement exactly this loop, so using it
    /// is cheaper and more correct than inventing a redirect.
    ClientRetry,
}

pub const PROTO_MQTT: u8 = 0;
pub const PROTO_KAFKA: u8 = 1;
pub const PROTO_AMQP: u8 = 2;

/// Redirect style for a protocol.
pub fn redirect_style(proto: u8) -> RedirectStyle {
    match proto {
        PROTO_KAFKA => RedirectStyle::ClientRetry,
        // MQTT and AMQP are session-oriented: the connection carries
        // subscriptions, inflight QoS state and delivery credit that a
        // reconnect would destroy.
        _ => RedirectStyle::Transparent,
    }
}

/// Kafka error codes a listener answers a non-local disposition with.
/// Values are Kafka's, because clients switch on them.
pub const KERR_NONE: i16 = 0;
pub const KERR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const KERR_LEADER_NOT_AVAILABLE: i16 = 5;

/// Map a disposition to the Kafka error a listener should answer with,
/// or `None` when the request is this node's to serve.
///
/// For Kafka this REPLACES forwarding. `redirect_style` already says
/// Kafka is [`RedirectStyle::ClientRetry`]: a driver that receives
/// NOT_LEADER_OR_FOLLOWER refreshes metadata and retries against the
/// right broker, which is cheaper than proxying the request and is a
/// loop every client already implements. Proxying Kafka would reinvent
/// a redirect the protocol already has — so forward-and-proxy is an
/// MQTT/AMQP requirement only, where the connection carries session
/// state a reconnect would destroy.
///
/// `Fenced` and `RefreshThenRetry` answer LEADER_NOT_AVAILABLE rather
/// than NOT_LEADER: both are transient and have no better owner to name
/// yet, and that code tells a client to retry WITHOUT pinning it to a
/// node that is mid-handover.
pub fn kafka_error_for(action: EdgeAction) -> Option<i16> {
    match action {
        EdgeAction::Local => None,
        EdgeAction::Forward(_) | EdgeAction::Redirect(_) => Some(KERR_NOT_LEADER_OR_FOLLOWER),
        EdgeAction::Fenced | EdgeAction::RefreshThenRetry => Some(KERR_LEADER_NOT_AVAILABLE),
    }
}

/// Whether an epoch update should be adopted.
///
/// Monotone: a stale or re-delivered update is ignored. Without this a
/// late-arriving older epoch would un-fence a shard whose transfer has
/// already completed.
pub fn accept_epoch(current: u32, incoming: u32) -> bool {
    incoming > current
}
