// Bounded, no_std, no-alloc KAFKA GROUP COORDINATOR core — membership,
// generations, and BROKER-OWNED session expiry. `include!`d by the host
// test crate and by the module that owns a group's coordinator state.
//
// ## Why the broker holds this
//
// The broker is the only party that can see a missed heartbeat, so it
// is the only party that can evict a member that died silently. Leave
// expiry to the clients and a dead member holds its partitions until
// some other client notices and forces a rebalance.
//
// So the broker is the authority here: it holds the deadline, evicts on
// expiry, and owns a monotone generation counter. The state is shaped
// to be replicated through the owning group's Raft log, so it survives
// a coordinator change rather than being reconstructed from whatever
// the clients happen to say next — a coordinator that lost the
// generation would let a stale member rejoin at a generation the group
// had already passed.
//
// ## What is NOT here
//
// The wire codecs (JoinGroup/SyncGroup/Heartbeat framing) — those are
// `kafka_core.rs`. This core owns the state machine those codecs drive,
// which is the part whose invariants matter.

/// Members per group. A per-shard budget in the module that owns the
/// state; a constant here so the core stays allocation-free.
pub const MAX_MEMBERS: usize = 64;

/// Group lifecycle. Mirrors Kafka's, because clients implement against
/// it — inventing a different one would break every off-the-shelf
/// consumer.
pub const GROUP_EMPTY: u8 = 0;
pub const GROUP_PREPARING_REBALANCE: u8 = 1;
pub const GROUP_COMPLETING_REBALANCE: u8 = 2;
pub const GROUP_STABLE: u8 = 3;
pub const GROUP_DEAD: u8 = 4;

/// Why a rebalance was triggered — kept because "the group rebalanced"
/// with no cause is the least actionable line an operator can read.
pub const REBALANCE_JOIN: u8 = 0;
pub const REBALANCE_LEAVE: u8 = 1;
pub const REBALANCE_EXPIRED: u8 = 2;

#[derive(Clone, Copy)]
struct Member {
    /// 0 = free slot.
    id: u64,
    /// Deadline after which this member is considered gone. Absolute,
    /// so a slow step cannot silently extend a session.
    deadline_ms: u64,
    session_timeout_ms: u32,
    /// Set once the member has completed the current generation's sync.
    synced: bool,
}

impl Member {
    const fn empty() -> Self {
        Self {
            id: 0,
            deadline_ms: 0,
            session_timeout_ms: 0,
            synced: false,
        }
    }
}

/// One consumer group's coordinator state.
#[derive(Clone, Copy)]
pub struct Group {
    /// 0 = no group in this slot.
    pub group_id_hash: u64,
    pub state: u8,
    /// Monotone. Every membership change bumps it, and a request
    /// carrying an older generation is stale by construction — which is
    /// how a partitioned-away member is prevented from acting on an
    /// assignment the group has since revoked.
    pub generation: u32,
    /// Member that computes the assignment for this generation.
    pub leader: u64,
    members: [Member; MAX_MEMBERS],
    count: u8,
    /// Members evicted for missing their deadline, since boot.
    pub expired: u32,
    pub rebalances: u32,
}

impl Group {
    pub const fn new() -> Self {
        Self {
            group_id_hash: 0,
            state: GROUP_EMPTY,
            generation: 0,
            leader: 0,
            members: [Member::empty(); MAX_MEMBERS],
            count: 0,
            expired: 0,
            rebalances: 0,
        }
    }

    pub fn member_count(&self) -> usize {
        self.count as usize
    }

    pub fn contains(&self, member_id: u64) -> bool {
        self.index_of(member_id).is_some()
    }

    fn index_of(&self, member_id: u64) -> Option<usize> {
        let mut i = 0usize;
        while i < self.count as usize {
            if self.members[i].id == member_id {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    /// Add or refresh a member, opening a rebalance if the membership
    /// actually changed.
    ///
    /// A rejoin by an EXISTING member is not a membership change: it
    /// refreshes the deadline and leaves the generation alone. Bumping
    /// on every rejoin would let a flapping client rebalance the group
    /// continuously.
    pub fn join(&mut self, now_ms: u64, member_id: u64, session_timeout_ms: u32) -> bool {
        if member_id == 0 || self.state == GROUP_DEAD {
            return false;
        }
        if let Some(i) = self.index_of(member_id) {
            self.members[i].deadline_ms = now_ms + session_timeout_ms as u64;
            self.members[i].session_timeout_ms = session_timeout_ms;
            return true;
        }
        if self.count as usize >= MAX_MEMBERS {
            return false;
        }
        let i = self.count as usize;
        self.members[i] = Member {
            id: member_id,
            deadline_ms: now_ms + session_timeout_ms as u64,
            session_timeout_ms,
            synced: false,
        };
        self.count += 1;
        self.begin_rebalance(REBALANCE_JOIN);
        true
    }

    /// A member leaves cleanly.
    pub fn leave(&mut self, member_id: u64) -> bool {
        let Some(i) = self.index_of(member_id) else {
            return false;
        };
        self.remove_at(i);
        self.begin_rebalance(REBALANCE_LEAVE);
        true
    }

    /// Refresh a member's deadline. Refused when the generation is
    /// stale: a heartbeat from a member that has missed a rebalance
    /// must not keep it alive in a group that has moved on.
    pub fn heartbeat(&mut self, now_ms: u64, member_id: u64, generation: u32) -> bool {
        if generation != self.generation {
            return false;
        }
        let Some(i) = self.index_of(member_id) else {
            return false;
        };
        self.members[i].deadline_ms = now_ms + self.members[i].session_timeout_ms as u64;
        true
    }

    /// BROKER-OWNED EXPIRY. Evict every member past its deadline and
    /// open a rebalance if any went.
    ///
    /// This is the half that was missing: only the broker can see that a
    /// member stopped heartbeating, so only the broker can free its
    /// partitions. Leaving it to a client means a dead consumer holds
    /// its assignment until some other client happens to force a
    /// rebalance.
    pub fn expire_due(&mut self, now_ms: u64) -> u32 {
        let mut evicted = 0u32;
        let mut i = 0usize;
        while i < self.count as usize {
            if now_ms >= self.members[i].deadline_ms {
                self.remove_at(i);
                evicted += 1;
                // `remove_at` compacts, so do not advance `i`.
            } else {
                i += 1;
            }
        }
        if evicted > 0 {
            self.expired = self.expired.wrapping_add(evicted);
            self.begin_rebalance(REBALANCE_EXPIRED);
        }
        evicted
    }

    /// A member completes the current generation's sync.
    pub fn sync(&mut self, member_id: u64, generation: u32) -> bool {
        if generation != self.generation || self.state != GROUP_COMPLETING_REBALANCE {
            return false;
        }
        let Some(i) = self.index_of(member_id) else {
            return false;
        };
        self.members[i].synced = true;
        if self.all_synced() {
            self.state = GROUP_STABLE;
        }
        true
    }

    /// All members have joined this generation, so assignment can begin.
    pub fn close_join_window(&mut self) -> bool {
        if self.state != GROUP_PREPARING_REBALANCE {
            return false;
        }
        if self.count == 0 {
            self.state = GROUP_EMPTY;
            return true;
        }
        self.state = GROUP_COMPLETING_REBALANCE;
        true
    }

    fn all_synced(&self) -> bool {
        let mut i = 0usize;
        while i < self.count as usize {
            if !self.members[i].synced {
                return false;
            }
            i += 1;
        }
        true
    }

    fn remove_at(&mut self, i: usize) {
        let last = self.count as usize - 1;
        self.members[i] = self.members[last];
        self.members[last] = Member::empty();
        self.count -= 1;
    }

    /// Open a rebalance: bump the generation, clear sync state, and pick
    /// a leader.
    ///
    /// The generation bump is what makes every in-flight request from
    /// the old generation stale, so a member that was partitioned away
    /// during the change cannot act on a revoked assignment.
    fn begin_rebalance(&mut self, _reason: u8) {
        self.generation = self.generation.wrapping_add(1);
        self.rebalances = self.rebalances.wrapping_add(1);
        let mut i = 0usize;
        while i < self.count as usize {
            self.members[i].synced = false;
            i += 1;
        }
        if self.count == 0 {
            self.state = GROUP_EMPTY;
            self.leader = 0;
        } else {
            self.state = GROUP_PREPARING_REBALANCE;
            // Lowest member id: deterministic, so every replica of this
            // coordinator picks the same leader from the same state.
            let mut lo = self.members[0].id;
            let mut j = 1usize;
            while j < self.count as usize {
                if self.members[j].id < lo {
                    lo = self.members[j].id;
                }
                j += 1;
            }
            self.leader = lo;
        }
    }
}

impl Default for Group {
    fn default() -> Self {
        Self::new()
    }
}

/// Which coordinator shard owns a group.
///
/// Answering `FindCoordinator` with "always me" is only right on a
/// single broker. Ownership follows the same virtual-shard routing as
/// everything else, so a group's coordinator is wherever its shard
/// lives — and every broker computes the same answer.
pub fn coordinator_shard(group_key_hash: u64, virtual_shards: u32) -> u32 {
    (group_key_hash % virtual_shards as u64) as u32
}
