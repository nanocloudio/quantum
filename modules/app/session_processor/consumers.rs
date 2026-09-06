//! consumers — Kafka consumer groups, committed offsets, and AMQP push
//! consumers.
//!
//! Three registries answering "who is reading, and from where":
//!
//!   - `groups` — Kafka group membership and generation. Assignment is
//!     computed client-side by the group leader (JoinGroup returns the
//!     member list to the leader, the leader pushes per-member
//!     assignments via SyncGroup); the broker stores and echoes them,
//!     so this is deliberately thin.
//!   - `offsets` — committed offsets per (group, topic, partition).
//!   - `amqp` — Basic.Consume registrations with their prefetch credit
//!     and delivery-tag bookkeeping.
//!
//! The record types stay private. Callers work through the operations
//! below and read state through small `Copy` views, so the invariants
//! this component owns — generation bumps on every membership change,
//! a group with no members deactivating, delivery-tag monotonicity —
//! cannot be bypassed by a caller holding a `&mut` to a record.
//!
//! Members are released when their connection drops, not only on an
//! explicit LeaveGroup: clients commonly close without leaving, and
//! ghosts would otherwise accumulate until the fixed member table
//! exhausts and bricks the group.
//!
//! ## Per-step bound
//!
//! Every entry point is O(groups × members), O(offsets) or O(consumers)
//! over fixed tables and returns without blocking.

use super::{
    ACONSUMERS, AMQP_TAG_MAX, KAFKA_MAX_TOPIC, KGROUPS, KGROUP_MEMBERS, KG_META, KG_NAME, KOFFSETS,
};

#[repr(C)]
#[derive(Clone, Copy)]
struct KGroupMember {
    active: u8,
    id_len: u8,
    /// The connection this member joined on. On MSG_SESSION_DISCONNECT the
    /// member is released so a crashed consumer (the common case — clients
    /// default to not sending LeaveGroup on close) can't accumulate as a
    /// ghost until the 8-slot table exhausts and bricks the group.
    conn_id: u16,
    meta_len: u16,
    assign_len: u16,
    id: [u8; KG_NAME],
    meta: [u8; KG_META],
    assign: [u8; KG_META],
}

impl KGroupMember {
    const fn zero() -> Self {
        Self {
            active: 0,
            id_len: 0,
            conn_id: 0,
            meta_len: 0,
            assign_len: 0,
            id: [0; KG_NAME],
            meta: [0; KG_META],
            assign: [0; KG_META],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct KGroup {
    active: u8,
    name_len: u8,
    proto_len: u8,
    _pad: u8,
    generation: i32,
    member_seq: u32,
    name: [u8; KG_NAME],
    proto: [u8; 16],
    members: [KGroupMember; KGROUP_MEMBERS],
}

impl KGroup {
    const fn zero() -> Self {
        Self {
            active: 0,
            name_len: 0,
            proto_len: 0,
            _pad: 0,
            generation: 0,
            member_seq: 0,
            name: [0; KG_NAME],
            proto: [0; 16],
            members: [KGroupMember::zero(); KGROUP_MEMBERS],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct KOffset {
    active: u8,
    group_len: u8,
    topic_len: u8,
    _pad: u8,
    partition: u16,
    _pad2: u16,
    offset: i64,
    group: [u8; KG_NAME],
    topic: [u8; KAFKA_MAX_TOPIC],
}

impl KOffset {
    const fn zero() -> Self {
        Self {
            active: 0,
            group_len: 0,
            topic_len: 0,
            _pad: 0,
            partition: 0,
            _pad2: 0,
            offset: -1,
            group: [0; KG_NAME],
            topic: [0; KAFKA_MAX_TOPIC],
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AmqpConsumer {
    active: u8,
    no_ack: u8,
    conn_id: u16,
    tag_len: u8,
    queue_len: u8,
    _pad: u8,
    channel: u16,
    prefetch: u16,
    unacked: u16,
    /// Next delivery tag to assign. Delivery tags are monotonic and
    /// channel-scoped (one consumer per channel here), starting at 1 —
    /// tag 0 is reserved by AMQP for the client's "all messages" ack.
    next_dtag: u64,
    /// Highest delivery tag the client has acknowledged. Outstanding
    /// tags are always the contiguous range `(last_acked_dtag,
    /// next_dtag)`, which makes both single and `multiple` acks (and the
    /// `dt==0, multiple=true` "ack everything" idiom) exact.
    last_acked_dtag: u64,
    /// Next store offset to deliver from.
    cursor: u64,
    tag: [u8; AMQP_TAG_MAX],
    queue: [u8; KAFKA_MAX_TOPIC],
}

impl AmqpConsumer {
    const fn zero() -> Self {
        Self {
            active: 0,
            conn_id: 0,
            no_ack: 0,
            tag_len: 0,
            queue_len: 0,
            _pad: 0,
            channel: 0,
            prefetch: 0,
            unacked: 0,
            next_dtag: 1,
            last_acked_dtag: 0,
            cursor: 0,
            tag: [0; AMQP_TAG_MAX],
            queue: [0; KAFKA_MAX_TOPIC],
        }
    }
}

fn group_find_idx(s: &Consumers, name: &[u8]) -> Option<usize> {
    (0..KGROUPS).find(|&i| {
        s.groups[i].active == 1 && &s.groups[i].name[..s.groups[i].name_len as usize] == name
    })
}

fn group_find_or_create_idx(s: &mut Consumers, name: &[u8]) -> Option<usize> {
    if name.is_empty() || name.len() > KG_NAME {
        return None;
    }
    if let Some(i) = group_find_idx(s, name) {
        return Some(i);
    }
    for i in 0..KGROUPS {
        if s.groups[i].active == 0 {
            s.groups[i] = KGroup::zero();
            s.groups[i].active = 1;
            s.groups[i].name_len = name.len() as u8;
            s.groups[i].name[..name.len()].copy_from_slice(name);
            s.groups[i].generation = 1;
            return Some(i);
        }
    }
    None
}

fn member_idx(g: &KGroup, id: &[u8]) -> Option<usize> {
    (0..KGROUP_MEMBERS).find(|&i| {
        g.members[i].active == 1 && &g.members[i].id[..g.members[i].id_len as usize] == id
    })
}

/// First active member = group leader (assignment computer).
fn leader_idx(g: &KGroup) -> Option<usize> {
    (0..KGROUP_MEMBERS).find(|&i| g.members[i].active == 1)
}

pub fn offset_store(s: &mut Consumers, group: &[u8], topic: &[u8], partition: u16, offset: i64) {
    if group.len() > KG_NAME || topic.len() > KAFKA_MAX_TOPIC {
        return;
    }
    for i in 0..KOFFSETS {
        let o = &s.offsets[i];
        if o.active == 1
            && o.partition == partition
            && &o.group[..o.group_len as usize] == group
            && &o.topic[..o.topic_len as usize] == topic
        {
            s.offsets[i].offset = offset;
            return;
        }
    }
    for i in 0..KOFFSETS {
        if s.offsets[i].active == 0 {
            let o = &mut s.offsets[i];
            o.active = 1;
            o.group_len = group.len() as u8;
            o.topic_len = topic.len() as u8;
            o.partition = partition;
            o.offset = offset;
            o.group[..group.len()].copy_from_slice(group);
            o.topic[..topic.len()].copy_from_slice(topic);
            return;
        }
    }
}

pub fn offset_get(s: &Consumers, group: &[u8], topic: &[u8], partition: u16) -> i64 {
    for i in 0..KOFFSETS {
        let o = &s.offsets[i];
        if o.active == 1
            && o.partition == partition
            && &o.group[..o.group_len as usize] == group
            && &o.topic[..o.topic_len as usize] == topic
        {
            return o.offset;
        }
    }
    -1
}

/// Component state. Owned exclusively by this subtree.
#[repr(C)]
pub struct Consumers {
    groups: [KGroup; KGROUPS],
    offsets: [KOffset; KOFFSETS],
    amqp: [AmqpConsumer; ACONSUMERS],
}

pub fn init(s: &mut Consumers) {
    for g in s.groups.iter_mut() {
        *g = KGroup::zero();
    }
    for o in s.offsets.iter_mut() {
        *o = KOffset::zero();
    }
    for c in s.amqp.iter_mut() {
        *c = AmqpConsumer::zero();
    }
}

// ── Groups ──────────────────────────────────────────────────────────

pub fn group_find(s: &Consumers, name: &[u8]) -> Option<usize> {
    group_find_idx(s, name)
}

pub fn group_find_or_create(s: &mut Consumers, name: &[u8]) -> Option<usize> {
    group_find_or_create_idx(s, name)
}

/// Raise a group's generation to at least `generation`, creating the
/// group if this node has never seen it.
///
/// The apply-side half of [`wire::QOP_KAFKA_GROUP_GEN`]. A MAXIMUM, not
/// an assignment: a replayed or re-delivered record must not move a
/// generation backwards, and a coordinator rebuilding a group after
/// failover must resume ABOVE every generation the previous one issued
/// rather than reissuing them.
///
/// Creating an empty group here is deliberate. A group with a high
/// generation and no members is exactly the state that stops the next
/// join from reusing a generation a zombie may still hold.
///
/// Returns false only when the group table is full.
pub fn group_generation_raise(s: &mut Consumers, name: &[u8], generation: i32) -> bool {
    let Some(gi) = group_find_or_create_idx(s, name) else {
        return false;
    };
    if generation > s.groups[gi].generation {
        s.groups[gi].generation = generation;
    }
    true
}

/// A group's `(name, generation)` if the slot is live, for the
/// reconciliation sweep that replicates generations.
pub fn group_snapshot(s: &Consumers, gi: usize, dst: &mut [u8]) -> Option<(usize, i32)> {
    if gi >= KGROUPS || s.groups[gi].active == 0 {
        return None;
    }
    let n = s.groups[gi].name_len as usize;
    if n == 0 || n > dst.len() {
        return None;
    }
    dst[..n].copy_from_slice(&s.groups[gi].name[..n]);
    Some((n, s.groups[gi].generation))
}

pub fn group_generation(s: &Consumers, gi: usize) -> i32 {
    if gi < KGROUPS {
        s.groups[gi].generation
    } else {
        0
    }
}

pub fn group_leader(s: &Consumers, gi: usize) -> Option<usize> {
    if gi < KGROUPS {
        leader_idx(&s.groups[gi])
    } else {
        None
    }
}

pub fn member_find(s: &Consumers, gi: usize, id: &[u8]) -> Option<usize> {
    if gi < KGROUPS {
        member_idx(&s.groups[gi], id)
    } else {
        None
    }
}

pub fn member_active(s: &Consumers, gi: usize, mi: usize) -> bool {
    gi < KGROUPS && mi < KGROUP_MEMBERS && s.groups[gi].members[mi].active == 1
}

/// Copy the group's protocol name out. Returns its length.
pub fn group_proto_into(s: &Consumers, gi: usize, dst: &mut [u8]) -> usize {
    if gi >= KGROUPS {
        return 0;
    }
    let n = (s.groups[gi].proto_len as usize).min(dst.len());
    dst[..n].copy_from_slice(&s.groups[gi].proto[..n]);
    n
}

pub fn member_id_into(s: &Consumers, gi: usize, mi: usize, dst: &mut [u8]) -> usize {
    if !member_active(s, gi, mi) {
        return 0;
    }
    let n = (s.groups[gi].members[mi].id_len as usize).min(dst.len());
    dst[..n].copy_from_slice(&s.groups[gi].members[mi].id[..n]);
    n
}

pub fn member_meta_into(s: &Consumers, gi: usize, mi: usize, dst: &mut [u8]) -> usize {
    if !member_active(s, gi, mi) {
        return 0;
    }
    let n = (s.groups[gi].members[mi].meta_len as usize).min(dst.len());
    dst[..n].copy_from_slice(&s.groups[gi].members[mi].meta[..n]);
    n
}

pub fn member_assignment_into(s: &Consumers, gi: usize, mi: usize, dst: &mut [u8]) -> usize {
    if !member_active(s, gi, mi) {
        return 0;
    }
    let n = (s.groups[gi].members[mi].assign_len as usize).min(dst.len());
    dst[..n].copy_from_slice(&s.groups[gi].members[mi].assign[..n]);
    n
}

/// Next broker-assigned member id for a client that sent none
/// ("qm-<seq>"), written into `dst`. Returns its length. Consumes a
/// sequence number, so call once per join that needs one.
pub fn next_member_id(s: &mut Consumers, gi: usize, dst: &mut [u8]) -> usize {
    if gi >= KGROUPS || dst.len() < 13 {
        return 0;
    }
    s.groups[gi].member_seq = s.groups[gi].member_seq.wrapping_add(1);
    let mut n = s.groups[gi].member_seq;
    dst[0] = b'q';
    dst[1] = b'm';
    dst[2] = b'-';
    let mut digits = [0u8; 10];
    let mut d = 0;
    loop {
        digits[d] = b'0' + (n % 10) as u8;
        n /= 10;
        d += 1;
        if n == 0 {
            break;
        }
    }
    for k in 0..d {
        dst[3 + k] = digits[d - 1 - k];
    }
    3 + d
}

/// Admit a member. Returns its index, or None when the member table is
/// full. A member that is already present is returned as-is; a new one
/// bumps the generation, because membership changed.
pub fn member_join(s: &mut Consumers, gi: usize, id: &[u8], conn_id: u16) -> Option<usize> {
    if gi >= KGROUPS || id.is_empty() || id.len() > KG_NAME {
        return None;
    }
    if let Some(mi) = member_idx(&s.groups[gi], id) {
        // Rebind. A member restored from the log after a coordinator
        // change carries the connection it joined on in the PREVIOUS
        // run, which no longer exists — leaving it stale would route
        // this member's deliveries at a dead connection. Rebinding also
        // covers a client that reconnects without leaving first.
        s.groups[gi].members[mi].conn_id = conn_id;
        return Some(mi);
    }
    let free = (0..KGROUP_MEMBERS).find(|&i| s.groups[gi].members[i].active == 0)?;
    let m = &mut s.groups[gi].members[free];
    *m = KGroupMember::zero();
    m.active = 1;
    m.id_len = id.len() as u8;
    m.conn_id = conn_id;
    m.id[..id.len()].copy_from_slice(id);
    s.groups[gi].generation = s.groups[gi].generation.wrapping_add(1);
    Some(free)
}

/// Re-admit a member from the replicated log, with its assignment,
/// WITHOUT bumping the generation.
///
/// The apply-side half of [`wire::QOP_KAFKA_GROUP_MEMBER`]. Not bumping
/// is the whole point: the generation is what a rejoining consumer
/// presents, and moving it would force the rebalance this record exists
/// to avoid. `conn_id` is left unbound — the connection this member
/// joined on belongs to the run that ended — and `member_join` rebinds
/// it when the client returns.
///
/// Idempotent: replaying the same record refreshes the assignment in
/// place rather than consuming a second member slot.
pub fn member_restore(s: &mut Consumers, name: &[u8], id: &[u8], assign: &[u8]) -> bool {
    if id.is_empty() || id.len() > KG_NAME {
        return false;
    }
    let Some(gi) = group_find_or_create_idx(s, name) else {
        return false;
    };
    let mi = match member_idx(&s.groups[gi], id) {
        Some(mi) => mi,
        None => {
            let Some(free) = (0..KGROUP_MEMBERS).find(|&i| s.groups[gi].members[i].active == 0)
            else {
                return false;
            };
            let m = &mut s.groups[gi].members[free];
            *m = KGroupMember::zero();
            m.active = 1;
            m.id_len = id.len() as u8;
            m.conn_id = 0;
            m.id[..id.len()].copy_from_slice(id);
            free
        }
    };
    let al = assign.len().min(KG_META);
    let m = &mut s.groups[gi].members[mi];
    m.assign[..al].copy_from_slice(&assign[..al]);
    m.assign_len = al as u16;
    true
}

/// Record the member's subscription metadata and the group's protocol
/// name, as carried by JoinGroup.
pub fn member_set_meta(s: &mut Consumers, gi: usize, mi: usize, meta: &[u8], proto: &[u8]) {
    if !member_active(s, gi, mi) {
        return;
    }
    let n = meta.len().min(KG_META);
    s.groups[gi].members[mi].meta_len = n as u16;
    s.groups[gi].members[mi].meta[..n].copy_from_slice(&meta[..n]);
    let p = proto.len().min(16);
    s.groups[gi].proto_len = p as u8;
    s.groups[gi].proto[..p].copy_from_slice(&proto[..p]);
}

/// Store one member's assignment, as pushed by the group leader in
/// SyncGroup. False if the member is unknown.
pub fn member_set_assignment(s: &mut Consumers, gi: usize, id: &[u8], assign: &[u8]) -> bool {
    if gi >= KGROUPS {
        return false;
    }
    let Some(mi) = member_idx(&s.groups[gi], id) else {
        return false;
    };
    let n = assign.len().min(KG_META);
    s.groups[gi].members[mi].assign_len = n as u16;
    s.groups[gi].members[mi].assign[..n].copy_from_slice(&assign[..n]);
    true
}

/// Release a member. Bumps the generation, and deactivates the group
/// once its last member is gone so a stale name cannot hold a slot.
pub fn member_leave(s: &mut Consumers, gi: usize, mi: usize) {
    if !member_active(s, gi, mi) {
        return;
    }
    s.groups[gi].members[mi] = KGroupMember::zero();
    s.groups[gi].generation = s.groups[gi].generation.wrapping_add(1);
    if leader_idx(&s.groups[gi]).is_none() {
        s.groups[gi].active = 0;
    }
}

/// Slot `i`'s committed offset, if it holds one for this
/// (group, topic). Lets a caller walk `0..OFFSET_SLOTS` to enumerate a
/// topic's committed partitions without borrowing the table.
pub fn offset_at(s: &Consumers, i: usize, group: &[u8], topic: &[u8]) -> Option<(u16, i64)> {
    if i >= KOFFSETS {
        return None;
    }
    let o = &s.offsets[i];
    if o.active != 1
        || &o.group[..o.group_len as usize] != group
        || &o.topic[..o.topic_len as usize] != topic
    {
        return None;
    }
    Some((o.partition, o.offset))
}

/// Slot count for `offset_at` walks.
pub const OFFSET_SLOTS: usize = KOFFSETS;

// ── AMQP push consumers ─────────────────────────────────────────────

/// What the delivery pump needs to decide whether to push to a consumer
/// and how to address the frame.
#[derive(Clone, Copy)]
pub struct ConsumerView {
    pub conn_id: u16,
    pub channel: u16,
    pub no_ack: bool,
    pub cursor: u64,
    pub next_dtag: u64,
}

pub fn consumer_active(s: &Consumers, ci: usize) -> bool {
    ci < ACONSUMERS && s.amqp[ci].active == 1
}

pub fn consumer_view(s: &Consumers, ci: usize) -> Option<ConsumerView> {
    if !consumer_active(s, ci) {
        return None;
    }
    let c = &s.amqp[ci];
    Some(ConsumerView {
        conn_id: c.conn_id,
        channel: c.channel,
        no_ack: c.no_ack == 1,
        cursor: c.cursor,
        next_dtag: c.next_dtag,
    })
}

/// True when the consumer may be sent another delivery: `no_ack`
/// consumers are always in credit, manual-ack consumers hold credit
/// until the client acks.
pub fn consumer_in_credit(s: &Consumers, ci: usize) -> bool {
    if !consumer_active(s, ci) {
        return false;
    }
    let c = &s.amqp[ci];
    c.no_ack == 1 || c.prefetch == 0 || c.unacked < c.prefetch
}

pub fn consumer_queue_into(s: &Consumers, ci: usize, dst: &mut [u8]) -> usize {
    if !consumer_active(s, ci) {
        return 0;
    }
    let n = (s.amqp[ci].queue_len as usize).min(dst.len());
    dst[..n].copy_from_slice(&s.amqp[ci].queue[..n]);
    n
}

pub fn consumer_tag_into(s: &Consumers, ci: usize, dst: &mut [u8]) -> usize {
    if !consumer_active(s, ci) {
        return 0;
    }
    let n = (s.amqp[ci].tag_len as usize).min(dst.len());
    dst[..n].copy_from_slice(&s.amqp[ci].tag[..n]);
    n
}

/// Find a free slot, or the existing registration for this
/// (conn, channel) — AMQP allows one consumer per channel here, so a
/// re-Consume replaces rather than duplicates.
pub fn consumer_slot(s: &Consumers, conn_id: u16, channel: u16) -> Option<usize> {
    for i in 0..ACONSUMERS {
        if s.amqp[i].active == 1 && s.amqp[i].conn_id == conn_id && s.amqp[i].channel == channel {
            return Some(i);
        }
    }
    (0..ACONSUMERS).find(|&i| s.amqp[i].active == 0)
}

/// Register a Basic.Consume. `cursor` is where delivery starts — the
/// caller passes the queue's current Get cursor so Get-consumed
/// messages are not redelivered.
#[allow(
    clippy::too_many_arguments,
    reason = "one registration record, passed flat"
)]
pub fn consumer_register(
    s: &mut Consumers,
    ci: usize,
    conn_id: u16,
    channel: u16,
    no_ack: bool,
    prefetch: u16,
    tag: &[u8],
    queue: &[u8],
    cursor: u64,
) {
    if ci >= ACONSUMERS {
        return;
    }
    let c = &mut s.amqp[ci];
    *c = AmqpConsumer::zero();
    c.active = 1;
    c.conn_id = conn_id;
    c.channel = channel;
    c.no_ack = u8::from(no_ack);
    c.prefetch = prefetch;
    let tl = tag.len().min(AMQP_TAG_MAX);
    let ql = queue.len().min(KAFKA_MAX_TOPIC);
    c.tag_len = tl as u8;
    c.queue_len = ql as u8;
    c.tag[..tl].copy_from_slice(&tag[..tl]);
    c.queue[..ql].copy_from_slice(&queue[..ql]);
    c.cursor = cursor;
}

pub fn consumer_set_cursor(s: &mut Consumers, ci: usize, cursor: u64) {
    if ci < ACONSUMERS {
        s.amqp[ci].cursor = cursor;
    }
}

/// Account one delivery: advance the cursor past `offset`, take the next
/// delivery tag, and consume prefetch credit for manual-ack consumers.
pub fn consumer_delivered(s: &mut Consumers, ci: usize, offset: u64) {
    if !consumer_active(s, ci) {
        return;
    }
    let c = &mut s.amqp[ci];
    c.cursor = offset + 1;
    c.next_dtag = c.next_dtag.wrapping_add(1);
    if c.no_ack == 0 {
        c.unacked = c.unacked.saturating_add(1);
    }
}

/// Find the consumer owning (conn, channel), if any.
pub fn consumer_on_channel(s: &Consumers, conn_id: u16, channel: u16) -> Option<usize> {
    (0..ACONSUMERS).find(|&i| {
        s.amqp[i].active == 1 && s.amqp[i].conn_id == conn_id && s.amqp[i].channel == channel
    })
}

/// Find the consumer on (conn, channel) whose tag matches, as
/// Basic.Cancel addresses it.
pub fn consumer_by_tag(s: &Consumers, conn_id: u16, channel: u16, tag: &[u8]) -> Option<usize> {
    (0..ACONSUMERS).find(|&i| {
        let c = &s.amqp[i];
        c.active == 1
            && c.conn_id == conn_id
            && c.channel == channel
            && &c.tag[..c.tag_len as usize] == tag
    })
}

pub fn consumer_release(s: &mut Consumers, ci: usize) {
    if ci < ACONSUMERS {
        s.amqp[ci] = AmqpConsumer::zero();
    }
}

/// Apply a Basic.Ack. Outstanding tags are always the contiguous range
/// `(last_acked, next_dtag)`, which makes single acks, `multiple` acks
/// and the `dt == 0, multiple` "ack everything" idiom all exact.
/// Returns how many deliveries the ack released.
pub fn consumer_ack(s: &mut Consumers, ci: usize, dt: u64, multiple: bool) -> u64 {
    if !consumer_active(s, ci) {
        return 0;
    }
    let c = &mut s.amqp[ci];
    let lowest = c.last_acked_dtag + 1;
    let highest = c.next_dtag.saturating_sub(1);
    if highest < lowest {
        return 0;
    }
    if multiple {
        let target = if dt == 0 { highest } else { dt.min(highest) };
        if target < lowest {
            return 0;
        }
        let released = target - c.last_acked_dtag;
        c.unacked = c.unacked.saturating_sub(released as u16);
        c.last_acked_dtag = target;
        released
    } else {
        if dt < lowest || dt > highest {
            return 0;
        }
        c.unacked = c.unacked.saturating_sub(1);
        if dt == lowest {
            c.last_acked_dtag = dt;
        }
        1
    }
}

/// Release every group member and AMQP consumer belonging to `conn_id`.
/// Clients commonly close without LeaveGroup, so this is the path that
/// actually reclaims most members.
pub fn release_conn(s: &mut Consumers, conn_id: u16) -> u32 {
    let mut released = 0;
    for gi in 0..KGROUPS {
        if s.groups[gi].active != 1 {
            continue;
        }
        let mut changed = false;
        for mi in 0..KGROUP_MEMBERS {
            if s.groups[gi].members[mi].active == 1 && s.groups[gi].members[mi].conn_id == conn_id {
                s.groups[gi].members[mi] = KGroupMember::zero();
                changed = true;
                released += 1;
            }
        }
        if changed {
            s.groups[gi].generation = s.groups[gi].generation.wrapping_add(1);
            if leader_idx(&s.groups[gi]).is_none() {
                s.groups[gi].active = 0;
            }
        }
    }
    for ci in 0..ACONSUMERS {
        if s.amqp[ci].active == 1 && s.amqp[ci].conn_id == conn_id {
            s.amqp[ci] = AmqpConsumer::zero();
            released += 1;
        }
    }
    released
}
