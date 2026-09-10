//! Topic Engine — Subscription index + publish routing.
//!
//! Merges subscription management and publish fan-out. Handles:
//! - MQTT wildcard matching (+, #)
//! - Shared subscription hashing (stable versioned seed)
//! - Per-PRG last_emit_index tracking
//!
//! Subscriptions are indexed by pattern class so a publish touches only
//! the slots that can match it: exact patterns hang off hash buckets
//! keyed by (tenant, pattern), wildcard patterns hang off a per-tenant
//! chain. Both use one intrusive link array, so a slot sits on exactly
//! one chain, and a free stack hands out slots without a scan.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK is include!'d wholesale and each module consumes only a subset; pending upstream allow attributes in target/fluxor/fluxor-abi/sdk/"
)]

use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "see file-level allow: SDK surface is shared across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

/// Edge routing dispositions — owner resolution, epoch comparison and
/// fence handling. Shared with the host test bed rather than restated
/// here, so the routing decision has exactly one definition.
#[path = "../../common/cores/edge_routing_core.rs"]
mod edge;

/// Kernel step ABI: 0=Continue, 1=Done, 2=Burst, 3=Ready. Returning
/// Burst re-runs the domain's exec rotation within the same tick (up to
/// the kernel's pass cap), so a record consumed here reaches its
/// consumer in this tick instead of the next. We burst only when a
/// record was actually consumed: an idle graph reports no burst and the
/// tick costs exactly one pass, as before.
const STEP_BURST: i32 = 2;

/// `session_slot` marking a subscription whose session lives on another
/// PRG. Mirrors `session_processor`'s constant; the two must agree.
const SESSION_SLOT_REMOTE: u32 = u32::MAX;

const MAX_SUBS: usize = 16384;
const MAX_TOPIC: usize = 256;
const MAX_SHARED_GROUPS: usize = 64;
/// Hash buckets for exact (wildcard-free) patterns. A power of two at
/// twice `MAX_SUBS`, so the mean chain length stays below one even with
/// every slot occupied.
const EXACT_BUCKETS: usize = MAX_SUBS * 2;
/// Wildcard chains, bucketed by tenant hash. A publish walks only the
/// wildcards of tenants sharing its bucket, and each walk re-checks the
/// tenant, so a bucket collision costs a compare rather than a wrong
/// delivery.
const TENANT_BUCKETS: usize = 64;
/// Read/write buffer cap for topic ops. Sized to the wire-channel
/// per-message capacity (`fluxor-abi::CHANNEL_BUFFER_SIZE` = 8192) so
/// a worst-case MQTT publish (`protocol::mqtt`'s `MAX_PACKET` = 4096) plus
/// `MSG_TOPIC_PUBLISH` framing (tenant + topic_len + topic prefix)
/// can flow without hitting the `channel_read_msg` discard path.
const MAX_TOPIC_MSG: usize = 8192;

/// Chain link: a subscription slot index plus one, so 0 is the end of a
/// chain and an all-zero state is an empty index.
type Link = u16;

const _: () = assert!(MAX_SUBS < Link::MAX as usize, "slot + 1 must fit a Link");
const _: () = assert!(EXACT_BUCKETS.is_power_of_two());
const _: () = assert!(TENANT_BUCKETS.is_power_of_two());

#[repr(C)]
#[derive(Clone, Copy)]
struct Subscription {
    tenant: u32,
    /// Local session index, or `SESSION_SLOT_REMOTE` when the
    /// subscriber's session lives on another PRG. Subscriptions are held
    /// by the TOPIC's owner (that is the node publishes route to), which
    /// at `prg_count > 1` is usually NOT the node holding the session.
    session_slot: u32,
    /// Names the subscriber across nodes. A local slot index means
    /// nothing on the node that must forward to it, so this is what a
    /// cross-PRG delivery is addressed by.
    stream_hash: u64,
    pattern: [u8; MAX_TOPIC],
    pattern_len: u16,
    qos: u8,
    shared_group_id: u16, // 0 = not shared
    remote_prg: u16,      // 0 = local, >0 = cross-PRG
    /// Which session worker on this node delivers to the slot: 0 on
    /// `deliver_out`, 1 on `deliver_out2`. Set from the subscribe
    /// record's worker byte and re-pointed by `MSG_TOPIC_MOVE` when
    /// the session moves in an anchor-preserved handoff.
    worker: u8,
    active: u8,
}

impl Subscription {
    const fn zero() -> Self {
        Self {
            tenant: 0,
            stream_hash: 0,
            session_slot: 0,
            pattern: [0; MAX_TOPIC],
            pattern_len: 0,
            qos: 0,
            shared_group_id: 0,
            remote_prg: 0,
            worker: 0,
            active: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct SharedGroup {
    group_id: u16,
    tenant: u32,
    member_slots: [u32; 16],
    member_count: u8,
    next_round_robin: u8,
    active: u8,
}

impl SharedGroup {
    const fn zero() -> Self {
        Self {
            group_id: 0,
            tenant: 0,
            member_slots: [0; 16],
            member_count: 0,
            next_round_robin: 0,
            active: 0,
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_op: i32,
    out_deliver: i32,
    out_metrics: i32,
    /// Deliveries for subscriptions held by session worker 1 (the
    /// standby of a handoff pair). Unwired (-1) on a one-worker node,
    /// where every delivery goes to `deliver_out`.
    out_deliver2: i32,
    /// Subscriptions re-pointed by `MSG_TOPIC_MOVE`.
    moves: u32,

    /// This node's placement: local PRG, epoch, PRG count and the
    /// migration fence. Held as the shared `EdgeMap` rather than as
    /// private fields so that the module deciding "is this mine?" and
    /// the core deciding "whose is it?" cannot drift apart — the
    /// shard->PRG divisor above all, which every routing module must
    /// resolve identically.
    view: edge::EdgeMap,
    /// Publishes refused because their shard was mid-transfer.
    routing_fenced: u32,
    /// Placement updates ignored as stale or re-delivered.
    routing_stale: u32,
    /// Control-plane routing input (optional).
    in_routing: i32,
    shared_hash_seed: u32,

    subs: [Subscription; MAX_SUBS],
    shared: [SharedGroup; MAX_SHARED_GROUPS],

    /// Subscription index. Every active slot is on exactly one chain:
    /// an exact pattern on the bucket for (tenant, pattern hash), a
    /// wildcard pattern on its tenant's chain. Chains are doubly linked
    /// so a slot unlinks in O(1) from any maintenance site.
    exact_head: [Link; EXACT_BUCKETS],
    wild_head: [Link; TENANT_BUCKETS],
    next: [Link; MAX_SUBS],
    prev: [Link; MAX_SUBS],
    /// Inactive slot indices; `free_len` is the stack depth. Filled in
    /// descending order so the lowest slot is handed out first.
    free_stack: [u16; MAX_SUBS],
    free_len: u32,

    subs_count: u32,
    publishes: u32,
    deliveries: u32,
    /// Count of subscriber deliveries that the downstream channel
    /// rejected (full ring). Without this, the silent-skip would mask
    /// post-durability subscriber-side data loss.
    deliveries_failed: u32,
    shared_deliveries: u32,
    last_emit_index: u64,
    last_metrics_ms: u64,
    // MAX_TOPIC_MSG sized to admit a worst-case MSG_TOPIC_PUBLISH
    // carrying a 4 KiB MQTT packet from `protocol::mqtt`
    // (`protocol::mqtt`'s MAX_PACKET) plus topic + envelope overhead.
    // Anything below this cap silently drops valid MQTT publishes at
    // the channel_read_msg discard path.
    buf: [u8; MAX_TOPIC_MSG],
    out_buf: [u8; MAX_TOPIC_MSG],
}

// `Subscription` is 280 B, so the 16384 slots are ~4.4 MiB; the index
// (three u16 arrays over the slots plus 32768 exact heads) adds ~160 KiB
// and the two channel buffers 16 KiB, ~4.7 MiB in all. The kernel
// allocates this whole struct per instance, so the bound here is what a
// graph must budget.
const _: () = assert!(core::mem::size_of::<ModuleState>() < 5 * 1024 * 1024);

/// Hash the topic hash + delivery id (publish ordinal) into a member index.
/// Stable versioned seed means the mapping survives restarts.
fn shared_member_index(topic_hash: u64, publish_ordinal: u64, member_count: u8, seed: u32) -> u8 {
    if member_count == 0 {
        return 0;
    }
    // FNV-1a mixing of the three inputs
    let mut h: u64 = 0xcbf29ce484222325;
    for byte in topic_hash
        .to_le_bytes()
        .iter()
        .chain(publish_ordinal.to_le_bytes().iter())
        .chain(seed.to_le_bytes().iter())
    {
        h ^= *byte as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h % member_count as u64) as u8
}

/// Detect if a subscription pattern is a shared subscription (MQTT 5 $share/group/topic).
/// Returns (group_id_hash_as_u16, stripped_pattern_offset).
fn parse_shared_prefix(pat: &[u8]) -> (u16, usize) {
    if pat.len() >= 7 && &pat[..7] == b"$share/" {
        let mut i = 7;
        while i < pat.len() && pat[i] != b'/' {
            i += 1;
        }
        if i < pat.len() && pat[i] == b'/' && i > 7 {
            let group_hash = wire::fnv1a_64(&pat[7..i]) as u16;
            // Avoid 0 (reserved for "not shared")
            return (if group_hash == 0 { 1 } else { group_hash }, i + 1);
        }
    }
    (0, 0)
}

fn session_has_shared_subscription(
    subs: &[Subscription; MAX_SUBS],
    tenant: u32,
    group_id: u16,
    session: u32,
) -> bool {
    subs.iter().any(|sub| {
        sub.active == 1
            && sub.tenant == tenant
            && sub.shared_group_id == group_id
            && sub.session_slot == session
    })
}

fn remove_shared_member(
    shared: &mut [SharedGroup; MAX_SHARED_GROUPS],
    tenant: u32,
    group_id: u16,
    session: u32,
) {
    let Some(group) = shared
        .iter_mut()
        .find(|g| g.active == 1 && g.tenant == tenant && g.group_id == group_id)
    else {
        return;
    };

    let mut kept = 0usize;
    for i in 0..group.member_count as usize {
        let member = group.member_slots[i];
        if member != session {
            group.member_slots[kept] = member;
            kept += 1;
        }
    }
    for slot in &mut group.member_slots[kept..] {
        *slot = 0;
    }
    group.member_count = kept as u8;
    if kept == 0 {
        *group = SharedGroup::zero();
    }
}

// ── Subscription index ──────────────────────────────────────────────────────

/// Bucket for an exact pattern: the pattern's FNV-1a hash with the
/// tenant folded in by four more FNV-1a steps, high half folded into
/// the low so the mask sees every input byte.
fn exact_bucket(tenant: u32, pattern_hash: u64) -> usize {
    let mut h = pattern_hash;
    for b in tenant.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    ((h ^ (h >> 32)) as usize) & (EXACT_BUCKETS - 1)
}

/// Chain for a tenant's wildcard patterns.
fn wild_bucket(tenant: u32) -> usize {
    let h = wire::fnv1a_64(&tenant.to_le_bytes());
    ((h ^ (h >> 32)) as usize) & (TENANT_BUCKETS - 1)
}

/// The head cell of the chain slot `i` belongs on, derived from the
/// slot's own tenant and pattern. Read the slot BEFORE zeroing it: a
/// zeroed slot resolves to the wrong chain.
fn chain_head(s: &mut ModuleState, i: usize) -> &mut Link {
    let sub = &s.subs[i];
    let pat = &sub.pattern[..sub.pattern_len as usize];
    if is_wildcard(pat) {
        &mut s.wild_head[wild_bucket(sub.tenant)]
    } else {
        &mut s.exact_head[exact_bucket(sub.tenant, wire::fnv1a_64(pat))]
    }
}

/// Push a filled slot onto the front of its chain.
fn index_link(s: &mut ModuleState, i: usize) {
    let link = (i + 1) as Link;
    let old = core::mem::replace(chain_head(s, i), link);
    s.next[i] = old;
    s.prev[i] = 0;
    if old != 0 {
        s.prev[old as usize - 1] = link;
    }
}

/// Take a slot off its chain, leaving its contents untouched.
fn index_unlink(s: &mut ModuleState, i: usize) {
    let nxt = s.next[i];
    let prv = s.prev[i];
    if prv == 0 {
        *chain_head(s, i) = nxt;
    } else {
        s.next[prv as usize - 1] = nxt;
    }
    if nxt != 0 {
        s.prev[nxt as usize - 1] = prv;
    }
    s.next[i] = 0;
    s.prev[i] = 0;
}

/// Empty the index and return every slot to the free stack. The slots
/// themselves are the caller's to zero.
fn index_reset(s: &mut ModuleState) {
    s.exact_head = [0; EXACT_BUCKETS];
    s.wild_head = [0; TENANT_BUCKETS];
    s.next = [0; MAX_SUBS];
    s.prev = [0; MAX_SUBS];
    for k in 0..MAX_SUBS {
        s.free_stack[k] = (MAX_SUBS - 1 - k) as u16;
    }
    s.free_len = MAX_SUBS as u32;
}

/// Claim a free slot, or `None` when every slot is in use. The slot is
/// still zeroed and off every chain; the caller fills it and links it.
fn index_alloc(s: &mut ModuleState) -> Option<usize> {
    if s.free_len == 0 {
        return None;
    }
    s.free_len -= 1;
    Some(s.free_stack[s.free_len as usize] as usize)
}

/// Retire an active slot: off its chain, zeroed, back on the free stack,
/// and out of `subs_count`. The one path by which a subscription ends,
/// so every teardown keeps the index and the free stack in step.
fn retire_subscription(s: &mut ModuleState, i: usize) {
    index_unlink(s, i);
    s.subs[i] = Subscription::zero();
    s.free_stack[s.free_len as usize] = i as u16;
    s.free_len += 1;
    s.subs_count = s.subs_count.saturating_sub(1);
}

/// Consume control-plane routing updates: the placement epoch and the
/// partition count this node should place against.
///
/// Monotone in epoch — a stale or re-delivered update is ignored, so an
/// out-of-order arrival cannot rewind the fence that forwarded frames
/// are stamped with.
///
/// # Safety
///
/// Caller must supply a valid `&SyscallTable` per the module ABI.
unsafe fn drain_routing(s: &mut ModuleState, sys: &SyscallTable) {
    if s.in_routing < 0 {
        return;
    }
    for _ in 0..4 {
        let (msg_type, plen) = wire::channel_read_msg(sys, s.in_routing, &mut s.buf);
        if msg_type == 0 && plen == 0 {
            break;
        }
        // Same channel as the placement update on purpose — see the
        // note at `session_processor`'s drain. Both modules resolve
        // ownership, so both must apply the map, and through the SAME
        // core parser rather than a second one that could drift.
        if msg_type == wire::MSG_SHARD_MAP_UPDATE {
            if edge::apply_shard_map_update(&mut s.view.overrides, &s.buf[..plen as usize])
                == edge::PlacementUpdate::Stale
            {
                s.routing_stale = s.routing_stale.wrapping_add(1);
            }
            continue;
        }
        if msg_type != wire::MSG_PLACEMENT_UPDATE {
            continue;
        }
        let pl = plen as usize;
        if pl < 4 {
            continue;
        }
        // Parsing, the monotone epoch gate and the optional tails all
        // live in the core, so `session_processor` reads the same frame
        // the same way rather than growing a second parser.
        match edge::apply_placement_update(&mut s.view.view, &s.buf[..pl]) {
            edge::PlacementUpdate::Applied => {}
            edge::PlacementUpdate::Stale => {
                s.routing_stale = s.routing_stale.wrapping_add(1);
            }
            edge::PlacementUpdate::Malformed => {}
        }
    }
}

/// The PRG owning a topic, through the SAME routing key as everything
/// else (`wire::shard_mqtt_topic`) and the SAME divisor as the core.
///
/// One derivation, one answer. A private hash here — mixing the tenant
/// and the topic differently from `partition_router`, say — lets the
/// router place a publish on one PRG while this module expects it on
/// another, and the subscriber then receives nothing.
fn topic_to_prg(view: &edge::EdgeMap, tenant: u32, topic: &[u8]) -> u16 {
    view.owner_prg(wire::shard_mqtt_topic(tenant, topic))
}

/// True when a subscription pattern contains an MQTT wildcard.
///
/// A wildcard subscription cannot be owned by one topic's shard — it
/// spans topics that hash to many PRGs. So it is kept LOCAL to every
/// PRG, and each PRG matches it against the topics it owns. That is
/// both correct and the cheaper arrangement: no PRG has to ship its
/// topics to a remote matcher.
fn is_wildcard(pattern: &[u8]) -> bool {
    let mut i = 0usize;
    while i < pattern.len() {
        if pattern[i] == b'+' || pattern[i] == b'#' {
            return true;
        }
        i += 1;
    }
    false
}

/// Deliver one publish to the matched subscription in slot `i`.
///
/// The publish is the `MSG_TOPIC_PUBLISH` body in `s.buf[..plen]`,
/// with its topic at `s.buf[8..8 + topic_len]`. Shared-group election
/// runs first: at most one member per group receives a given publish,
/// tracked in `shared_delivered` across every match of that publish.
///
/// # Safety
///
/// Caller must supply a valid `&SyscallTable` per the module ABI.
#[expect(
    clippy::too_many_arguments,
    reason = "one publish's decoded header, passed by value"
)]
unsafe fn deliver_match(
    s: &mut ModuleState,
    sys: &SyscallTable,
    i: usize,
    tenant: u32,
    pub_qos: u8,
    plen: usize,
    topic_hash: u64,
    shared_delivered: &mut [bool; MAX_SHARED_GROUPS],
) {
    // Shared subscription: deliver to exactly one member per group
    if s.subs[i].shared_group_id != 0 {
        let mut g_idx: Option<usize> = None;
        for g in 0..MAX_SHARED_GROUPS {
            if s.shared[g].active == 1
                && s.shared[g].group_id == s.subs[i].shared_group_id
                && s.shared[g].tenant == tenant
            {
                g_idx = Some(g);
                break;
            }
        }
        if let Some(g) = g_idx {
            if shared_delivered[g] {
                return;
            }
            let mc = s.shared[g].member_count;
            if mc == 0 {
                return;
            }
            let pick = shared_member_index(topic_hash, s.last_emit_index, mc, s.shared_hash_seed);
            let chosen = s.shared[g].member_slots[pick as usize];
            if chosen != s.subs[i].session_slot {
                return;
            }
            shared_delivered[g] = true;
            s.shared_deliveries = s.shared_deliveries.wrapping_add(1);
        }
    }

    // Local delivery
    // Deliver iff THIS node holds the subscriber's session.
    //
    // The test is session locality, NOT topic ownership. "Do I own the
    // topic?" (`remote_prg == local_prg`) asks the wrong question: the
    // node that must write to the subscriber's socket is the one
    // holding its SESSION, and at `prg_count > 1` that is usually a
    // different node — a subscriber filtered that way receives nothing.
    //
    // Session locality is also the stricter test, which is what makes
    // it safe: exactly ONE node holds a given session, so exactly one
    // delivers and no duplicate can arise. Every node applies every
    // publish from the shared log, so the holder always sees the
    // publish to match against — no forwarding hop is needed.
    //
    // A wildcard subscription still records `local_prg` and a local
    // session, so it is unaffected.
    if s.subs[i].session_slot == SESSION_SLOT_REMOTE {
        return;
    }
    // Envelope to session_processor:
    //   [session_slot:u32][sub_qos:u8][_pad:u8;3]
    //   [tenant:u32][topic_len:u16][topic][payload]
    // sub_qos = min(pub_qos, granted_sub_qos) per MQTT 3.1.1 §3.8.4.
    // The pub_qos+pad bytes in the incoming MSG_TOPIC_PUBLISH envelope
    // are stripped here so the downstream layout stays compact.
    let effective_qos = pub_qos.min(s.subs[i].qos);
    // Strip 2 bytes (pub_qos + pad) from the input.
    let downstream_payload_len = plen - 2;
    let total = 8 + downstream_payload_len;
    if total > s.out_buf.len() {
        return;
    }
    s.out_buf[0..4].copy_from_slice(&s.subs[i].session_slot.to_le_bytes());
    s.out_buf[4] = effective_qos;
    s.out_buf[5..8].fill(0);
    s.out_buf[8..12].copy_from_slice(&s.buf[0..4]);
    s.out_buf[12..total].copy_from_slice(&s.buf[6..plen]);
    // The port is the subscriber's worker's; a second worker's port that
    // is not wired falls back to the first, which is the one-worker
    // graph's behaviour exactly.
    let out_chan = if s.subs[i].worker != 0 && s.out_deliver2 >= 0 {
        s.out_deliver2
    } else {
        s.out_deliver
    };
    if out_chan >= 0 {
        // Skip the pre-write channel_poll: it races with the downstream
        // drain and silently drops deliveries when poll says full but
        // write would have succeeded. Use the write result directly;
        // failure here counts as a real backpressure event the operator
        // should see.
        let written =
            wire::channel_write_msg(sys, out_chan, wire::MSG_TOPIC_DELIVER, &s.out_buf[..total]);
        let expected = (wire::ENVELOPE_HDR + total) as i32;
        if written == expected {
            s.deliveries = s.deliveries.wrapping_add(1);
            dev_log(sys, 3, b"[topic] DELIVER".as_ptr(), 15);
        } else {
            s.deliveries_failed = s.deliveries_failed.wrapping_add(1);
        }
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

/// PIC module ABI entry: one-time process-wide init, before any instance
/// exists.
///
/// # Safety
/// `syscalls` is a kernel-owned table whose function pointers reach live
/// kernel routines for the lifetime of the process.
#[no_mangle]
#[link_section = ".text.module_init"]
pub unsafe extern "C" fn module_init(_syscalls: *const c_void) {}

/// PIC module ABI entry: construct module state in `state` (kernel-allocated
/// from the manifest-declared `state_size`).
///
/// # Safety
/// `state` / `params` / `syscalls` are kernel-owned buffers passed across the
/// module ABI. The kernel guarantees `state` is at least `state_size` bytes,
/// `params` is at least `params_len` bytes, and `state` is zero-initialised.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<ModuleState>() {
            return -2;
        }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_op = in_chan;
        s.out_deliver = out_chan;
        s.out_metrics = dev_channel_port(sys, 1, 1);
        // Declared LAST in the manifest so nothing above renumbers.
        s.out_deliver2 = dev_channel_port(sys, 1, 2);
        s.moves = 0;
        s.view = edge::EdgeMap::new(0, 0);
        s.routing_fenced = 0;
        s.routing_stale = 0;
        s.in_routing = dev_channel_port(sys, 0, 1);
        s.shared_hash_seed = 0x51_EC_A5_E1; // versioned
        for i in 0..MAX_SUBS {
            s.subs[i] = Subscription::zero();
        }
        index_reset(s);
        for i in 0..MAX_SHARED_GROUPS {
            s.shared[i] = SharedGroup::zero();
        }
        dev_log(sys, 3, b"[topic] init".as_ptr(), 12);
        0
    }
}

/// PIC module ABI entry: run one scheduler step against this instance.
///
/// # Safety
/// `state` is the kernel-owned buffer a prior `module_new` initialised, and is
/// exclusively borrowed for the duration of the call.
#[no_mangle]
#[link_section = ".text.module_step"]
pub unsafe extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let mut worked = 0u32;
        let now = dev_millis(sys);

        // Placement first: a publish handled this step must be stamped
        // with the epoch the control plane has already announced, not
        // the previous one.
        drain_routing(s, sys);

        // Drain op channel — SUBSCRIBE, UNSUBSCRIBE and PUBLISH come on the
        // same port, demuxed by msg_type. This matches
        // session_processor.topic_out → topic_engine.op_in.
        if s.in_op >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_op, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = {
                    worked += 1;
                    wire::channel_read_msg(sys, s.in_op, &mut s.buf)
                };
                dev_log(sys, 3, b"[topic] op rx".as_ptr(), 13);
                if plen == 0 {
                    continue;
                }

                if mt == wire::MSG_TOPIC_SUBSCRIBE {
                    dev_log(sys, 3, b"[topic] SUB".as_ptr(), 11);
                    if plen < 12 {
                        continue;
                    }
                    let plen = plen as usize;

                    let tenant = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                    let session = u32::from_le_bytes([s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);
                    let qos = s.buf[8];
                    // The worker that holds the subscriber's session.
                    let worker = s.buf[9];
                    let pat_len = u16::from_le_bytes([s.buf[10], s.buf[11]]) as usize;
                    if 12 + pat_len > plen || pat_len > MAX_TOPIC {
                        continue;
                    }
                    let pattern = &s.buf[12..12 + pat_len];
                    // `stream_hash` is appended after the pattern. A
                    // record without it is from a build that predates
                    // cross-PRG delivery; treat it as 0, which resolves
                    // to no remote subscriber rather than a wrong one.
                    let stream_hash = if 12 + pat_len + 8 <= plen {
                        let b = &s.buf[12 + pat_len..12 + pat_len + 8];
                        u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
                    } else {
                        0
                    };

                    // Check for MQTT 5 shared subscription prefix
                    let (group_id, strip_offset) = parse_shared_prefix(pattern);
                    let effective_pat = &pattern[strip_offset..];
                    let effective_len = effective_pat.len();

                    // Subscription ownership. An exact-topic
                    // subscription belongs to the PRG that owns that
                    // topic's shard, so a publish reaching that PRG can
                    // find its subscribers without a broadcast.
                    //
                    // A WILDCARD subscription cannot belong to one
                    // shard — it spans topics that hash to many PRGs —
                    // so it stays local to every PRG and each matches
                    // it against the topics it owns. `local_prg`
                    // records that deliberately, so the field carries
                    // the placement rather than a constant.
                    let remote_prg = if is_wildcard(effective_pat) {
                        s.view.view.local_prg
                    } else {
                        topic_to_prg(&s.view, tenant, effective_pat)
                    };

                    let Some(i) = index_alloc(s) else {
                        continue;
                    };
                    let effective_pat = &s.buf[12 + strip_offset..12 + pat_len];
                    s.subs[i].tenant = tenant;
                    s.subs[i].session_slot = session;
                    s.subs[i].stream_hash = stream_hash;
                    s.subs[i].qos = qos;
                    s.subs[i].shared_group_id = group_id;
                    s.subs[i].remote_prg = remote_prg;
                    s.subs[i].worker = worker;
                    s.subs[i].pattern_len = effective_len as u16;
                    s.subs[i].pattern[..effective_len].copy_from_slice(effective_pat);
                    s.subs[i].active = 1;
                    s.subs_count = s.subs_count.wrapping_add(1);
                    index_link(s, i);

                    // Add to shared group membership
                    if group_id != 0 {
                        let mut found = false;
                        for g in 0..MAX_SHARED_GROUPS {
                            if s.shared[g].active == 1
                                && s.shared[g].group_id == group_id
                                && s.shared[g].tenant == tenant
                            {
                                let mut already_member = false;
                                for i in 0..s.shared[g].member_count as usize {
                                    if s.shared[g].member_slots[i] == session {
                                        already_member = true;
                                        break;
                                    }
                                }
                                if !already_member
                                    && (s.shared[g].member_count as usize)
                                        < s.shared[g].member_slots.len()
                                {
                                    s.shared[g].member_slots[s.shared[g].member_count as usize] =
                                        session;
                                    s.shared[g].member_count += 1;
                                }
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            for g in 0..MAX_SHARED_GROUPS {
                                if s.shared[g].active == 0 {
                                    s.shared[g] = SharedGroup {
                                        group_id,
                                        tenant,
                                        member_slots: [0; 16],
                                        member_count: 1,
                                        next_round_robin: 0,
                                        active: 1,
                                    };
                                    s.shared[g].member_slots[0] = session;
                                    break;
                                }
                            }
                        }
                    }
                } else if mt == wire::MSG_TOPIC_UNSUBSCRIBE {
                    if plen < 10 {
                        continue;
                    }
                    let plen = plen as usize;

                    let tenant = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                    let session = u32::from_le_bytes([s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);
                    let pat_len = u16::from_le_bytes([s.buf[8], s.buf[9]]) as usize;
                    if 10 + pat_len > plen || pat_len > MAX_TOPIC {
                        continue;
                    }
                    let pattern = &s.buf[10..10 + pat_len];

                    let (group_id, strip_offset) = parse_shared_prefix(pattern);
                    // Copied out of `s.buf` so the chain walk below can
                    // retire slots through `&mut s`.
                    let effective_len = pat_len - strip_offset;
                    let mut pat_copy = [0u8; MAX_TOPIC];
                    pat_copy[..effective_len].copy_from_slice(&pattern[strip_offset..]);
                    let effective_pat = &pat_copy[..effective_len];

                    // Only the chain this pattern lives on can hold it.
                    // Every match is retired, so a session that
                    // subscribed the same filter twice is left with
                    // none of them.
                    let mut cur = if is_wildcard(effective_pat) {
                        s.wild_head[wild_bucket(tenant)]
                    } else {
                        s.exact_head[exact_bucket(tenant, wire::fnv1a_64(effective_pat))]
                    };
                    let mut removed = false;
                    while cur != 0 {
                        let i = cur as usize - 1;
                        cur = s.next[i];
                        let sub = &s.subs[i];
                        if sub.tenant != tenant
                            || sub.session_slot != session
                            || sub.shared_group_id != group_id
                            || sub.pattern_len as usize != effective_len
                            || &sub.pattern[..effective_len] != effective_pat
                        {
                            continue;
                        }
                        retire_subscription(s, i);
                        removed = true;
                    }

                    if group_id != 0
                        && removed
                        && !session_has_shared_subscription(&s.subs, tenant, group_id, session)
                    {
                        remove_shared_member(&mut s.shared, tenant, group_id, session);
                    }
                } else if mt == wire::MSG_TOPIC_PUBLISH {
                    dev_log(sys, 3, b"[topic] PUB".as_ptr(), 11);
                    // ── PUBLISH [tenant:u32][pub_qos:u8][_pad:u8][topic_len:u16][topic][payload] ──
                    if plen < 8 {
                        continue;
                    }
                    let plen = plen as usize;
                    let tenant = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                    let pub_qos = s.buf[4] & 0x03;
                    let topic_len = u16::from_le_bytes([s.buf[6], s.buf[7]]) as usize;
                    if 8 + topic_len > plen {
                        continue;
                    }
                    let topic_hash = wire::fnv1a_64(&s.buf[8..8 + topic_len]);
                    s.publishes = s.publishes.wrapping_add(1);
                    s.last_emit_index = s.last_emit_index.wrapping_add(1);

                    // Routing disposition, from the shared edge core so
                    // ownership, epoch and fence are decided in one
                    // place for every listener rather than by a local
                    // `!=` test. Decided BEFORE the fan-out below: a
                    // fenced shard must not be delivered locally either,
                    // or the fence holds only on the forward half and
                    // ownership splits anyway.
                    let egress_prg = topic_to_prg(&s.view, tenant, &s.buf[8..8 + topic_len]);
                    // A locally-originated publish states no epoch, so
                    // the sender-epoch arms never fire here; they carry
                    // the decision once forwarded frames arrive with the
                    // epoch they were routed under.
                    let action = edge::classify(&s.view.view, egress_prg, 0);
                    if action == edge::EdgeAction::Fenced {
                        // Mid-transfer: the losing owner has stopped
                        // accepting and the gaining one has not started.
                        // Refusing is the correct answer — QoS 0 drops,
                        // QoS 1+ goes unacknowledged and the client
                        // retries once the fence lifts.
                        s.routing_fenced = s.routing_fenced.wrapping_add(1);
                        continue;
                    }

                    // Deliver to matching local subscribers, track shared group exclusivity
                    let mut shared_delivered = [false; MAX_SHARED_GROUPS];
                    let mut match_count = 0u32;

                    // Exact subscriptions: the one bucket for (tenant,
                    // topic). An exact pattern matches by byte equality
                    // and nothing else, which is what a hash lookup
                    // answers — but the hash is a hint, so tenant and
                    // bytes are compared on every chain entry.
                    let mut cur = s.exact_head[exact_bucket(tenant, topic_hash)];
                    while cur != 0 {
                        let i = cur as usize - 1;
                        cur = s.next[i];
                        let sub = &s.subs[i];
                        if sub.tenant != tenant
                            || sub.pattern_len as usize != topic_len
                            || sub.pattern[..topic_len] != s.buf[8..8 + topic_len]
                        {
                            continue;
                        }
                        match_count += 1;
                        deliver_match(
                            s,
                            sys,
                            i,
                            tenant,
                            pub_qos,
                            plen,
                            topic_hash,
                            &mut shared_delivered,
                        );
                    }

                    // Wildcard subscriptions: the tenant's chain, each
                    // entry through the full matcher.
                    let mut cur = s.wild_head[wild_bucket(tenant)];
                    while cur != 0 {
                        let i = cur as usize - 1;
                        cur = s.next[i];
                        let sub = &s.subs[i];
                        if sub.tenant != tenant {
                            continue;
                        }
                        let pat = &sub.pattern[..sub.pattern_len as usize];
                        if !wire::mqtt_topic_match(pat, &s.buf[8..8 + topic_len]) {
                            continue;
                        }
                        match_count += 1;
                        deliver_match(
                            s,
                            sys,
                            i,
                            tenant,
                            pub_qos,
                            plen,
                            topic_hash,
                            &mut shared_delivered,
                        );
                    }

                    if match_count == 0 {
                        dev_log(sys, 3, b"[topic] no match".as_ptr(), 16);
                    }

                    // end MSG_TOPIC_PUBLISH branch
                } else if mt == wire::MSG_SESSION_DROP {
                    // Body: [session_slot:u32 LE]. Purge every subscription
                    // anchored to this slot so a future client reusing the
                    // numeric slot doesn't inherit the prior client's
                    // delivery routes. A session holds no index of its
                    // own, so this is the one op that sweeps every slot.
                    if plen >= 4 {
                        let slot = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                        for i in 0..MAX_SUBS {
                            if s.subs[i].active == 1 && s.subs[i].session_slot == slot {
                                let tenant = s.subs[i].tenant;
                                let group_id = s.subs[i].shared_group_id;
                                retire_subscription(s, i);
                                if group_id != 0
                                    && !session_has_shared_subscription(
                                        &s.subs, tenant, group_id, slot,
                                    )
                                {
                                    remove_shared_member(&mut s.shared, tenant, group_id, slot);
                                }
                            }
                        }
                    }
                } else if mt == wire::MSG_TOPIC_MOVE {
                    // Body: [session_slot:u32 LE][worker:u8]. The session
                    // kept its slot number and changed worker; every
                    // subscription anchored to the slot now delivers on
                    // that worker's port. Nothing else about the
                    // subscription changes, so a publish matched during
                    // the move is delivered exactly once — to whichever
                    // worker the slot pointed at when it was matched.
                    if plen >= 5 {
                        let slot = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                        let worker = s.buf[4];
                        for i in 0..MAX_SUBS {
                            if s.subs[i].active == 1 && s.subs[i].session_slot == slot {
                                s.subs[i].worker = worker;
                                s.moves = s.moves.wrapping_add(1);
                            }
                        }
                    }
                } else if mt == wire::MSG_APPLY_RESET_FANOUT {
                    // Apply-pipeline reset (docs/architecture/apply_path.md
                    // §Apply-pipeline reset). Wipe all subscriptions and
                    // shared-group state. Snapshot install will repopulate;
                    // until then state rebuilds from the next
                    // MSG_TOPIC_SUBSCRIBE flow.
                    for i in 0..MAX_SUBS {
                        s.subs[i] = Subscription::zero();
                    }
                    index_reset(s);
                    s.subs_count = 0;
                    for i in 0..MAX_SHARED_GROUPS {
                        s.shared[i] = SharedGroup::zero();
                    }
                } // end if mt == MSG_TOPIC_PUBLISH else chain
            } // end for loop
        } // end if s.in_op >= 0

        // Metrics
        if now.wrapping_sub(s.last_metrics_ms) >= 1000 && s.out_metrics >= 0 {
            s.last_metrics_ms = now;
            let mut m = [0u8; 24];
            m[0..4].copy_from_slice(&s.subs_count.to_le_bytes());
            m[4..8].copy_from_slice(&s.publishes.to_le_bytes());
            m[8..12].copy_from_slice(&s.deliveries.to_le_bytes());
            m[12..16].copy_from_slice(&s.shared_deliveries.to_le_bytes());
            // Routing health. `fenced` should be non-zero only while a
            // shard is mid-transfer; `stale` counts placement updates
            // ignored as not-newer, which rising steadily means the
            // control plane is republishing an epoch it already sent.
            m[16..20].copy_from_slice(&s.routing_fenced.to_le_bytes());
            m[20..24].copy_from_slice(&s.routing_stale.to_le_bytes());
            let p = (sys.channel_poll)(s.out_metrics, 0x02);
            if p > 0 && (p as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, s.out_metrics, wire::MSG_METRICS, &m);
            }
        }

        if worked > 0 {
            STEP_BURST
        } else {
            0
        }
    }
}
