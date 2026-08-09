//! Topic Engine — Subscription index + publish routing.
//!
//! Merges subscription management and publish fan-out. Handles:
//! - MQTT wildcard matching (+, #)
//! - Shared subscription hashing (stable versioned seed)
//! - Cross-PRG forward emission (forward_seq idempotence)
//! - Per-PRG last_emit_index tracking

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

/// Kernel step ABI: 0=Continue, 1=Done, 2=Burst, 3=Ready. Returning
/// Burst re-runs the domain's exec rotation within the same tick (up to
/// the kernel's pass cap), so a record consumed here reaches its
/// consumer in this tick instead of the next. We burst only when a
/// record was actually consumed: an idle graph reports no burst and the
/// tick costs exactly one pass, as before.
const STEP_BURST: i32 = 2;

const MAX_SUBS: usize = 2048;
const MAX_TOPIC: usize = 256;
const MAX_SHARED_GROUPS: usize = 64;
/// Read/write buffer cap for topic ops. Sized to the wire-channel
/// per-message capacity (`fluxor-abi::CHANNEL_BUFFER_SIZE` = 8192) so
/// a worst-case MQTT publish (`protocol::mqtt`'s `MAX_PACKET` = 4096) plus
/// `MSG_TOPIC_PUBLISH` framing (tenant + topic_len + topic prefix) and
/// the cross-PRG forward header (`[ingress_prg][egress_prg][epoch]`)
/// can all flow without hitting the `channel_read_msg` discard path.
const MAX_TOPIC_MSG: usize = 8192;

// Partition count per tenant (for PRG placement).
const PRG_COUNT: u16 = 16;

#[repr(C)]
#[derive(Clone, Copy)]
struct Subscription {
    tenant: u32,
    session_slot: u32,
    pattern: [u8; MAX_TOPIC],
    pattern_len: u16,
    qos: u8,
    shared_group_id: u16, // 0 = not shared
    remote_prg: u16,      // 0 = local, >0 = cross-PRG
    active: u8,
}

impl Subscription {
    const fn zero() -> Self {
        Self {
            tenant: 0,
            session_slot: 0,
            pattern: [0; MAX_TOPIC],
            pattern_len: 0,
            qos: 0,
            shared_group_id: 0,
            remote_prg: 0,
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
    out_forward: i32,
    out_metrics: i32,

    local_prg: u16,
    shared_hash_seed: u32,

    subs: [Subscription; MAX_SUBS],
    shared: [SharedGroup; MAX_SHARED_GROUPS],

    subs_count: u32,
    publishes: u32,
    deliveries: u32,
    /// Count of subscriber deliveries that the downstream channel
    /// rejected (full ring). Without this, the silent-skip would mask
    /// post-durability subscriber-side data loss.
    deliveries_failed: u32,
    shared_deliveries: u32,
    forwards: u32,
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

fn topic_to_prg(tenant: u32, topic_hash: u64, prg_count: u16) -> u16 {
    let h = wire::fnv1a_64(&tenant.to_le_bytes()).wrapping_add(topic_hash);
    (h % prg_count as u64) as u16
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
        s.out_forward = dev_channel_port(sys, 1, 1);
        s.out_metrics = dev_channel_port(sys, 1, 2);
        s.local_prg = 0;
        s.shared_hash_seed = 0x51_EC_A5_E1; // versioned
        for i in 0..MAX_SUBS {
            s.subs[i] = Subscription::zero();
        }
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
                    let _shared_flag = s.buf[9];
                    let pat_len = u16::from_le_bytes([s.buf[10], s.buf[11]]) as usize;
                    if 12 + pat_len > plen || pat_len > MAX_TOPIC {
                        continue;
                    }
                    let pattern = &s.buf[12..12 + pat_len];

                    // Check for MQTT 5 shared subscription prefix
                    let (group_id, strip_offset) = parse_shared_prefix(pattern);
                    let effective_pat = &pattern[strip_offset..];
                    let effective_len = effective_pat.len();

                    // Find subscription slot
                    let mut inserted = false;
                    for i in 0..MAX_SUBS {
                        if s.subs[i].active == 0 {
                            s.subs[i].tenant = tenant;
                            s.subs[i].session_slot = session;
                            s.subs[i].qos = qos;
                            s.subs[i].shared_group_id = group_id;
                            s.subs[i].remote_prg = 0; // local placeholder
                            s.subs[i].pattern_len = effective_len as u16;
                            s.subs[i].pattern[..effective_len].copy_from_slice(effective_pat);
                            s.subs[i].active = 1;
                            s.subs_count = s.subs_count.wrapping_add(1);
                            inserted = true;
                            break;
                        }
                    }
                    if !inserted {
                        continue;
                    }

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
                    let effective_pat = &pattern[strip_offset..];
                    let effective_len = effective_pat.len();

                    let mut removed = false;
                    for i in 0..MAX_SUBS {
                        if s.subs[i].active == 0
                            || s.subs[i].tenant != tenant
                            || s.subs[i].session_slot != session
                            || s.subs[i].shared_group_id != group_id
                            || s.subs[i].pattern_len as usize != effective_len
                        {
                            continue;
                        }
                        if &s.subs[i].pattern[..effective_len] != effective_pat {
                            continue;
                        }
                        s.subs[i] = Subscription::zero();
                        s.subs_count = s.subs_count.saturating_sub(1);
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

                    // Deliver to matching local subscribers, track shared group exclusivity
                    let mut shared_delivered = [false; MAX_SHARED_GROUPS];
                    let mut match_count = 0u32;
                    for i in 0..MAX_SUBS {
                        if s.subs[i].active == 0 || s.subs[i].tenant != tenant {
                            continue;
                        }
                        let pat = &s.subs[i].pattern[..s.subs[i].pattern_len as usize];
                        let topic = &s.buf[8..8 + topic_len];
                        if !wire::mqtt_topic_match(pat, topic) {
                            continue;
                        }
                        match_count += 1;

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
                                    continue;
                                }
                                let mc = s.shared[g].member_count;
                                if mc == 0 {
                                    continue;
                                }
                                let pick = shared_member_index(
                                    topic_hash,
                                    s.last_emit_index,
                                    mc,
                                    s.shared_hash_seed,
                                );
                                let chosen = s.shared[g].member_slots[pick as usize];
                                if chosen != s.subs[i].session_slot {
                                    continue;
                                }
                                shared_delivered[g] = true;
                                s.shared_deliveries = s.shared_deliveries.wrapping_add(1);
                            }
                        }

                        // Local delivery
                        if s.subs[i].remote_prg == 0 || s.subs[i].remote_prg == s.local_prg {
                            // Envelope to session_processor:
                            //   [session_slot:u32][sub_qos:u8][_pad:u8;3]
                            //   [tenant:u32][topic_len:u16][topic][payload]
                            // sub_qos = min(pub_qos, granted_sub_qos) per
                            // MQTT 3.1.1 §3.8.4. The pub_qos+pad bytes in the
                            // incoming MSG_TOPIC_PUBLISH envelope are stripped
                            // here so the downstream layout stays compact.
                            let effective_qos = pub_qos.min(s.subs[i].qos);
                            // Strip 2 bytes (pub_qos + pad) from the input.
                            let downstream_payload_len = plen - 2;
                            let total = 8 + downstream_payload_len;
                            if total > s.out_buf.len() {
                                continue;
                            }
                            s.out_buf[0..4].copy_from_slice(&s.subs[i].session_slot.to_le_bytes());
                            s.out_buf[4] = effective_qos;
                            s.out_buf[5..8].fill(0);
                            s.out_buf[8..12].copy_from_slice(&s.buf[0..4]);
                            s.out_buf[12..total].copy_from_slice(&s.buf[6..plen]);
                            if s.out_deliver >= 0 {
                                // Skip the pre-write channel_poll: it races
                                // with the downstream drain and silently drops
                                // deliveries when poll says full but write
                                // would have succeeded. Use the write result
                                // directly; failure here counts as a real
                                // backpressure event the operator should see.
                                let written = wire::channel_write_msg(
                                    sys,
                                    s.out_deliver,
                                    wire::MSG_TOPIC_DELIVER,
                                    &s.out_buf[..total],
                                );
                                let expected = (wire::ENVELOPE_HDR + total) as i32;
                                if written == expected {
                                    s.deliveries = s.deliveries.wrapping_add(1);
                                    dev_log(sys, 3, b"[topic] DELIVER".as_ptr(), 15);
                                } else {
                                    s.deliveries_failed = s.deliveries_failed.wrapping_add(1);
                                }
                            }
                        }
                    }
                    if match_count == 0 {
                        dev_log(sys, 3, b"[topic] no match".as_ptr(), 16);
                    }

                    // Cross-PRG forward emission: compute target PRG from hash
                    let egress_prg = topic_to_prg(tenant, topic_hash, PRG_COUNT);
                    if egress_prg != s.local_prg && s.out_forward >= 0 {
                        // [ingress_prg:u16][egress_prg:u16][routing_epoch:u32][payload...]
                        let header_len = 8;
                        let total = header_len + plen;
                        if total <= s.out_buf.len() {
                            s.out_buf[0..2].copy_from_slice(&s.local_prg.to_le_bytes());
                            s.out_buf[2..4].copy_from_slice(&egress_prg.to_le_bytes());
                            s.out_buf[4..8].copy_from_slice(&1u32.to_le_bytes()); // routing_epoch placeholder
                            s.out_buf[8..total].copy_from_slice(&s.buf[..plen]);
                            let p = (sys.channel_poll)(s.out_forward, 0x02);
                            if p > 0 && (p as u32 & 0x02) != 0 {
                                wire::channel_write_msg(
                                    sys,
                                    s.out_forward,
                                    wire::MSG_FORWARD_REQUEST,
                                    &s.out_buf[..total],
                                );
                                s.forwards = s.forwards.wrapping_add(1);
                            }
                        }
                    }
                    // end MSG_TOPIC_PUBLISH branch
                } else if mt == wire::MSG_SESSION_DROP {
                    // Body: [session_slot:u32 LE]. Purge every subscription
                    // anchored to this slot so a future client reusing the
                    // numeric slot doesn't inherit the prior client's
                    // delivery routes.
                    if plen >= 4 {
                        let slot = u32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                        for i in 0..MAX_SUBS {
                            if s.subs[i].active == 1 && s.subs[i].session_slot == slot {
                                let tenant = s.subs[i].tenant;
                                let group_id = s.subs[i].shared_group_id;
                                s.subs[i] = Subscription::zero();
                                s.subs_count = s.subs_count.saturating_sub(1);
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
                } else if mt == wire::MSG_APPLY_RESET_FANOUT {
                    // Apply-pipeline reset (docs/architecture/apply_path.md
                    // §Apply-pipeline reset). Wipe all subscriptions and
                    // shared-group state. Snapshot install will repopulate;
                    // until then state rebuilds from the next
                    // MSG_TOPIC_SUBSCRIBE flow.
                    for i in 0..MAX_SUBS {
                        s.subs[i] = Subscription::zero();
                    }
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
            let mut m = [0u8; 20];
            m[0..4].copy_from_slice(&s.subs_count.to_le_bytes());
            m[4..8].copy_from_slice(&s.publishes.to_le_bytes());
            m[8..12].copy_from_slice(&s.deliveries.to_le_bytes());
            m[12..16].copy_from_slice(&s.shared_deliveries.to_le_bytes());
            m[16..20].copy_from_slice(&s.forwards.to_le_bytes());
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
