//! Consumer Group Coordinator — Kafka-style consumer groups.
//!
//! Tracks group membership, rebalance state, and partition assignments.
//! Heartbeat monitoring with configurable session timeout.

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
include!("../../../target/fluxor/fluxor-abi/sdk/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/types.rs"]
mod types;

use types::*;

const MAX_GROUPS: usize = 128;
const MAX_MEMBERS_PER_GROUP: usize = 32;

#[repr(C)]
#[derive(Clone, Copy)]
struct Member {
    session_slot: u32,
    last_heartbeat_ms: u64,
    active: u8,
}

impl Member {
    const fn zero() -> Self {
        Self { session_slot: 0, last_heartbeat_ms: 0, active: 0 }
    }
}

const MAX_PARTITIONS: u16 = 128;

#[repr(C)]
#[derive(Clone, Copy)]
struct Group {
    tenant: u32,
    group_hash: u64,
    state: u8,
    generation: u32,
    partition_count: u16,
    members: [Member; MAX_MEMBERS_PER_GROUP],
    // Assignments: for each partition p, members[assignments[p]] owns it.
    // 0xFF means unassigned.
    assignments: [u8; 128],
    active: u8,
}

impl Group {
    const fn zero() -> Self {
        Self {
            tenant: 0, group_hash: 0, state: GROUP_EMPTY, generation: 0,
            partition_count: 16,
            members: [Member::zero(); MAX_MEMBERS_PER_GROUP],
            assignments: [0xFF; 128],
            active: 0,
        }
    }

    /// Range assignment: divide partitions contiguously among active members.
    /// Members are ordered by session_slot for stability across rebalances.
    fn assign_range(&mut self) {
        // Collect active member indices sorted by session_slot
        let mut active_idx = [0u8; MAX_MEMBERS_PER_GROUP];
        let mut n_active = 0u8;
        for i in 0..MAX_MEMBERS_PER_GROUP {
            if self.members[i].active == 1 {
                active_idx[n_active as usize] = i as u8;
                n_active += 1;
            }
        }
        if n_active == 0 {
            for p in 0..128 { self.assignments[p] = 0xFF; }
            return;
        }

        // Insertion sort by session_slot
        for i in 1..n_active as usize {
            let key = active_idx[i];
            let ks = self.members[key as usize].session_slot;
            let mut j = i;
            while j > 0 && self.members[active_idx[j - 1] as usize].session_slot > ks {
                active_idx[j] = active_idx[j - 1];
                j -= 1;
            }
            active_idx[j] = key;
        }

        let pc = self.partition_count as usize;
        let base = pc / n_active as usize;
        let rem = pc % n_active as usize;
        let mut cur = 0usize;
        for m in 0..n_active as usize {
            let size = base + if m < rem { 1 } else { 0 };
            let end = cur + size;
            for p in cur..end.min(128) {
                self.assignments[p] = active_idx[m];
            }
            cur = end;
        }
        for p in cur..128 {
            self.assignments[p] = 0xFF;
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_op: i32,
    out_assign: i32,
    out_metrics: i32,

    session_timeout_ms: u32,
    rebalance_timeout_ms: u32,
    heartbeat_interval_ms: u32,

    groups: [Group; MAX_GROUPS],
    ops_processed: u32,
    rebalances: u32,
    last_scan_ms: u64,
    buf: [u8; 256],
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 { core::mem::size_of::<ModuleState>() as u32 }

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32, out_chan: i32, _ctrl_chan: i32,
    _params: *const u8, _params_len: usize,
    state: *mut u8, state_size: usize, syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() { return -1; }
        if state_size < core::mem::size_of::<ModuleState>() { return -2; }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_op = in_chan;
        s.out_assign = out_chan;
        s.out_metrics = dev_channel_port(sys, 1, 1);
        s.session_timeout_ms = 10_000;
        s.rebalance_timeout_ms = 60_000;
        s.heartbeat_interval_ms = 3_000;
        for i in 0..MAX_GROUPS {
            s.groups[i] = Group::zero();
        }
        dev_log(sys, 3, b"[grp] init".as_ptr(), 9);
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // Drain group ops: [op_type:u8][tenant:u32][group_hash:u64][session:u32]
        if s.in_op >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_op, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_op, &mut s.buf);
                // op_in is a shared bus from session_processor.messaging_out;
                // ignore any message that isn't a group op.
                if mt != wire::MSG_GROUP_OP { continue; }
                if plen < 17 { continue; }

                let op_type = s.buf[0];
                let tenant = u32::from_le_bytes([s.buf[1], s.buf[2], s.buf[3], s.buf[4]]);
                let group_hash = u64::from_le_bytes([s.buf[5], s.buf[6], s.buf[7], s.buf[8], s.buf[9], s.buf[10], s.buf[11], s.buf[12]]);
                let session = u32::from_le_bytes([s.buf[13], s.buf[14], s.buf[15], s.buf[16]]);

                s.ops_processed = s.ops_processed.wrapping_add(1);

                // Find or create group
                let mut group_idx: Option<usize> = None;
                for i in 0..MAX_GROUPS {
                    if s.groups[i].active == 1 && s.groups[i].tenant == tenant && s.groups[i].group_hash == group_hash {
                        group_idx = Some(i);
                        break;
                    }
                }
                if group_idx.is_none() && op_type != 2 {  // not Leave
                    for i in 0..MAX_GROUPS {
                        if s.groups[i].active == 0 {
                            s.groups[i] = Group {
                                tenant, group_hash,
                                state: GROUP_PREPARING, generation: 1,
                                partition_count: MAX_PARTITIONS,
                                members: [Member::zero(); MAX_MEMBERS_PER_GROUP],
                                assignments: [0xFF; 128],
                                active: 1,
                            };
                            group_idx = Some(i);
                            break;
                        }
                    }
                }

                if let Some(gi) = group_idx {
                    let mut changed = false;
                    match op_type {
                        0 => {  // Join
                            for mi in 0..MAX_MEMBERS_PER_GROUP {
                                if s.groups[gi].members[mi].active == 0 {
                                    s.groups[gi].members[mi] = Member {
                                        session_slot: session, last_heartbeat_ms: now, active: 1,
                                    };
                                    s.groups[gi].generation = s.groups[gi].generation.wrapping_add(1);
                                    s.groups[gi].state = GROUP_REBALANCING;
                                    s.rebalances = s.rebalances.wrapping_add(1);
                                    changed = true;
                                    break;
                                }
                            }
                        }
                        1 => {  // Heartbeat
                            for mi in 0..MAX_MEMBERS_PER_GROUP {
                                if s.groups[gi].members[mi].active == 1
                                    && s.groups[gi].members[mi].session_slot == session
                                {
                                    s.groups[gi].members[mi].last_heartbeat_ms = now;
                                    break;
                                }
                            }
                        }
                        2 => {  // Leave
                            for mi in 0..MAX_MEMBERS_PER_GROUP {
                                if s.groups[gi].members[mi].active == 1
                                    && s.groups[gi].members[mi].session_slot == session
                                {
                                    s.groups[gi].members[mi].active = 0;
                                    s.groups[gi].generation = s.groups[gi].generation.wrapping_add(1);
                                    changed = true;
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }

                    // Recompute partition assignments on membership change
                    if changed {
                        s.groups[gi].assign_range();
                        s.groups[gi].state = GROUP_STABLE;

                        // Emit one assignment per member with their partition range
                        let gen = s.groups[gi].generation;
                        let pc = s.groups[gi].partition_count as usize;
                        for mi in 0..MAX_MEMBERS_PER_GROUP {
                            if s.groups[gi].members[mi].active == 0 { continue; }
                            let member_session = s.groups[gi].members[mi].session_slot;
                            // Build assignment: [tenant:u32][group_hash:u64][generation:u32]
                            //                  [session:u32][partition_count:u16][partitions...]
                            let mut a = [0u8; 256];
                            a[0..4].copy_from_slice(&tenant.to_le_bytes());
                            a[4..12].copy_from_slice(&group_hash.to_le_bytes());
                            a[12..16].copy_from_slice(&gen.to_le_bytes());
                            a[16..20].copy_from_slice(&member_session.to_le_bytes());
                            let mut pidx_out = 22usize;
                            let mut pcnt = 0u16;
                            for p in 0..pc.min(128) {
                                if s.groups[gi].assignments[p] == mi as u8 {
                                    if pidx_out + 2 <= a.len() {
                                        a[pidx_out..pidx_out + 2].copy_from_slice(&(p as u16).to_le_bytes());
                                        pidx_out += 2;
                                        pcnt += 1;
                                    }
                                }
                            }
                            a[20..22].copy_from_slice(&pcnt.to_le_bytes());
                            if s.out_assign >= 0 {
                                let poll_out = (sys.channel_poll)(s.out_assign, 0x02);
                                if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                                    wire::channel_write_msg(sys, s.out_assign, wire::MSG_GROUP_ASSIGN, &a[..pidx_out]);
                                }
                            }
                        }
                    } else {
                        // Simple heartbeat ack: [tenant:u32][group_hash:u64][gen:u32][session:u32]
                        let mut a = [0u8; 20];
                        a[0..4].copy_from_slice(&tenant.to_le_bytes());
                        a[4..12].copy_from_slice(&group_hash.to_le_bytes());
                        a[12..16].copy_from_slice(&s.groups[gi].generation.to_le_bytes());
                        a[16..20].copy_from_slice(&session.to_le_bytes());
                        if s.out_assign >= 0 {
                            let poll_out = (sys.channel_poll)(s.out_assign, 0x02);
                            if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                                wire::channel_write_msg(sys, s.out_assign, wire::MSG_GROUP_ASSIGN, &a);
                            }
                        }
                    }
                }
            }
        }

        // Session timeout scan
        if now.wrapping_sub(s.last_scan_ms) >= 1000 {
            s.last_scan_ms = now;
            for gi in 0..MAX_GROUPS {
                if s.groups[gi].active == 0 { continue; }
                let mut expired = false;
                for mi in 0..MAX_MEMBERS_PER_GROUP {
                    let m = &mut s.groups[gi].members[mi];
                    if m.active == 1 && now.wrapping_sub(m.last_heartbeat_ms) >= s.session_timeout_ms as u64 {
                        m.active = 0;
                        expired = true;
                    }
                }
                if expired {
                    s.groups[gi].generation = s.groups[gi].generation.wrapping_add(1);
                    s.rebalances = s.rebalances.wrapping_add(1);
                    s.groups[gi].assign_range();
                    s.groups[gi].state = GROUP_STABLE;
                }
            }

            if s.out_metrics >= 0 {
                let mut m = [0u8; 8];
                m[0..4].copy_from_slice(&s.ops_processed.to_le_bytes());
                m[4..8].copy_from_slice(&s.rebalances.to_le_bytes());
                let poll = (sys.channel_poll)(s.out_metrics, 0x02);
                if poll > 0 && (poll as u32 & 0x02) != 0 {
                    wire::channel_write_msg(sys, s.out_metrics, wire::MSG_METRICS, &m);
                }
            }
        }

        0
    }
}
