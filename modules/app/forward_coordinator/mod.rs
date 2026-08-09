//! Forward Coordinator — Cross-PRG forwarding with idempotence.
//!
//! Tracks (ingress_prg, egress_prg, routing_epoch) → monotone forward_seq.
//! Same-node forwards emit proposals directly to consensus.
//! Cross-node forwards emit routed envelopes to peer_router.

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

const MAX_FORWARDS: usize = 1024;
const MAX_ROUTES: usize = 256;
/// Read buffer cap for `MSG_FORWARD_REQUEST`. Sized to the wire-channel
/// per-message capacity (`fluxor-abi::CHANNEL_BUFFER_SIZE` = 8192) so a
/// forwarded `MSG_TOPIC_PUBLISH` carrying a worst-case 4 KiB MQTT
/// payload + cross-PRG routing header fits without hitting the
/// `channel_read_msg` discard path.
const MAX_FWD_BUF: usize = 8192;

#[repr(C)]
#[derive(Clone, Copy)]
struct ForwardRoute {
    ingress_prg: u16,
    egress_prg: u16,
    routing_epoch: u32,
    next_seq: u64,
    active: u8,
}

impl ForwardRoute {
    const fn zero() -> Self {
        Self {
            ingress_prg: 0,
            egress_prg: 0,
            routing_epoch: 0,
            next_seq: 1,
            active: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct InflightForward {
    forward_seq: u64,
    egress_prg: u16,
    is_local: u8,
    sent_ms: u64,
    active: u8,
}

impl InflightForward {
    const fn zero() -> Self {
        Self {
            forward_seq: 0,
            egress_prg: 0,
            is_local: 0,
            sent_ms: 0,
            active: 0,
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_forward: i32,
    in_replay: i32,      // in[1]: WAL replay entries for forward_seq reconstruction
    out_local: i32,      // to consensus (same-node)
    out_remote: i32,     // to peer_router (cross-node)
    out_wal_marker: i32, // to consensus for persisting forward state
    out_metrics: i32,

    timeout_ms: u32,
    local_prg: u16,
    replay_phase: u8,

    routes: [ForwardRoute; MAX_ROUTES],
    inflight: [InflightForward; MAX_FORWARDS],
    local_sent: u32,
    remote_sent: u32,
    timeouts: u32,
    replayed_entries: u32,
    last_scan_ms: u64,

    buf: [u8; MAX_FWD_BUF],
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
        s.in_forward = in_chan;
        s.out_local = out_chan;
        s.in_replay = dev_channel_port(sys, 0, 1);
        s.out_remote = dev_channel_port(sys, 1, 1);
        s.out_wal_marker = dev_channel_port(sys, 1, 2);
        s.out_metrics = dev_channel_port(sys, 1, 3);
        s.timeout_ms = 5000;
        s.local_prg = 0;
        s.replay_phase = 1;
        for i in 0..MAX_ROUTES {
            s.routes[i] = ForwardRoute::zero();
        }
        for i in 0..MAX_FORWARDS {
            s.inflight[i] = InflightForward::zero();
        }
        dev_log(sys, 3, b"[fwd] init".as_ptr(), 9);
        0
    }
}

fn get_or_create_route(
    routes: &mut [ForwardRoute; MAX_ROUTES],
    ingress: u16,
    egress: u16,
    epoch: u32,
) -> usize {
    if let Some(i) = routes.iter().position(|r| {
        r.active == 1
            && r.ingress_prg == ingress
            && r.egress_prg == egress
            && r.routing_epoch == epoch
    }) {
        return i;
    }
    if let Some(i) = routes.iter().position(|r| r.active == 0) {
        routes[i] = ForwardRoute {
            ingress_prg: ingress,
            egress_prg: egress,
            routing_epoch: epoch,
            next_seq: 1,
            active: 1,
        };
        return i;
    }
    0
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

        // Replay phase: reconstruct forward_seq counters from WAL replay entries.
        // Format: [ingress:u16][egress:u16][epoch:u32][seq:u64]
        if s.replay_phase == 1 && s.in_replay >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_replay, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (_, plen) = {
                    worked += 1;
                    wire::channel_read_msg(sys, s.in_replay, &mut s.buf)
                };
                if plen < 16 {
                    continue;
                }
                let ingress = u16::from_le_bytes([s.buf[0], s.buf[1]]);
                let egress = u16::from_le_bytes([s.buf[2], s.buf[3]]);
                let epoch = u32::from_le_bytes([s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);
                let seq = u64::from_le_bytes([
                    s.buf[8], s.buf[9], s.buf[10], s.buf[11], s.buf[12], s.buf[13], s.buf[14],
                    s.buf[15],
                ]);
                let idx = get_or_create_route(&mut s.routes, ingress, egress, epoch);
                if seq >= s.routes[idx].next_seq {
                    s.routes[idx].next_seq = seq + 1;
                }
                s.replayed_entries = s.replayed_entries.wrapping_add(1);
            }
        }

        // Drain forward requests: [ingress_prg:u16][egress_prg:u16][routing_epoch:u32][payload...]
        if s.in_forward >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_forward, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (_, plen) = {
                    worked += 1;
                    wire::channel_read_msg(sys, s.in_forward, &mut s.buf)
                };
                if plen < 8 {
                    continue;
                }
                let plen = plen as usize;

                let ingress = u16::from_le_bytes([s.buf[0], s.buf[1]]);
                let egress = u16::from_le_bytes([s.buf[2], s.buf[3]]);
                let epoch = u32::from_le_bytes([s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);

                let route_idx = get_or_create_route(&mut s.routes, ingress, egress, epoch);
                let seq = s.routes[route_idx].next_seq;
                s.routes[route_idx].next_seq = seq.wrapping_add(1);

                let is_local = egress == s.local_prg;

                // Build forwarded message: [forward_seq:u64][ingress:u16][egress:u16][epoch:u32][payload...]
                let body_len = plen - 8;
                let total = 16 + body_len;
                if total > MAX_FWD_BUF {
                    continue;
                }
                let mut fwd = [0u8; MAX_FWD_BUF];
                fwd[0..8].copy_from_slice(&seq.to_le_bytes());
                fwd[8..10].copy_from_slice(&ingress.to_le_bytes());
                fwd[10..12].copy_from_slice(&egress.to_le_bytes());
                fwd[12..16].copy_from_slice(&epoch.to_le_bytes());
                fwd[16..total].copy_from_slice(&s.buf[8..plen]);

                let target = if is_local { s.out_local } else { s.out_remote };
                if target >= 0 {
                    let poll_out = (sys.channel_poll)(target, 0x02);
                    if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                        wire::channel_write_msg(
                            sys,
                            target,
                            wire::MSG_FORWARD_REQUEST,
                            &fwd[..total],
                        );
                        if is_local {
                            s.local_sent = s.local_sent.wrapping_add(1);
                        } else {
                            s.remote_sent = s.remote_sent.wrapping_add(1);
                        }

                        // Emit WAL persistence marker so replay can rebuild forward_seq.
                        // Format: [ingress:u16][egress:u16][epoch:u32][seq:u64]
                        if s.out_wal_marker >= 0 {
                            let mut marker = [0u8; 16];
                            marker[0..2].copy_from_slice(&ingress.to_le_bytes());
                            marker[2..4].copy_from_slice(&egress.to_le_bytes());
                            marker[4..8].copy_from_slice(&epoch.to_le_bytes());
                            marker[8..16].copy_from_slice(&seq.to_le_bytes());
                            let p = (sys.channel_poll)(s.out_wal_marker, 0x02);
                            if p > 0 && (p as u32 & 0x02) != 0 {
                                wire::channel_write_msg(
                                    sys,
                                    s.out_wal_marker,
                                    wire::MSG_CLIENT_PROPOSAL,
                                    &marker,
                                );
                            }
                        }

                        // Track inflight
                        for i in 0..MAX_FORWARDS {
                            if s.inflight[i].active == 0 {
                                s.inflight[i] = InflightForward {
                                    forward_seq: seq,
                                    egress_prg: egress,
                                    is_local: is_local as u8,
                                    sent_ms: now,
                                    active: 1,
                                };
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Periodic timeout scan
        if now.wrapping_sub(s.last_scan_ms) >= 1000 {
            s.last_scan_ms = now;
            for i in 0..MAX_FORWARDS {
                if s.inflight[i].active == 1 {
                    let age = now.wrapping_sub(s.inflight[i].sent_ms);
                    if age >= s.timeout_ms as u64 {
                        s.inflight[i].active = 0;
                        s.timeouts = s.timeouts.wrapping_add(1);
                    }
                }
            }

            if s.out_metrics >= 0 {
                let mut m = [0u8; 12];
                m[0..4].copy_from_slice(&s.local_sent.to_le_bytes());
                m[4..8].copy_from_slice(&s.remote_sent.to_le_bytes());
                m[8..12].copy_from_slice(&s.timeouts.to_le_bytes());
                let poll = (sys.channel_poll)(s.out_metrics, 0x02);
                if poll > 0 && (poll as u32 & 0x02) != 0 {
                    wire::channel_write_msg(sys, s.out_metrics, wire::MSG_METRICS, &m);
                }
            }
        }

        if worked > 0 {
            STEP_BURST
        } else {
            0
        }
    }
}
