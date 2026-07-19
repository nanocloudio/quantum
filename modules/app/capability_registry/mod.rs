//! Capability Registry — Fluxor-native storage capability advertisement.
//!
//! Owns the typed `(StorageSurface, FenceKind)` vocabulary from Fluxor's
//! capability_surface.md and broadcasts the local node's offering on
//! `local_ad` so consumers can match at deployment time. Mirrors the
//! schema from src/control/capabilities.rs (the legacy Rust crate) so
//! the on-the-wire byte layout round-trips across both paths.
//!
//! `FenceKind` byte values are deliberately the rank order — the
//! deployment-time matcher refuses any consumer requirement whose byte
//! is higher than the offered byte, without needing to interpret the
//! enum semantically. This is the load-bearing rule: silent downgrades
//! are impossible because the rank bytes are comparable as integers.

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

// ─── Schema ──────────────────────────────────────────────────────────

// Surface byte values match the wire-form taxonomy.
const SURFACE_STORAGE_BLOCK: u8 = 0;
const SURFACE_FILE_DATA: u8 = 1;
const SURFACE_STORAGE_NAMESPACE: u8 = 2;
const SURFACE_STORAGE_OBJECT: u8 = 3;

// Fence rank: byte value is the rank itself. Used directly by the
// matcher: provided_byte >= required_byte ⇒ match.
const FENCE_VOLATILE: u8 = 0;
const FENCE_VIEW_CONSISTENT: u8 = 1;
const FENCE_LOCAL_DURABLE: u8 = 2;
const FENCE_REVISION_MONOTONE: u8 = 3;
const FENCE_CONTENT_HASHED: u8 = 4;
const FENCE_REPLICATED_DURABLE: u8 = 5;

// MSG type allocated for capability traffic. Picks a number in the
// 0x60+ range used for sideband telemetry/control messages.
const MSG_CAP_ADVERTISE: u8 = 0x66;
const MSG_CAP_QUERY: u8 = 0x67;
const MSG_CAP_QUERY_REPLY: u8 = 0x68;

// Local-node id length cap and entry cap. Tight for embedded targets;
// graphs that need bigger names should raise these and re-pack.
const MAX_NODE_ID: usize = 32;
const MAX_LOCAL_ENTRIES: usize = 8;
const MAX_PEERS: usize = 16;
const MAX_FRAME: usize = 512;

// Period (in step calls) between automatic re-advertisements. At a
// 1ms tick this is roughly 500ms; tunable via the params blob once
// the params reader is wired in.
const ADVERTISE_PERIOD: u32 = 500;

#[repr(C)]
#[derive(Clone, Copy)]
struct LocalEntry {
    surface: u8,
    max_fence: u8,
    active: u8,
}

impl LocalEntry {
    const fn zero() -> Self {
        Self { surface: 0, max_fence: 0, active: 0 }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PeerEntry {
    node_id_len: u8,
    node_id: [u8; MAX_NODE_ID],
    n_entries: u8,
    entries: [(u8, u8); MAX_LOCAL_ENTRIES],
    last_seen_tick: u32,
    active: u8,
}

impl PeerEntry {
    const fn zero() -> Self {
        Self {
            node_id_len: 0,
            node_id: [0; MAX_NODE_ID],
            n_entries: 0,
            entries: [(0, 0); MAX_LOCAL_ENTRIES],
            last_seen_tick: 0,
            active: 0,
        }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_query: i32,
    in_peer_ad: i32,
    out_local_ad: i32,
    out_query_reply: i32,
    out_metrics: i32,

    tick: u32,
    advertise_at_next_tick: u8,

    node_id_len: u8,
    node_id: [u8; MAX_NODE_ID],
    local: [LocalEntry; MAX_LOCAL_ENTRIES],
    peers: [PeerEntry; MAX_PEERS],

    ads_emitted: u32,
    queries_handled: u32,
    peer_ads_received: u32,
    rejected_advertisements: u32,

    buf: [u8; MAX_FRAME],
}

impl ModuleState {
    fn add_local(&mut self, surface: u8, max_fence: u8) -> bool {
        for slot in self.local.iter_mut() {
            if slot.active == 0 {
                slot.surface = surface;
                slot.max_fence = max_fence;
                slot.active = 1;
                return true;
            }
            if slot.active == 1 && slot.surface == surface {
                // Update existing surface entry rather than duplicating.
                slot.max_fence = max_fence;
                return true;
            }
        }
        false
    }

    fn store_peer(&mut self, node_id: &[u8], entries: &[(u8, u8)]) -> bool {
        let id_len = core::cmp::min(node_id.len(), MAX_NODE_ID);
        let n = core::cmp::min(entries.len(), MAX_LOCAL_ENTRIES);
        // Replace existing entry for the same node_id, else use first free slot.
        let mut chosen: Option<usize> = None;
        for (i, p) in self.peers.iter().enumerate() {
            if p.active == 1 && p.node_id_len as usize == id_len
                && p.node_id[..id_len] == node_id[..id_len]
            {
                chosen = Some(i);
                break;
            }
        }
        if chosen.is_none() {
            for (i, p) in self.peers.iter().enumerate() {
                if p.active == 0 { chosen = Some(i); break; }
            }
        }
        let idx = match chosen { Some(i) => i, None => return false };
        let dst = &mut self.peers[idx];
        dst.node_id_len = id_len as u8;
        dst.node_id[..id_len].copy_from_slice(&node_id[..id_len]);
        dst.n_entries = n as u8;
        for i in 0..n { dst.entries[i] = entries[i]; }
        dst.last_seen_tick = self.tick;
        dst.active = 1;
        true
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

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
        s.in_query = in_chan;
        s.in_peer_ad = dev_channel_port(sys, 0, 1);
        s.out_local_ad = out_chan;
        s.out_query_reply = dev_channel_port(sys, 1, 1);
        s.out_metrics = dev_channel_port(sys, 1, 2);

        s.tick = 0;
        s.advertise_at_next_tick = 1;
        s.ads_emitted = 0;
        s.queries_handled = 0;
        s.peer_ads_received = 0;
        s.rejected_advertisements = 0;

        for i in 0..MAX_LOCAL_ENTRIES { s.local[i] = LocalEntry::zero(); }
        for i in 0..MAX_PEERS { s.peers[i] = PeerEntry::zero(); }

        // Bootstrap: identify as "quantum-node" + seed the canonical
        // quantum offerings. Graphs with multiple quantum nodes should
        // distinguish via the params blob once a parser is added.
        let id = b"quantum-node";
        let id_len = id.len();
        s.node_id_len = id_len as u8;
        s.node_id[..id_len].copy_from_slice(id);

        // Default offer: this node is a Raft-replicated messaging
        // fabric, so storage.object and storage.namespace operate
        // under ReplicatedDurable while file.data falls back to
        // LocalDurable (per-replica WAL fsync).
        s.add_local(SURFACE_STORAGE_OBJECT, FENCE_REPLICATED_DURABLE);
        s.add_local(SURFACE_STORAGE_NAMESPACE, FENCE_REPLICATED_DURABLE);
        s.add_local(SURFACE_FILE_DATA, FENCE_LOCAL_DURABLE);

        dev_log(sys, 3, b"[cap] init".as_ptr(), 10);
        0
    }
}

/// Pack the local advertisement payload into `out`. Returns bytes written.
fn pack_advertisement(s: &ModuleState, out: &mut [u8]) -> usize {
    let mut p = 0usize;
    if out.len() < 2 + s.node_id_len as usize + 1 { return 0; }
    out[p] = s.node_id_len; p += 1;
    let id_len = s.node_id_len as usize;
    out[p..p + id_len].copy_from_slice(&s.node_id[..id_len]); p += id_len;
    let active = s.local.iter().filter(|e| e.active == 1).count();
    out[p] = active as u8; p += 1;
    for e in s.local.iter() {
        if e.active != 1 { continue; }
        if p + 2 > out.len() { break; }
        out[p] = e.surface; p += 1;
        out[p] = e.max_fence; p += 1;
    }
    p
}

/// Parse an advertisement payload into (node_id slice, entries). Returns
/// None on malformed input.
fn parse_advertisement(buf: &[u8]) -> Option<(&[u8], [(u8, u8); MAX_LOCAL_ENTRIES], usize)> {
    if buf.is_empty() { return None; }
    let id_len = buf[0] as usize;
    if 1 + id_len + 1 > buf.len() { return None; }
    let node_id = &buf[1..1 + id_len];
    let n = buf[1 + id_len] as usize;
    let entries_start = 2 + id_len;
    if entries_start + n * 2 > buf.len() { return None; }
    let mut entries = [(0u8, 0u8); MAX_LOCAL_ENTRIES];
    let take = core::cmp::min(n, MAX_LOCAL_ENTRIES);
    for i in 0..take {
        let off = entries_start + i * 2;
        entries[i] = (buf[off], buf[off + 1]);
    }
    Some((node_id, entries, take))
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;

        s.tick = s.tick.wrapping_add(1);

        // ── Periodic local advertisement ──
        if s.advertise_at_next_tick == 1
            || (s.tick % ADVERTISE_PERIOD == 0)
        {
            s.advertise_at_next_tick = 0;
            if s.out_local_ad >= 0 {
                let mut payload = [0u8; MAX_FRAME];
                let n = pack_advertisement(s, &mut payload);
                if n > 0 {
                    let poll = (sys.channel_poll)(s.out_local_ad, 0x02);
                    if poll > 0 && (poll as u32 & 0x02) != 0 {
                        wire::channel_write_msg(sys, s.out_local_ad, MSG_CAP_ADVERTISE, &payload[..n]);
                        s.ads_emitted = s.ads_emitted.wrapping_add(1);
                    }
                }
            }
        }

        // ── Peer advertisement ingest ──
        if s.in_peer_ad >= 0 {
            for _ in 0..4 {
                let poll = (sys.channel_poll)(s.in_peer_ad, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_peer_ad, &mut s.buf);
                if mt != MSG_CAP_ADVERTISE || plen == 0 {
                    s.rejected_advertisements = s.rejected_advertisements.wrapping_add(1);
                    continue;
                }
                let n = plen as usize;
                // Copy the slice we need so the immutable borrow on
                // `s.buf` is released before we mutate `s` in store_peer.
                let parsed = parse_advertisement(&s.buf[..n]).map(
                    |(id, entries, count)| {
                        let mut id_copy = [0u8; MAX_NODE_ID];
                        let take = core::cmp::min(id.len(), MAX_NODE_ID);
                        id_copy[..take].copy_from_slice(&id[..take]);
                        (id_copy, take, entries, count)
                    },
                );
                if let Some((id_copy, take, entries, count)) = parsed {
                    if s.store_peer(&id_copy[..take], &entries[..count]) {
                        s.peer_ads_received = s.peer_ads_received.wrapping_add(1);
                    } else {
                        s.rejected_advertisements = s.rejected_advertisements.wrapping_add(1);
                    }
                } else {
                    s.rejected_advertisements = s.rejected_advertisements.wrapping_add(1);
                }
            }
        }

        // ── Query handling ──
        //
        // A MSG_CAP_QUERY payload is one byte:
        //   [surface:u8][min_fence:u8]
        // We reply with MSG_CAP_QUERY_REPLY containing every known
        // (node_id, surface, max_fence) tuple that satisfies the
        // requirement — local first, then peers. Empty reply means no
        // satisfier; consumers must refuse the placement, not downgrade.
        if s.in_query >= 0 {
            for _ in 0..4 {
                let poll = (sys.channel_poll)(s.in_query, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_query, &mut s.buf);
                if mt != MSG_CAP_QUERY || plen < 2 { continue; }
                let req_surface = s.buf[0];
                let req_fence = s.buf[1];

                let mut reply = [0u8; MAX_FRAME];
                let mut p = 0usize;
                // Reply format: [n_matches:u8] then
                //   N × [node_id_len:u8][node_id:N][surface:u8][max_fence:u8]
                let count_off = p; p += 1;
                let mut matches = 0u8;

                // Local matches.
                for e in s.local.iter() {
                    if e.active != 1 || e.surface != req_surface { continue; }
                    if e.max_fence < req_fence { continue; }
                    let id_len = s.node_id_len as usize;
                    if p + 1 + id_len + 2 > reply.len() { break; }
                    reply[p] = s.node_id_len; p += 1;
                    reply[p..p + id_len].copy_from_slice(&s.node_id[..id_len]); p += id_len;
                    reply[p] = e.surface; p += 1;
                    reply[p] = e.max_fence; p += 1;
                    matches += 1;
                }
                // Peer matches.
                for peer in s.peers.iter() {
                    if peer.active != 1 { continue; }
                    let id_len = peer.node_id_len as usize;
                    for i in 0..(peer.n_entries as usize) {
                        let (surf, fence) = peer.entries[i];
                        if surf != req_surface || fence < req_fence { continue; }
                        if p + 1 + id_len + 2 > reply.len() { break; }
                        reply[p] = peer.node_id_len; p += 1;
                        reply[p..p + id_len].copy_from_slice(&peer.node_id[..id_len]); p += id_len;
                        reply[p] = surf; p += 1;
                        reply[p] = fence; p += 1;
                        matches = matches.saturating_add(1);
                    }
                }
                reply[count_off] = matches;

                if s.out_query_reply >= 0 {
                    let poll_out = (sys.channel_poll)(s.out_query_reply, 0x02);
                    if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                        wire::channel_write_msg(
                            sys, s.out_query_reply, MSG_CAP_QUERY_REPLY, &reply[..p],
                        );
                    }
                }
                s.queries_handled = s.queries_handled.wrapping_add(1);
            }
        }

        0
    }
}
