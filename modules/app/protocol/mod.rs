//! Protocol — client protocol classification, wire codecs and response
//! multiplexing.
//!
//! One graph module composed of up to four components (standards
//! `fluxor-modules.md` §8):
//!
//!   - [`router`] — per-connection protocol classification from the
//!     first bytes, pinned for the connection's lifetime.
//!   - [`mqtt`]   — MQTT 3.1 / 3.1.1 / 5.0 codec.
//!   - [`kafka`]  — Kafka binary-protocol codec.
//!   - [`amqp`]   — AMQP 0-9-1 codec.
//!
//! HTTP is deliberately absent: the diagnostic/admin surface is wave's
//! `http` module (`app` variant) on its own listener, feeding
//! `operations` — a graph-composition decision, not a codec here. A
//! graph with no admin/debug surface carries no HTTP at all.
//!
//! Unlike this project's other composites, these components genuinely
//! interact: the router names the protocol that owns each inbound record
//! and the dispatch table hands the record to that codec's `on_frame` —
//! one message-shaped call, so a client's first byte reaches its codec
//! in the same step rather than a tick later.
//!
//! Response multiplexing is structural rather than a component: every
//! codec encodes onto the same `frames_out` handle.
//!
//! ## Variants
//!
//!   - `full` (default) — every codec; ALPN/first-byte classification
//!     picks between them per connection.
//!   - `mqtt` / `kafka` / `amqp` — a single codec. The router still
//!     classifies (so a mismatched client is dropped rather than
//!     misparsed), but the other codecs are compiled out along with
//!     their state and their share of the module arena.
//!
//! The MQTT-over-QUIC graph uses the `mqtt` variant behind the standalone
//! `mqtt_quic_adapter`, which bridges the QUIC transport to the same
//! `raw_in` / `frames_out` framing the TCP path uses.
//!
//! ## Dispatch table
//!
//! One drain of `raw_in` per step, at most [`RX_BUDGET`] records:
//!
//!   1. `router::route` — classify the record's connection. The verdict
//!      is the protocol that owns it; MSG_CONN_CLOSED returns the
//!      protocol the connection had been pinned to so the close reaches
//!      that codec, and PROTO_UNKNOWN means "not yet decidable", which
//!      drops the record exactly as the standalone router did.
//!   2. Hand the record to the owning codec's `on_frame`.
//!   3. One drain of the shared `responses_in` bus — each record is
//!      dispatched to its codec on the session proto tag, and encoded
//!      onto `frames_out`.
//!   4. Metrics emission, one component-tagged frame per component,
//!      once per [`METRICS_INTERVAL_MS`].
//!
//! Step effects are reported per component at their original sites; the
//! module step itself always returns 0.

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

/// What a Metadata response says about the cluster — which brokers
/// exist and who leads a partition. Shared with the host test bed so
/// the decision has one definition and can be tested without a broker.
#[cfg(feature = "kafka")]
#[path = "../../common/cores/kafka_metadata_core.rs"]
mod kafka_metadata;

mod anchor;
mod router;

#[cfg(feature = "amqp")]
mod amqp;
#[cfg(feature = "kafka")]
mod kafka;
#[cfg(feature = "mqtt")]
mod mqtt;

use router::{PROTO_AMQP, PROTO_KAFKA, PROTO_MQTT, PROTO_UNKNOWN};

define_params! {
    ModuleState;

    // Broker address advertised in Kafka Metadata responses. Must be the
    // address CLIENTS can reach (the rig DUT's address, say), not the
    // bind address. Default 127.0.0.1 keeps local dev working.
    1, advertised_host, str, 0
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            kafka::set_advertised_host(&mut s.kafka, d, len);
        };

    2, advertised_port, u16, 9090
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            { s.kafka.advertised_port = p_u16(d, len, 0, 9090); }
        };

    // Partition count advertised per topic (clamped to 1..=16 so a full
    // 8-topic Metadata response always fits the encode buffer). Each
    // `(topic, partition)` is its own routing key, its own log and its
    // own offset sequence, so the count is real placement and not just
    // client-side batching.
    3, partitions, u16, 1
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            { s.kafka.partitions = p_u16(d, len, 0, 1).clamp(1, 16); }
        };

    // This node's id, mixed into the high bits of every producer id it
    // issues. Producer ids are otherwise a per-node counter starting at
    // 1, so two nodes hand out the SAME id — and idempotence state is
    // keyed by `(producer_id, partition)`, so two unrelated producers
    // would read each other's sequence numbers and be told their
    // batches are duplicates or out of order. Distinct ids per node are
    // the minimum for that keying to mean anything in a cluster.
    4, node_id, u16, 0
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            { s.kafka.node_id = p_u16(d, len, 0, 0); }
        };

    // Cluster shape for Metadata. Without these a client is told the
    // cluster is ONE machine — this node — and cannot distribute load
    // however well the substrate partitions underneath.
    5, peer_count, u16, 1
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            { s.kafka.peer_count = p_u16(d, len, 0, 1) as u8; }
        };
    6, peer0_port, u16, 0
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            { s.kafka.peer_ports[0] = p_u16(d, len, 0, 0); }
        };
    7, peer1_port, u16, 0
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            { s.kafka.peer_ports[1] = p_u16(d, len, 0, 0); }
        };
    8, peer2_port, u16, 0
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            { s.kafka.peer_ports[2] = p_u16(d, len, 0, 0); }
        };

    // Per-peer advertised host. Unset falls back to this node's address,
    // which is right only for a co-located peer — across machines,
    // advertising a wrong host sends the client somewhere that does not
    // serve the partition.
    9, peer0_host, str, 0
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            kafka::set_peer_host(&mut s.kafka, 0, d, len);
        };
    10, peer1_host, str, 0
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            kafka::set_peer_host(&mut s.kafka, 1, d, len);
        };
    11, peer2_host, str, 0
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            kafka::set_peer_host(&mut s.kafka, 2, d, len);
        };

    // ── Transport anchor (docs/architecture/session_continuity.md) ──
    //
    // Eight bytes naming this anchor on every session id it mints and
    // every MON_SESSION line it emits.
    12, anchor_id, str, 0
        => |s, d, len| { anchor::set_anchor_id(&mut s.anchor, d, len); };
    // Swap every attached session to the other worker every N
    // forwarded envelopes (0 = never). The gates' trigger; a
    // deployment drives swaps from its control plane.
    13, handoff_after_records, u32, 0
        => |s, d, len| { s.anchor.handoff_after_records = p_u32(d, len, 0, 0); };
    // The swap window: the deadline every DRAIN carries and the time a
    // handoff may take before it is refused. Must sit below the
    // shortest client keep-alive the deployment admits.
    14, session_drain_ms, u32, 500
        => |s, d, len| { s.anchor.session_drain_ms = p_u32(d, len, 0, 500).max(1); };
    // The rebinding hold: bytes of decoded envelopes held while an
    // attachment is not live, and what an overflow does — 0 closes the
    // connection (a publish we cannot hold is one the client must
    // resend), 1 drops the envelope and counts it.
    15, hold_bytes, u32, 65536
        => |s, d, len| {
            s.anchor.hold_bytes = p_u32(d, len, 0, 65536).min(anchor::HOLD_MAX as u32);
        };
    16, hold_overflow, u8, 0
        => |s, d, len| { s.anchor.hold_overflow = p_u8(d, len, 0, 0); };
    // Worker new connections attach to (0 or 1).
    17, default_worker, u8, 0
        => |s, d, len| { s.anchor.default_worker = p_u8(d, len, 0, 0) & 1; };
    // Swaps the record trigger may start (0 = unlimited).
    18, handoff_max_swaps, u32, 0
        => |s, d, len| { s.anchor.handoff_max_swaps = p_u32(d, len, 0, 0); };
}

/// Kernel step ABI: 0=Continue, 1=Done, 2=Burst, 3=Ready. Returning
/// Burst re-runs the domain's exec rotation within the same tick (up to
/// the kernel's pass cap), so a record that arrives here reaches its
/// consumer in this tick instead of the next. We burst only when a
/// record was actually consumed: an idle graph reports no burst and the
/// tick costs exactly one pass, as before.
const STEP_BURST: i32 = 2;

/// Records drained from `raw_in` per step. Sized to stay ahead of a full
/// peer_router ingest burst so classification never becomes the
/// pipeline's rate limiter.
const RX_BUDGET: usize = 64;

/// Records drained from `responses_in` per step.
const RESP_BUDGET: usize = 64;

/// Session-side protocol discriminator at `payload[2]` of a
/// MSG_SESSION_RESPONSE, right after the `u16 LE` conn id. Distinct
/// from the router's PROTO_* vocabulary: these are the values
/// session_processor stamps (see its `emit_kafka_response` /
/// `emit_amqp_response`).
const SESSION_PROTO_MQTT: u8 = 0;
const SESSION_PROTO_KAFKA: u8 = 1;
const SESSION_PROTO_AMQP: u8 = 2;

/// Component identity stamped into the metric envelope's `metric_id`
/// field so governance's telemetry keys per component rather than by payload
/// hash (standards `fluxor-modules.md` §8 rule 8).
const METRIC_ID_ROUTER: u8 = 0x21;
const METRIC_ID_MQTT: u8 = 0x22;
const METRIC_ID_KAFKA: u8 = 0x23;
const METRIC_ID_AMQP: u8 = 0x24;
const METRIC_ID_ANCHOR: u8 = 0x25;

/// Telemetry cadence, matched to governance's telemetry rollup interval so
/// each component contributes exactly one sample per rollup window.
const METRICS_INTERVAL_MS: u64 = 10_000;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_raw: i32,
    in_responses: i32,
    out_frames: i32,
    out_metrics: i32,
    last_metrics_ms: u64,

    router: router::Router,
    /// Transport-anchor role (`anchor.rs`).
    anchor: anchor::Anchor,
    #[cfg(feature = "mqtt")]
    mqtt: mqtt::Mqtt,
    #[cfg(feature = "kafka")]
    kafka: kafka::Kafka,
    #[cfg(feature = "amqp")]
    amqp: amqp::Amqp,

    /// Module-owned read buffer. Each record is read once here and
    /// dispatched to the owning codec as a borrowed payload.
    buf: [u8; router::READ_BUF],
    /// A session response its codec could not write (the frame edge
    /// refused it), kept whole and offered again ahead of the next
    /// drain. Length 0 = none. Responses are consumed from the channel
    /// before a codec sees them, so this is what keeps a refusal from
    /// becoming a lost PUBACK.
    resp_held_len: u16,
    /// Worker slot the held response came from.
    resp_held_w: u8,
    resp_held: [u8; router::READ_BUF],
    /// Per-second accounting for the `[proto] hb` line: raw records
    /// taken off `raw_in` and responses taken off `responses_in` since
    /// the line last went out.
    hb_raw: u32,
    hb_resp: u32,
    last_hb_ms: u64,
}

/// Write `v` in decimal at `out[pos..]`; returns the new position.
fn fmt_u32(out: &mut [u8], mut pos: usize, mut v: u32) -> usize {
    let mut digits = [0u8; 10];
    let mut n = 0usize;
    loop {
        digits[n] = b'0' + (v % 10) as u8;
        n += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    while n > 0 && pos < out.len() {
        n -= 1;
        out[pos] = digits[n];
        pos += 1;
    }
    pos
}

/// Hand the response in `s.buf[..n]` to the codec its proto tag names.
/// `false` means the codec could not write its frame and the response
/// must be offered again unchanged.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
unsafe fn dispatch_response(s: &mut ModuleState, sys: &SyscallTable, n: usize) -> bool {
    match s.buf[2] {
        #[cfg(feature = "mqtt")]
        SESSION_PROTO_MQTT => mqtt::on_response(&mut s.mqtt, sys, &s.buf[..n]),
        #[cfg(feature = "kafka")]
        SESSION_PROTO_KAFKA => {
            kafka::on_response(&mut s.kafka, sys, &s.buf[..n]);
            true
        }
        #[cfg(feature = "amqp")]
        SESSION_PROTO_AMQP => {
            amqp::on_response(&mut s.amqp, sys, &s.buf[..n]);
            true
        }
        _ => true,
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
    // SAFETY: per the module ABI (target/fluxor/fluxor-abi/sdk/abi.rs),
    // the kernel passes a valid, exclusively-borrowed `state` of at least
    // `module_state_size()` bytes, and a `syscalls` table whose function
    // pointers reach live kernel routines. The dereferences and syscall
    // invocations below rely on those guarantees.
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
        s.in_raw = in_chan;
        s.last_metrics_ms = 0;

        // out[0] proposals_out, out[1] frames_out, out[2] metrics.
        // Every codec shares the proposal and frame handles; the
        // consumer demuxes by the PROTO_* tag each codec stamps,
        // exactly as it did across the separate edges.
        let out_frames = dev_channel_port(sys, 1, 1);
        s.out_frames = out_frames;
        s.out_metrics = dev_channel_port(sys, 1, 2);
        s.in_responses = dev_channel_port(sys, 0, 1);
        s.resp_held_len = 0;
        s.hb_raw = 0;
        s.hb_resp = 0;
        s.last_hb_ms = 0;
        // Input 2: raft leader hints, so Metadata can name the real
        // leader instead of always naming this node.
        #[cfg(feature = "kafka")]
        {
            s.kafka.in_leader_state = dev_channel_port(sys, 0, 2);
        }

        router::init(&mut s.router);
        anchor::init(&mut s.anchor);
        s.anchor.prop_out[0] = out_chan;
        s.anchor.resp_in[0] = s.in_responses;
        s.anchor.out_frames = out_frames;
        // Continuity ports, declared last in the manifest.
        s.anchor.ctrl_in[0] = dev_channel_port(sys, 0, 3);
        s.anchor.ctrl_in[1] = dev_channel_port(sys, 0, 4);
        s.anchor.resp_in[1] = dev_channel_port(sys, 0, 5);
        s.anchor.ctrl_out[0] = dev_channel_port(sys, 1, 3);
        s.anchor.ctrl_out[1] = dev_channel_port(sys, 1, 4);
        s.anchor.prop_out[1] = dev_channel_port(sys, 1, 5);
        s.anchor.dir_in = dev_channel_port(sys, 0, 6);
        s.anchor.dir_out = dev_channel_port(sys, 1, 6);
        s.resp_held_w = 0;
        let anchor_ptr: *mut anchor::Anchor = &mut s.anchor;

        #[cfg(feature = "mqtt")]
        {
            mqtt::init(&mut s.mqtt);
            s.mqtt.anchor = anchor_ptr;
            s.mqtt.out_frames = out_frames;
            dev_log(sys, 3, b"[mqtt] init".as_ptr(), 11);
        }
        #[cfg(feature = "kafka")]
        {
            kafka::init(&mut s.kafka);
            s.kafka.anchor = anchor_ptr;
            s.kafka.out_frames = out_frames;
            dev_log(sys, 3, b"[kfk] init".as_ptr(), 10);
        }
        #[cfg(feature = "amqp")]
        {
            amqp::init(&mut s.amqp);
            s.amqp.anchor = anchor_ptr;
            s.amqp.out_frames = out_frames;
            dev_log(sys, 3, b"[amqp] init".as_ptr(), 11);
        }

        set_defaults(s);
        if !_params.is_null() && _params_len >= 4 {
            parse_tlv(s, _params, _params_len);
        }
        #[cfg(feature = "kafka")]
        kafka::finish_init(&mut s.kafka);

        dev_log(sys, 3, b"[prot] init".as_ptr(), 11);
        0
    }
}

/// Emit one component's metrics under the dimensional envelope
/// `[dim_flag=1][tenant:u32][protocol:u8][prg:u16][metric_id][payload]`.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
unsafe fn emit_metrics(sys: &SyscallTable, chan: i32, metric_id: u8, payload: &[u8]) {
    if chan < 0 {
        return;
    }
    let mut env = [0u8; 9 + 24];
    env[0] = 1; // dimensional
    env[8] = metric_id;
    let total = 9 + payload.len();
    if total > env.len() {
        return;
    }
    env[9..total].copy_from_slice(payload);
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let poll = (sys.channel_poll)(chan, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            wire::channel_write_msg(sys, chan, wire::MSG_METRICS, &env[..total]);
        }
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
    // SAFETY: per the module ABI (target/fluxor/fluxor-abi/sdk/abi.rs),
    // the kernel passes a valid, exclusively-borrowed `state` of at least
    // `module_state_size()` bytes, and a `syscalls` table whose function
    // pointers reach live kernel routines. The dereferences and syscall
    // invocations below rely on those guarantees.
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);
        let mut worked = 0u32;

        // ── 1-2: classify each record, hand it to the owning codec ───
        if s.in_raw >= 0 {
            // Leader hints first, so a Metadata answer built this step
            // names the leader as of this step rather than the last.
            #[cfg(feature = "kafka")]
            kafka::drain_leader_state(&mut s.kafka, sys);

            // A packet the proposal edge refused goes first; no raw
            // bytes are taken while it is held (see `mqtt::flush_held`).
            #[cfg(feature = "mqtt")]
            let rx_open = mqtt::flush_held(&mut s.mqtt, sys);
            #[cfg(not(feature = "mqtt"))]
            let rx_open = true;
            let rx_budget = if rx_open { RX_BUDGET } else { 0 };
            for _ in 0..rx_budget {
                let poll = (sys.channel_poll)(s.in_raw, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mtype, plen) = wire::channel_read_msg(sys, s.in_raw, &mut s.buf);
                let n = plen as usize;
                if n < 2 || n > s.buf.len() {
                    continue;
                }
                worked += 1;
                s.hb_raw = s.hb_raw.wrapping_add(1);
                let conn_id = u16::from_le_bytes([s.buf[0], s.buf[1]]);
                let proto = router::route(&mut s.router, conn_id, mtype, &s.buf[2..n]);
                if mtype == wire::MSG_CONN_CLOSED {
                    anchor::conn_closed(&mut s.anchor, sys, conn_id);
                }

                match proto {
                    #[cfg(feature = "mqtt")]
                    PROTO_MQTT => {
                        if mqtt::on_frame(&mut s.mqtt, sys, mtype, &s.buf[..n]) {
                            break;
                        }
                    }
                    #[cfg(feature = "kafka")]
                    PROTO_KAFKA => kafka::on_frame(&mut s.kafka, sys, mtype, &s.buf[..n]),
                    #[cfg(feature = "amqp")]
                    PROTO_AMQP => amqp::on_frame(&mut s.amqp, sys, mtype, &s.buf[..n]),
                    // PROTO_UNKNOWN (needs more bytes), PROTO_UNSUPPORTED,
                    // or a protocol this variant does not carry.
                    _ => {}
                }
            }
        }

        // ── 3: one drain of each worker's response bus, demuxed ──────
        //
        // Every codec shares this handle, so a per-codec drain would
        // CONSUME the other codecs' records. The demux lives here
        // instead: read once, dispatch on the session proto tag at
        // payload[2]. Both workers' buses are drained before their
        // control channels (§Delivery cursors: what a worker emitted
        // before DRAINED must be counted before its export is read).
        {
            // The held response goes first, and nothing else is drained
            // until its edge takes it.
            let mut resp_open = true;
            let held = s.resp_held_len as usize;
            if held > 0 {
                s.buf[..held].copy_from_slice(&s.resp_held[..held]);
                if dispatch_response(s, sys, held) {
                    let conn = u16::from_le_bytes([s.buf[0], s.buf[1]]);
                    let w = usize::from(s.resp_held_w);
                    anchor::note_relayed(&mut s.anchor, w, conn, wire::ENVELOPE_HDR + held);
                    s.resp_held_len = 0;
                } else {
                    resp_open = false;
                }
            }
            for w in 0..2 {
                let chan = s.anchor.resp_in[w];
                if chan < 0 || !resp_open {
                    continue;
                }
                for _ in 0..RESP_BUDGET {
                    let poll = (sys.channel_poll)(chan, 0x01);
                    if poll <= 0 || (poll as u32 & 0x01) == 0 {
                        break;
                    }
                    let (mtype, plen) = wire::channel_read_msg(sys, chan, &mut s.buf);
                    let n = plen as usize;
                    if mtype != wire::MSG_SESSION_RESPONSE || n < 3 || n > s.buf.len() {
                        continue;
                    }
                    worked += 1;
                    s.hb_resp = s.hb_resp.wrapping_add(1);
                    let conn = u16::from_le_bytes([s.buf[0], s.buf[1]]);
                    if dispatch_response(s, sys, n) {
                        anchor::note_relayed(&mut s.anchor, w, conn, wire::ENVELOPE_HDR + n);
                    } else {
                        s.resp_held[..n].copy_from_slice(&s.buf[..n]);
                        s.resp_held_len = n as u16;
                        s.resp_held_w = w as u8;
                        resp_open = false;
                        break;
                    }
                }
            }
        }

        // ── 3b: SessionCtrlV1 from each worker, then the anchor's own
        // step (handshake, deadlines, hold release, swap trigger).
        for w in 0..2 {
            worked += anchor::handle_ctrl(&mut s.anchor, sys, w, now);
        }
        worked += anchor::handle_dir(&mut s.anchor, sys);
        anchor::step(&mut s.anchor, sys, now);

        // One accounting line per second: records in, packets decoded
        // and written to the session, packets held, responses in,
        // frames out, frames refused. Cumulative where the counter is
        // cumulative (`dec`, `held`, `enc`, `refused`).
        if now.wrapping_sub(s.last_hb_ms) >= 1000 {
            s.last_hb_ms = now;
            #[cfg(feature = "mqtt")]
            let (dec, held, enc, refused) = mqtt::hb_counters(&s.mqtt);
            #[cfg(not(feature = "mqtt"))]
            let (dec, held, enc, refused) = (0u32, 0u32, 0u32, 0u32);
            let (att, reloc, hoffref, heldrec) = anchor::hb_counters(&s.anchor);
            let mut line = [0u8; 200];
            let mut pos = 0usize;
            for (label, v) in [
                (&b"[proto] hb raw="[..], s.hb_raw),
                (&b" dec="[..], dec),
                (&b" held="[..], held),
                (&b" resp="[..], s.hb_resp),
                (&b" enc="[..], enc),
                (&b" refused="[..], refused),
                (&b" att="[..], att),
                (&b" reloc="[..], reloc),
                (&b" hoffref="[..], hoffref),
                (&b" heldrec="[..], heldrec),
            ] {
                for &b in label {
                    line[pos] = b;
                    pos += 1;
                }
                pos = fmt_u32(&mut line, pos, v);
            }
            dev_log(sys, 3, line.as_ptr(), pos);
            s.hb_raw = 0;
            s.hb_resp = 0;
        }

        // ── 4: component-tagged telemetry ────────────────────────────
        if now.wrapping_sub(s.last_metrics_ms) >= METRICS_INTERVAL_MS {
            s.last_metrics_ms = now;
            let mut m = [0u8; 24];
            let n = router::metrics(&s.router, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_ROUTER, &m[..n]);
            let n = anchor::metrics(&s.anchor, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_ANCHOR, &m[..n]);
            #[cfg(feature = "mqtt")]
            {
                let n = mqtt::metrics(&s.mqtt, &mut m);
                emit_metrics(sys, s.out_metrics, METRIC_ID_MQTT, &m[..n]);
            }
            #[cfg(feature = "kafka")]
            {
                let n = kafka::metrics(&s.kafka, &mut m);
                emit_metrics(sys, s.out_metrics, METRIC_ID_KAFKA, &m[..n]);
            }
            #[cfg(feature = "amqp")]
            {
                let n = amqp::metrics(&s.amqp, &mut m);
                emit_metrics(sys, s.out_metrics, METRIC_ID_AMQP, &m[..n]);
            }
        }

        if worked > 0 {
            STEP_BURST
        } else {
            0
        }
    }
}
