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
//!   2. Hand the record to the owning codec's `on_frame`, or — for
//!      PROTO_HTTP — forward it verbatim on `http_out`.
//!   3. `mqtt::step` / `kafka::step` / `amqp::step` — each drains its
//!      share of the shared `responses_in` bus, filtering on its own
//!      PROTO_* tag, and encodes frames onto `frames_out`.
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

mod router;

#[cfg(feature = "mqtt")]
mod mqtt;
#[cfg(feature = "kafka")]
mod kafka;
#[cfg(feature = "amqp")]
mod amqp;

use router::{PROTO_AMQP, PROTO_HTTP, PROTO_KAFKA, PROTO_MQTT, PROTO_UNKNOWN};

define_params! {
    ModuleState;

    // Broker address advertised in Kafka Metadata responses. Must be the
    // address CLIENTS can reach (e.g. 192.168.1.9 on the rig), not the
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
    // 8-topic Metadata response always fits the encode buffer). Offsets
    // are WAL indexes, so partitions share one durability pipeline; >1
    // only spreads client-side batching, it does not add server
    // parallelism yet.
    3, partitions, u16, 1
        => |s, d, len| {
            #[cfg(feature = "kafka")]
            { s.kafka.partitions = p_u16(d, len, 0, 1).clamp(1, 16); }
        };
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

/// Session-side protocol discriminator at `payload[1]` of a
/// MSG_SESSION_RESPONSE. Distinct from the router's PROTO_* vocabulary:
/// these are the values session_processor stamps (see its
/// `emit_kafka_response` / `emit_amqp_response`).
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

/// Telemetry cadence, matched to governance's telemetry rollup interval so
/// each component contributes exactly one sample per rollup window.
const METRICS_INTERVAL_MS: u64 = 10_000;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_raw: i32,
    in_responses: i32,
    out_http: i32,
    out_metrics: i32,
    last_metrics_ms: u64,

    router: router::Router,
    #[cfg(feature = "mqtt")]
    mqtt: mqtt::Mqtt,
    #[cfg(feature = "kafka")]
    kafka: kafka::Kafka,
    #[cfg(feature = "amqp")]
    amqp: amqp::Amqp,

    /// Module-owned read buffer. Each record is read once here and
    /// dispatched to the owning codec as a borrowed payload.
    buf: [u8; router::READ_BUF],
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

        // out[0] proposals_out, out[1] frames_out, out[2] http_out,
        // out[3] metrics. Every codec shares the proposal and frame
        // handles; the consumer demuxes by the PROTO_* tag each codec
        // stamps, exactly as it did across the separate edges.
        let out_frames = dev_channel_port(sys, 1, 1);
        s.out_http = dev_channel_port(sys, 1, 2);
        s.out_metrics = dev_channel_port(sys, 1, 3);
        s.in_responses = dev_channel_port(sys, 0, 1);

        router::init(&mut s.router);

        #[cfg(feature = "mqtt")]
        {
            mqtt::init(&mut s.mqtt);
            s.mqtt.out_proposals = out_chan;
            s.mqtt.out_frames = out_frames;
            dev_log(sys, 3, b"[mqtt] init".as_ptr(), 11);
        }
        #[cfg(feature = "kafka")]
        {
            kafka::init(&mut s.kafka);
            s.kafka.out_proposals = out_chan;
            s.kafka.out_frames = out_frames;
            dev_log(sys, 3, b"[kfk] init".as_ptr(), 10);
        }
        #[cfg(feature = "amqp")]
        {
            amqp::init(&mut s.amqp);
            s.amqp.out_proposals = out_chan;
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

/// Forward an HTTP-classified record verbatim on `http_out`.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
unsafe fn forward_http(sys: &SyscallTable, chan: i32, payload: &[u8]) {
    if chan < 0 || payload.is_empty() {
        return;
    }
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        wire::channel_write_msg(sys, chan, wire::MSG_CLIENT_FRAME, payload);
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
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
            for _ in 0..RX_BUDGET {
                let poll = (sys.channel_poll)(s.in_raw, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mtype, plen) = wire::channel_read_msg(sys, s.in_raw, &mut s.buf);
                let n = plen as usize;
                if n < 1 || n > s.buf.len() {
                    continue;
                }
                worked += 1;
                let conn_id = s.buf[0];
                let proto = router::route(&mut s.router, conn_id, mtype, &s.buf[1..n]);

                match proto {
                    #[cfg(feature = "mqtt")]
                    PROTO_MQTT => mqtt::on_frame(&mut s.mqtt, sys, mtype, &s.buf[..n]),
                    #[cfg(feature = "kafka")]
                    PROTO_KAFKA => kafka::on_frame(&mut s.kafka, sys, mtype, &s.buf[..n]),
                    #[cfg(feature = "amqp")]
                    PROTO_AMQP => amqp::on_frame(&mut s.amqp, sys, mtype, &s.buf[..n]),
                    PROTO_HTTP => forward_http(sys, s.out_http, &s.buf[..n]),
                    // PROTO_UNKNOWN (needs more bytes), PROTO_UNSUPPORTED,
                    // or a protocol this variant does not carry.
                    _ => {}
                }
            }
        }

        // ── 3: one drain of the shared response bus, demuxed ─────────
        //
        // Every codec shares this one handle, so a per-codec drain would
        // CONSUME the other codecs' records. The demux lives here
        // instead: read once, dispatch on the session proto tag at
        // payload[1].
        if s.in_responses >= 0 {
            for _ in 0..RESP_BUDGET {
                let poll = (sys.channel_poll)(s.in_responses, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mtype, plen) = wire::channel_read_msg(sys, s.in_responses, &mut s.buf);
                let n = plen as usize;
                if mtype != wire::MSG_SESSION_RESPONSE || n < 2 || n > s.buf.len() {
                    continue;
                }
                worked += 1;
                match s.buf[1] {
                    #[cfg(feature = "mqtt")]
                    SESSION_PROTO_MQTT => mqtt::on_response(&mut s.mqtt, sys, &s.buf[..n]),
                    #[cfg(feature = "kafka")]
                    SESSION_PROTO_KAFKA => kafka::on_response(&mut s.kafka, sys, &s.buf[..n]),
                    #[cfg(feature = "amqp")]
                    SESSION_PROTO_AMQP => amqp::on_response(&mut s.amqp, sys, &s.buf[..n]),
                    _ => {}
                }
            }
        }

        // ── 4: component-tagged telemetry ────────────────────────────
        if now.wrapping_sub(s.last_metrics_ms) >= METRICS_INTERVAL_MS {
            s.last_metrics_ms = now;
            let mut m = [0u8; 24];
            let n = router::metrics(&s.router, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_ROUTER, &m[..n]);
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

        if worked > 0 { STEP_BURST } else { 0 }
    }
}
