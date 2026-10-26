//! quantum_mqtt_anchor — the MQTT transport anchor
//! (docs/architecture/mqtt_anchor.md).
//!
//! The client-facing half of an edge-anchored MQTT session as a module
//! of its own: it holds the client's connection open while the session
//! worker behind it is drained, replaced or lost, and keeps only what
//! that needs — the connection's identity, the MQTT framing state, safe
//! liveness, and bounded queues of frames waiting for a worker and
//! responses waiting for the transport, each under a stable id. Session
//! state proper (subscriptions, QoS, dedupe, retained, offline) is the
//! worker's and the committed log's.
//!
//! Two components, one implementation each, shared with `protocol`:
//!
//!   - `mqtt` — the MQTT 3.1 / 3.1.1 / 5.0 codec (`protocol/mqtt.rs`):
//!     the incremental fixed-header / remaining-length parser and its
//!     partial frame bytes, decode to session envelopes, encode of the
//!     worker's responses.
//!   - [`anchor`] — the SessionCtrlV1 anchor role over the pure
//!     `mqtt_anchor_core`: attachment, delivery envelopes, the hold and
//!     the read-pause backpressure, liveness-only mode, the handoff and
//!     the operator swap trigger.
//!
//! `protocol` mounts both too, for the multi-protocol front door; a graph
//! that serves MQTT alone uses this module and carries no classifier and
//! no Kafka or AMQP codec.
//!
//! ## Step
//!
//!   1. Unless the hold is at its high-water mark, drain `raw_in` (at
//!      most [`RX_BUDGET`] records) into the codec, which hands every
//!      decoded envelope to the anchor.
//!   2. Drain each worker's response bus; only the worker a connection
//!      is bound to may emit for it.
//!   3. Each worker's SessionCtrlV1 channel, the directory, `cmd_in`, and
//!      the anchor's own step.
//!   4. The `[qanchor] hb` and `[anchor] hb` lines, and telemetry.

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

mod anchor;

/// The MQTT codec is `protocol`'s, mounted so both front doors frame and
/// encode MQTT identically.
#[path = "../protocol/mqtt.rs"]
mod mqtt;

define_params! {
    ModuleState;

    // Eight bytes naming this anchor on every session id it mints, every
    // MON_SESSION line and every swap command addressed to it.
    1, anchor_id, str, 0
        => |s, d, len| { anchor::set_anchor_id(&mut s.anchor, d, len); };
    // Record-count swap trigger for the gates (0 = never): every N
    // forwarded envelopes every attached session moves to the other
    // worker. A deployment drives swaps with MSG_ANCHOR_SWAP instead.
    2, handoff_after_records, u32, 0
        => |s, d, len| { s.anchor.handoff_after_records = p_u32(d, len, 0, 0); };
    // The swap window: the deadline every DRAIN carries and the time a
    // handoff may take before it is refused. Must sit below the
    // shortest client keep-alive the deployment admits.
    3, session_drain_ms, u32, 500
        => |s, d, len| { s.anchor.session_drain_ms = p_u32(d, len, 0, 500).max(1); };
    // The hold: bytes of decoded envelopes kept while no worker is
    // authoritative, and what an overflow does — 0 closes the
    // connection (a publish we cannot hold is one the client must
    // resend), 1 drops the envelope and counts it.
    4, hold_bytes, u32, 65536
        => |s, d, len| {
            s.anchor.hold_bytes = p_u32(d, len, 0, 65536).min(anchor::HOLD_MAX as u32);
        };
    5, hold_overflow, u8, 0
        => |s, d, len| { s.anchor.hold_overflow = p_u8(d, len, 0, 0); };
    // Worker new connections attach to (0 or 1).
    6, default_worker, u8, 0
        => |s, d, len| { s.anchor.default_worker = p_u8(d, len, 0, 0) & 1; };
    // Swaps the record trigger may start (0 = unlimited).
    7, handoff_max_swaps, u32, 0
        => |s, d, len| { s.anchor.handoff_max_swaps = p_u32(d, len, 0, 0); };
    // Route policy: 1 admits liveness-only mode — while no worker is
    // authoritative the anchor answers a PINGREQ itself, and never
    // PUBLISH, SUBSCRIBE, UNSUBSCRIBE or AUTH. 0 owes the PINGRESP until
    // a worker is.
    8, liveness_only, u8, 0
        => |s, d, len| { s.anchor.liveness_only = p_u8(d, len, 0, 0); };
    // Hold fill, in percent, at which raw bytes stop being read (0 =
    // never). The stream contract has no receive-window verb, so not
    // reading is the backpressure: the transport's buffers fill and the
    // client's TCP window closes before the hold can overflow.
    9, hold_high_water_pct, u8, 75
        => |s, d, len| { s.anchor.hold_high_water_pct = p_u8(d, len, 0, 75).min(100); };
    // Listener identity recorded on every connection.
    10, listener_id, u16, 0
        => |s, d, len| { s.anchor.listener_id = p_u16(d, len, 0, 0); };
    // Worker liveness: a probe every `worker_probe_ms`, and a worker
    // that answers none for `worker_loss_ms` is lost and its sessions
    // are adopted by the standby. Either at 0 disables detection by
    // silence; an operator `QASW` mode-1 record still declares a loss.
    // `worker_loss_ms` plus the adoption must sit below the shortest
    // client keep-alive admitted.
    11, worker_probe_ms, u32, 0
        => |s, d, len| { s.anchor.worker_probe_ms = p_u32(d, len, 0, 0); };
    12, worker_loss_ms, u32, 0
        => |s, d, len| { s.anchor.worker_loss_ms = p_u32(d, len, 0, 0); };
    // Bytes of frames retained for replay to a replacement worker until a
    // checkpoint covers them (0 = none: a loss closes the connections).
    13, replay_bytes, u32, 0
        => |s, d, len| {
            s.anchor.replay_bytes = p_u32(d, len, 0, 0).min(anchor::REPLAY_MAX as u32);
        };
    // Anchor continuity (docs/architecture/anchor_continuity.md): 1 runs
    // every connection with a production gate, attaches it as
    // transport-migratable, exports its QAC1 record on `state_out` and
    // imports a moved connection from `state_in`. 0 = edge-anchored.
    14, continuity, u8, 0
        => |s, d, len| { s.anchor.continuity = p_u8(d, len, 0, 0) & 1; };
}

const STEP_BURST: i32 = 2;
/// Records drained from `raw_in` per step.
const RX_BUDGET: usize = 64;
/// Records drained from each worker's response bus per step.
const RESP_BUDGET: usize = 64;
/// Session-side protocol tag of an MQTT response.
const SESSION_PROTO_MQTT: u8 = 0;
/// Component ids in the dimensional metric envelope.
const METRIC_ID_MQTT: u8 = 0x22;
const METRIC_ID_ANCHOR: u8 = 0x25;
const METRICS_INTERVAL_MS: u64 = 10_000;
/// Read buffer: one `[conn_id][tcp chunk]` record or one response — up
/// to a worker's largest response envelope (8 KiB) wrapped as a
/// production.
const READ_BUF: usize = 8192 + anchor::PRODUCTION_INSERT;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_raw: i32,
    out_metrics: i32,
    last_metrics_ms: u64,
    anchor: anchor::Anchor,
    mqtt: mqtt::Mqtt,
    buf: [u8; READ_BUF],
    /// A response the codec could not write, offered again first; kept
    /// whole, so it keeps the production id it will be accepted under.
    resp_held_len: u16,
    resp_held_w: u8,
    /// The held frame's message type (a response or a production).
    resp_held_mt: u8,
    resp_held: [u8; READ_BUF],
    hb_raw: u32,
    hb_resp: u32,
    last_hb_ms: u64,
}

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

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

/// PIC module ABI entry: one-time process-wide init.
///
/// # Safety
/// `syscalls` is a kernel-owned table whose function pointers reach live
/// kernel routines for the lifetime of the process.
#[no_mangle]
#[link_section = ".text.module_init"]
pub unsafe extern "C" fn module_init(_syscalls: *const c_void) {}

/// PIC module ABI entry: construct module state.
///
/// # Safety
/// `state` / `params` / `syscalls` are kernel-owned buffers passed across
/// the module ABI: `state` is at least `state_size` bytes and zeroed,
/// `params` at least `params_len` bytes.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    // SAFETY: per the module ABI the kernel passes a valid, exclusively
    // borrowed `state` of at least `module_state_size()` bytes and a live
    // syscall table.
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
        s.resp_held_len = 0;
        s.resp_held_w = 0;
        s.resp_held_mt = 0;
        s.hb_raw = 0;
        s.hb_resp = 0;
        s.last_hb_ms = 0;

        // Inputs: [0] raw_in [1] responses_in [2] ctrl_in [3] ctrl2_in
        // [4] responses2_in [5] dir_in [6] cmd_in [7] state_in.
        // Outputs: [0] proposals_out [1] frames_out [2] metrics
        // [3] ctrl_out [4] ctrl2_out [5] proposals2_out [6] dir_out
        // [7] state_out.
        let out_frames = dev_channel_port(sys, 1, 1);
        s.out_metrics = dev_channel_port(sys, 1, 2);
        anchor::init(&mut s.anchor);
        s.anchor.prop_out[0] = out_chan;
        s.anchor.resp_in[0] = dev_channel_port(sys, 0, 1);
        s.anchor.out_frames = out_frames;
        s.anchor.ctrl_in[0] = dev_channel_port(sys, 0, 2);
        s.anchor.ctrl_in[1] = dev_channel_port(sys, 0, 3);
        s.anchor.resp_in[1] = dev_channel_port(sys, 0, 4);
        s.anchor.dir_in = dev_channel_port(sys, 0, 5);
        s.anchor.cmd_in = dev_channel_port(sys, 0, 6);
        s.anchor.ctrl_out[0] = dev_channel_port(sys, 1, 3);
        s.anchor.ctrl_out[1] = dev_channel_port(sys, 1, 4);
        s.anchor.prop_out[1] = dev_channel_port(sys, 1, 5);
        s.anchor.dir_out = dev_channel_port(sys, 1, 6);
        s.anchor.state_in = dev_channel_port(sys, 0, 7);
        s.anchor.state_out = dev_channel_port(sys, 1, 7);

        mqtt::init(&mut s.mqtt);
        s.mqtt.anchor = &mut s.anchor;
        s.mqtt.out_frames = out_frames;

        set_defaults(s);
        if !params.is_null() && params_len >= 4 {
            parse_tlv(s, params, params_len);
        }
        dev_log(sys, 3, b"[qanchor] init".as_ptr(), 14);
        0
    }
}

/// Emit one component's metrics under the dimensional envelope.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn emit_metrics(sys: &SyscallTable, chan: i32, metric_id: u8, payload: &[u8]) {
    if chan < 0 {
        return;
    }
    let mut env = [0u8; 9 + 24];
    env[0] = 1;
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

/// Hand the response in `s.buf[..n]` to the codec. `false` means the
/// frame edge refused it and it must be offered again unchanged.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn dispatch_response(s: &mut ModuleState, sys: &SyscallTable, n: usize) -> bool {
    if s.buf[2] != SESSION_PROTO_MQTT {
        return true;
    }
    mqtt::on_response(&mut s.mqtt, sys, &s.buf[..n])
}

/// Offer the frame of type `mt` in `s.buf[..n]` from worker `w` to the
/// transport: a plain response through the codec; a production through
/// its connection's gate first, its response rebuilt behind the
/// connection id the transport knows it by. `false` means the frame edge
/// refused it: the caller holds the frame whole and offers it again, so
/// a production keeps the id it will be given under.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn offer_response(
    s: &mut ModuleState,
    sys: &SyscallTable,
    w: usize,
    mt: u8,
    n: usize,
    now: u64,
) -> bool {
    if mt == wire::MSG_SESSION_RESPONSE {
        let conn = u16::from_le_bytes([s.buf[0], s.buf[1]]);
        if !anchor::response_admitted(&mut s.anchor, w, conn) {
            return true;
        }
        // Every byte written to a gated connection is a production. A
        // plain response for one has no place in its numbering: a
        // DISCONNECT (the worker sending the client away) ends the
        // connection instead, anything else is a stale emitter's.
        if anchor::gated(&s.anchor, conn) {
            anchor::note_relayed(&mut s.anchor, w, conn, wire::ENVELOPE_HDR + n, now);
            anchor::plain_on_gated(
                &mut s.anchor,
                sys,
                conn,
                n > 3 && s.buf[3] == anchor::MQTT_DISCONNECT,
            );
            return true;
        }
        if dispatch_response(s, sys, n) {
            anchor::note_relayed(&mut s.anchor, w, conn, wire::ENVELOPE_HDR + n, now);
            return true;
        }
        return false;
    }
    let (conn, id, written, through) =
        match anchor::production_step(&mut s.anchor, sys, w, &s.buf[..n]) {
            anchor::ProductionStep::Done => return true,
            anchor::ProductionStep::Answered => {
                let conn = anchor::production_conn(&s.anchor, &s.buf[..n]);
                anchor::note_relayed(&mut s.anchor, w, conn, wire::ENVELOPE_HDR + n, now);
                return true;
            }
            anchor::ProductionStep::Hold => return false,
            anchor::ProductionStep::Offer {
                conn,
                id,
                written,
                through,
            } => (conn, id, written, through),
        };
    // Keep the production whole in case the edge refuses its response.
    s.resp_held[..n].copy_from_slice(&s.buf[..n]);
    let body_at = 2 + anchor::PRODUCTION_INSERT;
    let blen = n - body_at;
    s.buf.copy_within(body_at..n, 2);
    s.buf[..2].copy_from_slice(&conn.to_le_bytes());
    // Placed against the transport's horizon: bytes of it the standby
    // already holds (a re-offer after a takeover) are not written again.
    s.mqtt.place_on = true;
    s.mqtt.place_written = written;
    s.mqtt.place_through = through;
    s.mqtt.placed_len = 0;
    let taken = dispatch_response(s, sys, 2 + blen);
    s.mqtt.place_on = false;
    if taken {
        let len = s.mqtt.placed_len as u64;
        anchor::production_taken(&mut s.anchor, sys, w, conn, id, len);
        anchor::note_relayed(&mut s.anchor, w, conn, wire::ENVELOPE_HDR + n, now);
        return true;
    }
    s.buf[..n].copy_from_slice(&s.resp_held[..n]);
    false
}

/// PIC module ABI entry: run one scheduler step.
///
/// # Safety
/// `state` is the kernel-owned buffer a prior `module_new` initialised,
/// exclusively borrowed for the call.
#[no_mangle]
#[link_section = ".text.module_step"]
pub unsafe extern "C" fn module_step(state: *mut u8) -> i32 {
    // SAFETY: per the module ABI `state` is valid and exclusively
    // borrowed, and the syscall table is live.
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);
        let mut worked = 0u32;

        // ── 1: imports first, so a connection the transport presents in
        // the same step finds its staged record; then raw bytes into the
        // codec, unless the hold is full ─────────────────────────────
        worked += anchor::handle_state(&mut s.anchor, sys, now);
        if s.in_raw >= 0 {
            let rx_open = mqtt::flush_held(&mut s.mqtt, sys) && !anchor::read_paused(&mut s.anchor);
            let budget = if rx_open { RX_BUDGET } else { 0 };
            for _ in 0..budget {
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
                if mtype == wire::MSG_CLIENT_PEER && n >= wire::CLIENT_PEER_LEN {
                    let mut addr = [0u8; 16];
                    addr.copy_from_slice(&s.buf[3..19]);
                    let peer = anchor::Peer {
                        family: s.buf[2],
                        addr,
                        port: u16::from_le_bytes([s.buf[19], s.buf[20]]),
                    };
                    anchor::on_peer(&mut s.anchor, conn_id, peer);
                    continue;
                }
                if mtype == wire::MSG_CLIENT_RESUMED {
                    // A connection a takeover moved here: the session its
                    // client's staged record names, not a new client.
                    if let Some((v, in_bytes)) =
                        anchor::on_resumed(&mut s.anchor, sys, &s.buf[..n], now)
                    {
                        if !mqtt::import_conn(&mut s.mqtt, conn_id, v, in_bytes) {
                            anchor::refuse_import(&mut s.anchor, sys, conn_id);
                        }
                    }
                    continue;
                }
                anchor::on_data(&mut s.anchor, conn_id);
                if mtype == wire::MSG_CONN_CLOSED {
                    anchor::retire_qac(&mut s.anchor, sys, conn_id);
                    anchor::conn_closed(&mut s.anchor, sys, conn_id);
                    anchor::gate_close(&mut s.anchor, conn_id);
                }
                if mtype == wire::MSG_CLIENT_HORIZON {
                    anchor::transport_horizon(&mut s.anchor, sys, &s.buf[..n]);
                    continue;
                }
                if mqtt::on_frame(&mut s.mqtt, sys, mtype, &s.buf[..n]) {
                    break;
                }
            }
        }

        // ── 2: each worker's responses, fenced to the bound worker ───
        {
            let mut resp_open = true;
            let held = s.resp_held_len as usize;
            if held > 0 {
                s.buf[..held].copy_from_slice(&s.resp_held[..held]);
                let (w, mt) = (usize::from(s.resp_held_w), s.resp_held_mt);
                if offer_response(s, sys, w, mt, held, now) {
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
                    let min = if mtype == wire::MSG_ANCHOR_PRODUCTION {
                        2 + anchor::PRODUCTION_INSERT + 1
                    } else {
                        3
                    };
                    if (mtype != wire::MSG_SESSION_RESPONSE && mtype != wire::MSG_ANCHOR_PRODUCTION)
                        || n < min
                        || n > s.buf.len()
                    {
                        continue;
                    }
                    worked += 1;
                    s.hb_resp = s.hb_resp.wrapping_add(1);
                    if !offer_response(s, sys, w, mtype, n, now) {
                        s.resp_held[..n].copy_from_slice(&s.buf[..n]);
                        s.resp_held_len = n as u16;
                        s.resp_held_w = w as u8;
                        s.resp_held_mt = mtype;
                        resp_open = false;
                        break;
                    }
                }
            }
        }

        // ── 3: control, directory, operator commands, anchor step ────
        for w in 0..2 {
            worked += anchor::handle_ctrl(&mut s.anchor, sys, w, now);
        }
        worked += anchor::handle_dir(&mut s.anchor, sys);
        worked += anchor::handle_cmd(&mut s.anchor, sys, now);
        anchor::step(&mut s.anchor, sys, now);

        // Heartbeats: the front door's own line (records in and out, the
        // codec's decode / hold / encode / refusal counts), then the
        // anchor's. `raw` and `resp` are per second, the rest cumulative.
        if now.wrapping_sub(s.last_hb_ms) >= 1000 {
            s.last_hb_ms = now;
            let (dec, held, enc, refused) = mqtt::hb_counters(&s.mqtt);
            let mut line = [0u8; 128];
            let mut pos = 0usize;
            for (label, v) in [
                (&b"[qanchor] hb raw="[..], s.hb_raw),
                (&b" dec="[..], dec),
                (&b" held="[..], held),
                (&b" resp="[..], s.hb_resp),
                (&b" enc="[..], enc),
                (&b" refused="[..], refused),
            ] {
                for &b in label {
                    line[pos] = b;
                    pos += 1;
                }
                pos = fmt_u32(&mut line, pos, v);
            }
            dev_log(sys, 3, line.as_ptr(), pos);
            anchor::log_hb(&s.anchor, sys);
            s.hb_raw = 0;
            s.hb_resp = 0;
        }

        if now.wrapping_sub(s.last_metrics_ms) >= METRICS_INTERVAL_MS {
            s.last_metrics_ms = now;
            let mut m = [0u8; 24];
            let n = anchor::metrics(&s.anchor, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_ANCHOR, &m[..n]);
            let n = mqtt::metrics(&s.mqtt, &mut m);
            emit_metrics(sys, s.out_metrics, METRIC_ID_MQTT, &m[..n]);
        }

        if worked > 0 {
            STEP_BURST
        } else {
            0
        }
    }
}
