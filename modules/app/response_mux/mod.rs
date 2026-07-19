//! Response Mux — Explicit response-stream serializer.
//!
//! Fluxor can model fan-in/fan-out at the graph level, but this module keeps
//! client response arbitration explicit inside Quantum when a single
//! `peer_router.client_resp` writer is operationally convenient. It reads from
//! up to 4 input streams (MQTT frames, Kafka frames, AMQP frames, HTTP
//! responses) and forwards each to a single output.
//!
//! Envelope is passed through unchanged: `[conn_id][response bytes]`.
//!
//! Ports:
//!   in[0]  mqtt_in   — `[conn_id][mqtt frame]` from mqtt_codec.frames_out
//!   in[1]  kafka_in  — `[conn_id][kafka frame]` from kafka_codec.frames_out
//!   in[2]  amqp_in   — `[conn_id][amqp frame]` from amqp_codec.frames_out
//!   in[3]  http_in   — `[conn_id][http response]` from http_surface.responses_out
//!   out[0] mux_out   — unified stream to peer_router.client_resp

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

/// Frame buffer cap for codec → peer_router responses, including a
/// PUBLISH being delivered to a subscribing client. Sized to the
/// wire-channel per-message capacity (`fluxor-abi::CHANNEL_BUFFER_SIZE`
/// = 8192) so a worst-case MQTT packet (`mqtt_codec::MAX_PACKET` = 4096)
/// plus any per-codec response framing flows without hitting the
/// `channel_read_msg` discard path.
const MAX_FRAME: usize = 8192;

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_mqtt: i32,
    in_kafka: i32,
    in_amqp: i32,
    in_http: i32,
    out_mux: i32,

    mqtt_forwarded: u32,
    kafka_forwarded: u32,
    amqp_forwarded: u32,
    http_forwarded: u32,
    dropped: u32,

    buf: [u8; MAX_FRAME],
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
        s.in_mqtt = in_chan;
        s.out_mux = out_chan;
        s.in_kafka = dev_channel_port(sys, 0, 1);
        s.in_amqp = dev_channel_port(sys, 0, 2);
        s.in_http = dev_channel_port(sys, 0, 3);
        dev_log(sys, 3, b"[mux] init".as_ptr(), 10);
        0
    }
}

/// Drain one input channel and forward each message to the mux output.
/// Both directions use envelope framing — without it back-to-back
/// frames coalesce on the byte FIFO and the downstream consumer
/// mis-parses the second one's `conn_id` prefix as part of the first
/// frame's payload. The msg_type from each input is forwarded
/// unchanged so peer_router and any future demuxer can dispatch on it.
/// # Safety
unsafe fn drain_to_mux(sys: &SyscallTable, chan: i32, out: i32, buf: &mut [u8]) -> u32 {
    if chan < 0 || out < 0 { return 0; }
    let mut n_forwarded = 0u32;
    for _ in 0..8 {
        let poll_in = (sys.channel_poll)(chan, 0x01);
        if poll_in <= 0 || (poll_in as u32 & 0x01) == 0 { break; }
        let (mt, plen) = wire::channel_read_msg(sys, chan, buf);
        dev_log(sys, 3, b"[mux] rx".as_ptr(), 8);
        if plen < 2 { continue; }

        let w = wire::channel_write_msg(sys, out, mt, &buf[..plen as usize]);
        if w > 0 {
            dev_log(sys, 3, b"[mux] -> peer".as_ptr(), 13);
            n_forwarded += 1;
        }
    }
    n_forwarded
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        s.mqtt_forwarded = s.mqtt_forwarded.wrapping_add(
            drain_to_mux(sys, s.in_mqtt, s.out_mux, &mut s.buf));
        s.kafka_forwarded = s.kafka_forwarded.wrapping_add(
            drain_to_mux(sys, s.in_kafka, s.out_mux, &mut s.buf));
        s.amqp_forwarded = s.amqp_forwarded.wrapping_add(
            drain_to_mux(sys, s.in_amqp, s.out_mux, &mut s.buf));
        s.http_forwarded = s.http_forwarded.wrapping_add(
            drain_to_mux(sys, s.in_http, s.out_mux, &mut s.buf));
        0
    }
}
