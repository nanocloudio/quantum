//! Backpressure Propagator — Maps substrate backpressure to protocol signals.
//!
//! Consumes ThrottleEnvelope from flow_controller and rejected-proposal
//! envelopes from throttle_gate. Tracks queue depths. Emits protocol-native
//! backpressure signals to session_processor and status to http_surface.

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

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_envelope: i32,      // in[0]: ThrottleEnvelope from flow_controller
    in_rejected: i32,      // in[1]: rejected proposals from throttle_gate
    out_signals: i32,      // out[0]: protocol signals to session_processor
    out_status: i32,       // out[1]: status to http_surface
    out_metrics: i32,      // out[2]: metrics to metrics_aggregator

    pause_ack_depth: u32,
    drop_qos0_depth: u32,
    retained_buffer_bytes: u32,

    current_entry_credits: i32,
    current_byte_credits: i32,
    rejections: u32,
    signals_emitted: u32,
    last_status_ms: u64,

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
        s.in_envelope = in_chan;
        s.out_signals = out_chan;
        s.in_rejected = dev_channel_port(sys, 0, 1);
        s.out_status = dev_channel_port(sys, 1, 1);
        s.out_metrics = dev_channel_port(sys, 1, 2);
        s.pause_ack_depth = 10_000;
        s.drop_qos0_depth = 5_000;
        s.retained_buffer_bytes = 16 * 1024 * 1024;

        dev_log(sys, 3, b"[bp] init".as_ptr(), 8);
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

        // Drain envelope updates
        if s.in_envelope >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_envelope, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_envelope, &mut s.buf);
                if mt == wire::MSG_THROTTLE_ENVELOPE && plen >= 8 {
                    s.current_entry_credits = i32::from_le_bytes([s.buf[0], s.buf[1], s.buf[2], s.buf[3]]);
                    s.current_byte_credits = i32::from_le_bytes([s.buf[4], s.buf[5], s.buf[6], s.buf[7]]);
                }
            }
        }

        // Drain rejected proposals → emit protocol-native backpressure signal
        if s.in_rejected >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_rejected, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, plen) = wire::channel_read_msg(sys, s.in_rejected, &mut s.buf);
                if plen == 0 { continue; }
                s.rejections = s.rejections.wrapping_add(1);

                // Emit a structured backpressure signal:
                // [reason: u8] [entry_credits: i32] [byte_credits: i32]
                let reason = if s.current_entry_credits <= 0 {
                    BP_TRANSIENT
                } else if s.current_byte_credits <= 0 {
                    BP_TRANSIENT
                } else {
                    BP_PERMANENT_DURABILITY
                };
                let mut sig = [0u8; 9];
                sig[0] = reason;
                sig[1..5].copy_from_slice(&s.current_entry_credits.to_le_bytes());
                sig[5..9].copy_from_slice(&s.current_byte_credits.to_le_bytes());

                if s.out_signals >= 0 {
                    let poll_out = (sys.channel_poll)(s.out_signals, 0x02);
                    if poll_out > 0 && (poll_out as u32 & 0x02) != 0 {
                        wire::channel_write_msg(sys, s.out_signals, wire::MSG_BP_SIGNAL, &sig);
                        s.signals_emitted = s.signals_emitted.wrapping_add(1);
                    }
                }
            }
        }

        // Periodic status emission to http_surface
        if now.wrapping_sub(s.last_status_ms) >= 1000 && s.out_status >= 0 {
            s.last_status_ms = now;
            let mut m = [0u8; 16];
            m[0..4].copy_from_slice(&s.current_entry_credits.to_le_bytes());
            m[4..8].copy_from_slice(&s.current_byte_credits.to_le_bytes());
            m[8..12].copy_from_slice(&s.rejections.to_le_bytes());
            m[12..16].copy_from_slice(&s.signals_emitted.to_le_bytes());
            let poll = (sys.channel_poll)(s.out_status, 0x02);
            if poll > 0 && (poll as u32 & 0x02) != 0 {
                wire::channel_write_msg(sys, s.out_status, wire::MSG_THROTTLE_ENVELOPE, &m);
            }
        }

        0
    }
}
