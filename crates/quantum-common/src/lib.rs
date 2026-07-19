//! Quantum common — host-consumable façade over `modules/common/*.rs`.
//!
//! Mounts each common file as its own `pub mod` so cross-file
//! `crate::FOO::*` references resolve the same way they do in PIC
//! builds (where each module does its own `#[path]`-mounted
//! `mod wire;` / `mod types;`). The `common/` directory is symlinked
//! at the crate root so cargo bundles the source files at package
//! time.

#![no_std]
#![allow(
    unsafe_code,
    reason = "common helpers operate on raw byte buffers and ABI-level syscall types"
)]
#![allow(
    dead_code,
    reason = "consumers use a subset of the common surface; the whole tree is mounted so every #[path] consumer agrees"
)]
#![allow(
    clippy::missing_safety_doc,
    clippy::too_many_arguments,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::identity_op,
    clippy::collapsible_if,
    clippy::collapsible_else_if,
    reason = "common source is shared with PIC builds where a different lint baseline applies"
)]

// Re-export fluxor-abi as `crate::abi` so `crate::abi::SyscallTable`
// references inside the bundled common files (e.g. `wire.rs`)
// resolve the same way they do in PIC builds — which `#[path]`-mount
// fluxor's abi.rs as `mod abi`.
pub use fluxor_abi as abi;

// `#[rustfmt::skip]` on each `mod` declaration preserves any
// hand-aligned const tables in the bundled common files.

#[rustfmt::skip]
#[path = "../common/types.rs"]
pub mod types;

#[rustfmt::skip]
#[path = "../common/wire.rs"]
pub mod wire;
