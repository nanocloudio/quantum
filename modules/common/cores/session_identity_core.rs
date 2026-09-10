// session_identity_core — the reconciliation between Quantum's session
// identity and Fluxor's continuity identity.
//
// Written to be `include!`d by the PIC modules (`protocol` as the
// anchor, `session_processor` as the worker) and by the host test bed
// under `tools/core_tests`, so the rule below has ONE definition and a
// test that fails if the two identities can drift.
//
// ── Two counters, two names ──────────────────────────────────────────
//
// Quantum and Fluxor each keep a monotone counter over a session, and
// they mean different things:
//
//   session_generation   Quantum. Fences the MQTT SESSION: advances on
//                        every committed CONNECT (clean start, expiry,
//                        takeover) and keys the dedupe table as
//                        `(tenant, client_id, session_generation, msg_id)`
//                        and every in-flight QoS completion.
//
//   session_epoch        Fluxor (SessionCtrlV1). Fences the PLACEMENT of
//                        one client attachment: advances on every
//                        authoritative rebind of the attachment to a
//                        worker. A control message carrying a stale one
//                        is refused by the directory and the worker.
//
// The rule that keeps them from disagreeing about which generation of
// a session is live:
//
//   1. A rebind (RESUME at a higher epoch) NEVER changes the generation.
//      The dedupe ledger and the QoS in-flight table are keyed by the
//      generation, and a worker move must leave every existing key
//      reachable — otherwise a PUBREL after the move misses its PUBREC's
//      record and the flow completes twice.
//   2. A CONNECT NEVER changes the epoch. A new connection is a new
//      Fluxor session (a new `session_id`, epoch 1); the MQTT session it
//      resumes keeps its own generation counter.
//   3. An import is admitted only if the blob's generation equals the
//      generation of any replicated record the importing worker already
//      holds for that MQTT session. A mismatch means the blob describes
//      a generation the log has moved past — the handoff is refused
//      `STATUS_STALE_EPOCH` rather than admitted against the wrong
//      ledger.
//   4. Epochs only move forward: a RESUME at the current epoch is the
//      return-to-service of a refused handoff (the worker still holds
//      the state it exported, nothing advanced); a RESUME below the
//      current epoch is stale and refused; a RESUME above it after an
//      import is the rebind.
//
// ── Session identity ─────────────────────────────────────────────────
//
// Fluxor's `session_id` is 16 opaque bytes minted by the anchor at the
// continuity boundary (protocol_surfaces.md §Many sessions on one
// control channel). `CMD_SC_ATTACH` carries no data-plane address, so
// the id is minted such that the connection it names is recoverable
// from the identity alone:
//
//   [anchor_id:8][conn_id:2 BE][attach_generation:4 BE][reserved:2 = 0]
//
// `attach_generation` is an anchor-wide counter, so an id is never
// reused even though a `conn_id` is. The mapping to Quantum's identity
// is single-valued in both directions:
//
//   session_id  → conn_id       by `session_id_conn` (bytes 8..10)
//   conn_id     → session_id    by the anchor's binding table (one live
//                               attachment per connection)
//   conn_id     → (tenant, client_id)  by the worker's session table,
//                               bound at CONNECT and cleared at
//                               disconnect (`Session.conn_id`)

/// Bytes of a Fluxor `session_id`.
pub const SESSION_ID_LEN: usize = 16;
/// Bytes of an `anchor_id` / `worker_id`.
pub const PEER_ID_LEN: usize = 8;

/// SessionCtrlV1 status codes this core answers with (mirrors
/// `contracts/net/session_ctrl.rs`).
pub const SI_STATUS_OK: u8 = 0;
pub const SI_STATUS_STALE_EPOCH: u8 = 1;

/// Mint the session id for an attachment. `attach_generation` must be
/// unique per anchor for the life of the anchor (a counter that never
/// repeats), which is what makes the id unique even though `conn_id`
/// is recycled.
pub fn mint_session_id(
    anchor_id: &[u8; PEER_ID_LEN],
    conn_id: u16,
    attach_generation: u32,
) -> [u8; SESSION_ID_LEN] {
    let mut id = [0u8; SESSION_ID_LEN];
    id[..PEER_ID_LEN].copy_from_slice(anchor_id);
    id[8..10].copy_from_slice(&conn_id.to_be_bytes());
    id[10..14].copy_from_slice(&attach_generation.to_be_bytes());
    id
}

/// The connection a minted session id names.
#[inline]
pub fn session_id_conn(id: &[u8; SESSION_ID_LEN]) -> u16 {
    u16::from_be_bytes([id[8], id[9]])
}

/// The anchor that minted a session id.
#[inline]
pub fn session_id_anchor(id: &[u8; SESSION_ID_LEN]) -> [u8; PEER_ID_LEN] {
    let mut a = [0u8; PEER_ID_LEN];
    a.copy_from_slice(&id[..PEER_ID_LEN]);
    a
}

/// The attach generation a minted session id carries.
#[inline]
pub fn session_id_generation(id: &[u8; SESSION_ID_LEN]) -> u32 {
    u32::from_be_bytes([id[10], id[11], id[12], id[13]])
}

/// Where one attachment is in its life on a worker. The variants are
/// the SessionCtrlV1 phases a worker answers; the epoch rules below
/// are stated against them.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BindingPhase {
    /// No attachment.
    Idle = 0,
    /// ATTACHED (or RESUMED): the worker serves the session.
    Attached = 1,
    /// DRAIN received; consuming the inbound tail to dry.
    Draining = 2,
    /// DRAINED declared and the blob exported; nothing is consumed or
    /// emitted for the session until RESUME (return-to-service) or
    /// DETACH.
    Exported = 3,
    /// A relayed export is being reassembled.
    Importing = 4,
    /// The import committed; waiting for RESUME at a higher epoch.
    Imported = 5,
}

/// One attachment's Fluxor-side fencing state. It deliberately carries
/// NO session generation: the generation belongs to the MQTT session
/// record, and keeping it out of this struct is what makes rule 1
/// structural — there is no field a rebind could bump.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Binding {
    pub phase: BindingPhase,
    pub epoch: u32,
}

impl Binding {
    pub const fn idle() -> Self {
        Binding {
            phase: BindingPhase::Idle,
            epoch: 0,
        }
    }

    /// ATTACH at the anchor's epoch (1 for a fresh attachment).
    pub fn attach(epoch: u32) -> Self {
        Binding {
            phase: BindingPhase::Attached,
            epoch,
        }
    }

    /// The importing worker's binding once EXPORT_END committed: it
    /// holds the exporter's epoch and waits for the rebind.
    pub fn imported(export_epoch: u32) -> Self {
        Binding {
            phase: BindingPhase::Imported,
            epoch: export_epoch,
        }
    }

    /// Rule 4. Answer a RESUME carrying `new_epoch`.
    ///
    /// * `Imported` — the rebind; admitted only strictly above the
    ///   imported epoch, and the binding advances to it.
    /// * `Draining` / `Exported` — return-to-service of a refused
    ///   handoff; admitted only AT the current epoch (nothing was
    ///   committed anywhere, so nothing advanced).
    /// * anything else — not a state that can resume.
    pub fn resume(&mut self, new_epoch: u32) -> u8 {
        match self.phase {
            BindingPhase::Imported => {
                if new_epoch > self.epoch {
                    self.epoch = new_epoch;
                    self.phase = BindingPhase::Attached;
                    SI_STATUS_OK
                } else {
                    SI_STATUS_STALE_EPOCH
                }
            }
            BindingPhase::Draining | BindingPhase::Exported => {
                if new_epoch == self.epoch {
                    self.phase = BindingPhase::Attached;
                    SI_STATUS_OK
                } else {
                    SI_STATUS_STALE_EPOCH
                }
            }
            _ => SI_STATUS_STALE_EPOCH,
        }
    }

    /// Every session-scoped command names the epoch it was issued
    /// under; one below the binding's is stale.
    #[inline]
    pub fn admits_epoch(&self, epoch: u32) -> bool {
        epoch == self.epoch
    }
}

/// Rule 3. The generation an imported session resumes under, given the
/// blob's generation and what (if anything) the importing worker's
/// replicated state already says about that MQTT session.
///
/// Returns `(status, generation)`. On `SI_STATUS_STALE_EPOCH` the
/// generation is the LOCAL one the blob disagreed with, for the log.
pub fn reconcile_generation(blob_generation: u32, local: Option<u32>) -> (u8, u32) {
    match local {
        None => (SI_STATUS_OK, blob_generation),
        Some(g) if g == blob_generation => (SI_STATUS_OK, g),
        Some(g) => (SI_STATUS_STALE_EPOCH, g),
    }
}

/// Rule 2. The epoch a fresh attachment starts at. A new connection is
/// a new Fluxor session, whatever the MQTT session's generation.
pub const FIRST_EPOCH: u32 = 1;
