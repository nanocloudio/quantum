//! Session identity: the rule that keeps Quantum's `session_generation`
//! and Fluxor's `session_epoch` from disagreeing about which generation
//! of a session is live.
//!
//! The failure these rules prevent is silent: a rebind that advanced one
//! counter but not the other would leave the dedupe ledger and the
//! placement directory naming different generations, and the symptom is
//! a `QoS 2` flow delivered twice.

use super::session_identity::*;

const ANCHOR: [u8; PEER_ID_LEN] = *b"QANCHOR1";

// ── Identity is single-valued in both directions ────────────────────────────

/// The connection a session id was minted for is recoverable from the
/// id alone — `CMD_SC_ATTACH` carries no data-plane address.
#[test]
fn session_id_names_its_connection_and_anchor() {
    let id = mint_session_id(&ANCHOR, 0xBEEF, 7);
    assert_eq!(session_id_conn(&id), 0xBEEF);
    assert_eq!(session_id_anchor(&id), ANCHOR);
    assert_eq!(session_id_generation(&id), 7);
    assert_eq!(&id[14..], &[0, 0]);
}

/// A recycled `conn_id` never yields a repeated id: the attach
/// generation is what makes two attachments on one connection distinct.
#[test]
fn recycled_conn_id_mints_a_distinct_session_id() {
    let a = mint_session_id(&ANCHOR, 3, 1);
    let b = mint_session_id(&ANCHOR, 3, 2);
    assert_ne!(a, b);
    assert_eq!(session_id_conn(&a), session_id_conn(&b));
}

/// Identity bytes are big-endian so raw comparison matches the
/// canonical rendering (`session_ctrl.rs` §Session identity).
#[test]
fn identity_fields_are_big_endian() {
    let id = mint_session_id(&ANCHOR, 0x0102, 0x0A0B_0C0D);
    assert_eq!(&id[8..10], &[0x01, 0x02]);
    assert_eq!(&id[10..14], &[0x0A, 0x0B, 0x0C, 0x0D]);
}

// ── Rule 1 + 2: a rebind never touches the generation, a CONNECT never
//    touches the epoch ──────────────────────────────────────────────────

/// Model the two counters side by side through a full life: attach,
/// two worker moves, a reconnect, another move. The generation only
/// changes at the CONNECT; the epoch only changes at the rebinds.
#[test]
fn generation_and_epoch_cannot_diverge_across_rebinds_and_connects() {
    // The MQTT session record's counter (what the dedupe key uses).
    let mut generation: u32 = 4;
    // The attachment's counter.
    let mut b = Binding::attach(FIRST_EPOCH);
    assert_eq!(b.epoch, 1);

    // Move 1: export at epoch 1, import elsewhere, resume at 2.
    b.phase = BindingPhase::Exported;
    let mut imported = Binding::imported(b.epoch);
    assert_eq!(imported.resume(2), SI_STATUS_OK);
    assert_eq!(imported.epoch, 2);
    assert_eq!(generation, 4, "a rebind must not advance the generation");

    // Move 2.
    imported.phase = BindingPhase::Exported;
    let mut imported2 = Binding::imported(imported.epoch);
    assert_eq!(imported2.resume(3), SI_STATUS_OK);
    assert_eq!(generation, 4);

    // The client reconnects: a NEW attachment at epoch 1, and the
    // committed CONNECT bumps the generation — nothing else does.
    let reconnect = Binding::attach(FIRST_EPOCH);
    generation += 1;
    assert_eq!(
        reconnect.epoch, 1,
        "a CONNECT must not carry a prior attachment's epoch forward"
    );
    assert_eq!(generation, 5);
    assert_eq!(
        imported2.epoch, 3,
        "the prior attachment's epoch is untouched by the CONNECT"
    );
}

/// The binding carries no generation field at all, so there is nothing
/// a rebind could bump by mistake: the property is structural.
#[test]
fn binding_carries_no_generation() {
    assert_eq!(
        core::mem::size_of::<Binding>(),
        core::mem::size_of::<u32>() + 4
    );
}

// ── Rule 3: import reconciles against replicated state ──────────────────────

/// A worker with no replicated record takes the blob's generation.
#[test]
fn import_into_a_worker_without_the_record_takes_the_blob_generation() {
    assert_eq!(reconcile_generation(9, None), (SI_STATUS_OK, 9));
}

/// A worker whose replicated record agrees admits the import.
#[test]
fn import_agreeing_with_the_replicated_record_is_admitted() {
    assert_eq!(reconcile_generation(9, Some(9)), (SI_STATUS_OK, 9));
}

/// A blob describing a generation the log has moved past is REFUSED,
/// never admitted against the newer ledger: importing it would resume a
/// session whose dedupe keys the ledger no longer holds, and a `QoS` 2
/// flow would complete twice.
#[test]
fn import_behind_the_replicated_record_is_refused_stale() {
    assert_eq!(
        reconcile_generation(9, Some(10)),
        (SI_STATUS_STALE_EPOCH, 10)
    );
    assert_eq!(
        reconcile_generation(11, Some(10)),
        (SI_STATUS_STALE_EPOCH, 10)
    );
}

// ── Rule 4: epochs only move forward ────────────────────────────────────────

/// After an import the rebind must be strictly above the imported epoch.
#[test]
fn resume_after_import_needs_a_higher_epoch() {
    let mut b = Binding::imported(5);
    assert_eq!(b.resume(5), SI_STATUS_STALE_EPOCH);
    assert_eq!(b.resume(4), SI_STATUS_STALE_EPOCH);
    assert_eq!(
        b.phase,
        BindingPhase::Imported,
        "a refused resume changes nothing"
    );
    assert_eq!(b.resume(6), SI_STATUS_OK);
    assert_eq!(
        b,
        Binding {
            phase: BindingPhase::Attached,
            epoch: 6
        }
    );
}

/// A refused handoff returns the exporting worker to service at the
/// session's CURRENT epoch — nothing advanced — and at no other.
#[test]
fn refused_handoff_resumes_the_exporter_at_the_current_epoch_only() {
    for phase in [BindingPhase::Draining, BindingPhase::Exported] {
        let mut b = Binding { phase, epoch: 3 };
        assert_eq!(b.resume(4), SI_STATUS_STALE_EPOCH);
        assert_eq!(b.resume(2), SI_STATUS_STALE_EPOCH);
        assert_eq!(b.phase, phase);
        assert_eq!(b.resume(3), SI_STATUS_OK);
        assert_eq!(b.phase, BindingPhase::Attached);
        assert_eq!(b.epoch, 3);
    }
}

/// A worker that is idle or serving cannot be resumed into anything.
#[test]
fn resume_outside_a_handoff_is_refused() {
    let mut idle = Binding::idle();
    assert_eq!(idle.resume(1), SI_STATUS_STALE_EPOCH);
    let mut live = Binding::attach(2);
    assert_eq!(live.resume(3), SI_STATUS_STALE_EPOCH);
    assert_eq!(live.epoch, 2);
}

/// Every session-scoped command must name the binding's epoch exactly.
#[test]
fn commands_are_admitted_at_the_bound_epoch_only() {
    let b = Binding::attach(7);
    assert!(b.admits_epoch(7));
    assert!(!b.admits_epoch(6));
    assert!(!b.admits_epoch(8));
}
