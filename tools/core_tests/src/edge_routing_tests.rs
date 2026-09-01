//! Edge plane: the rules that decide whether a client's work proceeds
//! on the node it happened to reach, and whether two groups can ever
//! accept for the same shard at once.
//!
//! The failure these rules prevent is not "a slow hop" — it is a frame
//! acted on under a placement that has been revoked, which is how two
//! PRGs end up owning the same shard.

use super::edge_routing::*;

/// Returns an `EdgeMap`, not a bare `EdgeView`: ownership lives on the
/// map so a caller cannot answer "who owns this shard?" while ignoring
/// the override table. `classify` still takes the transport view, hence
/// `.view` at those call sites.
fn view(local_prg: u16, epoch: u32) -> EdgeMap {
    EdgeMap::new(local_prg, epoch)
}

// ── EDGE-ANYNODE: a hop is not an error ─────────────────────────────────────

/// A frame for a shard this node owns is handled here.
#[test]
fn an_owned_shard_is_handled_locally() {
    assert_eq!(classify(&view(2, 5).view, 2, 5), EdgeAction::Local);
}

/// A frame for someone else's shard is FORWARDED, not refused. Landing
/// on a non-owner must cost a hop, never an error — that is the whole
/// point of decoupling connections from leadership.
#[test]
fn a_foreign_shard_is_forwarded_not_refused() {
    assert_eq!(classify(&view(2, 5).view, 7, 5), EdgeAction::Forward(7));
}

/// A plain client states no epoch, and must never be treated as stale
/// for it — clients do not know about placement epochs at all.
#[test]
fn a_client_without_an_epoch_is_not_stale() {
    assert_eq!(classify(&view(2, 9).view, 2, 0), EdgeAction::Local);
    assert_eq!(classify(&view(2, 9).view, 7, 0), EdgeAction::Forward(7));
}

// ── EDGE-EPOCH: never decide from a map you know is behind ──────────────────

/// A sender ahead of us means our map is stale. Refresh and retry —
/// answering now would route from a placement we already know is
/// superseded.
#[test]
fn a_newer_sender_forces_a_refresh() {
    assert_eq!(
        classify(&view(2, 5).view, 2, 6),
        EdgeAction::RefreshThenRetry,
        "even for a shard we believe we own"
    );
}

/// A sender behind us is acting on a revoked placement and is told so,
/// with the current owner.
#[test]
fn a_stale_sender_is_redirected_to_the_current_owner() {
    assert_eq!(classify(&view(2, 9).view, 7, 8), EdgeAction::Redirect(7));
}

/// The epoch check comes BEFORE ownership. A stale sender must be
/// redirected even when this node happens to be the owner under the
/// current map — otherwise it keeps acting on the old placement and
/// never learns.
#[test]
fn the_epoch_check_precedes_ownership() {
    assert_eq!(
        classify(&view(2, 9).view, 2, 8),
        EdgeAction::Redirect(2),
        "stale sender, current owner is us: still tell it the epoch moved"
    );
}

// ── EDGE-FENCE: the losing owner stops before the gaining one starts ────────

/// A PRG mid-transfer is briefly unavailable, whether this node is the
/// one losing it or the one gaining it. Accepting at EITHER end during
/// the fence would split ownership, which is the failure the whole
/// migration design exists to prevent.
#[test]
fn a_fenced_prg_is_unavailable_at_both_ends() {
    let mut v = view(2, 5);
    v.view.fenced_active = true;
    v.view.fenced_prg = 2;
    // This node IS the fenced PRG: it has stopped accepting.
    assert_eq!(classify(&v.view, 2, 5), EdgeAction::Fenced, "at the source");
    // Seen from elsewhere, the fenced PRG is not forwarded to either.
    let mut w = view(7, 5);
    w.view.fenced_active = true;
    w.view.fenced_prg = 2;
    assert_eq!(classify(&w.view, 2, 5), EdgeAction::Fenced, "from a peer");
}

/// The fence applies to the named PRG only — a migration must not stall
/// traffic for groups it did not touch.
#[test]
fn the_fence_is_scoped_to_one_prg() {
    let mut v = view(2, 5);
    v.view.fenced_active = true;
    v.view.fenced_prg = 9;
    assert_eq!(classify(&v.view, 2, 5), EdgeAction::Local);
    assert_eq!(classify(&v.view, 7, 5), EdgeAction::Forward(7));
}

/// An inactive fence is inert even when it names this PRG, so clearing
/// the flag is enough to lift a fence.
#[test]
fn an_inactive_fence_does_not_bite() {
    let mut v = view(2, 5);
    v.view.fenced_active = false;
    v.view.fenced_prg = 2;
    assert_eq!(classify(&v.view, 2, 5), EdgeAction::Local);
}

/// An epoch mismatch outranks the fence: a sender on the wrong epoch
/// needs to learn that first, and its retry will meet the fence if the
/// fence still stands.
#[test]
fn epoch_disposition_outranks_the_fence() {
    let mut v = view(2, 5);
    v.view.fenced_active = true;
    v.view.fenced_prg = 2;
    assert_eq!(classify(&v.view, 2, 6), EdgeAction::RefreshThenRetry);
    assert_eq!(classify(&v.view, 2, 4), EdgeAction::Redirect(2));
}

// ── EDGE-NOAFFINITY: the redirect style is per protocol ─────────────────────

/// MQTT and AMQP rebalances are invisible to the client. Both are
/// session-oriented: the connection carries subscriptions, inflight `QoS`
/// state and delivery credit that a reconnect would destroy.
#[test]
fn session_protocols_are_redirected_transparently() {
    assert_eq!(redirect_style(PROTO_MQTT), RedirectStyle::Transparent);
    assert_eq!(redirect_style(PROTO_AMQP), RedirectStyle::Transparent);
}

/// Kafka clients already implement a metadata-refresh loop on a
/// retryable error, so a rebalance uses that rather than a new
/// mechanism.
#[test]
fn kafka_uses_the_retry_loop_its_drivers_already_have() {
    assert_eq!(redirect_style(PROTO_KAFKA), RedirectStyle::ClientRetry);
}

/// An unknown protocol defaults to transparent — forwarding is always
/// safe, whereas telling a client to retry a protocol that has no such
/// notion is not.
#[test]
fn an_unknown_protocol_defaults_to_the_safe_style() {
    assert_eq!(redirect_style(200), RedirectStyle::Transparent);
}

// ── Epoch adoption ──────────────────────────────────────────────────────────

/// Epochs advance only. A late-arriving older epoch must not un-fence a
/// shard whose transfer has already completed.
#[test]
fn epoch_adoption_is_monotone() {
    assert!(accept_epoch(5, 6));
    assert!(!accept_epoch(5, 5), "a re-delivery is not progress");
    assert!(!accept_epoch(5, 4), "and a late older one is not either");
    assert!(accept_epoch(0, 1), "the first epoch is adopted");
}

/// Sweep: for any pair of views and any sender epoch, the decision is
/// one of the five actions and never panics — a listener classifies
/// every frame it receives, including malformed ones.
#[test]
fn classification_is_total() {
    for local in 0..4u16 {
        for epoch in 0..4u32 {
            for owner in 0..4u16 {
                for sender in 0..6u32 {
                    let mut v = view(local, epoch);
                    v.view.fenced_active = (owner % 2) == 0;
                    v.view.fenced_prg = owner;
                    let _ = classify(&v.view, owner, sender);
                }
            }
        }
    }
}

// ── W6: the Kafka half is an error code, not a proxy ───────────────────────

/// A request this node owns is served here — no error, no redirect.
#[test]
fn a_local_kafka_request_gets_no_error() {
    assert_eq!(kafka_error_for(EdgeAction::Local), None);
}

/// A partition owned elsewhere answers `NOT_LEADER_OR_FOLLOWER`. The
/// driver refreshes metadata and retries against the right broker —
/// the loop it already implements — so Kafka needs no forward-and-proxy
/// transport at all.
#[test]
fn a_foreign_partition_answers_not_leader() {
    assert_eq!(
        kafka_error_for(EdgeAction::Forward(3)),
        Some(KERR_NOT_LEADER_OR_FOLLOWER)
    );
    assert_eq!(
        kafka_error_for(EdgeAction::Redirect(2)),
        Some(KERR_NOT_LEADER_OR_FOLLOWER)
    );
}

/// Mid-handover and stale-view are TRANSIENT and have no better owner
/// to name yet, so they answer `LEADER_NOT_AVAILABLE` — retry, but do not
/// pin the client to a node that is mid-handover.
#[test]
fn transient_states_answer_leader_not_available() {
    assert_eq!(
        kafka_error_for(EdgeAction::Fenced),
        Some(KERR_LEADER_NOT_AVAILABLE)
    );
    assert_eq!(
        kafka_error_for(EdgeAction::RefreshThenRetry),
        Some(KERR_LEADER_NOT_AVAILABLE)
    );
}

/// Every disposition maps to something: a listener must answer every
/// request it classifies.
#[test]
fn every_disposition_maps() {
    for a in [
        EdgeAction::Local,
        EdgeAction::Forward(1),
        EdgeAction::Redirect(1),
        EdgeAction::Fenced,
        EdgeAction::RefreshThenRetry,
    ] {
        let e = kafka_error_for(a);
        assert!(
            e.is_none() || e != Some(KERR_NONE),
            "{a:?} mapped to a success code"
        );
    }
}

// ── Placement view: parsing an update, and resolving a shard's owner ──
//
// Both live in the core so that every module answering "do I own this
// shard?" reads one parser and one divisor. Two modules disagreeing
// about a shard's owner is the failure this prevents: the router places
// a publish on one PRG while the topic engine expects it on another.

/// `[epoch:u32][prg_count:u16][local_prg:u16][fenced_prg:u16][fenced:u8]`
fn update(epoch: u32, prg_count: u16, local_prg: u16, fenced_prg: u16, fenced: bool) -> [u8; 11] {
    let mut b = [0u8; 11];
    b[0..4].copy_from_slice(&epoch.to_le_bytes());
    b[4..6].copy_from_slice(&prg_count.to_le_bytes());
    b[6..8].copy_from_slice(&local_prg.to_le_bytes());
    b[8..10].copy_from_slice(&fenced_prg.to_le_bytes());
    b[10] = u8::from(fenced);
    b
}

#[test]
fn a_full_update_sets_every_field() {
    let mut v = EdgeMap::new(0, 0);
    assert_eq!(
        apply_placement_update(&mut v.view, &update(7, 4, 2, 3, true)),
        PlacementUpdate::Applied
    );
    assert_eq!(v.view.epoch, 7);
    assert_eq!(v.view.prg_count, 4);
    assert_eq!(v.view.local_prg, 2);
    assert_eq!(v.view.fenced_prg, 3);
    assert!(v.view.fenced_active);
}

/// STRICTLY newer. An update at the epoch already held must NOT apply:
/// two placements published at one epoch would both take effect and the
/// second would silently win, at which point the epoch has stopped
/// identifying a placement.
#[test]
fn an_update_at_the_epoch_held_is_stale() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(5, 4, 1, 0, false));
    assert_eq!(
        apply_placement_update(&mut v.view, &update(5, 9, 8, 0, false)),
        PlacementUpdate::Stale
    );
    assert_eq!(v.view.prg_count, 4, "a stale update must change nothing");
    assert_eq!(v.view.local_prg, 1);
}

#[test]
fn an_older_update_cannot_rewind_the_view() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(9, 4, 1, 0, false));
    assert_eq!(
        apply_placement_update(&mut v.view, &update(2, 1, 0, 0, false)),
        PlacementUpdate::Stale
    );
    assert_eq!(v.view.epoch, 9);
}

/// Each tail is independently optional: a sender that knows only the
/// epoch leaves placement alone rather than zeroing it.
#[test]
fn an_epoch_only_update_leaves_placement_alone() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 2, 0, false));
    let mut just_epoch = [0u8; 4];
    just_epoch.copy_from_slice(&9u32.to_le_bytes());
    assert_eq!(
        apply_placement_update(&mut v.view, &just_epoch),
        PlacementUpdate::Applied
    );
    assert_eq!(v.view.epoch, 9);
    assert_eq!(v.view.prg_count, 4);
    assert_eq!(v.view.local_prg, 2);
}

/// `prg_count == 0` means "unchanged", NOT "no groups" — a zero divisor
/// would panic rather than merely misroute.
#[test]
fn a_zero_prg_count_means_unchanged() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 8, 0, 0, false));
    apply_placement_update(&mut v.view, &update(2, 0, 0, 0, false));
    assert_eq!(v.view.prg_count, 8);
}

/// The sentinel exists so a fence-only update cannot silently reassign
/// this node to PRG 0.
#[test]
fn the_unchanged_sentinel_preserves_the_local_prg() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 3, 0, false));
    apply_placement_update(&mut v.view, &update(2, 4, PLACEMENT_UNCHANGED, 1, true));
    assert_eq!(v.view.local_prg, 3, "PLACEMENT_UNCHANGED must not reassign");
    assert!(v.view.fenced_active);
}

/// An update that names no fence CLEARS one — the transfer that raised
/// it has completed. Without this a fence outlives its migration.
#[test]
fn an_update_without_a_fence_tail_clears_the_fence() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 0, 2, true));
    assert!(v.view.fenced_active);
    apply_placement_update(&mut v.view, &update(2, 4, 0, 0, false)[..8]);
    assert!(
        !v.view.fenced_active,
        "a fenceless update must lift the fence"
    );
}

#[test]
fn a_payload_too_short_for_an_epoch_is_malformed() {
    let mut v = EdgeMap::new(1, 5);
    assert_eq!(
        apply_placement_update(&mut v.view, &[0u8; 3]),
        PlacementUpdate::Malformed
    );
    assert_eq!(v.view.epoch, 5, "a malformed frame must change nothing");
}

/// A node that has heard no placement owns everything it is asked
/// about. Defaulting to more groups than exist would place state on
/// PRGs that do not exist, and it would be dropped.
#[test]
fn an_unconfigured_view_owns_every_shard() {
    let v = EdgeMap::new(0, 0);
    assert_eq!(v.view.prg_count, DEFAULT_PRG_COUNT);
    for shard in [0u32, 1, 7, 1023, 262_143] {
        assert_eq!(v.owner_prg(shard), 0);
        assert!(v.owns_shard(shard));
    }
}

#[test]
fn shards_spread_across_the_configured_prgs() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 0, 0, false));
    // Jump consistent hash over 4 PRGs (see `baseline_is_jump_consistent_hash`).
    assert_eq!(v.owner_prg(0), 0);
    assert_eq!(v.owner_prg(2), 3);
    assert_eq!(v.owner_prg(4), 1);
    assert_eq!(v.owner_prg(6), 2);
    assert!(v.owns_shard(0));
    assert!(!v.owns_shard(4), "shard 4 belongs to PRG 1, not this node");
}

/// Ownership is not just "the arithmetic says mine": a shard whose PRG
/// is mid-fence is owned by NOBODY until the transfer completes.
/// Serving it at either end is how two groups accept for one shard.
#[test]
fn a_fenced_shard_is_not_owned_even_by_its_own_prg() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 0, 0, true));
    assert_eq!(v.owner_prg(0), 0);
    assert_eq!(v.view.local_prg, 0);
    assert!(
        !v.owns_shard(0),
        "the local PRG being fenced must withdraw ownership, not keep it"
    );

    // A fence on a DIFFERENT PRG leaves this node's own shards alone.
    // Needs a second view: with the fence on PRG 0 above, EVERY shard
    // this node owns is by definition fenced, so there is no shard in
    // the first view that can show the negative.
    let mut elsewhere = EdgeMap::new(0, 0);
    apply_placement_update(&mut elsewhere.view, &update(1, 4, 0, 2, true));
    assert_eq!(elsewhere.owner_prg(0), 0);
    assert!(
        elsewhere.owns_shard(0),
        "a fence on PRG 2 must not withdraw PRG 0's shards"
    );
    assert!(!elsewhere.owns_shard(2), "shard 2 is on the fenced PRG");
}

/// `owns_shard` and `classify` must agree — they are the same question
/// asked by a state store and by a listener, and a disagreement is two
/// components acting on different owners for one shard.
#[test]
fn owns_shard_agrees_with_classify() {
    let mut v = EdgeMap::new(1, 0);
    apply_placement_update(&mut v.view, &update(3, 4, 1, 2, true));
    for shard in 0u32..64 {
        let owner = v.owner_prg(shard);
        let local = classify(&v.view, owner, 0) == EdgeAction::Local;
        assert_eq!(
            v.owns_shard(shard),
            local,
            "shard {shard} (owner {owner}): owns_shard and classify disagree"
        );
    }
}

// ── is_local vs owns_shard ───────────────────────────────────────────
//
// The handover rests on these two NOT being the same question.
// Conflating them destroys state on a fence that a later abort was
// supposed to leave untouched.

/// A fenced shard is still MINE. Its state must survive, because an
/// aborted migration lifts the fence and leaves ownership unmoved.
#[test]
fn a_fenced_shard_is_still_local_even_though_unowned() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 0, 0, true));
    assert!(
        v.is_local(0),
        "a fence must not make a shard foreign — an abort would then \
         have destroyed its state for nothing"
    );
    assert!(
        !v.owns_shard(0),
        "a fenced shard must not be served while the transfer is open"
    );
}

/// A reassigned shard is neither local nor owned: its state is released
/// AND it stops being served.
#[test]
fn a_reassigned_shard_is_neither_local_nor_owned() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 0, 0, false));
    assert!(!v.is_local(4), "shard 4 maps to PRG 1, not this node");
    assert!(!v.owns_shard(4));
}

/// With no fence the two agree exactly — the distinction costs nothing
/// in the steady state, which is what makes it safe to apply everywhere.
#[test]
fn without_a_fence_is_local_and_owns_shard_agree() {
    let mut v = EdgeMap::new(2, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 2, 0, false));
    for shard in 0u32..64 {
        assert_eq!(
            v.is_local(shard),
            v.owns_shard(shard),
            "shard {shard} disagreed with no fence standing"
        );
    }
}

/// The release predicate must never fire for a shard that is merely
/// fenced, at any PRG count, for any shard. This is the invariant that
/// stops a migration abort from having destroyed live sessions.
#[test]
fn a_fence_never_makes_any_shard_releasable() {
    for prg_count in 1u16..8 {
        for fenced_prg in 0u16..prg_count {
            let mut v = EdgeMap::new(0, 0);
            apply_placement_update(&mut v.view, &update(1, prg_count, 0, fenced_prg, true));
            for shard in 0u32..128 {
                if v.owner_prg(shard) == 0 {
                    assert!(
                        v.is_local(shard),
                        "prg_count={prg_count} fenced={fenced_prg} shard={shard}: \
                         a fence made an owned shard releasable"
                    );
                }
            }
        }
    }
}

/// A routed message (a publish) is not gated by OWNERSHIP — the shard
/// key says where it lands, not who may accept it — but it IS gated by
/// the FENCE. Accepting for a shard mid-transfer at either end is the
/// split the fence exists to prevent, and dropping this distinction let
/// a publish through mid-migration.
#[test]
fn a_fenced_shard_is_fenced_for_every_node_not_just_its_owner() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 0, 2, true));
    // PRG 2 is fenced. Shard 6 belongs to it under the baseline.
    assert_eq!(v.owner_prg(6), 2);
    assert!(
        v.is_shard_fenced(6),
        "fenced at the node that does NOT own it"
    );
    assert!(!v.owns_shard(6), "and it is not owned here either");
    // A shard on an unfenced PRG is routable from anywhere.
    assert!(!v.is_shard_fenced(1));
    assert!(!v.is_shard_fenced(0));
}

#[test]
fn the_local_prg_being_fenced_fences_its_own_shards() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 0, 0, true));
    assert!(
        v.is_shard_fenced(0),
        "the local PRG's own shards are frozen too"
    );
    assert!(!v.owns_shard(0));
}

#[test]
fn no_shard_is_fenced_without_an_active_fence() {
    let mut v = EdgeMap::new(0, 0);
    apply_placement_update(&mut v.view, &update(1, 4, 0, 2, false));
    for shard in 0u32..8 {
        assert!(!v.is_shard_fenced(shard), "shard {shard}");
    }
}

// ── ROUTE-MAP: overrides refine ownership, not the raft partition ───────────

/// The map names the PRG that SERVES a shard; the shard's raft
/// partition is a separate namespace and is not touched by it.
#[test]
fn an_override_moves_a_shard_to_another_prg() {
    let mut m = EdgeMap::new(0, 0);
    apply_placement_update(&mut m.view, &update(1, 4, 0, 0, false));
    // Baseline: 4 PRGs, so shard 5 belongs to PRG 1.
    assert_eq!(m.owner_prg(5), 1);
    assert!(!m.owns_shard(5), "not this node's under the baseline");

    m.overrides.set(5, 0);
    assert_eq!(m.owner_prg(5), 0, "the override decides, not the baseline");
    assert!(m.owns_shard(5), "and it is now this node's");

    // Every other shard keeps its baseline — the map is SPARSE.
    assert_eq!(m.owner_prg(6), 2);
}

/// Clearing returns the shard to its baseline rather than to PRG 0.
/// Those differ for most shards, so a clear that reset to zero would
/// silently hand the shard to the wrong node.
#[test]
fn clearing_an_override_restores_the_baseline() {
    let mut m = EdgeMap::new(0, 0);
    apply_placement_update(&mut m.view, &update(1, 4, 0, 0, false));
    m.overrides.set(5, 0);
    assert_eq!(m.owner_prg(5), 0);
    m.overrides.set(5, SHARD_OVERRIDE_CLEAR);
    assert_eq!(m.owner_prg(5), 1, "back to the baseline");
    assert!(m.overrides.is_empty());
}

/// A fence still applies to an OVERRIDDEN owner. The override decides
/// who owns the shard; the fence decides whether anyone may act on it,
/// and conflating them would let a migration's own target accept during
/// the cutover it is fenced for.
#[test]
fn a_fence_still_covers_an_overridden_owner() {
    let mut m = EdgeMap::new(0, 0);
    apply_placement_update(&mut m.view, &update(1, 4, 0, 0, true));
    m.overrides.set(5, 0);
    assert_eq!(m.owner_prg(5), 0);
    assert!(m.is_shard_fenced(5), "fenced PRG 0 now owns shard 5");
    assert!(!m.owns_shard(5), "so it must not be acted on");
}

/// On overflow the override is REFUSED and the shard keeps its baseline
/// owner. Every node applies the same bound to the same map, so all of
/// them refuse the same entry and agree — accepting it on some nodes
/// and not others is what splits ownership.
#[test]
fn overflow_refuses_rather_than_evicting() {
    let mut m = EdgeMap::new(0, 0);
    apply_placement_update(&mut m.view, &update(1, 4, 0, 0, false));
    for shard in 0..MAX_SHARD_OVERRIDES {
        m.overrides
            .set(u32::try_from(shard).expect("bounded by the table size"), 0);
    }
    assert_eq!(m.overrides.len(), MAX_SHARD_OVERRIDES);
    let victim = u32::try_from(MAX_SHARD_OVERRIDES).expect("bounded") + 1;
    let baseline = m.owner_prg(victim);
    m.overrides.set(victim, 0);
    assert_eq!(m.overrides.refused, 1);
    assert_eq!(
        m.owner_prg(victim),
        baseline,
        "a refused override must leave the baseline standing, not half-apply"
    );
}

/// A stale map is ignored wholesale. Admitting one at or below the
/// epoch held would let a replayed update undo a newer placement.
#[test]
fn a_stale_shard_map_is_refused() {
    let mut m = EdgeMap::new(0, 0);
    assert!(m.overrides.admit_epoch(7));
    assert!(!m.overrides.admit_epoch(7), "equal is not newer");
    assert!(!m.overrides.admit_epoch(3));
    assert!(m.overrides.admit_epoch(8));
}

/// The baseline is jump consistent hash, and it must agree with the
/// control plane's copy in clustor: both are pinned to these vectors.
/// (`clustor/tests/keyed_routing.rs` holds the same table.)
#[test]
fn baseline_is_jump_consistent_hash() {
    let v3 = [0, 0, 0, 2, 1, 1, 2, 0, 0, 2, 2, 2];
    let v4 = [0, 0, 3, 3, 1, 1, 2, 0, 0, 2, 2, 2];
    let v7 = [0, 6, 6, 3, 1, 4, 5, 0, 4, 2, 6, 5];
    for shard in 0..12u32 {
        assert_eq!(
            baseline_prg(shard, 3),
            v3[shard as usize],
            "shard {shard} of 3"
        );
        assert_eq!(
            baseline_prg(shard, 4),
            v4[shard as usize],
            "shard {shard} of 4"
        );
        assert_eq!(
            baseline_prg(shard, 7),
            v7[shard as usize],
            "shard {shard} of 7"
        );
        assert_eq!(baseline_prg(shard, 1), 0);
        assert_eq!(
            baseline_prg(shard, 0),
            0,
            "0 means unchanged, never a divisor"
        );
    }
}

/// What the baseline is FOR: growing the PRG count moves only the
/// shards that land on the new PRG. Under modulo most of the space
/// would move.
#[test]
fn a_resize_moves_only_the_shards_the_new_prg_takes() {
    let mut moved = 0u32;
    for shard in 0..20_000u32 {
        let before = baseline_prg(shard, 3);
        let after = baseline_prg(shard, 4);
        if before != after {
            moved += 1;
            assert_eq!(after, 3, "a shard that moves lands on the NEW PRG");
        }
    }
    // 1/4 of the space, within sampling noise.
    assert!((4_500..5_500).contains(&moved), "moved {moved} of 20000");
}
