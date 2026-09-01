//! Kafka group coordinator: the rules that decide whether a dead
//! consumer's partitions are ever released, and whether a stale member
//! can act on an assignment the group has revoked.
//!
//! Only the broker can observe a member that stopped heartbeating, so
//! only the broker can free its partitions; left to the clients, a dead
//! consumer holds its assignment until some other client happens to
//! force a rebalance. These tests are written from that failure side.

use super::kafka_group::*;

fn stable_group_of(n: u64, now: u64, timeout: u32) -> Group {
    let mut g = Group::new();
    g.group_id_hash = 42;
    for i in 1..=n {
        assert!(g.join(now, i, timeout));
    }
    assert!(g.close_join_window());
    let gen = g.generation;
    for i in 1..=n {
        assert!(g.sync(i, gen));
    }
    assert_eq!(g.state, GROUP_STABLE);
    g
}

// ── Broker-owned expiry: the missing half ───────────────────────────────────

/// A member that stops heartbeating is evicted BY THE BROKER once its
/// deadline passes. This is the property whose absence let a dead
/// consumer hold partitions indefinitely.
#[test]
fn a_silent_member_is_evicted_by_the_broker() {
    let mut g = stable_group_of(3, 1_000, 10_000);
    // Two members keep heartbeating; the third goes silent.
    let gen = g.generation;
    assert!(g.heartbeat(5_000, 1, gen));
    assert!(g.heartbeat(5_000, 2, gen));

    assert_eq!(g.expire_due(10_500), 0, "nobody is past their deadline yet");
    assert_eq!(g.member_count(), 3);

    // Member 3's deadline was 1_000 + 10_000 = 11_000.
    assert_eq!(g.expire_due(11_000), 1, "the silent member is evicted");
    assert_eq!(g.member_count(), 2);
    assert!(!g.contains(3));
    assert_eq!(g.expired, 1);
}

/// Eviction opens a rebalance — freeing the partitions is the point, and
/// that only happens through a generation change.
#[test]
fn eviction_triggers_a_rebalance() {
    let mut g = stable_group_of(2, 0, 1_000);
    let before = g.generation;
    g.expire_due(5_000);
    assert!(g.generation > before, "the generation must advance");
    assert_eq!(g.state, GROUP_EMPTY, "both members expired");
}

/// Expiring several members at once evicts all of them — the compaction
/// in the removal loop must not skip a neighbour.
#[test]
fn simultaneous_expiry_evicts_every_member() {
    let mut g = stable_group_of(5, 0, 100);
    assert_eq!(g.expire_due(1_000), 5);
    assert_eq!(g.member_count(), 0);
}

/// A heartbeat genuinely extends the session, so a live member is never
/// evicted.
#[test]
fn a_heartbeating_member_is_never_evicted() {
    let mut g = stable_group_of(1, 0, 1_000);
    let gen = g.generation;
    for t in (500..10_000).step_by(500) {
        assert!(g.heartbeat(t, 1, gen), "t={t}");
        assert_eq!(g.expire_due(t), 0, "t={t}");
    }
    assert_eq!(g.member_count(), 1);
}

// ── Generations fence stale members ─────────────────────────────────────────

/// A heartbeat carrying an old generation is refused. A member that
/// missed a rebalance must not keep itself alive in a group that has
/// moved on — that is exactly how a revoked assignment stays live.
#[test]
fn a_stale_generation_heartbeat_is_refused() {
    let mut g = stable_group_of(2, 0, 10_000);
    let old_gen = g.generation;
    assert!(g.join(0, 3, 10_000)); // new member forces a rebalance
    assert!(g.generation > old_gen);

    assert!(
        !g.heartbeat(100, 1, old_gen),
        "a heartbeat from the previous generation must not refresh"
    );
    assert!(g.heartbeat(100, 1, g.generation), "the current one does");
}

/// Sync is refused at a stale generation too.
#[test]
fn a_stale_generation_sync_is_refused() {
    let mut g = Group::new();
    g.join(0, 1, 1_000);
    let old = g.generation;
    g.join(0, 2, 1_000); // bumps
    g.close_join_window();
    assert!(!g.sync(1, old));
    assert!(g.sync(1, g.generation));
}

/// The generation is monotone across every membership change — it is
/// what makes an old request identifiable as old.
#[test]
fn the_generation_only_advances() {
    let mut g = Group::new();
    let mut last = g.generation;
    for i in 1..=8u64 {
        g.join(0, i, 1_000);
        assert!(g.generation > last, "join {i}");
        last = g.generation;
    }
    for i in 1..=4u64 {
        g.leave(i);
        assert!(g.generation > last, "leave {i}");
        last = g.generation;
    }
    g.expire_due(u64::MAX);
    assert!(g.generation > last);
}

// ── Rejoin is not a membership change ───────────────────────────────────────

/// An existing member rejoining refreshes its deadline WITHOUT bumping
/// the generation. Bumping on every rejoin would let one flapping client
/// rebalance the group continuously.
#[test]
fn rejoining_does_not_rebalance() {
    let mut g = stable_group_of(3, 0, 1_000);
    let gen = g.generation;
    let rebalances = g.rebalances;
    assert!(g.join(500, 2, 1_000));
    assert_eq!(g.generation, gen, "a rejoin is not a membership change");
    assert_eq!(g.rebalances, rebalances);
    assert_eq!(g.member_count(), 3);

    // …but the deadline moved: at t=1400 the two members that joined at
    // t=0 are past their 1000ms session, while the rejoiner is not.
    assert_eq!(g.expire_due(1_400), 2);
    assert!(g.contains(2), "the rejoiner's session was extended");
    assert!(!g.contains(1));
    assert!(!g.contains(3));
}

// ── Leader selection is deterministic ───────────────────────────────────────

/// Every replica of a coordinator must pick the SAME leader from the
/// same membership, or two replicas would compute different assignments
/// for one generation.
#[test]
fn leader_selection_is_deterministic() {
    let mut a = Group::new();
    let mut b = Group::new();
    for id in [7u64, 3, 9, 1, 5] {
        a.join(0, id, 1_000);
    }
    // Same members, different arrival order.
    for id in [5u64, 1, 9, 3, 7] {
        b.join(0, id, 1_000);
    }
    assert_eq!(a.leader, b.leader);
    assert_eq!(a.leader, 1, "lowest member id");
}

/// The leader is re-picked when the current one leaves.
#[test]
fn the_leader_moves_when_it_leaves() {
    let mut g = stable_group_of(3, 0, 1_000);
    assert_eq!(g.leader, 1);
    g.leave(1);
    assert_eq!(g.leader, 2);
}

// ── Lifecycle ───────────────────────────────────────────────────────────────

/// The full join → sync → stable path.
#[test]
fn the_group_reaches_stable() {
    let g = stable_group_of(3, 0, 1_000);
    assert_eq!(g.state, GROUP_STABLE);
    assert_eq!(g.member_count(), 3);
}

/// A group is not stable until EVERY member has synced — a partial sync
/// would let some members act on an assignment others never received.
#[test]
fn partial_sync_does_not_reach_stable() {
    let mut g = Group::new();
    g.join(0, 1, 1_000);
    g.join(0, 2, 1_000);
    g.close_join_window();
    let gen = g.generation;
    assert!(g.sync(1, gen));
    assert_eq!(g.state, GROUP_COMPLETING_REBALANCE, "one member still out");
    assert!(g.sync(2, gen));
    assert_eq!(g.state, GROUP_STABLE);
}

/// An emptied group returns to EMPTY rather than lingering STABLE with
/// no members.
#[test]
fn an_emptied_group_returns_to_empty() {
    let mut g = stable_group_of(1, 0, 1_000);
    g.leave(1);
    assert_eq!(g.state, GROUP_EMPTY);
    assert_eq!(g.member_count(), 0);
    assert_eq!(g.leader, 0);
}

/// A full group refuses further members rather than evicting one — an
/// eviction here would drop a live consumer to admit a new one.
#[test]
fn a_full_group_refuses_new_members() {
    let mut g = Group::new();
    for i in 1..=MAX_MEMBERS as u64 {
        assert!(g.join(0, i, 1_000), "member {i}");
    }
    assert!(!g.join(0, MAX_MEMBERS as u64 + 1, 1_000));
    assert_eq!(g.member_count(), MAX_MEMBERS);
}

// ── Coordinator placement ───────────────────────────────────────────────────

/// A group's coordinator is decided by its shard, not by "whoever was
/// asked" — `FindCoordinator` answering "always me" is only right on a
/// single broker. Every broker must compute the same answer.
#[test]
fn coordinator_placement_is_a_pure_function_of_the_group() {
    let h = 0x1234_5678_9ABC_DEF0u64;
    let a = coordinator_shard(h, 1 << 18);
    let b = coordinator_shard(h, 1 << 18);
    assert_eq!(a, b);
    assert!(a < (1 << 18));
}

/// Different groups land on different shards, so coordination
/// distributes instead of piling onto one broker.
#[test]
fn distinct_groups_spread_across_shards() {
    let mut seen = std::collections::HashSet::new();
    for i in 0..64u64 {
        seen.insert(coordinator_shard(
            i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            1 << 18,
        ));
    }
    assert!(
        seen.len() > 32,
        "got {} distinct shards from 64 groups",
        seen.len()
    );
}
