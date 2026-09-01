//! Tests for what a Metadata response says about the cluster.
//!
//! The bug these guard: advertising one broker that leads everything,
//! which tells a Kafka client the cluster is a single machine and stops
//! it distributing load no matter how the substrate partitions.

use crate::kafka_metadata::*;

fn cluster(self_id: u8, n: u8, leader: u8) -> MetadataView {
    let mut v = MetadataView::single(self_id, 9090);
    v.peer_count = n;
    v.peer_ports = [9090, 9091, 9092, 0, 0, 0, 0];
    v.leader_hint = leader;
    v
}

/// The single-node default must be unchanged: one broker, itself.
/// Every existing deployment depends on this answer.
#[test]
fn a_single_node_advertises_only_itself() {
    let v = MetadataView::single(0, 9090);
    assert_eq!(v.broker_count(), 1);
    assert_eq!(
        v.broker(0),
        Some(Broker {
            node_id: 0,
            port: 9090
        })
    );
    assert_eq!(v.broker(1), None);
    assert_eq!(v.leader_for(0), 0);
}

/// A three-node cluster must name all three, so a client can reach any
/// of them. This is the whole point.
#[test]
fn a_cluster_advertises_every_broker() {
    let v = cluster(0, 3, 0);
    assert_eq!(v.broker_count(), 3);
    assert_eq!(
        v.broker(0),
        Some(Broker {
            node_id: 0,
            port: 9090
        })
    );
    assert_eq!(
        v.broker(1),
        Some(Broker {
            node_id: 1,
            port: 9091
        })
    );
    assert_eq!(
        v.broker(2),
        Some(Broker {
            node_id: 2,
            port: 9092
        })
    );
    assert_eq!(v.broker(3), None);
}

/// The leader is the RAFT leader, not always self — that is what sends
/// a client's writes to the node that can serve them.
#[test]
fn the_leader_is_the_raft_leader_not_self() {
    let v = cluster(2, 3, 1);
    assert_eq!(v.leader_for(0), 1);
    assert_eq!(v.leader_for(5), 1, "one raft group leads every partition");
}

/// Before any hint arrives, answer SELF. This node is serving the
/// request, so it can serve the partition; reporting "unknown" would
/// make a client back off from a broker that demonstrably works, and
/// it reproduces the previous single-broker behaviour exactly.
#[test]
fn an_unknown_leader_falls_back_to_self_not_to_minus_one() {
    let v = cluster(2, 3, HINT_UNKNOWN);
    assert_eq!(v.leader_for(0), 2);
}

/// A leader outside the advertised set cannot be reached by a client,
/// so naming it would send the client nowhere. Say unknown instead.
#[test]
fn a_leader_outside_the_advertised_set_is_unknown() {
    let v = cluster(0, 3, 5);
    assert_eq!(v.leader_for(0), LEADER_UNKNOWN);
}

/// Every partition of one topic reports the same leader today, because
/// one Raft group serves them all. Pinning this makes the change
/// visible when placement starts answering per shard.
#[test]
fn every_partition_reports_the_same_leader_today() {
    let v = cluster(0, 3, 1);
    let leaders: [i32; 4] = [
        v.leader_for(0),
        v.leader_for(1),
        v.leader_for(2),
        v.leader_for(3),
    ];
    assert!(leaders.iter().all(|&l| l == 1), "leaders={leaders:?}");
}

/// A misconfigured `peer_count` must never yield zero brokers: a
/// response naming none is useless to a client.
#[test]
fn broker_count_is_never_zero_and_never_exceeds_the_cap() {
    let mut v = cluster(0, 0, 0);
    assert_eq!(v.broker_count(), 1);
    v.peer_count = 99;
    assert_eq!(v.broker_count(), MAX_BROKERS);
}

/// An unconfigured peer port must not renumber the brokers after it.
/// Kafka identifies a broker by `node_id`, so a client reconnecting on
/// a renumbered id would reach the wrong node.
#[test]
fn an_unconfigured_peer_keeps_its_node_id() {
    let mut v = cluster(0, 4, 0);
    v.peer_ports[3] = 0;
    assert_eq!(v.broker(3).map(|b| b.node_id), Some(3));
    assert_eq!(
        v.broker(2),
        Some(Broker {
            node_id: 2,
            port: 9092
        })
    );
}

/// Replicas and ISR span every node hosting the group — not `[0]`.
#[test]
fn replicas_span_the_cluster() {
    assert_eq!(cluster(0, 3, 0).replica_count(), 3);
    assert_eq!(MetadataView::single(0, 9090).replica_count(), 1);
}
