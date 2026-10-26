//! When a session's next client packet, or a delivery to it, may be taken
//! (`inflight_gate_core.rs`).
//!
//! The failures prevented: a pipelined publish dropped while the slot it
//! needs is about to be freed by a commit; a client packet waiting on a
//! slot only the client itself can free — what it sends next is behind
//! the packet that waits; a seated session's ring spent on a delivery so
//! an answer emitted once at commit finds it full; deliveries filling the
//! table so the client's publishes can never get a slot.

use super::inflight_gate::*;

const CAP: usize = 16;

fn publish(qos: u8) -> GateFlow {
    GateFlow {
        publish: true,
        qos,
        phase: 1,
        rec_sent: false,
    }
}

// A QoS 2 publish answered with its PUBREC, waiting for the client's PUBREL.
fn awaiting_pubrel() -> GateFlow {
    GateFlow {
        rec_sent: true,
        ..publish(2)
    }
}

// A QoS 2 flow whose PUBREL arrived and is being committed.
fn pubrel_on_commit() -> GateFlow {
    GateFlow {
        phase: GATE_QOS2_PUBREL,
        ..awaiting_pubrel()
    }
}

fn delivery(qos: u8) -> GateFlow {
    GateFlow {
        publish: false,
        ..publish(qos)
    }
}

#[test]
fn flows_being_committed_are_the_ones_answered_without_the_client() {
    assert!(on_commit(&publish(1)));
    assert!(on_commit(&publish(2)), "before its PUBREC");
    assert!(on_commit(&pubrel_on_commit()), "before its PUBCOMP");
    assert!(
        !on_commit(&awaiting_pubrel()),
        "waits on the client's PUBREL"
    );
    assert!(!on_commit(&delivery(1)), "waits on the client's PUBACK");
    assert!(!on_commit(&delivery(2)));
}

#[test]
fn a_publish_waits_on_a_full_table_only_for_a_commit() {
    let mut flows = vec![publish(1); CAP - 1];
    assert!(!publish_waits(&flows, CAP), "a slot is free");
    flows.push(publish(1));
    assert!(publish_waits(&flows, CAP));
    // A table of PUBRELs being committed: their PUBCOMPs free it.
    let flows = vec![pubrel_on_commit(); CAP];
    assert!(publish_waits(&flows, CAP));
    // Only flows waiting on the client: nothing would ever free a slot.
    let flows = vec![awaiting_pubrel(); CAP];
    assert!(!publish_waits(&flows, CAP));
    let mut flows = vec![awaiting_pubrel(); CAP / 2];
    flows.extend(vec![delivery(1); CAP / 2]);
    assert!(!publish_waits(&flows, CAP));
}

#[test]
fn a_seat_keeps_a_ring_slot_for_every_answer_a_commit_owes() {
    let flows = [
        publish(1),
        publish(2),
        pubrel_on_commit(),
        awaiting_pubrel(),
        delivery(1),
    ];
    assert_eq!(answers_owed(&flows), 3);
    assert!(!seat_may_take(3, &flows), "every free slot is owed");
    assert!(seat_may_take(4, &flows));
    assert!(seat_may_take(1, &[]));
    assert!(!seat_may_take(0, &[]));
}

#[test]
fn deliveries_leave_half_the_table_to_the_publishes() {
    let mut flows = vec![delivery(1); CAP / 2 - 1];
    assert!(delivery_may_take(1, &flows, CAP));
    flows.push(delivery(2));
    assert!(!delivery_may_take(1, &flows, CAP));
    assert!(!delivery_may_take(2, &flows, CAP));
    assert!(delivery_may_take(0, &flows, CAP), "QoS 0 takes no slot");
    // The client's publishes do not count against the deliveries' half.
    let flows = vec![publish(1); CAP - 1];
    assert!(delivery_may_take(1, &flows, CAP));
}
