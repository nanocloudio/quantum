//! Where a delivery to a session goes (`delivery_order_core.rs`).
//!
//! The failures prevented: a fresh delivery to a session whose offline
//! queue is draining parked behind the queue's parked head and so
//! delivered ahead of the rest of the queue (seen on the paired-anchor
//! rig under load as deliveries out of order); a delivery overtaking one
//! its session has parked; a drain's answer going anywhere but its
//! credited park slot; one client's slowness holding a park slot.

use super::delivery_order::*;

#[test]
fn a_fresh_delivery_to_a_draining_session_joins_the_queue_whatever_is_parked() {
    // The queue's head may be parked here as a drain's answers; the fresh
    // one belongs behind the whole queue, so it is offered to join it.
    assert_eq!(
        arrive(From::Fresh, true, true),
        Arrive::Offer { ahead: false }
    );
    assert_eq!(
        arrive(From::Fresh, true, false),
        Arrive::Offer { ahead: false }
    );
}

#[test]
fn any_other_delivery_waits_behind_what_its_session_has_parked() {
    assert_eq!(arrive(From::Fresh, false, true), Arrive::BehindParked);
    assert_eq!(arrive(From::Retained, false, true), Arrive::BehindParked);
    assert_eq!(arrive(From::Retained, true, true), Arrive::BehindParked);
    assert_eq!(arrive(From::Drained, true, true), Arrive::BehindParked);
}

#[test]
fn with_nothing_parked_answers_go_ahead_of_the_queue_and_fresh_ones_do_not() {
    assert_eq!(
        arrive(From::Fresh, false, false),
        Arrive::Offer { ahead: false }
    );
    assert_eq!(
        arrive(From::Retained, true, false),
        Arrive::Offer { ahead: true }
    );
    assert_eq!(
        arrive(From::Drained, true, false),
        Arrive::Offer { ahead: true }
    );
}

#[test]
fn a_drain_answer_always_parks_in_its_credited_slot() {
    for why in [Waits::Edge, Waits::Client] {
        for room in [false, true] {
            assert_eq!(place(From::Drained, why, room), Place::Park);
        }
        assert_eq!(replace(From::Drained, why), Place::Park);
    }
}

#[test]
fn a_wait_on_the_client_queues_and_a_shared_wait_parks_while_there_is_room() {
    for from in [From::Fresh, From::Retained] {
        assert_eq!(place(from, Waits::Client, true), Place::Queue);
        assert_eq!(place(from, Waits::Edge, true), Place::Park);
        assert_eq!(place(from, Waits::Edge, false), Place::Queue);
        assert_eq!(replace(from, Waits::Edge), Place::Park);
        assert_eq!(
            replace(from, Waits::Client),
            Place::Queue,
            "never holds a slot on a client"
        );
    }
}
