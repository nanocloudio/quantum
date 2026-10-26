// Bounded, no_std, no-alloc DELIVERY ORDER core — where one delivery to a
// session goes so that the session receives its deliveries in the order
// they were published. `include!`d by `session_processor` and by the host
// test crate; it names nothing outside this file.
//
// A session's deliveries can be in three places: parked on the worker
// (waiting on something shared), in its offline queue (waiting on its
// own client, or while it was away), or delivered. The queue, while it
// drains, is the session's backlog: what of it the worker holds — a
// drain's answers, parked — is its head. So a fresh delivery to a
// session with a backlog joins the queue's tail, whatever is parked; a
// session's other deliveries go behind what it has parked; a drain's
// answer, being the queue's own, goes ahead of the queue.

/// Where a delivery comes from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum From {
    /// From the topic engine.
    Fresh,
    /// A retained read's answer: a retained set has no order of its own.
    Retained,
    /// An offline drain's answer: the session's queue, oldest first.
    Drained,
}

/// What to do with a delivery as it arrives.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Arrive {
    /// Offer it to the session; `ahead` of its offline queue, or else
    /// joining the queue while it drains.
    Offer { ahead: bool },
    /// Wait behind the session's parked deliveries.
    BehindParked,
}

/// Why an offered delivery could not go.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Waits {
    /// On something shared that clears without any client.
    Edge,
    /// On the session's own client.
    Client,
}

/// Where a delivery that could not go waits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Place {
    /// In the worker's park.
    Park,
    /// In the session's offline queue, with what it has parked moved
    /// ahead of it.
    Queue,
}

/// A delivery from `from` arrives for a session that has a draining
/// offline queue (`backlog`) and has deliveries parked here
/// (`parked`).
pub fn arrive(from: From, backlog: bool, parked: bool) -> Arrive {
    match from {
        // The backlog's tail, behind everything in the queue, its parked
        // head included.
        From::Fresh if backlog => Arrive::Offer { ahead: false },
        _ if parked => Arrive::BehindParked,
        From::Fresh => Arrive::Offer { ahead: false },
        From::Retained | From::Drained => Arrive::Offer { ahead: true },
    }
}

/// Where a delivery from `from` that could not go, for `why`, waits:
/// `park_room` says whether a park slot is free beyond those credited to
/// drains. A drain's answer always parks, in the slot its credit
/// reserved; anything else waiting on its client queues, so nothing
/// shared waits on one client.
pub fn place(from: From, why: Waits, park_room: bool) -> Place {
    match (from, why) {
        (From::Drained, _) => Place::Park,
        (_, Waits::Edge) if park_room => Place::Park,
        _ => Place::Queue,
    }
}

/// Where a parked delivery from `from`, offered again and still unable
/// to go for `why`, waits: kept in the park, or moved to its session's
/// queue with the session's later parked ones.
pub fn replace(from: From, why: Waits) -> Place {
    match (from, why) {
        (From::Drained, _) | (_, Waits::Edge) => Place::Park,
        (_, Waits::Client) => Place::Queue,
    }
}
