// Bounded, no_std, no-alloc IN-FLIGHT GATE core — when a session's next
// client packet, or a delivery to it, may be taken, judged from its
// in-flight table. `include!`d by `session_processor` and by the host
// test crate; it names nothing outside this file.
//
// A client packet waits only on what clears without anything more from
// the client — a commit, or the anchor's horizon — because what the
// client sends next is behind the packet that waits. So the gate counts
// the client's publish flows being committed: each will be answered, and
// frees or moves on its slot, by the commit alone.

/// The QoS 2 phase of a publisher flow whose PUBREL is being committed.
pub const GATE_QOS2_PUBREL: u8 = 2;

/// One active flow of a session's in-flight table.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct GateFlow {
    /// The client's publish (else a delivery to it).
    pub publish: bool,
    pub qos: u8,
    /// QoS 2 phase.
    pub phase: u8,
    /// The PUBREC was sent.
    pub rec_sent: bool,
}

/// Whether `f` is a publish flow of the client's being committed: a QoS 1
/// PUBLISH, a QoS 2 PUBLISH before its PUBREC, a QoS 2 PUBREL before its
/// PUBCOMP. Its answer comes, and its slot frees or moves on, on the
/// commit alone.
pub fn on_commit(f: &GateFlow) -> bool {
    f.publish && (f.qos == 1 || (f.qos == 2 && (!f.rec_sent || f.phase == GATE_QOS2_PUBREL)))
}

/// Answers the session's flows being committed will need.
pub fn answers_owed(flows: &[GateFlow]) -> usize {
    flows.iter().filter(|f| on_commit(f)).count()
}

/// Whether the client's next QoS 1/2 PUBLISH waits for a slot: the table
/// of `cap` is full and a flow being committed will free or move one.
/// A full table with none such — only flows that wait on the client — is
/// one the publish cannot wait for.
pub fn publish_waits(flows: &[GateFlow], cap: usize) -> bool {
    flows.len() >= cap && flows.iter().any(on_commit)
}

/// Whether a seated session may spend a production ring slot on anything
/// but an owed answer: `room` free slots, more than its flows being
/// committed will need.
pub fn seat_may_take(room: usize, flows: &[GateFlow]) -> bool {
    room > answers_owed(flows)
}

/// Whether a delivery at `qos` may take an in-flight slot: deliveries
/// leave half of the table of `cap` to the client's publishes, so a full
/// table always holds one a commit will free.
pub fn delivery_may_take(qos: u8, flows: &[GateFlow], cap: usize) -> bool {
    qos == 0 || flows.iter().filter(|f| !f.publish).count() < cap / 2
}
