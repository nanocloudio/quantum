// Bounded, no_std, no-alloc KAFKA METADATA core — what a Metadata
// response should SAY about the cluster. `include!`d by the host test
// crate and by the protocol module that encodes the response.
//
// ## Why this is a core
//
// A Metadata response is how a client learns the shape of the cluster,
// and it is the only thing a client will believe: name one broker and
// every connection opens against that node, however well the substrate
// partitions underneath. The numbers are therefore worth testing on
// their own, without a broker to produce them.
//
// This core owns the DECISION — which brokers exist, and who leads a
// partition. The response encoding stays in the protocol module.

/// Brokers a Metadata response can name. Matches the substrate's
/// `MAX_NODES`; a cluster larger than this cannot be described by one
/// response anyway.
pub const MAX_BROKERS: usize = 7;

/// `node_id` meaning "no leader is known". Kafka uses -1 for an
/// unavailable leader, and clients treat it as "retry later" rather
/// than "connect to node -1".
pub const LEADER_UNKNOWN: i32 = -1;

/// Leader hint value meaning the raft leader is not yet known. Mirrors
/// `gateway/codec.rs`'s `LEADER_UNKNOWN` sentinel on the wire.
pub const HINT_UNKNOWN: u8 = 0xFF;

/// One advertised broker.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Broker {
    pub node_id: i32,
    pub port: u16,
}

/// The cluster as this node can describe it.
pub struct MetadataView {
    /// This node's replica id — the broker a client is talking to.
    pub self_id: u8,
    /// How many nodes the cluster has. 1 means "not configured for a
    /// cluster", which is the single-node default.
    pub peer_count: u8,
    /// Client-facing port per node, indexed by node id.
    pub peer_ports: [u16; MAX_BROKERS],
    /// Raft leader id, or [`HINT_UNKNOWN`] before the first hint.
    pub leader_hint: u8,
}

impl MetadataView {
    pub const fn single(self_id: u8, port: u16) -> Self {
        let mut peer_ports = [0u16; MAX_BROKERS];
        peer_ports[0] = port;
        Self {
            self_id,
            peer_count: 1,
            peer_ports,
            leader_hint: HINT_UNKNOWN,
        }
    }

    /// Brokers to advertise. Always at least one — a response naming no
    /// broker is useless to a client, and a misconfigured `peer_count`
    /// must not produce one.
    pub fn broker_count(&self) -> usize {
        (self.peer_count as usize).clamp(1, MAX_BROKERS)
    }

    /// The `i`th advertised broker.
    ///
    /// A port of 0 means the graph did not configure that peer. Such a
    /// broker is still NAMED — omitting it would renumber the ones
    /// after it, and Kafka identifies brokers by `node_id`, so a client
    /// that reconnects on a renumbered id reaches the wrong node. It is
    /// advertised with its own port instead, which is the honest
    /// answer: the id exists, this node cannot say where it listens.
    pub fn broker(&self, i: usize) -> Option<Broker> {
        if i >= self.broker_count() {
            return None;
        }
        let port = if self.peer_ports[i] != 0 {
            self.peer_ports[i]
        } else {
            self.peer_ports[self.self_id.min((MAX_BROKERS - 1) as u8) as usize]
        };
        Some(Broker {
            node_id: i as i32,
            port,
        })
    }

    /// Who leads a partition.
    ///
    /// With one Raft group serving all partitions, the leader of that
    /// group is the leader of each, so the raft leader is the exact
    /// answer rather than a placeholder. It stops being exact when
    /// K > 1 groups host different partitions, at which point the answer
    /// comes from placement per shard — hence the partition parameter,
    /// so the signature does not move when it starts mattering.
    ///
    /// Before any hint arrives, answers SELF rather than
    /// [`LEADER_UNKNOWN`]: this node is serving the request, so it can
    /// certainly serve the partition, and reporting "unknown" would
    /// make a client back off from a broker that works.
    pub fn leader_for(&self, _partition: u32) -> i32 {
        if self.leader_hint == HINT_UNKNOWN {
            return self.self_id as i32;
        }
        if (self.leader_hint as usize) >= self.broker_count() {
            // A leader outside the advertised set cannot be reached by a
            // client, so naming it would send the client nowhere.
            return LEADER_UNKNOWN;
        }
        self.leader_hint as i32
    }

    /// Replica set for a partition: every node hosting the group.
    /// Written into both `replicas` and `isr`.
    pub fn replica_count(&self) -> usize {
        self.broker_count()
    }
}
