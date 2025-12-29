//! MQTT 5.0 flow control implementation.
//!
//! This module implements Receive Maximum flow control as defined in
//! MQTT 5.0 specification section 3.1.2.11.4 and 3.2.2.3.2.
//!
//! Tasks 11-13: Receive maximum flow control and quota exceeded handling.

use std::collections::{HashMap, VecDeque};

// ---------------------------------------------------------------------------
// Receive Maximum Flow Control (Task 11)
// ---------------------------------------------------------------------------

/// Flow control state for a single connection.
///
/// Tracks in-flight QoS 1 and QoS 2 messages that have been sent but not
/// yet acknowledged. The receive maximum limits how many can be outstanding.
#[derive(Debug, Clone)]
pub struct ReceiveMaximumController {
    /// Client's receive maximum (max unacknowledged QoS 1/2 messages we can send).
    client_receive_max: u16,
    /// Server's receive maximum (max unacknowledged QoS 1/2 messages client can send).
    server_receive_max: u16,
    /// Currently in-flight outbound packet IDs.
    outbound_inflight: VecDeque<u16>,
    /// Currently in-flight inbound packet IDs.
    inbound_inflight: VecDeque<u16>,
    /// Available outbound packet IDs (recycled after acknowledgment).
    available_packet_ids: VecDeque<u16>,
    /// Next packet ID to allocate if pool is empty.
    next_packet_id: u16,
}

impl Default for ReceiveMaximumController {
    fn default() -> Self {
        Self::new(65535, 65535)
    }
}

impl ReceiveMaximumController {
    /// Create a new controller with the given receive maximum values.
    ///
    /// - `client_receive_max`: Maximum QoS 1/2 messages we can send to the client
    /// - `server_receive_max`: Maximum QoS 1/2 messages the client can send to us
    pub fn new(client_receive_max: u16, server_receive_max: u16) -> Self {
        Self {
            // MQTT 5.0 spec: receive_max of 0 is a protocol error, default to 65535
            client_receive_max: if client_receive_max == 0 {
                65535
            } else {
                client_receive_max
            },
            server_receive_max: if server_receive_max == 0 {
                65535
            } else {
                server_receive_max
            },
            outbound_inflight: VecDeque::new(),
            inbound_inflight: VecDeque::new(),
            available_packet_ids: VecDeque::new(),
            next_packet_id: 1,
        }
    }

    /// Update client's receive maximum (from CONNACK).
    pub fn set_client_receive_max(&mut self, max: u16) {
        if max > 0 {
            self.client_receive_max = max;
        }
    }

    /// Update server's receive maximum (from configuration).
    pub fn set_server_receive_max(&mut self, max: u16) {
        if max > 0 {
            self.server_receive_max = max;
        }
    }

    /// Get client's receive maximum.
    pub fn client_receive_max(&self) -> u16 {
        self.client_receive_max
    }

    /// Get server's receive maximum.
    pub fn server_receive_max(&self) -> u16 {
        self.server_receive_max
    }

    // -------------------------------------------------------------------------
    // Outbound flow control (messages we send to client)
    // -------------------------------------------------------------------------

    /// Check if we can send another QoS 1/2 message to the client.
    pub fn can_send(&self) -> bool {
        (self.outbound_inflight.len() as u16) < self.client_receive_max
    }

    /// Get available send quota.
    pub fn available_send_quota(&self) -> u16 {
        self.client_receive_max
            .saturating_sub(self.outbound_inflight.len() as u16)
    }

    /// Allocate a packet ID for sending a QoS 1/2 message.
    ///
    /// Returns None if the receive maximum has been reached.
    pub fn allocate_packet_id(&mut self) -> Option<u16> {
        if !self.can_send() {
            return None;
        }

        let packet_id = if let Some(id) = self.available_packet_ids.pop_front() {
            id
        } else {
            let id = self.next_packet_id;
            self.next_packet_id = self.next_packet_id.wrapping_add(1);
            if self.next_packet_id == 0 {
                self.next_packet_id = 1; // Skip 0
            }
            id
        };

        self.outbound_inflight.push_back(packet_id);
        Some(packet_id)
    }

    /// Acknowledge a sent message (PUBACK/PUBCOMP received).
    pub fn acknowledge_outbound(&mut self, packet_id: u16) -> bool {
        if let Some(pos) = self
            .outbound_inflight
            .iter()
            .position(|&id| id == packet_id)
        {
            self.outbound_inflight.remove(pos);
            self.available_packet_ids.push_back(packet_id);
            true
        } else {
            false
        }
    }

    /// Get current outbound inflight count.
    pub fn outbound_inflight_count(&self) -> usize {
        self.outbound_inflight.len()
    }

    // -------------------------------------------------------------------------
    // Inbound flow control (messages client sends to us)
    // -------------------------------------------------------------------------

    /// Check if we can accept another QoS 1/2 message from the client.
    pub fn can_receive(&self) -> bool {
        (self.inbound_inflight.len() as u16) < self.server_receive_max
    }

    /// Get available receive quota.
    pub fn available_receive_quota(&self) -> u16 {
        self.server_receive_max
            .saturating_sub(self.inbound_inflight.len() as u16)
    }

    /// Track an incoming QoS 1/2 message.
    ///
    /// Returns an error if the receive maximum has been exceeded (quota exceeded).
    pub fn track_inbound(&mut self, packet_id: u16) -> Result<(), FlowControlError> {
        if !self.can_receive() {
            return Err(FlowControlError::QuotaExceeded {
                current: self.inbound_inflight.len() as u16,
                max: self.server_receive_max,
            });
        }

        // Check for duplicate packet ID (protocol violation)
        if self.inbound_inflight.contains(&packet_id) {
            return Err(FlowControlError::DuplicatePacketId(packet_id));
        }

        self.inbound_inflight.push_back(packet_id);
        Ok(())
    }

    /// Acknowledge an incoming message (we sent PUBACK/PUBCOMP).
    pub fn acknowledge_inbound(&mut self, packet_id: u16) -> bool {
        if let Some(pos) = self.inbound_inflight.iter().position(|&id| id == packet_id) {
            self.inbound_inflight.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get current inbound inflight count.
    pub fn inbound_inflight_count(&self) -> usize {
        self.inbound_inflight.len()
    }

    /// Clear all inflight state (on disconnect).
    pub fn clear(&mut self) {
        self.outbound_inflight.clear();
        self.inbound_inflight.clear();
        // Don't clear available_packet_ids - we can reuse them on reconnect
    }

    /// Get inflight packet IDs for snapshot.
    pub fn snapshot(&self) -> FlowControlSnapshot {
        FlowControlSnapshot {
            client_receive_max: self.client_receive_max,
            server_receive_max: self.server_receive_max,
            outbound_inflight: self.outbound_inflight.iter().copied().collect(),
            inbound_inflight: self.inbound_inflight.iter().copied().collect(),
        }
    }

    /// Restore from snapshot.
    pub fn restore(&mut self, snapshot: &FlowControlSnapshot) {
        self.client_receive_max = snapshot.client_receive_max;
        self.server_receive_max = snapshot.server_receive_max;
        self.outbound_inflight = snapshot.outbound_inflight.iter().copied().collect();
        self.inbound_inflight = snapshot.inbound_inflight.iter().copied().collect();
    }
}

/// Snapshot of flow control state for persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FlowControlSnapshot {
    pub client_receive_max: u16,
    pub server_receive_max: u16,
    pub outbound_inflight: Vec<u16>,
    pub inbound_inflight: Vec<u16>,
}

// ---------------------------------------------------------------------------
// Quota Exceeded Handling (Task 12)
// ---------------------------------------------------------------------------

/// Error types for flow control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowControlError {
    /// Receive maximum quota exceeded.
    QuotaExceeded { current: u16, max: u16 },
    /// Duplicate packet ID received.
    DuplicatePacketId(u16),
    /// No packet IDs available.
    NoPacketIdsAvailable,
}

impl std::fmt::Display for FlowControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QuotaExceeded { current, max } => {
                write!(f, "receive maximum exceeded: {current}/{max}")
            }
            Self::DuplicatePacketId(id) => write!(f, "duplicate packet id: {id}"),
            Self::NoPacketIdsAvailable => write!(f, "no packet ids available"),
        }
    }
}

impl std::error::Error for FlowControlError {}

/// MQTT 5.0 disconnect reason codes related to flow control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DisconnectReason {
    /// Normal disconnection.
    NormalDisconnection = 0x00,
    /// Receive Maximum exceeded.
    ReceiveMaximumExceeded = 0x93,
    /// Packet Identifier in use.
    PacketIdentifierInUse = 0x91,
    /// Packet too large.
    PacketTooLarge = 0x95,
    /// Quota exceeded.
    QuotaExceeded = 0x97,
}

impl DisconnectReason {
    pub fn from_flow_error(error: &FlowControlError) -> Self {
        match error {
            FlowControlError::QuotaExceeded { .. } => Self::ReceiveMaximumExceeded,
            FlowControlError::DuplicatePacketId(_) => Self::PacketIdentifierInUse,
            FlowControlError::NoPacketIdsAvailable => Self::QuotaExceeded,
        }
    }
}

// ---------------------------------------------------------------------------
// Multi-Session Flow Control Manager
// ---------------------------------------------------------------------------

/// Manages flow control across multiple sessions.
#[derive(Debug, Clone, Default)]
pub struct FlowControlManager {
    /// Per-client flow controllers.
    controllers: HashMap<String, ReceiveMaximumController>,
    /// Default client receive max for new connections.
    default_client_receive_max: u16,
    /// Default server receive max for new connections.
    default_server_receive_max: u16,
}

impl FlowControlManager {
    pub fn new() -> Self {
        Self {
            controllers: HashMap::new(),
            default_client_receive_max: 65535,
            default_server_receive_max: 65535,
        }
    }

    pub fn with_defaults(mut self, client_max: u16, server_max: u16) -> Self {
        self.default_client_receive_max = if client_max == 0 { 65535 } else { client_max };
        self.default_server_receive_max = if server_max == 0 { 65535 } else { server_max };
        self
    }

    /// Get or create a flow controller for a client.
    pub fn get_or_create(&mut self, client_id: &str) -> &mut ReceiveMaximumController {
        self.controllers
            .entry(client_id.to_string())
            .or_insert_with(|| {
                ReceiveMaximumController::new(
                    self.default_client_receive_max,
                    self.default_server_receive_max,
                )
            })
    }

    /// Get a flow controller for a client.
    pub fn get(&self, client_id: &str) -> Option<&ReceiveMaximumController> {
        self.controllers.get(client_id)
    }

    /// Get a mutable flow controller for a client.
    pub fn get_mut(&mut self, client_id: &str) -> Option<&mut ReceiveMaximumController> {
        self.controllers.get_mut(client_id)
    }

    /// Remove a client's flow controller.
    pub fn remove(&mut self, client_id: &str) -> Option<ReceiveMaximumController> {
        self.controllers.remove(client_id)
    }

    /// Clear a client's inflight state (on disconnect).
    pub fn clear_inflight(&mut self, client_id: &str) {
        if let Some(controller) = self.controllers.get_mut(client_id) {
            controller.clear();
        }
    }

    /// Get total inflight messages across all clients.
    pub fn total_inflight(&self) -> usize {
        self.controllers
            .values()
            .map(|c| c.outbound_inflight_count() + c.inbound_inflight_count())
            .sum()
    }

    /// Get number of tracked clients.
    pub fn client_count(&self) -> usize {
        self.controllers.len()
    }
}

// ---------------------------------------------------------------------------
// QoS 2 State Machine
// ---------------------------------------------------------------------------

/// QoS 2 message state for exactly-once delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Qos2State {
    /// PUBLISH sent/received, awaiting PUBREC.
    PendingPubrec,
    /// PUBREC sent/received, awaiting PUBREL.
    PendingPubrel,
    /// PUBREL sent/received, awaiting PUBCOMP.
    PendingPubcomp,
    /// Transaction complete.
    Complete,
}

/// Tracks QoS 2 message state.
#[derive(Debug, Clone)]
pub struct Qos2StateTracker {
    /// Outbound QoS 2 states (messages we're sending).
    outbound: HashMap<u16, Qos2State>,
    /// Inbound QoS 2 states (messages we're receiving).
    inbound: HashMap<u16, Qos2State>,
}

impl Default for Qos2StateTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl Qos2StateTracker {
    pub fn new() -> Self {
        Self {
            outbound: HashMap::new(),
            inbound: HashMap::new(),
        }
    }

    // -------------------------------------------------------------------------
    // Outbound QoS 2 (we're the publisher)
    // -------------------------------------------------------------------------

    /// Start outbound QoS 2 flow (PUBLISH sent).
    pub fn start_outbound(&mut self, packet_id: u16) {
        self.outbound.insert(packet_id, Qos2State::PendingPubrec);
    }

    /// Handle PUBREC received for outbound message.
    pub fn handle_pubrec_received(&mut self, packet_id: u16) -> Result<(), FlowControlError> {
        match self.outbound.get(&packet_id) {
            Some(Qos2State::PendingPubrec) => {
                self.outbound.insert(packet_id, Qos2State::PendingPubcomp);
                Ok(())
            }
            Some(_) => Err(FlowControlError::DuplicatePacketId(packet_id)),
            None => Err(FlowControlError::DuplicatePacketId(packet_id)),
        }
    }

    /// Handle PUBCOMP received for outbound message.
    pub fn handle_pubcomp_received(&mut self, packet_id: u16) -> bool {
        if let Some(Qos2State::PendingPubcomp) = self.outbound.get(&packet_id) {
            self.outbound.remove(&packet_id);
            true
        } else {
            false
        }
    }

    /// Get outbound QoS 2 state.
    pub fn outbound_state(&self, packet_id: u16) -> Option<Qos2State> {
        self.outbound.get(&packet_id).copied()
    }

    // -------------------------------------------------------------------------
    // Inbound QoS 2 (we're the subscriber)
    // -------------------------------------------------------------------------

    /// Start inbound QoS 2 flow (PUBLISH received).
    pub fn start_inbound(&mut self, packet_id: u16) {
        self.inbound.insert(packet_id, Qos2State::PendingPubrel);
    }

    /// Handle PUBREL received for inbound message.
    pub fn handle_pubrel_received(&mut self, packet_id: u16) -> Result<(), FlowControlError> {
        match self.inbound.get(&packet_id) {
            Some(Qos2State::PendingPubrel) => {
                self.inbound.remove(&packet_id);
                Ok(())
            }
            Some(_) => Err(FlowControlError::DuplicatePacketId(packet_id)),
            None => {
                // PUBREL for unknown packet - might be a retry, accept it
                Ok(())
            }
        }
    }

    /// Get inbound QoS 2 state.
    pub fn inbound_state(&self, packet_id: u16) -> Option<Qos2State> {
        self.inbound.get(&packet_id).copied()
    }

    /// Clear all state (on disconnect).
    pub fn clear(&mut self) {
        self.outbound.clear();
        self.inbound.clear();
    }

    /// Get pending outbound count.
    pub fn pending_outbound_count(&self) -> usize {
        self.outbound.len()
    }

    /// Get pending inbound count.
    pub fn pending_inbound_count(&self) -> usize {
        self.inbound.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_receive_maximum_controller_can_send() {
        let mut controller = ReceiveMaximumController::new(2, 10);

        assert!(controller.can_send());
        assert_eq!(controller.available_send_quota(), 2);

        let id1 = controller.allocate_packet_id().unwrap();
        assert_eq!(controller.available_send_quota(), 1);

        let id2 = controller.allocate_packet_id().unwrap();
        assert_eq!(controller.available_send_quota(), 0);
        assert!(!controller.can_send());

        // Can't allocate more
        assert!(controller.allocate_packet_id().is_none());

        // Acknowledge one
        assert!(controller.acknowledge_outbound(id1));
        assert!(controller.can_send());
        assert_eq!(controller.available_send_quota(), 1);

        // Can allocate again
        let _id3 = controller.allocate_packet_id().unwrap();
        assert!(!controller.can_send());

        // Clean up
        controller.acknowledge_outbound(id2);
    }

    #[test]
    fn test_receive_maximum_controller_inbound() {
        let mut controller = ReceiveMaximumController::new(10, 2);

        assert!(controller.can_receive());
        assert_eq!(controller.available_receive_quota(), 2);

        controller.track_inbound(1).unwrap();
        controller.track_inbound(2).unwrap();

        assert!(!controller.can_receive());

        // Quota exceeded
        let result = controller.track_inbound(3);
        assert!(matches!(
            result,
            Err(FlowControlError::QuotaExceeded { .. })
        ));

        // Acknowledge one to make room
        controller.acknowledge_inbound(2);
        assert!(controller.can_receive());

        // Now try duplicate packet ID (1 is still in flight)
        let result = controller.track_inbound(1);
        assert!(matches!(
            result,
            Err(FlowControlError::DuplicatePacketId(1))
        ));

        // Acknowledge
        controller.acknowledge_inbound(1);
        assert!(controller.can_receive());
    }

    #[test]
    fn test_receive_maximum_controller_packet_id_reuse() {
        let mut controller = ReceiveMaximumController::new(2, 10);

        let id1 = controller.allocate_packet_id().unwrap();
        let id2 = controller.allocate_packet_id().unwrap();

        controller.acknowledge_outbound(id1);
        controller.acknowledge_outbound(id2);

        // Packet IDs should be reused
        let id3 = controller.allocate_packet_id().unwrap();
        let id4 = controller.allocate_packet_id().unwrap();

        // id3 and id4 should be id1 and id2 (reused)
        assert!(id3 == id1 || id3 == id2);
        assert!(id4 == id1 || id4 == id2);
    }

    #[test]
    fn test_flow_control_manager() {
        let mut manager = FlowControlManager::new().with_defaults(10, 10);

        let controller = manager.get_or_create("client1");
        assert_eq!(controller.client_receive_max(), 10);

        controller.allocate_packet_id().unwrap();
        assert_eq!(manager.total_inflight(), 1);

        manager.clear_inflight("client1");
        assert_eq!(manager.total_inflight(), 0);
    }

    #[test]
    fn test_qos2_state_tracker_outbound() {
        let mut tracker = Qos2StateTracker::new();

        tracker.start_outbound(1);
        assert_eq!(tracker.outbound_state(1), Some(Qos2State::PendingPubrec));

        tracker.handle_pubrec_received(1).unwrap();
        assert_eq!(tracker.outbound_state(1), Some(Qos2State::PendingPubcomp));

        assert!(tracker.handle_pubcomp_received(1));
        assert_eq!(tracker.outbound_state(1), None);
    }

    #[test]
    fn test_qos2_state_tracker_inbound() {
        let mut tracker = Qos2StateTracker::new();

        tracker.start_inbound(1);
        assert_eq!(tracker.inbound_state(1), Some(Qos2State::PendingPubrel));

        tracker.handle_pubrel_received(1).unwrap();
        assert_eq!(tracker.inbound_state(1), None);
    }

    #[test]
    fn test_disconnect_reason() {
        let error = FlowControlError::QuotaExceeded {
            current: 10,
            max: 10,
        };
        assert_eq!(
            DisconnectReason::from_flow_error(&error),
            DisconnectReason::ReceiveMaximumExceeded
        );

        let error = FlowControlError::DuplicatePacketId(1);
        assert_eq!(
            DisconnectReason::from_flow_error(&error),
            DisconnectReason::PacketIdentifierInUse
        );
    }

    #[test]
    fn test_snapshot_restore() {
        let mut controller = ReceiveMaximumController::new(10, 10);
        controller.allocate_packet_id().unwrap();
        controller.track_inbound(100).unwrap();

        let snapshot = controller.snapshot();
        assert_eq!(snapshot.outbound_inflight.len(), 1);
        assert_eq!(snapshot.inbound_inflight.len(), 1);

        let mut new_controller = ReceiveMaximumController::new(1, 1);
        new_controller.restore(&snapshot);

        assert_eq!(new_controller.client_receive_max(), 10);
        assert_eq!(new_controller.outbound_inflight_count(), 1);
        assert_eq!(new_controller.inbound_inflight_count(), 1);
    }
}
