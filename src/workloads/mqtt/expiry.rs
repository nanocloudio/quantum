//! Message expiry interval handling for MQTT 5.0.
//!
//! This module implements message expiry as defined in MQTT 5.0
//! specification section 3.3.2.3.3 (Message Expiry Interval).
//!
//! Tasks 8-10: Message expiry interval handling and offline queue integration.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Message Expiry (Task 8)
// ---------------------------------------------------------------------------

/// A message with expiry information.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExpiringMessage {
    /// Original topic.
    pub topic: String,
    /// Message payload.
    pub payload: Vec<u8>,
    /// QoS level.
    pub qos: crate::mqtt::Qos,
    /// Retain flag.
    pub retain: bool,
    /// Message expiry interval in seconds (None = no expiry).
    pub expiry_interval: Option<u32>,
    /// Timestamp when message was created (Unix epoch seconds).
    pub created_at: u64,
}

impl ExpiringMessage {
    /// Create a new expiring message.
    pub fn new(
        topic: String,
        payload: Vec<u8>,
        qos: crate::mqtt::Qos,
        retain: bool,
        expiry_interval: Option<u32>,
    ) -> Self {
        Self {
            topic,
            payload,
            qos,
            retain,
            expiry_interval,
            created_at: current_unix_timestamp(),
        }
    }

    /// Check if the message has expired.
    pub fn is_expired(&self) -> bool {
        self.is_expired_at(current_unix_timestamp())
    }

    /// Check if the message has expired at a given timestamp.
    pub fn is_expired_at(&self, now_secs: u64) -> bool {
        match self.expiry_interval {
            Some(interval) => {
                let expiry_time = self.created_at.saturating_add(interval as u64);
                now_secs >= expiry_time
            }
            None => false,
        }
    }

    /// Get remaining TTL in seconds (0 if expired, None if no expiry).
    pub fn remaining_ttl(&self) -> Option<u32> {
        self.remaining_ttl_at(current_unix_timestamp())
    }

    /// Get remaining TTL at a given timestamp.
    pub fn remaining_ttl_at(&self, now_secs: u64) -> Option<u32> {
        match self.expiry_interval {
            Some(interval) => {
                let expiry_time = self.created_at.saturating_add(interval as u64);
                if now_secs >= expiry_time {
                    Some(0)
                } else {
                    Some((expiry_time - now_secs) as u32)
                }
            }
            None => None,
        }
    }

    /// Update message expiry interval to remaining TTL.
    ///
    /// Per MQTT 5.0 spec, when forwarding a message, the Message Expiry Interval
    /// must be set to the remaining lifetime of the message.
    pub fn update_expiry_for_forward(&mut self) {
        if let Some(ttl) = self.remaining_ttl() {
            self.expiry_interval = Some(ttl);
            self.created_at = current_unix_timestamp();
        }
    }
}

/// Get current Unix timestamp in seconds.
fn current_unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

// ---------------------------------------------------------------------------
// Expiry Tracker (Task 9)
// ---------------------------------------------------------------------------

/// Tracks message expiry for efficient cleanup.
#[derive(Debug, Clone)]
pub struct ExpiryTracker {
    /// Messages with expiry, sorted by expiry time.
    entries: Vec<ExpiryEntry>,
    /// Total expired messages removed.
    expired_count: u64,
}

/// Entry in the expiry tracker.
#[derive(Debug, Clone)]
struct ExpiryEntry {
    /// Message identifier.
    message_id: String,
    /// Expiry timestamp (Unix epoch seconds).
    expires_at: u64,
}

impl Default for ExpiryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpiryTracker {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            expired_count: 0,
        }
    }

    /// Track a message with expiry.
    pub fn track(&mut self, message_id: String, expiry_interval: u32) {
        let expires_at = current_unix_timestamp().saturating_add(expiry_interval as u64);
        let entry = ExpiryEntry {
            message_id,
            expires_at,
        };

        // Insert in sorted order by expiry time
        let pos = self
            .entries
            .binary_search_by_key(&expires_at, |e| e.expires_at)
            .unwrap_or_else(|p| p);
        self.entries.insert(pos, entry);
    }

    /// Get expired message IDs and remove them from tracking.
    pub fn collect_expired(&mut self) -> Vec<String> {
        self.collect_expired_at(current_unix_timestamp())
    }

    /// Get expired message IDs at a given timestamp.
    pub fn collect_expired_at(&mut self, now_secs: u64) -> Vec<String> {
        let mut expired = Vec::new();

        while let Some(entry) = self.entries.first() {
            if entry.expires_at > now_secs {
                break;
            }
            if let Some(e) = self.entries.first().cloned() {
                self.entries.remove(0);
                expired.push(e.message_id);
                self.expired_count += 1;
            }
        }

        expired
    }

    /// Remove a message from tracking (e.g., when delivered).
    pub fn remove(&mut self, message_id: &str) -> bool {
        if let Some(pos) = self.entries.iter().position(|e| e.message_id == message_id) {
            self.entries.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get time until next expiry.
    pub fn next_expiry_in(&self) -> Option<Duration> {
        self.entries.first().map(|e| {
            let now = current_unix_timestamp();
            if e.expires_at > now {
                Duration::from_secs(e.expires_at - now)
            } else {
                Duration::ZERO
            }
        })
    }

    /// Get pending count.
    pub fn pending_count(&self) -> usize {
        self.entries.len()
    }

    /// Get total expired count.
    pub fn expired_count(&self) -> u64 {
        self.expired_count
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

// ---------------------------------------------------------------------------
// Offline Queue Expiry Filter (Task 10)
// ---------------------------------------------------------------------------

/// Filter for offline queue entries based on message expiry.
#[derive(Debug, Clone)]
pub struct OfflineExpiryFilter {
    /// Default expiry interval if not set in message (0 = no default).
    default_expiry: u32,
    /// Maximum expiry interval allowed (0 = unlimited).
    max_expiry: u32,
    /// Whether to drop expired messages silently or track them.
    silent_drop: bool,
    /// Count of dropped messages.
    dropped_count: u64,
}

impl Default for OfflineExpiryFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl OfflineExpiryFilter {
    pub fn new() -> Self {
        Self {
            default_expiry: 0,
            max_expiry: 0,
            silent_drop: true,
            dropped_count: 0,
        }
    }

    pub fn with_default_expiry(mut self, seconds: u32) -> Self {
        self.default_expiry = seconds;
        self
    }

    pub fn with_max_expiry(mut self, seconds: u32) -> Self {
        self.max_expiry = seconds;
        self
    }

    pub fn with_tracking(mut self) -> Self {
        self.silent_drop = false;
        self
    }

    /// Apply expiry policy to a message, returning the effective expiry interval.
    ///
    /// Returns None if the message should be dropped immediately.
    pub fn apply_policy(&self, expiry_interval: Option<u32>) -> Option<u32> {
        let interval = match expiry_interval {
            Some(i) => i,
            None => {
                if self.default_expiry > 0 {
                    self.default_expiry
                } else {
                    return expiry_interval; // No expiry
                }
            }
        };

        // Check against max
        if self.max_expiry > 0 && interval > self.max_expiry {
            Some(self.max_expiry)
        } else {
            Some(interval)
        }
    }

    /// Check if a message with given creation time and expiry should be kept.
    pub fn should_keep(&mut self, created_at: u64, expiry_interval: Option<u32>) -> bool {
        let now = current_unix_timestamp();
        match expiry_interval {
            Some(interval) => {
                let expiry_time = created_at.saturating_add(interval as u64);
                if now >= expiry_time {
                    self.dropped_count += 1;
                    false
                } else {
                    true
                }
            }
            None => true, // No expiry, always keep
        }
    }

    /// Filter a slice of expiring messages, returning only non-expired ones.
    pub fn filter_expired(&mut self, messages: &[ExpiringMessage]) -> Vec<ExpiringMessage> {
        messages
            .iter()
            .filter(|m| self.should_keep(m.created_at, m.expiry_interval))
            .cloned()
            .collect()
    }

    /// Get count of dropped messages.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    /// Reset dropped count.
    pub fn reset_dropped_count(&mut self) {
        self.dropped_count = 0;
    }
}

// ---------------------------------------------------------------------------
// Session Expiry (Related to Message Expiry)
// ---------------------------------------------------------------------------

/// Session expiry tracking for clean session management.
#[derive(Debug, Clone)]
pub struct SessionExpiry {
    /// Client ID.
    pub client_id: String,
    /// Session expiry interval in seconds.
    pub expiry_interval: u32,
    /// Last activity timestamp (Unix epoch seconds).
    pub last_activity: u64,
    /// Whether the session is currently connected.
    pub connected: bool,
}

impl SessionExpiry {
    pub fn new(client_id: String, expiry_interval: u32) -> Self {
        Self {
            client_id,
            expiry_interval,
            last_activity: current_unix_timestamp(),
            connected: true,
        }
    }

    /// Mark session as disconnected.
    pub fn disconnect(&mut self) {
        self.connected = false;
        self.last_activity = current_unix_timestamp();
    }

    /// Mark session as connected.
    pub fn connect(&mut self) {
        self.connected = true;
        self.last_activity = current_unix_timestamp();
    }

    /// Update last activity timestamp.
    pub fn touch(&mut self) {
        self.last_activity = current_unix_timestamp();
    }

    /// Check if the session has expired (only applies to disconnected sessions).
    pub fn is_expired(&self) -> bool {
        self.is_expired_at(current_unix_timestamp())
    }

    /// Check if the session has expired at a given timestamp.
    pub fn is_expired_at(&self, now_secs: u64) -> bool {
        if self.connected {
            return false;
        }
        if self.expiry_interval == 0 {
            return false; // 0 means no expiry (session never expires)
        }
        if self.expiry_interval == u32::MAX {
            return false; // Max value also means no expiry
        }
        let expiry_time = self
            .last_activity
            .saturating_add(self.expiry_interval as u64);
        now_secs >= expiry_time
    }

    /// Get remaining session lifetime in seconds.
    pub fn remaining_lifetime(&self) -> Option<u32> {
        if self.connected || self.expiry_interval == 0 || self.expiry_interval == u32::MAX {
            return None;
        }
        let now = current_unix_timestamp();
        let expiry_time = self
            .last_activity
            .saturating_add(self.expiry_interval as u64);
        if now >= expiry_time {
            Some(0)
        } else {
            Some((expiry_time - now) as u32)
        }
    }
}

// ---------------------------------------------------------------------------
// Will Delay (Task 20 related)
// ---------------------------------------------------------------------------

/// Will message with delay support.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DelayedWill {
    /// Will topic.
    pub topic: String,
    /// Will payload.
    pub payload: Vec<u8>,
    /// Will QoS.
    pub qos: crate::mqtt::Qos,
    /// Will retain flag.
    pub retain: bool,
    /// Will delay interval in seconds.
    pub delay_interval: u32,
    /// Timestamp when delay started (when client disconnected).
    pub delay_started: Option<u64>,
}

impl DelayedWill {
    pub fn new(
        topic: String,
        payload: Vec<u8>,
        qos: crate::mqtt::Qos,
        retain: bool,
        delay_interval: u32,
    ) -> Self {
        Self {
            topic,
            payload,
            qos,
            retain,
            delay_interval,
            delay_started: None,
        }
    }

    /// Start the will delay timer.
    pub fn start_delay(&mut self) {
        self.delay_started = Some(current_unix_timestamp());
    }

    /// Cancel the will delay (client reconnected).
    pub fn cancel_delay(&mut self) {
        self.delay_started = None;
    }

    /// Check if the will should be published.
    pub fn should_publish(&self) -> bool {
        self.should_publish_at(current_unix_timestamp())
    }

    /// Check if the will should be published at a given timestamp.
    pub fn should_publish_at(&self, now_secs: u64) -> bool {
        match self.delay_started {
            Some(started) => {
                let publish_at = started.saturating_add(self.delay_interval as u64);
                now_secs >= publish_at
            }
            None => false,
        }
    }

    /// Get remaining delay in seconds.
    pub fn remaining_delay(&self) -> Option<u32> {
        match self.delay_started {
            Some(started) => {
                let now = current_unix_timestamp();
                let publish_at = started.saturating_add(self.delay_interval as u64);
                if now >= publish_at {
                    Some(0)
                } else {
                    Some((publish_at - now) as u32)
                }
            }
            None => None,
        }
    }

    /// Convert to ExpiringMessage for publishing.
    pub fn to_message(&self, message_expiry: Option<u32>) -> ExpiringMessage {
        ExpiringMessage::new(
            self.topic.clone(),
            self.payload.clone(),
            self.qos,
            self.retain,
            message_expiry,
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::Qos;

    #[test]
    fn test_expiring_message_no_expiry() {
        let msg = ExpiringMessage::new(
            "test/topic".to_string(),
            b"hello".to_vec(),
            Qos::AtLeastOnce,
            false,
            None,
        );

        assert!(!msg.is_expired());
        assert!(msg.remaining_ttl().is_none());
    }

    #[test]
    fn test_expiring_message_with_expiry() {
        let msg = ExpiringMessage::new(
            "test/topic".to_string(),
            b"hello".to_vec(),
            Qos::AtLeastOnce,
            false,
            Some(60), // 60 seconds
        );

        assert!(!msg.is_expired());
        let ttl = msg.remaining_ttl().unwrap();
        assert!(ttl <= 60);
        assert!(ttl > 0);
    }

    #[test]
    fn test_expiring_message_expired() {
        let mut msg = ExpiringMessage::new(
            "test/topic".to_string(),
            b"hello".to_vec(),
            Qos::AtLeastOnce,
            false,
            Some(10),
        );
        // Set created_at to past
        msg.created_at = current_unix_timestamp().saturating_sub(20);

        assert!(msg.is_expired());
        assert_eq!(msg.remaining_ttl(), Some(0));
    }

    #[test]
    fn test_expiry_tracker() {
        let mut tracker = ExpiryTracker::new();

        // Track with 0 second expiry (immediate)
        tracker.track("msg1".to_string(), 0);
        // Track with 1000 second expiry (future)
        tracker.track("msg2".to_string(), 1000);

        let expired = tracker.collect_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], "msg1");
        assert_eq!(tracker.pending_count(), 1);
    }

    #[test]
    fn test_expiry_tracker_remove() {
        let mut tracker = ExpiryTracker::new();
        tracker.track("msg1".to_string(), 1000);
        tracker.track("msg2".to_string(), 1000);

        assert!(tracker.remove("msg1"));
        assert!(!tracker.remove("msg1")); // Already removed
        assert_eq!(tracker.pending_count(), 1);
    }

    #[test]
    fn test_offline_expiry_filter_policy() {
        let filter = OfflineExpiryFilter::new()
            .with_default_expiry(300)
            .with_max_expiry(3600);

        // No expiry -> gets default
        assert_eq!(filter.apply_policy(None), Some(300));

        // Under max -> keeps original
        assert_eq!(filter.apply_policy(Some(600)), Some(600));

        // Over max -> capped to max
        assert_eq!(filter.apply_policy(Some(7200)), Some(3600));
    }

    #[test]
    fn test_offline_expiry_filter_should_keep() {
        let mut filter = OfflineExpiryFilter::new();
        let now = current_unix_timestamp();

        // Not expired
        assert!(filter.should_keep(now, Some(100)));

        // Expired
        assert!(!filter.should_keep(now.saturating_sub(200), Some(100)));
        assert_eq!(filter.dropped_count(), 1);

        // No expiry
        assert!(filter.should_keep(now.saturating_sub(1000), None));
    }

    #[test]
    fn test_session_expiry_connected() {
        let session = SessionExpiry::new("client1".to_string(), 300);
        assert!(!session.is_expired());
        assert!(session.remaining_lifetime().is_none());
    }

    #[test]
    fn test_session_expiry_disconnected() {
        let mut session = SessionExpiry::new("client1".to_string(), 300);
        session.disconnect();

        assert!(!session.is_expired());
        let remaining = session.remaining_lifetime().unwrap();
        assert!(remaining > 0 && remaining <= 300);
    }

    #[test]
    fn test_session_expiry_expired() {
        let mut session = SessionExpiry::new("client1".to_string(), 10);
        session.disconnect();
        session.last_activity = current_unix_timestamp().saturating_sub(20);

        assert!(session.is_expired());
        assert_eq!(session.remaining_lifetime(), Some(0));
    }

    #[test]
    fn test_session_expiry_no_expiry() {
        let mut session = SessionExpiry::new("client1".to_string(), 0);
        session.disconnect();
        session.last_activity = current_unix_timestamp().saturating_sub(1_000_000);

        // 0 means session never expires
        assert!(!session.is_expired());
    }

    #[test]
    fn test_delayed_will() {
        let mut will = DelayedWill::new(
            "will/topic".to_string(),
            b"goodbye".to_vec(),
            Qos::AtLeastOnce,
            false,
            0, // No delay
        );

        assert!(!will.should_publish()); // Not started
        will.start_delay();
        assert!(will.should_publish()); // 0 delay means immediate

        will.cancel_delay();
        assert!(!will.should_publish()); // Cancelled
    }

    #[test]
    fn test_delayed_will_with_delay() {
        let mut will = DelayedWill::new(
            "will/topic".to_string(),
            b"goodbye".to_vec(),
            Qos::AtLeastOnce,
            false,
            60, // 60 second delay
        );

        will.start_delay();
        assert!(!will.should_publish()); // Not yet
        let remaining = will.remaining_delay().unwrap();
        assert!(remaining > 0 && remaining <= 60);
    }

    #[test]
    fn test_update_expiry_for_forward() {
        let mut msg = ExpiringMessage::new(
            "test/topic".to_string(),
            b"hello".to_vec(),
            Qos::AtLeastOnce,
            false,
            Some(100),
        );

        // Simulate some time passed
        let original_created = msg.created_at;
        std::thread::sleep(std::time::Duration::from_millis(10));

        msg.update_expiry_for_forward();

        // After update, expiry should be less than or equal to original
        let new_ttl = msg.expiry_interval.unwrap();
        assert!(new_ttl <= 100);
        assert!(msg.created_at >= original_created);
    }
}
