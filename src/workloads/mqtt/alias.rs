//! Topic alias management for MQTT 5.0.
//!
//! This module implements client-to-server and server-to-client topic alias
//! mapping as defined in MQTT 5.0 specification section 3.3.2.3.

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Client-to-Server Topic Alias (Task 5)
// ---------------------------------------------------------------------------

/// Manages client-to-server topic alias mappings.
///
/// When a client sends a PUBLISH with a topic alias, the broker must maintain
/// a mapping from alias number to topic string. This map is per-connection
/// and cleared on reconnect.
#[derive(Debug, Clone, Default)]
pub struct ClientTopicAliasMap {
    /// Alias -> Topic string mapping.
    aliases: HashMap<u16, String>,
    /// Maximum alias value accepted (from server's topic_alias_max).
    max_alias: u16,
}

impl ClientTopicAliasMap {
    /// Create a new alias map with the given maximum alias count.
    pub fn new(max_alias: u16) -> Self {
        Self {
            aliases: HashMap::new(),
            max_alias,
        }
    }

    /// Set the maximum alias value.
    pub fn set_max_alias(&mut self, max: u16) {
        self.max_alias = max;
        // Remove any aliases that exceed the new max
        self.aliases.retain(|&alias, _| alias <= max);
    }

    /// Get the maximum alias value.
    pub fn max_alias(&self) -> u16 {
        self.max_alias
    }

    /// Store or update a topic alias mapping.
    ///
    /// Returns an error if the alias exceeds the maximum or is zero.
    pub fn set(&mut self, alias: u16, topic: String) -> Result<(), TopicAliasError> {
        if alias == 0 {
            return Err(TopicAliasError::ZeroAlias);
        }
        if alias > self.max_alias {
            return Err(TopicAliasError::AliasExceedsMax {
                alias,
                max: self.max_alias,
            });
        }
        self.aliases.insert(alias, topic);
        Ok(())
    }

    /// Resolve a topic alias to its topic string.
    ///
    /// Returns None if the alias is not registered.
    pub fn get(&self, alias: u16) -> Option<&str> {
        self.aliases.get(&alias).map(|s| s.as_str())
    }

    /// Resolve a PUBLISH's topic, handling topic alias.
    ///
    /// - If topic is non-empty and alias is set: store mapping, return topic
    /// - If topic is empty and alias is set: lookup alias, return stored topic
    /// - If topic is non-empty and no alias: return topic as-is
    /// - If topic is empty and no alias: protocol error
    pub fn resolve(
        &mut self,
        topic: Option<&str>,
        alias: Option<u16>,
    ) -> Result<String, TopicAliasError> {
        match (topic, alias) {
            // Topic provided with alias: store mapping
            (Some(t), Some(a)) if !t.is_empty() => {
                self.set(a, t.to_string())?;
                Ok(t.to_string())
            }
            // Empty topic with alias: lookup
            (Some("") | None, Some(a)) => self
                .get(a)
                .map(|s| s.to_string())
                .ok_or(TopicAliasError::UnknownAlias(a)),
            // Topic without alias
            (Some(t), None) if !t.is_empty() => Ok(t.to_string()),
            // Empty topic without alias: protocol error
            (Some(_) | None, None) => Err(TopicAliasError::EmptyTopicNoAlias),
            // Catch-all for any other Some(topic) + Some(alias) combination
            // This shouldn't be reachable but satisfies exhaustiveness
            (Some(_), Some(a)) => self
                .get(a)
                .map(|s| s.to_string())
                .ok_or(TopicAliasError::UnknownAlias(a)),
        }
    }

    /// Clear all aliases (on disconnect).
    pub fn clear(&mut self) {
        self.aliases.clear();
    }

    /// Get the number of registered aliases.
    pub fn len(&self) -> usize {
        self.aliases.len()
    }

    /// Check if no aliases are registered.
    pub fn is_empty(&self) -> bool {
        self.aliases.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Server-to-Client Topic Alias (Task 6)
// ---------------------------------------------------------------------------

/// Manages server-to-client topic alias mappings.
///
/// The server can assign aliases to topics it sends to clients to reduce
/// bandwidth. This tracks which topics have been assigned aliases for each
/// outbound connection.
#[derive(Debug, Clone)]
pub struct ServerTopicAliasMap {
    /// Topic -> Alias mapping for quick lookup.
    topic_to_alias: HashMap<String, u16>,
    /// Alias -> Topic mapping for consistency check.
    alias_to_topic: HashMap<u16, String>,
    /// Maximum alias value (from client's topic_alias_max in CONNECT).
    max_alias: u16,
    /// Next alias to assign.
    next_alias: u16,
    /// LRU tracking for alias eviction (topic name -> last use timestamp).
    usage_order: Vec<String>,
}

impl ServerTopicAliasMap {
    /// Create a new server alias map with the given maximum.
    pub fn new(max_alias: u16) -> Self {
        Self {
            topic_to_alias: HashMap::new(),
            alias_to_topic: HashMap::new(),
            max_alias,
            next_alias: 1,
            usage_order: Vec::new(),
        }
    }

    /// Get the maximum alias value.
    pub fn max_alias(&self) -> u16 {
        self.max_alias
    }

    /// Check if aliases are enabled for this connection.
    pub fn is_enabled(&self) -> bool {
        self.max_alias > 0
    }

    /// Get or assign an alias for a topic.
    ///
    /// Returns `(alias, needs_topic)` where:
    /// - `alias` is the alias to use
    /// - `needs_topic` is true if the topic string must be included (new alias)
    pub fn get_or_assign(&mut self, topic: &str) -> Option<(u16, bool)> {
        if self.max_alias == 0 {
            return None;
        }

        // Check if already assigned
        if let Some(&alias) = self.topic_to_alias.get(topic) {
            // Update LRU
            self.touch(topic);
            return Some((alias, false));
        }

        // Need to assign new alias
        let alias = if self.next_alias <= self.max_alias {
            // Use next available
            let alias = self.next_alias;
            self.next_alias += 1;
            alias
        } else {
            // Evict LRU alias
            self.evict_lru()?
        };

        // Store new mapping
        self.topic_to_alias.insert(topic.to_string(), alias);
        self.alias_to_topic.insert(alias, topic.to_string());
        self.usage_order.push(topic.to_string());

        Some((alias, true))
    }

    /// Get existing alias for a topic without assigning.
    pub fn get(&self, topic: &str) -> Option<u16> {
        self.topic_to_alias.get(topic).copied()
    }

    /// Evict the least recently used alias.
    fn evict_lru(&mut self) -> Option<u16> {
        let topic = self.usage_order.first()?.clone();
        self.usage_order.remove(0);

        let alias = self.topic_to_alias.remove(&topic)?;
        self.alias_to_topic.remove(&alias);
        Some(alias)
    }

    /// Update LRU tracking for a topic.
    fn touch(&mut self, topic: &str) {
        if let Some(pos) = self.usage_order.iter().position(|t| t == topic) {
            self.usage_order.remove(pos);
            self.usage_order.push(topic.to_string());
        }
    }

    /// Clear all aliases (on disconnect).
    pub fn clear(&mut self) {
        self.topic_to_alias.clear();
        self.alias_to_topic.clear();
        self.usage_order.clear();
        self.next_alias = 1;
    }

    /// Get the number of assigned aliases.
    pub fn len(&self) -> usize {
        self.topic_to_alias.len()
    }

    /// Check if no aliases are assigned.
    pub fn is_empty(&self) -> bool {
        self.topic_to_alias.is_empty()
    }
}

impl Default for ServerTopicAliasMap {
    fn default() -> Self {
        Self::new(0)
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors related to topic alias handling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicAliasError {
    /// Alias value is zero (invalid).
    ZeroAlias,
    /// Alias exceeds the negotiated maximum.
    AliasExceedsMax { alias: u16, max: u16 },
    /// Alias has not been registered yet.
    UnknownAlias(u16),
    /// Empty topic provided without an alias.
    EmptyTopicNoAlias,
}

impl std::fmt::Display for TopicAliasError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicAliasError::ZeroAlias => write!(f, "topic alias cannot be zero"),
            TopicAliasError::AliasExceedsMax { alias, max } => {
                write!(f, "topic alias {alias} exceeds maximum {max}")
            }
            TopicAliasError::UnknownAlias(alias) => {
                write!(f, "topic alias {alias} has not been registered")
            }
            TopicAliasError::EmptyTopicNoAlias => {
                write!(f, "empty topic provided without topic alias")
            }
        }
    }
}

impl std::error::Error for TopicAliasError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_alias_set_and_get() {
        let mut map = ClientTopicAliasMap::new(10);

        map.set(1, "sensors/temp".to_string()).unwrap();
        map.set(2, "sensors/humidity".to_string()).unwrap();

        assert_eq!(map.get(1), Some("sensors/temp"));
        assert_eq!(map.get(2), Some("sensors/humidity"));
        assert_eq!(map.get(3), None);
    }

    #[test]
    fn test_client_alias_zero_rejected() {
        let mut map = ClientTopicAliasMap::new(10);
        assert!(matches!(
            map.set(0, "topic".to_string()),
            Err(TopicAliasError::ZeroAlias)
        ));
    }

    #[test]
    fn test_client_alias_exceeds_max() {
        let mut map = ClientTopicAliasMap::new(5);
        assert!(matches!(
            map.set(6, "topic".to_string()),
            Err(TopicAliasError::AliasExceedsMax { alias: 6, max: 5 })
        ));
    }

    #[test]
    fn test_client_alias_resolve_new() {
        let mut map = ClientTopicAliasMap::new(10);

        // New alias with topic
        let result = map.resolve(Some("sensors/temp"), Some(1));
        assert_eq!(result.unwrap(), "sensors/temp");
        assert_eq!(map.get(1), Some("sensors/temp"));
    }

    #[test]
    fn test_client_alias_resolve_existing() {
        let mut map = ClientTopicAliasMap::new(10);
        map.set(1, "sensors/temp".to_string()).unwrap();

        // Lookup by alias only
        let result = map.resolve(Some(""), Some(1));
        assert_eq!(result.unwrap(), "sensors/temp");
    }

    #[test]
    fn test_client_alias_resolve_unknown() {
        let mut map = ClientTopicAliasMap::new(10);

        let result = map.resolve(Some(""), Some(5));
        assert!(matches!(result, Err(TopicAliasError::UnknownAlias(5))));
    }

    #[test]
    fn test_client_alias_resolve_no_alias() {
        let mut map = ClientTopicAliasMap::new(10);

        // Topic without alias
        let result = map.resolve(Some("sensors/temp"), None);
        assert_eq!(result.unwrap(), "sensors/temp");

        // Empty topic without alias
        let result = map.resolve(Some(""), None);
        assert!(matches!(result, Err(TopicAliasError::EmptyTopicNoAlias)));
    }

    #[test]
    fn test_client_alias_clear() {
        let mut map = ClientTopicAliasMap::new(10);
        map.set(1, "sensors/temp".to_string()).unwrap();
        assert_eq!(map.len(), 1);

        map.clear();
        assert!(map.is_empty());
        assert_eq!(map.get(1), None);
    }

    #[test]
    fn test_server_alias_get_or_assign() {
        let mut map = ServerTopicAliasMap::new(10);

        // First assignment
        let (alias1, needs_topic1) = map.get_or_assign("sensors/temp").unwrap();
        assert_eq!(alias1, 1);
        assert!(needs_topic1);

        // Second access of same topic
        let (alias2, needs_topic2) = map.get_or_assign("sensors/temp").unwrap();
        assert_eq!(alias2, 1);
        assert!(!needs_topic2);

        // Different topic
        let (alias3, needs_topic3) = map.get_or_assign("sensors/humidity").unwrap();
        assert_eq!(alias3, 2);
        assert!(needs_topic3);
    }

    #[test]
    fn test_server_alias_disabled() {
        let mut map = ServerTopicAliasMap::new(0);
        assert!(!map.is_enabled());
        assert!(map.get_or_assign("sensors/temp").is_none());
    }

    #[test]
    fn test_server_alias_eviction() {
        let mut map = ServerTopicAliasMap::new(2);

        // Fill up aliases
        map.get_or_assign("topic1").unwrap();
        map.get_or_assign("topic2").unwrap();

        // Trigger eviction (LRU is topic1)
        let (alias, _) = map.get_or_assign("topic3").unwrap();
        assert_eq!(alias, 1); // Reused from topic1

        // topic1 should be gone
        assert!(map.get("topic1").is_none());
        assert!(map.get("topic2").is_some());
        assert!(map.get("topic3").is_some());
    }

    #[test]
    fn test_server_alias_lru_update() {
        let mut map = ServerTopicAliasMap::new(2);

        map.get_or_assign("topic1").unwrap();
        map.get_or_assign("topic2").unwrap();

        // Touch topic1 to make it more recent
        map.get_or_assign("topic1").unwrap();

        // Evict - should evict topic2 (now LRU)
        let (alias, _) = map.get_or_assign("topic3").unwrap();
        assert_eq!(alias, 2); // Reused from topic2

        assert!(map.get("topic1").is_some());
        assert!(map.get("topic2").is_none());
        assert!(map.get("topic3").is_some());
    }

    #[test]
    fn test_server_alias_clear() {
        let mut map = ServerTopicAliasMap::new(10);
        map.get_or_assign("topic1").unwrap();
        map.get_or_assign("topic2").unwrap();

        map.clear();
        assert!(map.is_empty());

        // Next assignment starts from 1 again
        let (alias, _) = map.get_or_assign("topic3").unwrap();
        assert_eq!(alias, 1);
    }
}
