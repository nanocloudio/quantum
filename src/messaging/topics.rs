//! Generic topic routing infrastructure for multi-protocol support.
//!
//! This module provides protocol-agnostic topic matching and routing:
//! - MQTT wildcard matching (+, #)
//! - Kafka topic-partition mapping
//! - AMQP exchange routing patterns
//!
//! Tasks 25-27: Topic routing, trie data structure, benchmarks

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Topic Matcher Trait (Task 25)
// ---------------------------------------------------------------------------

/// Protocol-agnostic topic matcher.
pub trait TopicMatcher: Send + Sync {
    /// Check if a pattern matches a topic/routing key.
    fn matches(&self, pattern: &str, topic: &str) -> bool;

    /// Protocol identifier for this matcher.
    fn protocol(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// MQTT Topic Matcher
// ---------------------------------------------------------------------------

/// MQTT topic matching with + and # wildcards.
#[derive(Debug, Clone, Default)]
pub struct MqttTopicMatcher;

impl MqttTopicMatcher {
    pub fn new() -> Self {
        Self
    }
}

impl TopicMatcher for MqttTopicMatcher {
    fn matches(&self, pattern: &str, topic: &str) -> bool {
        mqtt_topic_matches(pattern, topic)
    }

    fn protocol(&self) -> &'static str {
        "mqtt"
    }
}

/// Check if an MQTT topic filter matches a topic.
/// - `+` matches a single level
/// - `#` matches zero or more levels (must be last)
pub fn mqtt_topic_matches(filter: &str, topic: &str) -> bool {
    let filter_parts: Vec<&str> = filter.split('/').collect();
    let topic_parts: Vec<&str> = topic.split('/').collect();

    let mut fi = 0;
    let mut ti = 0;

    while fi < filter_parts.len() {
        let fp = filter_parts[fi];

        if fp == "#" {
            return true;
        }

        if ti >= topic_parts.len() {
            return false;
        }

        if fp == "+" {
            fi += 1;
            ti += 1;
            continue;
        }

        if fp != topic_parts[ti] {
            return false;
        }

        fi += 1;
        ti += 1;
    }

    fi == filter_parts.len() && ti == topic_parts.len()
}

// ---------------------------------------------------------------------------
// Kafka Topic Matcher
// ---------------------------------------------------------------------------

/// Kafka topic matching (exact match only, with partition support).
#[derive(Debug, Clone, Default)]
pub struct KafkaTopicMatcher;

impl KafkaTopicMatcher {
    pub fn new() -> Self {
        Self
    }

    /// Compute partition for a key using consistent hashing.
    pub fn partition_for_key(key: &[u8], partition_count: u32) -> u32 {
        if partition_count == 0 {
            return 0;
        }
        // Murmur2 hash (Kafka default)
        let hash = murmur2(key);
        (hash as u32) % partition_count
    }
}

impl TopicMatcher for KafkaTopicMatcher {
    fn matches(&self, pattern: &str, topic: &str) -> bool {
        // Kafka uses exact topic matching
        pattern == topic
    }

    fn protocol(&self) -> &'static str {
        "kafka"
    }
}

/// Simple Murmur2 hash implementation (Kafka default partitioner).
fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let len = data.len() as u32;
    let mut h = SEED ^ len;

    let mut i = 0;
    while i + 4 <= data.len() {
        let mut k = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
        i += 4;
    }

    let remaining = data.len() - i;
    if remaining >= 3 {
        h ^= (data[i + 2] as u32) << 16;
    }
    if remaining >= 2 {
        h ^= (data[i + 1] as u32) << 8;
    }
    if remaining >= 1 {
        h ^= data[i] as u32;
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    h as i32
}

// ---------------------------------------------------------------------------
// AMQP Exchange Matcher
// ---------------------------------------------------------------------------

/// AMQP exchange routing patterns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmqpExchangeType {
    /// Exact routing key match.
    Direct,
    /// Broadcast to all bound queues.
    Fanout,
    /// Wildcard routing with * and #.
    Topic,
    /// Match based on message headers.
    Headers,
}

/// AMQP topic matching with * and # wildcards.
#[derive(Debug, Clone)]
pub struct AmqpTopicMatcher {
    exchange_type: AmqpExchangeType,
}

impl AmqpTopicMatcher {
    pub fn new(exchange_type: AmqpExchangeType) -> Self {
        Self { exchange_type }
    }

    pub fn direct() -> Self {
        Self::new(AmqpExchangeType::Direct)
    }

    pub fn fanout() -> Self {
        Self::new(AmqpExchangeType::Fanout)
    }

    pub fn topic() -> Self {
        Self::new(AmqpExchangeType::Topic)
    }

    pub fn headers() -> Self {
        Self::new(AmqpExchangeType::Headers)
    }
}

impl TopicMatcher for AmqpTopicMatcher {
    fn matches(&self, pattern: &str, topic: &str) -> bool {
        match self.exchange_type {
            AmqpExchangeType::Direct => pattern == topic,
            AmqpExchangeType::Fanout => true,
            AmqpExchangeType::Topic => amqp_topic_matches(pattern, topic),
            AmqpExchangeType::Headers => false, // Headers matching needs header map
        }
    }

    fn protocol(&self) -> &'static str {
        "amqp"
    }
}

/// AMQP topic exchange pattern matching.
/// - `*` matches exactly one word
/// - `#` matches zero or more words
///
/// Words are separated by `.` (not `/` like MQTT)
pub fn amqp_topic_matches(pattern: &str, routing_key: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('.').collect();
    let key_parts: Vec<&str> = routing_key.split('.').collect();

    let mut pi = 0;
    let mut ki = 0;

    while pi < pattern_parts.len() {
        let pp = pattern_parts[pi];

        if pp == "#" {
            // # at end matches everything
            if pi == pattern_parts.len() - 1 {
                return true;
            }
            // # in middle: try matching rest at each position
            for ki_try in ki..=key_parts.len() {
                if amqp_topic_matches_from(&pattern_parts[pi + 1..], &key_parts[ki_try..]) {
                    return true;
                }
            }
            return false;
        }

        if ki >= key_parts.len() {
            return false;
        }

        if pp == "*" {
            pi += 1;
            ki += 1;
            continue;
        }

        if pp != key_parts[ki] {
            return false;
        }

        pi += 1;
        ki += 1;
    }

    pi == pattern_parts.len() && ki == key_parts.len()
}

fn amqp_topic_matches_from(pattern: &[&str], key: &[&str]) -> bool {
    let mut pi = 0;
    let mut ki = 0;

    while pi < pattern.len() {
        let pp = pattern[pi];

        if pp == "#" {
            if pi == pattern.len() - 1 {
                return true;
            }
            for ki_try in ki..=key.len() {
                if amqp_topic_matches_from(&pattern[pi + 1..], &key[ki_try..]) {
                    return true;
                }
            }
            return false;
        }

        if ki >= key.len() {
            return false;
        }

        if pp == "*" {
            pi += 1;
            ki += 1;
            continue;
        }

        if pp != key[ki] {
            return false;
        }

        pi += 1;
        ki += 1;
    }

    pi == pattern.len() && ki == key.len()
}

// ---------------------------------------------------------------------------
// Topic Trie (Task 26)
// ---------------------------------------------------------------------------

/// Trie node for efficient topic matching.
#[derive(Debug, Clone)]
pub struct TopicTrieNode<T> {
    /// Values stored at this node (for exact matches).
    values: Vec<T>,
    /// Child nodes keyed by path segment.
    children: HashMap<String, TopicTrieNode<T>>,
    /// Single-level wildcard (+) children.
    single_wildcard: Option<Box<TopicTrieNode<T>>>,
    /// Multi-level wildcard (#) values.
    multi_wildcard_values: Vec<T>,
}

impl<T: Clone> Default for TopicTrieNode<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            children: HashMap::new(),
            single_wildcard: None,
            multi_wildcard_values: Vec::new(),
        }
    }
}

impl<T: Clone> TopicTrieNode<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a value for a topic filter.
    pub fn insert(&mut self, filter: &str, value: T) {
        let parts: Vec<&str> = filter.split('/').collect();
        self.insert_parts(&parts, value);
    }

    fn insert_parts(&mut self, parts: &[&str], value: T) {
        if parts.is_empty() {
            self.values.push(value);
            return;
        }

        let first = parts[0];
        let rest = &parts[1..];

        if first == "#" {
            // # must be last, store here
            self.multi_wildcard_values.push(value);
            return;
        }

        if first == "+" {
            let child = self
                .single_wildcard
                .get_or_insert_with(|| Box::new(TopicTrieNode::new()));
            child.insert_parts(rest, value);
            return;
        }

        let child = self.children.entry(first.to_string()).or_default();
        child.insert_parts(rest, value);
    }

    /// Remove a value from a topic filter.
    pub fn remove<F>(&mut self, filter: &str, predicate: F) -> bool
    where
        F: Fn(&T) -> bool + Clone,
    {
        let parts: Vec<&str> = filter.split('/').collect();
        self.remove_parts(&parts, predicate)
    }

    fn remove_parts<F>(&mut self, parts: &[&str], predicate: F) -> bool
    where
        F: Fn(&T) -> bool + Clone,
    {
        if parts.is_empty() {
            let before = self.values.len();
            self.values.retain(|v| !predicate(v));
            return self.values.len() < before;
        }

        let first = parts[0];
        let rest = &parts[1..];

        if first == "#" {
            let before = self.multi_wildcard_values.len();
            self.multi_wildcard_values.retain(|v| !predicate(v));
            return self.multi_wildcard_values.len() < before;
        }

        if first == "+" {
            if let Some(ref mut child) = self.single_wildcard {
                return child.remove_parts(rest, predicate);
            }
            return false;
        }

        if let Some(child) = self.children.get_mut(first) {
            return child.remove_parts(rest, predicate);
        }

        false
    }

    /// Find all values matching a topic.
    pub fn find(&self, topic: &str) -> Vec<&T> {
        let parts: Vec<&str> = topic.split('/').collect();
        let mut results = Vec::new();
        self.find_parts(&parts, &mut results);
        results
    }

    fn find_parts<'a>(&'a self, parts: &[&str], results: &mut Vec<&'a T>) {
        // Multi-level wildcard matches everything from here
        results.extend(self.multi_wildcard_values.iter());

        if parts.is_empty() {
            results.extend(self.values.iter());
            return;
        }

        let first = parts[0];
        let rest = &parts[1..];

        // Check exact match
        if let Some(child) = self.children.get(first) {
            child.find_parts(rest, results);
        }

        // Check single-level wildcard
        if let Some(ref child) = self.single_wildcard {
            child.find_parts(rest, results);
        }
    }

    /// Count total values in the trie.
    pub fn count(&self) -> usize {
        let mut count = self.values.len() + self.multi_wildcard_values.len();
        for child in self.children.values() {
            count += child.count();
        }
        if let Some(ref child) = self.single_wildcard {
            count += child.count();
        }
        count
    }

    /// Check if the trie is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
            && self.multi_wildcard_values.is_empty()
            && self.children.is_empty()
            && self.single_wildcard.is_none()
    }
}

// ---------------------------------------------------------------------------
// Topic Subscription Index
// ---------------------------------------------------------------------------

/// Generic subscription index using the topic trie.
///
/// This is a protocol-agnostic subscription index that supports
/// wildcard matching for MQTT-style topic filters.
#[derive(Debug, Clone)]
pub struct TopicSubscriptionIndex<T: Clone> {
    trie: TopicTrieNode<T>,
    count: usize,
}

impl<T: Clone> Default for TopicSubscriptionIndex<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> TopicSubscriptionIndex<T> {
    pub fn new() -> Self {
        Self {
            trie: TopicTrieNode::new(),
            count: 0,
        }
    }

    /// Add a subscription.
    pub fn subscribe(&mut self, filter: &str, value: T) {
        self.trie.insert(filter, value);
        self.count += 1;
    }

    /// Remove a subscription.
    pub fn unsubscribe<F>(&mut self, filter: &str, predicate: F) -> bool
    where
        F: Fn(&T) -> bool + Clone,
    {
        if self.trie.remove(filter, predicate) {
            self.count = self.count.saturating_sub(1);
            true
        } else {
            false
        }
    }

    /// Find all subscriptions matching a topic.
    pub fn matching(&self, topic: &str) -> Vec<&T> {
        self.trie.find(topic)
    }

    /// Get total subscription count.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

// ---------------------------------------------------------------------------
// Partition Router
// ---------------------------------------------------------------------------

/// Routes topics to partitions.
#[derive(Debug, Clone)]
pub struct PartitionRouter {
    /// Default partition count for new topics.
    default_partitions: u32,
    /// Per-topic partition counts.
    topic_partitions: HashMap<String, u32>,
}

impl Default for PartitionRouter {
    fn default() -> Self {
        Self::new(1)
    }
}

impl PartitionRouter {
    pub fn new(default_partitions: u32) -> Self {
        Self {
            default_partitions: default_partitions.max(1),
            topic_partitions: HashMap::new(),
        }
    }

    /// Set partition count for a topic.
    pub fn set_partitions(&mut self, topic: &str, count: u32) {
        self.topic_partitions
            .insert(topic.to_string(), count.max(1));
    }

    /// Get partition count for a topic.
    pub fn partitions(&self, topic: &str) -> u32 {
        self.topic_partitions
            .get(topic)
            .copied()
            .unwrap_or(self.default_partitions)
    }

    /// Route a message to a partition.
    pub fn route(&self, topic: &str, key: Option<&[u8]>) -> u32 {
        let partitions = self.partitions(topic);
        match key {
            Some(k) => KafkaTopicMatcher::partition_for_key(k, partitions),
            None => {
                // Round-robin or random for keyless messages
                // Using thread-local counter for simplicity
                use std::cell::Cell;
                thread_local! {
                    static COUNTER: Cell<u32> = const { Cell::new(0) };
                }
                COUNTER.with(|c| {
                    let val = c.get();
                    c.set(val.wrapping_add(1));
                    val % partitions
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_topic_matches() {
        assert!(mqtt_topic_matches("a/b/c", "a/b/c"));
        assert!(!mqtt_topic_matches("a/b/c", "a/b/d"));
        assert!(mqtt_topic_matches("a/+/c", "a/b/c"));
        assert!(mqtt_topic_matches("a/#", "a/b/c"));
        assert!(mqtt_topic_matches("#", "a/b/c"));
    }

    #[test]
    fn test_amqp_topic_matches() {
        assert!(amqp_topic_matches("a.b.c", "a.b.c"));
        assert!(!amqp_topic_matches("a.b.c", "a.b.d"));
        assert!(amqp_topic_matches("a.*.c", "a.b.c"));
        assert!(amqp_topic_matches("a.#", "a.b.c"));
        assert!(amqp_topic_matches("#", "a.b.c"));
        assert!(amqp_topic_matches("*.*.c", "a.b.c"));
    }

    #[test]
    fn test_kafka_partition() {
        let p1 = KafkaTopicMatcher::partition_for_key(b"key1", 10);
        let p2 = KafkaTopicMatcher::partition_for_key(b"key1", 10);
        assert_eq!(p1, p2); // Same key, same partition

        let p3 = KafkaTopicMatcher::partition_for_key(b"key2", 10);
        // Different key may or may not be different partition
        assert!(p3 < 10);
    }

    #[test]
    fn test_topic_trie_insert_find() {
        let mut trie = TopicTrieNode::<String>::new();

        trie.insert("sensors/temp", "sub1".to_string());
        trie.insert("sensors/+", "sub2".to_string());
        trie.insert("sensors/#", "sub3".to_string());

        let matches = trie.find("sensors/temp");
        assert_eq!(matches.len(), 3);
        assert!(matches.contains(&&"sub1".to_string()));
        assert!(matches.contains(&&"sub2".to_string()));
        assert!(matches.contains(&&"sub3".to_string()));

        let matches = trie.find("sensors/humidity");
        assert_eq!(matches.len(), 2);
        assert!(matches.contains(&&"sub2".to_string()));
        assert!(matches.contains(&&"sub3".to_string()));

        let matches = trie.find("sensors/a/b/c");
        assert_eq!(matches.len(), 1);
        assert!(matches.contains(&&"sub3".to_string()));
    }

    #[test]
    fn test_topic_trie_remove() {
        let mut trie = TopicTrieNode::<String>::new();

        trie.insert("sensors/temp", "sub1".to_string());
        trie.insert("sensors/temp", "sub2".to_string());

        assert_eq!(trie.find("sensors/temp").len(), 2);

        trie.remove("sensors/temp", |v| v == "sub1");
        assert_eq!(trie.find("sensors/temp").len(), 1);
    }

    #[test]
    fn test_topic_subscription_index() {
        let mut index = TopicSubscriptionIndex::<String>::new();

        index.subscribe("sensors/+", "client1".to_string());
        index.subscribe("sensors/#", "client2".to_string());

        assert_eq!(index.len(), 2);

        let matches = index.matching("sensors/temp");
        assert_eq!(matches.len(), 2);

        index.unsubscribe("sensors/+", |v| v == "client1");
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_partition_router() {
        let mut router = PartitionRouter::new(4);

        assert_eq!(router.partitions("topic1"), 4);

        router.set_partitions("topic1", 8);
        assert_eq!(router.partitions("topic1"), 8);

        // Keyed routing is deterministic
        let p1 = router.route("topic1", Some(b"key"));
        let p2 = router.route("topic1", Some(b"key"));
        assert_eq!(p1, p2);
    }
}
