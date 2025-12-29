use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Shared Subscription Parsing (Task 1)
// ---------------------------------------------------------------------------

/// Parsed shared subscription from `$share/{group}/{filter}` format.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SharedSubscription {
    /// Share group name (e.g., "mygroup").
    pub group: String,
    /// Actual topic filter after the group prefix (e.g., "sensors/+/temp").
    pub filter: String,
}

impl SharedSubscription {
    /// Parse a topic filter into a shared subscription if it matches the
    /// `$share/{group}/{filter}` format. Returns `None` for regular subscriptions.
    pub fn parse(topic_filter: &str) -> Option<Self> {
        if !topic_filter.starts_with("$share/") {
            return None;
        }

        let rest = &topic_filter[7..]; // Skip "$share/"
        let slash_pos = rest.find('/')?;

        if slash_pos == 0 {
            // Empty group name is invalid
            return None;
        }

        let group = &rest[..slash_pos];
        let filter = &rest[slash_pos + 1..];

        // Validate group name: must not be empty and must not contain wildcards
        if group.is_empty() || group.contains('+') || group.contains('#') {
            return None;
        }

        // Filter must not be empty
        if filter.is_empty() {
            return None;
        }

        Some(SharedSubscription {
            group: group.to_string(),
            filter: filter.to_string(),
        })
    }

    /// Check if a topic filter is a shared subscription without full parsing.
    pub fn is_shared(topic_filter: &str) -> bool {
        topic_filter.starts_with("$share/")
    }

    /// Reconstruct the full shared subscription topic filter.
    pub fn to_topic_filter(&self) -> String {
        format!("$share/{}/{}", self.group, self.filter)
    }
}

// ---------------------------------------------------------------------------
// Subscription Record (Task 1)
// ---------------------------------------------------------------------------

/// Subscription record with optional shared subscription and subscription identifier.
#[derive(Debug, Clone)]
pub struct SubscriptionRecord {
    /// Original topic filter (may be shared subscription format).
    pub topic_filter: String,
    /// Parsed shared subscription info (if applicable).
    pub shared: Option<SharedSubscription>,
    /// MQTT 5 subscription identifier for matching.
    pub subscription_identifier: Option<u32>,
    /// Requested QoS level.
    pub qos: crate::mqtt::Qos,
    /// No local flag (don't receive own publishes).
    pub no_local: bool,
    /// Retain as published flag.
    pub retain_as_published: bool,
    /// Retain handling option (0, 1, or 2).
    pub retain_handling: u8,
}

impl SubscriptionRecord {
    /// Create a new subscription record, parsing shared subscription if present.
    pub fn new(
        topic_filter: String,
        qos: crate::mqtt::Qos,
        no_local: bool,
        retain_as_published: bool,
        retain_handling: u8,
        subscription_identifier: Option<u32>,
    ) -> Self {
        let shared = SharedSubscription::parse(&topic_filter);
        Self {
            topic_filter,
            shared,
            subscription_identifier,
            qos,
            no_local,
            retain_as_published,
            retain_handling,
        }
    }

    /// Get the actual topic filter for matching (strips $share prefix).
    pub fn match_filter(&self) -> &str {
        match &self.shared {
            Some(shared) => &shared.filter,
            None => &self.topic_filter,
        }
    }

    /// Check if this is a shared subscription.
    pub fn is_shared(&self) -> bool {
        self.shared.is_some()
    }

    /// Get the share group name if this is a shared subscription.
    pub fn share_group(&self) -> Option<&str> {
        self.shared.as_ref().map(|s| s.group.as_str())
    }
}

// ---------------------------------------------------------------------------
// Subscription Manager (Task 1)
// ---------------------------------------------------------------------------

/// Manages subscriptions for a session with support for shared subscriptions.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionManager {
    /// Regular subscriptions indexed by topic filter.
    subscriptions: HashMap<String, SubscriptionRecord>,
    /// Shared subscription group memberships: group -> set of filters.
    shared_groups: HashMap<String, HashSet<String>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or update a subscription.
    pub fn subscribe(&mut self, record: SubscriptionRecord) {
        let filter = record.topic_filter.clone();

        // Track shared group membership
        if let Some(ref shared) = record.shared {
            self.shared_groups
                .entry(shared.group.clone())
                .or_default()
                .insert(shared.filter.clone());
        }

        self.subscriptions.insert(filter, record);
    }

    /// Remove a subscription.
    pub fn unsubscribe(&mut self, topic_filter: &str) -> Option<SubscriptionRecord> {
        let record = self.subscriptions.remove(topic_filter)?;

        // Remove from shared group tracking
        if let Some(ref shared) = record.shared {
            if let Some(filters) = self.shared_groups.get_mut(&shared.group) {
                filters.remove(&shared.filter);
                if filters.is_empty() {
                    self.shared_groups.remove(&shared.group);
                }
            }
        }

        Some(record)
    }

    /// Get a subscription by topic filter.
    pub fn get(&self, topic_filter: &str) -> Option<&SubscriptionRecord> {
        self.subscriptions.get(topic_filter)
    }

    /// Get all subscriptions.
    pub fn all(&self) -> impl Iterator<Item = &SubscriptionRecord> {
        self.subscriptions.values()
    }

    /// Get subscriptions matching a topic using MQTT wildcard rules.
    pub fn matching(&self, topic: &str) -> Vec<&SubscriptionRecord> {
        self.subscriptions
            .values()
            .filter(|sub| topic_matches(sub.match_filter(), topic))
            .collect()
    }

    /// Get all share groups this manager is participating in.
    pub fn share_groups(&self) -> impl Iterator<Item = &String> {
        self.shared_groups.keys()
    }

    /// Get the filters subscribed under a share group.
    pub fn share_group_filters(&self, group: &str) -> Option<&HashSet<String>> {
        self.shared_groups.get(group)
    }

    /// Count of subscriptions.
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    /// Clear all subscriptions.
    pub fn clear(&mut self) {
        self.subscriptions.clear();
        self.shared_groups.clear();
    }
}

// ---------------------------------------------------------------------------
// Topic Matching (Task 1)
// ---------------------------------------------------------------------------

/// Check if a topic filter matches a topic using MQTT wildcard rules.
/// - `+` matches a single level
/// - `#` matches zero or more levels (must be last)
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    let filter_parts: Vec<&str> = filter.split('/').collect();
    let topic_parts: Vec<&str> = topic.split('/').collect();

    let mut fi = 0;
    let mut ti = 0;

    while fi < filter_parts.len() {
        let fp = filter_parts[fi];

        if fp == "#" {
            // # matches everything remaining
            return true;
        }

        if ti >= topic_parts.len() {
            // Topic exhausted but filter continues
            return false;
        }

        if fp == "+" {
            // + matches exactly one level
            fi += 1;
            ti += 1;
            continue;
        }

        if fp != topic_parts[ti] {
            // Literal mismatch
            return false;
        }

        fi += 1;
        ti += 1;
    }

    // Both must be exhausted for a match
    fi == filter_parts.len() && ti == topic_parts.len()
}

// ---------------------------------------------------------------------------
// Round-Robin Balancer (Task 2)
// ---------------------------------------------------------------------------

/// Member state in a shared subscription group.
#[derive(Debug, Clone)]
pub struct GroupMember {
    /// Client identifier.
    pub client_id: String,
    /// Session epoch to fence stale members.
    pub session_epoch: u64,
    /// Whether the member is currently connected.
    pub connected: bool,
    /// Messages delivered to this member (for fairness tracking).
    pub delivered_count: u64,
}

/// Shared subscription group state.
#[derive(Debug, Clone, Default)]
pub struct SharedGroup {
    /// Group name.
    pub name: String,
    /// Topic filter this group subscribes to.
    pub filter: String,
    /// Members in delivery order.
    members: Vec<GroupMember>,
    /// Current round-robin cursor.
    cursor: usize,
}

impl SharedGroup {
    pub fn new(name: String, filter: String) -> Self {
        Self {
            name,
            filter,
            members: Vec::new(),
            cursor: 0,
        }
    }

    /// Add a member to the group.
    pub fn add_member(&mut self, client_id: String, session_epoch: u64) {
        // Check if member already exists
        if let Some(member) = self.members.iter_mut().find(|m| m.client_id == client_id) {
            member.session_epoch = session_epoch;
            member.connected = true;
            return;
        }

        self.members.push(GroupMember {
            client_id,
            session_epoch,
            connected: true,
            delivered_count: 0,
        });
    }

    /// Remove a member from the group.
    pub fn remove_member(&mut self, client_id: &str) -> bool {
        if let Some(pos) = self.members.iter().position(|m| m.client_id == client_id) {
            self.members.remove(pos);
            if self.cursor > 0 && pos < self.cursor {
                self.cursor -= 1;
            }
            if !self.members.is_empty() && self.cursor >= self.members.len() {
                self.cursor = 0;
            }
            return true;
        }
        false
    }

    /// Mark a member as disconnected (but retain for session persistence).
    pub fn disconnect_member(&mut self, client_id: &str) {
        if let Some(member) = self.members.iter_mut().find(|m| m.client_id == client_id) {
            member.connected = false;
        }
    }

    /// Mark a member as reconnected.
    pub fn reconnect_member(&mut self, client_id: &str, session_epoch: u64) {
        if let Some(member) = self.members.iter_mut().find(|m| m.client_id == client_id) {
            member.connected = true;
            member.session_epoch = session_epoch;
        }
    }

    /// Get connected members.
    pub fn connected_members(&self) -> Vec<&GroupMember> {
        self.members.iter().filter(|m| m.connected).collect()
    }

    /// Select the next member for delivery using round-robin.
    pub fn select_next(&mut self) -> Option<&mut GroupMember> {
        let connected: Vec<usize> = self
            .members
            .iter()
            .enumerate()
            .filter(|(_, m)| m.connected)
            .map(|(i, _)| i)
            .collect();

        if connected.is_empty() {
            return None;
        }

        // Find next connected member starting from cursor
        let start = self.cursor % self.members.len();
        let mut idx = start;
        loop {
            if self.members[idx].connected {
                self.cursor = (idx + 1) % self.members.len();
                self.members[idx].delivered_count += 1;
                return Some(&mut self.members[idx]);
            }
            idx = (idx + 1) % self.members.len();
            if idx == start {
                break;
            }
        }

        None
    }

    /// Check if the group is empty (no members).
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Check if the group has any connected members.
    pub fn has_connected(&self) -> bool {
        self.members.iter().any(|m| m.connected)
    }

    /// Get member count.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Get connected member count.
    pub fn connected_count(&self) -> usize {
        self.members.iter().filter(|m| m.connected).count()
    }
}

/// Round-robin balancer for MQTT shared subscriptions that also suppresses duplicates.
#[derive(Debug, Default, Clone)]
pub struct SharedSubscriptionBalancer {
    next: HashMap<String, usize>,
    assignments: HashMap<(String, u64), String>,
    /// Shared subscription groups: (group_name, filter) -> SharedGroup.
    groups: HashMap<(String, String), SharedGroup>,
}

impl SharedSubscriptionBalancer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Assign a packet to a member of the shared subscription group.
    /// Subsequent calls for the same packet_id return the original member to avoid duplicates.
    pub fn assign(&mut self, group: &str, members: &[String], packet_id: u64) -> Option<String> {
        if members.is_empty() {
            return None;
        }
        let key = (group.to_string(), packet_id);
        if let Some(existing) = self.assignments.get(&key) {
            return Some(existing.clone());
        }
        let cursor = self.next.entry(group.to_string()).or_insert(0);
        let idx = *cursor % members.len();
        *cursor = (*cursor + 1) % members.len();
        let member = members[idx].clone();
        self.assignments.insert(key, member.clone());
        Some(member)
    }

    /// Release an assignment once the delivery is acknowledged so future packets can reuse the slot.
    pub fn release(&mut self, group: &str, packet_id: u64) {
        let key = (group.to_string(), packet_id);
        let _ = self.assignments.remove(&key);
    }

    /// Get or create a shared group.
    pub fn get_or_create_group(&mut self, group: &str, filter: &str) -> &mut SharedGroup {
        let key = (group.to_string(), filter.to_string());
        self.groups
            .entry(key)
            .or_insert_with(|| SharedGroup::new(group.to_string(), filter.to_string()))
    }

    /// Get a shared group.
    pub fn get_group(&self, group: &str, filter: &str) -> Option<&SharedGroup> {
        self.groups.get(&(group.to_string(), filter.to_string()))
    }

    /// Get a mutable shared group.
    pub fn get_group_mut(&mut self, group: &str, filter: &str) -> Option<&mut SharedGroup> {
        self.groups
            .get_mut(&(group.to_string(), filter.to_string()))
    }

    /// Remove a shared group if empty.
    pub fn remove_group_if_empty(&mut self, group: &str, filter: &str) -> bool {
        let key = (group.to_string(), filter.to_string());
        if let Some(g) = self.groups.get(&key) {
            if g.is_empty() {
                self.groups.remove(&key);
                return true;
            }
        }
        false
    }

    /// Add a member to a shared subscription group.
    pub fn add_member(&mut self, group: &str, filter: &str, client_id: &str, session_epoch: u64) {
        let grp = self.get_or_create_group(group, filter);
        grp.add_member(client_id.to_string(), session_epoch);
    }

    /// Remove a member from a shared subscription group.
    pub fn remove_member(&mut self, group: &str, filter: &str, client_id: &str) {
        if let Some(grp) = self.get_group_mut(group, filter) {
            grp.remove_member(client_id);
        }
        self.remove_group_if_empty(group, filter);
    }

    /// Select the next recipient for a shared subscription message.
    pub fn select_recipient(&mut self, group: &str, filter: &str) -> Option<String> {
        let grp = self.get_group_mut(group, filter)?;
        grp.select_next().map(|m| m.client_id.clone())
    }

    /// List all groups.
    pub fn list_groups(&self) -> Vec<(&str, &str)> {
        self.groups
            .keys()
            .map(|(g, f)| (g.as_str(), f.as_str()))
            .collect()
    }

    /// Get group stats for metrics.
    pub fn group_stats(&self) -> Vec<SharedGroupStats> {
        self.groups
            .values()
            .map(|g| SharedGroupStats {
                group: g.name.clone(),
                filter: g.filter.clone(),
                total_members: g.member_count(),
                connected_members: g.connected_count(),
            })
            .collect()
    }

    /// Snapshot for persistence.
    pub fn snapshot(&self) -> SharedSubscriptionSnapshot {
        SharedSubscriptionSnapshot {
            groups: self
                .groups
                .iter()
                .map(|(k, v)| {
                    (
                        (k.0.clone(), k.1.clone()),
                        SharedGroupSnapshot {
                            name: v.name.clone(),
                            filter: v.filter.clone(),
                            members: v
                                .members
                                .iter()
                                .map(|m| GroupMemberSnapshot {
                                    client_id: m.client_id.clone(),
                                    session_epoch: m.session_epoch,
                                    connected: m.connected,
                                    delivered_count: m.delivered_count,
                                })
                                .collect(),
                            cursor: v.cursor,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Restore from snapshot.
    pub fn restore(&mut self, snapshot: SharedSubscriptionSnapshot) {
        self.groups.clear();
        for ((group, filter), gs) in snapshot.groups {
            let mut grp = SharedGroup::new(gs.name, gs.filter);
            grp.cursor = gs.cursor;
            for ms in gs.members {
                grp.members.push(GroupMember {
                    client_id: ms.client_id,
                    session_epoch: ms.session_epoch,
                    connected: ms.connected,
                    delivered_count: ms.delivered_count,
                });
            }
            self.groups.insert((group, filter), grp);
        }
    }
}

/// Statistics for a shared subscription group.
#[derive(Debug, Clone)]
pub struct SharedGroupStats {
    pub group: String,
    pub filter: String,
    pub total_members: usize,
    pub connected_members: usize,
}

/// Snapshot of shared subscription state for persistence.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SharedSubscriptionSnapshot {
    pub groups: HashMap<(String, String), SharedGroupSnapshot>,
}

/// Snapshot of a single shared group.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SharedGroupSnapshot {
    pub name: String,
    pub filter: String,
    pub members: Vec<GroupMemberSnapshot>,
    pub cursor: usize,
}

/// Snapshot of a group member.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GroupMemberSnapshot {
    pub client_id: String,
    pub session_epoch: u64,
    pub connected: bool,
    pub delivered_count: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_subscription_parse() {
        // Valid shared subscriptions
        assert_eq!(
            SharedSubscription::parse("$share/mygroup/sensors/+/temp"),
            Some(SharedSubscription {
                group: "mygroup".to_string(),
                filter: "sensors/+/temp".to_string(),
            })
        );

        assert_eq!(
            SharedSubscription::parse("$share/g1/topic"),
            Some(SharedSubscription {
                group: "g1".to_string(),
                filter: "topic".to_string(),
            })
        );

        assert_eq!(
            SharedSubscription::parse("$share/group/#"),
            Some(SharedSubscription {
                group: "group".to_string(),
                filter: "#".to_string(),
            })
        );

        // Invalid: not a shared subscription
        assert_eq!(SharedSubscription::parse("sensors/temp"), None);
        assert_eq!(SharedSubscription::parse("$SYS/broker/clients"), None);

        // Invalid: empty group
        assert_eq!(SharedSubscription::parse("$share//topic"), None);

        // Invalid: empty filter
        assert_eq!(SharedSubscription::parse("$share/group/"), None);

        // Invalid: wildcard in group name
        assert_eq!(SharedSubscription::parse("$share/+/topic"), None);
        assert_eq!(SharedSubscription::parse("$share/#/topic"), None);
    }

    #[test]
    fn test_shared_subscription_is_shared() {
        assert!(SharedSubscription::is_shared("$share/g/topic"));
        assert!(!SharedSubscription::is_shared("topic"));
        assert!(!SharedSubscription::is_shared("$SYS/broker"));
    }

    #[test]
    fn test_shared_subscription_to_topic_filter() {
        let sub = SharedSubscription {
            group: "mygroup".to_string(),
            filter: "sensors/+".to_string(),
        };
        assert_eq!(sub.to_topic_filter(), "$share/mygroup/sensors/+");
    }

    #[test]
    fn test_topic_matches_exact() {
        assert!(topic_matches("a/b/c", "a/b/c"));
        assert!(!topic_matches("a/b/c", "a/b/d"));
        assert!(!topic_matches("a/b", "a/b/c"));
        assert!(!topic_matches("a/b/c", "a/b"));
    }

    #[test]
    fn test_topic_matches_plus_wildcard() {
        assert!(topic_matches("a/+/c", "a/b/c"));
        assert!(topic_matches("a/+/c", "a/x/c"));
        assert!(!topic_matches("a/+/c", "a/b/d"));
        assert!(!topic_matches("a/+/c", "a/b/c/d"));
        assert!(topic_matches("+/+/+", "a/b/c"));
        assert!(!topic_matches("+/+/+", "a/b"));
    }

    #[test]
    fn test_topic_matches_hash_wildcard() {
        assert!(topic_matches("a/#", "a"));
        assert!(topic_matches("a/#", "a/b"));
        assert!(topic_matches("a/#", "a/b/c"));
        assert!(topic_matches("#", "a"));
        assert!(topic_matches("#", "a/b/c"));
        assert!(!topic_matches("a/#", "b"));
    }

    #[test]
    fn test_topic_matches_combined() {
        assert!(topic_matches("a/+/#", "a/b"));
        assert!(topic_matches("a/+/#", "a/b/c"));
        assert!(topic_matches("a/+/#", "a/b/c/d"));
        assert!(!topic_matches("a/+/#", "a"));
    }

    #[test]
    fn test_subscription_record() {
        let record = SubscriptionRecord::new(
            "$share/mygroup/sensors/+".to_string(),
            crate::mqtt::Qos::AtLeastOnce,
            false,
            true,
            0,
            Some(42),
        );

        assert!(record.is_shared());
        assert_eq!(record.share_group(), Some("mygroup"));
        assert_eq!(record.match_filter(), "sensors/+");
        assert_eq!(record.subscription_identifier, Some(42));
    }

    #[test]
    fn test_subscription_manager() {
        let mut mgr = SubscriptionManager::new();

        mgr.subscribe(SubscriptionRecord::new(
            "sensors/temp".to_string(),
            crate::mqtt::Qos::AtMostOnce,
            false,
            false,
            0,
            None,
        ));

        mgr.subscribe(SubscriptionRecord::new(
            "$share/g1/sensors/+".to_string(),
            crate::mqtt::Qos::AtLeastOnce,
            false,
            false,
            0,
            Some(1),
        ));

        assert_eq!(mgr.len(), 2);
        assert!(mgr.get("sensors/temp").is_some());
        assert!(mgr.get("$share/g1/sensors/+").is_some());

        let matching = mgr.matching("sensors/temp");
        assert_eq!(matching.len(), 2); // Both match

        let groups: Vec<_> = mgr.share_groups().collect();
        assert_eq!(groups, vec!["g1"]);

        mgr.unsubscribe("$share/g1/sensors/+");
        assert_eq!(mgr.len(), 1);
        assert!(mgr.share_groups().next().is_none());
    }

    #[test]
    fn test_shared_group_round_robin() {
        let mut group = SharedGroup::new("g1".to_string(), "topic".to_string());

        group.add_member("client1".to_string(), 1);
        group.add_member("client2".to_string(), 1);
        group.add_member("client3".to_string(), 1);

        // Round-robin selection
        assert_eq!(
            group.select_next().map(|m| m.client_id.as_str()),
            Some("client1")
        );
        assert_eq!(
            group.select_next().map(|m| m.client_id.as_str()),
            Some("client2")
        );
        assert_eq!(
            group.select_next().map(|m| m.client_id.as_str()),
            Some("client3")
        );
        assert_eq!(
            group.select_next().map(|m| m.client_id.as_str()),
            Some("client1")
        );

        // Disconnect a member
        group.disconnect_member("client2");
        assert_eq!(group.connected_count(), 2);

        // Should skip disconnected
        assert_eq!(
            group.select_next().map(|m| m.client_id.as_str()),
            Some("client3")
        );
        assert_eq!(
            group.select_next().map(|m| m.client_id.as_str()),
            Some("client1")
        );
    }

    #[test]
    fn test_shared_subscription_balancer() {
        let mut balancer = SharedSubscriptionBalancer::new();

        balancer.add_member("g1", "sensors/+", "client1", 1);
        balancer.add_member("g1", "sensors/+", "client2", 1);

        let r1 = balancer.select_recipient("g1", "sensors/+");
        let r2 = balancer.select_recipient("g1", "sensors/+");

        assert!(r1.is_some());
        assert!(r2.is_some());
        assert_ne!(r1, r2); // Round-robin should alternate

        // Stats
        let stats = balancer.group_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].group, "g1");
        assert_eq!(stats[0].total_members, 2);
    }
}
