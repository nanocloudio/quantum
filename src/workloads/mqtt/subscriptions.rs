use std::collections::HashMap;

/// Round-robin balancer for MQTT shared subscriptions that also suppresses duplicates.
#[derive(Debug, Default, Clone)]
pub struct SharedSubscriptionBalancer {
    next: HashMap<String, usize>,
    assignments: HashMap<(String, u64), String>,
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
}
