use std::collections::BTreeMap;

/// Retained store with deterministic ordering for snapshots.
#[derive(Debug, Default)]
pub struct RetainedStore {
    messages: BTreeMap<String, Vec<u8>>,
}

impl RetainedStore {
    pub fn upsert(&mut self, topic: String, payload: Vec<u8>) {
        self.messages.insert(topic, payload);
    }

    pub fn get(&self, topic: &str) -> Option<&Vec<u8>> {
        self.messages.get(topic)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<u8>)> {
        self.messages.iter()
    }
}

/// Subscription index with deterministic iteration order.
#[derive(Debug, Default)]
pub struct SubscriptionIndex {
    entries: BTreeMap<String, Vec<String>>,
}

impl SubscriptionIndex {
    pub fn add(&mut self, topic: String, subscriber: String) {
        let subs = self.entries.entry(topic).or_default();
        if !subs.contains(&subscriber) {
            subs.push(subscriber);
            subs.sort();
        }
    }

    pub fn remove(&mut self, topic: &str, subscriber: &str) {
        if let Some(subs) = self.entries.get_mut(topic) {
            subs.retain(|s| s != subscriber);
            if subs.is_empty() {
                self.entries.remove(topic);
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<String>)> {
        self.entries.iter()
    }
}
