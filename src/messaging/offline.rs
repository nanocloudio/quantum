use std::collections::VecDeque;

/// Offline queue entry with durable enqueue order.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OfflineEntry {
    pub client_id: String,
    pub topic: String,
    pub qos: crate::mqtt::Qos,
    pub retain: bool,
    pub enqueue_index: u64,
    pub payload: Vec<u8>,
}

/// Per-session offline queue with compaction floor tracking.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct OfflineQueue {
    entries: VecDeque<OfflineEntry>,
    earliest_index: Option<u64>,
    #[serde(default)]
    total_bytes: u64,
}

impl OfflineQueue {
    pub fn enqueue(&mut self, entry: OfflineEntry) {
        self.earliest_index = Some(
            self.earliest_index
                .map(|idx| idx.min(entry.enqueue_index))
                .unwrap_or(entry.enqueue_index),
        );
        self.total_bytes = self.total_bytes.saturating_add(entry.payload.len() as u64);
        self.entries.push_back(entry);
    }

    pub fn dequeue(&mut self) -> Option<OfflineEntry> {
        let item = self.entries.pop_front();
        self.earliest_index = self.entries.front().map(|e| e.enqueue_index);
        if let Some(ref e) = item {
            self.total_bytes = self.total_bytes.saturating_sub(e.payload.len() as u64);
        }
        item
    }

    pub fn earliest_offline_queue_index(&self) -> u64 {
        self.earliest_index.unwrap_or(0)
    }

    pub fn enqueue_index(&self) -> u64 {
        self.entries.back().map(|e| e.enqueue_index).unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn bytes(&self) -> u64 {
        self.total_bytes
    }
}
