use quantum::config::{CommitVisibility, DurabilityConfig, DurabilityMode};
use quantum::dedupe::Direction;
use quantum::offline::{OfflineEntry, OfflineQueue};
use quantum::routing::{jump_consistent_hash, session_partition};
use quantum::session::SessionProcessor;
use quantum::time::SystemClock;
use std::time::{Instant, SystemTime};

#[test]
fn jump_hash_is_stable_and_bounded() {
    let a = session_partition("tenant", "client", 16);
    let b = session_partition("tenant", "client", 16);
    assert_eq!(a, b);
    let expanded = session_partition("tenant", "client", 32);
    assert!(expanded < 32);
    let direct = jump_consistent_hash(42, 8);
    assert!(direct < 8);
}

#[test]
fn offline_queue_tracks_earliest_index() {
    let mut queue = OfflineQueue::default();
    queue.enqueue(OfflineEntry {
        client_id: "c1".into(),
        topic: "tenant/t1/foo".into(),
        qos: quantum::mqtt::Qos::AtLeastOnce,
        retain: false,
        enqueue_index: 5,
        payload: vec![],
    });
    queue.enqueue(OfflineEntry {
        client_id: "c1".into(),
        topic: "tenant/t1/foo".into(),
        qos: quantum::mqtt::Qos::AtLeastOnce,
        retain: false,
        enqueue_index: 10,
        payload: vec![],
    });
    assert_eq!(queue.earliest_offline_queue_index(), 5);
    let _ = queue.dequeue();
    assert_eq!(queue.earliest_offline_queue_index(), 10);
}

#[test]
fn receive_max_hint_reuses_window_within_interval() {
    let ack = quantum::raft::AckContract::new(&DurabilityConfig {
        durability_mode: DurabilityMode::Strict,
        commit_visibility: CommitVisibility::DurableOnly,
        ..Default::default()
    });
    let mut processor = SessionProcessor::new(SystemClock, ack);
    let mut state = quantum::session::SessionState {
        client_id: "c1".into(),
        tenant_id: "t1".into(),
        session_epoch: 0,
        inflight: std::collections::HashMap::new(),
        dedupe_floor_index: 0,
        offline_floor_index: 0,
        status: quantum::session::SessionStatus::Connected,
        earliest_offline_queue_index: 0,
        offline: OfflineQueue::default(),
        keep_alive: 0,
        session_expiry_interval: None,
        last_packet_at: Some(Instant::now()),
        last_packet_at_wall: Some(SystemTime::now()),
        will: None,
        outbound: std::collections::HashMap::new(),
        outbound_tx: None,
        dedupe_entries: Vec::new(),
        forward_progress: Vec::new(),
    };
    let _ = processor.record_dedupe(&mut state, 7, Direction::Publish, 1);
    let first = processor.receive_max_hint(0.5);
    let second = processor.receive_max_hint(0.2);
    assert_eq!(first, second);
}
