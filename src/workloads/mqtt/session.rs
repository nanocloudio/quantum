use super::protocol::Will;
use crate::dedupe::{DedupeKey, DedupeTable, Direction, PersistedDedupeEntry};
use crate::flow::{credit_hint, CreditParams, RECEIVE_MAX_RECALC_INTERVAL};
use crate::forwarding::ForwardReceipt;
use crate::offline::OfflineQueue;
use crate::prg::{ForwardError, PrgManager};
use crate::replication::consensus::AckContract;
use crate::routing::{ForwardSeqEntry, ForwardSeqTracker, PrgId, RoutingEpoch};
use crate::storage::compaction::ProductFloors;
use crate::time::Clock;
use anyhow::Result;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    Connected,
    Disconnected,
    Migrating,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OutboundMessage {
    pub client_id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: crate::mqtt::Qos,
    pub retain: bool,
    pub enqueue_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OutboundStage {
    WaitPubAck,
    WaitPubRec,
    WaitPubComp,
}

#[derive(Debug, Clone)]
pub struct OutboundTracking {
    pub msg: OutboundMessage,
    pub stage: OutboundStage,
    pub mid: u16,
    pub retry_at: Instant,
    pub retry_delay: Duration,
}

/// Session state persisted for dedupe/offline queue bookkeeping and resume.
#[derive(Debug, Clone)]
pub struct SessionState {
    pub client_id: String,
    pub tenant_id: String,
    pub session_epoch: u64,
    pub inflight: HashMap<u16, InflightRecord>,
    pub dedupe_floor_index: u64,
    pub offline_floor_index: u64,
    pub status: SessionStatus,
    pub earliest_offline_queue_index: u64,
    pub offline: OfflineQueue,
    pub keep_alive: u16,
    pub session_expiry_interval: Option<u32>,
    pub last_packet_at: Option<Instant>,
    pub last_packet_at_wall: Option<SystemTime>,
    pub will: Option<Will>,
    pub outbound: HashMap<u16, OutboundTracking>,
    pub outbound_tx: Option<mpsc::Sender<OutboundMessage>>,
    pub dedupe_entries: Vec<PersistedDedupeEntry>,
    pub forward_progress: Vec<ForwardSeqEntry>,
}

#[derive(Debug, Clone)]
pub struct InflightRecord {
    pub message_id: u16,
    pub created_at: Duration,
}

/// Session processor hook; actual MQTT handling will live here.
pub struct SessionProcessor<C: Clock> {
    #[allow(dead_code)]
    pub(crate) clock: C,
    #[allow(dead_code)]
    ack_contract: AckContract,
    dedupe: DedupeTable,
    forward_seq: ForwardSeqTracker,
    credit_params: CreditParams,
    last_receive_max: Option<(std::time::Instant, u16)>,
    next_mid: u16,
}

impl<C: Clock> SessionProcessor<C> {
    pub fn new(clock: C, ack_contract: AckContract) -> Self {
        let horizon = Duration::from_secs(72 * 60 * 60);
        Self {
            clock,
            ack_contract,
            dedupe: DedupeTable::new(4_096, 64_000_000, horizon),
            forward_seq: ForwardSeqTracker::default(),
            credit_params: CreditParams::default(),
            last_receive_max: None,
            next_mid: 1,
        }
    }

    /// Fence inflight via session_epoch++ during migration.
    pub fn fence_inflight(&mut self, state: &mut SessionState) {
        state.session_epoch = state.session_epoch.saturating_add(1);
        state.inflight.clear();
        state.status = SessionStatus::Migrating;
        state.last_packet_at = Some(self.clock.now());
    }

    /// Placeholder for cross-PRG publish; enforces routing epoch gate.
    #[allow(clippy::too_many_arguments)]
    pub async fn forward_publish(
        &mut self,
        prg_manager: &PrgManager<C>,
        session_prg: &PrgId,
        topic_prg: &PrgId,
        routing_epoch: RoutingEpoch,
        topic: &str,
        payload: &[u8],
        qos: crate::mqtt::Qos,
        retain: bool,
    ) -> Result<ForwardReceipt, ForwardError> {
        prg_manager
            .check_epoch(routing_epoch)
            .await
            .map_err(ForwardError::Routing)?;
        let session_label = format!("{}:{}", session_prg.tenant_id, session_prg.partition_index);
        let topic_label = format!("{}:{}", topic_prg.tenant_id, topic_prg.partition_index);
        let seq = self.next_forward_seq(&session_label, &topic_label, routing_epoch);
        prg_manager
            .forward_topic_publish(
                session_prg,
                topic_prg,
                seq,
                routing_epoch,
                topic,
                payload,
                qos,
                retain,
            )
            .await
    }

    /// Record dedupe entry; returns true if duplicate.
    pub fn record_dedupe(
        &mut self,
        state: &mut SessionState,
        message_id: u16,
        direction: Direction,
        wal_index: u64,
    ) -> bool {
        let key = DedupeKey {
            tenant_id: state.tenant_id.clone(),
            client_id: state.client_id.clone(),
            session_epoch: state.session_epoch,
            message_id,
            direction,
        };
        if self.dedupe.is_duplicate(&key) {
            true
        } else {
            let _ = self.dedupe.record(key, wal_index);
            state.dedupe_floor_index = self.dedupe.earliest_index();
            state.dedupe_entries = self.dedupe.snapshot();
            false
        }
    }

    /// Update the dedupe wal index for a publish after commit.
    pub fn update_publish_dedupe_floor(
        &mut self,
        state: &mut SessionState,
        message_id: u16,
        wal_index: u64,
    ) {
        let key = DedupeKey {
            tenant_id: state.tenant_id.clone(),
            client_id: state.client_id.clone(),
            session_epoch: state.session_epoch,
            message_id,
            direction: Direction::Publish,
        };
        self.dedupe.update_wal_index(&key, wal_index);
        state.dedupe_floor_index = self.dedupe.earliest_index();
        state.dedupe_entries = self.dedupe.snapshot();
    }

    /// Record PUBREL/PUBREC progress for QoS2 idempotence; returns true if duplicate.
    pub fn record_ack_progress(
        &mut self,
        state: &mut SessionState,
        message_id: u16,
        wal_index: u64,
    ) -> bool {
        let key = DedupeKey {
            tenant_id: state.tenant_id.clone(),
            client_id: state.client_id.clone(),
            session_epoch: state.session_epoch,
            message_id,
            direction: Direction::Ack,
        };
        let duplicate = self.dedupe.is_duplicate(&key);
        let _ = self.dedupe.record_or_update(key, wal_index);
        state.dedupe_floor_index = self.dedupe.earliest_index();
        state.dedupe_entries = self.dedupe.snapshot();
        duplicate
    }

    pub fn dedupe_wal_index(
        &self,
        state: &SessionState,
        message_id: u16,
        direction: Direction,
    ) -> Option<u64> {
        let key = DedupeKey {
            tenant_id: state.tenant_id.clone(),
            client_id: state.client_id.clone(),
            session_epoch: state.session_epoch,
            message_id,
            direction,
        };
        self.dedupe.wal_index(&key)
    }

    /// Generate monotone forward_seq per (session_PRG, topic_PRG, routing_epoch).
    pub fn next_forward_seq(
        &mut self,
        session_prg: &str,
        topic_prg: &str,
        routing_epoch: RoutingEpoch,
    ) -> u64 {
        self.forward_seq
            .next_seq(session_prg, topic_prg, routing_epoch)
    }

    /// Compute compaction floors for this session PRG.
    pub fn product_floors(&self, state: &SessionState, clustor_floor: u64) -> ProductFloors {
        ProductFloors {
            clustor_floor,
            local_dedup_floor: state.dedupe_floor_index,
            earliest_offline_queue_index: state
                .earliest_offline_queue_index
                .max(state.offline_floor_index),
            forward_chain_floor: state.offline_floor_index,
        }
    }

    pub fn record_inflight(&mut self, state: &mut SessionState, message_id: u16) -> bool {
        if state.inflight.contains_key(&message_id) {
            return false;
        }
        let created_at = self.clock.now().elapsed();
        state.inflight.insert(
            message_id,
            InflightRecord {
                message_id,
                created_at,
            },
        );
        true
    }

    pub fn receive_max_hint(&mut self, headroom: f32) -> u16 {
        let now = self.clock.now();
        if let Some((ts, value)) = self.last_receive_max {
            if now.duration_since(ts) < RECEIVE_MAX_RECALC_INTERVAL {
                return value;
            }
        }
        let hint = credit_hint(
            self.credit_params.base_window,
            headroom,
            self.credit_params.follower_factor,
        ) as u16;
        self.last_receive_max = Some((now, hint));
        hint
    }

    pub fn next_mid(&mut self) -> u16 {
        let mid = self.next_mid;
        self.next_mid = if mid == u16::MAX { 1 } else { mid + 1 };
        mid
    }

    pub fn record_activity(&self, state: &mut SessionState) {
        state.last_packet_at = Some(self.clock.now());
        state.last_packet_at_wall = Some(SystemTime::now());
    }

    pub fn keep_alive_expired(&self, state: &SessionState) -> bool {
        if state.keep_alive == 0 {
            return false;
        }
        if let Some(last) = state.last_packet_at {
            if self.clock.now().saturating_duration_since(last).as_secs() >= state.keep_alive as u64
            {
                return true;
            }
        }
        if let Some(wall) = state.last_packet_at_wall {
            if let Ok(elapsed) = SystemTime::now().duration_since(wall) {
                return elapsed.as_secs() >= state.keep_alive as u64;
            }
        }
        false
    }

    pub fn session_expired(&self, state: &SessionState) -> bool {
        if let Some(secs) = state.session_expiry_interval {
            if let Some(last) = state.last_packet_at {
                if self.clock.now().saturating_duration_since(last).as_secs() >= secs as u64 {
                    return true;
                }
            }
            if let Some(wall) = state.last_packet_at_wall {
                if let Ok(elapsed) = SystemTime::now().duration_since(wall) {
                    return elapsed.as_secs() >= secs as u64;
                }
            }
        }
        false
    }

    /// Seed dedupe and forward_seq state from persisted session metadata.
    pub fn hydrate(&mut self, state: &SessionState) {
        self.dedupe.hydrate(&state.dedupe_entries, self.clock.now());
        self.forward_seq.hydrate(&state.forward_progress);
    }

    /// Snapshot dedupe entries for persistence.
    pub fn dedupe_snapshot(&self) -> Vec<PersistedDedupeEntry> {
        self.dedupe.snapshot()
    }

    /// Snapshot forward_seq tracker for persistence.
    pub fn forward_seq_snapshot(&self) -> Vec<ForwardSeqEntry> {
        self.forward_seq.snapshot()
    }
}

/// Persisted inflight QoS tracking entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedInflight {
    pub message_id: u16,
}

/// Persisted outbound delivery tracker.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedOutbound {
    pub mid: u16,
    pub stage: OutboundStage,
    pub msg: OutboundMessage,
    #[serde(default)]
    pub retry_delay_ms: u64,
}
