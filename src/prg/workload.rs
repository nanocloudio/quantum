//! Workload abstraction for PRG hosts.
//!
//! This module defines the trait and helper contexts used by the runtime to
//! interact with protocol workloads (MQTT today). It keeps per-workload state
//! isolated while letting the host drive lifecycle hooks, snapshots, and apply
//! logic via a uniform API.

use crate::audit;
use crate::flow::{BackpressureSnapshot, BackpressureState};
use crate::forwarding::{ForwardRequest, ForwardingEngine};
use crate::offline::OfflineEntry;
use crate::replication::consensus::AckContract;
use crate::routing::PrgId;
use crate::storage::compaction::ProductFloors;
use bitflags::bitflags;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::Span;

bitflags! {
    /// Capability surface a workload exposes to the runtime and control plane.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CapabilityFlags: u32 {
        const OFFLINE_QUEUES = 0b0001;
        const DEDUPE = 0b0010;
        const RETAINED = 0b0100;
        const CUSTOM_COMPACTION = 0b1000;
    }
}

/// Descriptor recorded alongside placements and telemetry.
#[derive(Debug, Clone, Copy)]
pub struct WorkloadDescriptor {
    pub label: &'static str,
    pub version: u16,
    pub capabilities: CapabilityFlags,
}

impl WorkloadDescriptor {
    pub const fn new(label: &'static str, version: u16, capabilities: CapabilityFlags) -> Self {
        Self {
            label,
            version,
            capabilities,
        }
    }
}

/// Feature toggles resolved from the control plane manifest.
#[derive(Debug, Clone)]
pub struct WorkloadFeatures {
    pub enable_offline_queues: bool,
    pub enable_dedupe: bool,
    pub enable_retained: bool,
    pub enable_custom_compaction: bool,
}

impl Default for WorkloadFeatures {
    fn default() -> Self {
        Self {
            enable_offline_queues: true,
            enable_dedupe: true,
            enable_retained: true,
            enable_custom_compaction: false,
        }
    }
}

/// Immutable data provided when a workload instance is initialized.
#[derive(Clone)]
pub struct WorkloadInit<'a> {
    pub prg_id: &'a PrgId,
    pub ack_contract: Arc<AckContract>,
    pub schemas: SchemaRegistryHandle,
    pub features: &'a WorkloadFeatures,
    pub leadership: LeadershipSignal,
    pub span: Span,
}

impl<'a> WorkloadInit<'a> {
    pub fn new(
        prg_id: &'a PrgId,
        ack_contract: Arc<AckContract>,
        schemas: SchemaRegistryHandle,
        features: &'a WorkloadFeatures,
        leadership: LeadershipSignal,
        span: Span,
    ) -> Self {
        Self {
            prg_id,
            ack_contract,
            schemas,
            features,
            leadership,
            span,
        }
    }
}

/// Snapshot of metadata exposed to workloads during apply.
#[derive(Debug, Clone, Copy)]
pub struct ApplyMeta {
    pub term: u64,
    pub index: u64,
    pub ack_floor: u64,
}

impl ApplyMeta {
    pub const fn new(term: u64, index: u64, ack_floor: u64) -> Self {
        Self {
            term,
            index,
            ack_floor,
        }
    }
}

/// Forwarding intent captured during apply.
#[derive(Debug, Clone)]
pub struct ForwardIntent {
    pub request: ForwardRequest,
    pub envelope: Vec<u8>,
    pub workload: &'static str,
}

/// Handle to track compaction floors without exposing raw host state.
#[derive(Clone)]
pub struct CompactionHandle {
    floors: Arc<Mutex<ProductFloors>>,
}

impl CompactionHandle {
    pub fn new(initial: ProductFloors) -> Self {
        Self {
            floors: Arc::new(Mutex::new(initial)),
        }
    }

    pub fn snapshot(&self) -> ProductFloors {
        self.floors.lock().map(|f| *f).unwrap_or(ProductFloors {
            clustor_floor: 0,
            local_dedup_floor: 0,
            earliest_offline_queue_index: 0,
            forward_chain_floor: 0,
        })
    }

    pub fn update(&self, floors: ProductFloors) {
        if let Ok(mut guard) = self.floors.lock() {
            *guard = floors;
        }
    }
}

impl Default for CompactionHandle {
    fn default() -> Self {
        Self::new(ProductFloors {
            clustor_floor: 0,
            local_dedup_floor: 0,
            earliest_offline_queue_index: 0,
            forward_chain_floor: 0,
        })
    }
}

/// Workload-facing metrics wrapper.
#[derive(Clone)]
pub struct WorkloadMetrics {
    state: Arc<BackpressureState>,
}

impl WorkloadMetrics {
    pub fn new(state: Arc<BackpressureState>) -> Self {
        Self { state }
    }

    pub fn snapshot(&self) -> BackpressureSnapshot {
        self.state.snapshot()
    }
}

impl Default for WorkloadMetrics {
    fn default() -> Self {
        Self {
            state: Arc::new(BackpressureState::default()),
        }
    }
}

/// Schema registry shim so workloads can validate schemas during init.
#[derive(Clone, Default)]
pub struct SchemaRegistryHandle {
    inner: Arc<SchemaRegistry>,
}

impl SchemaRegistryHandle {
    pub fn register_workload(&self, descriptor: WorkloadDescriptor) {
        self.inner.register(descriptor);
    }

    pub fn ensure_workload(
        &self,
        descriptor: &WorkloadDescriptor,
    ) -> Result<(), SchemaRegistryError> {
        self.inner.ensure(descriptor)
    }
}

#[derive(Default)]
struct SchemaRegistry {
    workloads: Mutex<HashMap<&'static str, WorkloadDescriptor>>,
}

impl SchemaRegistry {
    fn register(&self, descriptor: WorkloadDescriptor) {
        if let Ok(mut guard) = self.workloads.lock() {
            guard.insert(descriptor.label, descriptor);
        }
    }

    fn ensure(&self, descriptor: &WorkloadDescriptor) -> Result<(), SchemaRegistryError> {
        let guard = self
            .workloads
            .lock()
            .map_err(|_| SchemaRegistryError::Unavailable("lock poisoned".into()))?;
        if let Some(existing) = guard.get(descriptor.label) {
            if existing.version >= descriptor.version {
                return Ok(());
            }
            return Err(SchemaRegistryError::VersionMismatch {
                expected: existing.version,
                actual: descriptor.version,
            });
        }
        Err(SchemaRegistryError::Missing(descriptor.label.into()))
    }
}

/// Errors surfaced by the schema registry.
#[derive(Debug, thiserror::Error)]
pub enum SchemaRegistryError {
    #[error("workload schema missing: {0}")]
    Missing(Cow<'static, str>),
    #[error("schema registry unavailable: {0}")]
    Unavailable(Cow<'static, str>),
    #[error("schema version mismatch (expected >= {expected}, got {actual})")]
    VersionMismatch { expected: u16, actual: u16 },
}

/// Context exposed to workloads during apply.
pub struct ApplyContext {
    ack_contract: Arc<AckContract>,
    metrics: WorkloadMetrics,
    compaction: CompactionHandle,
    schemas: SchemaRegistryHandle,
    prg_id: PrgId,
    meta: ApplyMeta,
    forward_intents: Vec<ForwardIntent>,
    offline_entries: Vec<OfflineEntry>,
    #[allow(dead_code)]
    span: Span,
    forwarding: ForwardingEngine,
}

impl ApplyContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ack_contract: Arc<AckContract>,
        metrics: WorkloadMetrics,
        compaction: CompactionHandle,
        schemas: SchemaRegistryHandle,
        prg_id: PrgId,
        meta: ApplyMeta,
        span: Span,
        forwarding: ForwardingEngine,
    ) -> Self {
        Self {
            ack_contract,
            metrics,
            compaction,
            schemas,
            prg_id,
            meta,
            forward_intents: Vec::new(),
            offline_entries: Vec::new(),
            span,
            forwarding,
        }
    }

    pub fn metrics(&self) -> &WorkloadMetrics {
        &self.metrics
    }

    pub fn ack_contract(&self) -> &AckContract {
        &self.ack_contract
    }

    pub fn compaction(&self) -> &CompactionHandle {
        &self.compaction
    }

    pub fn schemas(&self) -> &SchemaRegistryHandle {
        &self.schemas
    }

    pub fn prg_id(&self) -> &PrgId {
        &self.prg_id
    }

    pub fn meta(&self) -> ApplyMeta {
        self.meta
    }

    pub fn forwarding(&self) -> ForwardingEngine {
        self.forwarding.clone()
    }

    pub fn record_forward_intent(
        &mut self,
        request: ForwardRequest,
        envelope: Vec<u8>,
        workload: &'static str,
    ) {
        self.forward_intents.push(ForwardIntent {
            request,
            envelope,
            workload,
        });
    }

    pub fn take_forward_intents(&mut self) -> Vec<ForwardIntent> {
        std::mem::take(&mut self.forward_intents)
    }

    pub fn forward_intents(&self) -> &[ForwardIntent] {
        &self.forward_intents
    }

    pub fn record_offline_entry(&mut self, entry: OfflineEntry) {
        self.offline_entries.push(entry);
    }

    pub fn offline_entries(&self) -> &[OfflineEntry] {
        &self.offline_entries
    }

    pub fn take_offline_entries(&mut self) -> Vec<OfflineEntry> {
        std::mem::take(&mut self.offline_entries)
    }
}

/// Context provided once replay is finished so workloads can publish readiness signals.
pub struct ReadyContext {
    prg_id: PrgId,
    metrics: WorkloadMetrics,
    leadership: LeadershipSignal,
    #[allow(dead_code)]
    span: Span,
}

impl ReadyContext {
    pub fn new(
        prg_id: PrgId,
        metrics: WorkloadMetrics,
        leadership: LeadershipSignal,
        span: Span,
    ) -> Self {
        Self {
            prg_id,
            metrics,
            leadership,
            span,
        }
    }

    pub fn emit_audit(&self, event: &str, client: &str, message: &str) {
        audit::emit(event, &self.prg_id.tenant_id, client, message);
    }

    pub fn metrics(&self) -> &WorkloadMetrics {
        &self.metrics
    }

    pub fn leadership(&self) -> LeadershipSignal {
        self.leadership.clone()
    }
}

/// Result returned by workload callbacks.
pub struct WorkloadResult<T> {
    outcome: Result<T, WorkloadError>,
    client_hint: Option<ClientErrorHint>,
    metric_label: Option<Cow<'static, str>>,
}

impl<T> WorkloadResult<T> {
    pub fn ok(value: T) -> Self {
        Self {
            outcome: Ok(value),
            client_hint: None,
            metric_label: None,
        }
    }

    pub fn from_error(error: WorkloadError) -> Self {
        Self {
            outcome: Err(error),
            client_hint: None,
            metric_label: None,
        }
    }

    pub fn into_result(self) -> Result<T, WorkloadError> {
        self.outcome
    }

    pub fn with_hint(mut self, hint: ClientErrorHint) -> Self {
        self.client_hint = Some(hint);
        self
    }

    pub fn with_metric_label(mut self, label: &'static str) -> Self {
        self.metric_label = Some(label.into());
        self
    }

    pub fn client_hint(&self) -> Option<&ClientErrorHint> {
        self.client_hint.as_ref()
    }

    pub fn metric_label(&self) -> Option<&str> {
        self.metric_label.as_deref()
    }
}

pub trait PrgWorkload: Send + Sync + 'static {
    type Snapshot: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static;
    type LogEntry: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static;

    fn descriptor(&self) -> WorkloadDescriptor;
    fn init(&mut self, init: WorkloadInit<'_>) -> WorkloadResult<()>;
    fn hydrate(&mut self, snapshot: &Self::Snapshot) -> WorkloadResult<()>;
    fn build_snapshot(&self) -> WorkloadResult<Self::Snapshot>;
    fn apply(&mut self, entry: Self::LogEntry, ctx: &mut ApplyContext) -> WorkloadResult<()>;
    fn on_ready(&mut self, _ready: &mut ReadyContext) -> WorkloadResult<()> {
        WorkloadResult::ok(())
    }
}

/// Uniform workload error contract surfaced back to `PrgHost`.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WorkloadError {
    #[error("permanent workload failure: {0}")]
    Permanent(Cow<'static, str>),
    #[error("transient workload failure: {0}")]
    Transient(Cow<'static, str>),
    #[error("invalid workload input: {0}")]
    Input(Cow<'static, str>),
    #[error("workload backpressure: {0}")]
    Backpressure(Cow<'static, str>),
}

/// Hints surfaced back to listeners/control-plane about workload failures.
#[derive(Debug, Clone)]
pub enum ClientErrorHint {
    ReasonCode(u16),
    RetryAfter(Duration),
    DropConnection,
}

/// Observable leadership state shared with workloads.
#[derive(Clone, Default)]
pub struct LeadershipSignal {
    leader: Arc<AtomicBool>,
}

impl LeadershipSignal {
    pub fn new() -> Self {
        Self {
            leader: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set(&self, is_leader: bool) {
        self.leader.store(is_leader, Ordering::Relaxed);
    }

    pub fn is_leader(&self) -> bool {
        self.leader.load(Ordering::Relaxed)
    }
}
