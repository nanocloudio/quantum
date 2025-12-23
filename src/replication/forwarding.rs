use crate::flow::BackpressureState;
use crate::prg::workload::CapabilityFlags;
use crate::replication::consensus::AckContract;
use crate::routing::{PrgId, RoutingEpoch, RoutingError};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type ForwardKey = (PrgId, PrgId, u64);

// -----------------------------------------------------------------------------
// Workload-agnostic Forward Envelope Types
// -----------------------------------------------------------------------------

/// Workload-agnostic envelope for cross-PRG forwards.
///
/// Wraps schema ID and payload bytes so the forwarding engine stays
/// protocol-agnostic. Workloads create these envelopes before handing
/// them to the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadForwardEnvelope {
    /// Workload label (e.g., "mqtt").
    pub workload: Cow<'static, str>,
    /// Entry type within the workload (e.g., "commit_publish").
    pub entry_type: Cow<'static, str>,
    /// Schema version for encoding compatibility.
    pub schema_version: u16,
    /// Capability flags required for this forward.
    pub capability_bits: u32,
    /// Serialized workload-specific payload.
    pub payload: Vec<u8>,
}

impl WorkloadForwardEnvelope {
    /// Create a new forward envelope.
    pub fn new(
        workload: impl Into<Cow<'static, str>>,
        entry_type: impl Into<Cow<'static, str>>,
        schema_version: u16,
        capabilities: CapabilityFlags,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            workload: workload.into(),
            entry_type: entry_type.into(),
            schema_version,
            capability_bits: capabilities.bits(),
            payload,
        }
    }

    /// Get the workload label.
    pub fn workload(&self) -> &str {
        &self.workload
    }

    /// Get the entry type.
    pub fn entry_type(&self) -> &str {
        &self.entry_type
    }

    /// Get capability flags.
    pub fn capabilities(&self) -> CapabilityFlags {
        CapabilityFlags::from_bits_truncate(self.capability_bits)
    }

    /// Encode envelope for RPC transport.
    pub fn encode(&self) -> Result<Vec<u8>, ForwardEnvelopeError> {
        bincode::serialize(self).map_err(|e| ForwardEnvelopeError::Encode(e.to_string()))
    }

    /// Decode envelope from RPC transport.
    pub fn decode(bytes: &[u8]) -> Result<Self, ForwardEnvelopeError> {
        bincode::deserialize(bytes).map_err(|e| ForwardEnvelopeError::Decode(e.to_string()))
    }
}

/// Forward-plane message for RPC transport.
///
/// This is the wire format for cross-node forwards, carrying all metadata
/// needed for the receiving host to dispatch to the correct workload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardPlaneMessage {
    /// Source PRG ID.
    pub source_prg: PrgId,
    /// Target PRG ID.
    pub target_prg: PrgId,
    /// Sequence number for duplicate suppression.
    pub seq: u64,
    /// Routing epoch at time of forward.
    pub routing_epoch: u64,
    /// Local epoch of the sender.
    pub local_epoch: u64,
    /// Whether to allow grace window for epoch drift.
    pub allow_grace: bool,
    /// Workload label.
    pub workload: String,
    /// Entry type within the workload.
    pub entry_type: String,
    /// Schema version.
    pub schema_version: u16,
    /// Required capability bits.
    pub capability_bits: u32,
    /// Serialized payload (base64 encoded for JSON transport).
    pub payload: String,
    /// Optional trace context for distributed tracing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
}

impl ForwardPlaneMessage {
    /// Create from a forward request and envelope.
    pub fn from_request(req: &ForwardRequest, envelope: &WorkloadForwardEnvelope) -> Self {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;

        Self {
            source_prg: req.session_prg.clone(),
            target_prg: req.topic_prg.clone(),
            seq: req.seq,
            routing_epoch: req.routing_epoch.0,
            local_epoch: req.local_epoch.0,
            allow_grace: req.allow_grace,
            workload: envelope.workload.to_string(),
            entry_type: envelope.entry_type.to_string(),
            schema_version: envelope.schema_version,
            capability_bits: envelope.capability_bits,
            payload: STANDARD.encode(&envelope.payload),
            traceparent: None,
        }
    }

    /// Extract the forward request.
    pub fn to_request(&self) -> ForwardRequest {
        ForwardRequest {
            session_prg: self.source_prg.clone(),
            topic_prg: self.target_prg.clone(),
            seq: self.seq,
            routing_epoch: RoutingEpoch(self.routing_epoch),
            local_epoch: RoutingEpoch(self.local_epoch),
            allow_grace: self.allow_grace,
        }
    }

    /// Extract the envelope.
    pub fn to_envelope(&self) -> Result<WorkloadForwardEnvelope, ForwardEnvelopeError> {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine;

        let payload = STANDARD
            .decode(&self.payload)
            .map_err(|e| ForwardEnvelopeError::Decode(e.to_string()))?;

        Ok(WorkloadForwardEnvelope {
            workload: self.workload.clone().into(),
            entry_type: self.entry_type.clone().into(),
            schema_version: self.schema_version,
            capability_bits: self.capability_bits,
            payload,
        })
    }

    /// Set trace context.
    pub fn with_traceparent(mut self, traceparent: impl Into<String>) -> Self {
        self.traceparent = Some(traceparent.into());
        self
    }
}

/// Forward-plane RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForwardPlaneResponse {
    /// Whether this was a duplicate.
    pub duplicate: bool,
    /// ACK index from the target.
    pub ack_index: u64,
    /// Whether the entry was applied.
    pub applied: bool,
    /// Latency in milliseconds.
    #[serde(default)]
    pub latency_ms: u64,
    /// Optional durability proof (term, index).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof: Option<(u64, u64)>,
    /// Optional throttle hint for backpressure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throttle_hint_ms: Option<u64>,
}

impl ForwardPlaneResponse {
    /// Create a duplicate response.
    pub fn duplicate(ack_index: u64) -> Self {
        Self {
            duplicate: true,
            ack_index,
            applied: false,
            latency_ms: 0,
            proof: None,
            throttle_hint_ms: None,
        }
    }

    /// Create an applied response.
    pub fn applied(ack_index: u64, latency_ms: u64) -> Self {
        Self {
            duplicate: false,
            ack_index,
            applied: true,
            latency_ms,
            proof: None,
            throttle_hint_ms: None,
        }
    }

    /// Add proof to the response.
    pub fn with_proof(mut self, term: u64, index: u64) -> Self {
        self.proof = Some((term, index));
        self
    }

    /// Add throttle hint.
    pub fn with_throttle(mut self, hint_ms: u64) -> Self {
        self.throttle_hint_ms = Some(hint_ms);
        self
    }

    /// Convert to ForwardReceipt.
    pub fn to_receipt(&self) -> ForwardReceipt {
        ForwardReceipt {
            ack_index: self.ack_index,
            duplicate: self.duplicate,
            applied: self.applied,
            latency_ms: self.latency_ms,
        }
    }
}

/// Errors during envelope encoding/decoding.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ForwardEnvelopeError {
    #[error("failed to encode forward envelope: {0}")]
    Encode(String),
    #[error("failed to decode forward envelope: {0}")]
    Decode(String),
    #[error("unsupported workload: {0}")]
    UnsupportedWorkload(String),
    #[error("schema version mismatch: expected {expected}, got {actual}")]
    SchemaVersionMismatch { expected: u16, actual: u16 },
    #[error("missing required capability: {0}")]
    MissingCapability(String),
}

// -----------------------------------------------------------------------------
// Forward Intent and Coordinator
// -----------------------------------------------------------------------------

/// Forward intent created by workloads before dispatch.
///
/// Workloads create intents and hand them to the forward coordinator.
/// The coordinator handles routing validation, duplicate suppression,
/// and telemetry.
#[derive(Debug, Clone)]
pub struct ForwardIntent {
    /// Source PRG.
    pub source_prg: PrgId,
    /// Target PRG.
    pub target_prg: PrgId,
    /// Routing epoch.
    pub routing_epoch: RoutingEpoch,
    /// Workload envelope containing the payload.
    pub envelope: WorkloadForwardEnvelope,
    /// Optional tracing span for distributed tracing.
    pub trace_id: Option<String>,
}

impl ForwardIntent {
    /// Create a new forward intent.
    pub fn new(
        source_prg: PrgId,
        target_prg: PrgId,
        routing_epoch: RoutingEpoch,
        envelope: WorkloadForwardEnvelope,
    ) -> Self {
        Self {
            source_prg,
            target_prg,
            routing_epoch,
            envelope,
            trace_id: None,
        }
    }

    /// Set trace ID for distributed tracing.
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Check if this is a cross-PRG forward.
    pub fn is_cross_prg(&self) -> bool {
        self.source_prg != self.target_prg
    }

    /// Get the workload label.
    pub fn workload(&self) -> &str {
        self.envelope.workload()
    }
}

/// Result of a forward operation with proof metadata.
#[derive(Debug, Clone)]
pub struct ForwardResult {
    /// The receipt from the forward.
    pub receipt: ForwardReceipt,
    /// Durability proof from the target (term, index).
    pub target_proof: Option<(u64, u64)>,
    /// Whether backpressure was signaled.
    pub backpressure: bool,
}

impl ForwardResult {
    /// Create from a receipt.
    pub fn from_receipt(receipt: ForwardReceipt) -> Self {
        Self {
            receipt,
            target_proof: None,
            backpressure: false,
        }
    }

    /// Add target proof.
    pub fn with_proof(mut self, term: u64, index: u64) -> Self {
        self.target_proof = Some((term, index));
        self
    }

    /// Mark as backpressured.
    pub fn with_backpressure(mut self) -> Self {
        self.backpressure = true;
        self
    }
}

/// Callback for workloads when a forward is applied on the target.
pub trait ForwardAppliedCallback: Send + Sync {
    /// Called when the forward is applied on the target PRG.
    fn on_forward_applied(
        &self,
        source_prg: &PrgId,
        target_prg: &PrgId,
        seq: u64,
        target_proof: Option<(u64, u64)>,
    );
}

/// No-op callback for workloads that don't need notifications.
pub struct NoopForwardCallback;

impl ForwardAppliedCallback for NoopForwardCallback {
    fn on_forward_applied(
        &self,
        _source_prg: &PrgId,
        _target_prg: &PrgId,
        _seq: u64,
        _target_proof: Option<(u64, u64)>,
    ) {
    }
}

/// Forwarding request describing a session->topic hop.
#[derive(Debug, Clone)]
pub struct ForwardRequest {
    pub session_prg: PrgId,
    pub topic_prg: PrgId,
    pub seq: u64,
    pub routing_epoch: RoutingEpoch,
    pub local_epoch: RoutingEpoch,
    pub allow_grace: bool,
}

/// Result of forwarding a publish to the owning topic PRG.
#[derive(Debug, Clone, Copy)]
pub struct ForwardReceipt {
    pub ack_index: u64,
    pub duplicate: bool,
    pub applied: bool,
    pub latency_ms: u64,
}

/// Cross-PRG forwarding engine enforcing duplicate suppression and ordered commit.
#[derive(Clone)]
pub struct ForwardingEngine {
    ack: AckContract,
    metrics: Arc<BackpressureState>,
    last_seq: Arc<Mutex<HashMap<ForwardKey, u64>>>,
    grace_window: Duration,
}

impl ForwardingEngine {
    pub fn new(ack: AckContract, metrics: Arc<BackpressureState>, grace_window: Duration) -> Self {
        Self {
            ack,
            metrics,
            last_seq: Arc::new(Mutex::new(HashMap::new())),
            grace_window,
        }
    }

    fn check_epoch(&self, req: &ForwardRequest) -> Result<(), RoutingError> {
        if req.local_epoch == req.routing_epoch {
            return Ok(());
        }
        if req.allow_grace && req.local_epoch.0.saturating_sub(req.routing_epoch.0) <= 1 {
            return Ok(());
        }
        Err(RoutingError::StaleEpoch {
            local: req.local_epoch.0,
            expected: req.routing_epoch.0,
        })
    }

    fn mark_seq(&self, req: &ForwardRequest) -> bool {
        let key = (
            req.session_prg.clone(),
            req.topic_prg.clone(),
            req.routing_epoch.0,
        );
        let mut guard = self.last_seq.lock().unwrap();
        let last = guard.get(&key).cloned().unwrap_or(0);
        if req.seq <= last {
            return true;
        }
        guard.insert(key, req.seq);
        false
    }

    pub fn prepare_forward(&self, req: &ForwardRequest) -> Result<bool, RoutingError> {
        self.check_epoch(req)?;
        Ok(self.mark_seq(req))
    }

    /// Forward to the topic PRG, run `apply` after de-duplication, and only ACK after commit.
    pub async fn forward_and_commit<Fut>(
        &self,
        req: ForwardRequest,
        apply: impl FnOnce() -> Fut,
    ) -> Result<ForwardReceipt, RoutingError>
    where
        Fut: std::future::Future<Output = ()> + Send,
    {
        let duplicate = self.prepare_forward(&req)?;
        if duplicate {
            return Ok(ForwardReceipt {
                ack_index: self.ack.clustor_floor(),
                duplicate: true,
                applied: false,
                latency_ms: 0,
            });
        }

        let started = Instant::now();
        apply().await;
        let elapsed = started.elapsed();
        let waited = elapsed.as_secs();
        if req.session_prg != req.topic_prg {
            self.metrics
                .record_forward_latency(elapsed.as_millis() as u64);
        }
        if waited > 0 {
            self.metrics.add_forward_backpressure(waited);
        }
        let ack_index = self.ack.clustor_floor();
        Ok(ForwardReceipt {
            ack_index,
            duplicate: false,
            applied: true,
            latency_ms: elapsed.as_millis() as u64,
        })
    }

    /// Allow tests to reset state between routing epochs.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.last_seq.lock() {
            guard.clear();
        }
    }

    /// Clear forward sequence tracking for a specific session PRG.
    ///
    /// This should be called when a session is reset (clean_start=true) to allow
    /// new forward sequences starting from 1 to be accepted.
    pub fn clear_session(&self, session_prg: &PrgId) {
        if let Ok(mut guard) = self.last_seq.lock() {
            guard.retain(|(s, _, _), _| s != session_prg);
        }
    }

    pub fn grace_window(&self) -> Duration {
        self.grace_window
    }
}

// -----------------------------------------------------------------------------
// Forward Coordinator
// -----------------------------------------------------------------------------

/// Workload handler for processing forwards on the target PRG.
pub trait WorkloadForwardHandler: Send + Sync {
    /// Workload label this handler processes.
    fn workload(&self) -> &'static str;

    /// Check if the handler supports the given entry type.
    fn supports_entry(&self, entry_type: &str) -> bool;

    /// Apply the forwarded entry. Returns proof metadata.
    fn apply(
        &self,
        envelope: &WorkloadForwardEnvelope,
    ) -> Result<Option<(u64, u64)>, ForwardEnvelopeError>;
}

/// Registry of workload handlers for forward dispatch.
#[derive(Default)]
pub struct WorkloadForwardRegistry {
    handlers: HashMap<&'static str, Arc<dyn WorkloadForwardHandler>>,
}

impl WorkloadForwardRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a workload handler.
    pub fn register(&mut self, handler: Arc<dyn WorkloadForwardHandler>) {
        self.handlers.insert(handler.workload(), handler);
    }

    /// Get handler for a workload.
    pub fn get(&self, workload: &str) -> Option<&Arc<dyn WorkloadForwardHandler>> {
        self.handlers.get(workload)
    }

    /// Check if a workload is registered.
    pub fn contains(&self, workload: &str) -> bool {
        self.handlers.contains_key(workload)
    }

    /// List registered workloads.
    pub fn workloads(&self) -> Vec<&'static str> {
        self.handlers.keys().copied().collect()
    }
}

/// Forward coordinator that orchestrates cross-PRG forwards.
///
/// The coordinator:
/// - Validates routing epoch
/// - Enforces duplicate suppression via the forwarding engine
/// - Dispatches to registered workload handlers
/// - Tracks telemetry (latency, backpressure, cross-workload forwards)
/// - Applies policy hints from workloads
#[derive(Clone)]
pub struct ForwardCoordinator {
    engine: ForwardingEngine,
    registry: Arc<WorkloadForwardRegistry>,
    metrics: Arc<BackpressureState>,
}

impl ForwardCoordinator {
    /// Create a new forward coordinator.
    pub fn new(
        engine: ForwardingEngine,
        registry: Arc<WorkloadForwardRegistry>,
        metrics: Arc<BackpressureState>,
    ) -> Self {
        Self {
            engine,
            registry,
            metrics,
        }
    }

    /// Get the underlying forwarding engine.
    pub fn engine(&self) -> &ForwardingEngine {
        &self.engine
    }

    /// Check if a workload is registered.
    pub fn supports_workload(&self, workload: &str) -> bool {
        self.registry.contains(workload)
    }

    /// Dispatch a forward intent to the appropriate handler.
    pub async fn dispatch<F, Fut>(
        &self,
        intent: ForwardIntent,
        seq: u64,
        local_epoch: RoutingEpoch,
        apply: F,
    ) -> Result<ForwardResult, ForwardCoordinatorError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()> + Send,
    {
        // Check workload is registered
        if !self.supports_workload(intent.workload()) {
            return Err(ForwardCoordinatorError::UnsupportedWorkload(
                intent.workload().to_string(),
            ));
        }

        // Build forward request
        let request = ForwardRequest {
            session_prg: intent.source_prg.clone(),
            topic_prg: intent.target_prg.clone(),
            seq,
            routing_epoch: intent.routing_epoch,
            local_epoch,
            allow_grace: true,
        };

        // Forward through the engine
        let receipt = self
            .engine
            .forward_and_commit(request.clone(), apply)
            .await
            .map_err(ForwardCoordinatorError::Routing)?;

        // Record cross-workload metrics if applicable
        if intent.is_cross_prg() {
            self.metrics.record_forward_latency(receipt.latency_ms);
        }

        let mut result = ForwardResult::from_receipt(receipt);
        if receipt.latency_ms > 1000 {
            result = result.with_backpressure();
        }

        Ok(result)
    }

    /// Process an incoming forward-plane message from RPC.
    pub fn process_message(
        &self,
        message: &ForwardPlaneMessage,
    ) -> Result<(), ForwardCoordinatorError> {
        // Extract envelope
        let envelope = message
            .to_envelope()
            .map_err(ForwardCoordinatorError::Envelope)?;

        // Get handler
        let handler = self.registry.get(&envelope.workload).ok_or_else(|| {
            ForwardCoordinatorError::UnsupportedWorkload(envelope.workload.to_string())
        })?;

        // Check entry type is supported
        if !handler.supports_entry(&envelope.entry_type) {
            return Err(ForwardCoordinatorError::UnsupportedEntry {
                workload: envelope.workload.to_string(),
                entry: envelope.entry_type.to_string(),
            });
        }

        // Apply via handler
        handler
            .apply(&envelope)
            .map_err(ForwardCoordinatorError::Envelope)?;

        Ok(())
    }

    /// Get registered workloads.
    pub fn registered_workloads(&self) -> Vec<&'static str> {
        self.registry.workloads()
    }
}

/// Errors from the forward coordinator.
#[derive(Debug, thiserror::Error)]
pub enum ForwardCoordinatorError {
    #[error("unsupported workload: {0}")]
    UnsupportedWorkload(String),
    #[error("unsupported entry type {entry} for workload {workload}")]
    UnsupportedEntry { workload: String, entry: String },
    #[error("routing error: {0}")]
    Routing(#[from] RoutingError),
    #[error("envelope error: {0}")]
    Envelope(#[from] ForwardEnvelopeError),
    #[error("capability not enabled: {0}")]
    CapabilityDisabled(String),
}

// -----------------------------------------------------------------------------
// Forward Metrics
// -----------------------------------------------------------------------------

/// Forward telemetry labels for workload-aware metrics.
#[derive(Debug, Clone, Default)]
pub struct ForwardMetricLabels {
    /// Source workload label.
    pub source_workload: Option<String>,
    /// Target workload label.
    pub target_workload: Option<String>,
    /// Whether this is a cross-PRG forward.
    pub cross_prg: bool,
    /// Whether this is a cross-workload forward.
    pub cross_workload: bool,
}

impl ForwardMetricLabels {
    /// Create labels for an intent.
    pub fn from_intent(intent: &ForwardIntent, target_workload: Option<&str>) -> Self {
        let source = intent.workload().to_string();
        let target = target_workload.map(String::from);
        let cross_workload = target.as_deref() != Some(intent.workload());

        Self {
            source_workload: Some(source),
            target_workload: target,
            cross_prg: intent.is_cross_prg(),
            cross_workload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_forward_envelope_roundtrip() {
        let envelope = WorkloadForwardEnvelope::new(
            "mqtt",
            "commit_publish",
            1,
            CapabilityFlags::OFFLINE_QUEUES,
            b"test payload".to_vec(),
        );

        let encoded = envelope.encode().unwrap();
        let decoded = WorkloadForwardEnvelope::decode(&encoded).unwrap();

        assert_eq!(decoded.workload(), "mqtt");
        assert_eq!(decoded.entry_type(), "commit_publish");
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded.payload, b"test payload");
    }

    #[test]
    fn test_forward_plane_message_roundtrip() {
        let request = ForwardRequest {
            session_prg: PrgId {
                tenant_id: "tenant1".to_string(),
                partition_index: 0,
            },
            topic_prg: PrgId {
                tenant_id: "tenant1".to_string(),
                partition_index: 1,
            },
            seq: 42,
            routing_epoch: RoutingEpoch(5),
            local_epoch: RoutingEpoch(5),
            allow_grace: true,
        };

        let envelope = WorkloadForwardEnvelope::new(
            "mqtt",
            "commit_publish",
            1,
            CapabilityFlags::empty(),
            b"payload".to_vec(),
        );

        let message = ForwardPlaneMessage::from_request(&request, &envelope);
        assert_eq!(message.workload, "mqtt");
        assert_eq!(message.seq, 42);

        let extracted_req = message.to_request();
        assert_eq!(extracted_req.seq, 42);
        assert_eq!(extracted_req.routing_epoch.0, 5);

        let extracted_env = message.to_envelope().unwrap();
        assert_eq!(extracted_env.workload(), "mqtt");
        assert_eq!(extracted_env.payload, b"payload");
    }

    #[test]
    fn test_forward_intent_cross_prg() {
        let same_prg = ForwardIntent::new(
            PrgId {
                tenant_id: "t".to_string(),
                partition_index: 0,
            },
            PrgId {
                tenant_id: "t".to_string(),
                partition_index: 0,
            },
            RoutingEpoch(1),
            WorkloadForwardEnvelope::new("mqtt", "test", 1, CapabilityFlags::empty(), vec![]),
        );
        assert!(!same_prg.is_cross_prg());

        let diff_prg = ForwardIntent::new(
            PrgId {
                tenant_id: "t".to_string(),
                partition_index: 0,
            },
            PrgId {
                tenant_id: "t".to_string(),
                partition_index: 1,
            },
            RoutingEpoch(1),
            WorkloadForwardEnvelope::new("mqtt", "test", 1, CapabilityFlags::empty(), vec![]),
        );
        assert!(diff_prg.is_cross_prg());
    }

    #[test]
    fn test_forward_plane_response() {
        let dup = ForwardPlaneResponse::duplicate(100);
        assert!(dup.duplicate);
        assert!(!dup.applied);

        let applied = ForwardPlaneResponse::applied(200, 50).with_proof(1, 100);
        assert!(!applied.duplicate);
        assert!(applied.applied);
        assert_eq!(applied.proof, Some((1, 100)));

        let receipt = applied.to_receipt();
        assert_eq!(receipt.ack_index, 200);
        assert_eq!(receipt.latency_ms, 50);
    }

    #[test]
    fn test_workload_forward_registry() {
        struct TestHandler;
        impl WorkloadForwardHandler for TestHandler {
            fn workload(&self) -> &'static str {
                "test"
            }
            fn supports_entry(&self, entry: &str) -> bool {
                entry == "test_entry"
            }
            fn apply(
                &self,
                _envelope: &WorkloadForwardEnvelope,
            ) -> Result<Option<(u64, u64)>, ForwardEnvelopeError> {
                Ok(Some((1, 100)))
            }
        }

        let mut registry = WorkloadForwardRegistry::new();
        registry.register(Arc::new(TestHandler));

        assert!(registry.contains("test"));
        assert!(!registry.contains("other"));
        assert_eq!(registry.workloads(), vec!["test"]);
    }
}
