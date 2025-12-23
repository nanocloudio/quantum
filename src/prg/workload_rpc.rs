//! Remote workload RPC scaffolding.
//!
//! Implements a lightweight HTTP client for the forward-plane RPC service so
//! cross-PRG forwards can complete even when the target workload is remote.
//!
//! The RPC layer is workload-agnostic: it sends `ForwardPlaneMessage` envelopes
//! that carry workload labels, schema versions, and capability bits. The
//! receiving host dispatches to the appropriate workload handler.

use crate::config::ForwardPlaneConfig;
use crate::forwarding::{
    ForwardPlaneMessage, ForwardPlaneResponse, ForwardRequest, WorkloadForwardEnvelope,
};
use crate::routing::PrgId;
use crate::workloads::mqtt::MqttApply;
use std::borrow::Cow;
use std::time::Duration;

/// Minimal RPC client facade that speaks to the forward-plane service.
#[derive(Clone)]
pub struct RemoteWorkloadClient {
    enabled: bool,
    scheme: String,
    port: u16,
    path: String,
    http: reqwest::Client,
}

impl RemoteWorkloadClient {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            scheme: "http".into(),
            port: 0,
            path: "/".into(),
            http: reqwest::Client::new(),
        }
    }

    pub fn from_config(cfg: &ForwardPlaneConfig) -> Self {
        if !cfg.enabled {
            return Self::disabled();
        }
        let timeout = Duration::from_millis(cfg.timeout_ms);
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|err| {
                tracing::warn!("failed to build remote workload client: {err:?}");
                reqwest::Client::new()
            });
        Self {
            enabled: true,
            scheme: cfg.scheme.clone(),
            port: cfg.port,
            path: cfg.path.clone(),
            http,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Forward a workload envelope to a remote host.
    ///
    /// This is the generic forwarding method that works with any workload type.
    /// It sends a `ForwardPlaneMessage` envelope to the target host.
    pub async fn forward_envelope(
        &self,
        host: &str,
        request: &ForwardRequest,
        envelope: &WorkloadForwardEnvelope,
    ) -> Result<RemoteForwardResponse, RemoteWorkloadError> {
        if !self.enabled {
            return Err(RemoteWorkloadError::Disabled);
        }
        let url = self.build_url(host);
        let message = ForwardPlaneMessage::from_request(request, envelope);

        let response = self
            .http
            .post(&url)
            .json(&message)
            .send()
            .await
            .map_err(|err| {
                RemoteWorkloadError::unavailable(
                    request.topic_prg.clone(),
                    envelope.entry_type.to_string(),
                    format!("forward RPC send failed: {err}"),
                )
            })?;

        if !response.status().is_success() {
            return Err(RemoteWorkloadError::unavailable(
                request.topic_prg.clone(),
                envelope.entry_type.to_string(),
                format!("forward RPC status {}", response.status()),
            ));
        }

        let parsed: ForwardPlaneResponse = response.json().await.map_err(|err| {
            RemoteWorkloadError::unavailable(
                request.topic_prg.clone(),
                envelope.entry_type.to_string(),
                format!("forward RPC decode failed: {err}"),
            )
        })?;

        Ok(RemoteForwardResponse {
            ack_index: parsed.ack_index,
            applied: parsed.applied,
            latency_ms: parsed.latency_ms,
            proof: parsed.proof,
            throttle_hint_ms: parsed.throttle_hint_ms,
        })
    }

    /// Forward an MQTT entry to a remote host (legacy compatibility).
    ///
    /// This method wraps `forward_envelope` with MQTT-specific serialization.
    pub async fn forward_mqtt_entry(
        &self,
        host: &str,
        prg: &PrgId,
        workload: &'static str,
        request: &ForwardRequest,
        entry: &MqttApply,
    ) -> Result<RemoteForwardResponse, RemoteWorkloadError> {
        if !self.enabled {
            return Err(RemoteWorkloadError::Disabled);
        }

        // Serialize the MQTT entry into an envelope
        let payload = match bincode::serialize(entry) {
            Ok(bytes) => bytes,
            Err(err) => {
                return Err(RemoteWorkloadError::unavailable(
                    prg.clone(),
                    entry.label().to_string(),
                    format!("serialize forward payload failed: {err:?}"),
                ))
            }
        };

        let envelope = WorkloadForwardEnvelope::new(
            workload,
            entry.label(),
            1, // schema version
            crate::prg::workload::CapabilityFlags::empty(),
            payload,
        );

        self.forward_envelope(host, request, &envelope).await
    }

    fn build_url(&self, host: &str) -> String {
        let trimmed_path = self.path.trim_start_matches('/');
        if host.starts_with("http://") || host.starts_with("https://") {
            format!("{}/{}", host.trim_end_matches('/'), trimmed_path)
        } else if self.port == 0 {
            format!("{}://{}/{}", self.scheme, host, trimmed_path)
        } else {
            format!("{}://{}:{}/{}", self.scheme, host, self.port, trimmed_path)
        }
    }
}

/// Remote forward response with proof and throttle metadata.
#[derive(Debug, Clone, Default)]
pub struct RemoteForwardResponse {
    /// ACK index from the target.
    pub ack_index: u64,
    /// Whether the entry was applied.
    pub applied: bool,
    /// Latency in milliseconds.
    pub latency_ms: u64,
    /// Optional durability proof (term, index).
    pub proof: Option<(u64, u64)>,
    /// Optional throttle hint for backpressure.
    pub throttle_hint_ms: Option<u64>,
}

impl RemoteForwardResponse {
    /// Convert to a forward receipt.
    pub fn to_receipt(&self) -> crate::forwarding::ForwardReceipt {
        crate::forwarding::ForwardReceipt {
            ack_index: self.ack_index,
            duplicate: false,
            applied: self.applied,
            latency_ms: self.latency_ms,
        }
    }

    /// Check if backpressure was signaled.
    pub fn has_throttle(&self) -> bool {
        self.throttle_hint_ms.is_some()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteWorkloadError {
    #[error("remote workload RPC disabled")]
    Disabled,
    #[error("remote workload RPC unavailable for {prg:?} entry={entry}: {message}")]
    Unavailable {
        prg: PrgId,
        entry: Cow<'static, str>,
        message: Cow<'static, str>,
    },
    #[error("unsupported workload: {0}")]
    UnsupportedWorkload(String),
    #[error("capability not enabled: {0}")]
    CapabilityDisabled(String),
}

impl RemoteWorkloadError {
    /// Create an unavailable error for a given entry.
    pub fn unavailable(
        prg: PrgId,
        entry: impl Into<Cow<'static, str>>,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self::Unavailable {
            prg,
            entry: entry.into(),
            message: message.into(),
        }
    }
}
