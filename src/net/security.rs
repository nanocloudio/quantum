use crate::config::QuotaLimits;
use crate::control::ControlPlaneClient;
use crate::time::Clock;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error)]
pub enum SecurityError {
    #[error("control plane cache stale")]
    StaleControlPlane,
    #[error("tenant identity mismatch")]
    TenantMismatch,
    #[error("quota exceeded for tenant")]
    QuotaExceeded,
    #[error("unauthorized operation")]
    Unauthorized,
    #[error("kms key in use by another tenant")]
    KmsKeyConflict,
    #[error("token missing or invalid")]
    InvalidToken,
    #[error("mTLS binding mismatch")]
    MtlsBindingMismatch,
}

/// AuthN inputs passed from listeners.
#[derive(Debug, Clone)]
pub struct AuthContext<'a> {
    pub tenant_id: &'a str,
    pub sni: Option<&'a str>,
    pub presented_spiffe: Option<&'a str>,
    pub bearer_token: Option<&'a str>,
    pub topic: Option<&'a str>,
}

/// Tracks per-tenant quotas and enforces session/publish limits.
#[derive(Clone)]
pub struct QuotaEnforcer {
    limits: Option<QuotaLimits>,
    usage: Arc<Mutex<HashMap<String, TenantUsage>>>,
    tenant_limits: Arc<Mutex<HashMap<String, QuotaLimits>>>,
}

#[derive(Default, Debug)]
struct TenantUsage {
    sessions: u64,
    publishes: u64,
    publish_window_start: Option<Instant>,
    retained_bytes: u64,
    dedupe_entries: u64,
    offline_entries: u64,
    offline_bytes: u64,
}

impl QuotaEnforcer {
    pub fn new(limits: Option<QuotaLimits>) -> Self {
        Self {
            limits,
            usage: Arc::new(Mutex::new(HashMap::new())),
            tenant_limits: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn limits(&self) -> Option<QuotaLimits> {
        self.limits.clone()
    }

    pub fn check_session_admission(&self, tenant_id: &str) -> Result<(), SecurityError> {
        if let Some(lim) = self.limits_for(tenant_id) {
            if let Some(max_sessions) = lim.max_sessions_per_tenant {
                let mut usage = self.usage.lock().unwrap();
                let entry = usage.entry(tenant_id.to_string()).or_default();
                if entry.sessions >= max_sessions {
                    return Err(SecurityError::QuotaExceeded);
                }
                entry.sessions += 1;
            }
        }
        Ok(())
    }

    pub fn release_session(&self, tenant_id: &str) {
        if let Ok(mut usage) = self.usage.lock() {
            if let Some(entry) = usage.get_mut(tenant_id) {
                entry.sessions = entry.sessions.saturating_sub(1);
            }
        }
    }

    pub fn record_publish(&self, tenant_id: &str) -> Result<(), SecurityError> {
        if let Some(lim) = self.limits_for(tenant_id) {
            if let Some(max_pub) = lim.max_publishes_per_second {
                let mut usage = self.usage.lock().unwrap();
                let entry = usage.entry(tenant_id.to_string()).or_default();
                let now = Instant::now();
                if let Some(window_start) = entry.publish_window_start {
                    if now.duration_since(window_start).as_secs() >= 1 {
                        entry.publishes = 0;
                        entry.publish_window_start = Some(now);
                    }
                } else {
                    entry.publish_window_start = Some(now);
                }
                if entry.publishes >= max_pub {
                    return Err(SecurityError::QuotaExceeded);
                }
                entry.publishes += 1;
            }
        }
        Ok(())
    }

    pub fn record_retained_bytes(&self, tenant_id: &str, delta: i64) -> Result<(), SecurityError> {
        if let Some(lim) = self.limits_for(tenant_id) {
            if let Some(max_bytes) = lim.max_retained_bytes {
                let mut usage = self.usage.lock().unwrap();
                let entry = usage.entry(tenant_id.to_string()).or_default();
                if delta.is_negative() {
                    let abs = delta.checked_abs().unwrap_or(i64::MAX) as u64;
                    entry.retained_bytes = entry.retained_bytes.saturating_sub(abs);
                } else {
                    entry.retained_bytes = entry.retained_bytes.saturating_add(delta as u64);
                }
                if entry.retained_bytes > max_bytes {
                    return Err(SecurityError::QuotaExceeded);
                }
            }
        }
        Ok(())
    }

    pub fn record_dedupe_entries(&self, tenant_id: &str, delta: i64) -> Result<(), SecurityError> {
        if let Some(lim) = self.limits_for(tenant_id) {
            if let Some(max_entries) = lim.max_dedupe_entries {
                let mut usage = self.usage.lock().unwrap();
                let entry = usage.entry(tenant_id.to_string()).or_default();
                if delta.is_negative() {
                    let abs = delta.checked_abs().unwrap_or(i64::MAX) as u64;
                    entry.dedupe_entries = entry.dedupe_entries.saturating_sub(abs);
                } else {
                    entry.dedupe_entries = entry.dedupe_entries.saturating_add(delta as u64);
                }
                if entry.dedupe_entries > max_entries {
                    return Err(SecurityError::QuotaExceeded);
                }
            }
        }
        Ok(())
    }

    pub fn record_offline_enqueue(
        &self,
        tenant_id: &str,
        entries_delta: i64,
        bytes_delta: i64,
    ) -> Result<(), SecurityError> {
        if let Some(lim) = self.limits_for(tenant_id) {
            let mut usage = self.usage.lock().unwrap();
            let entry = usage.entry(tenant_id.to_string()).or_default();
            if entries_delta.is_negative() {
                let abs = entries_delta.checked_abs().unwrap_or(i64::MAX) as u64;
                entry.offline_entries = entry.offline_entries.saturating_sub(abs);
            } else {
                entry.offline_entries = entry.offline_entries.saturating_add(entries_delta as u64);
            }
            if bytes_delta.is_negative() {
                let abs = bytes_delta.checked_abs().unwrap_or(i64::MAX) as u64;
                entry.offline_bytes = entry.offline_bytes.saturating_sub(abs);
            } else {
                entry.offline_bytes = entry.offline_bytes.saturating_add(bytes_delta as u64);
            }
            if let Some(max_entries) = lim.max_offline_entries {
                if entry.offline_entries > max_entries {
                    return Err(SecurityError::QuotaExceeded);
                }
            }
            if let Some(max_bytes) = lim.max_offline_bytes {
                if entry.offline_bytes > max_bytes {
                    return Err(SecurityError::QuotaExceeded);
                }
            }
        }
        Ok(())
    }

    pub fn set_tenant_limit(&self, tenant_id: &str, limits: QuotaLimits) {
        if let Ok(mut guard) = self.tenant_limits.lock() {
            guard.insert(tenant_id.to_string(), limits);
        }
    }

    fn limits_for(&self, tenant_id: &str) -> Option<QuotaLimits> {
        if let Ok(guard) = self.tenant_limits.lock() {
            if let Some(lim) = guard.get(tenant_id) {
                return Some(lim.clone());
            }
        }
        self.limits.clone()
    }
}

/// Enforces hard single-tenant PRG identity and ACL freshness gating.
#[derive(Clone)]
pub struct SecurityManager<C: Clock + Clone> {
    control_plane: ControlPlaneClient<C>,
    quotas: QuotaEnforcer,
    kms_registry: Arc<Mutex<HashMap<String, String>>>,
    kms_keys_in_use: Arc<Mutex<HashSet<String>>>,
}

impl<C: Clock + Clone> SecurityManager<C> {
    pub fn new(control_plane: ControlPlaneClient<C>, quotas: QuotaLimits) -> Self {
        Self {
            control_plane,
            quotas: QuotaEnforcer::new(Some(quotas)),
            kms_registry: Arc::new(Mutex::new(HashMap::new())),
            kms_keys_in_use: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn ensure_fresh(&self) -> Result<(), SecurityError> {
        match self.control_plane.require_fresh_read() {
            Ok(_) => Ok(()),
            Err(err) if self.control_plane.is_embedded() => {
                let snapshot = self.control_plane.snapshot();
                if !snapshot.placements.is_empty() {
                    warn!(
                        "allowing stale control-plane cache for embedded mode err={err:?} epoch={} placements={} strict_fallback={}",
                        snapshot.routing_epoch.0,
                        snapshot.placements.len(),
                        snapshot.strict_fallback
                    );
                    self.control_plane.set_strict_fallback(false);
                    self.control_plane.mark_refreshed();
                    Ok(())
                } else {
                    Err(SecurityError::StaleControlPlane)
                }
            }
            Err(_) => Err(SecurityError::StaleControlPlane),
        }
    }

    pub fn enforce_single_tenant(
        &self,
        prg_tenant_id: &str,
        op_tenant_id: &str,
    ) -> Result<(), SecurityError> {
        if prg_tenant_id != op_tenant_id {
            return Err(SecurityError::TenantMismatch);
        }
        Ok(())
    }

    pub fn authorize(&self) -> Result<(), SecurityError> {
        self.ensure_fresh().map(|_| {
            let snapshot = self.control_plane.snapshot();
            for (tenant, quotas) in snapshot.tenant_quotas {
                self.quotas.set_tenant_limit(&tenant, quotas);
            }
        })
    }

    pub fn authorize_connect(&self, ctx: AuthContext<'_>) -> Result<(), SecurityError> {
        self.authorize()?;
        self.enforce_sni_binding(ctx.tenant_id, ctx.sni)?;
        let snapshot = self.control_plane.snapshot();
        if let Some(certs) = snapshot.tenant_certs.get(ctx.tenant_id) {
            let presented = ctx
                .presented_spiffe
                .ok_or(SecurityError::MtlsBindingMismatch)?;
            if !certs.iter().any(|c| c == presented) {
                return Err(SecurityError::MtlsBindingMismatch);
            }
        }
        if let Some(tokens) = snapshot.tenant_tokens.get(ctx.tenant_id) {
            let token = ctx
                .bearer_token
                .ok_or(SecurityError::InvalidToken)?
                .to_string();
            if !tokens.contains(&token) {
                return Err(SecurityError::InvalidToken);
            }
        }
        if let Some(topic) = ctx.topic {
            self.authorize_acl(ctx.tenant_id, topic)?;
        }
        Ok(())
    }

    pub fn enforce_sni_binding(
        &self,
        tenant_id: &str,
        sni: Option<&str>,
    ) -> Result<(), SecurityError> {
        self.ensure_fresh()?;
        if let Some(sni_val) = sni {
            if sni_val != tenant_id {
                return Err(SecurityError::TenantMismatch);
            }
        }
        Ok(())
    }

    pub fn authorize_acl(&self, tenant_id: &str, topic: &str) -> Result<(), SecurityError> {
        self.ensure_fresh()?;
        let snapshot = self.control_plane.snapshot();
        if let Some(prefixes) = snapshot.tenant_acl_prefixes.get(tenant_id) {
            if !prefixes.iter().any(|p| topic.starts_with(p)) {
                return Err(SecurityError::Unauthorized);
            }
        } else if self.control_plane.is_embedded() {
            // In embedded/dev mode there is no external control-plane to seed ACL prefixes,
            // so allow all topics for the tenant unless explicit prefixes were installed.
            // Multi-tenant deployments should configure ACLs via the control-plane.
            return Ok(());
        } else if !topic.starts_with(&format!("tenant/{}/", tenant_id)) {
            return Err(SecurityError::Unauthorized);
        }
        Ok(())
    }

    pub fn quotas(&self) -> &QuotaEnforcer {
        &self.quotas
    }

    pub fn admit_session(&self, tenant_id: &str) -> Result<(), SecurityError> {
        self.ensure_fresh()?;
        self.quotas.check_session_admission(tenant_id)
    }

    pub fn release_session(&self, tenant_id: &str) {
        self.quotas.release_session(tenant_id);
    }

    pub fn record_publish(&self, tenant_id: &str) -> Result<(), SecurityError> {
        self.ensure_fresh()?;
        self.quotas.record_publish(tenant_id)
    }

    pub fn record_offline_usage(
        &self,
        tenant_id: &str,
        entries_delta: i64,
        bytes_delta: i64,
    ) -> Result<(), SecurityError> {
        self.ensure_fresh()?;
        self.quotas
            .record_offline_enqueue(tenant_id, entries_delta, bytes_delta)
    }

    /// Ensure KMS key is unique per tenant.
    pub fn register_kms_key(&self, tenant_id: &str, kms_key_id: &str) -> Result<(), SecurityError> {
        self.ensure_fresh()?;
        let mut registry = self.kms_registry.lock().unwrap();
        let mut in_use = self.kms_keys_in_use.lock().unwrap();
        if let Some(existing) = registry.get(tenant_id) {
            if existing == kms_key_id {
                return Ok(());
            }
        }
        if in_use.contains(kms_key_id) {
            return Err(SecurityError::KmsKeyConflict);
        }
        registry.insert(tenant_id.to_string(), kms_key_id.to_string());
        in_use.insert(kms_key_id.to_string());
        Ok(())
    }

    /// Mid-session re-auth hook; real implementation will validate tokens/certs via CP.
    pub fn refresh_credentials(
        &self,
        _tenant_id: &str,
        _client_id: &str,
    ) -> Result<(), SecurityError> {
        self.ensure_fresh()
    }
}
