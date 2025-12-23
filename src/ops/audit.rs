use std::sync::{Arc, Mutex, OnceLock};
use tracing::event;

#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub event_type: String,
    pub tenant_id: String,
    pub client_id: String,
    pub message: String,
}

#[derive(Clone, Default)]
pub struct AuditSink {
    inner: Arc<Mutex<Vec<AuditEvent>>>,
}

impl AuditSink {
    pub fn record(&self, event: AuditEvent) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.push(event);
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

static AUDIT_SINK: OnceLock<AuditSink> = OnceLock::new();
static AUDIT_DROPS: OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();

pub fn install_sink(sink: AuditSink) {
    let _ = AUDIT_SINK.set(sink);
    let _ = AUDIT_DROPS.set(std::sync::atomic::AtomicU64::new(0));
}

pub trait AuditPublisher: Send + Sync {
    fn publish(&self, event: &AuditEvent);
}

static AUDIT_PUBLISHER: OnceLock<Mutex<Option<Arc<dyn AuditPublisher + Send + Sync>>>> =
    OnceLock::new();

pub fn install_publisher(publisher: Arc<dyn AuditPublisher + Send + Sync>) {
    let cell = AUDIT_PUBLISHER.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = cell.lock() {
        *guard = Some(publisher);
    }
}

pub fn sink_len() -> usize {
    AUDIT_SINK.get().map(|s| s.len()).unwrap_or(0)
}

/// Return a copy of audit events for mirroring to external sinks.
pub fn snapshot() -> Vec<AuditEvent> {
    AUDIT_SINK
        .get()
        .and_then(|s| s.inner.lock().ok().map(|g| g.clone()))
        .unwrap_or_default()
}

pub fn dropped() -> u64 {
    AUDIT_DROPS
        .get()
        .map(|d| d.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(0)
}

/// Emit an immutable audit event; will fan out to an installed publisher when present.
pub fn emit(event_type: &str, tenant_id: &str, client_id: &str, message: &str) {
    event!(
        target: "audit",
        tracing::Level::INFO,
        %event_type,
        %tenant_id,
        %client_id,
        %message
    );
    let sink = AUDIT_SINK.get_or_init(AuditSink::default);
    let event = AuditEvent {
        event_type: event_type.to_string(),
        tenant_id: tenant_id.to_string(),
        client_id: client_id.to_string(),
        message: message.to_string(),
    };
    sink.record(event.clone());
    let mut published = false;
    if let Some(lock) = AUDIT_PUBLISHER.get() {
        if let Ok(guard) = lock.lock() {
            if let Some(publisher) = guard.as_ref() {
                publisher.publish(&event);
                published = true;
            }
        }
    }
    if !published {
        if let Some(drop_ctr) = AUDIT_DROPS.get() {
            drop_ctr.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}
