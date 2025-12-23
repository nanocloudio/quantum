use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

/// Version guard to enforce ≤1 minor skew.
#[derive(Clone, Copy)]
pub struct VersionGate {
    current_minor: u8,
}

impl VersionGate {
    pub const fn new(current_minor: u8) -> Self {
        Self { current_minor }
    }

    pub fn compatible(&self, peer_minor: u8) -> bool {
        let diff = self.current_minor.max(peer_minor) - self.current_minor.min(peer_minor);
        diff <= 1
    }

    pub fn current_minor(&self) -> u8 {
        self.current_minor
    }
}

/// Fault/chaos injection toggles for ops validation.
#[derive(Default, Debug)]
struct FaultInjectionState {
    election_delay_enabled: AtomicBool,
    fsync_slow_enabled: AtomicBool,
    kms_outage_enabled: AtomicBool,
    latency_ms: AtomicU8,
}

/// Fault/chaos injection toggles for ops validation.
#[derive(Clone, Default, Debug)]
pub struct FaultInjector {
    state: Arc<FaultInjectionState>,
}

impl FaultInjector {
    pub fn enable_election_delay(&self, enabled: bool) {
        self.state
            .election_delay_enabled
            .store(enabled, Ordering::Relaxed);
    }

    pub fn enable_fsync_slow(&self, enabled: bool) {
        self.state
            .fsync_slow_enabled
            .store(enabled, Ordering::Relaxed);
    }

    pub fn enable_kms_outage(&self, enabled: bool) {
        self.state
            .kms_outage_enabled
            .store(enabled, Ordering::Relaxed);
    }

    pub fn set_latency_ms(&self, ms: u8) {
        self.state.latency_ms.store(ms, Ordering::Relaxed);
    }

    pub fn election_delay(&self) -> bool {
        self.state.election_delay_enabled.load(Ordering::Relaxed)
    }

    pub fn fsync_slow(&self) -> bool {
        self.state.fsync_slow_enabled.load(Ordering::Relaxed)
    }

    pub fn kms_outage(&self) -> bool {
        self.state.kms_outage_enabled.load(Ordering::Relaxed)
    }

    pub fn latency_ms(&self) -> u8 {
        self.state.latency_ms.load(Ordering::Relaxed)
    }
}
