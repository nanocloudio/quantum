use std::time::{Duration, Instant};

/// Clock abstraction to enforce deterministic time sourcing in core paths.
pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> Instant;
    fn sleep(&self, duration: Duration) -> tokio::time::Sleep;
}

/// System-backed clock; replaceable in tests or deterministic replay.
#[derive(Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) -> tokio::time::Sleep {
        tokio::time::sleep(duration)
    }
}
