use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct ProxyStats {
    pub frames_client_to_upstream: AtomicU64,
    pub frames_upstream_to_client: AtomicU64,
    pub bytes_client_to_upstream: AtomicU64,
    pub bytes_upstream_to_client: AtomicU64,
    pub rewritten_responses: AtomicU64,
    pub passthrough_responses: AtomicU64,
    pub max_c2u_buffer: AtomicUsize,
    pub max_u2c_buffer: AtomicUsize,
    pub max_rewrite_buffer: AtomicUsize,
}

impl ProxyStats {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn update_max(max: &AtomicUsize, value: usize) {
        let mut old = max.load(Ordering::Relaxed);
        while value > old {
            match max.compare_exchange(old, value, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(next) => old = next,
            }
        }
    }
}
