//! A shared stop request. It is a notification, not a replacement for a lock.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Default)]
pub struct Shutdown(Arc<AtomicBool>);

impl Shutdown {
    pub fn request(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    pub fn is_requested(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

// On stack unwinding, notify the accept loop before a worker disappears.
pub(crate) struct StopOnPanic(pub Shutdown);
impl Drop for StopOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.request();
        }
    }
}

// The supervisor declares this AFTER the pool. Reverse drop order matters.
pub(crate) struct StopOnDrop(pub Shutdown);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.request();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clones_observe_the_same_stop_request() {
        let stop = Shutdown::default();
        let clone = stop.clone();
        assert!(!stop.is_requested());
        clone.request();
        assert!(stop.is_requested());
    }
}
