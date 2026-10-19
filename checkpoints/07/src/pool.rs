//! A bounded teaching thread pool. One job owns one whole connection.
use crate::shutdown::{Shutdown, StopOnPanic};
use std::{
    io,
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
};

type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitError {
    Full,
    Closed,
}

pub(crate) struct ThreadPool {
    sender: Option<SyncSender<Job>>,
    workers: Vec<JoinHandle<io::Result<()>>>,
}

impl ThreadPool {
    pub(crate) fn new(size: usize, capacity: usize, stop: Shutdown) -> io::Result<Self> {
        if !(1..=64).contains(&size) || !(1..=1024).contains(&capacity) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "workers: 1..=64; queue: 1..=1024",
            ));
        }
        let (sender, receiver) = mpsc::sync_channel::<Job>(capacity);
        let queue = Arc::new(Mutex::new(receiver));
        // Build the pool first. If a later spawn fails, Drop joins earlier workers.
        let mut pool = Self {
            sender: Some(sender),
            workers: Vec::with_capacity(size),
        };
        for id in 0..size {
            let queue = Arc::clone(&queue);
            let stop = stop.clone();
            let worker = thread::Builder::new()
                .name(format!("kv-worker-{id}"))
                .spawn(move || worker_loop(queue, stop))?;
            pool.workers.push(worker);
        }
        Ok(pool)
    }

    pub(crate) fn try_execute<F>(&self, job: F) -> Result<(), SubmitError>
    where
        F: FnOnce() + Send + 'static,
    {
        let sender = self.sender.as_ref().ok_or(SubmitError::Closed)?;
        match sender.try_send(Box::new(job)) {
            Ok(()) => Ok(()),
            // The rejected closure is dropped. Its socket closes with it.
            Err(TrySendError::Full(_)) => Err(SubmitError::Full),
            Err(TrySendError::Disconnected(_)) => Err(SubmitError::Closed),
        }
    }

    /// Close admission and join EVERY worker, even if one join reports a panic.
    pub(crate) fn close_and_join(&mut self) -> io::Result<()> {
        drop(self.sender.take());
        let mut failure = None;
        for worker in self.workers.drain(..) {
            let result = match worker.join() {
                Ok(result) => result,
                Err(_) => Err(io::Error::other("worker panicked")),
            };
            failure = failure.or(result.err());
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn worker_loop(queue: Arc<Mutex<Receiver<Job>>>, stop: Shutdown) -> io::Result<()> {
    let _panic_notice = StopOnPanic(stop.clone());
    loop {
        let received = {
            let receiver = match queue.lock() {
                Ok(receiver) => receiver,
                Err(_) => {
                    stop.request();
                    return Err(io::Error::other("worker queue lock is poisoned"));
                }
            };
            receiver.recv()
        }; // Release the QUEUE lock before running any job.
        let Ok(job) = received else {
            return Ok(());
        };
        if !stop.is_requested() {
            job();
        }
        // On stop, queued jobs are dropped, not started. Active jobs cooperate.
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // Normal shutdown uses close_and_join and reports errors to the caller.
        // Drop is a non-panicking fallback. It cannot return an error.
        let _ = self.close_and_join();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    const WAIT: Duration = Duration::from_secs(5);

    #[test]
    fn queue_capacity_is_enforced_without_blocking_admission() {
        let mut pool = ThreadPool::new(1, 1, Shutdown::default()).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        pool.try_execute(move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(WAIT).unwrap();
        })
        .unwrap();
        started_rx.recv_timeout(WAIT).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let other = Arc::clone(&count);
        pool.try_execute(move || {
            other.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
        assert_eq!(pool.try_execute(|| {}), Err(SubmitError::Full));
        release_tx.send(()).unwrap();
        pool.close_and_join().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn jobs_run_outside_the_queue_lock() {
        let mut pool = ThreadPool::new(2, 2, Shutdown::default()).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let mut releases = Vec::new();
        for _ in 0..2 {
            let started_tx = started_tx.clone();
            let (release_tx, release_rx) = mpsc::channel();
            releases.push(release_tx);
            pool.try_execute(move || {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(WAIT).unwrap();
            })
            .unwrap();
        }
        started_rx.recv_timeout(WAIT).unwrap();
        started_rx.recv_timeout(WAIT).unwrap();
        for release in releases {
            release.send(()).unwrap();
        }
        pool.close_and_join().unwrap();
    }

    #[test]
    fn panic_requests_shutdown_and_join_reports_it() {
        let stop = Shutdown::default();
        let mut pool = ThreadPool::new(1, 1, stop.clone()).unwrap();
        pool.try_execute(|| panic!("intentional worker test panic"))
            .unwrap();
        assert!(pool.close_and_join().is_err());
        assert!(stop.is_requested());
    }

    #[test]
    fn stop_discards_queued_jobs_but_does_not_interrupt_the_active_job() {
        let stop = Shutdown::default();
        let mut pool = ThreadPool::new(1, 1, stop.clone()).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        pool.try_execute(move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(WAIT).unwrap();
        })
        .unwrap();
        started_rx.recv_timeout(WAIT).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let other = Arc::clone(&count);
        pool.try_execute(move || {
            other.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
        stop.request();
        release_tx.send(()).unwrap();
        pool.close_and_join().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn invalid_worker_configuration_is_rejected() {
        assert!(ThreadPool::new(0, 1, Shutdown::default()).is_err());
        assert!(ThreadPool::new(1, 0, Shutdown::default()).is_err());
    }
}
