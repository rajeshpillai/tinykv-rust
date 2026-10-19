use crate::{
    Shutdown, Store,
    connection::{ConnectionLimits, handle_connection},
    database::StoreLimits,
    pool::{SubmitError, ThreadPool},
    shutdown::StopOnDrop,
};
use std::{
    io,
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy)]
pub struct ServerConfig {
    pub workers: usize,
    pub queue_capacity: usize,
    pub store_limits: StoreLimits,
    pub connection_limits: ConnectionLimits,
    pub sweep_interval: Duration,
    pub accept_poll: Duration,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            queue_capacity: 32,
            store_limits: StoreLimits::default(),
            connection_limits: ConnectionLimits::default(),
            sweep_interval: Duration::from_millis(100),
            accept_poll: Duration::from_millis(10),
        }
    }
}

#[derive(Debug, Default)]
pub struct ServerReport {
    pub accepted_connections: usize,
    pub rejected_connections: usize,
    pub connection_errors: usize,
}

/// Serve only a loopback listener. A caller binds port zero for isolated tests.
/// Shutdown completes the current admitted command within its I/O policy.
/// It discards partial frames and queued sessions. It is not a durability promise.
pub fn serve(
    listener: TcpListener,
    config: ServerConfig,
    stop: Shutdown,
) -> io::Result<ServerReport> {
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "this teaching server accepts only loopback listeners",
        ));
    }
    config.connection_limits.validate()?;
    if config.sweep_interval.is_zero()
        || config.sweep_interval > Duration::from_secs(60)
        || config.accept_poll.is_zero()
        || config.accept_poll > Duration::from_secs(1)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid accept or sweep interval",
        ));
    }
    listener.set_nonblocking(true)?;
    let store = Arc::new(Store::new(config.store_limits).map_err(io::Error::other)?);
    let errors = Arc::new(AtomicUsize::new(0));
    let mut pool = ThreadPool::new(config.workers, config.queue_capacity, stop.clone())?;
    // On unwind, this requests stop BEFORE the pool's Drop waits for handlers.
    let _stop_on_exit = StopOnDrop(stop.clone());
    let mut report = ServerReport::default();

    // Keep fallible loop work inside a closure. Always run cleanup afterwards.
    let outcome = (|| -> io::Result<()> {
        let mut last_sweep = Instant::now();
        while !stop.is_requested() {
            if last_sweep.elapsed() >= config.sweep_interval {
                store.purge_expired().map_err(io::Error::other)?;
                last_sweep = Instant::now();
            }
            match listener.accept() {
                Ok((stream, _peer)) => {
                    report.accepted_connections += 1;
                    if stop.is_requested() {
                        break;
                    }
                    let accepted_at = Instant::now();
                    let store = Arc::clone(&store);
                    let stop = stop.clone();
                    let errors = Arc::clone(&errors);
                    let limits = config.connection_limits;
                    match pool.try_execute(move || {
                        if handle_connection(stream, accepted_at, store, stop, limits).is_err() {
                            // Count failures without logging keys, values, or raw input.
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }) {
                        Ok(()) => {}
                        Err(SubmitError::Full) => report.rejected_connections += 1,
                        Err(SubmitError::Closed) => {
                            return Err(io::Error::other("worker pool is closed"));
                        }
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(config.accept_poll)
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    })();

    stop.request();
    drop(listener); // Stop network admission before joining workers.
    let joined = pool.close_and_join();
    outcome?;
    joined?;
    report.connection_errors = errors.load(Ordering::Relaxed);
    Ok(report)
}
