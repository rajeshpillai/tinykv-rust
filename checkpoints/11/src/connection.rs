//! One bounded connection session. The worker owns the socket until this returns.
use crate::{
    Shutdown, Store,
    framing::{self, Frame},
    protocol::Reply,
};
use std::{
    io::{self, BufReader},
    net::TcpStream,
    sync::Arc,
    time::{Duration, Instant},
};

const IO_POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy)]
pub struct ConnectionLimits {
    /// One complete frame must arrive in this period, including idle time.
    pub frame_timeout: Duration,
    /// Overall response-write deadline, not a fresh limit for every write call.
    pub write_timeout: Duration,
    pub max_lifetime: Duration,
    pub queue_timeout: Duration,
    pub max_commands: usize,
}
impl Default for ConnectionLimits {
    fn default() -> Self {
        Self {
            frame_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(2),
            max_lifetime: Duration::from_secs(60),
            queue_timeout: Duration::from_secs(2),
            max_commands: 256,
        }
    }
}
impl ConnectionLimits {
    pub(crate) fn validate(self) -> io::Result<()> {
        let durations = [
            self.frame_timeout,
            self.write_timeout,
            self.max_lifetime,
            self.queue_timeout,
        ];
        if durations
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(3600))
            || !(1..=10_000).contains(&self.max_commands)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connection durations: >0..=3600s; commands: 1..=10000",
            ));
        }
        Ok(())
    }
}

fn after(start: Instant, duration: Duration) -> io::Result<Instant> {
    start
        .checked_add(duration)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "deadline overflow"))
}

fn send(reader: &mut BufReader<TcpStream>, text: &str, limits: ConnectionLimits) -> io::Result<()> {
    framing::write_response(
        reader.get_mut(),
        text.as_bytes(),
        after(Instant::now(), limits.write_timeout)?,
    )
}

pub(crate) fn handle_connection(
    stream: TcpStream,
    accepted_at: Instant,
    store: Arc<Store>,
    stop: Shutdown,
    limits: ConnectionLimits,
) -> io::Result<()> {
    if stop.is_requested() {
        return Ok(());
    }
    // Explicitly use blocking sockets with short OS timeouts on every platform.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(IO_POLL))?;
    stream.set_write_timeout(Some(IO_POLL))?;
    let mut reader = BufReader::new(stream);
    if Instant::now().saturating_duration_since(accepted_at) >= limits.queue_timeout {
        send(&mut reader, "ERR QUEUE_TIMEOUT\n", limits)?;
        return Ok(());
    }
    let connection_deadline = after(accepted_at, limits.max_lifetime)?;
    for _ in 0..limits.max_commands {
        if stop.is_requested() {
            return Ok(());
        }
        let frame_deadline = after(Instant::now(), limits.frame_timeout)?.min(connection_deadline);
        let line = match framing::read_frame(&mut reader, &stop, frame_deadline) {
            Ok(Frame::Line(line)) => line,
            Ok(Frame::Closed | Frame::Stopped) => return Ok(()),
            Err(error) => {
                let message = match error.kind() {
                    io::ErrorKind::InvalidData => Some("ERR BAD_FRAME\n"),
                    io::ErrorKind::TimedOut => Some("ERR TIMEOUT\n"),
                    _ => None,
                };
                if let Some(message) = message {
                    let _ = send(&mut reader, message, limits);
                }
                return Err(error);
            }
        };
        // This is the admission check for a command, not just a connection.
        if stop.is_requested() {
            return Ok(());
        }
        let reply = match store.execute_line(&line) {
            Ok(reply) => reply,
            Err(error) => {
                stop.request(); // Do not continue with a possibly inconsistent map.
                let _ = send(&mut reader, "ERR INTERNAL\n", limits);
                return Err(io::Error::other(error));
            }
        };
        let quit = matches!(&reply, Reply::Bye);
        // No store guard exists here. A slow peer can hold a worker, not the map.
        send(&mut reader, &reply.to_string(), limits)?;
        if quit {
            return Ok(());
        }
    }
    Ok(()) // Close after the configured number of commands. No extra reply.
}
