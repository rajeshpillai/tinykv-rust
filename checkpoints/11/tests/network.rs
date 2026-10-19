//! Real-socket tests. These use an ephemeral port, not the tutorial's fixed port.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::{Shutdown as SocketShutdown, SocketAddr, TcpListener, TcpStream},
    sync::mpsc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tinykv_lab::{ServerConfig, ServerReport, Shutdown, serve};

const WAIT: Duration = Duration::from_secs(5);
struct Harness {
    address: SocketAddr,
    stop: Shutdown,
    done: mpsc::Receiver<io::Result<ServerReport>>,
    worker: Option<JoinHandle<()>>,
}
impl Harness {
    fn start(config: ServerConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Shutdown::default();
        let worker_stop = stop.clone();
        let (done_tx, done) = mpsc::channel();
        let worker = thread::spawn(move || {
            let result = serve(listener, config, worker_stop);
            let _ = done_tx.send(result);
        });
        Self {
            address,
            stop,
            done,
            worker: Some(worker),
        }
    }
    fn connect(&self) -> BufReader<TcpStream> {
        let stream = TcpStream::connect_timeout(&self.address, WAIT).unwrap();
        stream.set_read_timeout(Some(WAIT)).unwrap();
        stream.set_write_timeout(Some(WAIT)).unwrap();
        BufReader::new(stream)
    }
    fn finish(mut self) -> ServerReport {
        self.stop.request();
        let report = self
            .done
            .recv_timeout(WAIT)
            .expect("server did not stop before test deadline")
            .unwrap();
        self.worker.take().unwrap().join().unwrap();
        report
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.request();
        // Failure-path test cleanup must not hang after a deadline assertion.
        // Successful tests call finish(), which joins the server thread.
    }
}
fn command(client: &mut BufReader<TcpStream>, text: &str) -> String {
    writeln!(client.get_mut(), "{text}").unwrap();
    response(client)
}
fn response(client: &mut BufReader<TcpStream>) -> String {
    let mut reply = String::new();
    assert!(client.read_line(&mut reply).unwrap() > 0, "unexpected EOF");
    reply
}

#[test]
fn complete_session_uses_one_persistent_connection() {
    let server = Harness::start(ServerConfig::default());
    let mut client = server.connect();
    assert_eq!(command(&mut client, "PING"), "PONG\n");
    assert_eq!(command(&mut client, "SET name Rajesh Pillai"), "OK\n");
    assert_eq!(command(&mut client, "GET name"), "VALUE Rajesh Pillai\n");
    assert_eq!(command(&mut client, "DEL name"), "INTEGER 1\n");
    assert_eq!(command(&mut client, "GET name"), "NIL\n");
    assert_eq!(command(&mut client, "QUIT"), "BYE\n");
    server.finish();
}

#[test]
fn invalid_input_does_not_change_an_existing_value() {
    let server = Harness::start(ServerConfig::default());
    let mut client = server.connect();
    command(&mut client, "SET a old");
    assert_eq!(command(&mut client, "SETEX a 0 new"), "ERR INVALID_TTL\n");
    assert_eq!(command(&mut client, "GET a extra"), "ERR SYNTAX\n");
    assert_eq!(command(&mut client, "GET a"), "VALUE old\n");
    server.finish();
}

#[test]
fn separate_writes_and_pipelined_commands_are_framed() {
    let server = Harness::start(ServerConfig::default());
    let mut client = server.connect();
    client.get_mut().write_all(b"SET a hel").unwrap();
    client.get_mut().write_all(b"lo\nGET a\r\nPING\n").unwrap();
    assert_eq!(response(&mut client), "OK\n");
    assert_eq!(response(&mut client), "VALUE hello\n");
    assert_eq!(response(&mut client), "PONG\n");
    server.finish();
}

#[test]
fn values_can_contain_unicode_and_response_words() {
    let server = Harness::start(ServerConfig::default());
    let mut client = server.connect();
    assert_eq!(command(&mut client, "SET a नमस्ते"), "OK\n");
    assert_eq!(command(&mut client, "GET a"), "VALUE नमस्ते\n");
    command(&mut client, "SET a NIL");
    assert_eq!(command(&mut client, "GET a"), "VALUE NIL\n");
    server.finish();
}

#[test]
fn partial_line_at_eof_is_not_a_command() {
    let server = Harness::start(ServerConfig::default());
    let mut client = server.connect();
    client.get_mut().write_all(b"SET a unfinished").unwrap();
    client.get_mut().shutdown(SocketShutdown::Write).unwrap();
    assert_eq!(response(&mut client), "ERR BAD_FRAME\n");
    let mut check = server.connect();
    assert_eq!(command(&mut check, "GET a"), "NIL\n");
    server.finish();
}

#[test]
fn an_oversized_frame_does_not_mutate_the_store() {
    let server = Harness::start(ServerConfig::default());
    let mut client = server.connect();
    let oversized = format!("SET a {}\n", "x".repeat(9000));
    let _ = client.get_mut().write_all(oversized.as_bytes());
    let mut reply = String::new();
    // Closing a socket with unread peer data can produce a reset, not a reply.
    match client.read_line(&mut reply) {
        Ok(0) => {}
        Ok(_) => assert_eq!(reply, "ERR BAD_FRAME\n"),
        Err(error) => assert!(matches!(
            error.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
        )),
    }
    let mut check = server.connect();
    assert_eq!(command(&mut check, "GET a"), "NIL\n");
    server.finish();
}

#[test]
fn invalid_utf8_is_a_frame_error() {
    let server = Harness::start(ServerConfig::default());
    let mut client = server.connect();
    client.get_mut().write_all(&[0xff, b'\n']).unwrap();
    assert_eq!(response(&mut client), "ERR BAD_FRAME\n");
    server.finish();
}

#[test]
fn multiple_clients_observe_one_shared_store() {
    let server = Harness::start(ServerConfig::default());
    let workers: Vec<_> = (0..8)
        .map(|id| {
            let address = server.address;
            thread::spawn(move || {
                let stream = TcpStream::connect_timeout(&address, WAIT).unwrap();
                stream.set_read_timeout(Some(WAIT)).unwrap();
                stream.set_write_timeout(Some(WAIT)).unwrap();
                let mut client = BufReader::new(stream);
                assert_eq!(
                    command(&mut client, &format!("SET client:{id} value:{id}")),
                    "OK\n"
                );
                command(&mut client, "QUIT");
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let mut check = server.connect();
    for id in 0..8 {
        assert_eq!(
            command(&mut check, &format!("GET client:{id}")),
            format!("VALUE value:{id}\n")
        );
    }
    server.finish();
}

#[test]
fn expiry_is_observable_over_tcp() {
    let server = Harness::start(ServerConfig::default());
    let mut client = server.connect();
    assert_eq!(command(&mut client, "PSETEX a 80 v"), "OK\n");
    let deadline = Instant::now() + WAIT;
    loop {
        if command(&mut client, "GET a") == "NIL\n" {
            break;
        }
        assert!(Instant::now() < deadline, "key did not expire");
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(command(&mut client, "TTL a"), "INTEGER -2\n");
    server.finish();
}

#[test]
fn idle_clients_do_not_prevent_shutdown() {
    let server = Harness::start(ServerConfig::default());
    let mut clients: Vec<_> = (0..4).map(|_| server.connect()).collect();
    for client in &mut clients {
        assert_eq!(command(client, "PING"), "PONG\n");
    }
    server.finish(); // The harness enforces a 5-second deadline.
}

#[test]
fn a_partial_frame_does_not_prevent_shutdown() {
    let server = Harness::start(ServerConfig::default());
    let mut client = server.connect();
    command(&mut client, "PING"); // Confirm that a worker owns this connection.
    client.get_mut().write_all(b"SET a partial").unwrap();
    server.finish();
}

#[test]
fn full_queue_rejects_extra_connections() {
    let config = ServerConfig {
        workers: 1,
        queue_capacity: 1,
        ..ServerConfig::default()
    };
    let server = Harness::start(config);
    let mut active = server.connect();
    assert_eq!(command(&mut active, "PING"), "PONG\n");
    let _queued = server.connect();
    let mut rejected = server.connect();
    let mut byte = [0];
    match rejected.read(&mut byte) {
        Ok(count) => assert_eq!(count, 0),
        Err(error) => assert!(matches!(
            error.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
        )),
    }
    assert!(server.finish().rejected_connections >= 1);
}

#[test]
fn command_deadline_also_limits_idle_time() {
    let mut config = ServerConfig::default();
    config.connection_limits.frame_timeout = Duration::from_millis(100);
    let server = Harness::start(config);
    let mut client = server.connect();
    assert_eq!(response(&mut client), "ERR TIMEOUT\n");
    server.finish();
}

#[test]
fn command_count_limits_a_persistent_session() {
    let mut config = ServerConfig::default();
    config.connection_limits.max_commands = 2;
    let server = Harness::start(config);
    let mut client = server.connect();
    assert_eq!(command(&mut client, "PING"), "PONG\n");
    assert_eq!(command(&mut client, "PING"), "PONG\n");
    let mut byte = [0];
    assert_eq!(client.read(&mut byte).unwrap(), 0);
    server.finish();
}
