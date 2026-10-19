//! TCP is a byte stream. LF ends a frame; one read is not one command.
use crate::Shutdown;
use std::{
    io::{self, BufRead, Write},
    thread,
    time::{Duration, Instant},
};

pub(crate) const MAX_FRAME_BYTES: usize = 8192; // Includes LF and optional CR.

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Frame {
    Line(String),
    Closed,
    Stopped,
}

fn timeout() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "command or connection deadline elapsed",
    )
}
fn bad_frame(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(crate) fn read_frame<R: BufRead>(
    reader: &mut R,
    stop: &Shutdown,
    deadline: Instant,
) -> io::Result<Frame> {
    let mut bytes = Vec::new();
    loop {
        if stop.is_requested() {
            return Ok(Frame::Stopped);
        }
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        let available = match reader.fill_buf() {
            Ok(available) => available,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                // Keep the partial frame. Do not start again with an empty buffer.
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            Err(error) => return Err(error),
        };
        if available.is_empty() {
            return if bytes.is_empty() {
                Ok(Frame::Closed)
            } else {
                Err(bad_frame("EOF before LF"))
            };
        }
        let take = available
            .iter()
            .position(|&b| b == b'\n')
            .map_or(available.len(), |index| index + 1);
        if take > MAX_FRAME_BYTES - bytes.len() {
            return Err(bad_frame("frame exceeds byte limit"));
        }
        let complete = available[take - 1] == b'\n';
        bytes.extend_from_slice(&available[..take]);
        reader.consume(take);
        if complete {
            bytes.pop(); // Remove LF.
            if bytes.last() == Some(&b'\r') {
                bytes.pop();
            }
            // Recheck AFTER a potentially blocking read. Do not execute a late frame.
            if stop.is_requested() {
                return Ok(Frame::Stopped);
            }
            if Instant::now() >= deadline {
                return Err(timeout());
            }
            let line = String::from_utf8(bytes).map_err(|_| bad_frame("frame is not UTF-8"))?;
            return Ok(Frame::Line(line));
        }
    }
}

pub(crate) fn write_response<W: Write>(
    writer: &mut W,
    bytes: &[u8],
    deadline: Instant,
) -> io::Result<()> {
    let mut sent = 0;
    while sent < bytes.len() {
        if Instant::now() >= deadline {
            return Err(timeout());
        }
        match writer.write(&bytes[sent..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "peer accepts no more bytes",
                ));
            }
            Ok(count) => sent += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
    }
    // TcpStream is not a userspace buffered writer; no flush is required here.
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor, Read};
    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    #[test]
    fn pipelined_lines_stay_separate_and_crlf_is_supported() {
        let mut reader = BufReader::new(Cursor::new(b"PING\r\nGET a\n"));
        let stop = Shutdown::default();
        assert_eq!(
            read_frame(&mut reader, &stop, deadline()).unwrap(),
            Frame::Line("PING".into())
        );
        assert_eq!(
            read_frame(&mut reader, &stop, deadline()).unwrap(),
            Frame::Line("GET a".into())
        );
        assert_eq!(
            read_frame(&mut reader, &stop, deadline()).unwrap(),
            Frame::Closed
        );
    }

    #[test]
    fn fragmented_input_crosses_buffer_boundaries() {
        let mut reader = BufReader::with_capacity(2, Cursor::new("SET a नमस्ते\n".as_bytes()));
        assert_eq!(
            read_frame(&mut reader, &Shutdown::default(), deadline()).unwrap(),
            Frame::Line("SET a नमस्ते".into())
        );
    }

    #[test]
    fn eof_does_not_turn_a_partial_command_into_a_write() {
        let mut reader = Cursor::new(b"SET a value");
        assert_eq!(
            read_frame(&mut reader, &Shutdown::default(), deadline())
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn frame_limit_is_checked_before_appending() {
        let mut exact = vec![b'a'; MAX_FRAME_BYTES - 1];
        exact.push(b'\n');
        assert!(read_frame(&mut Cursor::new(exact), &Shutdown::default(), deadline()).is_ok());
        let mut long = vec![b'a'; MAX_FRAME_BYTES];
        long.push(b'\n');
        assert_eq!(
            read_frame(&mut Cursor::new(long), &Shutdown::default(), deadline())
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn invalid_utf8_is_rejected() {
        assert!(
            read_frame(
                &mut Cursor::new([0xff, b'\n']),
                &Shutdown::default(),
                deadline()
            )
            .is_err()
        );
    }

    #[test]
    fn stop_is_observed_without_waiting_for_a_new_line() {
        let stop = Shutdown::default();
        stop.request();
        assert_eq!(
            read_frame(&mut Cursor::new(b""), &stop, deadline()).unwrap(),
            Frame::Stopped
        );
    }

    #[test]
    fn an_elapsed_deadline_does_not_accept_buffered_data() {
        let mut reader = Cursor::new(b"PING\n");
        assert_eq!(
            read_frame(&mut reader, &Shutdown::default(), Instant::now())
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
    }

    struct IntermittentReader {
        calls: u8,
    }
    impl Read for IntermittentReader {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            self.calls += 1;
            match self.calls {
                1 => {
                    out[..2].copy_from_slice(b"PI");
                    Ok(2)
                }
                2 => Err(io::Error::from(io::ErrorKind::TimedOut)),
                3 => {
                    out[..3].copy_from_slice(b"NG\n");
                    Ok(3)
                }
                _ => Ok(0),
            }
        }
    }
    #[test]
    fn a_socket_timeout_does_not_erase_partial_bytes() {
        let mut reader = BufReader::with_capacity(8, IntermittentReader { calls: 0 });
        assert_eq!(
            read_frame(&mut reader, &Shutdown::default(), deadline()).unwrap(),
            Frame::Line("PING".into())
        );
    }

    #[derive(Default)]
    struct PartialWriter {
        output: Vec<u8>,
        calls: u8,
    }
    impl Write for PartialWriter {
        fn write(&mut self, input: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            if self.calls == 2 {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            let count = input.len().min(2);
            self.output.extend_from_slice(&input[..count]);
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn partial_writes_do_not_duplicate_bytes() {
        let mut writer = PartialWriter::default();
        write_response(&mut writer, b"VALUE hello\n", deadline()).unwrap();
        assert_eq!(writer.output, b"VALUE hello\n");
    }

    #[test]
    fn write_deadline_is_an_overall_limit() {
        let mut writer = PartialWriter::default();
        assert_eq!(
            write_response(&mut writer, b"OK\n", Instant::now())
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        assert!(writer.output.is_empty());
    }
}
