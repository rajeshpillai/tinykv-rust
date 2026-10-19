# TinyKV — a TCP key-value cache in Rust

TinyKV is a small key-value cache that serves clients over TCP. It is the code for a six-episode
video course. Each episode types every line on screen, compiles it, and runs it. The course also
compiles the wrong versions on purpose, so you see the real compiler errors and failed tests.

TinyKV is like Redis in its core ideas: commands, expiry, many clients, and a clean shutdown. It is
**not** Redis. It speaks its own line protocol, not the Redis protocol.

Verified with **rustc 1.95.0 (59807616e 2026-04-14), edition 2024**, on Windows x64.

## Run it

You need Rust 1.95.0. The file `rust-toolchain.toml` selects it for you when you use rustup.

```sh
cargo test                  # 37 library tests and 14 network tests
cargo run                   # listens on 127.0.0.1:7878; Ctrl+C stops it
```

In a second terminal, send commands with the Python client. It uses only the standard library.

```sh
python3 scripts/client.py "PING" "SET greeting hello Rust" "GET greeting" "QUIT"
```

The server replies `PONG`, `OK`, `VALUE hello Rust` and `BYE`.

To run every release check in order (fmt, check, test, Clippy with warnings as errors, and the tests
in release mode):

```sh
python3 scripts/verify.py
```

To try the core without a socket:

```sh
cargo run --example in_memory
```

## Commands

One command per line. A line ends with `\n`. The server also accepts `\r\n`.

| Command | Reply |
|---|---|
| `PING` | `PONG` |
| `SET key value` | `OK`. A plain SET also removes an old deadline. |
| `SETEX key seconds value`, `PSETEX key ms value` | `OK`. The key expires after that time. |
| `GET key` | `VALUE value`, or `NIL` when the key is missing or expired |
| `DELETE key` (or `DEL`) | `INTEGER 1` when it removed a key, else `INTEGER 0` |
| `EXISTS key` | `INTEGER 1` or `INTEGER 0` |
| `EXPIRE key seconds`, `PEXPIRE key ms` | `INTEGER 1` when the key exists, else `INTEGER 0`. A time of 0 deletes the key. |
| `TTL key`, `PTTL key` | `INTEGER n`: the time left (TTL rounds down to whole seconds), `-1` for no deadline, `-2` for no such key |
| `PERSIST key` | `INTEGER 1` when it removed a deadline, else `INTEGER 0` |
| `STATS` | `STATS keys=N payload_bytes=N`: the number of keys, and the bytes of their keys and values |
| `QUIT` | `BYE`, then the server closes the connection |

A bad line gets `ERR` and a code, for example `ERR SYNTAX` or `ERR UNKNOWN_COMMAND`. A bad line
never changes the store. A stored value always starts with `VALUE`, so a value cannot look like
`NIL`.

Limits: a key is at most 128 bytes, a value at most 4096 bytes, and a whole line at most 8192 bytes, with its line ending.
A deadline is at most 30 days.

## Follow along by chapter

The root of this repository is the finished code, at the end of chapter 12. The folder
`checkpoints/NN` holds the project at the end of chapter NN. Each checkpoint is a complete crate:
go into the folder and run `cargo test`.

| Episode | Chapters | Checkpoints |
|---|---|---|
| 1 Commands before sockets | 1–3 | `checkpoints/01` to `03` |
| 2 Keys that expire | 4 | `checkpoints/04` |
| 3 Arc, Mutex and a thread pool | 5–7 | `checkpoints/05` to `07` |
| 4 One read is not one command | 8–9 | `checkpoints/08`, `09` |
| 5 Many clients, one clean stop | 10–11 | `checkpoints/10`, `11` |
| 6 Prove it, then say what it is not | 12 | the repository root |

## What TinyKV does not do

- It keeps nothing on disk. A restart loses every key.
- It listens only on loopback. That limits who can reach it, but it is not authentication. Any
  program on the same machine can read and change it.
- It has no TLS, no users, no permissions, and no replication. Do not put secrets, or the only copy
  of anything, in it.
- It has one lock for the whole map, and one worker thread for each connected client. Four idle
  clients can hold all four workers until their deadlines end.
- The byte budget counts keys and values. It is not the memory of the process.
- The shutdown is cooperative. It is fast because every loop checks the stop flag. Nothing forces a
  thread to stop.
- `scripts/signal_smoke.py` tests a real SIGINT on Linux and macOS. On Windows it prints NOT RUN and
  exits with code 2. The course tests Ctrl+C on Windows by hand.

## Credit

This course builds on a Rust lesson by Will Velida:
[the video](https://www.youtube.com/watch?v=kYrEs3pdeW8) and
[the code](https://github.com/willvelida/learn-rust/tree/lesson20/key-value-store) (MIT). His
server has SET, GET and DELETE on a pool of four worker threads that share one map. This course
keeps the pool and the shared map, and adds the rest: expiry, limits, the bounded queue, the frame
reader, the clean shutdown, and the tests. He has not reviewed or endorsed this code.

## License

MIT. See [LICENSE](LICENSE).
