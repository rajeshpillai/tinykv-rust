#!/usr/bin/env python3
"""Check SIGINT shutdown of the built server on POSIX. This is not a Windows console test."""
from __future__ import annotations
import os
from pathlib import Path
import queue
import signal
import socket
import subprocess
import sys
import threading


def main() -> int:
    if os.name != "posix":
        print("NOT RUN: this script checks POSIX SIGINT. Test real Ctrl+C in a native Windows console.")
        return 2
    binary = Path(__file__).resolve().parents[1] / "target" / "debug" / "tinykv_lab"
    if not binary.is_file():
        print("NOT RUN: build the server first with cargo build.", file=sys.stderr)
        return 2
    process = subprocess.Popen(
        [str(binary), "127.0.0.1:0"], stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True, bufsize=1,
    )
    lines: queue.Queue[str | None] = queue.Queue()
    transcript: list[str] = []
    clients: list[socket.socket] = []

    def collect() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            transcript.append(line.rstrip())
            lines.put(line.rstrip())
        lines.put(None)

    reader_thread = threading.Thread(target=collect, daemon=True)
    reader_thread.start()
    try:
        line = lines.get(timeout=5)
        if line is None or "127.0.0.1:" not in line:
            raise RuntimeError(f"Unexpected startup output: {line!r}")
        # The server prints one numeric address in its first startup line.
        import re
        match = re.search(r"127\.0\.0\.1:(\d+)", line)
        if match is None:
            raise RuntimeError("The startup output has no listening port.")
        port = int(match.group(1))
        for _ in range(4):
            client = socket.create_connection(("127.0.0.1", port), timeout=3)
            clients.append(client)
            client.sendall(b"PING\n")
            reply = bytearray()
            while not reply.endswith(b"\n") and len(reply) <= 32:
                chunk = client.recv(32)
                if not chunk:
                    break
                reply.extend(chunk)
            if reply != b"PONG\n":
                raise RuntimeError(f"Unexpected PING response: {reply!r}")
        # Each client has an assigned worker. Leave all four clients idle.
        process.send_signal(signal.SIGINT)
        return_code = process.wait(timeout=5)
        reader_thread.join(timeout=1)
        if return_code != 0:
            raise RuntimeError(f"The server exited with code {return_code}.")
        print("PASS: the server exited normally after SIGINT with four idle clients.")
        print("This test checks the process signal path, not every shutdown race.")
        return 0
    except (OSError, ValueError, RuntimeError, queue.Empty, subprocess.TimeoutExpired) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    finally:
        for client in clients:
            client.close()
        if process.poll() is None:
            # Clean up only the child process that this script created.
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        reader_thread.join(timeout=1)
        for line in transcript:
            print(f"server: {line}")
        if process.stdout is not None and not reader_thread.is_alive():
            process.stdout.close()


if __name__ == "__main__":
    raise SystemExit(main())
