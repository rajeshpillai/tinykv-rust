#!/usr/bin/env python3
"""A line-protocol client. Uses only the Python standard library."""
from __future__ import annotations
import argparse
import ipaddress
import socket
import sys
from typing import BinaryIO

MAX_RESPONSE = 8192

def exchange(sock: socket.socket, reader: BinaryIO, command: str) -> str:
    if "\n" in command or "\r" in command:
        raise ValueError("Send one command at a time. Do not include CR or LF.")
    payload = command.encode("utf-8") + b"\n"
    if len(payload) > 8192:
        raise ValueError("The command exceeds the 8192-byte frame limit.")
    sock.sendall(payload)
    response = reader.readline(MAX_RESPONSE + 1)
    if not response:
        raise ConnectionError("The server closed the connection. It can be busy, stopped, or at a session limit.")
    if len(response) > MAX_RESPONSE or not response.endswith(b"\n"):
        raise ValueError("The server sent an invalid response frame.")
    return response.decode("utf-8").removesuffix("\n")

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7878)
    parser.add_argument("commands", nargs="*", help='Quote each command, for example: "SET a hello Rust" "GET a"')
    args = parser.parse_args()
    try:
        if not ipaddress.ip_address(args.host).is_loopback or not 1 <= args.port <= 65535:
            raise ValueError("Use a numeric loopback address and a port from 1 to 65535.")
        with socket.create_connection((args.host, args.port), timeout=5) as sock:
            with sock.makefile("rb") as reader:
                if args.commands:
                    for command in args.commands:
                        print(exchange(sock, reader, command))
                        if command.strip() == "QUIT":
                            break
                else:
                    print("The server closes idle sessions after 10 seconds. Type QUIT to close now.")
                    while True:
                        try:
                            command = input("tinykv> ")
                        except EOFError:
                            break
                        print(exchange(sock, reader, command))
                        if command.strip() == "QUIT":
                            break
        return 0
    except (OSError, ValueError, UnicodeError) as error:
        print(f"client: {error}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
