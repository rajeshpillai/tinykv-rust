#!/usr/bin/env python3
"""Run Rust quality checks. This script does not claim that they already passed."""
from __future__ import annotations
import argparse
from pathlib import Path
import shutil
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", action="store_true", help="Run rustfmt before checking; this changes formatting only.")
    args = parser.parse_args()
    cargo = shutil.which("cargo")
    if cargo is None:
        print("Cargo is not installed or is not on PATH.", file=sys.stderr)
        return 2
    project = Path(__file__).resolve().parents[1]
    commands = [[cargo, "--version"], ["rustc", "--version"]]
    if args.format:
        commands.append([cargo, "fmt", "--all"])
    commands += [
        [cargo, "fmt", "--all", "--", "--check"],
        [cargo, "check", "--all-targets"],
        [cargo, "test", "--all-targets"],
        [cargo, "clippy", "--all-targets", "--", "-D", "warnings"],
        [cargo, "test", "--release", "--all-targets"],
    ]
    for command in commands:
        print("+ " + " ".join(command), flush=True)
        result = subprocess.run(command, cwd=project, check=False)
        if result.returncode:
            return result.returncode
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
