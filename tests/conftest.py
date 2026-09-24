"""Starts NATS once per test session.

SQLite is a file in the session temp directory. NATS prefers a local
nats-server binary and falls back to Docker.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@dataclass(slots=True)
class Infra:
    database_path: str
    nats_url: str


@pytest.fixture(scope="session")
def infra() -> Iterator[Infra]:
    if os.environ.get("DATABASE_PATH") and os.environ.get("NATS_URL"):
        yield Infra(os.environ["DATABASE_PATH"], os.environ["NATS_URL"])
        return
    with tempfile.TemporaryDirectory(prefix="controlplane-it-") as tmp:
        work = Path(tmp)
        nats = _Nats(work / "nats")
        try:
            nats.start()
            yield Infra(str(work / "controlplane.db"), nats.url)
        finally:
            nats.stop()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_tcp(port: int, timeout: float = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"nothing accepted connections on port {port}")


class _Nats:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.port = _free_port()
        self.url = f"nats://127.0.0.1:{self.port}"
        self.proc: subprocess.Popen[bytes] | None = None
        self.container = ""

    def start(self) -> None:
        binary = _nats_binary()
        if binary is not None:
            self.directory.mkdir(parents=True, exist_ok=True)
            self.proc = subprocess.Popen(
                [
                    str(binary),
                    "-js",
                    "-sd",
                    str(self.directory),
                    "-a",
                    "127.0.0.1",
                    "-p",
                    str(self.port),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            _wait_tcp(self.port)
            return
        if shutil.which("docker") is None:
            pytest.fail("nats-server binary or Docker is required to run the integration tests")
        self.container = f"cp-nats-{self.port}"
        subprocess.check_call(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                self.container,
                "-p",
                f"127.0.0.1:{self.port}:4222",
                "nats:2.15-alpine",
                "-js",
            ],
            stdout=subprocess.DEVNULL,
        )
        self.url = f"nats://127.0.0.1:{self.port}"
        _wait_tcp(self.port)

    def stop(self) -> None:
        _stop_process(self.proc)
        if self.container:
            subprocess.call(["docker", "rm", "-f", self.container], stdout=subprocess.DEVNULL)


def _stop_process(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def _nats_binary() -> Path | None:
    local = ROOT / ".tools" / "nats-server"
    if local.is_file():
        return local
    found = shutil.which("nats-server")
    return Path(found) if found else None
