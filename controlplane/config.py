"""Process configuration. Environment variables override the defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    http_addr: str
    database_path: str
    nats_url: str
    outbox_interval: float
    subject_prefix: str
    stream_name: str

    @classmethod
    def from_env(cls) -> Settings:
        database_path = os.environ.get("DATABASE_PATH", "controlplane.db")
        if database_path == "":
            raise RuntimeError("DATABASE_PATH is required")
        prefix = os.environ.get("SUBJECT_PREFIX", "tasks")
        stream = os.environ.get("STREAM_NAME", "PROVISIONING")
        if prefix == "" or stream == "":
            raise RuntimeError("subject prefix and stream name are required")
        return cls(
            http_addr=os.environ.get("HTTP_ADDR", ":8080"),
            database_path=database_path,
            nats_url=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"),
            outbox_interval=_duration(os.environ.get("OUTBOX_INTERVAL", "0.5")),
            subject_prefix=prefix,
            stream_name=stream,
        )

    @property
    def accepted_subject(self) -> str:
        return f"{self.subject_prefix}.accepted"

    @property
    def progress_subject(self) -> str:
        return f"{self.subject_prefix}.progress"


def parse_http_addr(addr: str) -> tuple[str, int]:
    if addr.startswith(":"):
        return "0.0.0.0", int(addr[1:])  # nosec B104
    host, _, port = addr.rpartition(":")
    if host == "" or port == "":
        raise RuntimeError(f"invalid HTTP_ADDR {addr!r}")
    return host, int(port)


def _duration(raw: str) -> float:
    text = raw.strip()
    try:
        if text.endswith("ms"):
            seconds = float(text[:-2]) / 1000
        elif text.endswith("s"):
            seconds = float(text[:-1])
        else:
            seconds = float(text)
    except ValueError as exc:
        raise RuntimeError(f"OUTBOX_INTERVAL {raw!r} is invalid") from exc
    if seconds <= 0:
        raise RuntimeError("OUTBOX_INTERVAL must be positive")
    return seconds
