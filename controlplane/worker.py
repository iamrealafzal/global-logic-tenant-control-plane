"""Provisioning simulator.

Consumes accepted tasks and publishes in_progress, then done or failed.
The idempotency key is stable per task and status, so a redelivery is a no-op.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random

from controlplane.broker import Broker, PoisonMessage
from controlplane.domain import stable_update_id
from controlplane.messages import ProgressEvent, TaskEvent

log = logging.getLogger(__name__)


class Config:
    def __init__(
        self,
        nats_url: str,
        stream: str,
        prefix: str,
        fail_rate: float,
        min_delay: float,
        max_delay: float,
    ) -> None:
        if fail_rate < 0 or fail_rate > 1:
            raise RuntimeError("fail-rate must be between 0 and 1")
        if min_delay < 0 or max_delay < min_delay:
            raise RuntimeError("delay range is invalid")
        self.nats_url = nats_url
        self.stream = stream or "PROVISIONING"
        self.prefix = prefix or "tasks"
        self.fail_rate = fail_rate
        self.min_delay = min_delay
        self.max_delay = max_delay


class Worker:
    def __init__(self, config: Config, broker: Broker) -> None:
        self.config = config
        self.broker = broker
        self._rng = random.Random()  # nosec B311

    async def handle(self, payload: bytes) -> None:
        try:
            raw = json.loads(payload)
            event = TaskEvent(
                id=str(raw["id"]),
                type=str(raw["type"]),
                tenant_id=str(raw["tenant_id"]),
                status=str(raw["status"]),
                tenant_slug=str(raw.get("tenant_slug", "")),
                tenant_name=str(raw.get("tenant_name", "")),
            )
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise PoisonMessage("task event") from exc
        if event.id == "" or event.tenant_id == "":
            raise PoisonMessage("task event")
        log.info(
            "task received task_id=%s type=%s tenant_id=%s", event.id, event.type, event.tenant_id
        )
        await asyncio.sleep(self._delay())
        await self._publish(event.id, "in_progress")
        await asyncio.sleep(self._delay())
        outcome = "failed" if self._failed() else "done"
        await self._publish(event.id, outcome)
        log.info("task finished task_id=%s outcome=%s", event.id, outcome)

    async def _publish(self, task_id: str, status: str) -> None:
        update_id = stable_update_id(task_id, status)
        body = ProgressEvent(update_id=update_id, task_id=task_id, status=status).encode()
        await self.broker.publish(self.broker.progress_subject, update_id, body)

    def _delay(self) -> float:
        span = self.config.max_delay - self.config.min_delay
        if span == 0:
            return self.config.min_delay
        return self.config.min_delay + self._rng.random() * span  # nosec B311

    def _failed(self) -> bool:
        if self.config.fail_rate == 0:
            return False
        if self.config.fail_rate == 1:
            return True
        return self._rng.random() < self.config.fail_rate  # nosec B311


async def run(config: Config) -> None:
    last: Exception | None = None
    broker: Broker | None = None
    for attempt in range(1, 31):
        try:
            broker = await Broker.connect(config.nats_url, config.prefix, config.stream)
            break
        except Exception as exc:
            last = exc
            log.warning("waiting for nats attempt=%s err=%s", attempt, exc)
            await asyncio.sleep(1)
    if broker is None:
        if last is None:
            raise RuntimeError("nats connection failed")
        raise last
    worker = Worker(config, broker)
    log.info(
        "worker started fail_rate=%s min_delay=%s max_delay=%s",
        config.fail_rate,
        config.min_delay,
        config.max_delay,
    )
    try:
        await broker.consume("worker", broker.accepted_subject, worker.handle)
    finally:
        await broker.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="worker")
    parser.add_argument("--nats-url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    parser.add_argument("--stream", default=os.environ.get("STREAM_NAME", "PROVISIONING"))
    parser.add_argument("--subject-prefix", default=os.environ.get("SUBJECT_PREFIX", "tasks"))
    parser.add_argument("--fail-rate", type=float, default=_env_float("FAIL_RATE", 0))
    parser.add_argument("--min-delay-ms", type=int, default=_env_int("MIN_DELAY_MS", 500))
    parser.add_argument("--max-delay-ms", type=int, default=_env_int("MAX_DELAY_MS", 1500))
    args = parser.parse_args()
    config = Config(
        nats_url=args.nats_url,
        stream=args.stream,
        prefix=args.subject_prefix,
        fail_rate=args.fail_rate,
        min_delay=args.min_delay_ms / 1000,
        max_delay=args.max_delay_ms / 1000,
    )
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        return


def _env_int(key: str, fallback: int) -> int:
    raw = os.environ.get(key)
    if not raw:
        return fallback
    try:
        return int(raw)
    except ValueError:
        return fallback


def _env_float(key: str, fallback: float) -> float:
    raw = os.environ.get(key)
    if not raw:
        return fallback
    try:
        return float(raw)
    except ValueError:
        return fallback


if __name__ == "__main__":
    main()
