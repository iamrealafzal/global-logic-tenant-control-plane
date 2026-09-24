"""JetStream transport between the control plane and the worker."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, StorageType, StreamConfig
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError

log = logging.getLogger(__name__)


class PoisonMessage(Exception):
    """A delivery that will never become valid. The consumer terminates it."""


class Broker:
    def __init__(self, connection: nats.NATS, prefix: str, stream: str) -> None:
        self.connection = connection
        self.prefix = prefix
        self.stream = stream
        self.js: JetStreamContext = connection.jetstream()

    @classmethod
    async def connect(cls, url: str, prefix: str, stream: str) -> Broker:
        connection = await nats.connect(
            url,
            name="controlplane",
            max_reconnect_attempts=-1,
            reconnect_time_wait=0.5,
            connect_timeout=2,
        )
        broker = cls(connection, prefix, stream)
        await broker.ensure_stream()
        return broker

    async def close(self) -> None:
        if not self.connection.is_closed:
            await self.connection.close()

    async def ping(self) -> None:
        if not self.connection.is_connected:
            raise RuntimeError("nats is not connected")
        await self.connection.flush(timeout=1)

    async def ensure_stream(self) -> None:
        config = StreamConfig(
            name=self.stream,
            subjects=[f"{self.prefix}.>"],
            storage=StorageType.FILE,
            duplicate_window=120,
            max_age=7 * 24 * 60 * 60,
        )
        try:
            await self.js.stream_info(self.stream)
        except NotFoundError:
            await self.js.add_stream(config)
            return
        await self.js.update_stream(config)

    async def publish(self, subject: str, msg_id: str, payload: bytes) -> None:
        headers = {"Nats-Msg-Id": msg_id} if msg_id else None
        await self.js.publish(subject, payload, headers=headers)

    async def consume(
        self,
        durable: str,
        subject: str,
        handler: Callable[[bytes], Awaitable[None]],
    ) -> None:
        """Pull deliveries until cancelled.

        A successful handler is acked. PoisonMessage is terminated immediately.
        Any other error is retried with backoff and then terminated so one bad
        delivery cannot stop the process.
        """
        subscription = await self.js.pull_subscribe(
            subject,
            durable=durable,
            stream=self.stream,
            config=ConsumerConfig(
                durable_name=durable,
                filter_subject=subject,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=600,
                max_deliver=10,
            ),
        )
        try:
            while True:
                try:
                    messages = await subscription.fetch(1, timeout=1)
                except TimeoutError, NatsTimeoutError:
                    continue
                for message in messages:
                    await _settle(message, handler)
        finally:
            await subscription.unsubscribe()

    @property
    def accepted_subject(self) -> str:
        return f"{self.prefix}.accepted"

    @property
    def progress_subject(self) -> str:
        return f"{self.prefix}.progress"


async def _settle(message, handler: Callable[[bytes], Awaitable[None]]) -> None:
    heartbeat = asyncio.create_task(_heartbeat(message))
    try:
        await handler(message.data)
    except PoisonMessage:
        log.exception("terminating poison message")
        await message.term()
        return
    except Exception:
        delivered = _delivery_count(message)
        log.exception("delivery failed")
        if delivered >= 8:
            await message.term()
            return
        await message.nak(delay=_retry_delay(delivered))
        return
    else:
        await message.ack()
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat


async def _heartbeat(message) -> None:
    while True:
        await asyncio.sleep(10)
        try:
            await message.in_progress()
        except Exception:
            return


def _delivery_count(message) -> int:
    try:
        return int(message.metadata.num_delivered)
    except Exception:
        return 1


def _retry_delay(delivered: int) -> float:
    if delivered <= 1:
        return 0.2
    if delivered == 2:
        return 0.5
    if delivered == 3:
        return 1
    if delivered == 4:
        return 2
    return 5
