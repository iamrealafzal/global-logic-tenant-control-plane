"""Publish one progress message, for duplicate and out-of-order checks."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from uuid import uuid4

from controlplane.broker import Broker
from controlplane.messages import ProgressEvent


def main() -> None:
    parser = argparse.ArgumentParser(prog="publish")
    parser.add_argument("--nats-url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    parser.add_argument("--stream", default=os.environ.get("STREAM_NAME", "PROVISIONING"))
    parser.add_argument("--subject-prefix", default=os.environ.get("SUBJECT_PREFIX", "tasks"))
    parser.add_argument("--task-id", default="")
    parser.add_argument("--status", default="")
    parser.add_argument("--update-id", default="")
    args = parser.parse_args()
    if args.task_id == "" or args.status == "":
        print("task-id and status are required", file=sys.stderr)
        raise SystemExit(2)
    update_id = args.update_id or str(uuid4())
    event = ProgressEvent(update_id=update_id, task_id=args.task_id, status=args.status)
    try:
        event.validate()
    except Exception as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(2) from exc
    asyncio.run(_publish(args.nats_url, args.stream, args.subject_prefix, event))
    print(f"published {args.subject_prefix}.progress update_id={event.update_id}")


async def _publish(url: str, stream: str, prefix: str, event: ProgressEvent) -> None:
    broker = await Broker.connect(url, prefix, stream)
    try:
        await broker.publish(f"{prefix}.progress", event.update_id, event.encode())
    finally:
        await broker.close()


if __name__ == "__main__":
    main()
