"""Wire format shared by the control plane, the worker, and the publish command."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from uuid import UUID

from controlplane.domain import InvalidProgress, TaskStatus


@dataclass(slots=True)
class TaskEvent:
    id: str
    type: str
    tenant_id: str
    status: str
    tenant_slug: str
    tenant_name: str

    def encode(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()


@dataclass(slots=True)
class ProgressEvent:
    update_id: str
    task_id: str
    status: str

    def encode(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()

    def validate(self) -> None:
        if self.update_id == "" or len(self.update_id) > 200:
            raise InvalidProgress("update_id is required and must be at most 200 characters")
        try:
            UUID(self.task_id)
        except ValueError as exc:
            raise InvalidProgress("task_id must be a uuid") from exc
        if self.status not in (TaskStatus.IN_PROGRESS, TaskStatus.DONE, TaskStatus.FAILED):
            raise InvalidProgress("status must be in_progress, done, or failed")


def decode_progress(payload: bytes) -> ProgressEvent:
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise InvalidProgress("progress payload must be json") from exc
    if not isinstance(raw, dict):
        raise InvalidProgress("progress payload must be a json object")
    try:
        event = ProgressEvent(
            update_id=str(raw["update_id"]),
            task_id=str(raw["task_id"]),
            status=str(raw["status"]),
        )
    except KeyError as exc:
        raise InvalidProgress(f"progress payload missing {exc.args[0]}") from exc
    event.validate()
    return event
