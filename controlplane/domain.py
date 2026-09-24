"""Tenant and task state machines, plus the error codes the API returns."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

SLUG_PATTERN = re.compile(r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$")


class TenantStatus(StrEnum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    UPDATING = "updating"
    DESTROYING = "destroying"
    DESTROYED = "destroyed"
    FAILED = "failed"


class TaskType(StrEnum):
    DEPLOY = "deploy"
    UPDATE = "update"
    DESTROY = "destroy"


class TaskStatus(StrEnum):
    ACCEPTED = "accepted"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"


@dataclass(slots=True)
class Tenant:
    id: str
    slug: str
    name: str
    status: TenantStatus
    version: int
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class Task:
    id: str
    type: TaskType
    tenant_id: str
    status: TaskStatus
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class ProgressOutcome:
    duplicate: bool = False
    ignored: bool = False
    task_updated: bool = False
    tenant_updated: bool = False


@dataclass(slots=True)
class ProgressDecision:
    ignore: bool = False
    next_task: TaskStatus | None = None
    update_tenant: bool = False
    next_tenant: TenantStatus | None = None


class AppError(Exception):
    """A failure the HTTP layer can turn into a structured error body."""

    code = "internal_error"
    status = 500

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ValidationError(AppError):
    code = "validation_error"
    status = 400


class TenantNotFound(AppError):
    code = "tenant_not_found"
    status = 404

    def __init__(self) -> None:
        super().__init__("tenant not found")


class TaskNotFound(AppError):
    code = "task_not_found"
    status = 404

    def __init__(self) -> None:
        super().__init__("task not found")


class TenantAlreadyExists(AppError):
    code = "tenant_already_exists"
    status = 409

    def __init__(self) -> None:
        super().__init__("a tenant with this slug already exists")


class TenantUpdateNotAllowed(AppError):
    code = "tenant_update_not_allowed"
    status = 409

    def __init__(self) -> None:
        super().__init__("operation is not allowed for the tenant's current status")


class TenantVersionConflict(AppError):
    code = "tenant_version_conflict"
    status = 409

    def __init__(self) -> None:
        super().__init__("tenant version does not match")


class InvalidProgress(AppError):
    code = "invalid_progress"
    status = 400


def validate_slug(slug: str) -> None:
    if SLUG_PATTERN.fullmatch(slug) is None:
        raise ValidationError("slug must match ^[a-z][-a-z0-9]{1,26}[a-z0-9]$")


def validate_name(name: str) -> str:
    cleaned = name.strip()
    if cleaned == "":
        raise ValidationError("name must not be empty")
    if len(cleaned) > 200:
        raise ValidationError("name must be at most 200 characters")
    return cleaned


def validate_version(version: int) -> None:
    if version < 1:
        raise ValidationError("version is required and must be >= 1")


def known_tenant_status(value: str) -> TenantStatus:
    try:
        return TenantStatus(value)
    except ValueError as exc:
        raise ValidationError("unknown tenant status") from exc


def known_task_status(value: str) -> TaskStatus:
    try:
        return TaskStatus(value)
    except ValueError as exc:
        raise ValidationError("unknown task status") from exc


def can_update(status: TenantStatus) -> bool:
    return status == TenantStatus.ACTIVE


def can_delete(status: TenantStatus) -> bool:
    return status in (TenantStatus.ACTIVE, TenantStatus.FAILED)


def expected_tenant_status(task_type: TaskType) -> TenantStatus:
    match task_type:
        case TaskType.DEPLOY:
            return TenantStatus.PROVISIONING
        case TaskType.UPDATE:
            return TenantStatus.UPDATING
        case TaskType.DESTROY:
            return TenantStatus.DESTROYING
        case _:
            raise InvalidProgress(f"task type {task_type!r}")


def decide_progress(
    task_type: TaskType, current: TaskStatus, incoming: TaskStatus
) -> ProgressDecision:
    """Apply one worker update.

    A terminal task ignores every later update, so state cannot regress.
    ``in_progress`` applies only from ``accepted``. ``done`` and ``failed``
    also apply directly from ``accepted``, so a lost ``in_progress`` cannot
    stall the tenant. The late ``in_progress`` is then ignored.
    """
    if incoming not in (TaskStatus.IN_PROGRESS, TaskStatus.DONE, TaskStatus.FAILED):
        raise InvalidProgress(f"status {incoming!r}")
    if current in (TaskStatus.DONE, TaskStatus.FAILED):
        return ProgressDecision(ignore=True)
    if incoming == TaskStatus.IN_PROGRESS:
        if current == TaskStatus.ACCEPTED:
            return ProgressDecision(next_task=TaskStatus.IN_PROGRESS)
        return ProgressDecision(ignore=True)
    if current not in (TaskStatus.ACCEPTED, TaskStatus.IN_PROGRESS):
        return ProgressDecision(ignore=True)
    return ProgressDecision(
        next_task=incoming,
        update_tenant=True,
        next_tenant=_tenant_after_terminal(task_type, incoming),
    )


def _tenant_after_terminal(task_type: TaskType, outcome: TaskStatus) -> TenantStatus:
    failed = outcome == TaskStatus.FAILED
    match task_type:
        case TaskType.DEPLOY | TaskType.UPDATE:
            return TenantStatus.FAILED if failed else TenantStatus.ACTIVE
        case TaskType.DESTROY:
            return TenantStatus.FAILED if failed else TenantStatus.DESTROYED
        case _:
            raise InvalidProgress(f"task type {task_type!r}")


def format_time(value: datetime) -> str:
    """ISO 8601 UTC with milliseconds and a Z suffix."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    utc = value.astimezone(UTC)
    millis = utc.microsecond // 1000
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}Z"


def stable_update_id(task_id: str, status: str) -> str:
    return f"{task_id}:{status}"


def utcnow() -> datetime:
    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
