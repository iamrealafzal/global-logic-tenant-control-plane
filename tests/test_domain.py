from datetime import datetime, timedelta, timezone

import pytest
from controlplane.domain import (
    InvalidProgress,
    TaskStatus,
    TaskType,
    TenantStatus,
    ValidationError,
    can_delete,
    can_update,
    decide_progress,
    format_time,
    validate_name,
    validate_slug,
)


def test_validate_slug() -> None:
    ok = ["abc", "a-b", "a--b", "ns-1", "a123456789012345678901234567"]
    bad = ["", "ab", "A", "Abc", "-ab", "ab-", "a_b", "1ab", "aa"]
    assert len(ok[-1]) == 28
    for slug in ok:
        validate_slug(slug)
    for slug in bad:
        with pytest.raises(ValidationError):
            validate_slug(slug)
    with pytest.raises(ValidationError):
        validate_slug("a1234567890123456789012345678")


def test_validate_name() -> None:
    assert validate_name("  Acme  ") == "Acme"
    with pytest.raises(ValidationError):
        validate_name("   ")


def test_decide_progress() -> None:
    cases = [
        (
            TaskType.DEPLOY,
            TaskStatus.ACCEPTED,
            TaskStatus.IN_PROGRESS,
            False,
            TaskStatus.IN_PROGRESS,
            None,
            False,
        ),
        (
            TaskType.DEPLOY,
            TaskStatus.IN_PROGRESS,
            TaskStatus.DONE,
            False,
            TaskStatus.DONE,
            TenantStatus.ACTIVE,
            True,
        ),
        (
            TaskType.DEPLOY,
            TaskStatus.IN_PROGRESS,
            TaskStatus.FAILED,
            False,
            TaskStatus.FAILED,
            TenantStatus.FAILED,
            True,
        ),
        (
            TaskType.DEPLOY,
            TaskStatus.ACCEPTED,
            TaskStatus.DONE,
            False,
            TaskStatus.DONE,
            TenantStatus.ACTIVE,
            True,
        ),
        (
            TaskType.UPDATE,
            TaskStatus.IN_PROGRESS,
            TaskStatus.DONE,
            False,
            TaskStatus.DONE,
            TenantStatus.ACTIVE,
            True,
        ),
        (
            TaskType.UPDATE,
            TaskStatus.IN_PROGRESS,
            TaskStatus.FAILED,
            False,
            TaskStatus.FAILED,
            TenantStatus.FAILED,
            True,
        ),
        (
            TaskType.DESTROY,
            TaskStatus.IN_PROGRESS,
            TaskStatus.DONE,
            False,
            TaskStatus.DONE,
            TenantStatus.DESTROYED,
            True,
        ),
        (
            TaskType.DESTROY,
            TaskStatus.ACCEPTED,
            TaskStatus.FAILED,
            False,
            TaskStatus.FAILED,
            TenantStatus.FAILED,
            True,
        ),
        (TaskType.DEPLOY, TaskStatus.IN_PROGRESS, TaskStatus.IN_PROGRESS, True, None, None, False),
        (TaskType.DEPLOY, TaskStatus.DONE, TaskStatus.IN_PROGRESS, True, None, None, False),
        (TaskType.DEPLOY, TaskStatus.DONE, TaskStatus.FAILED, True, None, None, False),
        (TaskType.DESTROY, TaskStatus.FAILED, TaskStatus.DONE, True, None, None, False),
    ]
    for task_type, current, incoming, ignore, next_task, next_tenant, update_tenant in cases:
        got = decide_progress(task_type, current, incoming)
        assert got.ignore is ignore
        assert got.next_task == next_task
        assert got.next_tenant == next_tenant
        assert got.update_tenant is update_tenant
    with pytest.raises(InvalidProgress):
        decide_progress(TaskType.DEPLOY, TaskStatus.ACCEPTED, "nope")  # type: ignore[arg-type]


def test_guards() -> None:
    assert can_update(TenantStatus.ACTIVE)
    assert not can_update(TenantStatus.PROVISIONING)
    assert not can_update(TenantStatus.FAILED)
    assert can_delete(TenantStatus.ACTIVE)
    assert can_delete(TenantStatus.FAILED)
    assert not can_delete(TenantStatus.DESTROYING)
    assert not can_delete(TenantStatus.DESTROYED)


def test_format_time() -> None:
    ist = timezone(timedelta(hours=5, minutes=30))
    moment = datetime(2026, 9, 24, 7, 8, 9, 123000, ist)
    assert format_time(moment) == "2026-09-24T01:38:09.123Z"
