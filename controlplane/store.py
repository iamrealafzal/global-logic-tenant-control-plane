"""SQLite persistence for tenants, tasks, idempotency keys, and the outbox."""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from controlplane.domain import (
    ProgressDecision,
    ProgressOutcome,
    Task,
    TaskNotFound,
    TaskStatus,
    TaskType,
    Tenant,
    TenantAlreadyExists,
    TenantNotFound,
    TenantStatus,
    TenantUpdateNotAllowed,
    TenantVersionConflict,
    decide_progress,
    ensure_utc,
    expected_tenant_status,
    utcnow,
)
from controlplane.messages import TaskEvent

MIGRATIONS = (
    """
    CREATE TABLE IF NOT EXISTS tenants (
        id TEXT PRIMARY KEY,
        slug TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        version INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        tenant_id TEXT NOT NULL REFERENCES tenants (id),
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS tenants_created_idx ON tenants (created_at, id)",
    "CREATE INDEX IF NOT EXISTS tasks_created_idx ON tasks (created_at, id)",
    """
    CREATE INDEX IF NOT EXISTS tasks_tenant_created_idx
    ON tasks (tenant_id, created_at, id)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS tasks_one_open_per_tenant
    ON tasks (tenant_id)
    WHERE status IN ('accepted', 'in_progress')
    """,
    """
    CREATE TABLE IF NOT EXISTS progress_updates (
        update_id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        status TEXT NOT NULL,
        applied_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS outbox (
        id TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        created_at TEXT NOT NULL,
        published_at TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (created_at)
    WHERE published_at IS NULL
    """,
)


@dataclass(slots=True)
class TenantFilter:
    status: TenantStatus | None = None
    before: datetime | None = None
    before_id: str | None = None
    limit: int = 20


@dataclass(slots=True)
class TaskFilter:
    tenant_id: str | None = None
    status: TaskStatus | None = None
    before: datetime | None = None
    before_id: str | None = None
    limit: int = 20


class Database:
    """One SQLite connection. Writers take an immediate lock so two requests cannot interleave."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.conn: sqlite3.Connection | None = None
        self.lock = asyncio.Lock()

    async def connect(self) -> None:
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        self.conn = conn

    async def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    @asynccontextmanager
    async def transaction(self):
        conn = self.conn
        if conn is None:
            raise RuntimeError("database is not open")
        async with self.lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()


class Store:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def migrate(self) -> None:
        conn = self.db.conn
        if conn is None:
            raise RuntimeError("database is not open")
        async with self.db.lock:
            for statement in MIGRATIONS:
                conn.execute(statement)

    async def ping(self) -> None:
        conn = self.db.conn
        if conn is None:
            raise RuntimeError("database is not open")
        async with self.db.lock:
            conn.execute("SELECT 1")

    async def create_tenant(self, slug: str, name: str) -> tuple[Tenant, Task]:
        """Insert the tenant, its deploy task, and the outbox row together.

        Nothing is published here. A rollback leaves the outbox empty.
        """
        now = utcnow()
        tenant = Tenant(
            id=str(uuid4()),
            slug=slug,
            name=name,
            status=TenantStatus.PROVISIONING,
            version=1,
            created_at=now,
            updated_at=now,
        )
        task = Task(
            id=str(uuid4()),
            type=TaskType.DEPLOY,
            tenant_id=tenant.id,
            status=TaskStatus.ACCEPTED,
            created_at=now,
            updated_at=now,
        )
        async with self.db.transaction() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO tenants (id, slug, name, status, version, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tenant.id,
                        tenant.slug,
                        tenant.name,
                        tenant.status.value,
                        tenant.version,
                        _dump(tenant.created_at),
                        _dump(tenant.updated_at),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                if "tenants.slug" not in str(exc):
                    raise
                raise TenantAlreadyExists from exc
            _insert_task(conn, task)
            _insert_outbox(conn, task, tenant.slug, tenant.name, now)
        return tenant, task

    async def update_tenant(self, tenant_id: str, name: str, version: int) -> tuple[Tenant, Task]:
        now = utcnow()
        async with self.db.transaction() as conn:
            row = _one(
                conn,
                """
                UPDATE tenants
                SET name = ?, status = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND version = ? AND status = ?
                RETURNING id, slug, name, status, version, created_at, updated_at
                """,
                (
                    name,
                    TenantStatus.UPDATING.value,
                    _dump(now),
                    tenant_id,
                    version,
                    TenantStatus.ACTIVE.value,
                ),
            )
            if row is None:
                _explain_update_miss(conn, tenant_id, version)
            tenant = _tenant_from_row(row)
            task = Task(
                id=str(uuid4()),
                type=TaskType.UPDATE,
                tenant_id=tenant.id,
                status=TaskStatus.ACCEPTED,
                created_at=now,
                updated_at=now,
            )
            _insert_task(conn, task)
            _insert_outbox(conn, task, tenant.slug, tenant.name, now)
        return tenant, task

    async def delete_tenant(self, tenant_id: str) -> tuple[Tenant, Task]:
        now = utcnow()
        async with self.db.transaction() as conn:
            row = _one(
                conn,
                """
                UPDATE tenants
                SET status = ?, version = version + 1, updated_at = ?
                WHERE id = ? AND status IN ('active', 'failed')
                RETURNING id, slug, name, status, version, created_at, updated_at
                """,
                (TenantStatus.DESTROYING.value, _dump(now), tenant_id),
            )
            if row is None:
                found = _one(conn, "SELECT status FROM tenants WHERE id = ?", (tenant_id,))
                if found is None:
                    raise TenantNotFound
                raise TenantUpdateNotAllowed
            tenant = _tenant_from_row(row)
            task = Task(
                id=str(uuid4()),
                type=TaskType.DESTROY,
                tenant_id=tenant.id,
                status=TaskStatus.ACCEPTED,
                created_at=now,
                updated_at=now,
            )
            _insert_task(conn, task)
            _insert_outbox(conn, task, tenant.slug, tenant.name, now)
        return tenant, task

    async def get_tenant(self, tenant_id: str) -> Tenant:
        async with self.db.transaction() as conn:
            row = _one(
                conn,
                """
                SELECT id, slug, name, status, version, created_at, updated_at
                FROM tenants WHERE id = ?
                """,
                (tenant_id,),
            )
        if row is None:
            raise TenantNotFound
        return _tenant_from_row(row)

    async def list_tenants(self, filt: TenantFilter) -> list[Tenant]:
        status = filt.status.value if filt.status is not None else None
        before = _dump(filt.before) if filt.before is not None else None
        async with self.db.transaction() as conn:
            rows = _all(
                conn,
                """
                SELECT id, slug, name, status, version, created_at, updated_at
                FROM tenants
                WHERE (? IS NULL OR status = ?)
                  AND (? IS NULL OR (created_at, id) < (?, ?))
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (status, status, before, before, filt.before_id, filt.limit),
            )
        return [_tenant_from_row(row) for row in rows]

    async def get_task(self, task_id: str) -> Task:
        async with self.db.transaction() as conn:
            row = _one(
                conn,
                """
                SELECT id, type, tenant_id, status, created_at, updated_at
                FROM tasks WHERE id = ?
                """,
                (task_id,),
            )
        if row is None:
            raise TaskNotFound
        return _task_from_row(row)

    async def list_tasks(self, filt: TaskFilter) -> list[Task]:
        status = filt.status.value if filt.status is not None else None
        before = _dump(filt.before) if filt.before is not None else None
        async with self.db.transaction() as conn:
            rows = _all(
                conn,
                """
                SELECT id, type, tenant_id, status, created_at, updated_at
                FROM tasks
                WHERE (? IS NULL OR tenant_id = ?)
                  AND (? IS NULL OR status = ?)
                  AND (? IS NULL OR (created_at, id) < (?, ?))
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (
                    filt.tenant_id,
                    filt.tenant_id,
                    status,
                    status,
                    before,
                    before,
                    filt.before_id,
                    filt.limit,
                ),
            )
        return [_task_from_row(row) for row in rows]

    async def apply_progress(
        self, update_id: str, task_id: str, status: TaskStatus
    ) -> ProgressOutcome:
        """Record the idempotency key and apply a legal transition in one transaction.

        The same key commits nothing the second time. A stale update is stored
        and ignored so a retry does not fail and does not rewind the task.
        """
        async with self.db.transaction() as conn:
            task_row = _one(
                conn,
                "SELECT type, status, tenant_id FROM tasks WHERE id = ?",
                (task_id,),
            )
            if task_row is None:
                raise TaskNotFound
            inserted = _one(
                conn,
                """
                INSERT INTO progress_updates (update_id, task_id, status, applied_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (update_id) DO NOTHING
                RETURNING update_id
                """,
                (update_id, task_id, status.value, _dump(utcnow())),
            )
            if inserted is None:
                return ProgressOutcome(duplicate=True)
            decision = decide_progress(
                TaskType(task_row["type"]),
                TaskStatus(task_row["status"]),
                status,
            )
            if decision.ignore:
                return ProgressOutcome(ignored=True)
            now = utcnow()
            changed = conn.execute(
                """
                UPDATE tasks SET status = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (decision.next_task.value, _dump(now), task_id, task_row["status"]),
            )
            if changed.rowcount != 1:
                raise RuntimeError(f"task {task_id} changed concurrently")
            outcome = ProgressOutcome(task_updated=True)
            if decision.update_tenant:
                _apply_tenant_outcome(conn, task_row, decision, now)
                outcome.tenant_updated = True
        return outcome

    async def publish_pending(
        self, publish: Callable[[str, bytes], Awaitable[None]], limit: int = 20
    ) -> int:
        """Publish committed outbox rows and mark them after the broker accepts them.

        The mark shares the transaction that locked the rows. If publishing
        raises, the transaction rolls back and the rows stay unpublished.
        """
        async with self.db.transaction() as conn:
            rows = _all(
                conn,
                """
                SELECT id, payload
                FROM outbox
                WHERE published_at IS NULL
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (limit,),
            )
            now = _dump(utcnow())
            for row in rows:
                await publish(row["id"], row["payload"].encode())
                conn.execute(
                    "UPDATE outbox SET published_at = ? WHERE id = ?",
                    (now, row["id"]),
                )
        return len(rows)

    async def outbox_published(self, task_id: str) -> bool:
        async with self.db.transaction() as conn:
            value = _one(conn, "SELECT published_at FROM outbox WHERE id = ?", (task_id,))
        return value is not None and value["published_at"] is not None

    async def seen_update(self, update_id: str) -> bool:
        async with self.db.transaction() as conn:
            row = _one(
                conn,
                "SELECT 1 FROM progress_updates WHERE update_id = ?",
                (update_id,),
            )
        return row is not None

    async def truncate(self) -> None:
        """Test helper. The server never calls this."""
        async with self.db.transaction() as conn:
            conn.execute("DELETE FROM progress_updates")
            conn.execute("DELETE FROM outbox")
            conn.execute("DELETE FROM tasks")
            conn.execute("DELETE FROM tenants")


def _explain_update_miss(conn: sqlite3.Connection, tenant_id: str, version: int) -> None:
    row = _one(conn, "SELECT status, version FROM tenants WHERE id = ?", (tenant_id,))
    if row is None:
        raise TenantNotFound
    # Version wins over status so two callers holding the same version both
    # observe a conflict, even after the winner has already moved the tenant.
    if int(row["version"]) != version:
        raise TenantVersionConflict
    if row["status"] != TenantStatus.ACTIVE:
        raise TenantUpdateNotAllowed
    raise TenantVersionConflict


def _apply_tenant_outcome(
    conn: sqlite3.Connection,
    task_row: sqlite3.Row,
    decision: ProgressDecision,
    now: datetime,
) -> None:
    expected = expected_tenant_status(TaskType(task_row["type"]))
    changed = conn.execute(
        """
        UPDATE tenants
        SET status = ?, version = version + 1, updated_at = ?
        WHERE id = ? AND status = ?
        """,
        (decision.next_tenant.value, _dump(now), task_row["tenant_id"], expected.value),
    )
    if changed.rowcount != 1:
        raise RuntimeError(f"tenant {task_row['tenant_id']} was not in status {expected.value}")


def _insert_task(conn: sqlite3.Connection, task: Task) -> None:
    conn.execute(
        """
        INSERT INTO tasks (id, type, tenant_id, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            task.id,
            task.type.value,
            task.tenant_id,
            task.status.value,
            _dump(task.created_at),
            _dump(task.updated_at),
        ),
    )


def _insert_outbox(
    conn: sqlite3.Connection, task: Task, slug: str, name: str, now: datetime
) -> None:
    payload = TaskEvent(
        id=task.id,
        type=task.type.value,
        tenant_id=task.tenant_id,
        status=task.status.value,
        tenant_slug=slug,
        tenant_name=name,
    ).encode()
    conn.execute(
        "INSERT INTO outbox (id, payload, created_at) VALUES (?, ?, ?)",
        (task.id, payload.decode(), _dump(now)),
    )


def _tenant_from_row(row: sqlite3.Row) -> Tenant:
    return Tenant(
        id=row["id"],
        slug=row["slug"],
        name=row["name"],
        status=TenantStatus(row["status"]),
        version=int(row["version"]),
        created_at=_load(row["created_at"]),
        updated_at=_load(row["updated_at"]),
    )


def _task_from_row(row: sqlite3.Row) -> Task:
    return Task(
        id=row["id"],
        type=TaskType(row["type"]),
        tenant_id=row["tenant_id"],
        status=TaskStatus(row["status"]),
        created_at=_load(row["created_at"]),
        updated_at=_load(row["updated_at"]),
    )


def _one(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Row | None:
    return conn.execute(sql, params).fetchone()


def _all(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    return list(conn.execute(sql, params).fetchall())


def _dump(value: datetime) -> str:
    return ensure_utc(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _load(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
