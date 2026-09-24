"""Use cases shared by the HTTP API and the progress consumer."""

from __future__ import annotations

import asyncio
import base64
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from controlplane.broker import Broker, PoisonMessage
from controlplane.config import Settings
from controlplane.domain import (
    Task,
    TaskStatus,
    Tenant,
    ValidationError,
    format_time,
    known_task_status,
    known_tenant_status,
    validate_name,
    validate_slug,
    validate_version,
)
from controlplane.messages import decode_progress
from controlplane.store import Store, TaskFilter, TenantFilter

log = logging.getLogger(__name__)


@dataclass(slots=True)
class Page:
    tenants: list[Tenant]
    tasks: list[Task]
    next_cursor: str = ""


class Service:
    def __init__(self, store: Store, broker: Broker, settings: Settings) -> None:
        self.store = store
        self.broker = broker
        self.settings = settings

    async def healthy(self) -> None:
        await self.store.ping()
        await self.broker.ping()

    async def create_tenant(self, slug: str, name: str) -> tuple[Tenant, Task]:
        validate_slug(slug)
        name = validate_name(name)
        tenant, task = await self.store.create_tenant(slug, name)
        await self._publish_after_commit(task.id)
        return tenant, task

    async def update_tenant(self, tenant_id: str, name: str, version: int) -> tuple[Tenant, Task]:
        _parse_id(tenant_id)
        name = validate_name(name)
        validate_version(version)
        tenant, task = await self.store.update_tenant(tenant_id, name, version)
        await self._publish_after_commit(task.id)
        return tenant, task

    async def delete_tenant(self, tenant_id: str) -> tuple[Tenant, Task]:
        _parse_id(tenant_id)
        tenant, task = await self.store.delete_tenant(tenant_id)
        await self._publish_after_commit(task.id)
        return tenant, task

    async def get_tenant(self, tenant_id: str) -> Tenant:
        _parse_id(tenant_id)
        return await self.store.get_tenant(tenant_id)

    async def list_tenants(self, status: str, cursor: str, limit: int) -> Page:
        filt = TenantFilter(limit=limit + 1)
        if status:
            filt.status = known_tenant_status(status)
        if cursor:
            filt.before, filt.before_id = decode_cursor(cursor)
        items = await self.store.list_tenants(filt)
        return _page_tenants(items, limit)

    async def get_task(self, task_id: str) -> Task:
        _parse_id(task_id)
        return await self.store.get_task(task_id)

    async def list_tasks(self, tenant_id: str, status: str, cursor: str, limit: int) -> Page:
        filt = TaskFilter(limit=limit + 1)
        if tenant_id:
            try:
                _parse_id(tenant_id)
            except ValidationError as exc:
                raise ValidationError("tenant_id must be a uuid") from exc
            filt.tenant_id = tenant_id
        if status:
            filt.status = known_task_status(status)
        if cursor:
            filt.before, filt.before_id = decode_cursor(cursor)
        items = await self.store.list_tasks(filt)
        return _page_tasks(items, limit)

    async def publish_pending(self) -> int:
        async def publish(task_id: str, payload: bytes) -> None:
            await self.broker.publish(self.settings.accepted_subject, task_id, payload)

        return await self.store.publish_pending(publish)

    async def run_outbox(self) -> None:
        while True:
            try:
                await self.publish_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("outbox relay")
            await asyncio.sleep(self.settings.outbox_interval)

    async def run_progress(self) -> None:
        while True:
            try:
                await self.broker.consume(
                    "controlplane",
                    self.settings.progress_subject,
                    self.apply_delivery,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("progress consumer stopped")
                await asyncio.sleep(1)

    async def apply_delivery(self, payload: bytes) -> None:
        try:
            event = decode_progress(payload)
        except Exception as exc:
            raise PoisonMessage(str(exc)) from exc
        outcome = await self.store.apply_progress(
            event.update_id, event.task_id, TaskStatus(event.status)
        )
        log.info(
            "progress update_id=%s task_id=%s status=%s duplicate=%s ignored=%s tenant=%s",
            event.update_id,
            event.task_id,
            event.status,
            outcome.duplicate,
            outcome.ignored,
            outcome.tenant_updated,
        )

    async def _publish_after_commit(self, task_id: str) -> None:
        # The row is already committed. A broker failure is retried by the relay.
        try:
            await self.publish_pending()
        except Exception:
            log.exception("publish after commit task_id=%s", task_id)


def tenant_json(tenant: Tenant) -> dict:
    return {
        "id": tenant.id,
        "slug": tenant.slug,
        "name": tenant.name,
        "status": tenant.status.value,
        "version": tenant.version,
        "created_at": format_time(tenant.created_at),
        "updated_at": format_time(tenant.updated_at),
    }


def task_json(task: Task) -> dict:
    return {
        "id": task.id,
        "type": task.type.value,
        "tenant_id": task.tenant_id,
        "status": task.status.value,
        "created_at": format_time(task.created_at),
        "updated_at": format_time(task.updated_at),
    }


def mutation_json(tenant: Tenant, task: Task) -> dict:
    return {"tenant": tenant_json(tenant), "task": task_json(task)}


def encode_cursor(created_at: datetime, entity_id: str) -> str:
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    stamp = created_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    raw = f"{stamp}|{entity_id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(token: str) -> tuple[datetime, str]:
    padded = token + "=" * (-len(token) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded.encode()).decode()
        stamp, entity_id = decoded.split("|", 1)
        created_at = datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
        UUID(entity_id)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValidationError("invalid cursor") from exc
    return created_at, entity_id


def parse_limit(raw: str | None) -> int:
    if raw is None or raw == "":
        return 20
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValidationError("limit must be between 1 and 100") from exc
    if value < 1 or value > 100:
        raise ValidationError("limit must be between 1 and 100")
    return value


def _parse_id(value: str) -> None:
    try:
        UUID(value)
    except ValueError as exc:
        raise ValidationError("id must be a uuid") from exc


def _page_tenants(items: list[Tenant], limit: int) -> Page:
    if len(items) <= limit:
        return Page(tenants=items, tasks=[])
    page = items[:limit]
    last = page[-1]
    return Page(tenants=page, tasks=[], next_cursor=encode_cursor(last.created_at, last.id))


def _page_tasks(items: list[Task], limit: int) -> Page:
    if len(items) <= limit:
        return Page(tenants=[], tasks=items)
    page = items[:limit]
    last = page[-1]
    return Page(tenants=[], tasks=page, next_cursor=encode_cursor(last.created_at, last.id))
