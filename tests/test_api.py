from __future__ import annotations

import asyncio
import json
import socket
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import uuid4

import httpx
import nats
import pytest
import uvicorn
from controlplane.app import create_app
from controlplane.config import Settings
from controlplane.messages import TaskEvent
from controlplane.store import Database, Store
from controlplane.worker import Config, run
from tests.conftest import Infra


class _Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        return


@dataclass
class Env:
    base: str
    settings: Settings
    store: Store
    accepted: asyncio.Queue[bytes]
    app: object

    async def request(self, method: str, path: str, body: dict | None = None) -> httpx.Response:
        async with httpx.AsyncClient(base_url=self.base, timeout=5) as client:
            return await client.request(method, path, json=body)

    async def create(self, slug: str, name: str) -> dict:
        response = await self.request("POST", "/v1/tenants", {"slug": slug, "name": name})
        assert response.status_code == 201, response.text
        return response.json()

    async def progress(self, task_id: str, status: str, update_id: str = "") -> None:
        if update_id == "":
            update_id = f"{task_id}:{status}"
        payload = json.dumps(
            {"update_id": update_id, "task_id": task_id, "status": status}
        ).encode()
        await self.app.state.service.broker.publish(
            self.settings.progress_subject, update_id, payload
        )

    async def wait_tenant(self, tenant_id: str, status: str) -> dict:
        last = {}
        for _ in range(100):
            response = await self.request("GET", f"/v1/tenants/{tenant_id}")
            if response.status_code == 200:
                last = response.json()
                if last["status"] == status:
                    return last
            await asyncio.sleep(0.05)
        pytest.fail(f"tenant {tenant_id} did not reach {status}: {last}")

    async def wait_task(self, task_id: str, status: str) -> dict:
        last = {}
        for _ in range(100):
            response = await self.request("GET", f"/v1/tasks/{task_id}")
            assert response.status_code == 200, response.text
            last = response.json()
            if last["status"] == status:
                return last
            await asyncio.sleep(0.05)
        pytest.fail(f"task {task_id} did not reach {status}: {last}")

    async def wait_seen(self, update_id: str) -> None:
        for _ in range(100):
            if await self.store.seen_update(update_id):
                return
            await asyncio.sleep(0.05)
        pytest.fail(f"update {update_id} was not recorded")


def _port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@asynccontextmanager
async def stack(infra: Infra, *, reset: bool = True):
    prefix = "t" + uuid4().hex
    stream = "S" + uuid4().hex[:12]
    db = Database(infra.database_path)
    await db.connect()
    store = Store(db)
    await store.migrate()
    if reset:
        await store.truncate()
    settings = Settings(
        http_addr="127.0.0.1:0",
        database_path=infra.database_path,
        nats_url=infra.nats_url,
        outbox_interval=0.05,
        subject_prefix=prefix,
        stream_name=stream,
    )
    connection = await nats.connect(infra.nats_url)
    accepted: asyncio.Queue[bytes] = asyncio.Queue()

    async def on_accepted(message) -> None:
        accepted.put_nowait(message.data)

    await connection.subscribe(settings.accepted_subject, cb=on_accepted)
    await connection.flush()
    app = create_app(settings)
    port = _port()
    server = _Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    )
    task = asyncio.create_task(server.serve())
    base = f"http://127.0.0.1:{port}"
    try:
        await _wait_ready(server, base, task)
        yield Env(base, settings, store, accepted, app)
    finally:
        server.should_exit = True
        await task
        await connection.close()
        await db.close()


async def _wait_ready(server: _Server, base: str, task: asyncio.Task[None]) -> None:
    async with httpx.AsyncClient(timeout=1) as client:
        for _ in range(100):
            if task.done():
                raise RuntimeError(f"server exited: {task.exception()}")
            try:
                response = await client.get(base + "/healthz")
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.05)
    raise RuntimeError("server did not become healthy")


@pytest.mark.asyncio
async def test_deploy_lifecycle_and_message_shape(infra: Infra) -> None:
    async with stack(infra) as env:
        created = await env.create("acme", "Acme")
        assert created["tenant"]["status"] == "provisioning"
        assert created["tenant"]["version"] == 1
        assert created["task"]["type"] == "deploy"
        assert created["task"]["status"] == "accepted"
        assert created["tenant"]["created_at"].endswith("Z")
        raw = await asyncio.wait_for(env.accepted.get(), timeout=3)
        event = json.loads(raw)
        assert event["id"] == created["task"]["id"]
        assert event["type"] == "deploy"
        assert event["tenant_id"] == created["tenant"]["id"]
        assert event["status"] == "accepted"
        await env.progress(created["task"]["id"], "in_progress")
        task = await env.wait_task(created["task"]["id"], "in_progress")
        tenant = await env.wait_tenant(created["tenant"]["id"], "provisioning")
        assert task["status"] == "in_progress"
        assert tenant["version"] == 1
        await env.progress(created["task"]["id"], "done")
        active = await env.wait_tenant(created["tenant"]["id"], "active")
        assert active["version"] == 2
        assert (await env.wait_task(created["task"]["id"], "done"))["status"] == "done"


@pytest.mark.asyncio
async def test_duplicate_and_stale_progress_do_not_regress(infra: Infra) -> None:
    async with stack(infra) as env:
        created = await env.create("stale", "Stale")
        done_id = created["task"]["id"] + ":done"
        await env.progress(created["task"]["id"], "done", done_id)
        active = await env.wait_tenant(created["tenant"]["id"], "active")
        stale_id = created["task"]["id"] + ":late-running"
        late_failed = created["task"]["id"] + ":late-failed"
        await env.progress(created["task"]["id"], "done", done_id)
        await env.progress(created["task"]["id"], "in_progress", stale_id)
        await env.progress(created["task"]["id"], "failed", late_failed)
        await env.wait_seen(stale_id)
        await env.wait_seen(late_failed)
        got = await env.wait_tenant(created["tenant"]["id"], "active")
        assert got["version"] == active["version"]
        assert (await env.wait_task(created["task"]["id"], "done"))["status"] == "done"


@pytest.mark.asyncio
async def test_done_before_in_progress(infra: Infra) -> None:
    async with stack(infra) as env:
        created = await env.create("reorder", "Reorder")
        await env.progress(created["task"]["id"], "done")
        await env.wait_tenant(created["tenant"]["id"], "active")
        late = created["task"]["id"] + ":in_progress"
        await env.progress(created["task"]["id"], "in_progress", late)
        await env.wait_seen(late)
        assert (await env.wait_task(created["task"]["id"], "done"))["status"] == "done"


@pytest.mark.asyncio
async def test_poison_message_does_not_stall_consumer(infra: Infra) -> None:
    async with stack(infra) as env:
        created = await env.create("poison", "Poison")
        await env.app.state.service.broker.publish(env.settings.progress_subject, "bad-json", b"{")
        await env.progress(created["task"]["id"], "done")
        await env.wait_tenant(created["tenant"]["id"], "active")


@pytest.mark.asyncio
async def test_update_and_destroy_lifecycle(infra: Infra) -> None:
    async with stack(infra) as env:
        created = await env.create("lifecycle", "Lifecycle")
        await env.progress(created["task"]["id"], "done")
        active = await env.wait_tenant(created["tenant"]["id"], "active")
        response = await env.request(
            "PATCH",
            f"/v1/tenants/{created['tenant']['id']}",
            {"name": "Renamed", "version": active["version"]},
        )
        assert response.status_code == 202, response.text
        updated = response.json()
        assert updated["tenant"]["status"] == "updating"
        assert updated["tenant"]["name"] == "Renamed"
        assert updated["tenant"]["version"] == active["version"] + 1
        assert updated["task"]["type"] == "update"
        await env.progress(updated["task"]["id"], "failed")
        failed = await env.wait_tenant(created["tenant"]["id"], "failed")
        denied = await env.request(
            "PATCH",
            f"/v1/tenants/{created['tenant']['id']}",
            {"name": "Again", "version": failed["version"]},
        )
        assert denied.status_code == 409
        assert denied.json()["code"] == "tenant_update_not_allowed"
        deleted = await env.request("DELETE", f"/v1/tenants/{created['tenant']['id']}")
        assert deleted.status_code == 202, deleted.text
        body = deleted.json()
        assert body["tenant"]["status"] == "destroying"
        assert body["task"]["type"] == "destroy"
        await env.progress(body["task"]["id"], "done")
        destroyed = await env.wait_tenant(created["tenant"]["id"], "destroyed")
        assert destroyed["version"] > failed["version"]
        reused = await env.request("POST", "/v1/tenants", {"slug": "lifecycle", "name": "Again"})
        assert reused.status_code == 409
        assert reused.json()["code"] == "tenant_already_exists"


@pytest.mark.asyncio
async def test_guards_and_validation(infra: Infra) -> None:
    async with stack(infra) as env:
        created = await env.create("guards", "Guards")
        denied = await env.request(
            "PATCH", f"/v1/tenants/{created['tenant']['id']}", {"name": "Nope", "version": 1}
        )
        assert denied.status_code == 409
        assert denied.json()["code"] == "tenant_update_not_allowed"
        conflict = await env.request(
            "PATCH", f"/v1/tenants/{created['tenant']['id']}", {"name": "Nope", "version": 99}
        )
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "tenant_version_conflict"
        blocked = await env.request("DELETE", f"/v1/tenants/{created['tenant']['id']}")
        assert blocked.status_code == 409
        assert blocked.json()["code"] == "tenant_update_not_allowed"
        missing = "00000000-0000-4000-8000-000000000000"
        not_found = await env.request("GET", f"/v1/tenants/{missing}")
        assert not_found.status_code == 404
        assert not_found.json()["code"] == "tenant_not_found"
        missing_task = await env.request("GET", f"/v1/tasks/{missing}")
        assert missing_task.status_code == 404
        assert missing_task.json()["code"] == "task_not_found"
        bad_slug = await env.request("POST", "/v1/tenants", {"slug": "NO", "name": "X"})
        assert bad_slug.status_code == 400
        assert bad_slug.json()["code"] == "validation_error"
        blank = await env.request("POST", "/v1/tenants", {"slug": "ok-name", "name": "   "})
        assert blank.status_code == 400
        assert blank.json()["code"] == "validation_error"
        route = await env.request("GET", "/nope")
        assert route.status_code == 404
        assert route.json()["code"] == "not_found"


@pytest.mark.asyncio
async def test_concurrent_create_and_patch(infra: Infra) -> None:
    async with stack(infra) as env:

        async def create_once() -> httpx.Response:
            return await env.request("POST", "/v1/tenants", {"slug": "same-slug", "name": "Same"})

        created = await asyncio.gather(*[create_once() for _ in range(16)])
        assert sum(item.status_code == 201 for item in created) == 1
        assert (
            sum(
                item.status_code == 409 and item.json()["code"] == "tenant_already_exists"
                for item in created
            )
            == 15
        )
        listed = await env.request("GET", "/v1/tenants")
        tenants = listed.json()["tenants"]
        assert len(tenants) == 1
        tenant = tenants[0]
        tasks = await env.request("GET", "/v1/tasks", None)
        # request() only sends json when body is not None. Pass query via a direct client call.
        async with httpx.AsyncClient(base_url=env.base) as client:
            tasks = await client.get("/v1/tasks", params={"tenant_id": tenant["id"]})
        assert tasks.status_code == 200
        assert len(tasks.json()["tasks"]) == 1
        await env.progress(tasks.json()["tasks"][0]["id"], "done")
        active = await env.wait_tenant(tenant["id"], "active")

        async def patch_once() -> httpx.Response:
            return await env.request(
                "PATCH",
                f"/v1/tenants/{tenant['id']}",
                {"name": "Raced", "version": active["version"]},
            )

        patched = await asyncio.gather(*[patch_once() for _ in range(16)])
        assert sum(item.status_code == 202 for item in patched) == 1
        assert (
            sum(
                item.status_code == 409 and item.json()["code"] == "tenant_version_conflict"
                for item in patched
            )
            == 15
        )


@pytest.mark.asyncio
async def test_pagination_and_new_connection(infra: Infra) -> None:
    async with stack(infra) as env:
        ids = [
            (await env.create(slug, slug))["tenant"]["id"]
            for slug in ("page-a", "page-b", "page-c")
        ]
        seen: set[str] = set()
        cursor = ""
        async with httpx.AsyncClient(base_url=env.base) as client:
            for _ in range(5):
                params: dict[str, str | int] = {"limit": 2}
                if cursor:
                    params["cursor"] = cursor
                response = await client.get("/v1/tenants", params=params)
                assert response.status_code == 200, response.text
                page = response.json()
                assert 1 <= len(page["tenants"]) <= 2
                for tenant in page["tenants"]:
                    assert tenant["id"] not in seen
                    seen.add(tenant["id"])
                cursor = page.get("next_cursor", "")
                if cursor == "":
                    break
        assert len(seen) == 3
        other = Database(infra.database_path)
        await other.connect()
        try:
            reopened = Store(other)
            tenant = await reopened.get_tenant(ids[0])
            assert tenant.slug == "page-a"
        finally:
            await other.close()


@pytest.mark.asyncio
async def test_worker_success_and_failure(infra: Infra) -> None:
    async with stack(infra) as env:
        worker = asyncio.create_task(
            run(
                Config(
                    nats_url=infra.nats_url,
                    stream=env.settings.stream_name,
                    prefix=env.settings.subject_prefix,
                    fail_rate=0,
                    min_delay=0,
                    max_delay=0,
                )
            )
        )
        created = await env.create("worker-ok", "Worker")
        await env.wait_tenant(created["tenant"]["id"], "active")
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

        failing = asyncio.create_task(
            run(
                Config(
                    nats_url=infra.nats_url,
                    stream=env.settings.stream_name,
                    prefix=env.settings.subject_prefix,
                    fail_rate=1,
                    min_delay=0,
                    max_delay=0,
                )
            )
        )
        try:
            bad = await env.create("worker-bad", "Worker")
            await env.wait_tenant(bad["tenant"]["id"], "failed")
        finally:
            failing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await failing


@pytest.mark.asyncio
async def test_outbox_is_unpublished_until_relay_commits(infra: Infra) -> None:
    db = Database(infra.database_path)
    await db.connect()
    store = Store(db)
    await store.migrate()
    await store.truncate()
    _tenant, task = await store.create_tenant("outbox-a", "Outbox")
    assert await store.outbox_published(task.id) is False
    try:
        async with db.transaction() as conn:
            conn.execute(
                "UPDATE outbox SET published_at = ? WHERE id = ?",
                ("2026-09-24T00:00:00.000000Z", task.id),
            )
            raise RuntimeError("rollback")
    except RuntimeError:
        pass
    assert await store.outbox_published(task.id) is False
    async with db.transaction() as conn:
        row = conn.execute("SELECT payload FROM outbox WHERE id = ?", (task.id,)).fetchone()
    event = json.loads(row["payload"])
    assert event["id"] == task.id
    assert event["status"] == "accepted"
    assert event["type"] == "deploy"
    assert event["tenant_id"]
    await db.close()

    async with stack(infra, reset=False) as env:
        for _ in range(100):
            if await env.store.seen_update("unused"):
                break
            if await env.store.outbox_published(task.id):
                break
            await asyncio.sleep(0.05)
        assert await env.store.outbox_published(task.id) is True
        raw = await asyncio.wait_for(env.accepted.get(), timeout=3)
        published = TaskEvent(
            **{key: json.loads(raw)[key] for key in TaskEvent.__dataclass_fields__}
        )
        assert published.id == task.id
