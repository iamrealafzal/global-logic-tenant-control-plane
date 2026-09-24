# How the control plane is implemented

HTTP writes tenants and tasks into SQLite first. A NATS JetStream message is published only after that commit. A worker then reports progress. A crash, a redelivery, or two racing requests cannot invent a tenant, rewind a task, or apply the same update twice.

## What a request does

`POST /v1/tenants`, `PATCH`, and `DELETE` follow the same path.

1. `controlplane/api.py` validates the JSON body and calls the use case.
2. `controlplane/service.py` checks slug, name, id, and version, then calls the store.
3. `controlplane/store.py` writes the tenant change, the task, and an outbox row in one SQLite transaction, then commits.
4. Only after that commit does the service try to publish. A background relay retries anything still unpublished.

```python
async def create_tenant(self, slug: str, name: str) -> tuple[Tenant, Task]:
    validate_slug(slug)
    name = validate_name(name)
    tenant, task = await self.store.create_tenant(slug, name)
    await self._publish_after_commit(task.id)
    return tenant, task
```

`create_tenant`, `update_tenant`, and `delete_tenant` in `store.py` each open one transaction, insert the tenant change, insert the task, and insert the outbox row. The transaction commits when the `async with` block exits. The broker is not called inside it.

`controlplane/app.py` owns the process. On startup it migrates SQLite, connects to JetStream, and starts two tasks: `run_outbox` and `run_progress`.

## Publish after commit

Publishing and then rolling back the database would hand the worker a task that does not exist. The order here is the reverse.

The outbox row is the intent to publish. It is written in the same transaction as the tenant and the task (`_insert_outbox` in `store.py`). If that transaction rolls back, there is no row and nothing to publish.

`publish_pending` reads only rows whose `published_at` is null, publishes them, and sets `published_at` in that same transaction. If the broker raises, the transaction rolls back and `published_at` stays null. The relay in `service.run_outbox` retries on an interval. A crash after the broker ack but before the mark publishes the same task again. That duplicate is what the rest of the design tolerates.

`_publish_after_commit` is a fast path. If it fails, the request still returns success because the row is already committed. The relay is what makes the publish durable.

`tests/test_api.py` (`test_outbox_is_unpublished_until_relay_commits`) checks that a rolled-back mark leaves the row unpublished.

## At-least-once delivery

JetStream is configured for at-least-once. `controlplane/broker.py` creates stream `PROVISIONING` on subjects `tasks.>`, with a 2-minute `Nats-Msg-Id` dedup window. That window only collapses duplicates that arrive close together. It is not the correctness boundary.

The durable boundary is in SQLite.

**Accepted tasks.** The outbox id and the JetStream message id are both the task id (`broker.publish` sends `Nats-Msg-Id`). A second publish of the same task is the same message. The worker still has to be safe if it runs twice.

**Progress.** The worker in `controlplane/worker.py` publishes `tasks.progress` with a stable key from `domain.stable_update_id`: `<task-id>:in_progress`, `<task-id>:done`, or `<task-id>:failed`. A redelivery emits the same keys.

`store.apply_progress` inserts that key into `progress_updates` with `ON CONFLICT DO NOTHING`. The second delivery commits nothing and returns `duplicate=True`. A new key that would move a finished task, or repeat `in_progress`, is stored and ignored so the retry is acked and does not rewind state.

The legal moves live in `domain.decide_progress`:

| Current task | Incoming | Result |
| --- | --- | --- |
| `accepted` | `in_progress` | task only |
| `accepted` or `in_progress` | `done` or `failed` | task and tenant |
| `done` or `failed` | anything | ignore |
| already `in_progress` | `in_progress` | ignore |

A lost `in_progress` cannot stall the tenant, because `done` and `failed` are accepted straight from `accepted`. The first terminal result wins. A later message with a different outcome is ignored.

The consumer in `broker._settle` acks only after the handler succeeds. A malformed payload (`PoisonMessage`, from `messages.decode_progress`) is terminated immediately. A broker or database error is nacked with backoff and terminated after repeated delivery, so one bad message does not stop the loop. `service.run_progress` reconnects if the subscription drops.

## Where the races are closed

**Same slug, twice.** `tenants.slug` is `UNIQUE`. Both creates run under `BEGIN IMMEDIATE` plus an `asyncio.Lock` on the single connection (`Database.transaction`). One insert wins. The other raises `TenantAlreadyExists` (HTTP 409). Slugs stay unique after `destroyed`.

**Same version, twice.** `PATCH` updates only where `id`, `version`, and `status = 'active'` all match, and increments `version` in that statement. The loser sees the new version and gets `tenant_version_conflict`. A partial unique index, `tasks_one_open_per_tenant`, blocks a second open task for the same tenant.

**Progress versus redelivery.** Covered by `test_duplicate_and_stale_progress_do_not_regress` and `test_done_before_in_progress`.

The HTTP races are proven by a script and a test:

- `scripts/concurrency.sh` fires 16 creates of one slug against a running stack, waits until the winner is `active`, then fires 16 patches of that same version. It exits only if each batch has exactly one success.
- `tests/test_api.py` `test_concurrent_create_and_patch` does the same 16-and-16 check in-process and also asserts the loser codes (`tenant_already_exists` and `tenant_version_conflict`) and that only one tenant and one task were stored.

## Where each piece lives

| File | Role |
| --- | --- |
| `controlplane/api.py` | Routes and `{code, message}` errors |
| `controlplane/service.py` | Use cases, cursor paging, outbox relay, progress consumer |
| `controlplane/store.py` | Schema, transactions, outbox, idempotency keys |
| `controlplane/domain.py` | State machine and validation |
| `controlplane/broker.py` | JetStream publish, pull consume, ack / nak / term |
| `controlplane/messages.py` | `tasks.accepted` and `tasks.progress` JSON |
| `controlplane/worker.py` | Simulator: `in_progress`, then `done` or `failed` |
| `controlplane/app.py` | Process startup, the two background loops |
| `controlplane/publish.py` | One-shot progress publisher used by `scripts/publish-progress.sh` |
| `tests/test_api.py` | Lifecycle, duplicates, poison messages, concurrency, outbox |
| `tests/test_domain.py` | Slug, name, and state-machine cases with no I/O |

Tenant status follows the task. Create stores `provisioning` plus a `deploy` task. `done` moves it to `active`; `failed` moves it to `failed`. Update and destroy do the same from `updating` and `destroying`. `PATCH` is legal only from `active`. `DELETE` is legal from `active` or `failed`. Version increments on every tenant mutation, including a terminal worker result. `in_progress` changes the task only.
