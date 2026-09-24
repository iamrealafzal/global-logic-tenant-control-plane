# Testing handbook

Run these checks from the repository root. The stack must already be up:

```bash
docker compose up --build --wait
```

That starts NATS JetStream, the API on port 8080, and the worker. The worker default is success (`FAIL_RATE=0`).

## 1. Health

```bash
curl -sS localhost:8080/healthz
```

Expect HTTP 200. The body reports that SQLite and NATS are reachable.

## 2. Happy path

```bash
curl -sS -X POST localhost:8080/v1/tenants \
  -H 'content-type: application/json' \
  -d '{"slug":"acme","name":"Acme"}'
```

Success is `201`. The body contains a tenant with `status` `provisioning` and a `deploy` task with `status` `accepted`. Copy the tenant id and the task id.

```bash
curl -sS localhost:8080/v1/tenants/<tenant-id>
curl -sS "localhost:8080/v1/tasks?tenant_id=<tenant-id>"
curl -sS localhost:8080/v1/tasks/<task-id>
```

Within a couple of seconds the task moves `accepted` → `in_progress` → `done`, and the tenant moves `provisioning` → `active`. Tenant `version` increments when the deploy finishes.

## 3. Worked example: tenant `acme2`

This tenant was created against the running stack:

| Field | Value |
| --- | --- |
| id | `055e89a3-2b5c-4442-9034-ec2968e9bbac` |
| slug | `acme2` |
| name | `Acme2` |
| status | `active` |
| version | `2` |
| deploy task | `fdd632d6-cf09-4025-b03a-de0095b32b05` (`done`) |

Read it again before mutating. The `version` in each `PATCH` must be the version you just read. The commands below assume it is still version `2` and `active`.

```bash
curl -sS localhost:8080/v1/tenants/055e89a3-2b5c-4442-9034-ec2968e9bbac
```

### Rename

`PATCH` is legal only from `active`.

```bash
curl -sS -X PATCH localhost:8080/v1/tenants/055e89a3-2b5c-4442-9034-ec2968e9bbac \
  -H 'content-type: application/json' \
  -d '{"name":"Renamed","version":2}'
```

Success is `202`. The tenant moves to `updating`. Poll until the worker finishes:

```bash
curl -sS localhost:8080/v1/tenants/055e89a3-2b5c-4442-9034-ec2968e9bbac
```

The name is `Renamed`, the status is `active`, and the version is `3`.

### Stale version

```bash
curl -sS -X PATCH localhost:8080/v1/tenants/055e89a3-2b5c-4442-9034-ec2968e9bbac \
  -H 'content-type: application/json' \
  -d '{"name":"Nope","version":2}'
```

The body is `{"code":"tenant_version_conflict","message":...}` and the status is `409`.

### Delete

`DELETE` is legal from `active` or `failed`.

```bash
curl -sS -X DELETE localhost:8080/v1/tenants/055e89a3-2b5c-4442-9034-ec2968e9bbac
```

Success is `202`. Poll the same `GET` until `status` is `destroyed`.

Creating `acme2` again still returns `409` and `tenant_already_exists`. Slugs stay unique, including tenants that are already `destroyed`.

```bash
curl -sS -X POST localhost:8080/v1/tenants \
  -H 'content-type: application/json' \
  -d '{"slug":"acme2","name":"Acme2"}'
```

## 4. Update and delete on any active tenant

Replace `<tenant-id>` and `<version>` with the values from `GET`.

```bash
curl -sS -X PATCH localhost:8080/v1/tenants/<tenant-id> \
  -H 'content-type: application/json' \
  -d '{"name":"Renamed","version":<version>}'
```

Expect `202`. Poll until `status` is `active` and `name` is `Renamed`.

```bash
curl -sS -X DELETE localhost:8080/v1/tenants/<tenant-id>
```

Expect `202`. Poll until `status` is `destroyed`.

## 5. Conflicts and validation

Same slug again, including after destroy:

```bash
curl -sS -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/v1/tenants \
  -H 'content-type: application/json' \
  -d '{"slug":"acme","name":"Acme"}'
```

Expect `409` and `tenant_already_exists`.

Wrong version on an active tenant:

```bash
curl -sS -X PATCH localhost:8080/v1/tenants/<tenant-id> \
  -H 'content-type: application/json' \
  -d '{"name":"Nope","version":1}'
```

Expect `409` and `tenant_version_conflict`.

A bad slug, a missing name, or a bad id returns `400` and `validation_error`. An unknown tenant id returns `404` and `tenant_not_found`.

`PATCH` on a tenant that is `provisioning`, `updating`, `destroying`, `failed`, or `destroyed` returns `409` and `tenant_update_not_allowed`. `DELETE` is rejected the same way unless the status is `active` or `failed`.

## 6. Sixteen creates and sixteen patches

The worker must be in success mode. Exactly one create and one patch succeed. The rest are `409`.

```bash
chmod +x scripts/*.sh
./scripts/concurrency.sh
```

The last line is `concurrent create and patch each produced exactly one success`.

## 7. Forced failure, then success

```bash
FAIL_RATE=1 MIN_DELAY_MS=50 MAX_DELAY_MS=100 \
  docker compose up -d --force-recreate --no-deps worker
```

Create a new tenant. It ends `failed`, and the deploy task ends `failed`. `PATCH` returns `409` `tenant_update_not_allowed`. `DELETE` returns `202`.

Restore the worker:

```bash
FAIL_RATE=0 MIN_DELAY_MS=500 MAX_DELAY_MS=1500 \
  docker compose up -d --force-recreate --no-deps worker
```

## 8. Progress messages without the worker

Create a tenant and use its task id before it finishes, or stop the worker first so the task stays `accepted`:

```bash
docker compose stop worker
```

```bash
chmod +x scripts/publish-progress.sh

./scripts/publish-progress.sh --task-id <task-id> --status done --update-id <task-id>:done
./scripts/publish-progress.sh --task-id <task-id> --status in_progress --update-id stale-running-1
./scripts/publish-progress.sh \
  --task-id 00000000-0000-4000-8000-000000000099 \
  --status done \
  --update-id missing-task
```

The first message completes the task. Sending the same `--update-id` again changes nothing. A later `in_progress` does not move a finished task backward. The unknown task is nacked and then dropped. A later valid update still applies. A malformed payload is dropped immediately.

Start the worker again if you stopped it:

```bash
docker compose start worker
```

## 9. Automated suite

Tests need Python 3.14. They use a temporary SQLite file and start NATS from a local `nats-server` (including `.tools/nats-server`) or from Docker.

```bash
python3.14 -m venv .venv
.venv/bin/pip install -e '.[dev]'
make test
```

`make test` covers the deploy lifecycle, duplicate and stale progress, done-before-in-progress, poison messages, update and destroy, validation, concurrent create and patch, pagination, worker success and failure, and the outbox.

One command runs Ruff, Bandit, the test suite, and pip-audit:

```bash
make verify
```

## 10. Tear down

```bash
docker compose down -v
```

That removes the containers and the SQLite and NATS volumes.
