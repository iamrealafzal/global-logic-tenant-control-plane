#!/usr/bin/env bash
# Fires concurrent creates, waits until the winner is active, then fires
# concurrent patches that all present the same version.
set -euo pipefail

base="${BASE_URL:-http://127.0.0.1:8080}"
slug="race-$(date +%s)"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

create_one() {
  curl -sS -o "$1.body" -w "%{http_code}" -X POST "$base/v1/tenants" \
    -H 'content-type: application/json' \
    -d "{\"slug\":\"$slug\",\"name\":\"Race\"}" > "$1.code"
}

for i in $(seq 1 16); do
  create_one "$workdir/create-$i" &
done
wait

created=0
for i in $(seq 1 16); do
  code="$(cat "$workdir/create-$i.code")"
  if [ "$code" = "201" ]; then
    created=$((created + 1))
    cp "$workdir/create-$i.body" "$workdir/winner.json"
  elif [ "$code" != "409" ]; then
    echo "unexpected create status $code" >&2
    cat "$workdir/create-$i.body" >&2
    exit 1
  fi
done
if [ "$created" != "1" ]; then
  echo "expected exactly one create, got $created" >&2
  exit 1
fi

tenant_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["tenant"]["id"])' "$workdir/winner.json")"
task_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["task"]["id"])' "$workdir/winner.json")"
echo "winner tenant=$tenant_id task=$task_id"

for _ in $(seq 1 50); do
  status="$(curl -sS "$base/v1/tenants/$tenant_id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [ "$status" = "active" ]; then
    break
  fi
  if [ "$status" = "failed" ]; then
    echo "deploy failed; restart the worker with FAIL_RATE=0" >&2
    exit 1
  fi
  sleep 0.2
done
if [ "$status" != "active" ]; then
  echo "tenant did not become active (last status $status)" >&2
  exit 1
fi

version="$(curl -sS "$base/v1/tenants/$tenant_id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["version"])')"
echo "active version=$version task=$task_id"

patch_one() {
  curl -sS -o "$1.body" -w "%{http_code}" -X PATCH "$base/v1/tenants/$tenant_id" \
    -H 'content-type: application/json' \
    -d "{\"name\":\"Raced\",\"version\":$version}" > "$1.code"
}

for i in $(seq 1 16); do
  patch_one "$workdir/patch-$i" &
done
wait

patched=0
for i in $(seq 1 16); do
  code="$(cat "$workdir/patch-$i.code")"
  if [ "$code" = "202" ]; then
    patched=$((patched + 1))
  elif [ "$code" != "409" ]; then
    echo "unexpected patch status $code" >&2
    cat "$workdir/patch-$i.body" >&2
    exit 1
  fi
done
if [ "$patched" != "1" ]; then
  echo "expected exactly one patch, got $patched" >&2
  exit 1
fi
echo "concurrent create and patch each produced exactly one success"
