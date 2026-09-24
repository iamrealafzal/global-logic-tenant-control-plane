#!/usr/bin/env bash
# Publishes one progress message through the control-plane container.
# Example:
#   ./scripts/publish-progress.sh --task-id <uuid> --status in_progress --update-id stale-1
set -euo pipefail
exec docker compose exec -T controlplane python -m controlplane.publish "$@"
