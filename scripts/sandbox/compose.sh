#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/sandbox/docker-compose.yml"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Compose file not found: $COMPOSE_FILE" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found in PATH" >&2
  exit 1
fi

action="${1:-}"
shift || true

case "$action" in
  up)
    if [[ $# -eq 0 ]]; then
      echo "Usage: compose.sh up <service...>" >&2
      exit 1
    fi
    docker compose -f "$COMPOSE_FILE" up -d "$@"
    ;;
  down)
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    ;;
  ps)
    docker compose -f "$COMPOSE_FILE" ps
    ;;
  *)
    echo "Usage: compose.sh {up <service...>|down|ps}" >&2
    exit 1
    ;;
esac
