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

normalize_service() {
  local service="$1"
  case "$service" in
    postgresql)
      echo "postgres"
      ;;
    *)
      echo "$service"
      ;;
  esac
}

append_unique_service() {
  local value="$1"
  shift
  local existing
  for existing in "$@"; do
    if [[ "$existing" == "$value" ]]; then
      return 1
    fi
  done
  return 0
}

case "$action" in
  up)
    if [[ $# -eq 0 ]]; then
      docker compose -f "$COMPOSE_FILE" up -d
      exit 0
    fi

    declare -a services=()
    for raw in "$@"; do
      mapped="$(normalize_service "$raw")"
      if [[ ${#services[@]} -eq 0 ]]; then
        if append_unique_service "$mapped"; then
          services+=("$mapped")
        fi
      elif append_unique_service "$mapped" "${services[@]}"; then
        services+=("$mapped")
      fi
    done

    if [[ ${#services[@]} -eq 0 ]]; then
      docker compose -f "$COMPOSE_FILE" up -d
    else
      docker compose -f "$COMPOSE_FILE" up -d "${services[@]}"
    fi
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
