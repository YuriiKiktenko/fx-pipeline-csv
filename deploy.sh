#!/usr/bin/env bash
# Usage: ./deploy.sh --init | --no-init

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT" || { echo "❌ cannot cd to $PROJECT_ROOT"; exit 1; }

usage() {
  echo "Usage: $(basename "$0") --init | --no-init"
  echo "  --init     Run full airflow-init (migrations/user creation/etc.)"
  echo "  --no-init  Skip airflow-init (fast deploy for DAG/code changes)"
}

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

case "$1" in
  --init)
    SKIP_AIRFLOW_INIT=0
    ;;
  --no-init)
    SKIP_AIRFLOW_INIT=1
    ;;
  *)
    echo "Unknown option: $1"
    usage
    exit 2
    ;;
esac

echo "==> update repo (origin/main)"
git fetch origin
git reset --hard origin/main

if [[ "$SKIP_AIRFLOW_INIT" -eq 1 ]]; then
  echo "==> docker compose up (SKIP_AIRFLOW_INIT=1)"
  SKIP_AIRFLOW_INIT=1 docker compose up -d
else
  echo "==> docker compose up (init enabled)"
  docker compose up -d
fi
