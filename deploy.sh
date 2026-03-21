#!/usr/bin/env bash
# Usage: ./deploy.sh [-l|--local] [-n|--no-init]
#   Default: full airflow-init. Use -n / --no-init to skip (e.g. GitHub Actions after first setup).
#
#   Local (no git fetch/reset):
#     ./deploy.sh -l / --local
#     ./deploy.sh -ln  # same as --local --no-init

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT" || { echo "❌ cannot cd to $PROJECT_ROOT"; exit 1; }

usage() {
  echo "Usage: $(basename "$0") [-l|--local] [-n|--no-init]"
  echo "  -l, --local    Skip git fetch/reset (local machine)"
  echo "  -n, --no-init  Skip airflow-init (by default it runs)"
}

LOCAL_MODE=0
SKIP_AIRFLOW_INIT=0

for arg in "$@"; do
  case "$arg" in
    --local|-l)
      LOCAL_MODE=1
      ;;
    --no-init|-n)
      SKIP_AIRFLOW_INIT=1
      ;;
    -ln|-nl)
      LOCAL_MODE=1
      SKIP_AIRFLOW_INIT=1
      ;;
    *)
      echo "Unknown option: $arg"
      usage
      exit 2
      ;;
  esac
done

if [[ "$LOCAL_MODE" -eq 0 ]]; then
  echo "==> update repo (origin/main)"
  git fetch origin
  git reset --hard origin/main
else
  echo "==> local mode: skipping git fetch/reset"
fi

echo "==> build custom Airflow image"
docker compose build airflow-init

echo "==> install dbt packages (dbt deps in airflow-cli)"
docker compose run --rm airflow-cli bash -c "cd /opt/airflow/dbt/fx_dbt && dbt deps"

if [[ "$SKIP_AIRFLOW_INIT" -eq 1 ]]; then
  echo "==> docker compose up (SKIP_AIRFLOW_INIT=1)"
  SKIP_AIRFLOW_INIT=1 docker compose up -d
else
  echo "==> docker compose up (init enabled)"
  docker compose up -d
fi
