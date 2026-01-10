#!/usr/bin/env bash
# Usage: source activate.sh

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT" || { echo "❌ cannot cd to $PROJECT_ROOT"; return 1; }

echo "Project root: $PROJECT_ROOT"

# Activate venv
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
  source "$PROJECT_ROOT/.venv/bin/activate"
else
  echo "❌ .venv not found at $PROJECT_ROOT/.venv"
  return 1
fi

# Load env vars
if [ -f "$PROJECT_ROOT/.env.local" ]; then
  set -a
  source "$PROJECT_ROOT/.env.local"
  set +a
else
  echo "❌ .env.local not found (create it from .env.local.example)"
  return 1
fi

# Validate required vars (source-safe)
if [ -z "${GC_PROJECT_ID:-}" ]; then
  echo "❌ GC_PROJECT_ID missing (check .env.local)"
  return 1
fi

if [ -z "${FX_ZIP_URL:-}" ]; then
  echo "❌ FX_ZIP_URL missing (check .env.local)"
  return 1
fi

# Ensure PYTHONPATH contains airflow/dags
export PYTHONPATH="$PROJECT_ROOT/airflow/dags${PYTHONPATH:+:$PYTHONPATH}"

# dbt
export DBT_PROFILES_DIR="$PROJECT_ROOT/dbt"

# creds path
export GOOGLE_APPLICATION_CREDENTIALS="$PROJECT_ROOT/secrets/service-account.json"
[ -f "$GOOGLE_APPLICATION_CREDENTIALS" ] || {
  echo "❌ Credentials file not found: $GOOGLE_APPLICATION_CREDENTIALS"
  return 1
}

echo "✅ Environment activated"
echo "   GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS"
echo "   PYTHONPATH=$PYTHONPATH"
echo "   DBT_PROFILES_DIR=$DBT_PROFILES_DIR"
