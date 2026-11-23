#!/bin/bash
# Optional guided wrapper for deploying dbxredact to Databricks.
#
# You do NOT need this script. `databricks.yml` is a committed static bundle, so
# the plain flow works from the CLI or the workspace UI:
#   mkdir -p .databricks/bundle/dev
#   cp variable-overrides.example.json .databricks/bundle/dev/variable-overrides.json  # edit values
#   databricks bundle deploy -t dev
#   databricks bundle run dbxredact_app -t dev
#   ./grant_app_permissions.sh -t dev
#
# This wrapper is a convenience for users who prefer the .env flow and interactive
# confirmations, and for jobs-only deploys (DEPLOY_APP=false). It:
#   1. reads a dev.env / prod.env file,
#   2. writes .databricks/bundle/<target>/variable-overrides.json from it (the path
#      DAB auto-loads), so the bundle picks up catalog/schema/warehouse,
#   3. runs `databricks bundle validate` / `deploy`,
#   4. runs grant_app_permissions.sh and `databricks bundle run dbxredact_app`.
#
# The wheel is built automatically by the bundle's `artifacts` hook (uv build), and
# the React frontend dist/ is committed -- neither is built or uploaded here.
#
# Usage: ./deploy.sh [dev|prod] [--validate-only] [--yes|-y]

set -euo pipefail

# Bridge pip index config to UV_INDEX_URL for internal PyPI proxies (the bundle's
# `uv build` artifact hook inherits this).
if [ -z "${UV_INDEX_URL:-}" ]; then
    _pip_idx=$(pip3 config get global.index-url 2>/dev/null || true)
    [ -n "$_pip_idx" ] && export UV_INDEX_URL="$_pip_idx"
fi

cleanup() {
    local code=$?
    if [ $code -ne 0 ]; then
        echo ""
        echo "ERROR: deploy.sh failed (exit $code). Check the output above."
    fi
}
trap cleanup EXIT

ENV="${1:-dev}"
VALIDATE_ONLY=""
AUTO_YES=false
for arg in "$@"; do
    case "$arg" in
        --validate-only) VALIDATE_ONLY="--validate-only" ;;
        --yes|-y) AUTO_YES=true ;;
    esac
done

confirm() {
    local msg="$1"
    if [ "$AUTO_YES" = true ]; then return 0; fi
    echo ""
    echo "--- $msg ---"
    read -rp "Proceed? [Y/n/q] " answer
    answer=$(echo "$answer" | tr '[:upper:]' '[:lower:]')
    case "$answer" in
        q) echo "Aborted."; exit 0 ;;
        n) echo "Skipped."; return 1 ;;
        *) return 0 ;;
    esac
}

# Determine env file
if [ "$ENV" == "prod" ]; then
    ENV_FILE="prod.env"
    TARGET="prod"
else
    ENV_FILE="dev.env"
    TARGET="dev"
fi

echo "=== dbxredact Deployment ==="
echo "Environment: ${ENV}"
echo "Target: ${TARGET}"
echo "Env file: ${ENV_FILE}"
echo ""

# Check if env file exists
if [ ! -f "${ENV_FILE}" ]; then
    echo "Error: ${ENV_FILE} not found"
    echo "Create it from example.env:"
    echo "  cp example.env ${ENV_FILE}"
    exit 1
fi

# Load environment variables
echo "Loading environment from ${ENV_FILE}..."
set -a
source "${ENV_FILE}"
set +a

# Validate required variables
if [ -z "${CATALOG:-}" ]; then
    echo "Error: CATALOG not set in ${ENV_FILE}"
    exit 1
fi

if [ -z "${SCHEMA:-}" ]; then
    echo "Error: SCHEMA not set in ${ENV_FILE}"
    exit 1
fi

# App deployment flag (default: true)
DEPLOY_APP="${DEPLOY_APP:-true}"

if [ -z "${WAREHOUSE_ID:-}" ] && [ "${DEPLOY_APP}" != "false" ]; then
    echo "Error: WAREHOUSE_ID not set in ${ENV_FILE} (required when DEPLOY_APP=true)"
    exit 1
fi

# Export the workspace host when provided (otherwise the CLI profile /
# DATABRICKS_HOST env var is used, same as a plain bundle deploy).
if [ -n "${DATABRICKS_HOST:-}" ]; then
    export DATABRICKS_HOST
fi

PACKAGE_VERSION=$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')

echo ""
echo "  Host:          ${DATABRICKS_HOST:-<from CLI profile>}"
echo "  Catalog:       ${CATALOG}"
echo "  Schema:        ${SCHEMA}"
echo "  Warehouse ID:  ${WAREHOUSE_ID:-<none>}"
echo "  Version:       ${PACKAGE_VERSION}"
echo "  Deploy app:    ${DEPLOY_APP}"
echo ""

# Write the bundle variable-overrides file DAB auto-loads for this target. This is
# what carries catalog/schema/warehouse into the static databricks.yml.
OVERRIDE_DIR=".databricks/bundle/${TARGET}"
OVERRIDE_FILE="${OVERRIDE_DIR}/variable-overrides.json"
if confirm "Write bundle variable overrides to ${OVERRIDE_FILE}"; then
    mkdir -p "${OVERRIDE_DIR}"
    python3 - "$OVERRIDE_FILE" "$CATALOG" "$SCHEMA" "${WAREHOUSE_ID:-}" "$PACKAGE_VERSION" <<'PY'
import json, sys
path, catalog, schema, warehouse, version = sys.argv[1:6]
overrides = {"catalog": catalog, "schema": schema, "package_version": version}
if warehouse:
    overrides["sql_warehouse_id"] = warehouse
with open(path, "w") as f:
    json.dump(overrides, f, indent=2)
    f.write("\n")
PY
    echo "Wrote ${OVERRIDE_FILE}"
fi

# Jobs-only mode: comment out the app include so the app is not deployed.
# We edit a copy-safe way and restore on exit.
APP_INCLUDE_TOGGLED=false
restore_app_include() {
    if [ "${APP_INCLUDE_TOGGLED}" = true ]; then
        if [[ "$OSTYPE" == darwin* ]]; then
            sed -i '' 's|^  # - resources/app.yml  # (disabled by deploy.sh DEPLOY_APP=false)|  - resources/app.yml  # Comment out to skip app deployment (jobs-only)|' databricks.yml
        else
            sed -i 's|^  # - resources/app.yml  # (disabled by deploy.sh DEPLOY_APP=false)|  - resources/app.yml  # Comment out to skip app deployment (jobs-only)|' databricks.yml
        fi
    fi
}
trap 'restore_app_include; cleanup' EXIT

if [ "${DEPLOY_APP}" = "false" ]; then
    echo "DEPLOY_APP=false -- temporarily disabling the app include for this deploy."
    if [[ "$OSTYPE" == darwin* ]]; then
        sed -i '' 's|^  - resources/app.yml  # Comment out to skip app deployment (jobs-only)|  # - resources/app.yml  # (disabled by deploy.sh DEPLOY_APP=false)|' databricks.yml
    else
        sed -i 's|^  - resources/app.yml  # Comment out to skip app deployment (jobs-only)|  # - resources/app.yml  # (disabled by deploy.sh DEPLOY_APP=false)|' databricks.yml
    fi
    APP_INCLUDE_TOGGLED=true
fi

# Validate bundle (builds the wheel via the artifacts hook as part of validate/deploy)
if confirm "Validate Databricks bundle for target '${TARGET}'"; then
    databricks bundle validate -t "${TARGET}"
fi

if [ "$VALIDATE_ONLY" == "--validate-only" ]; then
    echo ""
    echo "=== Validation Complete (--validate-only) ==="
    exit 0
fi

# Deploy bundle (uv build runs automatically here)
if confirm "Deploy Databricks bundle to target '${TARGET}'"; then
    databricks bundle deploy -t "${TARGET}"
    echo ""
    echo "=== Deployment Complete ==="
    echo "Target: ${TARGET}"
fi

# Start/redeploy the app (creates the app + its service principal on first run)
if [ "${DEPLOY_APP}" = "false" ]; then
    echo "Skipping app start (DEPLOY_APP=false)"
elif confirm "Start/redeploy app 'dbxredact_app' on target '${TARGET}'"; then
    databricks bundle run dbxredact_app -t "${TARGET}"
fi

# Grant UC permissions to the app service principal (needs the app SPN to exist)
if [ "${DEPLOY_APP}" = "false" ]; then
    echo "Skipping app UC grants (DEPLOY_APP=false)"
elif confirm "Grant UC permissions to app service principal for ${CATALOG}.${SCHEMA}"; then
    CATALOG="${CATALOG}" SCHEMA="${SCHEMA}" WAREHOUSE_ID="${WAREHOUSE_ID}" \
        ./grant_app_permissions.sh -t "${TARGET}"
fi

echo ""
echo "=== Done ==="
