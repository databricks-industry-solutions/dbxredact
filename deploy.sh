#!/bin/bash
# Deploy dbxredact to Databricks
# Usage: ./deploy.sh [dev|prod] [--validate-only] [--yes|-y]

set -e

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
if [ -z "${DATABRICKS_HOST}" ]; then
    echo "Error: DATABRICKS_HOST not set in ${ENV_FILE}"
    exit 1
fi

if [ -z "${CATALOG}" ]; then
    echo "Error: CATALOG not set in ${ENV_FILE}"
    exit 1
fi

if [ -z "${SCHEMA}" ]; then
    echo "Error: SCHEMA not set in ${ENV_FILE}"
    exit 1
fi

# App deployment flag (default: true)
DEPLOY_APP="${DEPLOY_APP:-true}"

if [ -z "${WAREHOUSE_ID}" ] && [ "${DEPLOY_APP}" != "false" ]; then
    echo "Error: WAREHOUSE_ID not set in ${ENV_FILE} (required when DEPLOY_APP=true)"
    exit 1
fi

# Get package version from pyproject.toml (single source of truth)
PACKAGE_VERSION=$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/wheels"
WHEEL_FILE="dbxredact-${PACKAGE_VERSION}-py3-none-any.whl"

echo ""
echo "  Host:          ${DATABRICKS_HOST}"
echo "  Catalog:       ${CATALOG}"
echo "  Schema:        ${SCHEMA}"
echo "  Warehouse ID:  ${WAREHOUSE_ID}"
echo "  Version:       ${PACKAGE_VERSION}"
echo "  Volume:        ${VOLUME_PATH}"
echo "  Wheel:         ${WHEEL_FILE}"
echo "  Deploy app:    ${DEPLOY_APP}"
echo ""

# Generate databricks.yml from template
if confirm "Generate databricks.yml from template for target '${TARGET}' on ${DATABRICKS_HOST}"; then
    if [ ! -f "databricks.yml.template" ]; then
        echo "Error: databricks.yml.template not found"
        exit 1
    fi
    sed -e "s|__DATABRICKS_HOST__|${DATABRICKS_HOST}|g" \
        -e "s|__CATALOG__|${CATALOG}|g" \
        -e "s|__SCHEMA__|${SCHEMA}|g" \
        -e "s|__WAREHOUSE_ID__|${WAREHOUSE_ID}|g" \
        -e "s|__PACKAGE_VERSION__|${PACKAGE_VERSION}|g" \
        databricks.yml.template > databricks.yml
    if [ "${DEPLOY_APP}" = "false" ]; then
        sed -i.bak '/resources\/app\.yml/d' databricks.yml && rm -f databricks.yml.bak
        echo "Generated databricks.yml (app excluded)"
    else
        echo "Generated databricks.yml"
    fi
fi

# Build wheel
WHEEL_PATH="dist/${WHEEL_FILE}"
if confirm "Build wheel with poetry (version ${PACKAGE_VERSION})"; then
    poetry build
    echo "Built wheel: ${WHEEL_PATH}"
fi

# Upload wheel to volume
if confirm "Upload ${WHEEL_FILE} to ${VOLUME_PATH}/${WHEEL_FILE}"; then
    if ! databricks fs cp "${WHEEL_PATH}" "dbfs:${VOLUME_PATH}/${WHEEL_FILE}" --overwrite; then
        echo ""
        echo "Error: Failed to upload wheel to ${VOLUME_PATH}"
        echo ""
        echo "Check that the volume exists. Create it with:"
        echo "  CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.wheels"
        exit 1
    fi
    echo "Uploaded wheel to ${VOLUME_PATH}/${WHEEL_FILE}"
fi

# Validate bundle
if confirm "Validate Databricks bundle for target '${TARGET}'"; then
    databricks bundle validate -t "${TARGET}"
fi

if [ "$VALIDATE_ONLY" == "--validate-only" ]; then
    echo ""
    echo "=== Validation Complete (--validate-only) ==="
    exit 0
fi

# Deploy bundle
if confirm "Deploy Databricks bundle to target '${TARGET}' on ${DATABRICKS_HOST}"; then
    databricks bundle deploy -t "${TARGET}"
    echo ""
    echo "=== Deployment Complete ==="
    echo "Target: ${TARGET}"
    echo "Host: ${DATABRICKS_HOST}"
    echo "Wheel: ${VOLUME_PATH}/${WHEEL_FILE}"
fi

# Grant UC permissions to app service principal
APP_NAME="dbxredact-app"
if [ "${DEPLOY_APP}" = "false" ]; then
    echo "Skipping app UC grants (DEPLOY_APP=false)"
elif confirm "Grant UC permissions to app service principal '${APP_NAME}' for ${CATALOG}.${SCHEMA}"; then
    echo "Fetching service principal for app '${APP_NAME}'..."

    APP_JSON=$(databricks apps get "${APP_NAME}" --output json 2>&1) || {
        echo "ERROR: Failed to get app info: ${APP_JSON}"
        echo "The app may not exist yet -- it will be created by 'bundle run' below."
        echo "Re-run this script after the first deploy to grant permissions."
        APP_JSON=""
    }

    if [ -n "${APP_JSON}" ]; then
        SPN_ID=$(echo "${APP_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_id',''))")
        echo "App service principal ID: ${SPN_ID}"

        if [ -z "${SPN_ID}" ]; then
            echo "ERROR: service_principal_id not found in app response."
            echo "Full response: ${APP_JSON}"
            exit 1
        fi

        SPN_JSON=$(databricks service-principals get "${SPN_ID}" --output json 2>&1) || {
            echo "ERROR: Failed to get service principal: ${SPN_JSON}"
            exit 1
        }
        APP_ID=$(echo "${SPN_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('applicationId',''))")

        if [ -z "${APP_ID}" ]; then
            echo "ERROR: application_id not found for SPN ${SPN_ID}."
            echo "Full response: ${SPN_JSON}"
            exit 1
        fi

        echo "Service principal application_id: ${APP_ID}"
        echo "Granting UC permissions..."

        GRANT_STATEMENTS=(
            "GRANT USE CATALOG ON CATALOG \`${CATALOG}\` TO \`${APP_ID}\`"
            "GRANT USE SCHEMA ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_ID}\`"
            "GRANT CREATE TABLE ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_ID}\`"
            "GRANT SELECT ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_ID}\`"
            "GRANT MODIFY ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_ID}\`"
        )

        GRANT_FAILED=false
        for STMT in "${GRANT_STATEMENTS[@]}"; do
            echo "  Running: ${STMT}"
            RESULT=$(databricks api post /api/2.0/sql/statements \
                --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${STMT}\", \"wait_timeout\": \"30s\"}" 2>&1)
            STATUS=$(echo "${RESULT}" | python3 -c "import sys,json; d=json.load(sys.stdin); s=d.get('status',{}); print(s.get('state','UNKNOWN'))" 2>/dev/null || echo "PARSE_ERROR")
            if [ "${STATUS}" = "SUCCEEDED" ]; then
                echo "    OK"
            else
                echo "    FAILED (status: ${STATUS})"
                GRANT_FAILED=true
            fi
        done
        if [ "${GRANT_FAILED}" = true ]; then
            echo ""
            echo "WARNING: Some UC grants failed. This is common when your user does not"
            echo "have permission to grant USE CATALOG or manage schema-level grants."
            echo "The deployment will continue. You or a catalog admin can apply the"
            echo "missing grants manually later. The app needs these permissions to"
            echo "read/write tables in ${CATALOG}.${SCHEMA}."
        else
            echo "All grants applied successfully."
        fi
    fi
fi

# Start/redeploy the app
if [ "${DEPLOY_APP}" = "false" ]; then
    echo "Skipping app start (DEPLOY_APP=false)"
elif confirm "Start/redeploy app 'dbxredact_app' on target '${TARGET}'"; then
    databricks bundle run dbxredact_app -t "${TARGET}"
fi

echo ""
echo "=== Done ==="
