#!/bin/bash
# Deploy dbxredact to Databricks
# Usage: ./deploy.sh [dev|prod] [--validate-only]

set -e

ENV="${1:-dev}"
VALIDATE_ONLY="${2:-}"

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

echo "Databricks Host: ${DATABRICKS_HOST}"
echo "Catalog: ${CATALOG}"
echo "Schema: ${SCHEMA}"
echo ""

# Generate databricks.yml from template
echo "Generating databricks.yml from template..."
if [ ! -f "databricks.yml.template" ]; then
    echo "Error: databricks.yml.template not found"
    exit 1
fi

sed -e "s|__DATABRICKS_HOST__|${DATABRICKS_HOST}|g" \
    -e "s|__CATALOG__|${CATALOG}|g" \
    -e "s|__SCHEMA__|${SCHEMA}|g" \
    databricks.yml.template > databricks.yml
echo "Generated databricks.yml"

# Frontend is built at deploy time on the Databricks runtime via app.yaml.
# For local testing, run: cd apps/dbxredact-app && npm install && npm run build

# Build wheel
echo ""
echo "Building wheel with poetry..."
poetry build

# Get package version from pyproject.toml
PACKAGE_VERSION=$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')
WHEEL_FILE="dbxredact-${PACKAGE_VERSION}-py3-none-any.whl"
WHEEL_PATH="dist/${WHEEL_FILE}"

echo "Built wheel: ${WHEEL_PATH}"

# Upload wheel to volume
echo ""
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/wheels"
echo "Uploading wheel to ${VOLUME_PATH}..."

if ! databricks fs cp "${WHEEL_PATH}" "dbfs:${VOLUME_PATH}/${WHEEL_FILE}" --overwrite; then
    echo ""
    echo "Error: Failed to upload wheel to ${VOLUME_PATH}"
    echo ""
    echo "Check that the volume exists. Create it with:"
    echo "  CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.wheels"
    exit 1
fi
echo "Uploaded wheel to ${VOLUME_PATH}/${WHEEL_FILE}"

# Validate bundle
echo ""
echo "Validating Databricks bundle..."
databricks bundle validate -t "${TARGET}"

if [ "$VALIDATE_ONLY" == "--validate-only" ]; then
    echo ""
    echo "=== Validation Complete (--validate-only) ==="
    exit 0
fi

# Deploy bundle
echo ""
echo "Deploying Databricks bundle..."
databricks bundle deploy -t "${TARGET}"

echo ""
echo "=== Deployment Complete ==="
echo "Target: ${TARGET}"
echo "Host: ${DATABRICKS_HOST}"
echo "Wheel: ${VOLUME_PATH}/${WHEEL_FILE}"
echo ""

# Grant UC permissions to app service principal
APP_NAME="dbxredact-app"
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

    SQL_WAREHOUSE_ID=$(grep -A3 'sql_warehouse_id:' variables.yml | grep 'default:' | sed 's/.*default: *["]*\([^"]*\)["]*$/\1/' | tr -d ' ')

    if [ -z "${SQL_WAREHOUSE_ID}" ]; then
        echo "ERROR: sql_warehouse_id not set in variables.yml. Cannot run grants."
        exit 1
    fi

    GRANT_STATEMENTS=(
        "GRANT USE CATALOG ON CATALOG \`${CATALOG}\` TO \`${APP_ID}\`"
        "GRANT USE SCHEMA ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_ID}\`"
        "GRANT CREATE TABLE ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_ID}\`"
        "GRANT SELECT ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_ID}\`"
        "GRANT MODIFY ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_ID}\`"
    )

    for STMT in "${GRANT_STATEMENTS[@]}"; do
        echo "  Running: ${STMT}"
        RESULT=$(databricks api post /api/2.0/sql/statements \
            --json "{\"warehouse_id\": \"${SQL_WAREHOUSE_ID}\", \"statement\": \"${STMT}\", \"wait_timeout\": \"30s\"}" 2>&1)
        STATUS=$(echo "${RESULT}" | python3 -c "import sys,json; d=json.load(sys.stdin); s=d.get('status',{}); print(s.get('state','UNKNOWN'))" 2>/dev/null || echo "PARSE_ERROR")
        if [ "${STATUS}" = "SUCCEEDED" ]; then
            echo "    OK"
        else
            echo "    FAILED (status: ${STATUS})"
            echo "    Response: ${RESULT}"
            exit 1
        fi
    done
    echo "All grants applied successfully."
fi

# Start/redeploy the app
echo ""
echo "Starting app..."
databricks bundle run dbxredact_app -t "${TARGET}"

echo ""
echo "=== Done ==="
