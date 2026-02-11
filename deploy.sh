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
