#!/bin/bash
# Load JSL benchmark data to Databricks
# Usage: ./load_benchmark.sh [catalog] [schema] [volume]

set -e

CATALOG="${1:-your_catalog}"
SCHEMA="${2:-your_schema}"
VOLUME="${3:-benchmark_data}"

CSV_FILE="data/jsl_benchmark.csv"
VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}"

echo "=== JSL Benchmark Data Loader ==="
echo "Catalog: ${CATALOG}"
echo "Schema: ${SCHEMA}"
echo "Volume: ${VOLUME}"
echo "Volume Path: ${VOLUME_PATH}"
echo ""

# Check if CSV file exists
if [ ! -f "${CSV_FILE}" ]; then
    echo "Error: CSV file not found at ${CSV_FILE}"
    exit 1
fi

# Create schema if not exists
echo "Creating schema if not exists..."
databricks sql query --query "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}"

# Create volume if not exists
echo "Creating volume if not exists..."
databricks sql query --query "CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.${VOLUME}"

# Upload CSV to volume
echo "Uploading CSV to volume..."
databricks fs cp "${CSV_FILE}" "dbfs:${VOLUME_PATH}/jsl_benchmark.csv" --overwrite

echo "CSV uploaded to ${VOLUME_PATH}/jsl_benchmark.csv"

# Create ground truth table from CSV (multiline CSV with header)
echo "Creating ground truth table..."
databricks sql query --query "
DROP TABLE IF EXISTS ${CATALOG}.${SCHEMA}.jsl_benchmark_ground_truth;

CREATE TABLE ${CATALOG}.${SCHEMA}.jsl_benchmark_ground_truth
USING CSV
OPTIONS (
    path '${VOLUME_PATH}/jsl_benchmark.csv',
    header 'true',
    inferSchema 'true',
    multiLine 'true',
    escape '\"'
);
"

# Create source table with unique documents
echo "Creating source table..."
databricks sql query --query "
DROP TABLE IF EXISTS ${CATALOG}.${SCHEMA}.jsl_benchmark_source;

CREATE TABLE ${CATALOG}.${SCHEMA}.jsl_benchmark_source AS
SELECT 
    CAST(doc_id AS STRING) as doc_id,
    FIRST(text) as text
FROM ${CATALOG}.${SCHEMA}.jsl_benchmark_ground_truth
GROUP BY doc_id
ORDER BY doc_id;
"

# Optimize tables
echo "Optimizing tables..."
databricks sql query --query "OPTIMIZE ${CATALOG}.${SCHEMA}.jsl_benchmark_ground_truth"
databricks sql query --query "OPTIMIZE ${CATALOG}.${SCHEMA}.jsl_benchmark_source"

# Show summary
echo ""
echo "=== Summary ==="
databricks sql query --query "
SELECT 
    'ground_truth' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT doc_id) as unique_docs
FROM ${CATALOG}.${SCHEMA}.jsl_benchmark_ground_truth
UNION ALL
SELECT 
    'source' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT doc_id) as unique_docs
FROM ${CATALOG}.${SCHEMA}.jsl_benchmark_source
"

echo ""
echo "=== Setup Complete ==="
echo "Ground Truth Table: ${CATALOG}.${SCHEMA}.jsl_benchmark_ground_truth"
echo "Source Table: ${CATALOG}.${SCHEMA}.jsl_benchmark_source"

