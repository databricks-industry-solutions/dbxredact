#!/bin/bash
# Deploy and run the redaction benchmark job, then download cluster logs
# and extract [BENCHMARK_RESULTS] lines for local review.
#
# Usage:
#   ./scripts/run_benchmark.sh -d synthetic_benchmark_finance
#   ./scripts/run_benchmark.sh -d synthetic_benchmark_medical -p "use_gliner=true"
#   ./scripts/run_benchmark.sh -p "source_table=cat.schema.table,text_column=text,doc_id_column=doc_id"
#
# Flags:
#   -d / --dataset    Benchmark dataset name (overrides var.benchmark_dataset; default: jsl_benchmark)
#   -p / --params     Extra job params string (passed to --params for job-level parameters)
#   -t / --target     Bundle target (default: dev)
#   -s / --skip-deploy  Skip validate+deploy steps
#   --keep            Keep raw downloaded logs after extraction
#
# Requires: databricks CLI configured, dev.env with CATALOG and SCHEMA.

set +e

# Source configuration
if [ -f "dev.env" ]; then
    source dev.env
else
    echo "ERROR: dev.env not found. Copy example.env to dev.env and configure."
    exit 1
fi

: "${CATALOG:?Set CATALOG in dev.env}"
: "${SCHEMA:?Set SCHEMA in dev.env}"

TARGET="dev"
DATASET=""
JOB_PARAMS=""
SKIP_DEPLOY=false
KEEP_LOGS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--dataset) DATASET="$2"; shift 2 ;;
        -p|--params)  JOB_PARAMS="$2"; shift 2 ;;
        -t|--target)  TARGET="$2"; shift 2 ;;
        -s|--skip-deploy) SKIP_DEPLOY=true; shift ;;
        --keep)       KEEP_LOGS=true; shift ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [-d dataset_name] [-p \"key=val,...\"] [-t target] [-s] [--keep]"
            exit 1
            ;;
    esac
done

# Build --var flags for DAB variable overrides
VAR_FLAGS=""
if [ -n "$DATASET" ]; then
    VAR_FLAGS="--var benchmark_dataset=${DATASET}"
fi

# --- Step 1: Validate & Deploy ---
if [ "$SKIP_DEPLOY" = false ]; then
    echo "=== Step 1a: Validate bundle ==="
    databricks bundle validate -t "$TARGET" $VAR_FLAGS
    if [ $? -ne 0 ]; then
        echo "ERROR: Bundle validation failed"
        exit 1
    fi

    echo ""
    echo "=== Step 1b: Deploy bundle ==="
    databricks bundle deploy -t "$TARGET" $VAR_FLAGS
    if [ $? -ne 0 ]; then
        echo "ERROR: Bundle deploy failed"
        exit 1
    fi
else
    echo "=== Skipping validate & deploy ==="
fi

# --- Step 2: Run benchmark job ---
echo ""
echo "=== Step 2: Run benchmark job ==="
RUN_CMD="databricks bundle run redaction_benchmark -t $TARGET $VAR_FLAGS"
if [ -n "$JOB_PARAMS" ]; then
    RUN_CMD="$RUN_CMD --params \"$JOB_PARAMS\""
fi
echo "Running: $RUN_CMD"
eval $RUN_CMD
JOB_EXIT_CODE=$?
if [ $JOB_EXIT_CODE -ne 0 ]; then
    echo ""
    echo "WARNING: Job exited with code ${JOB_EXIT_CODE}"
fi

# --- Step 3: Wait for cluster logs ---
echo ""
echo "=== Step 3: Wait for cluster logs ==="

VOLUME_PATH="/Volumes/${CATALOG}/${SCHEMA}/cluster_logs"
FOUND_LOGS=false

for ATTEMPT in 1 2 3 4 5 6 7 8 9 10; do
    echo "Waiting for logs (attempt $ATTEMPT/10)..."

    NEWEST=$(databricks fs ls "dbfs:${VOLUME_PATH}" 2>/dev/null \
        | awk '{print $NF}' | grep -E '^[0-9]' | sort -r | head -1)

    if [ -n "$NEWEST" ]; then
        if databricks fs ls "dbfs:${VOLUME_PATH}/${NEWEST}/driver" &>/dev/null; then
            FOUND_LOGS=true
            break
        fi
    fi
    sleep 10
done

if [ "$FOUND_LOGS" = false ]; then
    echo "WARNING: Cluster logs not available after 100s. Check volume at ${VOLUME_PATH}"
    exit $JOB_EXIT_CODE
fi

# --- Step 4: Download & extract logs ---
echo ""
echo "=== Step 4: Download logs ==="

LOCAL_DIR="benchmark_results"
mkdir -p "${LOCAL_DIR}"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RAW_DIR="${LOCAL_DIR}/raw_${TIMESTAMP}"
SUMMARY_FILE="${LOCAL_DIR}/benchmark_summary_${TIMESTAMP}.txt"

CLUSTER_PATH="${VOLUME_PATH}/${NEWEST}"
echo "Cluster: ${NEWEST}"

mkdir -p "${RAW_DIR}/driver"
databricks fs cp "dbfs:${CLUSTER_PATH}/driver/stdout" "${RAW_DIR}/driver/stdout" 2>/dev/null || true
databricks fs cp "dbfs:${CLUSTER_PATH}/driver/stderr" "${RAW_DIR}/driver/stderr" 2>/dev/null || true

echo ""
echo "=== Step 5: Extract benchmark results ==="
{
    echo "=== Benchmark Results ==="
    echo "Timestamp: $(date -Iseconds)"
    echo "Target: ${TARGET}"
    echo "Dataset: ${DATASET:-jsl_benchmark}"
    echo "Params: ${JOB_PARAMS}"
    echo "Cluster: ${NEWEST}"
    echo ""
    grep -rh '\[BENCHMARK_RESULTS\]' "${RAW_DIR}" 2>/dev/null \
        | sed 's/.*\[BENCHMARK_RESULTS\] //' \
        || echo "(no [BENCHMARK_RESULTS] lines found -- check raw logs in ${RAW_DIR})"
} > "$SUMMARY_FILE"

echo ""
echo "=== ERRORS (if any) ==="
grep -rh -E "(ERROR|Exception|Traceback)" "${RAW_DIR}" 2>/dev/null \
    | grep -v "^Binary" | head -20 || echo "None found"

echo ""
cat "$SUMMARY_FILE"

if [ "$KEEP_LOGS" = false ]; then
    rm -rf "$RAW_DIR"
    echo ""
    echo "Raw logs cleaned up. Summary: ${SUMMARY_FILE}"
else
    echo ""
    echo "Raw logs kept at: ${RAW_DIR}"
    echo "Summary: ${SUMMARY_FILE}"
fi

echo ""
echo "=== Done ==="
