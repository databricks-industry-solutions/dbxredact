#!/bin/bash
# Post-deploy Unity Catalog grants for the dbxredact app service principal.
#
# Run AFTER the app + its service principal exist (i.e. after `databricks bundle
# deploy` and the app has been started at least once):
#   databricks bundle deploy -t <target> -p <profile>
#   databricks bundle run dbxredact_app -t <target> -p <profile>
#   ./grant_app_permissions.sh -t <target> -p <profile>
#
# Why this is a separate script and not part of the bundle:
#   DAB has no native `grants:` block for an app's OWN service principal, and the
#   SP does not exist until the app is created. So the grants must run as SQL
#   after deploy. deploy.sh calls this for you; you can also run it standalone.
#
# The script is idempotent -- safe to re-run. It resolves the app SP applicationId,
# then grants the SP the UC privileges the app needs to read/write tables in the
# deployment schema.
#
# EQUIVALENT MANUAL STEP: if you'd rather not run this script (or you lack the CLI),
# paste these 5 statements into a SQL editor as a catalog admin, replacing
# <application_id> with the app's service principal applicationId (find it under
# Admin Console > Service Principals > dbxredact-app), and <catalog>/<schema> with
# your values:
#   GRANT USE CATALOG ON CATALOG `<catalog>` TO `<application_id>`;
#   GRANT USE SCHEMA  ON SCHEMA `<catalog>`.`<schema>` TO `<application_id>`;
#   GRANT CREATE TABLE ON SCHEMA `<catalog>`.`<schema>` TO `<application_id>`;
#   GRANT SELECT ON SCHEMA `<catalog>`.`<schema>` TO `<application_id>`;
#   GRANT MODIFY ON SCHEMA `<catalog>`.`<schema>` TO `<application_id>`;
#
# Config (catalog/schema/warehouse) is read from the resolved bundle via
# `databricks bundle summary`, so it uses exactly the same variable values as the
# deploy. Override any value by exporting it before running (CATALOG, SCHEMA,
# WAREHOUSE_ID, APP_NAME).
set -e

TARGET="dev"
PROFILE=""

usage() {
    cat <<'EOF'
Usage: ./grant_app_permissions.sh [OPTIONS]
  -t, --target TARGET    Bundle target (default: dev)
  -p, --profile PROFILE  Databricks CLI profile (default: CLI default)
  -h, --help             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--target)  TARGET="$2"; shift 2 ;;
        -p|--profile) PROFILE="$2"; shift 2 ;;
        -h|--help)    usage; exit 0 ;;
        *) echo "Unknown option: $1 (use --help)"; exit 1 ;;
    esac
done

if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI not found." >&2
    exit 1
fi

# Build the shared --profile arg once (empty when no profile given).
PROFILE_ARG=()
[ -n "${PROFILE}" ] && PROFILE_ARG=(--profile "${PROFILE}")

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

APP_NAME="${APP_NAME:-dbxredact-app}"

# --- Resolve config from the deployed bundle (honors the deploy's var overrides) ---
echo "=== Resolving bundle config (target=${TARGET}) ==="
SUMMARY=$(databricks bundle summary -t "$TARGET" "${PROFILE_ARG[@]}" --output json 2>/dev/null) || SUMMARY=""

resolve_var() {
    # $1 = variable name, $2 = env override (if already set, wins)
    local name="$1" envval="$2"
    if [ -n "${envval}" ]; then echo "${envval}"; return; fi
    if [ -n "${SUMMARY}" ]; then
        echo "${SUMMARY}" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    v = d.get('variables', {}).get('$name', {})
    print(v.get('value', v.get('default', '')) or '')
except Exception:
    print('')
" 2>/dev/null || echo ""
    fi
}

CATALOG=$(resolve_var catalog "${CATALOG:-}")
SCHEMA=$(resolve_var schema "${SCHEMA:-}")
WAREHOUSE_ID=$(resolve_var sql_warehouse_id "${WAREHOUSE_ID:-}")

for pair in "catalog=${CATALOG}" "schema=${SCHEMA}" "sql_warehouse_id=${WAREHOUSE_ID}"; do
    if [ -z "${pair#*=}" ]; then
        echo "Error: could not resolve ${pair%%=*}. Set it in your bundle vars or export it before running." >&2
        exit 1
    fi
done

echo "  App:       ${APP_NAME}"
echo "  Catalog:   ${CATALOG}"
echo "  Schema:    ${SCHEMA}"
echo "  Warehouse: ${WAREHOUSE_ID}"

# --- Resolve the app service principal applicationId (UUID) ---
echo ""
echo "=== Resolving app service principal for: ${APP_NAME} ==="
APP_JSON=$(databricks apps get "${APP_NAME}" "${PROFILE_ARG[@]}" --output json 2>&1) || {
    echo "Error: app '${APP_NAME}' not found. Deploy and start the app first:" >&2
    echo "  databricks bundle deploy -t ${TARGET}" >&2
    echo "  databricks bundle run dbxredact_app -t ${TARGET}" >&2
    exit 1
}

SPN_APP_ID=$(echo "${APP_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id','') or '')" 2>/dev/null || echo "")
APP_SP_ID=$(echo "${APP_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_id','') or '')" 2>/dev/null || echo "")

# Fall back to SCIM lookup (requires workspace admin) if client_id absent.
if [ -z "${SPN_APP_ID}" ] || [ "${SPN_APP_ID}" = "None" ]; then
    if [ -n "${APP_SP_ID}" ] && [ "${APP_SP_ID}" != "None" ]; then
        echo "  service_principal_client_id absent; trying SCIM lookup on ${APP_SP_ID}..."
        SPN_JSON=$(databricks service-principals get "${APP_SP_ID}" "${PROFILE_ARG[@]}" --output json 2>&1) && \
            SPN_APP_ID=$(echo "${SPN_JSON}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('applicationId','') or '')" 2>/dev/null || echo "")
    fi
fi

if [ -z "${SPN_APP_ID}" ] || [ "${SPN_APP_ID}" = "None" ]; then
    echo "" >&2
    echo "WARNING: Could not resolve the app service principal's applicationId." >&2
    echo "  Grant UC permissions manually as a catalog admin (see the manual step in" >&2
    echo "  the header of this script). Find the applicationId under" >&2
    echo "  Admin Console > Service Principals > ${APP_NAME}." >&2
    exit 1
fi
echo "  App SPN applicationId: ${SPN_APP_ID}"

run_sql() {
    local stmt="$1"
    databricks api post /api/2.0/sql/statements "${PROFILE_ARG[@]}" \
        --json "{\"warehouse_id\": \"${WAREHOUSE_ID}\", \"statement\": \"${stmt}\", \"wait_timeout\": \"30s\"}" 2>&1
}
sql_state() { echo "$1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "PARSE_ERROR"; }

# --- Grant the UC privileges the app needs to read/write tables in the schema ---
echo ""
echo "=== Granting UC permissions to app SPN ==="
GRANT_STATEMENTS=(
    "GRANT USE CATALOG ON CATALOG \`${CATALOG}\` TO \`${SPN_APP_ID}\`"
    "GRANT USE SCHEMA ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
    "GRANT CREATE TABLE ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
    "GRANT SELECT ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
    "GRANT MODIFY ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${SPN_APP_ID}\`"
)
GRANT_FAIL=0
for STMT in "${GRANT_STATEMENTS[@]}"; do
    echo "  ${STMT}"
    RESULT=$(run_sql "${STMT}")
    if [ "$(sql_state "${RESULT}")" = "SUCCEEDED" ]; then echo "    OK"; else echo "    FAILED"; echo "    ${RESULT}"; GRANT_FAIL=1; fi
done
if [ "${GRANT_FAIL}" -eq 1 ]; then
    echo ""
    echo "WARNING: One or more GRANT statements failed. This is common when your user"
    echo "         does not have permission to grant USE CATALOG or manage schema-level"
    echo "         grants. The app will still start, but it needs these permissions to"
    echo "         read/write tables in ${CATALOG}.${SCHEMA}. Re-run this script as a"
    echo "         catalog admin, or apply the grants manually (see this script's header)."
    exit 0
fi

echo ""
echo "=== Grants complete ==="
