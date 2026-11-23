#!/bin/bash
# Ruff lint + format for dbxredact.
#
# Runs a PINNED ruff version (via uvx) using the [tool.ruff] config in
# pyproject.toml, so results are identical across machines regardless of any
# locally installed ruff. Keep RUFF_VERSION in sync with the rev pinned in
# .pre-commit-config.yaml.
#
# Usage:
#   scripts/lint.sh              # check staged Python files (default)
#   scripts/lint.sh --fix        # check + autofix + format staged files
#   scripts/lint.sh --all        # check every tracked Python file
#   scripts/lint.sh --fix --all  # fix + format the whole repo (big diff!)
#   scripts/lint.sh path/to/file.py [more.py ...]  # explicit files

set -euo pipefail

RUFF_VERSION="0.14.0"
cd "$(dirname "$0")/.."

FIX=0
SCOPE="staged"
FILES=()

for arg in "$@"; do
  case "$arg" in
    --fix) FIX=1 ;;
    --all) SCOPE="all" ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \?//'; exit 0 ;;
    *) FILES+=("$arg"); SCOPE="explicit" ;;
  esac
done

# Resolve the target file list.
# (Read into an array with a portable loop -- macOS ships bash 3.2 which has
# no `mapfile`.)
if [ "$SCOPE" = "explicit" ]; then
  : # FILES already populated
else
  if [ "$SCOPE" = "all" ]; then
    _list=$(git ls-files '*.py')
  else
    # staged: added/copied/modified Python files in the index
    _list=$(git diff --cached --name-only --diff-filter=ACM -- '*.py')
  fi
  while IFS= read -r _f; do
    [ -n "$_f" ] && FILES+=("$_f")
  done <<< "$_list"
fi

if [ "${#FILES[@]}" -eq 0 ]; then
  echo "lint: no Python files to check (scope=$SCOPE). Nothing to do."
  exit 0
fi

echo "lint: ruff $RUFF_VERSION on ${#FILES[@]} file(s) (scope=$SCOPE, fix=$FIX)"

if [ "$FIX" -eq 1 ]; then
  uvx "ruff@${RUFF_VERSION}" check --fix "${FILES[@]}"
  uvx "ruff@${RUFF_VERSION}" format "${FILES[@]}"
else
  uvx "ruff@${RUFF_VERSION}" check "${FILES[@]}"
  uvx "ruff@${RUFF_VERSION}" format --check "${FILES[@]}"
fi
