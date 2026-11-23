#!/bin/bash
# Fail if any tracked (or staged) file references an internal package proxy.
#
# Local tooling on Databricks machines routes pip/uv/npm through internal
# proxies, and lockfiles record those hosts (e.g. package-lock.json "resolved"
# URLs). This repo is public and its users can't reach those hosts, so they must
# never be committed. After `npm install`, rewrite package-lock.json hosts to
# https://registry.npmjs.org/ before committing.
#
# Usage:
#   scripts/check_no_internal_urls.sh            # scan all tracked files
#   scripts/check_no_internal_urls.sh --staged   # scan the staged index only

set -euo pipefail
cd "$(dirname "$0")/.."

# Built from parts so this script doesn't match itself.
PATTERN='\.dev''\.databricks\.com'

if [ "${1:-}" = "--staged" ]; then
  hits=$(git grep --cached -nIE "$PATTERN" -- . || true)
else
  hits=$(git grep -nIE "$PATTERN" -- . || true)
fi

if [ -n "$hits" ]; then
  echo "ERROR: internal proxy host found in tracked files:" >&2
  echo "$hits" | cut -c1-200 | head -20 >&2
  echo "Rewrite to the public registry (registry.npmjs.org / pypi.org) or untrack the file." >&2
  exit 1
fi
echo "check_no_internal_urls: OK"
