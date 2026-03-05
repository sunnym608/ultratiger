#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <previous_version>"
  exit 1
fi

ROOT="/opt/ultratiger"
PREV="$1"
PREV_DIR="$ROOT/releases/$PREV"

if [[ ! -x "$PREV_DIR/ultra-core" ]]; then
  echo "Version not found: $PREV_DIR/ultra-core"
  exit 1
fi

ln -sfn "$PREV_DIR" "$ROOT/current"
systemctl restart ultra-core
sleep 2
curl -sf http://127.0.0.1:3000/health/readiness >/dev/null

echo "Rollback successful: $PREV"
