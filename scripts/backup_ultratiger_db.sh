#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-/opt/ultratiger/ultra_core.db}"
BACKUP_DIR="${2:-/opt/ultratiger/backups}"
TS="$(date +%Y%m%d_%H%M%S)"
OUT="$BACKUP_DIR/ultra_core_$TS.sqlite3"

mkdir -p "$BACKUP_DIR"
cp "$DB_PATH" "$OUT"
echo "Backup created: $OUT"
