#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <new_version>"
  exit 1
fi

VERSION="$1"
"$(dirname "$0")/install_ultratiger.sh" "$VERSION"

sleep 2
curl -sf http://127.0.0.1:3000/health/readiness >/dev/null

echo "Upgrade successful: $VERSION"
