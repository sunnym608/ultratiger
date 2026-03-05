#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-dev}"
ROOT="/opt/ultratiger"
RELEASE_DIR="$ROOT/releases/$VERSION"
CURRENT_LINK="$ROOT/current"
SERVICE_FILE="/etc/systemd/system/ultra-core.service"

mkdir -p "$RELEASE_DIR"
mkdir -p "$ROOT/shared"

if [[ ! -f "target/release/ultra-core" ]]; then
  echo "Building ultra-core release binary..."
  cargo build --release -p ultra-core
fi

install -m 0755 target/release/ultra-core "$RELEASE_DIR/ultra-core"

if [[ ! -f "$ROOT/shared/.env" ]]; then
  cat > "$ROOT/shared/.env" <<ENV
RUST_LOG=ultra_core=info
ENV
fi

ln -sfn "$RELEASE_DIR" "$CURRENT_LINK"

cat > "$SERVICE_FILE" <<SERVICE
[Unit]
Description=Ultra Tiger Core Service
After=network.target

[Service]
Type=simple
WorkingDirectory=$ROOT/current
ExecStart=$ROOT/current/ultra-core
Restart=always
RestartSec=5
EnvironmentFile=-$ROOT/shared/.env
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$ROOT

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable ultra-core
systemctl restart ultra-core

echo "Installed Ultra Tiger version: $VERSION"
echo "Check status with: systemctl status ultra-core"
