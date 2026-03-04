# Ultra Tiger

Ultra Tiger is a security-first, cost-aware agent platform prototype.

## Current Implementation

This repository now includes a foundational `ultra-core` Rust service that implements:

- Guardian preflight budget checks
- Daily spend tracking with key revocation behavior
- Permission gate checks for sensitive actions
- Guardian status + manual reset controls for operational recovery
- Axum HTTP API endpoints for health and control-plane actions
- In-memory autonomy queue primitives (enqueue, worker tick, dead-letter)
- Observability primitives (heartbeat + action logs)
- Initial memory abstractions (`MemoryStore`) and `SqliteMemoryStore` for persistence
- Wasmtime skill-host MVP primitives (manifest parser + capability validator + host-call policy checks)

## Run locally

```bash
cargo run -p ultra-core
```

Server starts on `0.0.0.0:3000`.

## API Endpoints

- `GET /health`
- `GET /guardian/status`
- `POST /guardian/preflight`
- `POST /guardian/spend`
- `POST /guardian/reset`
- `POST /guardian/permission-check`
- `GET /queue/status`
- `POST /queue/enqueue`
- `POST /queue/worker-tick`
- `GET /observability/heartbeat`
- `GET /observability/actions`

## Skills (Wasmtime MVP Primitives)

The `skill` module currently provides:

- `parse_manifest` for `Manifest.json` parsing
- `validate_capabilities` for allow-list capability validation
- `check_host_call_allowed` for host-call policy checks

Supported capability prefixes:

- `fs.read`
- `fs.write`
- `net.outbound`
- `browser.control`

## Persistence

A starter SQL schema is provided in `sql/memory_schema.sql`, and `SqliteMemoryStore` initializes and uses it for:

- memory records
- approval audit events
- guardian daily spend snapshots

## 24/7 Deployment (systemd)

A service template is included at `deployment/systemd/ultra-core.service`.

Example installation:

```bash
sudo cp deployment/systemd/ultra-core.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ultra-core
sudo systemctl status ultra-core
```

## Bridge Adapters

Bridge adapter scaffolds are available for:

- Telegram
- WhatsApp sidecar

These are stubs for connector integration and credentials wiring.
