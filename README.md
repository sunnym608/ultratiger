# Ultra Tiger

Ultra Tiger is a security-first, cost-aware agent platform prototype.

## Current Implementation

This repository now includes a foundational `ultra-core` Rust service that implements:

- Guardian preflight budget checks
- Daily spend tracking with key revocation behavior
- Permission gate checks for sensitive actions
- Guardian status + manual reset controls for operational recovery
- Axum HTTP API endpoints for health and control-plane actions
- Phase-2 autonomous orchestrator with persistent SQLite task queue and dead-letter queue
- Background scheduler + worker pool for unattended 24/7 processing
- Exponential retry policy with jitter and max-attempt enforcement
- Dead-letter replay/requeue API
- Observability primitives (heartbeat + action logs)
- Initial memory abstractions (`MemoryStore`) and `SqliteMemoryStore` for persistence
- Phase-1 skill runtime implementation with Wasmtime engine/store/linker integration and capability-enforced host calls

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
- `GET /queue/dead-letter`
- `POST /queue/requeue`
- `POST /scheduler/register`
- `POST /scheduler/tick`
- `GET /observability/heartbeat`
- `GET /observability/actions`

## Autonomous Orchestrator (Phase 2)

- Task queue persisted in SQLite (`task_queue` table)
- Dead-letter queue persisted in SQLite (`dead_letter_queue` table)
- Scheduled jobs persisted in SQLite (`scheduled_jobs` table)
- Background workers run continuously using configurable concurrency (`worker_concurrency`)
- Retry policy uses exponential backoff + jitter (`retry_base_delay_seconds`, `retry_jitter_seconds`)

### Example: Register a recurring schedule (every 30 seconds)

```bash
curl -X POST http://127.0.0.1:3000/scheduler/register \
  -H 'content-type: application/json' \
  -d '{
    "id": "nightly-sync",
    "task_type": "sync",
    "payload": "{\"target\":\"workspace\"}",
    "trigger_kind": "every_seconds",
    "trigger_expr": "30",
    "max_attempts": 5
  }'
```

## Skill Runtime (Phase 1: Real Runtime)

The `skill` module now includes:

- signed package loading (`Manifest.json`, `skill.wasm`, `signature.sha256`)
- Wasmtime runtime integration (engine/store/linker + WASI)
- capability policy enforcement on host calls
- timeout + fuel + memory guardrails
- per-skill stdout/stderr + host-call logs

## 24/7 Deployment (systemd)

A service template is included at `deployment/systemd/ultra-core.service`.

Example installation:

```bash
sudo cp deployment/systemd/ultra-core.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ultra-core
sudo systemctl status ultra-core
```
