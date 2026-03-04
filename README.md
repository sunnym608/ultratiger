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
- **Phase-3 memory + context engine**:
  - chunking + deterministic embedding pipeline
  - vector storage with provenance metadata
  - hybrid retrieval (semantic + keyword ranking)
  - memory TTL purge + delete policy endpoints
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
- `POST /memory/ingest`
- `GET /memory/query`
- `POST /memory/retrieve`
- `POST /memory/purge`
- `DELETE /memory/:id`
- `POST /bridges/telegram/webhook`
- `POST /bridges/whatsapp/webhook`
- `POST /bridges/reply`
- `GET /bridges/health`
- `GET /observability/heartbeat`
- `GET /observability/actions`

## Memory + Context Engine (Phase 3)

### Ingest memory with chunking + embeddings

```bash
curl -X POST http://127.0.0.1:3000/memory/ingest \
  -H 'content-type: application/json' \
  -d '{
    "session_id": "s-001",
    "source": "chat",
    "content": "Ultra Tiger persists memory and retrieves relevant context.",
    "model": "deterministic-v1",
    "chunk_size": 128
  }'
```

### Hybrid retrieval with citations

```bash
curl -X POST http://127.0.0.1:3000/memory/retrieve \
  -H 'content-type: application/json' \
  -d '{
    "query": "how does Ultra Tiger retrieve context",
    "session_id": "s-001",
    "top_k": 5,
    "model": "deterministic-v1"
  }'
```

### Retention policy purge

```bash
curl -X POST http://127.0.0.1:3000/memory/purge \
  -H 'content-type: application/json' \
  -d '{"ttl_seconds": 604800}'
```


## Bridges (Phase 4)

Implemented bridge capabilities:

- real outbound clients for Telegram/WhatsApp via HTTP APIs
- inbound webhook normalization into queued `bridge.inbound` tasks
- outbound reply channel with retry + backoff via `bridge.reply` tasks
- signature verification (`sha256(secret:payload)`) for webhook protection
- per-bridge rate limiting and health metrics

Required environment variables:

- `ULTRA_TELEGRAM_BOT_TOKEN`
- `ULTRA_TELEGRAM_SIGNING_SECRET`
- `ULTRA_WHATSAPP_API_URL`
- `ULTRA_WHATSAPP_ACCESS_TOKEN`
- `ULTRA_WHATSAPP_SIGNING_SECRET`
- `ULTRA_BRIDGE_RATE_LIMIT_PER_MINUTE`
- `ULTRA_BRIDGE_OUTBOUND_MAX_RETRIES`

## 24/7 Deployment (systemd)

A service template is included at `deployment/systemd/ultra-core.service`.
