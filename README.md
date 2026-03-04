# Ultra Tiger

Ultra Tiger is a security-first, cost-aware agent platform prototype.

## Current Implementation

This repository now includes a foundational `ultra-core` Rust service that implements:

- Guardian preflight budget checks
- Daily spend tracking with key revocation behavior
- Permission gate checks for sensitive actions
- Guardian status + manual reset controls for operational recovery
- Axum HTTP API endpoints for health and control-plane actions

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

## Recommended Next Steps

1. Add persistence (SQLite) for budget state and approval audit logs.
2. Introduce provider adapters (Ollama/OpenAI/Anthropic) behind a trait-based interface.
3. Add Wasmtime-based skill runner with explicit capability grants.
4. Implement a React command-center UI (budget meter, heartbeat stream, approval queue).
5. Add CI checks (`cargo test`, `clippy`, `fmt`) and release pipelines.
