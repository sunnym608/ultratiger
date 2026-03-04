# Ultra Tiger

Ultra Tiger is a security-first, cost-aware agent platform prototype.

## Current Implementation

This repository now includes a foundational `ultra-core` Rust service that implements:

- Guardian preflight budget checks
- Daily spend tracking with key revocation behavior
- Permission gate checks for sensitive actions
- Axum HTTP API endpoints for health and control-plane actions

## Run locally

```bash
cargo run -p ultra-core
```

Server starts on `0.0.0.0:3000`.

## API Endpoints

- `GET /health`
- `POST /guardian/preflight`
- `POST /guardian/spend`
- `POST /guardian/permission-check`
