# Ultra Tiger

Ultra Tiger is a security-first, cost-aware agent platform prototype.

## Current Implementation

This repository now includes a foundational `ultra-core` Rust service that implements:

- Guardian preflight budget checks
- Daily spend tracking with key revocation behavior
- Permission gate checks for sensitive actions
- Guardian status + manual reset controls for operational recovery
- Axum HTTP API endpoints for health and control-plane actions
- Initial memory abstractions (`MemoryStore`) plus an in-memory implementation for local testing

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

## Memory Persistence Direction (SQLite vs Qdrant/PageIndex)

- **Operational state** (guardian budgets, approvals, audits) should remain in SQLite.
- **Long-term semantic memory** should use a vector-friendly store:
  - Start local with SQLite + vector extension or embedding blob table.
  - Move to Qdrant when retrieval scale, filtering, or multi-node sync becomes important.
- **PageIndex-style retrieval** is useful for strict document provenance, but should be layered on top of semantic retrieval, not used as a full replacement.

A starter SQL schema is provided in `sql/memory_schema.sql`.

## What is the Wasmtime Skill Runtime?

Wasmtime is the WebAssembly runtime used to execute skills in a sandbox:

- skills run in isolated Wasm modules
- no filesystem/network access by default
- host capabilities are explicitly granted per skill
- runtime policy can block dangerous operations before they affect the host

This is the core of Ultra Tiger's "capability-based isolation" model.

## Recommended Next Steps

1. Implement a `SqliteMemoryStore` using the schema in `sql/memory_schema.sql`.
2. Add embedding generation + retrieval traits (`Embedder`, `Retriever`) and keep provider implementations swappable.
3. Add Wasmtime skill host with capability tokens (`fs.read`, `fs.write`, `net.outbound`, `browser.control`).
4. Persist Guardian daily spend + approvals using the same local database.
5. Add CI checks (`cargo test`, `clippy`, `fmt`) and release pipelines.
