# Wasmtime Skill Runtime (Design Note)

This document defines the initial design for running Ultra Tiger skills securely.

## Goals

- Execute untrusted third-party skills with strict isolation.
- Enforce least-privilege capability grants at runtime.
- Keep execution deterministic and auditable for HITL workflows.

## Runtime Model

1. Skill package includes:
   - `Manifest.json` (declared capabilities)
   - Wasm module (`.wasm`)
   - optional UI metadata
2. Ultra Core validates manifest against policy.
3. Host creates a Wasmtime store with only approved imports.
4. Skill executes with explicit host calls (`host_fs_read`, `host_http_request`, etc.).
5. Every privileged host call is logged for audit and optional approval.

## Capability Examples

- `fs.read:/documents/work`
- `fs.write:/documents/work/reports`
- `net.outbound:api.openai.com:443`
- `browser.control`

## Enforcement Notes

- Unknown capabilities are rejected.
- Missing required capability causes host call failure.
- High-risk capabilities trigger HITL approval gate before execution.

## Next Code Tasks

- Add `skill` module with:
  - manifest parser
  - capability validator
  - host-call authorization middleware
- Add integration test fixtures with safe and unsafe skills.
