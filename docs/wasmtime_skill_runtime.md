# Wasmtime Skill Runtime (Phase 1 Implemented)

This document describes the implemented Phase-1 runtime and what follows next.

## Implemented in Phase 1

- Wasmtime engine/store/linker integration.
- Signed package loading from a directory containing:
  - `Manifest.json`
  - `skill.wasm`
  - `signature.sha256` (SHA-256 of wasm payload)
- Capability validation against allow-listed prefixes.
- Host-call policy enforcement for every shimmed call:
  - `host_fs_read`
  - `host_fs_write`
  - `host_http_request`
  - `host_browser_control`
- Runtime guardrails:
  - timeout wrapper around execution
  - fuel guard for interruption
  - memory size limiter
- Per-skill output and structured execution logs:
  - captured stdout/stderr from WASI pipes
  - host-call allow/deny log entries

## Runtime Flow

1. Load and parse `Manifest.json`.
2. Verify `skill.wasm` against `signature.sha256`.
3. Validate declared capabilities.
4. Build Wasmtime runtime and register host shims.
5. Execute `run` or fallback to `_start`.
6. Return stdout/stderr and host-call logs.

## Next (Phase 2+)

- Replace checksum-only signature with asymmetric signing/verification.
- Add strict path/domain-scoped capability checks (not prefix-only).
- Add richer host API ABI with argument marshalling and typed responses.
- Add integration tests with real wasm fixtures and malicious behavior cases.
