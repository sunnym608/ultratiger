# Technical Specification: Ultra Tiger

**Project Status:** Architectural Design & Core Implementation Phase  
**Core Philosophy:** Secure Autonomy, Financial Predictability, and Local-First Intelligence

## 1. Executive Summary

Ultra Tiger is a reimagined AI personal assistant designed to address the critical vulnerabilities and technical overhead of the original OpenClaw repository. While the original proved the power of "agentic OS" capabilities, it lacked a robust security sandbox and was prone to "bill shock" due to unoptimized token usage.

Ultra Tiger shifts the core from Node.js to Rust, introduces WebAssembly (Wasm) sandboxing, and integrates a Visual Flow Builder to make autonomous AI safe and accessible for power users.

## 2. Problem Statement & "Ultra" Solutions

| Problem in Original | Ultra Tiger Solution | Technical Implementation |
|---|---|---|
| Security risk | Capability-based isolation | Wasmtime sandboxing for all third-party skills |
| Credential theft | Secure enclave storage | OS keychain integration (Apple/Windows) via `keyring` crate |
| Bill shock | The "Guardian" interceptor | Token-cost pre-calculation with hard spending caps |
| Technical barrier | Visual logic orchestration | React Flow drag-and-drop dashboard[^1] |
| Server/mobile gap | 24/7 headless support | Axum backend served via Tailscale private tunnels |

## 3. System Architecture

Ultra Tiger follows a decoupled, multi-layered architecture to allow 24/7 background operation on servers while providing a rich desktop/mobile GUI.

### Layer 1: The Brain (Orchestration)

- **Logic engine:** Built in Rust (Tauri/Tokio). Rust handles the high-concurrency needs of an agent that monitors files, messages, and schedules simultaneously.
- **Model agnostic:** Seamless switching between local (Ollama) and cloud (OpenAI/Anthropic).
- **Memory:** Vector-based long-term memory utilizing a local SQLite-backed store for context persistence.

### Layer 2: The Guardian (Security & Cost)

- **Token interceptor:** Sits between the Brain and the LLM. It parses every outgoing request to estimate cost and checks against `Daily_Budget_Limit`.
- **Permission gate:** A Human-in-the-Loop (HITL) module that pauses execution for sensitive actions (for example, deleting a file) and waits for an approval ping from the user's phone.

### Layer 3: The Claws (Skill Sandbox)

- **Execution environment:** All skills (scripts) run in Wasmtime. This creates a "cold room" where code cannot access the host filesystem or network unless explicitly granted a capability token.
- **Capability scoping:** Permissions are granular (for example, `READ_ONLY: /Documents/Work`).

### Layer 4: The Bridges (Connectivity)

- **Telegram/WhatsApp:** Native Rust bridges using `teloxide` and a Go-based WhatsApp sidecar.
- **Remote UI:** An Axum-powered WebSocket server that allows users to access the dashboard on mobile via Tailscale without exposing ports to the public internet.

## 4. Implementation Details

### A. Bill Shock Prevention

- Every task initiated by the agent is assigned a `Max-Token-Budget`.
- **Pre-flight check:** If a task's estimated cost exceeds $0.10, the UI triggers an "Authorize?" modal.
- **Hard kill-switch:** If the total daily spend exceeds the user-defined limit, the Rust backend revokes all API keys until a manual reset is performed.

### B. The 24/7 "Headless" Mode

- Unlike standard Tauri apps, the Ultra Core is designed to run as a system service.
- **Desktop:** The GUI is a client for the Rust core.
- **Server:** On a headless Linux box, the core runs via `systemd`, serving the UI over a secure Tailscale IP.

### C. Skill Management

Skills are no longer just Markdown files. An Ultra Skill consists of:

- `Manifest.json`: Defines required permissions (filesystem, network, browser).
- `Logic` (Wasm/Python): The executable code.
- `UI Node`: A React component that appears in the Visual Flow Builder.

## 5. Development Roadmap

### Phase 1: The Secure Foundation *(Completed)*

- Initial project scaffolding with Tauri + React[^2]
- Implementation of the keyring vault for encrypted credential storage
- Development of the Rust Guardian module for budget monitoring

### Phase 2: Visual Command Center *(In Progress)*

- Integration of React Flow for visual agent orchestration[^3]
- Creation of the "Live Heartbeat" dashboard to monitor agent thoughts in real time

### Phase 3: Autonomous Bridges

- Deployment of the Telegram and WhatsApp sidecar services
- Implementation of the "Permission Push" notification system for mobile approval of agent actions

### Phase 4: Production & Deployment

- Bundling for multi-platform distribution (`.dmg`, `.exe`)
- Setup of GitHub Actions for automated CI/CD and security auditing of core binaries

## 6. Conclusion

Ultra Tiger is designed to be the professional-grade alternative to existing autonomous agents. By prioritizing security sandboxing and cost control, it moves the concept of a personal AI assistant from a risky experiment to a reliable, 24/7 digital employee.

---

[^1]: React Flow docs and ecosystem references.
[^2]: Tauri + React scaffolding as project baseline.
[^3]: React Flow integration as the visual orchestration substrate.
