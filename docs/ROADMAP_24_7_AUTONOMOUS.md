# Ultra Tiger Roadmap: From Current State to True 24/7 Autonomy

## Target Definition (Exit Criteria)

### Reliability
- 99.9% monthly uptime SLO for core API.
- 0 data-loss objective for queue and audit records during restart/crash scenarios.

### Autonomy
- User-defined workflows run continuously.
- Retries with jitter/backoff are enforced by policy.
- Dead-letter replay path is operational and observable.

### Safety
- Guardian budget checks enabled by default.
- Approval flow enabled for sensitive actions by default.
- Policy gates (capabilities + approval + budget) enforced at runtime.

### Operability
- One-command install/upgrade/rollback available for operators.
- Health/readiness/liveness checks integrated with service manager.
- Alerting + incident runbook available and tested.

### User Experience
- Onboarding wizard flow documented and implemented incrementally.
- Flow-builder UX for creating autonomous workflows.
- Actionable notifications for approvals/failures.
- “Why did the agent do this?” timeline and rationale trace.

---

## Milestone Plan

## M1 — Reliability & Operability Baseline
**Goal:** production-safe headless runtime.

### Deliverables
- Graceful shutdown + readiness drain (implemented).
- Systemd hardening + restart policy (implemented baseline).
- Backup/restore scripts for state safety.
- Log rotation config.
- Incident runbook and SLO error-budget policy.

### Acceptance Criteria
- Service survives restart without queue/audit loss in test drill.
- Readiness returns `503` during drain.
- Backup + restore dry-run verified.

## M2 — One-command Lifecycle Management
**Goal:** install/upgrade/rollback in one operator command each.

### Deliverables
- `scripts/install_ultratiger.sh`
- `scripts/upgrade_ultratiger.sh`
- `scripts/rollback_ultratiger.sh`
- Versioned release dir layout (`/opt/ultratiger/releases/<version>` + `current` symlink).

### Acceptance Criteria
- Fresh host install in <10 minutes.
- Upgrade with service health check.
- Rollback restores previous known-good binary and service state.

## M3 — Workflow Autonomy Core
**Goal:** explicit user-defined workflows, 24/7 execution.

### Deliverables
- Workflow model + CRUD.
- Trigger engine (schedule/webhook/bridge events).
- Idempotency keys, retry/backoff policy templates.
- Dead-letter replay UX and API parity.

### Acceptance Criteria
- Workflow executes continuously for 72h soak run.
- Failed tasks route to dead-letter and can be replayed.

## M4 — Safety by Default
**Goal:** no dangerous autonomous execution without policy gates.

### Deliverables
- Policy presets (safe, balanced, advanced).
- Approval channels (UI + bridge notification).
- Budget and approval policy defaults in onboarding.

### Acceptance Criteria
- Sensitive actions always require explicit user approval unless policy overrides exist.
- Budget exceed path revokes action execution path until reset.

## M5 — User-Friendly Product UX
**Goal:** non-technical users can onboard and automate quickly.

### Deliverables
- Onboarding wizard.
- Flow-builder UX.
- Notification center with actionable events.
- Explainability timeline (“why this action”).

### Acceptance Criteria
- First successful automation created by new user in <15 minutes median.
- 90%+ onboarding completion in user tests.

## M6 — Production SRE & Compliance Readiness
**Goal:** enterprise-grade 24/7 operation.

### Deliverables
- SLO dashboard + alerts.
- Incident response drills.
- Audit export/retention controls.
- Security review and threat model refresh.

### Acceptance Criteria
- 99.9% uptime met for two consecutive months.
- MTTR below target threshold from runbook drills.

---

## End-to-End User Stories

1. **Install & Start**
   - As an operator, I can install Ultra Tiger with one command and have the service running with health checks enabled.
2. **Safe Autonomy**
   - As a user, I can enable autonomous workflows with default safety controls (budget + approvals + policy gates).
3. **Continuous Execution**
   - As a user, my workflows run continuously, and failures are retried automatically before dead-lettering.
4. **Recovery**
   - As an operator, I can restore state from backup without losing audit history.
5. **Transparency**
   - As a user, I can see why each autonomous action happened in a timeline with rationale.
6. **Control**
   - As a user, I can approve/reject sensitive actions from UI or bridge notifications.
7. **Upgrade Confidence**
   - As an operator, I can upgrade and rollback safely using documented scripts.

---

## Agile Execution (Recommended)
- Sprint length: 2 weeks.
- Required sprint mix: reliability + UX + safety item every sprint.
- Definition of done:
  - feature implemented,
  - tests/checks pass,
  - docs/runbook updated,
  - observability hooks added.
