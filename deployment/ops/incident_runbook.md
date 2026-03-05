# Ultra Tiger Incident Runbook

## Severity
- **SEV-1:** Core API down or data-loss suspected.
- **SEV-2:** Partial degradation (queue backlog growth, bridge failures, approval delays).
- **SEV-3:** Non-critical defects or cosmetic UX issues.

## First 5 Minutes
1. Check service status:
   ```bash
   systemctl status ultra-core
   ```
2. Check readiness/liveness:
   ```bash
   curl -sf http://127.0.0.1:3000/health/liveness
   curl -sf http://127.0.0.1:3000/health/readiness
   ```
3. Check queue and dead-letter pressure:
   ```bash
   curl -sf http://127.0.0.1:3000/queue/status
   curl -sf http://127.0.0.1:3000/queue/dead-letter
   ```
4. Check logs:
   ```bash
   journalctl -u ultra-core -n 200 --no-pager
   ```

## Data Safety Checks
- Verify SQLite database file exists and is writable.
- Run backup script before risky mitigation:
  ```bash
  ./scripts/backup_ultratiger_db.sh
  ```

## Common Recovery Actions
- Restart service:
  ```bash
  sudo systemctl restart ultra-core
  ```
- Roll back release:
  ```bash
  sudo ./scripts/rollback_ultratiger.sh <previous_version>
  ```
- Requeue dead-letter tasks after root-cause fix:
  ```bash
  curl -X POST http://127.0.0.1:3000/queue/requeue \
    -H 'content-type: application/json' \
    -d '{"max_items":100}'
  ```

## Post-Incident
- Capture timeline from audit + task timeline APIs.
- Document root cause, blast radius, and permanent fix.
- Add/adjust an alert or policy to prevent recurrence.
