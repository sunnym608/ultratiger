# SLO and Error Budget Policy

## Availability SLO
- **Objective:** 99.9% monthly uptime for Ultra Core API.
- **Measurement window:** rolling 30 days.

## Error Budget
- Allowed downtime per 30-day month at 99.9%: **43m 12s**.

## Policy
- If burn-rate exceeds 50% of monthly budget in first half of month:
  - freeze non-critical feature deploys,
  - prioritize reliability bugs and runbook improvements.
- If budget is exhausted:
  - reliability-only changes until service stabilizes.

## Core Indicators
- `/health/liveness` and `/health/readiness` availability.
- Queue backlog trend and dead-letter growth.
- Approval latency and bridge delivery error rate.
