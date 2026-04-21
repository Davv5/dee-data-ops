# Session Handover — Track Q: release-gate severity flip (warn → error)

**Branch:** `Davv5/Track-Q-Release-Gate-Severity-Flip`
**Timestamp:** `2026-04-21_10-04` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## ⚠ GATING CONDITION

**Do not execute this track until all three release-gate tests are passing at `severity: 'warn'` for at least one consecutive week.** The gates were lowered during the initial warehouse load because the data hadn't caught up to the oracle snapshot (2026-03-19). Flipping back to `'error'` while parity is still missing will red-bar every CI run.

Verification before execution:
```bash
bq query --use_legacy_sql=false "
  SELECT
    'sales_activity_detail' AS mart, COUNT(*) AS rows, 3141 AS oracle,
    ABS(COUNT(*) - 3141) / 3141 AS delta_pct
  FROM dee-data-ops-prod.marts.sales_activity_detail
  UNION ALL
  SELECT 'lead_journey', COUNT(*), 15598, ABS(COUNT(*) - 15598) / 15598
  FROM dee-data-ops-prod.marts.lead_journey
  UNION ALL
  SELECT 'revenue_detail', COUNT(*), 1423, ABS(COUNT(*) - 1423) / 1423
  FROM dee-data-ops-prod.marts.revenue_detail
"
```
All three `delta_pct` values must be < 0.05 (±5%). If any exceeds, **STOP AND ASK**.

---

## Session goal

Flip the three release-gate singular tests from `severity: 'warn'` back to `'error'` (dbt default). These are the tripwires that keep mart drift from landing silently; they're only useful if they actually fail the build.

## Changed files (expected)

```
dbt/tests/release_gate_sales_activity_detail.sql   — edited — remove severity override
dbt/tests/release_gate_lead_journey.sql            — edited — remove severity override
dbt/tests/release_gate_revenue_detail.sql          — edited — remove severity override
WORKLOG.md                                          — edited — dated entry
```

## Tasks

- [ ] Run the gating-condition query above; confirm all three marts within ±5% of oracle
- [ ] For each of the three test files, locate the `{{ config(severity='warn') }}` block (or `{% if severity %}` — verify exact shape) and remove the override so the test falls back to dbt's default (`error`)
- [ ] Run `dbt build --target dev --select release_gate_sales_activity_detail release_gate_lead_journey release_gate_revenue_detail` — all three must pass green
- [ ] Run `dbt build --target ci --select state:modified+` locally (or confirm in CI on the PR) — no new failures
- [ ] Append WORKLOG entry with the three delta_pct values that authorized the flip
- [ ] Run `/handover`
- [ ] Commit locally

## Decisions already made

- **All three flip together.** Don't flip one and leave the others on warn — that creates inconsistent enforcement across the mart set.
- **No tolerance widening.** The original ±5% tolerance stays; if data drifts, the test catches it.

## Open questions

- What if one mart is within ±5% and another isn't? **STOP AND ASK** — don't partial-flip without David's call.

## Done when

- `grep -rn "severity='warn'" dbt/tests/release_gate_*.sql` returns zero hits
- Local `dbt test --select release_gate_*` passes
- CI on the branch passes
- WORKLOG entry cites the authorizing delta_pct numbers

## Context links

- Track F, L, M handovers (the original releases) — they set severity to warn with commit messages explaining "data hasn't caught up"
- Oracle: `dbt/seeds/validation/oracle_dashboard_metrics_20260319.csv`
- `.claude/rules/warehouse.md` — release-gate pattern
