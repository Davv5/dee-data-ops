# Y3 — `services/bq-ingest/sql/marts.sql` retirement audit

_Authored 2026-04-29. Sets the contract for batched porting of `marts.sql` (6,064 lines, 45 tables/views) to dbt so dbt becomes sole owner of the Marts schema._

## Summary

- **45** tables/views currently written by `services/bq-ingest/sql/marts.sql`
- **15** have real production consumers → **PORT** to dbt
- **30** are write-only or duplicate/superseded → **DROP** (no port required)
- **~2,662 lines (44%)** of `marts.sql` retire on day 1 with no porting effort
- **~3,406 lines** need porting across follow-up batches

The largest single porting unit is `mart_master_lead_wide` (977 lines), the central wide table consumed by `warehouse_queries.py` (the bq-ingest analytical query catalog) and `phase1_release_gate.sql`. It is also the only `VIEW` (not table) in the live set.

## Method

A "real consumer" means a Python module or non-validation SQL file outside `marts.sql` that reads the table. The following sources are explicitly **not** consumers for the purpose of retirement:

- `marts.sql` itself (internal CTEs / downstream blocks)
- Validation harnesses: `mart_validation.sql`, `phase1_release_gate.sql`, `speed_to_lead_validation.sql`, `master_lead_reliability_gate.sql`, `services/bq-ingest/sql/validate/*`
- Inline SQL comments
- Documentation (`*.md`, `analyst.py` docstring catalog, RUNBOOK)

Indirect references via `_table_ref(MARTS_DATASET, '<name>')` in `identity_pipeline.py` and the `warehouse_healthcheck.py` / `warehouse_queries.py` query catalogs **are** consumers.

`bridge_setter_identity` is read by `marts.sql` but written by `services/bq-ingest/sources/identity/identity_pipeline.py`, not by `marts.sql` — out of Y3 scope.

## Verdict by table

Format: `<verdict> | <table> | <lines in marts.sql> | <consumer note>`

### PORT (15 tables, ~3,406 lines) — needs a dbt model with the same contract

| Table | Lines | Consumers (non-validation) |
|---|---|---|
| `mart_master_lead_wide` | 977 | `warehouse_queries.py` (3 queries); `phase1_release_gate.sql` (release gate) |
| `dim_golden_contact` | 435 | `warehouse_healthcheck.py`; RUNBOOK debug spine |
| `fct_speed_to_lead` | 422 | `identity_pipeline.py` (5 queries) |
| `fct_payment_line_unified` | 331 | `warehouse_queries.py` |
| `rpt_funnel_conversion_week` | 266 | `warehouse_queries.py` |
| `dim_team_members` | 195 | `identity_pipeline.py` (via `_table_ref`) |
| `rpt_unbooked_lead_quality_by_campaign` | 137 | `warehouse_queries.py` |
| `rpt_rep_scorecard_week` | 135 | `warehouse_queries.py` (2 queries) |
| `rpt_campaign_funnel_month` | 118 | `warehouse_queries.py`; `warehouse_healthcheck.py` |
| `rpt_appt_funnel_week` | 118 | `warehouse_queries.py` (2 queries) |
| `fct_fanbasis_payment_line` | 75 | `warehouse_healthcheck.py`; identity bridge |
| `bridge_contact_closer` | 67 | `warehouse_queries.py` |
| `rpt_revenue_by_stage_month` | 59 | `warehouse_queries.py` |
| `rpt_speed_to_lead_week` | 48 | `warehouse_queries.py` |
| `dim_closers` | 23 | `warehouse_queries.py` |

### DROP — orphan (27 tables, ~2,413 lines) — built but never read

Confirmed zero non-validation consumers. Validation gates that reference these tables are dropped together with the table.

| Table | Lines |
|---|---|
| `fct_lead_magnet_activity` | 342 |
| `fct_deal_attribution` | 240 |
| `rpt_operations_kpi_panel` | 150 |
| `rpt_setter_unbooked_conversion_week` | 147 |
| `rpt_fathom_outcomes_week` | 134 |
| `rpt_setter_pre_appt_outreach_week` | 133 |
| `rpt_call_to_booking_rate_week` | 121 |
| `rpt_fathom_closer_effectiveness_week` | 99 |
| `fct_ghl_rep_activity` | 98 |
| `rpt_show_rate_week` | 87 |
| `rpt_payment_reconciliation_month` | 81 |
| `rpt_closer_close_rate_week` | 79 |
| `fct_ghl_opportunities_historical_attribution` | 77 |
| `rpt_closer_speed_to_close_week` | 71 |
| `fct_ghl_opportunities_attributed` | 68 |
| `rpt_ghl_activity_week` | 63 |
| `rpt_closer_pipeline_health_week` | 63 |
| `rpt_cost_per_qualified_appt_month` | 50 |
| `fct_funnel_stage_transitions` | 49 |
| `mrt_speed_to_lead_overall` | 46 |
| `rpt_call_outcome_week` | 40 |
| `rpt_calendly_status_week` | 38 |
| `rpt_identity_quality_daily` | 38 |
| `mrt_speed_to_lead_daily` | 31 |
| `rpt_closer_revenue_month` | 28 |
| `rpt_applications_month` | 17 |
| `rpt_calendly_routing_week` | 14 |

`mrt_speed_to_lead_daily` and `mrt_speed_to_lead_overall` are the v1 Speed-to-Lead Dashboard tables — Metabase v1.6 was retired post-v1 (per `CLAUDE.local.md`), so no live consumer remains.

### DROP — duplicate of dbt model (1 table, 144 lines)

| Table | Lines | Replacement |
|---|---|---|
| `bridge_identity_contact_payment` | 144 | dbt `Core.bridge_identity_contact_payment` (`2-dbt/models/warehouse/bridges/`) already exists; only `phase1_release_gate.sql` and `mart_validation.sql` reference the `Marts.*` copy. Repoint validation to `Core.*`, then drop. |

### DROP — Stripe-superseded (1 table, 55 lines)

| Table | Lines | Reason |
|---|---|---|
| `rpt_stripe_lifecycle_month` | 55 | D-DEE has banned Stripe; Fanbasis is the live revenue processor (memory `project_stripe_historical_only.md`). No live consumer. |

### DROP — Fanbasis aggregate orphan (1 table, 50 lines)

| Table | Lines | Reason |
|---|---|---|
| `rpt_fanbasis_customer_month` | 50 | No live consumer; `fct_fanbasis_payment_line` (PORT) is the spine. |

## Batch plan

1. **Batch 1 — DROP orphans (1 PR).** Delete the 30 DROP blocks from `marts.sql` plus the validation gates that reference only those tables. Mass deletion, low risk, recovers ~44% of the file. No dbt work.
2. **Batch 2 — Repoint duplicate (1 PR).** Repoint `phase1_release_gate.sql` and `mart_validation.sql` from `Marts.bridge_identity_contact_payment` to `Core.bridge_identity_contact_payment`. Delete the `Marts` copy from `marts.sql`. Verify dbt's `Core` version produces equivalent rows.
3. **Batch 3 — Port small dims (1 PR).** `dim_closers` (23) + `dim_team_members` (195) + `bridge_contact_closer` (67). Build dbt models, repoint `warehouse_queries.py` and `identity_pipeline.py`, delete from `marts.sql`.
4. **Batch 4 — Port `dim_golden_contact` (1 PR).** 435 lines; affects `warehouse_healthcheck.py` and the RUNBOOK debug spine. Standalone PR because of size and central role.
5. **Batches 5..N — Port rpt tables (~6 PRs).** Group by consumer query in `warehouse_queries.py` so each PR repoints a coherent slice. Roughly: speed-to-lead (`fct_speed_to_lead` + `rpt_speed_to_lead_week`), funnel (`rpt_campaign_funnel_month` + `rpt_funnel_conversion_week` + `rpt_appt_funnel_week`), revenue (`fct_payment_line_unified` + `fct_fanbasis_payment_line` + `rpt_revenue_by_stage_month`), rep performance (`rpt_rep_scorecard_week`, `rpt_unbooked_lead_quality_by_campaign`).
6. **Batch N — Port `mart_master_lead_wide` (1 PR).** 977 lines, sole VIEW, consumed by `warehouse_queries.py` (3 queries) and the release gate. Largest single porting unit; ports last so all upstream pieces are already in dbt.
7. **Batch N+1 — Retire writer (1 PR).** Once `marts.sql` is empty: delete `services/bq-ingest/sql/marts.sql`, `services/bq-ingest/sources/marts/mart_models.py`, `run_marts_with_dependencies` from `tasks.py`, and the `pipeline-marts-hourly` Cloud Run Job. Final PR.

## Open questions

- **`dim_golden_contact` lineage.** The dbt warehouse layer has its own dimensions (`dim_contacts`, `dim_users`, `dim_offers`, etc.). Is `dim_golden_contact` materially distinct, or can the port also reshape consumers to use the existing dims? **Resolve before Batch 4.**
- **`mart_master_lead_wide` shape.** The view fans out from `dim_golden_contact` and 5+ rpt tables. Confirm whether the dbt port should be 1:1 (a single wide model) or split into smaller marts that the BI layer composes. **Resolve before final batch.**
- **Validation harnesses.** `phase1_release_gate.sql` and `mart_validation.sql` together check 14+ Marts tables. After Y3, these gates either (a) move to dbt tests, (b) get rewritten against the new dbt-built tables, or (c) retire entirely. Decide per gate during Batch 1.

## Won't-do (out of Y3 scope)

- `bridge_setter_identity` — written by `identity_pipeline.py`, not `marts.sql`. Separate refactor.
- Validation SQL files in `services/bq-ingest/sql/validate/` that don't reference any Y3 table.
- The bq-ingest Python tasks/runner architecture beyond the mart task itself.
