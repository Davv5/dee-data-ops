# Y3 — `services/bq-ingest/sql/marts.sql` retirement audit

_Authored 2026-04-29. Revised 2026-04-29 evening after multi-persona doc review surfaced production-breaking gaps in the original verdict tables. Sets the contract for batched retirement of `marts.sql` (6,064 lines, 45 tables/views written) so dbt-built tables replace its analytical surface and the `pipeline-marts-hourly` Cloud Run Job retires._

## Goal — restated

**Retire `services/bq-ingest/sql/marts.sql` and its Cloud Run Job (`pipeline-marts-hourly`)** so dbt becomes the only writer that runs through `marts.sql`'s blocks. The earlier framing — "dbt becomes sole owner of the Marts schema" — overstates the scope: `services/bq-ingest/sources/identity/identity_pipeline.py` ALSO writes 5 Marts tables/views (`bridge_setter_identity`, `mart_speed_to_lead_enriched`, `rpt_setter_identity_unknown_queue`, `rpt_setter_identity_coverage_daily`, `v_unified_dashboard_schema`). Full schema ownership is a follow-on Y4; Y3 retires the `marts.sql` writer specifically.

## Summary

- **45** tables/views currently written by `marts.sql`.
- **15 PORT** — real production consumers exist; need a dbt-built equivalent before the `marts.sql` block can drop.
- **23 DROP — confirmed orphan.** Zero live consumers found (~1,706 lines).
- **4 DROP — cascade-hold** (~709 lines). PORT tables read these inside `marts.sql`; can only drop *after* the consuming PORT lands in dbt.
- **5 HOLD — analyst.py /ask disposition** (~458 lines, 2 overlap with cascade-hold above so net 7 unique problem tables). Listed in `SCHEMA_CONTEXT` (line 29 of `services/bq-ingest/sources/shared/analyst.py`), which is fed to Gemini at runtime for the live `POST /ask` route in `app.py:282`. NOT documentation. Resolution: either elevate to PORT and update SCHEMA_CONTEXT in lockstep, OR retire `/ask` as part of Y3 and trim SCHEMA_CONTEXT to surviving tables.
- **1 DROP — duplicate-of-dbt** (gated on parity test). `Marts.bridge_identity_contact_payment` is NOT byte-equivalent to dbt's `Core.bridge_identity_contact_payment`: Marts = 144 lines / 3 match tiers (`email_exact`, `email_canonical`, `phone_last10`); Core = 320 lines / 5 match tiers (adds `billing_email_direct`, `unmatched`). Repointing changes the contract `phase1_release_gate.sql` and `mart_validation.sql` see.
- **2 DROP — superseded** (`rpt_stripe_lifecycle_month`, `rpt_fanbasis_customer_month`).

The largest single PORT unit is `mart_master_lead_wide` (977 lines), the central wide VIEW consumed by `warehouse_queries.py` (3 queries) and `phase1_release_gate.sql`. Whether the dbt port stays a 1:1 wide model or splits into composable marts is unresolved (Open Question) and materially affects the final batch's effort.

## Method

A "real consumer" is anything that **reads** a Marts table from outside `marts.sql` itself. The original audit's filter (literal `Marts.<table>` reference in a non-validation file) missed:

- **`SCHEMA_CONTEXT` in `services/bq-ingest/sources/shared/analyst.py`** — runtime LLM prompt fed to Gemini for the `/ask` endpoint. Lists 10 tables in plain-English schema docs; Gemini emits SQL referencing them. This is a runtime consumer despite being plain text — the LLM sees the catalog and routes queries against it.
- **Internal cascade dependencies inside `marts.sql`** — PORT blocks that FROM-clause DROP-targeted blocks. Dropping the upstream first breaks the survivor.

Excluded as consumers (unchanged from the original framing):

- `marts.sql` internal CTEs that are NOT cross-block
- Validation harnesses (`mart_validation.sql`, `phase1_release_gate.sql`, `speed_to_lead_validation.sql`, `master_lead_reliability_gate.sql`, `services/bq-ingest/sql/validate/*`) — readers, but their disposition is part of Y3 not pre-condition
- Inline SQL comments
- Pure documentation (`*.md`, RUNBOOK)

**`bridge_setter_identity` and 4 other Marts tables/views written by `identity_pipeline.py` are out of Y3 scope** (see "Won't-do — Y4 follow-on" below).

**Caveat — ad-hoc human reads.** This audit only catches static refs in the codebase. David's BQ-console reads (e.g., for client prep) won't appear in any grep. An `INFORMATION_SCHEMA.JOBS_BY_PROJECT` scan over the last 30–90 days would surface them. Listed in Open Questions; recommend resolving before Batch 1 to convert "no automated consumer" → "truly orphan."

## Verdict by table

### PORT (15 tables, ~3,406 lines) — needs a dbt model with the same contract

| Table | Lines | Consumers (production code) |
|---|---|---|
| `mart_master_lead_wide` | 977 | `warehouse_queries.py` (3 queries); `phase1_release_gate.sql` |
| `dim_golden_contact` | 435 | `warehouse_healthcheck.py`; RUNBOOK debug spine |
| `fct_speed_to_lead` | 422 | `identity_pipeline.py` (5 queries via `_table_ref`) |
| `fct_payment_line_unified` | 331 | `warehouse_queries.py` |
| `rpt_funnel_conversion_week` | 266 | `warehouse_queries.py` |
| `dim_team_members` | 195 | `identity_pipeline.py` (via `_table_ref`) |
| `rpt_unbooked_lead_quality_by_campaign` | 137 | `warehouse_queries.py` |
| `rpt_rep_scorecard_week` | 135 | `warehouse_queries.py` (2 queries) |
| `rpt_campaign_funnel_month` | 118 | `warehouse_queries.py`; `warehouse_healthcheck.py` |
| `rpt_appt_funnel_week` | 118 | `warehouse_queries.py` (2 queries) |
| `fct_fanbasis_payment_line` | 75 | `warehouse_healthcheck.py` |
| `bridge_contact_closer` | 67 | `warehouse_queries.py` |
| `rpt_revenue_by_stage_month` | 59 | `warehouse_queries.py` |
| `rpt_speed_to_lead_week` | 48 | `warehouse_queries.py` |
| `dim_closers` | 23 | `warehouse_queries.py` |

### DROP — confirmed orphan (23 tables, ~1,706 lines)

Zero non-validation consumers AND no internal cascade dependency. Safe to delete in Batch 1 alongside their dedicated validation blocks.

| Table | Lines |
|---|---|
| `fct_deal_attribution` | 240 |
| `rpt_operations_kpi_panel` | 150 |
| `rpt_fathom_outcomes_week` | 134 |
| `rpt_fathom_closer_effectiveness_week` | 99 |
| `fct_ghl_rep_activity` | 98 |
| `rpt_show_rate_week` | 87 |
| `rpt_payment_reconciliation_month` | 81 |
| `fct_ghl_opportunities_historical_attribution` | 77 |
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
| `rpt_applications_month` | 17 |
| `rpt_calendly_routing_week` | 14 |
| `rpt_stripe_lifecycle_month` | 55 |
| `rpt_fanbasis_customer_month` | 50 |
| `bridge_identity_contact_payment` (Marts copy) | 144 |

`mrt_speed_to_lead_daily` / `mrt_speed_to_lead_overall` were the v1 Speed-to-Lead Dashboard tables; Metabase v1.6 retired post-v1 (per `CLAUDE.local.md`). `rpt_stripe_lifecycle_month` — no live consumer; Stripe data is historical-only in `Core.fct_stripe_*` (still queryable directly if a future cohort comparison wants it). `rpt_fanbasis_customer_month` — no live consumer; `fct_fanbasis_payment_line` (PORT) is the spine. **`bridge_identity_contact_payment` is listed here for the per-table count but its actual deletion is gated** — see "DROP — duplicate-of-dbt" below.

### DROP — cascade-hold (4 tables, ~709 lines) — only after consumer ports

These are reachable only because PORT blocks inside `marts.sql` read them. Cannot drop in Batch 1; must drop AFTER the consuming PORT moves to dbt and `marts.sql` no longer reads them.

| Table | Lines | Cascade consumer (PORT) inside `marts.sql` | Drop after |
|---|---|---|---|
| `fct_lead_magnet_activity` | 342 | `fct_speed_to_lead` (line 2853); `rpt_unbooked_lead_quality_by_campaign` (line 5439) | Both PORTs land in dbt |
| `rpt_setter_unbooked_conversion_week` | 147 | `rpt_rep_scorecard_week` (line 5794) | `rpt_rep_scorecard_week` PORT lands |
| `rpt_call_to_booking_rate_week` | 121 | `rpt_rep_scorecard_week` (line 5767) | `rpt_rep_scorecard_week` PORT lands |
| `rpt_closer_close_rate_week` | 79 | `rpt_rep_scorecard_week` (lines 5741, 5808) | `rpt_rep_scorecard_week` PORT lands |

When the PORTs land in dbt, the dbt model owns the join logic — these upstream tables don't need to exist in BigQuery anymore, so the `marts.sql` blocks can drop together with the consumer's `marts.sql` block in the same PR.

### HOLD — analyst.py /ask disposition (5 tables, ~458 lines; 2 overlap with cascade-hold)

`SCHEMA_CONTEXT` (line 29 of `services/bq-ingest/sources/shared/analyst.py`) is fed to Gemini at runtime for the live `/ask` endpoint (`app.py:282`). It lists 10 Mart tables Gemini is allowed to query. Five are on the original DROP list:

| Table | Lines | Cascade-hold? |
|---|---|---|
| `rpt_setter_pre_appt_outreach_week` | 133 | no |
| `rpt_setter_unbooked_conversion_week` | 147 | yes (also reached by `rpt_rep_scorecard_week`) |
| `rpt_closer_close_rate_week` | 79 | yes (also reached by `rpt_rep_scorecard_week`) |
| `rpt_closer_speed_to_close_week` | 71 | no |
| `rpt_closer_revenue_month` | 28 | no |

Two unique disposition paths:

- **(A) /ask is live and stays.** Reclassify these 5 to PORT (need dbt-built equivalents); update `SCHEMA_CONTEXT` to point at the dbt-built tables in lockstep with the PORTs.
- **(B) /ask is unused or retiring.** Retire the `/ask` route in `app.py:282`, delete `services/bq-ingest/sources/shared/analyst.py`, drop the 5 tables (they fold into DROP-orphan).

Resolution requires verifying live `/ask` traffic. **Default until verified: treat as live consumer** (path A's classification, even if path B is what actually ships). Verification: `gcloud logging read 'resource.type="cloud_run_revision" AND httpRequest.requestUrl=~"/ask"' --limit=50 --freshness=30d` against `bq-ingest`.

### DROP — duplicate-of-dbt, parity-gated (1 table, 144 lines)

`Marts.bridge_identity_contact_payment` (in `marts.sql` lines 684–827) and `Core.bridge_identity_contact_payment` (`2-dbt/models/warehouse/bridges/bridge_identity_contact_payment.sql`, 320 lines) are NOT byte-equivalent:

| Aspect | Marts copy | Core copy (dbt) |
|---|---|---|
| Lines | 144 | 320 |
| Match tiers | 3 (`email_exact`, `email_canonical`, `phone_last10`) | 5 (adds `billing_email_direct`, `unmatched`) |
| Match-method enum | 3 values | 5 values |

Repointing `phase1_release_gate.sql` and `mart_validation.sql` from `Marts.*` to `Core.*` silently changes:
- Row counts (Core may emit `unmatched` rows; Marts skips them entirely)
- `match_method` enum distribution (Core surfaces `billing_email_direct`)
- Any consumer that filters on `match_method` (e.g., `phase1_release_gate.sql:351-353` row-count check)

**Before Batch 2, run a parity test** comparing row counts and `match_method` distribution. If the difference is within a documented tolerance, proceed with the repoint+drop; otherwise, decide whether to widen Marts to match Core (port the 2 missing tiers) or update consumers to handle the new contract. The `altimate-data-parity` skill is built for this.

## Validation-harness disposition (resolved during the audit, not deferred)

`mart_validation.sql` (298 lines), `phase1_release_gate.sql` (506 lines), and `speed_to_lead_validation.sql` (~120 lines) each reference a MIX of PORT + DROP tables, plus tables outside Y3 scope. "Drop the gate together with the table" is wrong at file granularity — these need surgical block-level edits.

Per-file disposition:

- **`phase1_release_gate.sql`**: references only PORT tables (`mart_master_lead_wide`, `fct_speed_to_lead`, `fct_payment_line_unified`, `rpt_campaign_funnel_month`, `bridge_identity_contact_payment`, `fct_fanbasis_payment_line`). Cannot retire until Batch N (final PORT lands). Each block is repointed at the corresponding PORT's batch.
- **`mart_validation.sql`**: mixed file. Blocks that reference DROP tables get deleted in Batch 1 (block-by-block, not file-level). Blocks that reference PORT tables get repointed batch-by-batch as the PORTs land. Blocks that reference identity-pipeline tables (`rpt_setter_identity_*`, `v_unified_dashboard_schema`) STAY — they're out of Y3 scope.
- **`speed_to_lead_validation.sql`**: references `mrt_speed_to_lead_daily/overall` (DROP) plus `fct_speed_to_lead` (PORT). The DROP-targeted blocks delete in Batch 1; the PORT-targeted block repoints when `fct_speed_to_lead` lands.
- **`services/bq-ingest/sql/validate/*`**: each file maps 1:1 to a Mart table. Drop the file when its target table drops. Specifically: `validate/operations_kpi_panel.sql` (Batch 1), `validate/lead_magnet_url_fields.sql` (when `fct_lead_magnet_activity` cascade-drops), and any others that match a DROP target.

## Batch plan

The plan now has a Batch 0 for resolving blockers that the original audit punted into batches.

0. **Batch 0 — Resolve blockers (no PRs, just verification).** (a) Run the `/ask` traffic check; decide whether to elevate the 5 HOLD tables to PORT or retire `/ask`. (b) Run a parity test on `Marts.bridge_identity_contact_payment` vs `Core.bridge_identity_contact_payment`. (c) Run the `INFORMATION_SCHEMA.JOBS_BY_PROJECT` 30-day scan on the 23 confirmed-orphan tables to convert "no automated consumer" → "truly orphan". (d) Optional: ask David which orphan tables he's queried for client prep recently. Outcomes feed the per-batch scope.

1. **Batch 1 — DROP confirmed orphans (1 PR).** Delete the **23 confirmed-orphan blocks** from `marts.sql` (~1,706 lines). Surgically delete only the validation-harness blocks that reference these 23 tables (per the disposition map above); leave the rest. Delete `validate/*.sql` files that match. **Does NOT touch:** `bridge_identity_contact_payment` (parity-gated, Batch 2), the 4 cascade-hold tables (drop with their consumers), the 5 /ask-HOLD tables (resolution from Batch 0). Mass deletion within those constraints; ~28% of `marts.sql`, not 44%.
2. **Batch 2 — `bridge_identity_contact_payment` parity + repoint (1 PR).** Gated on Batch 0(b). If parity passes within tolerance: repoint `phase1_release_gate.sql:351-353` and `mart_validation.sql` to `Core.bridge_identity_contact_payment`; delete the 144-line block from `marts.sql`. If parity fails: widen Marts to match Core OR document the contract change and update consumers; only then drop. Either way, `fct_payment_line_unified` (PORT, line 898 reads the Marts copy) needs the repoint at the same time — it ports later but its `marts.sql` block must read from `Core.*` after this batch.
3. **Batch 3 — Port small dims (1 PR).** `dim_closers` (23) + `dim_team_members` (195) + `bridge_contact_closer` (67). Build dbt models, repoint `warehouse_queries.py` and `identity_pipeline.py`, delete from `marts.sql`. Possible merge with Batch 4 if `dim_golden_contact` lineage resolves to "use existing dbt dims" (then there's no new dim to port).
4. **Batch 4 — Port `dim_golden_contact` (1 PR).** 435 lines; affects `warehouse_healthcheck.py` and the RUNBOOK debug spine. Gated on Open Question 1 below. If "use existing dbt dims," merges into Batch 3; if "port 1:1," standalone PR.
5. **Batches 5..N — Port rpt tables grouped by consumer query (~5 PRs).** Each PR ports one consumer-coherent slice + the upstream PORT facts it needs + cascades the corresponding DROP-cascade-hold tables out of `marts.sql`:
   - Speed-to-lead slice — `fct_speed_to_lead` PORT + `rpt_speed_to_lead_week` PORT, cascade-drops `fct_lead_magnet_activity`.
   - Funnel slice — `rpt_campaign_funnel_month` + `rpt_funnel_conversion_week` + `rpt_appt_funnel_week` PORTs.
   - Revenue slice — `fct_payment_line_unified` + `fct_fanbasis_payment_line` + `rpt_revenue_by_stage_month` PORTs (also handles the bridge repoint deferred from Batch 2 inside `fct_payment_line_unified`).
   - Rep-performance slice — `rpt_rep_scorecard_week` + `rpt_unbooked_lead_quality_by_campaign` PORTs, cascade-drops `rpt_call_to_booking_rate_week`, `rpt_setter_unbooked_conversion_week`, `rpt_closer_close_rate_week`.
   - HOLD-resolution slice (only if Batch 0(a) decided "elevate to PORT") — `rpt_setter_pre_appt_outreach_week`, `rpt_closer_speed_to_close_week`, `rpt_closer_revenue_month`, plus updates to `analyst.py`'s `SCHEMA_CONTEXT`. Otherwise, these drop in Batch 1 alongside the `/ask` retirement.
6. **Batch N — Port `mart_master_lead_wide` (1 PR).** 977 lines, sole VIEW. Open Question 2 fork (1:1 vs split) materially affects effort: 1:1 is mostly mechanical; split redesigns the BI contract and repoints 3 `warehouse_queries.py` calls. Also: confirm dbt port preserves the `mart_refreshed_at` column semantics that `phase1_release_gate.sql:103-110` reads.
7. **Batch N+1 — Retire writer (1 PR).** Delete `services/bq-ingest/sql/marts.sql`, `services/bq-ingest/sources/marts/mart_models.py`, `run_marts_with_dependencies` from `tasks.py`, the `pipeline-marts-hourly` Cloud Run Job. Also retire `phase1_release_gate.sql` (its consumers all moved by now). Decide disposition of `services/bq-ingest/sources/shared/warehouse_queries.py`: keep (now reading dbt-built `Marts.*`), retire in favor of dabi-routed retrieval, or migrate. The HTTP analytical-catalog surface is a positioning decision separate from the writer retirement.

## Open questions

1. **`dim_golden_contact` lineage.** dbt's warehouse layer has `dim_contacts`, `dim_users`, `dim_offers`, etc. Is `dim_golden_contact` materially distinct, or can the port reshape consumers to use existing dims? Resolve before Batch 4. Resolution affects whether Batch 4 stays standalone or merges with Batch 3.
2. **`mart_master_lead_wide` shape.** 1:1 wide model OR split into composable marts? The view fans out from `dim_golden_contact` and 5+ rpt tables and is itself referenced 10+ times in `marts.sql` (lines 727, 919, 1379, 2403, 2875, etc.) — split is non-trivial. Also: does the dbt port preserve the `mart_refreshed_at` column the release gate reads? Resolve before Batch N is scheduled.
3. **`/ask` endpoint live status.** Run the `gcloud logging read` query above. Outcome decides whether the 5 HOLD tables become PORT (with `SCHEMA_CONTEXT` updates) or DROP (with `/ask` retirement).
4. **`bridge_identity_contact_payment` parity (Marts vs Core).** Run `altimate-data-parity` before Batch 2. Outcome decides repoint+drop vs Marts-widening vs consumer-update.
5. **Ad-hoc human reads.** `INFORMATION_SCHEMA.JOBS_BY_PROJECT` scan over the last 30–90 days against the 23 confirmed-orphan tables. Surface tables with David's user-account reads as candidates for re-classification.
6. **`warehouse_queries.py` post-Y3 disposition** (Batch N+1 decision). Keep the HTTP analytical-catalog surface backed by dbt-built tables, retire it in favor of dabi-routed retrieval, or migrate? Positioning decision; doesn't block other batches.
7. **When does Y3 actually start?** Project state currently flags Y3 as "deferred — multi-day, no urgency." Phase B is active; dabi build is the gating next step for client-visible value. Plan should not start until dabi reaches a natural checkpoint OR until an operational reason forces it (e.g., a `marts.sql` failure that costs more to debug than to retire).

## Won't-do — out of Y3 scope (Y4 follow-on)

`identity_pipeline.py` writes 5 Marts tables/views directly (not via `marts.sql`):
- `bridge_setter_identity` (TABLE, line 753)
- `mart_speed_to_lead_enriched` (VIEW, line 911)
- `rpt_setter_identity_unknown_queue` (TABLE, line 957)
- `rpt_setter_identity_coverage_daily` (TABLE, line 1004)
- `v_unified_dashboard_schema` (VIEW, line 1064)

Retiring these in favor of dbt-built equivalents is a separate "Y4" workstream. Y3 leaves them in place. After Y3, "dbt is sole writer through `marts.sql`" but identity_pipeline.py is still a Mart-schema writer. Calling that out so the gap is visible — full schema ownership requires Y4.

Other exclusions:
- The bq-ingest Python tasks/runner architecture beyond the mart task itself.
- Validation files in `services/bq-ingest/sql/validate/` whose target table is in PORT or out-of-scope (only the DROP-targeted ones retire as part of Y3).
