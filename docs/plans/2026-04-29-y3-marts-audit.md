# Y3 — `services/bq-ingest/sql/marts.sql` retirement audit

_Authored 2026-04-29. Revised 2026-04-29 evening after multi-persona doc review surfaced production-breaking gaps in the original verdict tables. Sets the contract for batched retirement of `marts.sql` (6,064 lines, 45 tables/views written) so dbt-built tables replace its analytical surface and the `pipeline-marts-hourly` Cloud Run Job retires._

> **Scope shift — PR #133 merged 2026-04-30 00:23 UTC.** PR #133 retired the dead HTTP analytical surface (`/ask`, `/query`, `/query/catalog`) and deleted `analyst.py` + `warehouse_queries.py`. **Two bucket structures collapse:**
>
> - **HOLD bucket — gone.** The 5 tables previously blocked on `/ask` disposition (`rpt_closer_close_rate_week`, `rpt_closer_revenue_month`, `rpt_closer_speed_to_close_week`, `rpt_setter_pre_appt_outreach_week`, `rpt_setter_unbooked_conversion_week`) redistribute into confirmed-orphan or cascade-hold per their remaining dependencies. The "HOLD — analyst.py /ask disposition" section below is now historical.
> - **9 PORT tables lost their only static consumer.** `fct_payment_line_unified`, `rpt_funnel_conversion_week`, `rpt_unbooked_lead_quality_by_campaign`, `rpt_rep_scorecard_week`, `rpt_appt_funnel_week`, `bridge_contact_closer`, `rpt_revenue_by_stage_month`, `rpt_speed_to_lead_week`, `dim_closers` all listed `warehouse_queries.py` as their only consumer outside `marts.sql`. With that file gone, these need re-disposition pending an `INFORMATION_SCHEMA.JOBS_BY_PROJECT` scan against David's account (Batch 0(c), now expanded to also cover these 9 tables). Until verified, treat them as **PORT-pending-verification** — likely-DROP if David has no ad-hoc reads, still-PORT if he does.
>
> **The verdict tables and counts below have NOT been re-keyed to reflect the post-#133 state.** A third revision pass will rewrite the tables once Batch 0 verification completes. The batch plan, validation-harness disposition, and Won't-do sections remain accurate as written. Routes-and-files specific changes:
>
> - Batch 0(a) /ask traffic check is **resolved** (path B chosen by retirement). The third disposition path "(C) broken or stale" is now moot.
> - Open Q 3 (/ask endpoint live status) is **resolved**.
> - Batch 5 "HOLD-resolution slice" no longer applies; the 5 ex-HOLD tables fold into Batch 1 (3 of them) or remain in cascade-hold (2 of them).
> - Open Q 6 (`warehouse_queries.py` post-Y3 disposition) is **resolved by retirement**, not "kept and repointed at Marts.* names." This means dbt PORT models target dbt-conventional names (`Core.*` or whatever the dbt convention dictates) without back-compat naming pressure.

## Goal — restated

**Retire `services/bq-ingest/sql/marts.sql` and its Cloud Run Job (`pipeline-marts-hourly`)** so dbt becomes the only writer that runs through `marts.sql`'s blocks. The earlier framing — "dbt becomes sole owner of the Marts schema" — overstates the scope: `services/bq-ingest/sources/identity/identity_pipeline.py` ALSO writes 5 Marts tables/views (`bridge_setter_identity`, `mart_speed_to_lead_enriched`, `rpt_setter_identity_unknown_queue`, `rpt_setter_identity_coverage_daily`, `v_unified_dashboard_schema`). Full schema ownership is a follow-on Y4; Y3 retires the `marts.sql` writer specifically.

## Summary

- **45** tables/views currently written by `marts.sql`.
- **15 PORT** — real production consumers exist; need a dbt-built equivalent before the `marts.sql` block can drop.
- **19 DROP — confirmed orphan** (~1,243 lines). Zero static-ref consumers AND no internal cascade. Caveat: still presumed-orphan until Batch 0(c) `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` scan (~27-day window since project creation) converts presumption to evidence.
- **5 DROP — cascade-hold** (~929 lines). PORT (or HOLD) tables read these inside `marts.sql`; can only drop *after* the consumer moves. **`fct_deal_attribution` is path-conditional** — only cascade-held under path A (its consumer `rpt_closer_revenue_month` is HOLD); under path B (HOLD retires) it's a confirmed orphan.
- **5 HOLD — analyst.py /ask disposition** (~458 lines; 2 overlap with cascade-hold so 8 unique problem tables across both lists). Listed in `SCHEMA_CONTEXT` (line 29 of `services/bq-ingest/sources/shared/analyst.py`), which is fed to Gemini at runtime for the live `POST /ask` route in `app.py:282`. NOT documentation. **Round 2 verified outcome**: a 30-day `gcloud logging read` against `bq-ingest` returned ONLY 404 OPTIONS preflights from a `lovable.app` frontend on 2026-04-11, no POST traffic. State is "broken or stale," not a clean live/unused signal — see Batch 0(a) for the third disposition path this opens.
- **1 DROP — duplicate-of-dbt** (gated on overlap-tier reconciliation, not byte parity). `Marts.bridge_identity_contact_payment` is known-different from `Core.bridge_identity_contact_payment` BY DESIGN: Marts = 144 lines / 3 match tiers (`email_exact`, `email_canonical`, `phone_last10`); Core = 320 lines / 5 match tiers (adds `billing_email_direct`, `unmatched`). Byte-parity will fail — reconciliation question is "do overlapping-tier rows match?" Repointing changes the contract `phase1_release_gate.sql:351-353` and `mart_validation.sql` see.
- **2 DROP — superseded** (`rpt_stripe_lifecycle_month`, `rpt_fanbasis_customer_month`). Reasoning preserved per-table in the verdict prose.

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

### DROP — confirmed orphan (19 tables, ~1,243 lines)

Zero non-validation consumers AND no internal cascade dependency. Safe to delete in Batch 1 alongside their dedicated validation blocks. (`fct_deal_attribution` removed from this list — see cascade-hold; `rpt_stripe_lifecycle_month` + `rpt_fanbasis_customer_month` moved to "DROP — superseded"; `bridge_identity_contact_payment` lives in "DROP — duplicate-of-dbt".)

| Table | Lines |
|---|---|
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

`mrt_speed_to_lead_daily` / `mrt_speed_to_lead_overall` were the v1 Speed-to-Lead Dashboard tables; Metabase v1.6 retired post-v1 (per `CLAUDE.local.md`).

### DROP — superseded (2 tables, 105 lines)

| Table | Lines | Reason |
|---|---|---|
| `rpt_stripe_lifecycle_month` | 55 | No live consumer + D-DEE banned Stripe (memory `project_stripe_historical_only.md`); `Core.fct_stripe_*` still queryable directly if a future cohort comparison wants the historical data. |
| `rpt_fanbasis_customer_month` | 50 | No live consumer; `fct_fanbasis_payment_line` (PORT) is the spine for live revenue (Fanbasis is the live processor). |

### DROP — cascade-hold (5 tables, ~929 lines) — only after consumer ports

These are reachable only because PORT (or HOLD) blocks inside `marts.sql` read them. Cannot drop in Batch 1; must drop AFTER the consuming block moves to dbt and `marts.sql` no longer reads them.

| Table | Lines | Cascade consumer inside `marts.sql` | Drop after |
|---|---|---|---|
| `fct_lead_magnet_activity` | 342 | `fct_speed_to_lead` (PORT, line 2853); `rpt_unbooked_lead_quality_by_campaign` (PORT, line 5439); `rpt_setter_unbooked_conversion_week` (cascade-hold, line 5287) | All three consumers move/drop |
| `rpt_setter_unbooked_conversion_week` | 147 | `rpt_rep_scorecard_week` (PORT, line 5794) | `rpt_rep_scorecard_week` PORT lands. **ALSO HOLD-listed** — both blockers must clear. |
| `rpt_call_to_booking_rate_week` | 121 | `rpt_rep_scorecard_week` (PORT, line 5767) | `rpt_rep_scorecard_week` PORT lands |
| `rpt_closer_close_rate_week` | 79 | `rpt_rep_scorecard_week` (PORT, lines 5741, 5808) | `rpt_rep_scorecard_week` PORT lands. **ALSO HOLD-listed** — both blockers must clear. |
| `fct_deal_attribution` | 240 | `rpt_closer_revenue_month` (HOLD, line 4738) | **Path-conditional**: under path A, drop after `rpt_closer_revenue_month` PORTs. Under path B (HOLD retires), drop in Batch 1 alongside `rpt_closer_revenue_month`. |

When the consuming PORTs land in dbt, the dbt port absorbs the join logic. **Open architectural decision (Batch 5)**: cascade-hold tables — particularly `fct_lead_magnet_activity` (342 lines, two PORT consumers) — must either inline into each consumer's dbt model OR ship as a dbt intermediate (e.g., `int_lead_magnet_activity` in the warehouse layer). Inlining duplicates 342 lines across two dbt models; intermediate-modeling adds a model the verdict tables don't list. Decide before Batch 5 ships.

### HOLD — analyst.py /ask disposition (5 tables, ~458 lines; 2 overlap with cascade-hold)

`SCHEMA_CONTEXT` (line 29 of `services/bq-ingest/sources/shared/analyst.py`) is fed to Gemini at runtime for the live `/ask` endpoint (`app.py:282`). It lists 10 Mart tables Gemini is allowed to query. Five are on the original DROP list:

| Table | Lines | Cascade-hold? |
|---|---|---|
| `rpt_setter_pre_appt_outreach_week` | 133 | no |
| `rpt_setter_unbooked_conversion_week` | 147 | **yes** — also reached by `rpt_rep_scorecard_week` (PORT). Both blockers must clear before drop. |
| `rpt_closer_close_rate_week` | 79 | **yes** — also reached by `rpt_rep_scorecard_week` (PORT). Both blockers must clear before drop. |
| `rpt_closer_speed_to_close_week` | 71 | no |
| `rpt_closer_revenue_month` | 28 | no — but **transitively cascades** `fct_deal_attribution` (read at marts.sql:4738) under path A; see cascade-hold table. |

Three disposition paths (Round 1 had two; Round 2's actual gcloud check returned an ambiguous third state):

- **(A) /ask is live and stays.** Reclassify these 5 to PORT (need dbt-built equivalents); update `SCHEMA_CONTEXT` to point at the dbt-built tables in lockstep with the PORTs. Note: `rpt_closer_revenue_month` becoming PORT activates `fct_deal_attribution`'s cascade-hold (it was conditional).
- **(B) /ask is unused, retire it.** Retire the `/ask` route in `app.py:282`, delete `services/bq-ingest/sources/shared/analyst.py`, drop the 3 non-cascade-overlap tables (`rpt_setter_pre_appt_outreach_week`, `rpt_closer_speed_to_close_week`, `rpt_closer_revenue_month`) in Batch 1. The 2 cascade-overlap tables (`rpt_setter_unbooked_conversion_week`, `rpt_closer_close_rate_week`) STILL wait for `rpt_rep_scorecard_week` PORT in Batch 5 — they cannot drop in Batch 1 even with /ask retired.
- **(C) /ask is broken and direction is undecided.** Round-2-verified actual state. The `gcloud logging read 'resource.type="cloud_run_revision" AND httpRequest.requestUrl=~"/ask"' --limit=50 --freshness=30d` against `bq-ingest` returned only 404 OPTIONS preflights from a `lovable.app` frontend on 2026-04-11. No POST traffic in 30 days; the 404 implies the route is currently failing. Decision required: ask David whether the `lovable.app` frontend is meant to consume `/ask` and is broken (→ path A but fix the 404 first), or whether `/ask` was an experiment and can retire (→ path B). Default until David weighs in: treat as path B but flag the lovable.app frontend.

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

**Byte-parity will fail by design** (3 vs 5 tiers; Core emits rows Marts skips). The reconciliation question is "do overlapping-tier rows match?" Protocol for Batch 2:

1. Filter Core to its 3 overlapping tiers (`email_exact`, `email_canonical`, `phone_last10`) and run `altimate-data-parity` against that filtered subset vs the Marts table — apples-to-apples.
2. Separately confirm the row-count delta (Core total ≥ Marts total; Core extra = `billing_email_direct` + `unmatched`).
3. Walk every consumer (`phase1_release_gate.sql:351-353`, `mart_validation.sql`, `marts.sql:898` inside `fct_fanbasis_payment_line`) and confirm what each does with the new `match_method` enum values. Specifically: do any filter on `match_method = 'unmatched'` or count `billing_email_direct` rows? If yes, the consumer change is non-trivial.
4. If overlapping-tier parity passes AND consumer impact is documented: proceed with repoint+drop. If parity fails OR consumer impact is unacceptable: widen Marts to match Core (port the 2 missing tiers) OR update consumers in lockstep.

## Validation-harness disposition (resolved during the audit, not deferred)

`mart_validation.sql` (298 lines), `phase1_release_gate.sql` (506 lines), and `speed_to_lead_validation.sql` (~120 lines) each reference a MIX of PORT + DROP tables, plus tables outside Y3 scope. "Drop the gate together with the table" is wrong at file granularity — these need surgical block-level edits.

Per-file disposition:

- **`phase1_release_gate.sql`**: references only PORT tables (`mart_master_lead_wide`, `fct_speed_to_lead`, `fct_payment_line_unified`, `rpt_campaign_funnel_month`, `bridge_identity_contact_payment`, `fct_fanbasis_payment_line`). Each block is repointed at the corresponding PORT's batch (incremental migration); the file is finally deleted in Batch N+1 once all blocks have been repointed and the file has nothing live left to gate. Not "retired at Batch 1," not "decided fresh at Batch N+1" — accumulating repoints across batches with a final delete.
- **`mart_validation.sql`**: mixed file. Blocks that reference DROP tables get deleted in Batch 1 (block-by-block, not file-level). Blocks that reference PORT tables get repointed batch-by-batch as the PORTs land. Blocks that reference identity-pipeline tables (`rpt_setter_identity_*`, `v_unified_dashboard_schema`) STAY — they're out of Y3 scope.
- **`speed_to_lead_validation.sql`**: references `mrt_speed_to_lead_daily/overall` (DROP) plus `fct_speed_to_lead` (PORT). The DROP-targeted blocks delete in Batch 1; the PORT-targeted block repoints when `fct_speed_to_lead` lands.
- **`services/bq-ingest/sql/validate/*`**: each file maps 1:1 to a Mart table. Drop the file when its target table drops. Specifically: `validate/operations_kpi_panel.sql` (Batch 1), `validate/lead_magnet_url_fields.sql` (when `fct_lead_magnet_activity` cascade-drops), and any others that match a DROP target.

## Batch plan

The plan now has a Batch 0 for resolving blockers that the original audit punted into batches.

0. **Batch 0 — Resolve blockers (no PRs, just verification).** Three Claude-executable checks + one David question. After all four resolve, the audit doc gets a follow-up revision PR with updated PORT/DROP/HOLD tables before Batch 1 ships.
   - (a) **`/ask` traffic check.** `gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="bq-ingest" AND httpRequest.requestUrl=~"/ask"' --limit=50 --freshness=30d --project=project-41542e21-470f-4589-96d`. Round-2 actual run on 2026-04-29 returned 404 OPTIONS preflights from `lovable.app` only — needs David input on whether `lovable.app` is meant to consume `/ask`. Decide path A / B / C per the HOLD section above.
   - (b) **Bridge reconciliation, not byte-parity.** Run the 4-step protocol in the "DROP — duplicate-of-dbt" section above, NOT a generic `altimate-data-parity`. The two tables are known-different by design; the question is overlapping-tier reconciliation.
   - (c) **Ad-hoc-reads scan.** `SELECT user_email, table_id, COUNT(*) FROM \`region-us\`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, UNNEST(referenced_tables) WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 27 DAY) AND dataset_id = 'Marts' AND table_id IN (...orphan list...) GROUP BY 1,2 ORDER BY 3 DESC`. Note: project's BigQuery retention is ~27 days from project creation, not the 90 days BQ defaults to elsewhere. View name MUST be region-prefixed (`region-us.INFORMATION_SCHEMA.*`) — bare `INFORMATION_SCHEMA.JOBS_BY_PROJECT` returns "Not found." Filter by user_email to separate David's reads (human) from service-account reads (automation already covered by static grep).
   - (d) **Required (not optional) David question:** "Have you queried any of these 19 tables in the BigQuery console for client prep in the last 60 days?" The 27-day INFORMATION_SCHEMA window may miss reads older than that; this question is the last line of defense.

1. **Batch 1 — DROP confirmed orphans (1 PR).** Delete the **19 confirmed-orphan blocks** from `marts.sql` (~1,243 lines) plus the 2 superseded blocks (~105 lines), totalling ~1,348 lines (~22% of `marts.sql`). Before deletion: copy the 21 deleted blocks into `docs/_archive/marts-sql-prior-art/<table>.sql` so future dabi-mart work has the column-shape reference in-tree (cheap insurance — `rpt_fathom_*` and `rpt_payment_reconciliation_month` map onto pre-build dabi roadmap areas). Surgically delete only the validation-harness blocks that reference these 21 tables (per disposition map above); leave the rest. Delete `validate/*.sql` files that match. Under path B (`/ask` retires), ALSO drop the 3 non-cascade-overlap HOLD tables (`rpt_setter_pre_appt_outreach_week`, `rpt_closer_speed_to_close_week`, `rpt_closer_revenue_month`) and trim `analyst.py` SCHEMA_CONTEXT or retire the `/ask` route. **Does NOT touch:** `bridge_identity_contact_payment` (Batch 2), the 5 cascade-hold tables (drop with consumers), the 2 cascade-overlap HOLD tables (cascade still blocks them under any path).
2. **Batch 2 — `bridge_identity_contact_payment` parity + repoint (1 PR).** Gated on Batch 0(b). If parity passes within tolerance: repoint `phase1_release_gate.sql:351-353` and `mart_validation.sql` to `Core.bridge_identity_contact_payment`; delete the 144-line block from `marts.sql`. If parity fails: widen Marts to match Core OR document the contract change and update consumers; only then drop. Either way, `fct_payment_line_unified` (PORT, line 898 reads the Marts copy) needs the repoint at the same time — it ports later but its `marts.sql` block must read from `Core.*` after this batch.
3. **Batch 3 — Port small dims (1 PR).** `dim_closers` (23) + `dim_team_members` (195) + `bridge_contact_closer` (67). Build dbt models, repoint `warehouse_queries.py` and `identity_pipeline.py`, delete from `marts.sql`. Possible merge with Batch 4 if `dim_golden_contact` lineage resolves to "use existing dbt dims" (then there's no new dim to port).
4. **Batch 4 — Port `dim_golden_contact` (1 PR).** 435 lines; affects `warehouse_healthcheck.py` and the RUNBOOK debug spine. Gated on Open Question 1 below. If "use existing dbt dims," merges into Batch 3; if "port 1:1," standalone PR. **Acceptance criterion**: dbt port MUST emit a `mart_refreshed_at` column with the same semantics as the current `CURRENT_TIMESTAMP() AS mart_refreshed_at` (`marts.sql:1314`). `mart_master_lead_wide` propagates this column at `marts.sql:1378` and `phase1_release_gate.sql:103-110` reads it as the upstream-lag signal. Drift to `dbt_utils.current_timestamp()` or any "updated_at" semantics will silently change the release gate.
5. **Batches 5..N — Port rpt tables grouped by consumer query (~5 PRs).** Each PR ports one consumer-coherent slice + the upstream PORT facts it needs + cascades the corresponding DROP-cascade-hold tables out of `marts.sql`:
   - Speed-to-lead slice — `fct_speed_to_lead` PORT + `rpt_speed_to_lead_week` PORT, cascade-drops `fct_lead_magnet_activity`.
   - Funnel slice — `rpt_campaign_funnel_month` + `rpt_funnel_conversion_week` + `rpt_appt_funnel_week` PORTs.
   - Revenue slice — `fct_payment_line_unified` + `fct_fanbasis_payment_line` + `rpt_revenue_by_stage_month` PORTs (also handles the bridge repoint deferred from Batch 2 inside `fct_payment_line_unified`).
   - Rep-performance slice — `rpt_rep_scorecard_week` + `rpt_unbooked_lead_quality_by_campaign` PORTs, cascade-drops `rpt_call_to_booking_rate_week`, `rpt_setter_unbooked_conversion_week`, `rpt_closer_close_rate_week`.
   - HOLD-resolution slice (only if Batch 0(a) decided path A — "elevate to PORT") — `rpt_setter_pre_appt_outreach_week`, `rpt_closer_speed_to_close_week`, `rpt_closer_revenue_month`, plus `SCHEMA_CONTEXT` updates in `analyst.py`. Note: `rpt_closer_revenue_month` PORTing activates `fct_deal_attribution`'s cascade-hold (it ports/drops with this slice). The 2 cascade-overlap HOLD tables (`rpt_setter_unbooked_conversion_week`, `rpt_closer_close_rate_week`) are handled in the rep-performance slice regardless of path. Under path B (/ask retires), the 3 non-cascade-overlap tables drop in Batch 1 instead of here, AND `fct_deal_attribution` becomes a Batch 1 confirmed-orphan drop (alongside `rpt_closer_revenue_month`).
6. **Batch N — Port `mart_master_lead_wide` (1 PR).** 977 lines, sole VIEW. Open Question 2 fork (1:1 vs split) materially affects effort: 1:1 is mostly mechanical; split redesigns the BI contract and repoints 3 `warehouse_queries.py` calls. Also: confirm dbt port preserves the `mart_refreshed_at` column semantics that `phase1_release_gate.sql:103-110` reads.
7. **Batch N+1 — Retire writer (1 PR).** Delete `services/bq-ingest/sql/marts.sql`, `services/bq-ingest/sources/marts/mart_models.py`, `run_marts_with_dependencies` from `tasks.py`, the `pipeline-marts-hourly` Cloud Run Job. Delete `phase1_release_gate.sql` (its blocks have been incrementally repointed across batches and now read entirely from dbt-built tables; this batch removes the now-fully-migrated file). Decide disposition of `services/bq-ingest/sources/shared/warehouse_queries.py`: keep (now reading dbt-built `Marts.*`), retire in favor of dabi-routed retrieval, or migrate.

## Open questions

1. **`dim_golden_contact` lineage.** dbt's warehouse layer has `dim_contacts`, `dim_users`, `dim_offers`, etc. Is `dim_golden_contact` materially distinct, or can the port reshape consumers to use existing dims? Resolve before Batch 4. Resolution affects whether Batch 4 stays standalone or merges with Batch 3.
2. **`mart_master_lead_wide` shape.** 1:1 wide model OR split into composable marts? The view fans out from `dim_golden_contact` and 5+ rpt tables and is itself referenced 10+ times in `marts.sql` (lines 727, 919, 1379, 2403, 2875, etc.) — split is non-trivial. Also: does the dbt port preserve the `mart_refreshed_at` column the release gate reads? Resolve before Batch N is scheduled.
3. **`/ask` endpoint live status (BLOCKING for Batch 1).** Round-2 `gcloud` run on 2026-04-29 returned only 404 OPTIONS preflights from `lovable.app` — broken-or-stale, not a clean live/unused signal. Decision needs David: is `lovable.app` meant to consume `/ask` (→ path A; fix the 404)? Or was `/ask` an experiment (→ path B; retire)?
4. **`bridge_identity_contact_payment` reconciliation (BLOCKING for Batch 2).** Run the 4-step protocol in the "DROP — duplicate-of-dbt" section. Byte-parity will fail by design; the reconciliation question is "do overlapping-tier rows match?"
5. **Ad-hoc human reads (BLOCKING for Batch 1).** Run the region-prefixed query in Batch 0(c) over the ~27-day retention window. PLUS ask David directly (Batch 0(d) — required, not optional) for the >27-day window the scan can't reach.
6. **`warehouse_queries.py` post-Y3 disposition.** Keep (now reading dbt-built `Marts.*`), retire in favor of dabi-routed retrieval, or migrate? Positioning decision; doesn't block batches but **could be answered in Batch 0** because it shapes whether PORT models target `Marts.<dbt_table>` (back-compatible names for warehouse_queries.py) vs `Core.*` (dbt-conventional, requires warehouse_queries.py edits per batch). Resolving up-front saves rework.
7. **When does Y3 actually start?** Scope is now Batch 0 + 7 batches across known forks; realistic estimate is **multi-day (drops-only path) to multi-week (full port through `mart_master_lead_wide` split)**, not "multi-day" as project-state claims. Phase B is active; dabi build is the gating next step for client-visible value. Trigger: do not start until either (a) the first dabi mart consumes a downstream client artifact (proving the BI direction), OR (b) a `marts.sql` failure incident makes retirement cheaper than debugging.
8. **Y4 trigger condition** (called out for visibility, not blocking). Identity_pipeline.py's 5 Marts writes are a Y4 follow-on per "Won't-do" below. Without a stated Y4 trigger, the schema-ownership goal is permanently parked at "two writers tolerated." Possible Y4 triggers to consider: third non-dbt Marts writer is proposed; OR dabi-routed retrieval supersedes warehouse_queries.py and the analytical-API surface retires; OR identity_pipeline.py needs material rework for an unrelated reason.

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
