# Duplicate Data Audit — D-DEE Cloud Projects

Date: 2026-05-01.

Purpose: identify where D-DEE data is duplicated, overlapping, or simply old-but-still-visible across the current and legacy GCP projects. This is read-only evidence for cleanup decisions. It is not a deletion plan.

Companion docs:

- `docs/discovery/cloud-project-provenance-map.md`
- `docs/discovery/current-data-layer-truth-map.md`

## Short Answer

David has mostly been working in the current consolidated project recently, but the old projects still contain enough data and live runtime to confuse any agent.

The current product path should stay on:

```text
project-41542e21-470f-4589-96d
```

The legacy projects should be treated as:

- `dee-data-ops-prod`: old prod/parity/rollback reference
- `dee-data-ops`: old raw/dev/reference estate

Do not start over from scratch unless the goal is psychological clarity rather than technical necessity. The current project has active raw, core, marts, scheduler jobs, Fanbasis/Fathom work, and the new dashboard path. The safer cleanup path is classify, compare, migrate only what is missing, then decommission.

## Audit Method

Read-only commands used:

- `bq ls --project_id=<project>`
- per-dataset `__TABLES__` row-count and freshness summaries
- object/entity counts inside current unified raw tables
- `gcloud run services list`
- `gcloud run jobs list`
- `gcloud scheduler jobs list`

Region-wide `INFORMATION_SCHEMA.TABLE_STORAGE` was not available to the active user, so this audit uses dataset-level metadata instead.

## Current Project: Active Truth

Project:

```text
project-41542e21-470f-4589-96d
```

The current project is active across all layers:

| Dataset | Table count | Row total | Latest modified UTC | Read |
|---|---:|---:|---|---|
| `Raw` | 33 | 359,461 | 2026-05-01 17:29:45 | Current unified raw landing. |
| `STG` | 53 | 201 | 2026-05-01 17:29:49 | Current staging/inline SQL surface. |
| `Core` | 73 | 405,331 | 2026-05-01 17:30:07 | Current modeled core. |
| `Marts` | 52 | 138,540 | 2026-05-01 17:00:35 | Current report/dashboard serving layer. |

Current runtime is also active:

- Cloud Run services: `bq-ingest`, `gtm-warehouse-mcp-phase0`
- Cloud Run jobs: GHL, Calendly, Fanbasis, Fathom, Stripe, Typeform, pipeline, marts, DQ
- Cloud Scheduler: 19 enabled jobs

Conclusion: this project is not a partial shell. It is the active home.

## Legacy Prod: Frozen Reference Plus Live Legacy Jobs

Project:

```text
dee-data-ops-prod
```

BigQuery footprint:

| Dataset | Table count | Row total | Latest modified UTC | Read |
|---|---:|---:|---|---|
| `marts` | 5 | 40,085 | 2026-04-23 11:56:38 | Old dashboard/parity marts. |
| `warehouse` | 13 | 71,358 | 2026-04-23 11:56:24 | Old clean-rebuild warehouse. |
| `staging` | 13 | 29 | 2026-04-23 11:55:37 | Old clean-rebuild staging. |
| `snapshots` | 1 | 16 | 2026-04-23 11:55:59 | Old snapshot residue. |
| `validation` | 6 | 172 | 2026-04-23 11:55:23 | Old validation residue. |

Runtime footprint:

- Cloud Run jobs: `calendly-poll`, `ghl-cold`, `ghl-hot`
- Cloud Scheduler jobs:
  - `calendly-poll`: every minute
  - `ghl-hot`: every minute
  - `ghl-cold`: every 15 minutes

Conclusion: `dee-data-ops-prod` is not the current target, but it is not inert. The old BigQuery outputs look frozen on April 23, while legacy jobs are still scheduled.

## Legacy Dev/Raw: Large Raw Estate

Project:

```text
dee-data-ops
```

BigQuery footprint:

| Dataset | Table count | Row total | Latest modified UTC | Read |
|---|---:|---:|---|---|
| `raw_ghl` | 8 | 35,204,444 | 2026-05-01 17:32:15 | Very large old GHL raw estate. |
| `raw_calendly` | 20 | 108,595 | 2026-05-01 17:32:10 | Old per-object Calendly raw estate. |
| `raw_stripe` | 97 | 718,621 | 2026-05-01 14:10:49 | Old normalized Stripe raw estate. |
| `raw_typeform` | 19 | 152,333 | 2026-05-01 14:11:17 | Old Typeform raw estate. |
| `dev_david` | 48 | 112,550 | 2026-04-28 14:37:28 | Personal/dev outputs. |
| `raw_fanbasis` | 1 | 0 | 2026-04-19 19:59:11 | Empty legacy Fanbasis raw. |

Cloud Run and Cloud Scheduler APIs are disabled in this project.

Conclusion: `dee-data-ops` is mostly data/reference, not active runtime. It may still contain raw history not yet represented in the current project.

## Current vs Legacy Prod Marts

Only two mart names overlap exactly:

| Mart | Current rows | Legacy rows | Read |
|---|---:|---:|---|
| `lead_journey` | 16,093 | 15,991 | Same concept, current is newer. |
| `revenue_detail` | 3,848 | 3,375 | Same concept, current is newer and includes Fanbasis progress. |

Legacy-only marts:

| Mart | Legacy rows | Read |
|---|---:|---|
| `speed_to_lead_detail` | 15,283 | Old Metabase/dbt parity reference. No longer current dbt mart. |
| `sales_activity_detail` | 5,435 | Old Metabase/dbt parity reference. No longer current dbt mart. |
| `stl_data_freshness` | 1 | Old freshness helper. |

Current-only Speed-to-Lead/report tables include:

- `fct_speed_to_lead`: 17,751 rows
- `mrt_speed_to_lead_daily`: 521 rows
- `mrt_speed_to_lead_overall`: 1 row
- `rpt_speed_to_lead_week`: 386 rows
- `rpt_rep_scorecard_week`: 232 rows

Conclusion: for the dashboard product, legacy prod marts are parity/reference material. The current Speed-to-Lead dashboard should not try to resurrect old dbt `speed_to_lead_detail` as its primary source.

## Current vs Legacy Warehouse/Core

Several old warehouse concepts exist in both places, but row counts differ:

| Table | Current rows | Legacy rows | Read |
|---|---:|---:|---|
| `dim_contacts` | 16,093 | 15,991 | Current is slightly newer/larger. |
| `fct_calls_booked` | 5,488 | 5,435 | Current is slightly newer/larger. |
| `fct_outreach` | 5,108 | 26,766 | Not equivalent as-is; likely logic/grain changed. |
| `fct_speed_to_lead_touch` | 5,482 | 15,283 | Not equivalent as-is; legacy was old Speed-to-Lead parity core. |
| `bridge_identity_contact_payment` | 3,848 | 3,375 | Current is newer/larger. |

Current-only Core now includes substantial newer work:

- Fanbasis: `fct_fanbasis_transactions`, `fct_fanbasis_refunds`, `dim_fanbasis_customers`, `dim_fanbasis_products`
- Fathom: calls, outcomes, classifier, match diagnostics
- GHL: conversations, form submissions, attribution, outbound calls, stage snapshots
- Typeform: responses and answers
- Stripe: payments, refunds, invoices, subscriptions, disputes

Conclusion: current Core is broader and fresher. Legacy warehouse is useful for explaining old metric parity, not for new modeling.

## Raw Layer Duplication Is Not Simple

The raw layer is where the most confusion lives.

### Current Unified Raw

Current project unified raw counts:

| Source table | Object | Rows | Latest ingested UTC |
|---|---|---:|---|
| `Raw.ghl_objects_raw` | opportunities | 26,219 | 2026-05-01 17:20:13 |
| `Raw.ghl_objects_raw` | form_submissions | 19,430 | 2026-05-01 17:20:26 |
| `Raw.ghl_objects_raw` | contacts | 16,095 | 2026-05-01 17:20:07 |
| `Raw.ghl_objects_raw` | outbound_call_logs | 5,759 | 2026-05-01 17:20:44 |
| `Raw.ghl_objects_raw` | conversations | 1,662 | 2026-05-01 17:20:49 |
| `Raw.calendly_objects_raw` | scheduled_events | 5,488 | 2026-05-01 17:33:16 |
| `Raw.calendly_objects_raw` | event_invitees | 5,488 | 2026-05-01 17:33:21 |
| `Raw.stripe_objects_raw` | charges | 3,375 | 2026-05-01 17:26:58 |
| `Raw.typeform_objects_raw` | responses | 5,014 | 2026-05-01 11:43:14 |

Other current raw tables:

- `Raw.fanbasis_transactions_txn_raw`: 473 rows
- `Raw.fathom_calls_raw`: 1,211 rows

### Legacy Raw

Legacy `dee-data-ops.raw_*` is shaped differently and is much larger in places:

| Legacy table | Rows | Latest modified UTC | Read |
|---|---:|---|---|
| `raw_ghl.opportunities` | 21,744,573 | 2026-05-01 17:33:33 | Not same grain as current opportunity object count. Likely history/version/sync-expanded. |
| `raw_ghl.contacts` | 13,342,617 | 2026-05-01 17:32:07 | Not same grain as current contact object count. |
| `raw_ghl.messages` | 76,922 | 2026-05-01 16:21:15 | Potentially useful old message estate. |
| `raw_ghl.conversations` | 18,371 | 2026-05-01 16:21:08 | Potentially useful old conversation estate. |
| `raw_calendly.scheduled_events` | 58,755 | 2026-05-01 17:41:51 | Much larger than current scheduled event count. |
| `raw_calendly.question_and_answer` | 22,274 | 2026-05-01 14:15:41 | Per-object detail not directly mirrored in current unified table. |
| `raw_stripe.charge` | 3,375 | 2026-04-20 03:33:12 | Same headline count as current Stripe charges, older shape. |
| `raw_stripe.checkout_session_shipping_address_allowed_countries` | 665,866 | 2026-04-19 21:29:49 | Very high-volume normalized child table; likely not useful for current dashboard. |
| `raw_typeform` | 152,333 total rows across 19 tables | 2026-05-01 14:11:17 | Needs object-level comparison before migration/deletion. |

Conclusion: legacy raw is not a clean duplicate of current raw. Some tables are the same business source at a different grain; some may contain historical detail that the current project does not yet preserve.

## Working Classification

| Area | Classification | Why |
|---|---|---|
| Current `Raw/STG/Core/Marts` | **Active** | Fresh on May 1; current runtime writes here. |
| Current lowercase `staging/warehouse` | **Transition residue / inspect before use** | Some tables fresh, but not the current naming target. |
| Legacy `dee-data-ops-prod.marts/warehouse` | **Parity archive / rollback** | Frozen April 23 old dbt outputs. |
| Legacy `dee-data-ops.raw_*` | **Raw-history candidate** | Large and fresh-looking raw estate; not safe to delete without source-by-source comparison. |
| Legacy `dee-data-ops-prod` jobs | **Live legacy runtime to audit** | Still scheduled; possible duplication or old-source feed. |
| Metabase artifacts | **Historical parity reference** | Useful for card/metric meaning, not current product surface. |

## What This Means For The Dashboard Product

Use current project tables first:

```text
project-41542e21-470f-4589-96d.Marts.*
```

For Speed-to-Lead v1, the safest dashboard inputs are:

- `Marts.mrt_speed_to_lead_overall`
- `Marts.mrt_speed_to_lead_daily`
- `Marts.fct_speed_to_lead`
- `Marts.rpt_speed_to_lead_week`

Use legacy `dee-data-ops-prod.marts.speed_to_lead_detail` only as a numerical/parity reference for old Metabase behavior.

Do not point the new dashboard directly at legacy raw tables.

## Recommended Next Cleanup Pass

Before deleting or pausing anything, do these in order:

1. **Legacy runtime audit:** inspect `dee-data-ops-prod` job env vars and targets for `ghl-hot`, `ghl-cold`, and `calendly-poll`. Determine whether they still write useful legacy raw or duplicate current ingestion.
2. **Raw source comparison:** for each source, compare current unified raw IDs to legacy raw IDs:
   - GHL contacts/opportunities/conversations/messages
   - Calendly events/invitees/questions
   - Stripe charges/refunds/customers
   - Typeform forms/responses
3. **Dashboard data contract:** wire first dashboard read against current `Marts.mrt_speed_to_lead_overall` and `Marts.mrt_speed_to_lead_daily`.
4. **Parity note:** compare the dashboard headline metric to legacy `dee-data-ops-prod.marts.speed_to_lead_detail` for the same window.
5. **Decommission checklist:** only after the above, write a pause/delete plan for legacy jobs and old datasets.

## Do Not Do Yet

- Do not delete `dee-data-ops`.
- Do not delete `dee-data-ops-prod`.
- Do not pause `dee-data-ops-prod` jobs until their write targets are confirmed.
- Do not rebuild the whole project from scratch.
- Do not let a future agent choose tables based on name alone.

## Bottom Line

The current project is good enough to keep moving. The cleanup task is not "start over"; it is "stop letting old projects pretend to be current."
