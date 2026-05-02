# Current Data Layer Truth Map

Last updated: 2026-05-02.

Purpose: this is the reset document for source → dbt → marts → dashboard work. Read this before building or modifying any mart, report table, or dashboard binding.

The project has been fighting stale guidance. Several plans and state files still describe marts and BI direction that no longer match `main`. This document names the current truth and gives future agents a safer path.

Companion infrastructure map: `docs/discovery/cloud-project-provenance-map.md`. Read it before any GCP, BigQuery, Cloud Run, or Scheduler change.

## Operating Doctrine

The architecture is the three-layer model from `docs/transcripts/3-layer-architecture-of-truth.md`:

```text
Raw API landing
  -> dbt staging views
  -> dbt warehouse facts/dimensions
  -> dbt marts / temporary bq-ingest report tables
  -> dashboard product
```

Each layer has one job:

- **Raw landing:** preserve source payloads. Ugly is acceptable. Business logic is not.
- **Staging:** 1:1 cleanup only. Rename, cast, normalize JSON. No joins. No aggregations.
- **Warehouse:** business truth. Facts are verbs, dimensions are nouns, bridges resolve identity.
- **Marts:** dashboard-ready contracts. Wide, plain-English, tested, and stable.
- **Dashboard:** consumes contracts. It does not repair truth.

Hard rule: no new dashboard logic until the mart or report-table contract is named.

## Present Reality

The current consolidated GCP project is `project-41542e21-470f-4589-96d`. Legacy projects `dee-data-ops-prod` and `dee-data-ops` still exist and still contain useful evidence, but they are not the target for new work.

There are two active data-serving worlds inside the current project:

1. **dbt-owned durable layer** under `2-dbt/models/`
2. **bq-ingest inline SQL report layer** in `services/bq-ingest/sql/marts.sql`

The long-term direction is dbt. The bq-ingest report layer may be consumed for Speed-to-Lead v1 because it is already live, but it should not become the durable modeling surface for new chapters.

## Current dbt Surface

### Staging

Current source staging exists for:

- Calendly: `stg_calendly__events`, `stg_calendly__event_invitees`, `stg_calendly__event_types`
- Fanbasis: `stg_fanbasis__transactions`, `stg_fanbasis__refunds`
- Fathom: `stg_fathom__calls`
- GHL: `stg_ghl__contacts`, `stg_ghl__conversations`, `stg_ghl__opportunities`, `stg_ghl__pipelines`, `stg_ghl__users`
- Stripe: `stg_stripe__charges`, `stg_stripe__customers`
- Typeform: `stg_typeform__responses`

Important correction to stale docs: Fanbasis staging does exist now. Any plan saying `stg_fanbasis__transactions` does not exist is stale.

### Warehouse

Current durable warehouse layer includes:

- Dimensions: `dim_contacts`, `dim_offers`, `dim_pipeline_stages`, `dim_sdr`, `dim_source`, `dim_users`, `dim_calendar_dates`
- Facts: `fct_calls_booked`, `fct_outreach`, `fct_payments`, `fct_refunds`
- Bridge: `bridge_identity_contact_payment`

The cleanest current modeling chain is revenue:

```text
Raw.fanbasis_transactions_txn_raw
  -> stg_fanbasis__transactions / stg_fanbasis__refunds
  -> bridge_identity_contact_payment
  -> fct_payments / fct_refunds
  -> revenue_detail
```

### Marts

Current dbt mart files on the active branch:

- `lead_journey`
- `lead_magnet_detail`
- `revenue_detail`

Important correction to stale docs: `speed_to_lead_detail` and `sales_activity_detail` are no longer dbt marts on `main`. They were retired in PR #142.

`lead_magnet_detail` is the newest durable mart candidate. It is one row per
GHL opportunity, treats each GHL pipeline as a lead-magnet/funnel lane, and
attributes outreach, bookings, and revenue inside each contact's opportunity
window. This matters because 44.6% of contacts appear in more than one
pipeline; do not join all contact revenue to every lead magnet.

## Current bq-ingest Report Surface

`services/bq-ingest/sql/marts.sql` still writes many `Marts.*` tables directly, including Speed-to-Lead and rep scorecard surfaces.

Treat these as **temporary dashboard-serving report tables**, not the canonical dbt mart architecture.

Usable for the first dashboard product slice:

- `Marts.mrt_speed_to_lead_overall`
- `Marts.mrt_speed_to_lead_daily`
- `Marts.fct_speed_to_lead`
- `Marts.rpt_speed_to_lead_week`
- `Marts.rpt_rep_scorecard_week`

Known issue cluster around `rpt_rep_scorecard_week`:

- `rpt_appt_funnel_week.setter_name` is still broken upstream.
- `rpt_call_to_booking_rate_week` and `rpt_setter_unbooked_conversion_week` currently produce 0 rows.
- `rep_role` is per rep per week, not one row per person.
- GHL seed/static-map drift remains a risk.

Use these tables pragmatically for Speed-to-Lead v1, but do not expand this file for new durable modeling unless there is an emergency.

## Dashboard Product Direction

The active BI direction is the click-around dashboard product:

- Plan: `docs/plans/2026-05-01-001-feat-dashboard-product-plan.md`
- Architecture: Cabinet shell, Kim simplicity
- First chapter: Speed-to-Lead Page 1
- Data source for v1: existing bq-ingest Speed-to-Lead report tables

The previous Kim/dabi generative-BI plans are superseded as active direction. They remain useful historical context only.

## Stale Artifact Register

These files either contained stale guidance during the reset or still need a deeper rewrite:

| File | Stale claim | Current truth |
|---|---|---|
| `CLAUDE.local.md` | Previously said BI direction is `dabi` | Patched 2026-05-01 to point at dashboard product |
| `.claude/state/project-state.md` | Previously listed `sales_activity_detail` and `speed_to_lead_detail` as active dbt marts | Patched 2026-05-01 to point at this truth map and current dbt marts |
| `docs/discovery/gold-layer-roadmap.md` | Fanbasis staging does not exist; `speed_to_lead_detail` / `sales_activity_detail` are shipped dbt marts | Banner-staled 2026-05-01; needs rewrite from current code |
| `docs/discovery/source-inventory.md` | Fanbasis has zero staging models | Banner-staled 2026-05-01; Fanbasis transactions/refunds staging exists |
| `docs/plans/2026-04-30-mart-collapse-fct-sks-plan.md` | Continue PR-2 collapse of `sales_activity_detail` | Banner-staled 2026-05-01; target mart no longer exists on `main` |
| `docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md` | Build/cut over to dabi | Marked superseded 2026-05-01 |
| `docs/plans/2026-04-24-002-feat-generative-bi-platform-plan.md` | Build dabi platform | Marked superseded 2026-05-01 |
| `3-bi/metabase/` references, if present | Metabase authoring remains active | Metabase is retired; use only as historical card reference |

## Recommended Work Order

### 1. Stop and align state

Before any new model/dashboard work:

- update `CLAUDE.local.md`
- update `.claude/state/project-state.md`
- mark stale plans superseded or banner-stale
- update `gold-layer-roadmap.md` to current code reality

This is not ceremony. Future agents read those files as authority.

### 2. Ship dashboard v1 from existing Speed-to-Lead report tables

Use bq-ingest `Marts.*` tables for Speed-to-Lead v1 because they are the already-live operational surface.

Do not try to resurrect deleted dbt marts just to satisfy stale plans.

### 3. Make revenue the first clean dbt-backed chapter

`revenue_detail` is the clearest current dbt mart:

- Fanbasis is live.
- staging exists.
- payment/refund facts exist.
- identity bridge exists.
- mart exists.

This should be the first non-Speed-to-Lead chapter to prove the durable dbt path.

### 4. Treat `lead_journey` as structurally right but incomplete

`lead_journey` is the right contact-grain shape, but many columns are placeholders and GHL/Typeform attribution is incomplete.

Do not overbuild dashboard logic around placeholder columns. Fill upstream first.

### 5. Use `lead_magnet_detail` for the next money-facing chapter

`lead_magnet_detail` is the first clean step toward answering which lead
magnet creates volume, gets worked, books calls, and turns into paid revenue.

Use it before building a lead-magnet dashboard, and keep three views separate:

- all opportunity windows
- first opportunity for acquisition/source quality
- latest opportunity for current operating state

Do not treat every GHL pipeline name as the final business label. The branch
now includes an initial `lead_magnet_pipeline_taxonomy` seed with 36 current
pipelines classified from their names. Review that seed with David before
calling the taxonomy final.

### 6. Defer rep-ops rebuild until GHL source truth is resolved

The current rep scorecard is useful enough for v1, but its foundations are patchy.

Before rebuilding rep ops in dbt, resolve:

- trusted GHL source path
- roster/static seed parity
- current-vs-booking-time owner semantics
- whether the 0-row bq-ingest reports should be fixed or retired

### 7. Defer Fathom and retention

Fathom transcripts and Fanbasis customer/subscription shape are not ready. Do not let those become the next rabbit hole.

## Guardrails for Future Agents

Before writing any mart or dashboard query, answer these:

1. What business question is being answered?
2. What table owns the contract?
3. Is the table dbt-owned or temporary bq-ingest-owned?
4. What is one row?
5. What is the primary key?
6. Which facts and dimensions feed it?
7. Which columns are trusted vs placeholders?
8. Which tests protect the contract?
9. Is any referenced plan stale relative to current `main`?

If those cannot be answered, do not build the dashboard tile yet.

## Suggested Near-Term PRs

### PR A — State reset

- Patch stale direction in `CLAUDE.local.md`
- Patch stale open threads in `.claude/state/project-state.md`
- Mark old dabi plans superseded
- Banner-stale the mart-collapse plan
- Link this truth map from state

### PR B — Dashboard product scaffold

- Start `3-bi/dashboard/`
- Speed-to-Lead v1 consumes bq-ingest report tables only

### PR C — Lead magnet chapter readiness

- Merge and promote `lead_magnet_detail`
- Review `lead_magnet_pipeline_taxonomy` classifications with David
- Build the lead-magnet dashboard chapter from the mart, not from raw GHL joins

### PR D — Revenue chapter readiness

- Verify `revenue_detail` row counts and refund parity
- Add any missing tests/docs
- Make `revenue_detail` the first dbt-backed dashboard chapter candidate

### PR E — Roadmap refresh

- Rewrite `gold-layer-roadmap.md` from current code, not from April 25 assumptions
- Reclassify `speed_to_lead_detail` / `sales_activity_detail` as retired dbt marts
- Promote `lead_magnet_detail` and `revenue_detail` as the cleanest current
  dbt-backed dashboard candidates

## Bottom Line

The path forward is not "more marts." It is fewer, clearer contracts:

- Speed-to-Lead v1: temporary bq-ingest report contract
- Lead Magnet: durable dbt mart contract
- Revenue: durable dbt mart contract
- Lead Journey: durable dbt mart contract, incomplete columns called out
- Rep Ops: temporary bq-ingest report contract until GHL truth is settled

Everything else waits.
