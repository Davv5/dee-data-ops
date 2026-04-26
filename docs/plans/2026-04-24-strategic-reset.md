# Strategic Reset — Data Discovery & Visibility Sprint (2026-04-24)

**Duration:** ~1–2 weeks (2026-04-24 → target 2026-05-08)
**Status:** Active
**Supersedes (temporarily):** `2026-04-23-001-feat-gtm-source-port-plan.md` — PAUSED at U3-complete

---

## Why

A strategic discussion with Grok concluded:

1. **The project is NOT restarting.** A third restart was considered and rejected.
2. **The technical foundation is sound** — BigQuery + dbt, 13 staging models resolving end-to-end, `(id, _ingested_at, payload)` raw-landing discipline. U1 preflight, U2 profile retarget, and U3 staging shims all stay.
3. **The real problem is visibility and business prioritization, not technology.** We have been building against a single metric (Speed-to-Lead) without mapping the full business surface, and we have been moving toward a GCP cutover without first confirming what the Gold layer has to produce.
4. **The fix is a Discovery Sprint, not another rebuild.** Pause new build work, map what data exists and what business areas need serving, let Grok help prioritize, then rebuild Gold against real priorities.

## What we are NOT doing

- **Not restarting.** U1 / U2 / U3 stay shipped.
- **Not rebuilding staging.** The 13 staging models + blob-shims are the contract with raw and do not change during the sprint.
- **Not abandoning the GCP consolidation plan.** `2026-04-23-001-feat-gtm-source-port-plan.md` is paused, not deleted. It resumes (with its warehouse scope rewritten) once the Gold-layer roadmap exists.
- **Not shipping any warehouse / mart / dashboard PRs during the sprint.** Docs-only.

## Strategic Reset Plan

The five bullets from the Grok discussion, with a one-line "what this means in practice":

1. **Pause new feature development for 1–2 weeks.**
   *In practice:* no dbt model PRs, no Metabase/Evidence.dev work, no source extractor fixes, no warehouse refactors. Only docs and bookkeeping land.
2. **Run a Data Discovery & Visibility Sprint.**
   *In practice:* five parallel workstreams (below) producing a discovery packet under `docs/discovery/`. The packet started with the four planned decision artifacts and now also includes model/gap/insight views that make the source reality easier to use.
3. **Get full clarity on what data actually exists across all sources.**
   *In practice:* per-source inventory — table / row count / freshness / owner / breakage — across GHL, Stripe, Typeform, Calendly, Fathom, Fanbasis.
4. **Map all important business areas (not just Speed-to-Lead).**
   *In practice:* explicit enumeration of lead acquisition, speed-to-lead, appointment setting, closing, revenue attribution, churn, SDR/AE performance, pipeline velocity, funnel ROI, refunds/chargebacks — with stakeholder, decision, and data dependency for each.
5. **Create a prioritized roadmap with Grok's guidance, then rebuild Gold against real priorities.**
   *In practice:* walk Grok through the inventory and business-area map; let Grok rank by value × feasibility; publish an ordered list of marts to build; supersede the U5+ section of the GCP cutover plan with the new roadmap.

---

## 7–10 day focus

Five workstreams, runnable in parallel. The decision artifacts remain the
business-area map, coverage matrix, and Gold-layer roadmap; supporting source,
model, gap, and insight views live beside them under `docs/discovery/`.

### 1. Source inventory (days 1–3)

**Output:** `docs/discovery/source-inventory.md`

Per-source fact sheet. One block per source, one row per table/topic. Columns: table, row count, last write, freshness vs SLA, owning extractor, current breakage (if any), notes.

Sources to cover:

- **GHL** — both `raw_ghl.ghl__<obj>_raw` (Phase-2, partial) and `Raw.ghl_objects_raw` (blob, fuller). Call out the 6-of-10 Phase-2 gap, the `conversations` 101-vs-1,314 undercount, and the zero-row `messages` / `users` / `tasks` across both paths.
- **Stripe** — `Raw.stripe_objects_raw` entity_types + per-object tables. Flag ~50-day staleness and D-DEE's account ban ("historical only" — Fanbasis is the live payment source, per memory `project_stripe_historical_only.md`).
- **Typeform** — `Raw.typeform_objects_raw` (responses) plus the `form_id` gap.
- **Calendly** — `Raw.calendly_objects_raw` (all three entity_types); Phase-2 `raw_calendly.*` is empty.
- **Fathom** — `Raw.fathom_calls_raw` (1,157 calls, 0% transcript coverage).
- **Fanbasis** — live payment processor; current state of extractor and landing.

Seed: `docs/_archive/gtm-gcp-inventory.md` (U1 preflight) — extend, don't recreate.

### 2. Business-area map (days 2–5)

**Output:** `docs/discovery/business-area-map.md`

For each business area D-DEE operates:

- **Area name**
- **Stakeholder(s)** — who consumes the answer
- **Decision(s) it drives** — one concrete decision per area
- **Data dependencies** — which raw sources/tables are required
- **Current state** — fully served / partially served / blocked / no data yet

Areas to enumerate (starter list, extend during sprint):

- Lead acquisition (funnel top)
- Speed-to-Lead (v1, already shipped)
- Appointment setting (SDR performance)
- Closing (AE performance)
- Revenue attribution (live revenue via Fanbasis, historical via Stripe)
- Refunds / chargebacks
- Churn / retention
- Pipeline velocity (stage dwell-time)
- Content / funnel ROI
- No-show / rescue flows
- DQ / lead quality

Input to Grok's prioritization step.

### 3. Data-to-business crosswalk (days 4–6)

**Output:** `docs/discovery/coverage-matrix.md`

Matrix of business areas (rows) × source tables (columns), color-coded:

- **Green** — fully served by existing raw
- **Yellow** — partially served (gaps documented)
- **Red** — blocked on a broken extractor
- **Grey** — blocked on a missing source

Highlights which areas are ready to build against today vs. which need an extractor fix first.

### 4. Grok roadmap review (days 6–8)

**Output:** `docs/discovery/gold-layer-roadmap.md`

Walk Grok through artifacts 1–3. Grok ranks business areas by **value × feasibility**. Output is an ordered list of marts to build:

```
Rank | Mart name | Grain | PK | Purpose (1 line) | Business area | Rationale | Data deps | Blockers
```

Each mart entry declares **grain + PK + 1-line purpose** upfront — per Joshua Kim, "[AE] The Order in which I Model Data" (Step 2: confirming fact-table grain is the most load-bearing decision; codified in `.claude/rules/data-modeling-process.md`). This bakes the grain decision into the artifact format so it can't be deferred to implementation.

This ordered list replaces the U5+ scope of `2026-04-23-001-feat-gtm-source-port-plan.md`.

### 5. Sprint retro + re-plan (days 9–10)

**Output:** `docs/plans/2026-05-xx-gold-layer-rebuild.md` (supersedes U5+ of the cutover plan)

- Resume the GCP cutover plan where it still applies (U4a plumbing parity against a frozen snapshot).
- Rewrite the U5+ warehouse scope using the Grok-prioritized mart list.
- Close the Sprint workstream in `.claude/state/project-state.md`; open the new build workstream.

**Daily cadence during sprint:** one-line WORKLOG.md entry per day capturing progress (even if no code lands). Zero dbt / warehouse / mart PRs unless the deliverable *is* docs.

---

## What stays in motion during the sprint

- WORKLOG.md hygiene (daily entries).
- Memory updates when David shares new context (user / feedback / project / reference memories).
- Read-only BQ queries for discovery (row counts, freshness, schema introspection).
- Answering ad-hoc questions with the existing foundation.

## Out of scope for the sprint

- U4a plumbing parity (frozen-snapshot reproduction of `fct_speed_to_lead_touch`)
- U4b live-raw business parity
- U5 warehouse rebuild
- U6 Fathom transcript fix
- U7 / U8 Stripe + Fanbasis revenue parity
- U9 Typeform `form_id` fix
- U12 identity-spine parity
- U14 `dee-data-ops*` decommission
- `merge-dbt-ci@` service account provisioning
- `bq-ingest` repair (GTM-repo work)

All resume once the Gold-layer roadmap is in place. The Sprint reshapes what U5+ looks like; U4a's contract (reproducing the 15,283-row fact) stays valid.

## Current Artifact Shape

The sprint packet has expanded beyond the original four files. Treat the files
below as complementary views, not competing plans:

| File | Status | Purpose |
|---|---|---|
| `docs/discovery/source-inventory.md` | Landed | Source-centric snapshot of raw landing, freshness, and extractor status. |
| `docs/discovery/staging-models.md` | Landed | Model-centric inventory of the 13 staging models and caveats. |
| `docs/discovery/gap-analysis.md` | Landed | Delta view: what exists, what is missing, what each gap blocks. |
| `docs/discovery/insights-summary.md` | Landed | Executive summary across the source, model, and gap views. |
| `docs/discovery/business-area-map.md` | Pending | Business areas, stakeholders, decisions, dependencies, and current state. |
| `docs/discovery/coverage-matrix.md` | Pending | Business areas × source coverage matrix. |
| `docs/discovery/gold-layer-roadmap.md` | Pending | Prioritized Phase B mart list with grain, PK, blockers, and unlock criteria. |

## Exit criteria

The sprint ends when all four decision gates are true:

1. `docs/discovery/source-inventory.md` exists and covers all six sources.
2. `docs/discovery/business-area-map.md` exists and enumerates every business area D-DEE cares about, each with stakeholder + decision + data deps + current state.
3. `docs/discovery/coverage-matrix.md` exists and maps every business area to its source dependencies with a status color.
4. `docs/discovery/gold-layer-roadmap.md` exists, has been reviewed with Grok, and carries an ordered list of marts to build.

When all four are in place, publish `docs/plans/2026-05-xx-gold-layer-rebuild.md` and flip `.claude/state/project-state.md` to the new build workstream.
