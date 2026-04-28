---
title: Consolidate data ops in project-41542e21 — retain GTM ingestion, migrate Merge's dbt + Metabase
type: feat
status: paused
date: 2026-04-23
deepened: 2026-04-23
origin: conversation on 2026-04-23 (no prior requirements doc)
supersedes: v1 of this plan (ported-sources-to-dee-data-ops-prod approach, discarded 2026-04-23)
---

# Consolidate data ops in `project-41542e21-470f-4589-96d`

> **Current status (2026-04-28+):** **U4a, U4b, U5, U6, U7, U8, U9, U12, U14 are all PAUSED.**
> The plan was paused at U3-complete on 2026-04-24 to run the Strategic Reset /
> Discovery Sprint. Phase A (Discovery Sprint) **closed 2026-04-27**; Phase B
> (Layer Build) is active and is shipping marts via `2-dbt/models/marts/`
> (PRs #84/#86/#88/#90/#92 landed B.1–B.4 on 2026-04-28).
>
> **Resumption is gated** on the Gold-layer roadmap (`docs/discovery/gold-layer-roadmap.md`)
> being ranked + published. Original plan named Grok as the ranker; Grok was
> removed (memory `project_grok_out_of_loop.md`). Ranking now uses the
> `mart-roadmap-rank` skill rubric. See `docs/plans/2026-04-24-strategic-reset.md`.
>
> The bq-ingest consolidation (`docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`)
> ran in parallel with this pause and shipped Steps 1–4 on 2026-04-28 — those
> bring the GTM-side ingestion code home into `services/bq-ingest/` without
> waiting for U4a+. Don't conflate that consolidation with this plan's resumption.
>
> Use `.claude/state/project-state.md` for live status and `docs/discovery/`
> for current source reality before executing any unchecked unit.

## Fresh session startup

If you're opening this plan in a new Claude session, do these in order **before touching anything**:

1. Read `CLAUDE.md` (repo conventions) and `CLAUDE.local.md` (locked Speed-to-Lead metric math — gitignored; David has it).
2. Read `.claude/state/project-state.md` for the current snapshot.
3. Confirm auto-loaded memory includes `project_gcp_consolidation_decision.md` + `feedback_preserve_working_infra.md` + `feedback_multi_agent_orchestration.md` (via `MEMORY.md`). The earlier `feedback_ship_over_ceremony.md` was **superseded** 2026-04-27 by `feedback_multi_agent_orchestration.md` — multi-agent pipelines are leverage when fanning out work, ceremony only when piling decisions back on one human.
4. Read this plan in full — especially **Key Technical Decisions**, **Phased Delivery**, and **Risks**.
5. Confirm which unit you're executing. Default: the next unchecked `- [ ]` in Implementation Units order. Wait for explicit user sign-off on any HARD GATE.

**Execution posture (per `feedback_multi_agent_orchestration.md`, supersedes prior `ship_over_ceremony`):** main-session direct execution remains the default for solo build steps; spawn parallel/specialized agents (CE reviewers, Altimate skills, data-engineer agent) when fanning out independent work. Always pair producers with reviewers per `.claude/rules/use-data-engineer-agent.md`.

**Phase boundaries are non-negotiable.** Finish all of Phase 1 before starting Phase 2. HARD GATES at U4a (plumbing parity), U8, U12 require David's explicit sign-off on parity results before the dependent work proceeds. U4b (live-raw business parity) gates U14 decommission, not U5. Parity windows (7-day U4b / U8, 14-day U12, 30-day U14 soak) are calendar-bound and cannot be compressed.

**Target GCP project:** `project-41542e21-470f-4589-96d`. **NOT** `dee-data-ops-prod` (decommissioned in U14). Billing account for both is `0114FD-8EC797-A11084`.

**GTM source repo (read-only reference):** `/Users/david/Documents/operating system/Projects/GTM lead warehouse`. Files there are specifications for U6/U7/U9/U11 — read, do not edit. Ported code lands in Merge's `1-raw-landing/<source>/` or `2-dbt/models/warehouse/bridges/`.

**Corpus before conventions.** Before writing any new `.claude/rules/*.md` file, dbt macro, or architectural decision doc, query the notebook per `.claude/rules/using-the-notebook.md`. It's free.

---

## Overview

David recovered the GTM Lead Warehouse project — 2 years of ingestion-first work with 17 Cloud Run Jobs, live extractors for 6 sources, a trained Fathom BQML classifier, Secret Manager wiring, and ~100+ MB of fresh raw data — all running today in GCP project `project-41542e21-470f-4589-96d`. Merge (`dee_data_ops`) is the clean-room rebuild that got dbt + Metabase working but never stood up its own ingestion.

**Decision (2026-04-23):** consolidate everything into GTM's GCP project. Keep the ingestion exactly where it is. Move Merge's dbt, Metabase, and the 15,283 Speed-to-Lead materialized rows over. Decommission `dee-data-ops-prod` + `dee-data-ops` + Fivetran after cutover holds.

This is the inverse of v1 of this plan. v1 assumed we'd port extractors to `dee-data-ops-prod`; David overruled because the working infrastructure in `project-41542e21-...` was the thing worth preserving.

---

## Problem Frame

**Two projects, duplicated work.** GTM's GCP has live ingestion, scheduled Cloud Run Jobs, trained BQML, and fresh data across 5 sources. Merge's `dee-data-ops-prod` has the dbt + Metabase production state (15,283 rows of Speed-to-Lead truth, locked metric math, v1.6 dashboard). Both are running. Both are on the same billing account. Only one needs to keep running.

**What consolidation unlocks:**
- Every source GTM already ingests (GHL, Calendly, Stripe, Typeform, Fathom) becomes available to Merge's dbt and Metabase with zero port work
- The BQML call classifier becomes queryable from Metabase with zero retrain work
- Fivetran can be torn down (Stripe + Typeform redundant)
- Maintenance collapses from three GCP projects to one

**Hard constraint:** the locked Speed-to-Lead metric — "% of Calendly-booked calls with a human SDR CALL/SMS touch within 5 minutes, SDR-attributed denominator" — cannot drift. The 15,283-row fact table currently in `dee-data-ops-prod.warehouse.fct_speed_to_lead_touch` must reproduce identically after cutover.

**Known hiccups (not surprises):**
- Fathom transcripts have a known landing issue (David's previous stuck point)
- GTM's GHL raw was 3 days stale as of 2026-04-23 (scheduler worth checking)
- Phase 2 per-object migration is complete for `raw_ghl` and `raw_calendly` only; Stripe / Typeform / Fathom still land in single-blob `Raw.<source>_objects_raw` shape inside GTM's project
- Merge's staging models assume per-object shape for all sources, so Stripe/Typeform/Fathom staging will need a shim until Phase-2 is finished in-place

---

## Requirements Trace

- R1. All Merge dbt models — staging, warehouse, marts — compile and materialize against `project-41542e21-470f-4589-96d` instead of `dee-data-ops-prod`.
- R2. Metabase v1.6 Speed-to-Lead dashboard reads from `project-41542e21-...`; all existing tiles render identically.
- R3. The locked Speed-to-Lead metric produces the same value (± 0 denominator, ± 1 numerator tolerance) after cutover, proven by a parity query against both projects.
- R4. Fathom raw data + BQML classifier are accessible from Merge's dbt and visible on Metabase via at least one tile (call intelligence). Transcript landing issue patched.
- R5. Stripe, Typeform, and Fathom raw data are migrated from single-blob `Raw.<source>_objects_raw` to Phase-2 per-object `raw_<source>.<source>__<obj>_raw` shape in-place.
- R6. Identity-matching rules (email_canon, phone_last10, name+domain with confidence bands) from `sources/identity/identity_pipeline.py` exist as dbt bridges in `2-dbt/models/warehouse/bridges/`.
- R7. Fivetran Stripe + Typeform connectors disabled, all Fivetran-landed datasets deleted, Fivetran billing terminated.
- R8. `dee-data-ops-prod` and `dee-data-ops` GCP projects decommissioned after 30 days of stable operation on the consolidated project.

---

## Scope Boundaries

- Fanbasis is **not** fixed in this plan. Discovery later found fresh raw rows in the consolidated project, but Merge-side dbt still has no Fanbasis staging model; track current status in `docs/discovery/gap-analysis.md`.
- `app.py` (GTM's 349-line webhook/admin server) is **not** ported unless a concrete webhook endpoint is proven to be in use.
- GTM's Python orchestration `sources/identity/identity_pipeline.py` (1,306 lines) is **not** ported — only the matching rules become dbt.
- GTM's own `2-dbt/` project (`gtm_lead_warehouse`) is **not** merged. Merge's `dee_data_ops` remains the single dbt project; the only change is where it points.
- GTM's legacy mixed-case datasets (`Raw`, `Core`, `Marts`, `STG`) are **not** wholesale renamed; we read from them during transition and retire them as Phase-2 per-object tables replace them.
- GTM's `ops/cloud/jobs.yaml`, `.claude/`, `docs/`, `CLAUDE.md`, Looker Studio specs are **not** merged wholesale; referenced as needed.
- GTM's GHL and Calendly extractors are **not** ported in this plan — they keep running from GTM's folder. Only extractors we touch in Phase 1–2 (Fathom, Stripe, Typeform) move to Merge.

### Deferred to Follow-Up Work

- Fanbasis repair (separate diagnostic session)
- GHL scheduler staleness fix (surface during U1 preflight; may slot into cutover or spin off)
- Webhook-based real-time ingestion for Calendly (only port `webhook.py` if polling proves insufficient)
- Retirement of the public dashboard URL once SDR/Manager Metabase accounts come online

---

## Context & Research

### Relevant Code and Patterns

**Merge-side (this repo):**
- `2-dbt/profiles.yml` (not yet in repo — will be created/updated in U2)
- `2-dbt/dbt_project.yml` — `dee_data_ops` project name; schema routing already expects `staging/warehouse/marts` layering that matches GTM's Phase-2 convention
- `2-dbt/models/staging/{ghl,calendly,stripe,typeform,fanbasis}/` — existing staging assumes per-object shape; will rewire in U3
- `2-dbt/models/warehouse/{bridges,dimensions,facts,volume_monitor}/` — existing; replays in U4a
- `2-dbt/models/marts/` — existing; replays in U4a
- `2-dbt/models/marts/rollups/speed_to_lead/` — locked metric's rollup layer; parity-gated
- `3-bi/metabase/authoring/` — Metabase authoring code; tile definitions survive cutover, connection changes

**GTM-side (source project, still running):**
- `project-41542e21-470f-4589-96d` — the new home
- `Raw` dataset — legacy single-blob tables for stripe/typeform/fathom/fanbasis/older GHL
- `raw_ghl`, `raw_calendly` — Phase-2 per-object tables (Merge's staging expects this shape)
- `Core.bqml_fathom_sales_call_classifier` — trained logistic-regression model
- 17 Cloud Run Jobs (enumerate during U1 preflight)
- Secret Manager entries for source API tokens (enumerate during U1)

**BigQuery verified state (2026-04-23):**
- `dee-data-ops-prod.marts.speed_to_lead_detail`: 15,283 rows, materialized 2026-04-23 11:56
- `dee-data-ops-prod.warehouse.fct_speed_to_lead_touch`: 15,283 rows
- `dee-data-ops-prod.warehouse.dim_contacts`: 15,991 rows
- `dee-data-ops-prod.staging.*`: all views; 0 materialized rows (they resolve at query time against raw, which is in `dee-data-ops` dev)
- `project-41542e21-...` raw: GHL 68k rows (3-day stale), Calendly 10.9k (fresh), Stripe 7.6k (fresh), Typeform 5.1k (fresh), Fathom 1.2k / 56 MB (fresh), Fanbasis 3 (broken)

### Institutional Learnings

No `docs/solutions/` exists yet. Relevant existing state from `.claude/state/project-state.md`:
- Speed-to-Lead headline metric locked 2026-04-19
- `GCP_SA_KEY_PROD` repo secret still unset — CI/CD blocked (becomes moot if CI retargets to GTM's project with a different service account)
- Stripe Fivetran sync gap noted in project state (not the main ingestion problem anymore — whole connector gets torn down)

### External References

- `dbt_style_guide.md` (repo root) — SQL/naming conventions
- GTM's `CLEANUP_PLAN.md` — phased migration design that inspired the Phase-2 per-object shape
- GTM's `ARCHITECTURE.md` — identity-spine design doc + known attribution leaks

---

## Key Technical Decisions

- **`project-41542e21-470f-4589-96d` becomes the single prod GCP project.** Merge's `dee-data-ops-prod` + `dee-data-ops` decommissioned post-cutover. Rationale: preserves 2 years of working ingestion infrastructure; David is sole stakeholder; billing is on the same account; naming hygiene doesn't outweigh preservation of effort.
- **Merge's dbt project stays canonical.** GTM's `gtm_lead_warehouse` dbt project is not touched; it's effectively abandoned alongside `dee-data-ops-prod`. Rationale: Merge's dbt is cleaner, tested, and already has the locked-metric logic.
- **Warehouse + mart replay, not `bq cp`.** `dbt build` regenerates the 15,283 Speed-to-Lead rows and other marts directly in `project-41542e21-...`. Parity against `dee-data-ops-prod` is the cutover gate. Rationale: cleaner than table copies; exercises the full dbt chain; surfaces any staging-shim bugs immediately. *(Exception: U4a uses `bq cp` to snapshot GTM's raw datasets so the parity comparison is against a frozen baseline — this is a raw-side technique, not a warehouse/mart copy, and does not contradict the replay rule.)*
- **Phase-2 per-object migration happens in-place inside `project-41542e21-...`.** Stripe/Typeform/Fathom extractors upgraded (or a one-shot transform added) to split single-blob `Raw.<source>_objects_raw` into `raw_<source>.<source>__<obj>_raw`. Rationale: matches the shape Merge's staging models already expect; no extractor rebuild from scratch.
- **Fathom BQML classifier stays in `Core`; accessed cross-dataset.** No retrain required. Later, if `Core` gets cleaned up, the model can be recreated in `warehouse`. Rationale: defers work that isn't blocking.
- **Staging shim for single-blob sources until Phase-2 lands.** For Stripe/Typeform/Fathom, a temporary `stg_<source>__<obj>` view decodes the JSON blob. Retired as soon as the per-object tables exist. Rationale: unblocks cutover without serializing on the Phase-2 refactor.
- **Three HARD GATES:** (1) plumbing parity for the 15,283 Speed-to-Lead rows (U4a→U5), (2) Stripe revenue parity after Phase-2 (U8), (3) identity-spine parity before mart swap (U12). U4b (live-raw business parity) is a soft/soak gate feeding U14, not a PR gate. No PR merges on the three hard gates without David sign-off on the parity proof.
- **`_gtm-import/` quarantine folder is dropped from this plan.** Original plan assumed David would drag files between repos; in the consolidation design, nothing gets copied — Merge's code stays in this repo and points at GTM's GCP project.
- **Extractors migrate to Merge on first touch.** When a Fathom / Stripe / Typeform extractor gets modified in Phase 1–2 (U6/U7/U9), it moves into Merge's `1-raw-landing/<source>/` folder as part of the same unit. Cloud Run Jobs for those sources are redeployed from Merge going forward. GHL and Calendly extractors stay in GTM's folder for now (not being touched; can migrate later if/when they need a code change). End state: one repo owns all live code; GTM's folder becomes a historical archive.

---

## Open Questions

### Resolved During Planning

- *Which GCP project is the single home?* `project-41542e21-470f-4589-96d` (David 2026-04-23).
- *Does the 15,283-row Speed-to-Lead state move via `bq cp` or dbt replay?* Replay — cleaner, validates the cutover.
- *Does the BQML classifier get retrained?* No, not in this plan. Stays in `Core`.
- *Does Merge's staging work against GTM's per-object raw for GHL/Calendly?* Expected yes (same shape). Verify during U1 preflight by compiling a single model.
- *Does Fivetran stay during the transition?* No — it gets torn down in U13 after Stripe + Typeform parity holds.

### Deferred to Implementation

- **Exact Cloud Run service-account permissions for the Merge-side CI that builds dbt in `project-41542e21-...`.** Needs a new service account or a permissions grant on the existing one. Resolved in U1.
- **Which secrets in GTM's Secret Manager are needed by the dbt CI job?** dbt doesn't need source API keys, only BQ credentials. But volume-monitor and freshness checks may. Inventoried in U1.
- **Whether the existing Fathom transcript issue is in the extractor, the enrichment pipeline, or the BQML feature engineering.** Diagnosed in U6.
- **Whether GTM's GHL 3-day staleness is a scheduler disablement, quota throttle, or silent API failure.** Diagnosed in U1 preflight; fix during cutover if trivial.
- **Whether Phase-2 per-object migration uses (a) extractor rewrite or (b) one-shot transform job that reads `Raw.<source>_objects_raw` and writes `raw_<source>.<source>__<obj>_raw`.** Decided during U7.
- **Public dashboard embed token — is it regenerated, or can the existing token be reused against the new BQ connection?** Resolved during U5.

---

## Output Structure

```
1-raw-landing/
  fathom/                                # NEW — U6 (ported from GTM with transcript fix)
    __init__.py
    extract.py
    client.py
    backfill.py
    Dockerfile
    requirements.txt
    README.md
  stripe/                                # NEW — U7 (ported from GTM, upgraded to Phase-2)
    __init__.py
    extract.py
    client.py
    objects.py
    backfill.py
    Dockerfile
    requirements.txt
    README.md
  typeform/                              # NEW — U9 (ported from GTM, upgraded to Phase-2)
    (same shape)
  ghl/                                   # unchanged — stays at GTM for now
  calendly/                              # unchanged — stays at GTM for now
  fanbasis/                              # unchanged (broken; deferred)
enrichment/
  fathom/                                # NEW — U6 (ported from GTM)
    prompts/
    sql/
    README.md
1-raw-landing/deploy/
  fathom-job.yaml                        # NEW — U6 (redeployed from Merge)
  stripe-job.yaml                        # NEW — U7
  typeform-job.yaml                      # NEW — U9
2-dbt/
  profiles.yml                           # MODIFIED — U2
  dbt_project.yml                        # unchanged
  models/
    staging/
      fathom/                            # NEW — U3 (shim) → U6 (real)
        stg_fathom__calls.sql
        stg_fathom__call_intelligence.sql
        _fathom__sources.yml
      stripe/                            # MODIFIED — U3 (shim) → U8 (rewired)
      typeform/                          # MODIFIED — U3 (shim) → U10 (rewired)
    warehouse/
      facts/
        fct_fathom_calls.sql             # NEW — U6
      bridges/
        bridge_email_canon.sql           # NEW — U11
        bridge_contact_closer.sql        # NEW — U11
        bridge_contact_payment.sql       # NEW — U11
    marts/
      rollups/
        fathom/
          fth_call_outcome_30d.sql       # NEW — U6
        typeform/
          tf_lead_magnet_30d.sql         # NEW — U10
  macros/
    generate_custom_schema.sql           # MODIFIED — U2 (if project-based routing is needed)
  analyses/
    bqml_fathom_classifier_predict.sql   # NEW — U6
docs/
  parity/
    cutover-speed-to-lead-plumbing-parity.sql  # NEW — U4a
    live-raw-parity.sql                  # NEW — U4b
    stripe-revenue-parity.sql            # NEW — U8
    identity-spine-parity.sql            # NEW — U12
  runbooks/
    gcp-consolidation-cutover.md         # NEW — U1–U5
    fivetran-teardown.md                 # NEW — U13
    dee-data-ops-decommission.md         # NEW — U14
.env.example                             # MODIFIED — U2
.github/workflows/
  dbt-deploy.yml                         # MODIFIED — U2 (new project target)
  cloud-run-deploy.yml                   # NEW — U6 (deploys 1-raw-landing/<source>/ to Cloud Run)
```

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```
   BEFORE                                       AFTER (target)
┌──────────────────────┐                      ┌──────────────────────────────┐
│ project-41542e21-…   │                      │ project-41542e21-…           │
│ (GTM's GCP)          │                      │ (consolidated)               │
│                      │                      │                              │
│  Cloud Run Jobs ▶    │                      │  Cloud Run Jobs ▶            │
│    Raw.* + raw_ghl/* │                      │    raw_ghl/*, raw_calendly/* │
│    BQML classifier   │                      │    raw_stripe/* (Phase-2)    │
│                      │                      │    raw_typeform/* (Phase-2)  │
│  Looker Studio       │                      │    raw_fathom/* (Phase-2)    │
│  (ignored)           │                      │    BQML classifier           │
└──────────────────────┘                      │                              │
                                              │  dbt (dee_data_ops) ▶        │
┌──────────────────────┐                      │    staging/warehouse/marts   │
│ dee-data-ops-prod    │                      │                              │
│                      │    ▶  REPLAY  ▶      │  Metabase v1.6               │
│  dbt staging (views) │                      │    Speed-to-Lead + new tiles │
│  warehouse (15k fct) │                      └──────────────────────────────┘
│  marts  (15k stl)    │                                  ▲
│                      │                                  │
│  Metabase points ────┼────────── repoint ───────────────┘
│  here                │
└──────────────────────┘                      ┌──────────────────────────────┐
                                              │ dee-data-ops-prod            │
┌──────────────────────┐                      │ dee-data-ops                 │
│ dee-data-ops (dev)   │                      │      DECOMMISSIONED          │
│  Fivetran raw_stripe │    ▶ teardown ▶      │                              │
│  Fivetran raw_type…  │                      │ Fivetran:                    │
│  custom raw_ghl/*    │                      │   DISABLED + DELETED         │
└──────────────────────┘                      └──────────────────────────────┘
```

---

## Implementation Units

- [x] U1. **Cutover preflight — audit GTM's GCP + inventory what's missing in Merge CI** *(landed 2026-04-23; archived snapshot at `docs/_archive/gtm-gcp-inventory.md`)*

**Goal:** Map the terrain before cutover. Know what Cloud Run Jobs exist and their health; what Secret Manager entries exist; what service-account permissions Merge CI will need; whether GHL's 3-day staleness is benign or a bug.

**Requirements:** R1, R4 (workflow precondition)

**Dependencies:** None.

**Files:**
- Create: `docs/runbooks/gcp-consolidation-cutover.md` (preflight checklist section)
- Create/archive: `docs/_archive/gtm-gcp-inventory.md` (one-time snapshot; contents generated by the checks below)

**Approach:**
- `gcloud run jobs list --project project-41542e21-470f-4589-96d --region <known region>` — enumerate jobs, schedules, last-run status
- `gcloud secrets list --project project-41542e21-470f-4589-96d` — inventory secrets
- `bq ls project-41542e21-470f-4589-96d` + `bq ls` per dataset — datasets and table counts (partly done 2026-04-23; formalize)
- Test-compile one Merge staging model against `project-41542e21-...` raw to confirm schema compatibility:
  `dbt compile -s stg_ghl__contacts --target dev_gtm` (new target points at GTM's project)
- Diagnose GHL staleness: check Cloud Run Job last-run, `bq query` the max `_ingested_at` in `raw_ghl.ghl__contacts_raw` against job schedule

**Patterns to follow:**
- Existing `docs/runbooks/` style

**Test scenarios:**
- Happy path: preflight document enumerates ≥ 6 Cloud Run Jobs, ≥ 1 Secret per source, and records "compatible" or "incompatible" for each Merge staging model against GTM's raw shape.
- Error path: if any Merge staging model fails to compile against GTM's raw, the incompatibility is logged with a proposed fix (usually a column-name or type adjustment).

**Verification:**
- Preflight doc exists; David reviews before U2 starts.
- GHL staleness has a diagnosis (fix-in-place, scheduler issue, or acceptable-as-is).

---

- [x] U2. **Retarget Merge's dbt profile, env, and CI to `project-41542e21-...`** *(landed 2026-04-23 as PR #67, commit `5d85afc`)*

**Goal:** All dbt commands from Merge's repo build into GTM's GCP project.

**Requirements:** R1

**Dependencies:** U1.

**Files:**
- Modify: `2-dbt/profiles.yml` (update `project:` field in dev/ci/prod targets)
- Modify: `.env.example` (`GCP_PROJECT_ID_DEV`, `GCP_PROJECT_ID_PROD`)
- Modify: `.github/workflows/dbt-deploy.yml` (change project target; update service-account secret reference)
- Modify: `2-dbt/macros/generate_custom_schema.sql` if any project-name logic is hardcoded
- Modify: `CLAUDE.md` (replace `dee-data-ops*` references with `project-41542e21-...`)
- Modify: `.claude/state/project-state.md` (snapshot new home)

**Approach:**
- Target `project-41542e21-...` in all three targets (dev, ci, prod). Schemas still route via `generate_custom_schema`: `dev_<user>`, `ci_pr_<n>`, and the production layer names.
- Secret Manager: Merge CI needs a service-account key for `project-41542e21-...` with BQ job-user + data-editor on the working datasets.
- Do **not** delete `dee-data-ops-prod` or `dee-data-ops` references until U14 — keep them documented as "decommissioned" until actually deleted.

**Execution note:** Verify-by-dry-run. Before merging: `dbt compile --target prod` must succeed from a fresh checkout. No `dbt build` yet (that's U4a).

**Patterns to follow:**
- Existing `profiles.yml` structure
- Existing CI workflow patterns in `.github/workflows/`

**Test scenarios:**
- Happy path: `dbt debug --target dev` green from David's laptop pointing at `project-41542e21-...`.
- Happy path: `dbt debug --target prod` green from CI (after service-account key is provisioned).
- Error path: missing IAM permission fails loudly with a clear "grant BQ data-editor on dataset X" message.
- Integration: a sample `dbt compile -s stg_ghl__contacts` produces valid SQL against the new project.

**Verification:**
- `dbt debug` green on all three targets.
- CI run on a dummy PR compiles successfully.

---

- [x] U3. **Staging shims (Stripe / Typeform / Fathom / Calendly) + GHL column-rename** *(landed 2026-04-23 as PR #69, commit `9ef4a0b`)*

**Goal:** Make Merge's dbt staging models resolve against GTM's current raw shape so that U4a plumbing parity can run end-to-end. Covers four blob-shim layers over `Raw.<source>_objects_raw` tables plus a column-rename pass over GHL staging — the per-object `raw_ghl` data that exists today uses column names Merge's staging does not.

**Requirements:** R1

**Dependencies:** U2.

**Files:**
- Modify: `2-dbt/models/staging/stripe/_stripe__sources.yml` (source table: `Raw.stripe_objects_raw`)
- Modify: `2-dbt/models/staging/stripe/stg_stripe__charges.sql` (JSON decode from object_type='charge')
- Modify: `2-dbt/models/staging/stripe/stg_stripe__customers.sql` (JSON decode from object_type='customer')
- Modify: `2-dbt/models/staging/typeform/_typeform__sources.yml`
- Modify: `2-dbt/models/staging/typeform/stg_typeform__responses.sql`
- Create: `2-dbt/models/staging/fathom/_fathom__sources.yml`
- Create: `2-dbt/models/staging/fathom/stg_fathom__calls.sql`
- Create: `2-dbt/models/staging/fathom/_fathom__models.yml`
- Modify: `2-dbt/models/staging/calendly/_calendly__sources.yml` (source table: `Raw.calendly_objects_raw`; Phase-2 `raw_calendly.*` tables are scaffolded but empty — confirmed in U1 preflight §7)
- Modify: `2-dbt/models/staging/calendly/stg_calendly__scheduled_events.sql` (JSON decode from entity_type='scheduled_events')
- Modify: `2-dbt/models/staging/calendly/stg_calendly__event_invitees.sql` (entity_type='event_invitees')
- Modify: `2-dbt/models/staging/calendly/stg_calendly__event_types.sql` (entity_type='event_types')
- Modify: `2-dbt/models/staging/ghl/_ghl__sources.yml` (per-table `identifier: ghl__<obj>_raw` overrides; `schema: raw_ghl` unchanged)
- Modify: `2-dbt/models/staging/ghl/stg_ghl__*.sql` (source CTE only — alias `entity_id AS id` and `SAFE.TO_JSON_STRING(payload_json) AS payload`; body of each staging model unchanged below the CTE)
- Test: `2-dbt/tests/staging_shim_row_count_sanity.sql` (each blob-shim decodes at least N rows per object/entity type)
- Test: `2-dbt/tests/staging_ghl_column_rename_parity.sql` (renamed CTE emits the same id set as legacy dee-data-ops-prod staging, characterization)

**Approach:**
- **Blob-shim pattern (Stripe / Typeform / Fathom / Calendly):** each shim is a CTE that reads `Raw.<source>_objects_raw`, filters on `object_type` or `entity_type`, and `JSON_EXTRACT_SCALAR`'s the fields needed downstream.
- Shim is intentionally temporary — commented `-- TODO: retire when raw_<source>.<source>__<obj>_raw exists and is populated (U7/U9 + Calendly Phase-2 landing in GTM)`.
- **Calendly is added to U3 per U1 finding.** Phase-2 `raw_calendly.*` tables exist in GTM but hold 0 rows. Live data is in `Raw.calendly_objects_raw` under `entity_type in ('scheduled_events', 'event_invitees', 'event_types')`. Treat exactly like Stripe/Typeform/Fathom — same blob-shim pattern, separate entity_type filters per staging model.
- **GHL column-rename pattern.** GHL Phase-2 tables (`raw_ghl.ghl__<obj>_raw`) have columns `(entity_id, _ingested_at, payload_json, location_id, partition_date, event_ts, updated_at_ts, source, backfill_run_id, is_backfill)`. Merge staging reads `(id, _ingested_at, payload)`. Keep each `stg_ghl__*.sql` body intact; change only the `source` CTE to `SELECT entity_id AS id, SAFE.TO_JSON_STRING(payload_json) AS payload, _ingested_at FROM {{ source('ghl', '<obj>') }}`. Route the actual table name via `identifier: ghl__<obj>_raw` per-table overrides in `_ghl__sources.yml`, so `{{ source() }}` calls in staging SQL do not change.
- **GHL Phase-2 partial coverage** (U1 preflight §7): `messages`, `notes`, `tasks`, `users` have 0 rows in `raw_ghl`. Default plan: add an additional blob-shim against `Raw.ghl_objects_raw` filtered by `entity_type` for each of the four missing objects, using the same column-rename CTE shape. Revisit per-table during U3 execution if any of the four turns out to be unused downstream.

**Execution note:** Write a shim once, not per-object. Each new staging model that needs a blob source grows by one CTE line, not a full file. GHL column-rename is one CTE line per source call too.

**Patterns to follow:**
- Existing `stg_ghl__contacts.sql` body (unchanged post-rename) for staging structure.

**Test scenarios:**
- Happy path: `stg_stripe__charges` returns non-zero rows when run against GTM's `Raw.stripe_objects_raw`.
- Happy path: `stg_calendly__scheduled_events` returns non-zero rows against `Raw.calendly_objects_raw` filtered on entity_type.
- Happy path: `stg_ghl__contacts` emits the same id set as the legacy dee-data-ops-prod staging run (characterization).
- Edge case: a JSON field missing from some rows yields `NULL`, not a compile error.
- Edge case: a row with `object_type` / `entity_type` not in the expected enum is dropped from the shim (not crashing the build).
- Integration: downstream warehouse model `fct_revenue` builds from shim-sourced Stripe data without error.
- Integration: downstream `fct_speed_to_lead_touch` builds from shim-sourced Calendly + renamed GHL without error.

**Verification:**
- `dbt build --target dev -s +fct_revenue +fct_speed_to_lead_touch` succeeds.
- Row count in shim-sourced `stg_stripe__charges` is within 10% of what Fivetran's `raw_stripe.charge` produced (order-of-magnitude check).
- GHL rename characterization test passes — same id set as legacy prod staging.

---

- [ ] U4a. **Plumbing parity — frozen snapshots, dbt wiring proves out (HARD GATE for U5)**

**Goal:** Prove the Merge dbt chain (U2 retarget + U3 shims + GHL rename) reproduces Speed-to-Lead state when given identical raw. Use a point-in-time snapshot of GTM's raw and a frozen `dee-data-ops-prod` baseline so raw-level drift (the `bq-ingest` staleness flagged in U1 preflight §13) is out of the comparison.

**Requirements:** R1, R3

**Dependencies:** U3 (all staging resolves).

**Files:**
- Create: `docs/parity/cutover-speed-to-lead-plumbing-parity.sql`
- Create: `2-dbt/tests/cutover_plumbing_parity_holds.sql`
- Create: `ops/bq/snapshot_gtm_raw.sh` (one-shot `bq cp` script — snapshot dataset cleanup documented)

**Approach:**
- `bq cp` GTM's `Raw.*` + `raw_ghl.*` + `raw_calendly.*` into a frozen snapshot dataset (e.g. `raw_snapshot_u4a_<YYYYMMDD>`) in `project-41542e21-...`. Everything downstream runs against the snapshot, not live raw.
- Simultaneously freeze `dee-data-ops-prod`'s `warehouse.*` and `marts.*` at the same wall-clock using BigQuery time-travel (`FOR SYSTEM_TIME AS OF <ts>`) — no copy needed, 7-day window.
- Point Merge's dbt staging at the snapshot dataset (one-line target override or a dedicated `plumbing_parity` target in `profiles.yml`) and run `dbt build --target plumbing_parity` end-to-end.
- Parity query (cross-project, with BQ time-travel on the prod side): compare row counts and key aggregates for:
  - `fct_speed_to_lead_touch` (expect exact match — snapshot is frozen, no incrementals in play)
  - `speed_to_lead_detail` (exact match)
  - `stl_headline_7d` headline metric (exact match)
  - `fct_revenue` sum (exact match)
  - `dim_contacts` row count (within 0.1%)
- If parity fails, diagnose: missing raw, schema drift, shim bug, rename-CTE bug. **Do not proceed to U5 until plumbing parity holds.**

**Execution note:** Test-first on parity. The parity SQL exists before the replay happens. Snapshot + baseline freeze are captured within the same ~10-minute window so the comparison is meaningful.

**Patterns to follow:**
- `dbt_expectations.expect_table_row_count_to_equal_other_table` is the idiom.

**Test scenarios:**
- Parity (hard gate): `fct_speed_to_lead_touch` row count exactly matches frozen baseline.
- Parity: `stl_headline_7d` headline metric identical.
- Parity: `sum(fct_revenue.amount_cents)` identical.
- Parity: `dim_contacts` email_canon distribution matches (shape test, not row-by-row).
- Error path: any parity failure produces a diagnostic report listing which model diverged and by how much.

**Verification:**
- `dbt build` green in `project-41542e21-...` against the snapshot.
- Plumbing parity SQL returns zero rows (no divergence).
- David signs off before U5.

---

- [ ] U4b. **Business parity — live raw, after `bq-ingest` fix (parallel with U6–U8)**

**Goal:** Once `bq-ingest` is healthy and GTM's raw is live again, prove that Merge's dbt running against live raw produces the same locked-metric numbers as `dee-data-ops-prod` running against its `dee-data-ops` raw. Catches drift the frozen U4a comparison couldn't see (incremental-logic bugs, late-arriving rows, raw-schema drift that only appears on fresh data).

**Requirements:** R3

**Dependencies:** U4a passed **+ `bq-ingest` service repaired** (external work in GTM's repo — not a Merge PR). Parallelizable with U6–U8; **not** a gate for U5.

**Files:**
- Create: `docs/parity/live-raw-parity.sql`
- Create: `2-dbt/tests/live_raw_parity_holds.sql`
- Create: `ops/monitoring/live_raw_parity_daily.sql` (scheduled query publishing the delta)

**Approach:**
- Stand up a daily cross-project parity check: same headline-metric query run against `project-41542e21-...` and `dee-data-ops-prod`; publish delta to `ops.live_raw_parity_daily`.
- Tolerance bands: headline metric within ± 0.1 pp; `fct_speed_to_lead_touch` row count within 0.1% (to absorb legitimate incremental-window differences between the two projects).
- Run for 7 days. If delta stays within band every day, business parity is proven.
- **Not a gate for U5** — U5 is already live on U4a plumbing proof. **Is a gate for U14** (`dee-data-ops-prod` decommission).

**Execution note:** This step is scheduled-query flavored, not PR-flavored. Each day's delta gets published and reviewed. No dbt build happens in U4b — it's observational.

**Test scenarios:**
- Happy path: 7 consecutive days within tolerance band.
- Error path: any day's delta exceeds band → diagnose which table / column / filter diverges → fix lives in Merge staging or in GTM ingest, both possible.

**Verification:**
- 7-day rolling window green.
- David signs off on "live parity proven" — feeds into U14 decommission gate.

---

- [ ] U5. **Repoint Metabase at `project-41542e21-...`; verify v1.6 tiles unchanged**

**Goal:** Move the live Metabase v1.6 Speed-to-Lead dashboard off `dee-data-ops-prod` onto the consolidated project with no visible change to the tiles.

**Requirements:** R2, R3

**Dependencies:** U4a (plumbing parity proven). U4b runs in parallel and does not gate U5.

**Files:**
- Modify: `3-bi/metabase/authoring/infrastructure/bigquery_connection.py` (project name, service-account key path)
- Modify: `3-bi/metabase/authoring/sync.py` if it hardcodes the project
- Create: `docs/runbooks/metabase-connection-cutover.md` (step-by-step for the repoint + rollback)

**Approach:**
- Option A (safer): add a **new** Metabase BQ database entry pointed at `project-41542e21-...`; clone the dashboard onto it; verify tile-by-tile; cut over by changing the database reference on the canonical dashboard.
- Option B (faster, riskier): edit the existing BQ database's connection config in place. Easier rollback path: revert the config.
- Re-issue embed token for the public dashboard URL; update wherever that URL lives.
- **Do not** delete the `dee-data-ops-prod` BQ database entry from Metabase until U14 — it's the rollback path.

**Test scenarios:**
- Happy path: a predefined tile-by-tile screenshot set (before/after) shows identical numbers on all v1.6 tiles.
- Happy path: public dashboard URL loads with same numbers (plus updated data freshness).
- Error path: any tile showing different numbers triggers an immediate rollback to the `dee-data-ops-prod` connection; diff root-caused before retry.

**Verification:**
- All v1.6 tiles visually identical.
- Public URL live.
- David confirms on a live call.

---

- [ ] U6. **Fathom extractor port + transcript fix + BQML inference + first Metabase tile**

**Goal:** Move the Fathom extractor from GTM's folder into Merge's `1-raw-landing/fathom/`, fix the transcript landing issue as part of the port, and surface call intelligence on Metabase. This is also the first extractor ported — it establishes the `1-raw-landing/<source>/` + `1-raw-landing/deploy/<source>-job.yaml` + `.github/workflows/cloud-run-deploy.yml` pattern for U7/U9.

**Requirements:** R4

**Dependencies:** U5 (cutover holds).

**Files:**
- Create: `1-raw-landing/fathom/extract.py` (ported + fixed from GTM `ingest/sources/fathom/extract.py`)
- Create: `1-raw-landing/fathom/client.py` (ported from GTM)
- Create: `1-raw-landing/fathom/backfill.py` (ported from GTM)
- Create: `1-raw-landing/fathom/__init__.py`, `Dockerfile`, `requirements.txt`, `README.md`
- Create: `enrichment/fathom/prompts/` (ported from GTM `enrichment/fathom/prompts/`)
- Create: `enrichment/fathom/sql/` (ported from GTM)
- Create: `enrichment/fathom/README.md`
- Create: `1-raw-landing/deploy/fathom-job.yaml` (job definition deployed from Merge)
- Create: `.github/workflows/cloud-run-deploy.yml` (new workflow; reusable for Stripe/Typeform in U7/U9)
- Create: `2-dbt/analyses/bqml_fathom_classifier_predict.sql`
- Modify: `2-dbt/models/staging/fathom/stg_fathom__calls.sql` (replace shim with real staging — still single-blob source until a separate Phase-2 unit for Fathom, unless scoped here)
- Create: `2-dbt/models/warehouse/facts/fct_fathom_calls.sql`
- Create: `2-dbt/models/marts/rollups/fathom/fth_call_outcome_30d.sql`
- Modify: `3-bi/metabase/authoring/dashboards/speed_to_lead.py` (add a call-intelligence tile) OR create a new dashboard authoring file
- Test: `1-raw-landing/fathom/test_extract.py` (transcript fetch, characterization)
- Test: `2-dbt/tests/fathom_transcript_coverage.sql` (≥ 80% of calls have a non-null transcript)
- Archive (optional): touch a `DEPRECATED` marker in GTM's `ingest/sources/fathom/` so future sessions don't edit the wrong copy

**Approach:**
- **Diagnose first, port second.** `bq query` to find how many rows in `Raw.fathom_calls_raw` have transcript JSON populated vs empty. Expected: most are empty (David's stuck point). Likely root causes to investigate: (a) Fathom API requires a separate async call to fetch transcripts, (b) API quota throttle, (c) auth scope missing transcript permissions, (d) extractor only captures metadata.
- **Port + fix in one PR.** Copy `ingest/sources/fathom/*` into `1-raw-landing/fathom/`, adjust imports, apply the transcript fix. Port `enrichment/fathom/*` alongside.
- **Cutover sequence:** (1) deploy Merge-side Cloud Run Job writing to a sandbox dataset first (`raw_fathom_sandbox`) for 24h verification; (2) once transcript coverage verified ≥ 80%, flip the schedule — disable GTM's Cloud Run Job, enable Merge's pointed at the real `raw_fathom`; (3) leave GTM's extractor code in place (disabled) for 30 days as rollback.
- **BQML inference cross-dataset.** `fct_fathom_calls` joins staging to `Core.bqml_fathom_sales_call_classifier` via `ML.PREDICT`. No retrain.
- **First Metabase tile:** outcome distribution by SDR, last 30 days.

**Execution note:** Characterization-first. Capture the current transcript-coverage percentage before changing anything; improvement is measured against that baseline. The port itself is also characterization-gated — the Merge-side extractor must produce the same row-count-per-run envelope as GTM's before the cutover flip.

**Patterns to follow:**
- Existing `1-raw-landing/ghl/` folder layout
- Existing `1-raw-landing/deploy/` job pattern (verify what's there during U1 preflight)

**Test scenarios:**
- Happy path: after fix, ≥ 80% of new Fathom calls land with a non-null transcript.
- Edge case: a call with no audio file (metadata-only) still lands a row with `transcript IS NULL` (not a crash).
- Integration: `fct_fathom_calls.predicted_outcome` populates for at least 80% of calls.
- Regression: `dbt_expectations.expect_column_proportion_of_unique_values_to_be_between` on `call_id` shows no duplicates after the fix.

**Verification:**
- Transcript coverage percentage improves measurably.
- Metabase call-intelligence tile renders; counts match SQL.
- BQML predictions visible on at least 80% of last-30-day calls.

---

- [ ] U7. **Port Stripe extractor to Merge + Phase-2 per-object upgrade**

**Goal:** Move the Stripe extractor from GTM's folder into Merge's `1-raw-landing/stripe/` *and* upgrade it to land per-object `raw_stripe.stripe__<obj>_raw` tables instead of the single-blob `Raw.stripe_objects_raw`.

**Requirements:** R5

**Dependencies:** U5, U6 (Cloud Run deploy pattern established).

**Files:**
- Create: `1-raw-landing/stripe/extract.py` (ported + upgraded from GTM)
- Create: `1-raw-landing/stripe/client.py` (ported from GTM)
- Create: `1-raw-landing/stripe/objects.py` (new — declares the Stripe object types that each land in their own `stripe__<obj>_raw` table)
- Create: `1-raw-landing/stripe/backfill.py` (ported; also handles the one-shot migration from `Raw.stripe_objects_raw`)
- Create: `1-raw-landing/stripe/__init__.py`, `Dockerfile`, `requirements.txt`, `README.md`
- Create: `1-raw-landing/deploy/stripe-job.yaml`
- Create: `docs/runbooks/stripe-phase2-migration.md`
- Test: `1-raw-landing/stripe/test_extract.py`
- Test: `2-dbt/tests/stripe_phase2_row_count_parity.sql`
- Archive: `DEPRECATED` marker in GTM's `ingest/sources/stripe/`

**Approach:**
- Inventory Stripe object_types present in `Raw.stripe_objects_raw` — at minimum: `charge`, `checkout_session`, `checkout_session_line_item`, `customer`, `invoice`, `payment_intent`, `balance_transaction`. Record in `1-raw-landing/stripe/objects.py`.
- Merge-side extractor writes directly to `raw_stripe.stripe__<object>_raw` with date partition + `object_id` clustering (pattern from `raw_ghl.*`).
- Backfill existing rows from `Raw.stripe_objects_raw` via a one-shot SQL transform in `1-raw-landing/stripe/backfill.py` (also replayable if gaps surface).
- **Dual-write window:** for 7 days, GTM's extractor (single-blob) and Merge's extractor (per-object) both run. Parity SQL compares row counts. At day 7, disable GTM's job.
- Leave GTM's Stripe extractor code in place (disabled, with a `DEPRECATED` marker) for 30 days as rollback.

**Execution note:** Characterization-first. Before modifying extractor behavior, a dry-run of the new extractor against a Stripe test-mode account must produce rows with the same columns the downstream warehouse expects.

**Test scenarios:**
- Parity: row count per object in per-object tables matches the blob filter (`SELECT count(*) FROM Raw.stripe_objects_raw WHERE object_type='charge'`).
- Happy path: a new incremental run writes to per-object tables only (not the blob) after cutover.
- Edge case: an object_type not in the expected list lands in an "other" overflow table, not dropped silently.

**Verification:**
- Per-object tables exist; row counts match blob filter; dbt staging reads successfully.

---

- [ ] U8. **Stripe staging rewire + Speed-to-Lead revenue parity gate**

**Goal:** Swap dbt staging from the U3 shim to the U7 per-object tables; prove revenue tiles unchanged.

**Requirements:** R3, R5

**Dependencies:** U7.

**Files:**
- Modify: `2-dbt/models/staging/stripe/_stripe__sources.yml` (point at `raw_stripe.stripe__<obj>_raw`)
- Modify: `2-dbt/models/staging/stripe/stg_stripe__*.sql` (drop JSON decode CTEs; pure column projection)
- Create: `docs/parity/stripe-revenue-parity.sql`
- Test: `2-dbt/tests/stripe_rewire_parity.sql`

**Approach:**
- Parity SQL compares `fct_revenue` and `revenue_detail` (sum of `amount_cents`, row count) before and after the staging swap.
- 7-day dual-source window — keep shim-based staging views behind a feature flag so rollback is flipping a variable, not editing SQL.

**Test scenarios:**
- Parity (hard gate): `sum(fct_revenue.amount_cents)` identical ± 0 for 7 consecutive days.
- Parity: `speed_to_lead_detail.revenue_attributed` identical for SDR-level aggregates.
- Regression: headline metric unchanged.

**Verification:**
- 7-day parity window green.
- David signs off on the staging swap PR.

---

- [ ] U9. **Port Typeform extractor to Merge + Phase-2 per-object upgrade**

**Goal:** Same as U7 but for Typeform — move into `1-raw-landing/typeform/` and upgrade to per-object `raw_typeform.typeform__<obj>_raw` tables.

**Requirements:** R5

**Dependencies:** U7 (pattern proven twice now, by U6 and U7).

**Files:**
- Create: `1-raw-landing/typeform/extract.py`, `client.py`, `objects.py`, `backfill.py`, `__init__.py`, `Dockerfile`, `requirements.txt`, `README.md`
- Create: `1-raw-landing/deploy/typeform-job.yaml`
- Create: `docs/runbooks/typeform-phase2-migration.md`
- Test: `1-raw-landing/typeform/test_extract.py`
- Archive: `DEPRECATED` marker in GTM's `ingest/sources/typeform/`

**Approach:** Mirror U7. Expected object_types: `form`, `response`, `response_answer`, `workspace`. Backfill from `Raw.typeform_objects_raw` via one-shot SQL. 7-day dual-write; then disable GTM's job.

**Test scenarios:**
- Parity: row count per object in per-object tables matches `Raw.typeform_objects_raw` filter by `object_type`.
- Happy path: a new response lands in `raw_typeform.typeform__response_raw` after cutover, not in the blob.

**Verification:**
- Per-object tables exist; dbt staging reads successfully; 7-day dual-write parity holds before GTM's job is disabled.

---

- [ ] U10. **Typeform staging rewire + lead-magnet rollup + Metabase tile**

**Goal:** Swap Typeform staging; ship the "best lead magnet" tile.

**Requirements:** R5

**Dependencies:** U9.

**Files:**
- Modify: `2-dbt/models/staging/typeform/_typeform__sources.yml`
- Modify: `2-dbt/models/staging/typeform/stg_typeform__responses.sql`
- Create: `2-dbt/models/staging/typeform/stg_typeform__response_answers.sql`
- Create: `2-dbt/models/marts/rollups/typeform/tf_lead_magnet_30d.sql`
- Modify: Metabase authoring (add lead-magnet tile)

**Test scenarios:**
- Happy path: rollup counts match raw `bq query` for a given form-id and date range.
- Integration: Metabase tile renders; responses-per-form counts match SQL.

**Verification:**
- Lead-magnet tile live.

---

- [ ] U11. **Identity-spine rules ported as dbt bridges**

**Goal:** Port email_canon / phone_last10 / name+domain matching rules from `identity_pipeline.py` into dbt, inside `project-41542e21-...`.

**Requirements:** R6

**Dependencies:** U5 (cutover stable) — identity rules don't depend on Phase-2 migrations.

**Files:**
- Create: `2-dbt/macros/email_canon.sql` (macro or UDF)
- Create: `2-dbt/macros/phone_last10.sql`
- Create: `2-dbt/models/warehouse/bridges/bridge_email_canon.sql`
- Create: `2-dbt/models/warehouse/bridges/bridge_contact_closer.sql` (Fathom → GHL, confidence bands)
- Create: `2-dbt/models/warehouse/bridges/bridge_contact_payment.sql` (Stripe/Fanbasis → GHL)
- Modify: `2-dbt/models/warehouse/bridges/bridge_identity_contact_payment.sql` (existing — extend or replace with U11 logic)
- Test: `2-dbt/tests/bridges_gmail_dots_collapse.sql`
- Test: `2-dbt/tests/bridges_phone_variant_collapse.sql`
- Test: `2-dbt/tests/bridges_confidence_banding.sql`

**Approach:**
- Read GTM's `sources/identity/identity_pipeline.py` as a spec. Translate each matching rule to a SQL CTE.
- Preserve the `high`/`medium`/`low` confidence banding.
- Do not wire bridges into any mart yet — U12 is the gated swap.

**Execution note:** Test-first. Each rule gets a synthetic-row dbt test proving the match.

**Test scenarios:**
- `bob@gmail.com` and `b.o.b+test@gmail.com` match under `bridge_email_canon`.
- `b.o.b@googlemail.com` matches `bob@gmail.com`.
- `bob@company.com` and `bob@company.co` do **not** match.
- `+1-555-123-4567` and `(555) 123-4567` match on phone_last10.
- A name-only match against a non-unique name yields `low` confidence.
- Idempotent: running the bridge twice produces the same output.

**Verification:**
- Bridge models materialize; unit-style tests pass.

---

- [ ] U12. **Identity-spine parity gate + mart swap**

**Goal:** Prove the bridges don't change Speed-to-Lead; swap the marts to use them.

**Requirements:** R3, R6

**Dependencies:** U11.

**Files:**
- Create: `docs/parity/identity-spine-parity.sql`
- Modify: `2-dbt/models/marts/rollups/speed_to_lead/stl_headline_7d.sql`
- Modify: `2-dbt/models/marts/rollups/speed_to_lead/stl_*.sql` (any that reference contact bridging)
- Modify: `2-dbt/models/marts/lead_journey.sql`, `sales_activity_detail.sql`
- Test: `2-dbt/tests/identity_spine_parity_holds.sql`

**Approach:**
- Parity query: headline metric with old bridge vs new bridge. 14-day window, `abs(a - b) < 0.1 pp`.
- Do **not** delete the old bridging code for 30 days. Retain for rollback.

**Test scenarios:**
- Parity (hard gate): 14 consecutive days of identical headline metric.
- Diagnostic: contacts that matched under old logic but not new (or vice versa) surface in a review query; David reviews each.

**Verification:**
- 14-day parity.
- David signs off on swap.

---

- [ ] U13. **Fivetran teardown**

**Goal:** Disable Fivetran Stripe + Typeform connectors; delete Fivetran datasets; terminate Fivetran billing.

**Requirements:** R7

**Dependencies:** U8 (Stripe parity), U10 (Typeform live).

**Files:**
- Create: `docs/runbooks/fivetran-teardown.md`
- Modify: `.env.example` (strip any Fivetran vars)
- Modify: `CLAUDE.md` / project-state (remove Fivetran references)

**Approach:**
- Disable connectors in Fivetran UI.
- Wait 48 hours; confirm no alerts or user complaints.
- Delete from `dee-data-ops` (Merge dev): `raw_stripe`, `raw_typeform`, `fivetran_metadata`, `fivetran_torches_yummy_staging`.
- Confirm Fivetran billing ends in Fivetran admin.

**Test scenarios:**
- Happy path: dbt builds green without Fivetran datasets.
- Happy path: Metabase tiles unchanged.
- Error path: a forgotten reference to a Fivetran table surfaces as a dbt compile error; patched.

**Verification:**
- Fivetran connector list empty; datasets absent; zero billing events in Fivetran for 30 days.

---

- [ ] U14. **Decommission `dee-data-ops-prod` and `dee-data-ops`**

**Goal:** Retire the old GCP projects cleanly.

**Requirements:** R8

**Dependencies:** U5, U8, U12 (all three parity gates clean for 30 days); U4b (7-day live-raw band held after `bq-ingest` repair); U13 (Fivetran gone).

**Files:**
- Create: `docs/runbooks/dee-data-ops-decommission.md`

**Approach:**
- Delete datasets inside `dee-data-ops-prod` (marts, warehouse, staging, snapshots, validation).
- Delete datasets inside `dee-data-ops` (dev_david, ci, ci_pr_*, raw_ghl, raw_calendly, raw_fanbasis, snapshots).
- Remove Metabase database entry for `dee-data-ops-prod`.
- `gcloud projects delete dee-data-ops-prod` and `gcloud projects delete dee-data-ops` (after 30 days of cutover stability; projects enter a 30-day recovery window before permanent deletion — accept this as a built-in safety net).
- Update `.claude/state/project-state.md` and `CLAUDE.md` final scrub.

**Test scenarios:**
- Happy path: no downstream references to the deleted projects; Metabase, CI, and dbt all green.

**Verification:**
- `gcloud projects list` no longer shows `dee-data-ops*`.
- Billing for those projects drops to zero.

---

## System-Wide Impact

- **Interaction graph:** dbt retarget (U2) touches profiles, env, CI, macros. Metabase repoint (U5) touches every dashboard. Plumbing parity (U4a) is the single crossover point where the new project's state must match a frozen `dee-data-ops-prod` baseline; live-raw business parity (U4b) is a separate, longer-running observational check.
- **Error propagation:** A parity failure at U4a stops the plan before U5. A parity failure at U4b stops the U14 decommission but not U5–U8. A parity failure at U8 or U12 stops the respective swap. Extractor failures in `project-41542e21-...` alert via Cloud Run (separate scope; already wired).
- **State lifecycle risks:** `dee-data-ops-prod` deletion (U14) is irreversible after GCP's 30-day recovery window. The 30-day stability soak + U4b live-raw parity before U14 is the safety net.
- **API surface parity:** Metabase public dashboard URL changes embed token post-U5. Anyone bookmarking the URL continues to work; anyone embedding the token re-issues.
- **Integration coverage:** Four parity gates (U4a frozen/plumbing, U4b live/business, U8, U12) are the only cross-layer tests that prove the locked metric holds. Unit tests alone cannot prove it.
- **Unchanged invariants:** Locked Speed-to-Lead formula, SDR-attributed denominator definition, Metabase tile math. Only the backing project changes.

---

## Risks & Dependencies

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Plumbing parity (U4a) fails because Merge's staging doesn't resolve against GTM's per-object raw schema | Medium | Plan-blocking | U1 preflight already surfaced GHL column-rename + empty Calendly Phase-2; U3 covers both before U4a |
| Plumbing parity fails because of subtle column-type drift (e.g., INT64 vs STRING on an id field) | Medium | Recoverable | Diagnosed from parity SQL output; fix in staging shim / rename CTE |
| Live-raw parity (U4b) fails because `bq-ingest` regression wasn't fully fixed | Medium | Delays U14 only | U4b is observational and runs in parallel with U6–U8; U5 is already live on U4a. Fix iterates in GTM repo until band is held |
| Fathom transcript fix reveals a larger API-design issue (async jobs, auth scopes) | High | Time cost | Scope-contained to U6; worst case: ship the call-intelligence tile with metadata-only and flag transcripts as a follow-up |
| Phase-2 per-object migration (U7/U9) breaks an incremental extractor | Medium | Lost data during migration window | Dual-write window; backfill from blob if gaps appear |
| Metabase repoint (U5) produces subtly different tile numbers despite parity-green dbt | Low | User confusion | Tile-by-tile before/after screenshots; rollback path to `dee-data-ops-prod` connection for 30 days |
| GHL 3-day staleness is caused by a quota or permission issue that re-surfaces inside Merge CI | Medium | Ingestion gap | Diagnosed in U1; may need separate fix |
| `dee-data-ops-prod` accidentally deleted before 30-day stability window | Low | Rollback lost | U14 explicitly waits 30 days; write gate documented |
| Service-account permissions for Merge CI on `project-41542e21-...` are more restrictive than needed | Medium | Deploys fail | U1 inventory + explicit IAM grant list in preflight doc |
| Identity-spine swap (U12) produces bounded but non-zero drift that exceeds 0.1 pp threshold | Low-Medium | Parity block | Diagnostic query surfaces which contacts changed; David decides whether to accept or re-tune rule |
| Both GTM-folder and Merge-folder extractors run simultaneously, causing duplicate writes | Medium | Duplicate rows in raw; downstream drift | Dual-write windows are explicit (U6/U7/U9); disabling GTM's Cloud Run Job is the gate, not just deploying Merge's |
| Merge-side Cloud Run Job lacks the Secret Manager grants GTM's job had | High (unresolved) | Extractor can't auth to source API | Enumerate in U1 preflight; grant before deploy |
| GTM's extractor has an undocumented behavior Merge's port misses (e.g., a retry quirk, a custom transform) | Medium | Silent data loss or schema drift | Characterization-first per U6/U7/U9 execution notes; dual-write parity catches divergence |
| GTM folder and Merge folder fall out of sync during the 30-day rollback window (someone edits GTM's deprecated code) | Low | Confusing state | `DEPRECATED` marker + README note in each ported GTM folder |

---

## Phased Delivery

### Phase 1 — Cutover (Week 1)
- U1 preflight
- U2 dbt profile retarget
- U3 staging shims (Stripe / Typeform / Fathom / Calendly) + GHL column-rename
- U4a plumbing parity — frozen snapshots (**HARD GATE for U5**)
- U5 Metabase repoint
- U6 Fathom transcript fix (parallelizable with U2–U4a)

**Exit condition:** Metabase v1.6 live against `project-41542e21-...` with identical tiles; Fathom transcripts landing; first call-intelligence tile visible.

### Phase 2 — Phase-2 per-object migrations + Fivetran replacement (Week 2)
- U4b live-raw business parity (starts when `bq-ingest` is repaired; runs in parallel with U6–U8; feeds U14)
- U7 Stripe Phase-2
- U8 Stripe staging rewire + revenue parity (**HARD GATE**)
- U9 Typeform Phase-2
- U10 Typeform rewire + lead-magnet tile

**Exit condition:** Stripe + Typeform staging reads from per-object tables; revenue tiles unchanged; lead-magnet tile live.

### Phase 3 — Identity spine (Week 3)
- U11 bridges as dbt
- U12 parity gate + mart swap (**HARD GATE**)

**Exit condition:** Marts use new bridges; 14-day parity holds; David signs off.

### Phase 4 — Cleanup (Week 4 + 30-day soak)
- U13 Fivetran teardown
- U14 Decommission `dee-data-ops-*` projects (gated by 30-day stability from U5 + U4b live-raw parity held)

**Exit condition:** One GCP project, no Fivetran, clean project-state document.

---

## Documentation / Operational Notes

- Each phase gate ships a WORKLOG entry per `.claude/rules/worklog.md`.
- `.claude/state/project-state.md` updated at each HARD GATE.
- Parity SQL files kept in `docs/parity/` permanently as regression artifacts.
- Metabase before/after screenshots captured at U5 and post-U12.
- Cutover runbook (`docs/runbooks/gcp-consolidation-cutover.md`) is the step-by-step for U1–U5; written during U1 and updated as steps complete.
- Rollback paths documented for U5 (Metabase connection), U8 (staging feature flag), U12 (old bridges retained 30 days), U14 (30-day GCP recovery window).

---

## Sources & References

- **Origin conversation:** 2026-04-23 planning session with David (no formal brainstorm doc). v1 of this plan (discarded) targeted `dee-data-ops-prod`; v2 flipped the direction per David's 2026-04-23 decision.
- GTM reference repo: `/Users/david/Documents/operating system/Projects/GTM lead warehouse` (not in this repo; source material only)
- Live BigQuery inventory (2026-04-23): GTM project datasets + `dee-data-ops-prod` marts/warehouse population confirmed via `bq ls` and `bq query __TABLES__`
- Merge project state: `.claude/state/project-state.md`
- dbt style: `dbt_style_guide.md`
- Locked metric definition: `CLAUDE.local.md` (gitignored) — David is authoritative
- Memory: `project_gcp_consolidation_decision.md` — the decision that drove this rewrite
