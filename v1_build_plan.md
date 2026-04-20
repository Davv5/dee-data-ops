# v1 Build Plan — Speed-to-Lead Dashboard

**Owner:** David (solo build)
**Status:** Internal execution plan — not for client
**Companion doc:** `client_v1_scope_speed_to_lead.md` (the *what* and *why*)
**This doc:** the *how* and *in what order*
**Target ship:** 2–3 weeks from kickoff
**Last updated:** 2026-04-19

---

## How this plan is organized

Eight phases following the canonical 3-layer pipeline + 3-environment design taught in the Data Ops corpus, sequenced **right-to-left**: dashboard requirements drove the scope doc, and this plan now executes left-to-right against that target (source: *"How to Create a Data Modeling Pipeline (3 Layer Approach)"*, Data Ops notebook).

Each phase has:
- **Deliverables** — concrete artifacts produced
- **Tasks** — check-off list, in execution order
- **Files / models / workflows** — what gets created
- **Done when** — objective exit criterion
- **Open decisions (mine)** — judgment calls I still need to make (distinct from Week-0 client questions)

**Decisions still owed by the client** are tracked in the scope doc's Week 0 section, not here. This doc only tracks decisions where I am the bottleneck.

---

## Phase 0 — Prerequisites

**Goal:** working local dev environment + cloud warehouse + GitHub repo, all wired together, before a single line of model code is written.

### Deliverables
- **Two** BigQuery projects provisioned — isolation at the project/database level, not schema prefix (source: *"Building a dbt Project from Scratch" + "DBT Project Environment Setup"*, Data Ops notebook):
  - `<client>-data-ops` (dev + ci) with datasets `dev_<user>`, `ci`
  - `<client>-data-ops-prod` with clean-named datasets `staging`, `warehouse`, `marts` (no `prod_` prefix)
- Python venv + dbt-bigquery installed
- dbt project initialized via `dbt init` (never manually — source: *Data Ops corpus, "Initial Setup" guidance*)
- `.env` populated and gitignored; `profiles.yml` reading from env vars
- GitHub repo created from this template, pushed, branch protection on `main`
- `dbt debug` returns "All checks passed!"

### Tasks
- [ ] Create BigQuery project `<client>-data-ops` (dev/ci)
- [ ] Create BigQuery project `<client>-data-ops-prod` (production, isolated)
- [ ] Create service account with BigQuery Data Editor + Job User on the dev project; download JSON key
- [ ] Create datasets: `dev_<user>`, `ci` in dev project
- [ ] Create datasets: `staging`, `warehouse`, `marts` in prod project (clean names — no `prod_` prefix; the project boundary *is* the isolation)
- [ ] `python3 -m venv .venv && source .venv/bin/activate`
- [ ] `pip install dbt-bigquery dbt-utils && pip freeze > requirements.txt`
- [ ] `cd dbt && dbt init` (let it scaffold, don't hand-roll directories)
- [ ] Copy `.env.example` → `.env`; fill in `GCP_PROJECT_ID`, `BQ_KEYFILE_PATH`, `DBT_TARGET=dev`, `DBT_SCHEMA=dev_david`
- [ ] Edit `dbt/profiles.yml` to read from env vars; verify `.env` is in `.gitignore`
- [ ] Add `macros/generate_schema_name.sql` (env-based schema routing — see Phase 6 for prod schema split)
- [ ] `set -a && source .env && set +a && dbt debug` — must pass
- [ ] `dbt deps` to install `dbt-utils`
- [ ] Create GitHub repo; push initial commit on `main`; create `develop` or work directly off PR branches
- [ ] Enable branch protection: require PR + passing CI before merging to `main`

### Files / workflows created
```
dbt/
  dbt_project.yml         (from dbt init, then edited for layer configs)
  profiles.yml            (env-var driven)
  packages.yml            (dbt-utils)
  macros/
    generate_schema_name.sql
.env                      (gitignored)
.env.example              (committed)
requirements.txt
.gitignore                (must include .env, .venv, target/, dbt_packages/, logs/)
```

### Done when
- `dbt debug` passes from a clean clone after `pip install -r requirements.txt && set -a && source .env && set +a`
- Pushing a no-op PR to `main` is blocked until a CI check exists (Phase 6 will add it)

### Open decisions (mine)
- **Repo name** — `<client-shortname>-dataops` vs. embedding the client's brand name; lean toward shortname for portability when this becomes a template
- **Service account scope** — single SA for both dev and prod, or split? Default to split for blast-radius reasons; revisit if it slows me down
- **Whether to lock down the `<client>-data-ops-prod` project to the deploy-workflow SA only from day one**, or open during dev and lock down before launch. Lean: lock from day one — the whole point of the project-level split is blast-radius, so enforcing it from the start means the prod deploy is the *only* path that writes to prod

---

## Phase 1 — Ingestion (raw landing zone)

**Goal:** raw data from all 5 v1 sources landing in BigQuery `raw_*` datasets daily, with zero transformation. **Start this Day 1 of Week 1** to surface GHL API surprises early (per scope doc Risk #2).

> "The data must land in its raw unforatted process" — *Data Ops notebook*. No casts, no renames, no joins at this layer.

### Deliverables
- Fivetran connectors live for Typeform, Calendly, Stripe → `raw_typeform`, `raw_calendly`, `raw_stripe`
- Python extractors for GHL and Fanbasis writing to `raw_ghl` and `raw_fanbasis`
- GitHub Actions cron workflow running both extractors daily at 06:00 UTC
- Secrets stored in GitHub Actions secrets (never committed)
- A first full extract is in BQ and queryable

### Tasks

**Fivetran (Typeform / Calendly / Stripe):**
- [ ] Create Fivetran account (free tier — confirmed under 500K MAR cap per scope §4)
- [ ] Connect Typeform → destination dataset `raw_typeform`, sync schedule daily
- [ ] Connect Calendly → `raw_calendly`, daily
- [ ] Connect Stripe → `raw_stripe`, daily
- [ ] Verify first sync completes; spot-check row counts vs. source UIs

**Python extractors (GHL / Fanbasis):**
- [ ] Create `ingestion/ghl/` with `extract.py`, `requirements.txt`, `README.md`
- [ ] Implement incremental pull for GHL endpoints needed for v1: `contacts`, `conversations` (calls/SMS), `opportunities` (pipeline stages), `users` (SDR roster)
- [ ] Use `last_modified_since` cursor stored in a small BQ state table (`raw_ghl._sync_state`) — no full refresh after first run
- [ ] Write to `raw_ghl.<endpoint>` as `WRITE_APPEND` with a `_ingested_at` column; dedupe in staging (Phase 2)
- [ ] Repeat structure for `ingestion/fanbasis/` — endpoints: `customers`, `subscriptions`, `payments`
- [ ] Have CSV-export fallback documented in `ingestion/fanbasis/README.md` (per scope Risk #5)

**GitHub Actions cron:**
- [ ] `.github/workflows/ingest.yml` — `schedule: '0 6 * * *'`, jobs for `ghl` and `fanbasis` in parallel
- [ ] Secrets: `GHL_API_KEY`, `FANBASIS_API_KEY`, `GCP_SA_KEY` (base64-encoded JSON)
- [ ] Job uses `google-github-actions/auth@v2` to authenticate to BQ; runs `python ingestion/<source>/extract.py`
- [ ] Add manual `workflow_dispatch` trigger so I can re-run on demand without waiting for cron
- [ ] First run: trigger manually, verify rows land in BQ

### Files / workflows created
```
ingestion/
  ghl/
    extract.py
    requirements.txt
    README.md
  fanbasis/
    extract.py
    requirements.txt
    README.md
.github/workflows/
  ingest.yml
```

### Done when
- All 5 sources have at least one successful sync visible in BQ
- `ingest.yml` ran successfully on both manual trigger and scheduled trigger (wait one cycle)
- GHL pull is incremental (second run pulls only deltas, confirmed via `_sync_state` table)
- No secrets in any committed file (run `gitleaks` or eyeball-check)

### Open decisions (mine)
- **GHL endpoint selection** — pull `tasks` and `notes` in v1 too (cheap to add now, painful to backfill later) or defer to v1.5? Lean: include in v1 raw, ignore in staging until needed
- **Schema-per-source vs. table-prefix** — `raw_ghl.contacts` (separate dataset per source) is cleaner than `raw.ghl_contacts`. Going with dataset-per-source
- **Fivetran transformation toggles** — Fivetran offers light dbt-style transforms. Keep them OFF; all transforms live in our dbt project so the lineage is one-tool

---

## Phase 2 — Staging (1:1 views)

**Goal:** one staging model per raw table, materialized as a view, with light cleanup only — no joins, no business logic.

> "Staging models are 1:1 views on top of the different source tables… no joins here. Only light transformations: renaming columns to snake_case, casting dates, basic conditional statements." — *Data Ops notebook*

> "Only `stg_` models should select from sources." — `dbt_style_guide.md`

### Deliverables
- `models/staging/<source>/stg_<source>__<table>.sql` for every raw table I'll actually use in v1
- `_<source>__sources.yml` declaring sources with freshness checks
- `_<source>__models.yml` documenting columns + at-minimum unique/not_null on PKs
- Materialization config: views (set in `dbt_project.yml`)

### Tasks
- [ ] In `dbt_project.yml`, set `models.<project>.staging.+materialized: view`
- [ ] For each source, create `models/staging/<source>/_<source>__sources.yml` with `freshness:` blocks
- [ ] Build staging models in this order (smallest to largest, fastest feedback first):
  - [ ] `stg_calendly__events` (the start clock for the headline metric)
  - [ ] `stg_ghl__conversations` (the stop clock — first outbound SDR call)
  - [ ] `stg_ghl__contacts`
  - [ ] `stg_ghl__opportunities`
  - [ ] `stg_ghl__users` (SDR roster)
  - [ ] `stg_typeform__responses` (funnel attribution)
  - [ ] `stg_stripe__charges`, `stg_stripe__customers`
  - [ ] `stg_fanbasis__payments`, `stg_fanbasis__customers`
- [ ] In each staging model: rename to snake_case, cast timestamps to UTC, prefix PKs as `<entity>_id` per `dbt_style_guide.md` §"Naming and field conventions"
- [ ] For GHL appends, dedupe in staging using `qualify row_number() over (partition by id order by _ingested_at desc) = 1`
- [ ] Add `unique` + `not_null` tests on the PK of each model in `_<source>__models.yml`
- [ ] `dbt build --select staging` — must pass green

### Files created
```
dbt/models/staging/
  calendly/
    _calendly__sources.yml
    _calendly__models.yml
    stg_calendly__events.sql
  ghl/
    _ghl__sources.yml
    _ghl__models.yml
    stg_ghl__conversations.sql
    stg_ghl__contacts.sql
    stg_ghl__opportunities.sql
    stg_ghl__users.sql
  typeform/      (... same pattern)
  stripe/        (... same pattern)
  fanbasis/      (... same pattern)
```

### Done when
- `dbt build --select staging` passes with all tests green
- Every staging model has unique + not_null on its PK
- A spot-check query (`select count(*) from stg_calendly__events`) matches what's in `raw_calendly.events`

### Open decisions (mine)
- **GHL dedupe key** — `id` alone, or `(id, updated_at)`? Need to look at the actual API payload before deciding. Default to `id` + latest `_ingested_at`
- **Whether to rename the GHL "conversations" entity to "interactions" in staging** — GHL's term is unfortunate (it's not what a normal person calls a phone call). Lean: keep GHL's name in staging (1:1 fidelity), rename in warehouse/marts
- **Timezone handling** — Calendly returns UTC, GHL is account-local. Cast everything to UTC at staging; let downstream models surface a `_pt` or `_et` column only where business semantics require it

---

## Phase 3 — Warehouse (Kimball star schema)

**Goal:** dimensions and facts in `models/warehouse/`, joined only from staging models, materialized as tables, structured to support the v1 dashboard *and* the v1.5 secondary/tertiary SLAs without rework.

> "Build dimension tables (`dim_`) to hold the descriptive context… Use `dbt-utils` to generate a surrogate key… Fact tables (`fct_`) represent the core business action — numeric aggregable metrics + surrogate keys mapping to dimensions." — *Data Ops notebook*

> "Never pull directly from raw sources in this layer." — *Data Ops notebook*

### Deliverables
- Dimensions covering every entity the dashboard slices by
- Fact tables at clearly-defined grains, surrogate-keyed to dims
- All warehouse models materialized as tables
- Unique + not_null tests on every surrogate key

### Tasks

**Dimensions (build first — facts will reference them):**
- [ ] `dim_contacts` — one row per lead. Sourced from `stg_ghl__contacts` (canonical) left-joined with `stg_typeform__responses` for funnel-source enrichment
- [ ] `dim_users` — single dim, one row per user, with a `role` column (values: `SDR | Setter | Triager | DM_Setter | Closer | Owner | Unknown`). From `stg_ghl__users` enriched by `ghl_sdr_roster` seed. **Supersedes the `dim_sdrs` + `dim_aes` split** — DataOps 2026-04-20 audit mandated single-dim-with-role-attribute per Kimball. View-level `dim_sdrs` / `dim_aes` may be added later as mart-layer filters if BI ergonomics demand; do not split in the warehouse.
- [ ] `dim_pipeline_stages` (new for v1) — produced by Track D from `stg_ghl__pipelines`; one row per (pipeline_id, stage_id); carries `is_booked_stage` boolean so downstream facts/marts can surface the GHL-native booked attribute without re-deriving the rule.
- [ ] `dim_offers` — one row per offer/program. May be a seed file in v1 if not in any source system
- [ ] `dim_calendar_dates` — generated via `dbt-utils.date_spine`, one row per day; needed for "% within SLA, by day" tiles

For every dim:
- [ ] Generate surrogate key: `{{ dbt_utils.generate_surrogate_key(['<natural_key_columns>']) }} as <entity>_sk`
- [ ] Keep the natural/source key alongside (`<entity>_id`) for traceability
- [ ] unique + not_null on the surrogate key

**Facts:**
- [ ] `fct_calls_booked` — grain: one row per Calendly booking. Surrogate keys to `dim_contacts`, `dim_sdrs` (assigned SDR if known), `dim_calendar_dates`. Includes `booked_at_utc`, `scheduled_for_utc`, `cancelled_at_utc`, `event_status`
- [ ] `fct_outreach` — grain: one row per outbound **user** touch (ANY role, no SDR filter) from `stg_ghl__conversations` / `stg_ghl__messages`. Surrogate keys to `dim_contacts`, `dim_users`, `dim_calendar_dates`. Includes `touched_at_utc`, `channel` (call/sms), `direction`. **Renamed from `fct_sdr_outreach`** per DataOps 2026-04-20 audit — role filter (`WHERE first_toucher.role = 'SDR'`) lives in the `sales_activity_detail` mart, not the warehouse.
- [ ] `fct_revenue` — grain: one row per payment event. Union of Stripe + Fanbasis. Surrogate keys to `dim_contacts`, `dim_offers`, `dim_calendar_dates`

For every fact:
- [ ] Strictly numeric/aggregable measures + surrogate keys + event timestamp. No descriptive text columns (those belong in dims)
- [ ] unique + not_null on the surrogate key
- [ ] `relationships` test from each FK to its dim

**Configuration:**
- [ ] In `dbt_project.yml`: `models.<project>.warehouse.+materialized: table`
- [ ] Subdirectories: `warehouse/dimensions/`, `warehouse/facts/`
- [ ] One `_dimensions__models.yml` and `_facts__models.yml` per the style guide

### Files created
```
dbt/models/warehouse/
  dimensions/
    _dimensions__models.yml
    _dimensions__docs.md
    dim_contacts.sql
    dim_users.sql
    dim_pipeline_stages.sql
    dim_offers.sql
    dim_calendar_dates.sql
  facts/
    _facts__models.yml
    fct_calls_booked.sql
    fct_outreach.sql
    fct_revenue.sql
```

### Done when
- `dbt build --select warehouse` passes
- Every fact joins cleanly to every dim it claims a relationship with (no orphan FKs)
- Spot-check: count of `fct_calls_booked` matches count of distinct Calendly events for the same window

### Open decisions (mine)
- **AE-only views of `dim_users`** — not needed for v1 (rep leaderboard is SDRs only). If a v2 AE dashboard demands it, add a view-layer `dim_aes` at the mart boundary rather than splitting the warehouse dim
- **Should `fct_revenue` include refunds as negative rows or as a separate fact?** Lean: negative rows in v1 (simpler), split if v2 finance dashboard demands it
- **Slowly Changing Dimensions on `dim_users`** — users leave / change roles. v1: Type 1 (overwrite). Revisit when AE leaderboard needs historical attribution
- **Whether to model "first SDR outbound touch per booking" as a derived column on `fct_calls_booked` or as a separate intermediate** — derived column for v1 to keep model count down

---

## Phase 4 — Marts (wide, business-friendly)

**Goal:** one wide mart that powers the v1 dashboard and architects for v1.5/v2 without splitting into per-report tables.

> "Marts lean towards being wide and denormalized… build fewer, wider marts that can be sliced and diced directly in the BI tool. Don't create one to one for each report." — *Data Ops notebook* / `.claude/rules/mart-naming.md`

> "Drop `fct_`/`dim_` prefixes — businesses aren't familiar with facts and dimensions; use business-friendly names." — *Data Ops notebook*

### Deliverables
- `sales_activity_detail` — one wide mart at the booked-call grain, joined out to all relevant dim attributes, with the headline SLA metric pre-computed
- Materialized as a table
- Mart-level tests + column descriptions for every field
- Conforms to all 6 rules in `.claude/rules/mart-naming.md`

### Tasks
- [ ] Confirm `dbt_project.yml` has `models.<project>.marts.+materialized: table` and `+schema: marts`
- [ ] Build `sales_activity_detail`:
  - Grain: one row per booked Calendly call
  - Joins: `fct_calls_booked` ⨝ `dim_contacts` ⨝ `dim_sdrs` (assigned + first-toucher) ⨝ `dim_calendar_dates` ⨝ `fct_sdr_outreach` (aggregated to "first outbound touch within X minutes" per booking)
  - Pre-computed columns:
    - `minutes_to_first_sdr_touch` — Calendly `booked_at` → first outbound GHL conversation `touched_at`
    - `is_within_5_min_sla` — boolean, the headline metric
    - `had_any_sdr_activity_within_1_hr` — boolean, the DQ diagnostic tile
    - `lead_source` (from Typeform funnel attribution)
    - `assigned_sdr_name`, `first_toucher_sdr_name`
- [ ] All field names use `is_` / `has_` prefix for booleans, `_at` suffix for timestamps (per style guide)
- [ ] `_marts__models.yml` with description on every column (this is what `dbt docs` will show stakeholders)
- [ ] Tests: unique + not_null on `booking_id`; `accepted_values` on `event_status`; `not_null` on `minutes_to_first_sdr_touch` for non-cancelled events
- [ ] `dbt build --select marts` — green

### Files created
```
dbt/models/marts/
  _marts__models.yml
  _marts__docs.md
  sales_activity_detail.sql
```

### Done when
- `sales_activity_detail` queryable in BQ; row count matches `fct_calls_booked`
- Headline metric computable in one BQ query: `select avg(case when is_within_5_min_sla then 1.0 else 0 end) from prod_marts.sales_activity_detail where date_trunc(booked_at, week) = ...`
- Every column has a description in YAML

### Open decisions (mine)
- **Whether `revenue_detail` ships in v1** — scope says no, but if Phase 3 lands on time, an extra ~half-day buys the finance-side stakeholder a freebie. Default: hold for v1.5 unless I'm ahead of schedule by end of Week 2
- **First-toucher attribution logic** — first outbound across any SDR, or first SDR who claimed the lead in GHL? Use first outbound across any SDR (objective, no claim-state dependency); if dashboard reveals weirdness, revisit
- **Whether to materialize incrementally** — v1 volume is tiny (<50/day); full-refresh table is fine. Don't optimize prematurely

---

## Phase 5 — Dashboard (Looker Studio)

**Goal:** five tiles wired to `sales_activity_detail` per scope §5, on one page, refreshed daily.

### Deliverables
- Looker Studio report connected to BQ via the native connector, sourcing only from `prod_marts.sales_activity_detail`
- Five tiles per scope:
  1. Headline `% within 5 min SLA` (last 7 days, WoW delta)
  2. Median confirmation time, last 7 days, by SDR
  3. Booked calls per day, stacked by lead source
  4. DQ diagnostic: `% with any SDR activity within 1 hour`
  5. SDR leaderboard: volume + % within SLA, last 30 days
- "Last refreshed" timestamp tile (sourced from `max(_ingested_at)` exposed via a one-line view)
- Dashboard URL shareable to client team via Google Workspace permissions

### Tasks
- [ ] Create new Looker Studio report; data source = BQ, point at `prod_marts.sales_activity_detail`
- [ ] Build tiles in scope-doc order; use Looker Studio's built-in scorecard / time-series / table chart types
- [ ] Add a clear text header at top: *"All metrics computed from logged GHL activity. Off-platform calls are not counted."* (per scope §5 transparency framing)
- [ ] Add one filter control: SDR dropdown (multi-select)
- [ ] Refresh cadence: data freshness set to 1-hour cache (BQ source data updates daily, so cache is generous)
- [ ] Share with client stakeholder emails as Viewer

### Files created
- No repo files — Looker Studio report lives in Google
- Add `dashboards/README.md` with the report URL + a screenshot for repo-side traceability

### Done when
- Five tiles render with non-null numbers
- Numbers reconcile with a hand-rolled BQ query for the same window (within rounding)
- Client stakeholders can open the link and see the dashboard

### Open decisions (mine)
- **Looker Studio vs. Evidence/Lightdash** — corpus mentions Tableau/Looker/Evidence. Looker Studio wins on (a) free, (b) zero hosting, (c) Google-native auth that the client likely already has. Locking in Looker Studio
- **Shared service account for the BQ connection vs. user-by-user OAuth** — service account is more reliable; trades off per-user audit. Go service account in v1
- **Embed the dashboard in a Notion page or just share the link?** Default to bare link; add embed only if requested

---

## Phase 6 — Tests, docs, CI/CD

**Goal:** automated trust + the deployment plumbing that prevents me (or future me) from breaking prod.

> "Never commit directly to main. Always branch." + "CI provides an automated double-check because humans make mistakes." — *Data Ops notebook*

### Deliverables
- Comprehensive `.yml` test coverage (already started in earlier phases — formalize here)
- `dbt docs` site generated and hosted (GH Pages or GCS bucket)
- `.github/workflows/ci.yml` running `dbt build` against `ci` schema on every PR
- `.github/workflows/deploy.yml` running `dbt build` against prod on merge to `main`
- `generate_schema_name` macro routing prod into `prod_staging` / `prod_warehouse` / `prod_marts`

### Tasks

**Tests (formalize):**
- [ ] Audit every `_*__models.yml`: unique + not_null on every PK/SK
- [ ] Add `relationships` tests on every FK in fact tables
- [ ] Add `accepted_values` on enums (e.g., `event_status`, `channel`)
- [ ] Add a singular test in `tests/`: `assert_no_negative_minutes_to_touch.sql` (sanity check that timestamps haven't gone backwards)
- [ ] `dbt build` — fully green

**Docs:**
- [ ] `dbt docs generate`
- [ ] Decide host: GH Pages (simpler) vs. GCS bucket (more native). Default to GH Pages
- [ ] Add `.github/workflows/docs.yml` — regenerates + publishes on every merge to `main`

**CI:**
- [ ] `.github/workflows/ci.yml`:
  - Trigger: `on: pull_request: branches: [main]`
  - Steps: checkout → setup Python → `pip install -r requirements.txt` → `dbt deps` → `dbt build --target ci` (uses `ci` schema)
  - Use Slim CI (`state:modified+`) once a baseline manifest exists in `main`
  - Required check on the branch protection rule

**Deploy:**
- [ ] `generate_schema_name.sql` macro: **prod** → use `custom_schema_name` as-is (yields `staging`, `warehouse`, `marts` inside the prod *project*); **dev / ci** → ignore custom schema, route everything to `target.schema` (`dev_<user>` or `ci`) so all layers consolidate in one dev/ci schema per the corpus pattern (source: *"DBT Project Environment Setup"*, Data Ops notebook)
- [ ] `.github/workflows/deploy.yml`:
  - Trigger: `on: push: branches: [main]`
  - Steps: checkout → install → `dbt build --target prod` → on success, regenerate docs
  - Add a hook in `.claude/settings.json` (already planned) that blocks `--target prod` from a local Claude session — prod runs only via this workflow

### Files created
```
.github/workflows/
  ci.yml
  deploy.yml
  docs.yml
dbt/macros/
  generate_schema_name.sql        (already stubbed in Phase 0; finalize here)
dbt/tests/
  assert_no_negative_minutes_to_touch.sql
.claude/settings.json              (add prod-target-block hook)
```

### Done when
- A throwaway PR with a deliberate broken model fails CI and is blocked from merging
- Merging a passing PR triggers `deploy.yml`, which builds prod and republishes docs
- `dbt docs serve` (locally) or the published URL shows every mart column with a description

### Open decisions (mine)
- **GH Pages vs. GCS for docs** — GH Pages for v1 (one less moving piece); GCS later if I need IP allowlisting
- **Slim CI on day one or after a baseline exists?** After. First few PRs run full builds; once `main` has a manifest artifact, switch to `state:modified+`
- **Whether to require a `dbt source freshness` check in CI** — yes for Calendly + GHL (the SLA-critical sources); warn-only for the others to avoid CI noise from upstream Fivetran lag

---

## Phase 7 — Handoff & launch

**Goal:** the client can use the dashboard without me in the room, and the next session of Claude (or future-me) can pick this project up cold from the worklog + docs.

> "Maintaining a `WORKLOG.md` is how future developers (or AI sessions) understand 'the present moment.'" — *Data Ops notebook*

### Deliverables
- A ≤10-minute Loom walkthrough showing leadership how to read the five tiles + the DQ diagnostic framing
- A short written README for the client at `dashboards/README.md`: dashboard URL, data freshness expectations, who to ping when numbers look wrong
- On-call setup: Slack/email alert wired to GitHub Actions failures on `ingest.yml` and `deploy.yml`
- WORKLOG.md updated with launch entry
- A "v1.5 backlog" file (`v1_5_backlog.md`) capturing everything I deferred — Fathom ingestion, secondary/tertiary SLAs, `revenue_detail` mart, Slack claim-time

### Tasks
- [ ] Record Loom (script: business context → headline metric → why DQ diagnostic exists → leaderboard → how to filter)
- [ ] Write `dashboards/README.md`
- [ ] Add GitHub Actions failure notifications: simplest path is the built-in email-on-failure to repo admins; upgrade to Slack webhook if the client already has one
- [ ] Run a final end-to-end smoke test: trigger `ingest.yml` manually, watch it land, watch the daily refresh of the dashboard 24h later
- [ ] Append WORKLOG entry: "v1 shipped to client"
- [ ] Create `v1_5_backlog.md` from scope §8 + open-decision residue from this plan

### Done when
- Client has watched the Loom and confirmed the dashboard answers the headline question
- One full daily cycle (ingest → build → dashboard refresh) has run end-to-end with no manual touch
- A failure injected into `ingest.yml` (e.g., bad credential) triggers a notification within one cron cycle

### Open decisions (mine)
- **Loom vs. live walkthrough** — Loom first (async, rewatchable, doubles as onboarding for future hires on the client side); live Q&A 24h after
- **On-call rotation** — solo. Set expectation in client README that response time is "next business day" for v1; tighten in v1.5 if needed
- **When to start client #2 conversations** — once Phase 7 is done and one full week of green daily runs is in the bag. Don't pitch the template until it's proven on engagement #1

---

## Cross-phase: open decisions I still own

Distinct from Week-0 client questions (those live in scope doc §7). These are mine:

- [ ] **Single Fivetran account or per-client account** — going single account in v1 to keep the free-tier MAR ceiling visible in one place; revisit if I onboard client #2 before launch
- [ ] **`dbt-utils` only, or also `dbt_expectations`?** Defer `dbt_expectations` until a test pattern emerges that the built-in tests can't express
- [ ] **Whether to add a pre-commit hook for `sqlfluff`** — yes, but only after Phase 4 is done so I'm not fighting the linter while iterating on model shapes
- [ ] **Cron time** — 06:00 UTC = 02:00 ET (overnight in client timezone, so dashboard is fresh by morning). Confirmed
- [ ] **Where to keep the Looker Studio report owner account** — needs to be a long-lived account, not my personal Google. Create a project-specific Workspace user before launch

---

## What's *not* in this plan (intentionally)

- Anything from scope §8 "Out of scope for v1" — Fathom, Slack, real-time alerts, LTV cohorting, AE metrics
- Reverse-ETL back into Slack/GHL — v2
- Multi-tenant schema split (`marts_sdr` / `marts_leadership` / `marts_finance`) — single `marts` schema in v1, split when permissions force it (per `.claude/rules/mart-naming.md` Rule 5)
- Anything that requires a Week-0 client decision I don't yet have (secondary/tertiary SLA thresholds, GHL tag mappings) — those gate Phase 4 / Phase 5 *content* but not Phase 0–3 *infrastructure*. Build forward; backfill the threshold logic when the client confirms

---

## Sequencing notes

- **Phase 0 → 1 must finish in Week 1.** GHL surprises are the biggest schedule risk; ingestion must be running by end of Week 1 so any API weirdness has the full Week 2 to be absorbed
- **Phases 2 → 4 are Week 2.** Staging → warehouse → marts is one continuous flow; resist the urge to build all dims before any facts — vertical slices through the headline metric first (Calendly → GHL → `sales_activity_detail`), then backfill width
- **Phase 5 starts as soon as `sales_activity_detail` has a single non-null row** — wiring Looker Studio in parallel with finalizing the mart speeds the feedback loop
- **Phase 6 (CI/CD) can be partly built in Week 1** — at minimum, the empty `ci.yml` should exist and run on every PR from day one, even if it just does `dbt parse`. Catch syntax errors free
- **Phase 7 is Week 3** — the buffer week from scope §7
