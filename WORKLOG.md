# Worklog

Rolling log of what's been done on this project. Newest entries at the top. Tail gets injected into Claude Code context at every session start via the `SessionStart` hook in `.claude/settings.json`.

**Conventions:**
- One entry per meaningful work session or deliverable
- Entries start with a dated H2 heading: `## YYYY-MM-DD — <one-line summary>`
- Three sub-sections per entry: `**What happened**`, `**Decisions**`, `**Open threads**`
- Keep bullets tight — this is a log, not a narrative
- Do not paste code, diffs, or long prose — link to files/commits instead

---

## 2026-04-19 — Phase 2: `stg_ghl__conversations` staging view (Speed-to-Lead numerator source)

**What happened**
- Shipped `stg_ghl__conversations.sql` on branch `phase-2/stg-ghl-conversations` → PR [#7](https://github.com/Davv5/dee-data-ops/pull/7). 1:1 view on `raw_ghl.conversations`, same pattern as `stg_ghl__users` (source → deduped → parsed → final, `qualify row_number()` latest-wins dedupe, `JSON_VALUE` extraction). 20 typed columns including the metric-critical `last_manual_message_at`, `last_outbound_action`, `last_message_direction`, `last_message_type`, `contact_id`, `assigned_user_id`. Epoch-millis → `TIMESTAMP` cast in staging so downstream layers never handle raw millis
- Tests: `unique` + `not_null` on `conversation_id`, `not_null` on `contact_id`. `dbt build` green (PASS=4)
- Row parity 15527 = 15527 distinct. Metric-relevant distributions confirmed: 5525 CALL/SMS, 4962 outbound, 4329 manual-action
- Intentionally omitted `lastMessageBody` (free-text, not needed for metric, privacy-first) and nested arrays (`followers`, `scoring`)

**Decisions**
- **Flagged `assigned_user_id` sparsity inline on the model.** Only 176/15527 rows (~1%) carry `assignedTo` on the conversation object. *Why:* the locked metric assumes SDR identity joins `conversations.assignedTo → users.id`, but at this population rate warehouse-layer attribution may need the GHL `/conversations/{id}/messages` endpoint (not yet ingested). Documented in the model description as a Phase 3 follow-up, not a blocker for this PR
- **Epoch-millis conversion happens in staging, not downstream.** *Why:* raw is JSON-string with millis; converting at the boundary (staging) means dimension/fact/mart layers deal only in native `TIMESTAMP`s — matches the corpus rule that staging is the "clean boundary"
- **No message body in the flattened view.** *Why:* the Speed-to-Lead metric doesn't need it, and keeping free-text PII contained to `raw_ghl` limits downstream exposure. Trivially addable later if a use case emerges

**Open threads**
- Parallel sessions shipped `stg_ghl__opportunities` (branch `phase-2/stg-ghl-opportunities`, commit `699f426`) and `stg_ghl__contacts` (branch `phase-2/stg-ghl-contacts`, commit `54bd853`) — PRs pending
- **Shared-working-tree hazard surfaced:** three parallel Claude sessions on one filesystem collided on branch switches + untracked files + concurrent yml edits. Recovered cleanly (nothing lost) but next time parallel sessions are run, use `git worktree add` per branch to get separate on-disk working dirs
- `assigned_user_id` attribution gap — resolve in Phase 3 by either (a) ingesting the `/messages` endpoint for message-level SDR attribution, or (b) confirming with David whether the warehouse can back-attribute from opportunity ownership

---

## 2026-04-19 — Phase 2 kickoff: first GHL staging view (`stg_ghl__users`) + sources declaration

**What happened**
- Queried the corpus to validate Phase 0–1 trajectory before entering Phase 2. Corpus confirmed JSON-payload raw landing is endorsed (store semi-structured blobs, flatten in staging), vertical slice through headline metric is the prescribed sequencing, and "no joins in staging" is absolute
- Wrote `dbt/models/staging/ghl/_ghl__sources.yml` (via subagent, parallel) — declares `raw_ghl` source with all 4 tables (users/opportunities/conversations/contacts), `loaded_at_field: _ingested_at`, freshness (36h warn / 48h error) on the two SLA-critical tables (conversations + opportunities). `users` + `contacts` intentionally skip freshness
- Wrote `stg_ghl__users.sql` — 1:1 view on `raw_ghl.users`, CTE structure (source → deduped → parsed → final), `qualify row_number() over (partition by id order by _ingested_at desc) = 1` dedupe, `JSON_VALUE(payload, '$.field')` extraction for 9 fields including `is_deleted` bool cast
- Wrote `_ghl__models.yml` — column descriptions + `unique`/`not_null` on `user_id`, `not_null` on `email`
- `dbt build --select stg_ghl__users` → PASS=4 (1 view + 3 tests). Row count 16 = 16 distinct, matches `raw_ghl.users` exactly

**Decisions**
- **Corpus rule enforced: no seed join in staging.** Original vertical-slice plan had `stg_ghl__users` joining the `ghl_sdr_roster` seed for role attribution; corpus is absolute on "no joins here." *Why:* staging must remain 1:1 with source for modularity + DRY. Seed join moves to `dim_sdrs` in Phase 3
- **Materialization lives in `dbt_project.yml` only** (no per-model `{{ config(...) }}` block). *Why:* staging = view is set globally; duplicating per-model is anti-DRY
- **Freshness on conversations + opportunities only.** *Why:* those two drive the headline metric, so stale data = business-visible failure. Skipping freshness on low-churn or non-SLA tables prevents alert noise from training the team to ignore warnings
- **`loaded_at_field` + `freshness` wrapped in `config:` blocks** — dbt 1.11 moved these schema properties from top-level to nested; fixed the resulting deprecations in-flight

**Open threads**
- Remaining 3 staging models in the GHL vertical slice (`stg_ghl__opportunities`, `stg_ghl__conversations`, `stg_ghl__contacts`) — same pattern, ship as separate PRs
- SDR roster seed (on branch `phase-2/prep/sdr-roster-seed`, 1 commit ahead of main) still un-merged — independent of staging work, merges on its own cadence
- Empty warehouse/marts config paths emit dbt warnings — expected; resolves as Phase 3/4 models land

---

## 2026-04-19 — Phase 1: GHL v2 extractor live end-to-end (four endpoints landed)

**What happened**
- Implemented real GHL v2 / LeadConnector fetchers in `ingestion/ghl/extract.py` — `contacts`, `conversations`, `opportunities`, `users` — and merged as PR #2 (branch `phase-1/ghl-implementation`, commits `1466f5f` → `9332103`)
- Added `GHL_LOCATION_ID` as a required env var / GH Actions secret (v2 PITs are location-scoped); `.env.example` + `ingest.yml` env block both document it
- Switched raw-landing schema for both GHL and Fanbasis extractors to a fixed three-column shape: `id STRING, _ingested_at TIMESTAMP REQUIRED, payload STRING REQUIRED`. `payload` holds `json.dumps(source_row)`; staging parses with `JSON_VALUE`/`PARSE_JSON`
- CI run `24640028389` on branch — both matrix legs green. Row counts in `raw_ghl`: `contacts=31784`, `conversations=15527`, `opportunities=25972`, `users=16`. `_sync_state` has a row per endpoint
- Spent three CI iterations debugging: (1) `401` because the `GHL_API_KEY` secret was stale — re-set with the correct PIT and it resolved; (2) `BadRequest: Repeated field must be imported as a JSON array` from autodetect seeing `contacts.customFields.value` as both array and scalar; (3) `422` on conversations because `lastMessageType` as a repeated query param was rejected — dropped the filter and moved it to staging

**Decisions**
- **Single `payload` JSON-string column instead of flat autodetected schema.** *Why:* GHL has structurally inconsistent nested fields (`customFields.value` is the canonical offender) and autodetect breaks on the second row. JSON-string landing is the standard raw-zone pattern, makes the extractor immune to any upstream schema drift, and defers all typing to staging where we own it. Trade-off: Phase 2 will carry more `JSON_VALUE` extraction than if we had flat columns — acceptable because we get to pick the exact typed shape per staging model
- **`conversations` is the only incremental endpoint for now.** GHL's other GET endpoints don't expose a reliable since-filter; accept full pulls + staging dedupe on `id` + latest `_ingested_at`. Revisit if daily volume becomes a cost concern
- **Per-endpoint `Version` header pinned in a dict** — `conversations` uses `2021-04-15`, the rest use `2021-07-28` per OpenAPI spec. Documented in the README

**Open threads**
- GHL PIT `pit-578d0c36-…` is now in the conversation transcript — rotate in GHL → Settings → Private Integrations after close of session, update `GHL_API_KEY` secret, re-run workflow to confirm
- Fanbasis `fetch_endpoint` still stub — unblocked on Week-0 docs/credentials
- Fivetran initial syncs (Typeform, Calendly, Stripe) should have landed during the above — verify `raw_typeform` / `raw_calendly` / `raw_stripe` datasets exist and have rows before closing Phase 1

---

## 2026-04-19 — Phase 1 kickoff: GHL + Fanbasis extractor skeletons + ingest workflow

**What happened**
- Scaffolded `ingestion/ghl/` (extract.py + requirements.txt + README.md) — BQ client, state-table DDL, cursor read/write, per-endpoint loop all wired; `fetch_endpoint` stubbed until Week-0 GHL credentials arrive. Endpoints: `contacts`, `conversations`, `opportunities`, `users`
- Mirrored the same shape in `ingestion/fanbasis/` for `customers`, `subscriptions`, `payments`; CSV-export fallback documented per scope Risk #5
- Created `raw_ghl` + `raw_fanbasis` BQ datasets (location US) in `dee-data-ops`; `_sync_state` tables auto-created on first dry-run via `ensure_state_table` DDL
- Smoke-tested both extractors with `python ingestion/<source>/extract.py --dry-run` — BQ auth, `_sync_state` creation, cursor read, endpoint loop all green
- Scaffolded `.github/workflows/ingest.yml` — cron `0 6 * * *` + `workflow_dispatch`, matrix over `[ghl, fanbasis]`, `google-github-actions/auth@v2` step, cached pip. Runs as a plumbing smoke test until Week-0 secrets (`GCP_SA_KEY`, `GHL_API_KEY`, `FANBASIS_API_KEY`) are set in GitHub
- Queried corpus before scaffolding — confirmed one-dataset-per-source + secrets-via-env patterns; noted explicitly in each README that cursor/append/`_ingested_at` choices are reasoned defaults, not corpus-prescribed

**Decisions**
- **Append-only + `_ingested_at` + dedupe-in-staging** over upsert-at-ingest. *Why:* keeps the extractor small and idempotent; corpus is silent on this, dbt style guide owns the dedupe pattern downstream (Phase 2 staging with `qualify row_number()`)
- **Wall-clock watermark in `raw_<source>._sync_state`** (not a per-record `updated_at` high-water mark) for v1. *Why:* GHL API behavior around `updatedSince` is unknown until Week 0; wall-clock is safe and overlap is deduped in staging. Revisit once API is confirmed
- **Matrix-over-source in a single job** (not two separate jobs). *Why:* one workflow file, DRY step definitions, parallel by default, clean rerun UX from the Actions tab
- **`GCP_SA_KEY` as raw JSON paste** (not base64) per `google-github-actions/auth@v2` convention. Flagged in workflow comments — easy to get wrong on setup
- **Skeleton ships before credentials arrive.** *Why:* plumbing smoke test is free and surfaces any auth/env issues with `GCP_SA_KEY` before GHL is in the critical path. Also satisfies v1 plan's "workflow exists from day one" requirement

**Open threads**
- Week-0 asks unchanged: GHL + Fanbasis API credentials, Fivetran account setup for Typeform/Calendly/Stripe, SLA thresholds, GHL tag mappings
- `fetch_endpoint` implementations for both extractors — fill in once credentials land; README TODO(week-0) markers document the handoff points
- GitHub Actions secrets not yet set (`GCP_SA_KEY` / `GHL_API_KEY` / `FANBASIS_API_KEY`) — workflow will fail on the auth step until they are; safe to merge because only triggered on cron + manual dispatch
- Prod-project `raw_*` datasets deferred to Phase 6 alongside the prod SA

---

## 2026-04-19 — Phase 0 mostly landed + two-project corpus correction + personal overlay layering

**What happened**
- Clarified engagement layering: David (data-ops IC) → **Precision Scaling** (employer/agency) → **D-DEE** (PS's client)
- Created `CLAUDE.local.md` (gitignored) as the engagement overlay on top of the portable `CLAUDE.md` template; updated project memory (`project_speedtolead_client.md`) to name PS + D-DEE + correct the stale HubSpot/SF reference to the locked GHL stack
- Created `.gitignore` at repo root (`.env`, `.venv/`, `target/`, `dbt_packages/`, `CLAUDE.local.md`, keyfiles)
- **Phase 0 — GCP side:**
  - Created `dee-data-ops` project (dev + ci)
  - Created `dee-data-ops-prod` project (prod, isolated) — *corpus-driven correction* mid-Phase-0 after the NotebookLM query surfaced that prod should be isolated by **project/database**, not `prod_*` schema prefix. Deleted the incorrectly-named `prod_staging`/`prod_warehouse`/`prod_marts` datasets from the dev project; recreated them with clean names (`staging`, `warehouse`, `marts`) in the new prod project. Both projects linked to billing account `0114FD-8EC797-A11084`
  - Created `dbt-dev` SA with BigQuery Data Editor + Job User on `dee-data-ops`; keyfile at `~/.config/gcloud/dee-data-ops/dbt-dev.json` (mode 600)
- **Phase 0 — local side:**
  - Upgraded from Python 3.9.6 (past Google-auth EOL) to Python 3.11.15 via Homebrew
  - First pip install pulled in `dbt-core 1.11.0-b3` (beta); rebuilt venv with pinned stable `dbt-core==1.11.8` + `dbt-bigquery==1.11.1`
  - `dbt init` scaffolded the project; renamed folder to `dbt/` per template convention
  - Deleted default `models/example/`; created 3-layer skeleton (`models/{staging,warehouse/dimensions,warehouse/facts,marts}`)
  - Wrote `dbt/dbt_project.yml` with layer configs (staging → view/`+schema: staging`; warehouse → table/`+schema: warehouse`; marts → table/`+schema: marts`)
  - Wrote `dbt/profiles.yml` (env-var driven, dev/ci/prod targets)
  - Wrote `dbt/packages.yml` (dbt-utils 1.3+), ran `dbt deps` → installed
  - Wrote `dbt/macros/generate_schema_name.sql` — prod uses custom schemas as-is; dev/ci consolidate into `target.schema`. Cited `"DBT Project Environment Setup"` from the Data Ops notebook
  - Wrote `.env.example` + `.env` (absolute path for `DBT_PROFILES_DIR` — relative path broke after `cd dbt`; space in repo path required quoting)
  - `dbt debug` → **All checks passed!**
- Patched `v1_build_plan.md` Phase 0 + Phase 6 to reflect the two-project pattern (was specifying `prod_*` prefixes — corpus-wrong)

**Decisions**
- **Two-project GCP layout** (`dee-data-ops` + `dee-data-ops-prod`) over single-project + `prod_*` schema prefixes. *Why:* corpus says isolate prod at the database/project level; BigQuery project = Snowflake database in this pattern. Cost of fixing later (data migration + ref rewrites) would've been painful; cost now was ~5 min of `gcloud` commands. Source: *"DBT Project Environment Setup"*, Data Ops notebook.
- **Python 3.11 over 3.9.** *Why:* Google libraries have dropped 3.9 support; every dbt run was emitting a wall of `FutureWarning`. 3.11 also matches what `CLAUDE.md` recommends and what the corpus assumes.
- **Pinned stable dbt versions** (`dbt-core==1.11.8`, `dbt-bigquery==1.11.1`). *Why:* pip's resolver picked up a `1.11.0-b3` beta on first install — explicit pins prevent that from recurring.
- **Repo-local profiles.yml** (in `dbt/`) with absolute `DBT_PROFILES_DIR` in `.env` rather than `~/.dbt/profiles.yml`. *Why:* keeps all config in-repo and reviewable; absolute path because repo lives in a space-containing directory (`/Users/david/Documents/data ops/`).
- **Personal overlay file is `CLAUDE.local.md` (gitignored)**, not user-level `~/.claude/CLAUDE.md`. *Why:* engagement context shouldn't leak into unrelated projects; template stays clean for client #2.

**Open threads**
- **GitHub remote — resolved**: switched `gh` auth from `heidyforero1` → `Davv5` (personal); created private repo `Davv5/dee-data-ops`; initial commit `85564e9` pushed to `main` (56 files). Old account still in keyring, inactive.
- **Branch protection on `main` — deferred to Phase 6**: GitHub Free doesn't support branch protection on private personal repos (both classic API + newer rulesets returned 403). Revisit with Pro upgrade or a free GitHub org when CI workflow lands.
- Prod service account (`dbt-prod`) not yet created — deferred to Phase 6 per v1 plan when CI/deploy workflows land
- `.claude/settings.json` hook to block local `dbt --target prod` runs — still deferred to Phase 6
- Pre-commit `sqlfluff` hook — deferred to after Phase 4 (per v1 plan cross-phase decisions)

---

**What happened**
- Wrote `v1_build_plan.md` (repo root) — internal execution plan, companion to `client_v1_scope_speed_to_lead.md`
- 8 phases mirroring the canonical pipeline: Prereqs → Ingestion → Staging → Warehouse → Marts → Dashboard → Tests/Docs/CI/CD → Handoff
- Each phase carries: deliverables, ordered task checklist, files/workflows created, "done when" criterion, and a section for open decisions still owned by me (distinct from Week-0 client questions)
- Grounded the phase sequence with a NotebookLM corpus query (free) — pulled the right-to-left planning principle, "raw landing zone, no transforms in ingestion," staging-as-views/no-joins, surrogate-key pattern via `dbt-utils` for dims, and the marts naming rules. Cited inline.
- Plan explicitly calls out: Phase 0→1 must close in Week 1 (GHL API risk); vertical slice through the headline metric first (Calendly → GHL → `sales_activity_detail`) before going wide; CI workflow exists from day one even if it only does `dbt parse`

**Decisions**
- Sequencing: ingestion starts Day 1 of Week 1 to surface GHL schema surprises with maximum slack — no "build models first, plug in real data later"
- Use `dbt-utils.generate_surrogate_key` for all dim SKs (corpus-grounded); keep natural keys alongside for traceability
- Looker Studio (not Evidence/Lightdash) for v1 — free, zero hosting, Google-native auth the client likely already has
- GH Pages for `dbt docs` hosting in v1 — one less moving piece than GCS; revisit if IP allowlisting is needed
- Slim CI deferred until a baseline `manifest.json` exists in `main` — full builds for the first few PRs
- `dim_aes` built in v1 even though no v1 tile uses it — nearly free now, painful to backfill
- v1.5 `revenue_detail` mart held back unless I'm visibly ahead at end of Week 2

**Open threads**
- Phase 6 hook to block local `--target prod` runs needs to be added to `.claude/settings.json` (not yet done)
- `v1_5_backlog.md` to be created at end of Phase 7 (placeholder; not yet on disk)
- `dashboards/README.md` deferred until Phase 7 (URL doesn't exist yet)
- All Week-0 client questions (SLA thresholds, GHL tag mappings, end-to-end access) still owed by client — gate Phase 4/5 *content* but not Phase 0–3 *infrastructure*
- Decide pre-commit `sqlfluff` adoption after Phase 4

---

## 2026-04-19 — Client v1 scope locked + mart-naming rule + corpus double-check norm

**What happened**
- Completed multi-round client discovery interview for the first engagement (high-ticket coaching, book-a-call funnel, previously worked at this client as SDR→closer)
- Drafted `client_v1_scope_speed_to_lead.md` — 11-section scope for the Speed-to-Lead Dashboard (headline metric: % of booked calls confirmed within 5 min, logged)
- Locked ingestion architecture: Typeform / Calendly / Stripe via Fivetran free tier; GHL / Fanbasis / Fathom via custom Python on GitHub Actions cron
- Ran corpus double-check on dashboard-per-audience question → three specifics became rules
- Created `.claude/rules/mart-naming.md` (6 rules incl. drop `fct_`/`dim_` in marts, fewer-wider marts, schema-per-audience) and synced to NotebookLM
- Added "Always double-check before finalizing" section to `.claude/rules/using-the-notebook.md` using the mart-naming scenario as the worked example; synced to notebook
- Updated `CLAUDE.md` — annotated structure tree with (exists)/(planned) markers, pointed at `dbt_style_guide.md`, added "Current State" note that template is pre-scaffolding
- Set up this worklog + `SessionStart` hook so the "present moment" is always in context

**Decisions**
- Primary SLA (Calendly booked → SDR confirmation within 5 min) ships in v1. Secondary (no-show rescue) and tertiary (unbooked Typeform chase) deferred to v1.5 — architected for, not exposed
- Marts layer uses business-friendly names (e.g., `sales_activity_detail`), not `fct_sales_activity`
- Start with a single `marts` schema. Split into `marts_sdr` / `marts_leadership` / `marts_finance` only when audience permissions require it
- One wide mart per domain > one mart per dashboard
- GitHub Actions + Python (not Airbyte/Hevo) for GHL + Fanbasis ingestion — $0 recurring, template-friendly for client #2
- Activity-logging gap stays in the dashboard as a management-visible DQ diagnostic tile (forcing function for GHL dialer adoption)

**Open threads**
- Week 0 client call: confirm layered-SLA thresholds (secondary + tertiary), GHL tag names for junk/DQ, end-to-end access verification
- Internal technical build plan (staging models, Python extractor skeletons, GH Actions workflow skeletons) — not yet drafted
- Client-facing kickoff deck — not yet drafted
- GHL API schema surprises: start ingestion Day 1 of Week 1 to surface unknowns early
- Fanbasis API reliability unknown — have CSV-export fallback in mind
