# Worklog

Rolling log of what's been done on this project. Newest entries at the top. Tail gets injected into Claude Code context at every session start via the `SessionStart` hook in `.claude/settings.json`.

**Conventions:**
- One entry per meaningful work session or deliverable
- Entries start with a dated H2 heading: `## YYYY-MM-DD — <one-line summary>`
- Three sub-sections per entry: `**What happened**`, `**Decisions**`, `**Open threads**`
- Keep bullets tight — this is a log, not a narrative
- Do not paste code, diffs, or long prose — link to files/commits instead

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
- **GitHub remote repo — blocked**: `gh` CLI active account is `heidyforero1`; David needs to confirm whether to switch to his own account or to a PS org before `gh repo create`. No commit has been made locally yet either (no `git init`).
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
