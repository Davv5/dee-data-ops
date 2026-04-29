# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-28 late-night (post-reconciliation-sweep: 7 stale plan/scope/rule artifacts retired, new `pivot-discipline.md` rule added)._

## Where we are

- **Step 4 of the bq-ingest consolidation is done.** Cloud Run service `bq-ingest` now serves revision `bq-ingest-00085-qar` at 100% traffic, deployed via `gcloud run deploy bq-ingest --source services/bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d --no-traffic --tag step4 --memory=1024Mi`. Parity check passed byte-equal between the `step4`-tagged URL and the prior live revision (`bq-ingest-00076-wtl`, retained at 0% as a rollback target): identical 22 routes, identical `/` health response. **dee-data-ops is now the production deploy origin** — `gtm-lead-warehouse` is no longer load-bearing.
- **Steps 1, 2, and 3** all on main (PRs #100, #102, #104). Path-scoped rule `.claude/rules/bq-ingest.md` ported the two facts from dropped per-source CLAUDE.md files; ingestion contract via `.claude/rules/ingest.md` covers `services/bq-ingest/**`; operational-health.md declares `services/bq-ingest/` canonical.
- **`bq-ingest` requires authenticated invocation.** Auth posture from PR #100 holds: `allUsers → roles/run.invoker` removed; URLs return 403 unauth, 200 with `gcloud auth print-identity-token`.
- **Latent bug confirmed live in production (NOT introduced by Step 4):** the daily `calendly-invitee-drain` Cloud Run Job has been failing 5 consecutive days (2026-04-24 through 2026-04-28, exit code 1). The dispatch in `ops/runner/tasks.py:71` points at a module that doesn't exist (`sources.calendly.calendly_invitee_drain:main`). The new revision's image inherits this bug — fix belongs to a follow-up cleanup PR, not Step 4.
- **Audit's pre-existing 7 deferred follow-ups** still tracked in §"Deferred follow-ups" of `docs/discovery/bq-ingest-dependency-audit.md`.
- **GHL transition snapshots remain LIVE.** `Core.fct_pipeline_stage_snapshots` compounds daily at 07:00 UTC. First usable transition signal in ~6 days.
- **`bq-ingest` service** now on revision `bq-ingest-00085-qar`, Python 3.13, 1024Mi, **deployed from `services/bq-ingest/` in this repo**. Old `bq-ingest-00076-wtl` retained at 0% traffic as rollback target.
- **Operational-health rule lives on main.** `.claude/rules/operational-health.md` distinguishes pausable vs non-pausable work; loud freshness gate replaces `|| true` in `dbt-nightly.yml`.
- **Phase B layer-build still on main** (PRs #84/#86/#88/#90/#92): Fanbasis staging, identity-contact-payment bridge, payments/refunds rename, revenue_detail net-of-refunds. Local-CI tooling (PRs #94/#95) remains the dev-loop bypass when GH Actions degrades.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **bq-ingest consolidation:** `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md` — Steps 1, 2, 3, 4 shipped. **Next: Step 5 (optional Cloud Build trigger watching `services/bq-ingest/**`).** Step 6 (archive `heidyforero1/gtm-lead-warehouse` + delete stale local clones at `~/Documents/{fanbasis-ingest,gtm,gtm-lead-warehouse}`) waits ~several days of clean operation from the new home before firing.
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-28 late-night (latest)** — Reconciliation sweep + `pivot-discipline.md` rule. Closed 7 stale plan-debt artifacts caused by un-walked pivots: retired `1-raw-landing/fanbasis/` skeleton (tombstone README pointing at `services/bq-ingest/sources/fanbasis/`); updated `ingest.md` v1 inventory; refreshed `CLAUDE.local.md` to reflect dabi as BI direction + Stripe-banned + Fanbasis live; added FROZEN banner to archived V1 scope; stripped Grok-in-loop from `2026-04-24-strategic-reset.md`; updated GTM source-port plan paused-banner. New rule `.claude/rules/pivot-discipline.md` requires same-session walk of superseded docs when a pivot memory is saved.
- **2026-04-28 night** — Step 4 deploy complete. New revision `bq-ingest-00085-qar` serves 100% traffic from `services/bq-ingest/`. Used `--no-traffic --tag step4` for the parity check (byte-equal /routes + /), then promoted with `update-traffic --to-revisions=...=100`. Old revision retained at 0% for rollback. Confirmed `calendly-invitee-drain` Job was already failing daily for 5 days pre-deploy (latent dispatch bug, not move-induced).
- **2026-04-28 night** — PR #104 merged (Step 3 pointer updates). Two parallel reviewers found 6 issues; 5 fixed, 2 deferred to a post-Step-4 cleanup window with rationale. New `.claude/rules/bq-ingest.md` ports the two facts from dropped per-source CLAUDE.md files.

## Open threads

- **Cleanup PR(s) post-Step-4** — bundle these mechanical fixes once the new revision has soaked for a session or two: (a) flip `app.py:42` brand string `fanbasis-ingest` → `bq-ingest` (deferred during Step 4 to keep parity check byte-equal); (b) fix `ops/runner/tasks.py:71` `backfill.calendly_invitee_drain` dispatch (currently points at a non-existent module — the Cloud Run Job has been failing daily for 5+ days); (c) restore or document `ops/env/triage/` (triage scripts exit non-zero on missing config); (d) RUNBOOK rewrite of stale Looker Studio core rule + prod-target paragraph; (e) the 6 CWD-fragile shell-script paths.
- **Step 5 (optional)** — Cloud Build trigger watching `services/bq-ingest/**` for auto-deploy on merge to main. The audit reframed this from "required" to "optional-but-recommended" because David is the sole operator; right rationale is deploy provenance / build reproducibility, not stale-clone defense.
- **Step 6 (after a few days of clean operation)** — archive `heidyforero1/gtm-lead-warehouse` and delete the three stale local clones at `~/Documents/{fanbasis-ingest,gtm,gtm-lead-warehouse}`.
- **bq-ingest pre-existing deferred follow-ups (per audit §"Deferred follow-ups"):** Cloud Run Jobs image rebuild path, `1-raw-landing/` consolidation, SQL resolution cleanup (6 broken-default modules; one — `mart_models.py`'s default `sql_file_path` — already documented in `.claude/rules/bq-ingest.md`), secret hygiene (pin all to versions, rename `Secret`), orphan SQL audit (5 spec-only files), services/ polyrepo precedent.
- **Snapshot architecture follow-ups:** (a) extend `Core.fct_ghl_opportunities` upstream to surface `assigned_to_user_id` (currently NULL-cast in the snapshot); (b) once `Core.fct_pipeline_stage_snapshots` has ≥2 daily partitions, swap GHL freshness signal from `MAX(_ingested_at)` to `MAX(snapshot_date)`.
- **Vestigial Cloud Run Jobs** (`ghl-incremental-v2`, `calendly-incremental-v2`, `ghl-backfill-v2`) — not invoked by active schedulers; produce false signals. Delete or label.
- **Pre-existing stale PRs in dee-data-ops:** #50 + #44 (Metabase, both predate dabi pivot). Close or evaluate.
- **Float64-for-money tech debt (Fanbasis)** — `stg_fanbasis__transactions` and `stg_fanbasis__refunds` cast amounts to `float64`; should be `numeric`. PR #92's parity test absorbs the drift via $0.01 tolerance for now.
- **`fct_calls_booked` SK gaps** — `assigned_user_sk` + `pipeline_stage_sk` are still hardcoded `cast(null as string)` at `2-dbt/models/warehouse/facts/fct_calls_booked.sql:70-71`. The `opportunities` + `pipeline_stages` CTEs are defined but unused; the join axis (`stg_calendly__event_invitees.invitee_email_norm`) DOES now exist (`contact_sk` resolves through it). Backlogged data-modeling work; matters when SDR/AE attribution slices ship.
- **GHL trusted-copy decision** — single named blocker for several Tier B / refresh marts.
- **GCP IAM hygiene (cosmetic, not blocking).** ADC via `dev_local` / `ci_local` is the working path; SA key for consolidated project still unprovisioned.
- **Fathom → GHL contact join key** — attendee email reliability. Affects future `fct_calls_held`.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` + `QUALITY_PARITY_FLOOR=0.6` ship provisional. Lock after first production queries.
- **Strategic-Reset-paused threads:** Typeform `form_id` upstream gap, GHL `messages`/`users`/`tasks` 0-row upstream, Fathom transcript landing, Stripe staleness. Re-evaluate per operational-health rule.
- **Week-0 client asks** — unchanged.

## Where to look

- **bq-ingest dependency audit (Step 1 output):** `docs/discovery/bq-ingest-dependency-audit.md` — load-bearing edges, Cloud Run service spec snapshot, pre-flight checklist for Step 2, deferred follow-ups
- **bq-ingest consolidation plan:** `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`
- **Operational-health rule:** `.claude/rules/operational-health.md` (worked-examples #1-4 cover today's audit)
- **bq-ingest source (canonical):** `services/bq-ingest/` in this repo. The `heidyforero1/gtm-lead-warehouse` repo is no longer the deploy origin. Local clones at `~/Documents/{fanbasis-ingest,gtm,gtm-lead-warehouse}` are stale; their deletion is Step 6.
- **bq-ingest production:** Cloud Run service `bq-ingest` in `project-41542e21-470f-4589-96d`, region `us-central1`, **revision `bq-ingest-00085-qar` (deployed 2026-04-28 night via Buildpack from `services/bq-ingest/`; no Procfile/app.yaml/.gcloudignore)**. URL: `https://bq-ingest-mjxxki4snq-uc.a.run.app`. Step-4 tagged URL `https://step4---bq-ingest-mjxxki4snq-uc.a.run.app` resolves to the same revision (kept for diagnostic continuity). **Requires authenticated invocation:** `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" /routes`.
- **Redeploy command (durable):** `gcloud run deploy bq-ingest --source services/bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d --memory=1024Mi`. Add `--no-traffic --tag <name>` for parity-check workflows.
- **PR #102 review thread (Step 3 done + Step 4 punch list):** https://github.com/Davv5/dee-data-ops/pull/102 — comments `4338500214` (round 1) and `4338545264` (round 2).
- **PR #104 review thread (Step 3 review-pass fixes + Step 4 deferrals):** https://github.com/Davv5/dee-data-ops/pull/104 — comment `4338668269`.
- **bq-ingest service rules:** `.claude/rules/bq-ingest.md` (path-scoped to `services/bq-ingest/**`).
- **Live snapshot table:** `project-41542e21-470f-4589-96d.Core.fct_pipeline_stage_snapshots`, partitioned by `snapshot_date`, clustered by `(pipeline_id, status)`.
- **Canonical roadmap:** `docs/discovery/gold-layer-roadmap.md`
- **Phase A → B ADR:** `docs/decisions/2026-04-27-phase-a-to-b-transition.md`
- **Mart architecture commitment:** `docs/discovery/coverage-matrix.md` + `.claude/rules/mart-naming.md` Rule 2
- **Phase B artifacts (on main):** `2-dbt/models/staging/fanbasis/`, `2-dbt/models/warehouse/{facts,bridges}/`, `2-dbt/models/marts/{lead_journey,revenue_detail,sales_activity_detail,speed_to_lead_detail}.sql`
- **Local dev loop:** `2-dbt/scripts/local-ci.sh` + `2-dbt/profiles.yml` (`dev_local` / `ci_local` ADC targets) + `2-dbt/README.md` "Local CI" section.
- **Data-engineer agent + LAW skills:** `~/.claude/agents/data-engineer.md`. Specialist seams via `altimate-{sql-review,data-parity,schema-migration,dbt-unit-tests}`. Discoverability rule: `.claude/rules/use-data-engineer-agent.md`.
- **Corpus engine v2:** `.claude/skills/ask-corpus/scripts/` + `SKILL.md`
- **Fast operating loop:** `docs/runbooks/operator-fast-loop.md`
- **Shared portable kit:** `/Users/david/Documents/agent-kit`
- **Codex parity:** `AGENTS.md` + `.agents/skills/{ask-corpus,skill-creator,worklog}/`
- **Sprint artifacts:** `docs/discovery/{source-inventory,source-shapes,staging-models,gap-analysis,insights-summary,business-area-map,coverage-matrix,gold-layer-roadmap}.md`
- **Memories (auto-loaded):** see `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `.claude/rules/*.md`
- **Routing rule for end-of-session:** `.claude/rules/worklog.md`
- **Pivot-discipline rule (new 2026-04-28):** `.claude/rules/pivot-discipline.md` — required walk of superseded docs when a strategic-pivot memory is saved
- **Full history:** `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-04-28 late-night (post-reconciliation-sweep + new `pivot-discipline.md` rule).
- WORKLOG: skipped for this session — the reconciliation PR description will carry the narrative (which 7 artifacts were retired, why, and how the new rule prevents recurrence). Per `.claude/rules/worklog.md` routing table: PR description captures shipped work; the rule file captures the convention; both replace a WORKLOG entry. Skip-reason recorded here.
- Earlier _meta entry (Step 4 deploy): the prior regen captured Step 4 production-deploy narrative — that entry's WORKLOG note was correctly appended at session end.
