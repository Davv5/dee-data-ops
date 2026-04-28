# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-28 evening (post-PR #97 operational-health rule, PR #98 bq-ingest consolidation plan, plus four-PR hotfix chain on `heidyforero1/gtm-lead-warehouse` that brought transition snapshots online)._

## Where we are

- **GHL transition snapshots are LIVE.** `Core.fct_pipeline_stage_snapshots` seeded today: **26,133 rows for 2026-04-28**, all four statuses captured (open / won / lost / abandoned). Daily 07:00 UTC cron compounds from here — first usable transition signal in ~7 days; stage-velocity at ~30. The earlier task framing (build `fct_opportunity_stage_transitions`) collapsed once the audit revealed the architecture *existed* in `bq-ingest` but had been silently failing.
- **`bq-ingest` service redeployed.** Revision `bq-ingest-00076-wtl`, Python 3.13 (3.11 dropped from current GCP universal builder), memory bumped 512Mi → 1024Mi (clears the `run_models()` OOMs that had been hitting Fathom + Calendly). New `/routes` endpoint exposes registered Flask routes — single-curl post-deploy verification that the running image matches expected source.
- **Operational-health rule lives on main.** `.claude/rules/operational-health.md` — distinguishes pausable extractor work (new builds, schema fills, vendor expansions) from non-pausable (operating deployed pipelines, snapshot integrity, memory limits, deploy-source freshness). Loud freshness gate replaces `|| true` in `dbt-nightly.yml`. Companion memory: `feedback_dont_dismiss_high_leverage_under_pause.md`.
- **Phase B layer-build still on main** (PRs #84/#86/#88/#90/#92): `stg_fanbasis__transactions`, `bridge_identity_contact_payment`, `fct_payments` rename, `fct_refunds` Fanbasis-only event grain, `revenue_detail` net-of-refunds extension. Local-CI tooling (PRs #94/#95) remains the dev-loop bypass when GH Actions degrades.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **bq-ingest consolidation:** `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md` — move service source from `heidyforero1/gtm-lead-warehouse` (which David doesn't actively push to — root cause of today's silent-deploy drift) into `dee-data-ops/services/bq-ingest/`. ~1.2 MB / 41 Python files. Plan estimates 2.5–3 hours of fresh-context work. **This is the next session.**
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-28 evening** — Brought GHL transition snapshots online via four-PR hotfix chain to `heidyforero1/gtm-lead-warehouse`: PR #1 (drop active-only WHERE filter + add `/routes`), #3 (pin Python 3.13 — 3.11 unavailable in current GCP universal builder, 3.14 broke protobuf), #4 (NULL-cast `assigned_to_user_id` since `Core.fct_ghl_opportunities` doesn't carry it — function had never end-to-end-run on the real schema). Production now on revision `00076-wtl`. Companion: `dee-data-ops` PR #97 lands operational-health rule + loud freshness gate.
- **2026-04-28 evening** — Operational-health rule articulates the principle that emerged from the audit: the Strategic Reset extractor pause was silently destroying data on operational pipelines (snapshot 404s, OOMs, vestigial Cloud Run Jobs masquerading as healthy). Pausable vs not-pausable distinction codified; freshness gate goes loud. Memory: `feedback_dont_dismiss_high_leverage_under_pause.md`.
- **2026-04-28 evening** — bq-ingest consolidation plan staged in dee-data-ops PR #98. Move target: `dee-data-ops/services/bq-ingest/`. Stale local clones (`~/Documents/{fanbasis-ingest,gtm,gtm-lead-warehouse}`) get deleted once the new-home deploy proves clean. After several days of operation: archive `heidyforero1/gtm-lead-warehouse`.

## Open threads

- **bq-ingest consolidation** (top of list) — execute the plan above. Until done, code changes to bq-ingest still go through the `gtm-lead-warehouse` PR flow.
- **Snapshot architecture follow-ups:** (a) extend `Core.fct_ghl_opportunities` upstream to surface `assigned_to_user_id` (currently NULL-cast in the snapshot); (b) once `Core.fct_pipeline_stage_snapshots` has ≥2 daily partitions, swap GHL freshness signal from `MAX(_ingested_at)` (misleading because of MERGE-by-id) to `MAX(snapshot_date)` to avoid first-nightly false alarm.
- **Vestigial Cloud Run Jobs** (`ghl-incremental-v2`, `calendly-incremental-v2`, `ghl-backfill-v2`, etc.) — not invoked by active schedulers; their last-run dates produce false signals in any health check that doesn't know they're vestigial. Delete or label.
- **Pre-existing stale PRs in dee-data-ops:** #50 (Metabase Track-E v1.3.1 polish, 2026-04-22) + #44 (Metabase startup curl hardening, 2026-04-22). Both predate the Metabase retirement / dabi pivot. Close or evaluate.
- **Float64-for-money tech debt (Fanbasis)** — `stg_fanbasis__transactions` and `stg_fanbasis__refunds` cast amounts to `float64`; should be `numeric`. PR #92's parity test absorbs the drift via $0.01 tolerance for now.
- **GHL trusted-copy decision** — single named blocker for several Tier B / refresh marts (legacy blob 1,314 vs Phase-2 101 conversation rows).
- **GCP IAM hygiene (cosmetic, not blocking).** ADC via `dev_local` / `ci_local` is the working path; SA key for consolidated project still unprovisioned.
- **Fathom → GHL contact join key** — attendee email reliability. Affects future `fct_calls_held`.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` + `QUALITY_PARITY_FLOOR=0.6` ship provisional. Lock after first production queries.
- **Strategic-Reset-paused threads:** Typeform `form_id` upstream gap, GHL `messages` / `users` / `tasks` 0-row upstream, Fathom transcript landing, Stripe staleness. Re-evaluate per the new operational-health rule (some may move from "paused" to "not-pausable").
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).

## Where to look

- **bq-ingest consolidation plan:** `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`
- **Operational-health rule:** `.claude/rules/operational-health.md` (worked-examples #1-4 cover today's audit)
- **bq-ingest source (today):** `heidyforero1/gtm-lead-warehouse` repo. Local working copy: `~/Documents/gtm-lead-warehouse` (current main; **other local clones at `fanbasis-ingest` and `gtm` are stale — do not deploy from them**)
- **bq-ingest production:** Cloud Run service `bq-ingest` in `project-41542e21-470f-4589-96d`, region `us-central1`, revision `bq-ingest-00076-wtl`. URL: `https://bq-ingest-mjxxki4snq-uc.a.run.app`. Verify routes: `curl /routes`. Seed snapshot ad-hoc: `curl -X POST /snapshot-pipeline-stages`.
- **Live snapshot table:** `project-41542e21-470f-4589-96d.Core.fct_pipeline_stage_snapshots`, partitioned by `snapshot_date`, clustered by `(pipeline_id, status)`.
- **Canonical roadmap:** `docs/discovery/gold-layer-roadmap.md`
- **Phase A → B ADR:** `docs/decisions/2026-04-27-phase-a-to-b-transition.md`
- **Mart architecture commitment:** `docs/discovery/coverage-matrix.md` "Mart architecture commitment" + `.claude/rules/mart-naming.md` Rule 2
- **Phase B artifacts (on main):** `2-dbt/models/staging/fanbasis/{stg_fanbasis__transactions,stg_fanbasis__refunds}.sql`; `2-dbt/models/warehouse/facts/{fct_payments,fct_refunds}.sql`; `2-dbt/models/warehouse/bridges/bridge_identity_contact_payment.sql`; `2-dbt/models/marts/{lead_journey,revenue_detail,sales_activity_detail,speed_to_lead_detail}.sql`; `2-dbt/tests/{bridge_payment_count_parity,bridge_match_rate_floor,fanbasis_refund_parity,revenue_detail_refunds_parity}.sql`
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
- **Full history:** `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-04-28 evening (post-PRs #97 + #98 + four-PR `gtm-lead-warehouse` hotfix chain + production deploy of revision `bq-ingest-00076-wtl`).
- WORKLOG: skipped. Today's narrative arc (audit → three silent failures → fix chain → snapshots live → consolidation plan staged) is captured by destinations: PR descriptions for the six PRs (#1/#2/#3/#4 on `gtm-lead-warehouse`, #97/#98 on `dee-data-ops`); rule body of `.claude/rules/operational-health.md` (worked-examples #1-4); plan doc at `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`; cross-session memory `feedback_dont_dismiss_high_leverage_under_pause.md`. No residual narrative needs WORKLOG.
