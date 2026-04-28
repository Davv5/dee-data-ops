# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-28 late evening (post-PR #102 bq-ingest Step 2 code move into `services/bq-ingest/`)._

## Where we are

- **Step 2 of the bq-ingest consolidation is done.** PR #102 merged at `94f2f56` — copied the load-bearing tree (`app.py`, `sources/`, `ops/`, `sql/`, `requirements.txt`, `pyproject.toml`, `.python-version`, `RUNBOOK.md`) from `heidyforero1/gtm-lead-warehouse@515c89a` into `services/bq-ingest/`. 91 net files, 26,511 insertions after adversarial review caught + dropped 9 per-source `CLAUDE.md` files that would have auto-loaded misleading guidance. **No deploy in this PR** — Cloud Run still serves from the OLD repo. Step 3 (pointer updates) is unblocked.
- **`bq-ingest` is no longer publicly invokable.** PR #100's audit caught `allUsers → roles/run.invoker` on the Cloud Run service — removed after verifying the 8 Cloud Schedulers authenticate via OIDC (`sa-scheduler@`) and Calendly subs target Zapier hooks. URLs verified closed (unauth=403, auth=200).
- **PR #102 review surfaced 4 Step 3 items + 3 Step 4 items.** Step 3: `services/bq-ingest/ops/README.md` has 5 hardcoded `/Users/david/Documents/fanbasis-ingest/...` paths to rewrite; `services/bq-ingest/pyproject.toml` excludes `dbt/*` (gtm-lead-warehouse layout) instead of `2-dbt/*`; `.claude/rules/ingest.md` frontmatter glob `sources/**` doesn't match `services/bq-ingest/sources/**`; `.python-version` 3.13 vs root py311 conflict needs a one-line note in `services/bq-ingest/RUNBOOK.md`. Step 4: deploy command MUST be `--source services/bq-ingest` not `--source .`; `ops/env/triage/` directory missing in BOTH source and dest (audit miss); 6 (not 1, as audit said) CWD-fragile shell scripts. Full thread on PR #102 comments.
- **Two facts to port from dropped CLAUDE.md files** to `.claude/rules/` in Step 3: Fathom split-runtime (core SQL vs LLM enrichment); Raw → Core → Marts ordering. Currently nowhere in this repo's rules.
- **Audit's pre-existing 7 deferred follow-ups** still tracked in §"Deferred follow-ups" of `docs/discovery/bq-ingest-dependency-audit.md`.
- **GHL transition snapshots remain LIVE.** `Core.fct_pipeline_stage_snapshots` compounds daily at 07:00 UTC. First usable transition signal in ~6 days.
- **`bq-ingest` service** still on revision `bq-ingest-00076-wtl`, Python 3.13, 1024Mi, deployed from the OLD repo path. The audit captured the full env-var spec, secret refs, and IAM policy — these are the inputs Step 4's parity check uses.
- **Operational-health rule lives on main.** `.claude/rules/operational-health.md` distinguishes pausable vs non-pausable work; loud freshness gate replaces `|| true` in `dbt-nightly.yml`.
- **Phase B layer-build still on main** (PRs #84/#86/#88/#90/#92): Fanbasis staging, identity-contact-payment bridge, payments/refunds rename, revenue_detail net-of-refunds. Local-CI tooling (PRs #94/#95) remains the dev-loop bypass when GH Actions degrades.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **bq-ingest consolidation:** `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md` — Steps 1 & 2 shipped (PRs #100, #102). **Next: Step 3 (pointer updates: rules, runbooks, project-state, `.env.example` merge — see PR #102 review comments for the full list).** Step 4 = first deploy from new home with `/routes` parity check.
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-28 evening (latest)** — PR #102 merged (Step 2 code move, 91 net files). Adversarial-review pass dropped 9 per-source `CLAUDE.md` files that would have auto-loaded broken doc paths; round-2 review confirmed the call and surfaced 4 Step 3 + 3 Step 4 follow-ups (no code changes recommended for the Step 2 PR — all deferrable). Step 2 stayed cleanly mechanical.
- **2026-04-28 late evening** — PR #100 merged (audit + auth-posture fix). Two doc-review rounds; audit reframed Step 5 (Cloud Build trigger) from "required" to "optional-but-recommended" — David is the sole operator, so the original "stale clone" threat model was wrong; right rationale is deploy provenance / build reproducibility.
- **2026-04-28 evening** — Operational-health rule articulates the principle that emerged from the audit: extractor-pause silently destroyed data on operational pipelines. Pausable vs not-pausable distinction codified. Memory: `feedback_dont_dismiss_high_leverage_under_pause.md`. Companion PR #97 lands the rule + loud freshness gate.

## Open threads

- **Step 3 of the bq-ingest consolidation (next)** — pointer updates. Concrete list from PR #102 review: rewrite 5 absolute paths in `services/bq-ingest/ops/README.md`, fix `services/bq-ingest/pyproject.toml` exclude paths, extend `.claude/rules/ingest.md` frontmatter to cover `services/bq-ingest/**`, document Python 3.13 vs py311 split in `services/bq-ingest/RUNBOOK.md`, port Fathom split-runtime + Raw→Core→Marts ordering rules, merge `.env.example`, regenerate project-state.
- **Step 4 of the bq-ingest consolidation** — first deploy from the new home. Deploy command MUST be `gcloud run deploy bq-ingest --source services/bq-ingest …` (not `--source .`). Pre-existing latent bugs to address before/during: missing `ops/env/triage/` directory (audit miss); 6 CWD-fragile shell scripts (`typeform_pipeline.py:383`, `manage_schedulers_from_manifest.py:145`, `run_data_quality.sh:7`, `run_master_lead_reliability_gate.sh:12`, `run_identity_gap_analysis.sh:12`, `run_ingestion_parity.sh:12`); `ops/runner/tasks.py:71` dispatches `backfill.calendly_invitee_drain` to a non-existent module (the daily 03:45 UTC scheduler is probably silently failing today — verify before redeploying).
- **bq-ingest pre-existing deferred follow-ups (per audit §"Deferred follow-ups"):** Cloud Run Jobs image rebuild path, `1-raw-landing/` consolidation, SQL resolution cleanup (6 broken-default modules), secret hygiene (pin all to versions, rename `Secret`), orphan SQL audit (5 spec-only files), services/ polyrepo precedent.
- **Snapshot architecture follow-ups:** (a) extend `Core.fct_ghl_opportunities` upstream to surface `assigned_to_user_id` (currently NULL-cast in the snapshot); (b) once `Core.fct_pipeline_stage_snapshots` has ≥2 daily partitions, swap GHL freshness signal from `MAX(_ingested_at)` to `MAX(snapshot_date)`.
- **Vestigial Cloud Run Jobs** (`ghl-incremental-v2`, `calendly-incremental-v2`, `ghl-backfill-v2`) — not invoked by active schedulers; produce false signals. Delete or label.
- **Pre-existing stale PRs in dee-data-ops:** #50 + #44 (Metabase, both predate dabi pivot). Close or evaluate.
- **Float64-for-money tech debt (Fanbasis)** — `stg_fanbasis__transactions` and `stg_fanbasis__refunds` cast amounts to `float64`; should be `numeric`. PR #92's parity test absorbs the drift via $0.01 tolerance for now.
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
- **bq-ingest source (this repo, post-Step-2):** `services/bq-ingest/` — canonical going forward. Step 4 will deploy from here. Old: `heidyforero1/gtm-lead-warehouse` still hosts the production-deployed revision; local working copy at `~/Documents/gtm-lead-warehouse` (clean, on `515c89a`). **Other local clones at `~/Documents/fanbasis-ingest` are stale — do not deploy from them.** Repo archive + local clone deletion is Step 6.
- **bq-ingest production:** Cloud Run service `bq-ingest` in `project-41542e21-470f-4589-96d`, region `us-central1`, revision `bq-ingest-00076-wtl` (deployed 2026-04-28T17:17 from OLD repo via Buildpack; no Procfile/app.yaml/.gcloudignore). URL: `https://bq-ingest-mjxxki4snq-uc.a.run.app`. **Requires authenticated invocation:** `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" /routes`.
- **PR #102 review thread (Step 3 + Step 4 punch list):** https://github.com/Davv5/dee-data-ops/pull/102 — comments `4338500214` (round 1) and `4338545264` (round 2).
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
- **Full history:** `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-04-28 late evening (post-PR #102 bq-ingest Step 2 code move).
- WORKLOG: skipped. Today's session shipped PR #102 with a thorough description and two thorough review-finding comments (round 1: 4338500214, round 2: 4338545264). The PR + comments are the audit log; routing rule "Code that shipped → PR description ✓" applies cleanly. No new rule, ADR, or memory landed this session — the rule/ADR ports to `.claude/rules/` (Fathom split-runtime, Raw→Core→Marts ordering) belong to Step 3.
