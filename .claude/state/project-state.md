# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-29 morning (post-PR-#107/#108 deploy + cleanup-PR #110 in flight)._

## Where we are

- **`bq-ingest` runs revision `bq-ingest-00087-zah` at 100% traffic.** Deployed 2026-04-29 from `services/bq-ingest/` after PR #107 (SQL-path fallback) and PR #108 (rule pairing) merged. `/ingest-calendly` returns 200 again (was failing 5 days with FileNotFoundError); 27 events caught up on first run. Old `bq-ingest-00085-qar` retained at 0% as rollback.
- **`/ingest-fathom` still 500s post-deploy — new failure mode.** PR #107 cleared the FileNotFoundError, but the BigQuery model-refresh now hangs past the gunicorn worker timeout (~180s). Distinct bug, not a regression. Tracked in Open threads.
- **Cleanup PR #110 in flight** — calendly-invitee-drain dispatch fix (smart run_id discovery + quiescence guard + explicit-run_id validation + non-COMPLETED → exit 1) and `app.py:42` brand-string flip. Adversarial + correctness + project-standards reviewers all run; HIGH/MEDIUM findings addressed pre-merge. Awaiting human review + merge + post-merge `ops/scripts/deploy_runtime_stack.sh` to rebuild `fanbasis-python-runner:latest` (the Cloud Run Job's image, distinct from the bq-ingest service image).
- **bq-ingest consolidation Steps 1–4 shipped** (PRs #100/#102/#104 + 2026-04-28 deploy). `dee-data-ops` is the production deploy origin; `gtm-lead-warehouse` is no longer load-bearing.
- **`bq-ingest` requires authenticated invocation.** `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" ...`
- **GHL transition snapshots remain LIVE.** `Core.fct_pipeline_stage_snapshots` compounds daily at 07:00 UTC. First usable transition signal in ~5 days.
- **Phase B layer-build still on main** (PRs #84/#86/#88/#90/#92): Fanbasis staging, identity-contact-payment bridge, payments/refunds rename, revenue_detail net-of-refunds. Local-CI tooling (PRs #94/#95) remains the dev-loop bypass.
- **`ask-corpus` v2 engine** lives on main (PR #74).
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline.
- **bq-ingest consolidation Steps 1–4 shipped** (`docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`); Steps 5/6 in Open threads. **Methodology corpus engine v2** is on main (11/13 units shipped, `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md`). **GTM source-port plan paused** (`docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`) — U4a+ resumes when GHL trusted-copy decision lands.
- **Headline metric (locked 2026-04-19):** unchanged.

## Last 3 decisions

- **2026-04-29 morning (latest)** — Merged PR #107 (SQL-path fallback fix) + PR #108 (Reviews always pair rule). Redeployed `bq-ingest` to revision `00087-zah` at 100% traffic. `/ingest-calendly` confirmed 200. Discovered `/ingest-fathom` has a new 500 failure mode (BQ query timeout, distinct from PR #107's scope). Opened PR #110 to close two Step-4 deferred items (calendly-invitee-drain dispatch + brand-string flip) with adversarial + correctness + standards review pass.
- **2026-04-28 late-night** — Reconciliation sweep + `pivot-discipline.md` rule. Closed 7 stale plan-debt artifacts caused by un-walked pivots; new rule requires same-session walk of superseded docs when a pivot memory is saved.
- **2026-04-28 night** — Step 4 deploy complete. Revision `bq-ingest-00085-qar` (now retained as rollback) reached 100% traffic. Confirmed `calendly-invitee-drain` Job was already failing daily for 5 days pre-deploy.

## Open threads

- **Cleanup PR(s) post-Step-4 — closed.** (a)+(b) shipped in PR #110; (c)+(d) shipped in PR #111; (e) shipped here. Audit framing of "6 CWD-fragile shell-script paths" was a mishmemory of the 6 Python broken-default modules — those were the actual CWD-relative bugs and were fixed by PR #107. Shell-script audit in this PR found 1 genuine bug (`run_data_quality.sh` referenced a non-existent module path); all other shell scripts use either `$ROOT_DIR/...` (absolute) or `cd "$ROOT"` (robust to wrong CWD). Two vestigial v2-cutover scripts (`run_canary_parity_check.sh`, `cutover_schedulers_to_v2.sh`) reference non-existent Cloud Run Jobs — folded into the vestigial-v2 thread below.
- **`/ingest-fathom` timeout — closed 2026-04-29 (PR #113 + operational env-var removal).** Root cause: `fathom_models.sql` is 22 statements summing >180s (`bridge_fathom_contact_match_candidates` 80-130s + BQML model retrain 70-100s + ML.PREDICT/enriched/diagnostics). The hourly `/ingest-fathom` HTTP endpoint had `FATHOM_RUN_MODELS_AFTER_INCREMENTAL=true` set on the bq-ingest Cloud Run service, causing it to call `run_models()` inline. Fix: removed the env-var override (code default `false` now applies); model refresh continues to happen in the `pipeline-marts-hourly` Cloud Run Job context. Verified: `/ingest-fathom` now returns 200 in ~5s with `models_refreshed=false`. Architectural rule documented in `.claude/rules/bq-ingest.md` §"Hourly HTTP path skips heavy model refresh".
- **`*_RUN_MODELS_AFTER_INCREMENTAL` audit — env-var flips done 2026-04-29; GHL contacts freshness gap unresolved.** Both `GHL_RUN_MODELS_AFTER_INCREMENTAL` and `CALENDLY_RUN_MODELS_AFTER_INCREMENTAL` removed from the bq-ingest service. PR #115 added `model.ghl` to `run_marts_with_dependencies` so GHL refresh continues hourly via the marts Cloud Run Job context. PR (this) removed legacy `Marts.mrt_speed_to_lead_{daily,overall}` definitions from `ghl_models.sql` so `marts.sql` is the canonical owner (was a schema-flip race — see HIGH thread below). Verified at HTTP layer: `/ingest-fathom`/`/ingest-calendly`/`/ingest-ghl` all return 200 with `models_refreshed=false`. **Caveat:** `/ingest-ghl` returns 200 overall but the `contacts` entity has been silently `status=FAILED` (see GHL contacts 422 thread below) — closure of this audit reflects the env-var architecture, not full data-freshness health. Architectural rule lives in `.claude/rules/bq-ingest.md` §"Hourly HTTP path skips heavy model refresh".
- **🔴 HIGH — GHL contacts entity silently failing (NEW 2026-04-29).** `/ingest-ghl` reports `entity_results[contacts].status=FAILED` with upstream 422 (`Invalid Operator (gte) passed for field date_updated`) — but the route returns HTTP 200 because per-entity failures are absorbed inside the loop (`ghl_pipeline.py:1211-1224`). Cloud Scheduler shows green; no monitoring catches this. **Contacts is the FIRST entity in the loop, so it fails first every hour** — this has presumably been silently broken since whenever the LeadConnectorHQ API last changed; the env-var flip just made the per-entity error visible in the JSON response. Verified-by-correctness-reviewer that the env-var removal didn't cause it (env-var only gates the post-loop `run_models()` call at line 1226-1229; contacts query at lines 716-723 is unaffected). Investigation needed: (a) what operator name does LeadConnectorHQ now expect on `dateUpdated` (note 422 echoes `date_updated` snake_case — API normalizes server-side then rejects); (b) `parse_object_types()` defaults to a list with `contacts` first, so every hour fails contacts first; (c) downstream impact — `Core.fct_calls_booked` and `mrt_speed_to_lead_daily` join GHL contacts → freshness has been degrading silently; need to query `MAX(_ingested_at) FROM Raw.ghl_objects_raw WHERE entity_type='contacts'` to quantify the gap. **Owns its own investigation PR.**
- **🟡 MEDIUM — `ghl-identity-sync` Job retargeted by `deploy_runtime_stack.sh` (NEW 2026-04-29).** `jobs.yaml:514` declares `target.command = sh -c "python3 -m ops.runner.cli run identity.ghl_users_sync && python3 -m ops.runner.cli run backfill.ghl"` — a chained identity-sync + GHL backfill. The prior `current.command` was identity-sync only. Today's `deploy_runtime_stack.sh` run (post-PR-#115) reconciled current → target, so every 2hr (per `ghl-identity-sync-2h` scheduler) the Job now runs an extra `backfill.ghl` invocation in addition to identity sync. The backfill has `GHL_BACKFILL_MAX_CONTACTS_PER_RUN=5000` set on the *backfill* Job's env (`jobs.yaml:485`), but the identity-sync Job's env doesn't inherit this — so the 5000-contact cap likely doesn't apply when backfill runs through the identity-sync Job context. Verify: was the chained command intended (e.g., a recent jobs.yaml edit), or is this a stale `target` that shouldn't have applied? If unintended, revert via `gcloud run jobs update ghl-identity-sync --command=python3 --args=...`. If intended, ensure the rate-limit env vars are set on this Job too.
- **`pipeline-marts-hourly` operating notes (corrections from PR #115 review 2026-04-29).** (a) Timeout is `timeout_seconds: 3600` (1hr) per `jobs.yaml:623`, not the 10800s I claimed elsewhere — current sequence runtime is ~5-10 min so plenty of headroom, but worst-case BQ slot contention could approach the cap. (b) Failure semantics in `run_marts_with_dependencies` (`tasks.py:122-136`) are fail-fast: any of calendly/fathom/ghl raising aborts the function, marts is skipped. Pre-PR-#115, GHL had its own independent cadence via the HTTP path; now it's coupled to calendly success — by design, but worth noting. (c) Wall-clock cadence shifted: previously GHL Job at `:20` wrote `mrt_speed_to_lead_daily` inline; now only marts Job at `:50` writes it. Average staleness ~same; bookings landing :21-:50 wait longer to surface in the headline mart. (d) Verification was via `Task succeeded` log line — we did NOT capture per-step `executed` integers. Future verification should record `dependencies['model.ghl']` count to confirm the table actually rewrote.
- **Step 5 (optional)** — Cloud Build trigger watching `services/bq-ingest/**`. Reframed by audit as deploy provenance / build reproducibility, not stale-clone defense.
- **Step 6 (after a few days of clean operation)** — archive `heidyforero1/gtm-lead-warehouse` + delete stale local clones at `~/Documents/{fanbasis-ingest,gtm,gtm-lead-warehouse}`.
- **bq-ingest pre-existing deferred follow-ups (per audit §"Deferred follow-ups"):** Cloud Run Jobs image rebuild path, `1-raw-landing/` consolidation, secret hygiene (pin all to versions, rename `Secret`), orphan SQL audit (5 spec-only files), services/ polyrepo precedent.
- **Snapshot architecture follow-ups:** (a) extend `Core.fct_ghl_opportunities` upstream to surface `assigned_to_user_id`; (b) once `Core.fct_pipeline_stage_snapshots` has ≥2 daily partitions, swap GHL freshness signal from `MAX(_ingested_at)` to `MAX(snapshot_date)`.
- **Calendly drain follow-ups (deferred from PR #110 review):** (a) advisory lock on concurrent drain Jobs — operator manual + scheduled trigger can race on the same run_id; (b) `mode` column on `calendly_backfill_state` to distinguish backfill (3hr Job timeout) vs hourly-incremental (60min request timeout) run_ids — the 60-min quiescence guard closes the hourly race but backfills with invitee fan-out >60min still race; (c) FAILED runs are silently abandoned by `_find_drainable_run_id` (filters `status IN ('PENDING','RUNNING')` only) — needs an explicit re-discovery path or operator alert.
- **Vestigial v2-cutover artifacts** — Cloud Run Jobs (`ghl-incremental-v2`, `calendly-incremental-v2`, `ghl-backfill-v2`) not invoked by active schedulers; produce false signals. Plus shell scripts that reference them: `run_canary_parity_check.sh` (references non-existent `pipeline-run-v2` and `python-runner-v2`), `cutover_schedulers_to_v2.sh` (already-done v1→v2 scheduler cutover machinery). Delete or label as a single cleanup pass.
- **Pre-existing stale PRs in dee-data-ops:** #50 + #44 (Metabase, both predate dabi pivot). Close or evaluate.
- **Float64-for-money tech debt (Fanbasis)** — `stg_fanbasis__transactions` and `stg_fanbasis__refunds` cast amounts to `float64`; should be `numeric`.
- **`fct_calls_booked` SK gaps** — `assigned_user_sk` + `pipeline_stage_sk` still hardcoded `cast(null as string)` at `2-dbt/models/warehouse/facts/fct_calls_booked.sql:70-71`. CTEs defined but unused.
- **GHL trusted-copy decision** — single named blocker for several Tier B / refresh marts.
- **GCP IAM hygiene (cosmetic, not blocking).** ADC via `dev_local` / `ci_local` is the working path; SA key for consolidated project still unprovisioned.
- **Fathom → GHL contact join key** — attendee email reliability. Affects future `fct_calls_held`.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` + `QUALITY_PARITY_FLOOR=0.6` ship provisional.
- **Strategic-Reset-paused threads:** Typeform `form_id` upstream gap, GHL `messages`/`users`/`tasks` 0-row upstream, Fathom transcript landing, Stripe staleness.
- **Week-0 client asks** — unchanged.

## Where to look

- **bq-ingest dependency audit:** `docs/discovery/bq-ingest-dependency-audit.md`
- **bq-ingest consolidation plan:** `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`
- **Operational-health rule:** `.claude/rules/operational-health.md`
- **bq-ingest source (canonical):** `services/bq-ingest/` in this repo.
- **bq-ingest production:** Cloud Run service `bq-ingest`, revision `bq-ingest-00087-zah` (deployed 2026-04-29). URL: `https://bq-ingest-mjxxki4snq-uc.a.run.app`. PR-110 tag URL `https://pr107---bq-ingest-mjxxki4snq-uc.a.run.app` resolves to the same revision.
- **Redeploy command (durable):** `gcloud run deploy bq-ingest --source services/bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d --memory=1024Mi`. Add `--no-traffic --tag <name>` for parity workflows.
- **Cloud Run Jobs redeploy** (separate image): `cd services/bq-ingest && ops/scripts/deploy_runtime_stack.sh` rebuilds `fanbasis-python-runner:latest` AND updates Jobs from `ops/cloud/jobs.yaml`.
- **bq-ingest service rules:** `.claude/rules/bq-ingest.md` (path-scoped to `services/bq-ingest/**`).
- **Live snapshot table:** `project-41542e21-470f-4589-96d.Core.fct_pipeline_stage_snapshots`, partitioned by `snapshot_date`, clustered by `(pipeline_id, status)`.
- **Canonical roadmap:** `docs/discovery/gold-layer-roadmap.md`
- **Phase A → B ADR:** `docs/decisions/2026-04-27-phase-a-to-b-transition.md`
- **Mart architecture commitment:** `docs/discovery/coverage-matrix.md` + `.claude/rules/mart-naming.md` Rule 2
- **Phase B artifacts (on main):** `2-dbt/models/staging/fanbasis/`, `2-dbt/models/warehouse/{facts,bridges}/`, `2-dbt/models/marts/{lead_journey,revenue_detail,sales_activity_detail,speed_to_lead_detail}.sql`
- **Local dev loop:** `2-dbt/scripts/local-ci.sh` + `2-dbt/profiles.yml` (`dev_local` / `ci_local`) + `2-dbt/README.md` "Local CI" section.
- **Data-engineer agent + LAW skills:** `~/.claude/agents/data-engineer.md`. Pairing rule: `.claude/rules/use-data-engineer-agent.md` "Reviews always pair" (added in PR #108).
- **Corpus engine v2:** `.claude/skills/ask-corpus/scripts/` + `SKILL.md`
- **Fast operating loop:** `docs/runbooks/operator-fast-loop.md`
- **Shared portable kit:** `/Users/david/Documents/agent-kit`
- **Codex parity:** `AGENTS.md` + `.agents/skills/{ask-corpus,skill-creator,worklog}/`
- **Sprint artifacts:** `docs/discovery/{source-inventory,source-shapes,staging-models,gap-analysis,insights-summary,business-area-map,coverage-matrix,gold-layer-roadmap}.md`
- **Memories (auto-loaded):** see `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `.claude/rules/*.md`
- **Routing rule for end-of-session:** `.claude/rules/worklog.md`
- **Pivot-discipline rule:** `.claude/rules/pivot-discipline.md`
- **Full history:** `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-04-29 morning (post-PR-#107/#108 merge + deploy + PR-#110 in flight).
- WORKLOG: deferred to PR #110 merge — that PR's description carries the narrative (calendly-invitee-drain dispatch fix, brand-string flip, three-reviewer pass + remediation). Per `.claude/rules/worklog.md` routing table: PR description captures shipped work, this regen captures present state, both replace a WORKLOG entry.
- Earlier _meta entries (Step 4 deploy, reconciliation sweep) — narratives carried by their respective PR descriptions and the `pivot-discipline.md` rule file.
