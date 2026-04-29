# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-29 evening (post PR-#118 through #130 — full session capture; first dbt-prod-green night since at least 2026-04-25)._

## Where we are

- **`bq-ingest` runs revision `bq-ingest-00083-5kk` at 100% traffic.** Includes PRs #118 + #119 + #127 (GHL contacts ingest fully fixed). `00093-xiv` retained at 0% as rollback. Live verified 2026-04-29 evening: `/ingest-ghl` returned `entity_results.contacts.status=COMPLETED` and 48 fresh contacts landed in `Raw.ghl_objects_raw` — first non-stale contacts ingest in 26+ hours.
- **dbt prod auth runs via Workload Identity Federation** (PR #126). Workflows authenticate to `sa-transform@project-41542e21-470f-4589-96d` via OIDC; pool `github-actions`, provider `github-oidc`. The `GCP_SA_KEY_PROD` secret is **dormant** — no workflow uses it. Org policy `constraints/iam.disableServiceAccountKeyCreation` blocks new SA-key creation; WIF is the only forward path.
- **dbt prod schemas are STG / Core / Marts** (capitalized — Y1 cutover, PR #129). dbt-built tables coexist alongside bq-ingest's inline-SQL writers in those datasets (different table names, no conflict). `dbt_project.yml` `+schema:` config + profiles.yml fallback `dataset: Marts` aligned. Snapshots route to Core (PR #130 fix removed a model-level override that was silently sending them to lowercase `snapshots`).
- **dbt nightly is GREEN.** Run `25130795764` (workflow_dispatch, 19:59 UTC): PASS=288 WARN=3 ERROR=0 / 291. The 3 WARNs are configured `severity: 'warn'` release-gate tests against the 2026-03-19 oracle baseline (40 days of expected drift, pre-existing). **Tomorrow's 08:00 UTC scheduled nightly is the first true compound verification** under cron context.
- **GHL sources read from `raw_ghl_v2` views** (PR #128). Per-entity views over `Raw.ghl_objects_raw` filtered by `entity_type` and aliasing `ingested_at AS _ingested_at`. The legacy `raw_ghl.ghl__<entity>_raw` tables are untouched (last-write 2026-04-19) — kept for historical reference.
- **`fct_calls_booked.{assigned_user_sk,pipeline_stage_sk}` are wired** (PR #123). Strategy: most-recent opp where `opportunity_created_at <= booked_at`, with `opportunity_id desc` as deterministic tiebreaker. Diagnostic attribution only — Speed-to-Lead numerator still sources first-touch identity from `raw_ghl.conversations`/`fct_outreach`.
- **Fanbasis money columns are NUMERIC end-to-end** (PR #120). `stg_fanbasis__{transactions,refunds}` cast amounts as NUMERIC; `fct_payments` Stripe arm flipped for UNION-ALL type-symmetry; `release_gate_revenue_detail` thresholds use `numeric '0.05'`/`numeric '0.10'` literals.
- **`bq-ingest` requires authenticated invocation.** `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" ...`
- **GHL transition snapshots remain LIVE.** `Core.fct_pipeline_stage_snapshots` compounds daily at 07:00 UTC.
- **bq-ingest consolidation Steps 1–4 shipped** (PRs #100/#102/#104 + 2026-04-28 deploy). `dee-data-ops` is the production deploy origin; `gtm-lead-warehouse` is no longer load-bearing.
- **Phase B layer-build still on main** (PRs #84/#86/#88/#90/#92): Fanbasis staging, identity-contact-payment bridge, payments/refunds rename, revenue_detail net-of-refunds.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline.
- **Headline metric (locked 2026-04-19):** unchanged.

## Last 3 decisions

- **2026-04-29 evening (latest)** — Closed the dbt-prod-fully-broken-since-consolidation arc. **Thirteen PRs total** (#118–#130). Four exposed cascading post-U2 consolidation residue: PR #126 (WIF cutover; sa-transform via OIDC), PR #127 (corrected GHL contacts filter to `operator: range` with inner `{gt, lt}` — earlier `gt` fix in #118 was wrong, live deploy revealed it), PR #128 (repointed dbt sources at `raw_ghl_v2` views), PR #129 (Y1 schema cutover: lowercase → capitalized STG/Core/Marts to match consolidated project's actual datasets). PR #130 was the post-Y1 cleanup: snapshot model-level `target_schema='snapshots'` override removed (now inherits project-level Core), plus revision-name drift in state/rule corrected (00093-xiv → 00083-5kk). Y3 deferred (dbt owns Marts entirely, retire bq-ingest's inline marts.sql — multi-day, no urgency).
- **2026-04-29 morning** — Merged PRs #107/#108/#110/#111/#112/#113/#114/#115/#116/#117. Redeployed `bq-ingest` to revision `00087-zah` (later superseded by 00093-xiv → 00083-5kk in evening cycle). Closed `/ingest-fathom` timeout via env-var removal. Added `model.ghl` to `run_marts_with_dependencies` (PR #115). Removed legacy `Marts.mrt_speed_to_lead_*` writes from `ghl_models.sql` (PR #117).
- **2026-04-28 late-night** — Reconciliation sweep + `pivot-discipline.md` rule. Closed 7 stale plan-debt artifacts caused by un-walked pivots; new rule requires same-session walk of superseded docs when a pivot memory is saved.

## Open threads

- **🟡 Decommission `GCP_SA_KEY_PROD` secret + revoke `dbt-prod@dee-data-ops-prod` keys** — secret is dormant post-WIF cutover. Recommend deleting after tomorrow's 08:00 UTC scheduled nightly confirms WIF stability under cron context. `gh secret delete GCP_SA_KEY_PROD` (irreversible).
- **🟡 Drop orphan lowercase `staging`/`warehouse`/`snapshots` datasets** — partial materialization from the failed PR-#128 deploy at 19:48 UTC. Currently same row counts as Y1 capitalized targets (both VIEWS over the same source data), but FUTURE risk: when staging logic next changes, only `STG.*` updates while `staging.*` definitions stay frozen. Drop with `bq rm -r --dataset` once confirmed nothing reads from lowercase names.
- **Tomorrow's 08:00 UTC scheduled nightly = first compound verification.** Today's green run was workflow_dispatch-triggered. Cron context may surface subtle differences (env, working directory). If red: most likely failure surface is something cron-context exposes that workflow_dispatch didn't.
- **Stripe staging `safe_cast` rule violation** (NEW from PR #120 review). `stg_stripe__charges.sql:28-30` uses `safe_cast(... as int64)` for amount columns. Per `staging.md` SAFE_CAST should only appear in WHERE clauses. Stripe is historical-only (frozen data); fix is a small follow-up PR with careful CI build.
- **dabi NUMERIC rendering verification** (NEW post-PR-#120). First dabi pipeline run after Y1 will surface whether NUMERIC columns render as expected. If not, mart-layer `format()` cast is the right place.
- **Multi-page contacts unit test** (NEW post-PR-#119/#127). bq-ingest has no test infra; reviewer recommended a stub-mocked test that returns two pages with `searchAfter` cursors and asserts page-2 body shape (and the `operator: range` filter shape, given the operator has been wrong twice).
- **Mart cleanup post-#123** — `sales_activity_detail.sql:127-156` and `lead_journey.sql:77-95` use divergent opp-selection rules. PR #123 picked one canonical rule at the fact layer; follow-up PR should collapse the marts to consume `fct_calls_booked.{assigned_user_sk,pipeline_stage_sk}` directly.
- **Y3 architectural cleanup** (deferred) — dbt owns Marts entirely, retire bq-ingest's inline `marts.sql` writers. The cleanest endpoint to the dual-write coexistence Y1 left in place. Multi-day; no urgency.
- **`pipeline-marts-hourly` operating notes** (corrections from PR #115 review). Timeout 3600s; `run_marts_with_dependencies` is fail-fast; marts Job at `:50` is the only writer of `mrt_speed_to_lead_daily` (post-#117).
- **Step 5 (optional)** — Cloud Build trigger watching `services/bq-ingest/**`. Reframed as deploy provenance, not stale-clone defense.
- **Step 6** — archive `heidyforero1/gtm-lead-warehouse` + delete stale local clones at `~/Documents/{fanbasis-ingest,gtm,gtm-lead-warehouse}`.
- **bq-ingest pre-existing deferred follow-ups (per audit §"Deferred follow-ups"):** Cloud Run Jobs image rebuild path, `1-raw-landing/` consolidation, secret hygiene, orphan SQL audit (5 spec-only files), services/ polyrepo precedent.
- **Snapshot architecture follow-ups:** (a) extend `Core.fct_ghl_opportunities` upstream to surface `assigned_to_user_id`; (b) once `Core.fct_pipeline_stage_snapshots` has ≥2 daily partitions, swap GHL freshness signal from `MAX(_ingested_at)` to `MAX(snapshot_date)`.
- **Calendly drain follow-ups (deferred from PR #110 review):** advisory lock; `mode` column on `calendly_backfill_state`; FAILED-run handling.
- **Vestigial v2-cutover artifacts** — `ghl-incremental-v2`, `calendly-incremental-v2`, `ghl-backfill-v2` Jobs + scripts referencing them. Delete or label.
- **Pre-existing stale PRs:** #50 + #44 (Metabase, both predate dabi pivot). Close or evaluate.
- **GHL trusted-copy decision** — single named blocker for several Tier B / refresh marts.
- **Fathom → GHL contact join key** — attendee email reliability. Affects future `fct_calls_held`.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` + `QUALITY_PARITY_FLOOR=0.6` ship provisional.
- **Strategic-Reset-paused threads:** Typeform `form_id` upstream gap, GHL `messages`/`users`/`tasks` 0-row upstream, Fathom transcript landing, Stripe staleness.
- **Week-0 client asks** — unchanged.

## Where to look

- **bq-ingest production:** Cloud Run service `bq-ingest`, revision `bq-ingest-00083-5kk` (deployed 2026-04-29 evening, includes PRs #118 + #119 + #127). URL: `https://bq-ingest-mjxxki4snq-uc.a.run.app`. `bq-ingest-00093-xiv` retained at 0% as rollback.
- **WIF setup:** Pool `github-actions`, provider `github-oidc` in project `project-41542e21-470f-4589-96d` (location global). SA `sa-transform@project-41542e21-470f-4589-96d` bound via `roles/iam.workloadIdentityUser`. Workflows: `dbt-{nightly,deploy,docs}.yml` use `workload_identity_provider:` instead of `credentials_json:`.
- **Y1 schema cutover:** dbt prod targets `STG`/`Core`/`Marts` (capitalized) datasets. Snapshots → `Core` (project-level `+target_schema: Core` in dbt_project.yml). Seeds → `STG`.
- **`raw_ghl_v2` views:** Per-entity views over `Raw.ghl_objects_raw`; created out-of-band 2026-04-29. Each view filters by `entity_type` and aliases `ingested_at AS _ingested_at`. dbt sources YAML's `schema: raw_ghl_v2`.
- **bq-ingest dependency audit:** `docs/discovery/bq-ingest-dependency-audit.md`
- **bq-ingest consolidation plan:** `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`
- **Operational-health rule:** `.claude/rules/operational-health.md`
- **bq-ingest source (canonical):** `services/bq-ingest/` in this repo.
- **Redeploy command (durable):** `gcloud run deploy bq-ingest --source services/bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d --memory=1024Mi`. Add `--no-traffic --tag <name>` for parity workflows.
- **Cloud Run Jobs redeploy** (separate image): `cd services/bq-ingest && ops/scripts/deploy_runtime_stack.sh` rebuilds `fanbasis-python-runner:latest` AND updates Jobs from `ops/cloud/jobs.yaml`.
- **bq-ingest service rules:** `.claude/rules/bq-ingest.md` (path-scoped to `services/bq-ingest/**`); empirical anchor names range-operator + searchAfter contract.
- **Live snapshot table:** `project-41542e21-470f-4589-96d.Core.fct_pipeline_stage_snapshots`, partitioned by `snapshot_date`, clustered by `(pipeline_id, status)`.
- **Canonical roadmap:** `docs/discovery/gold-layer-roadmap.md`
- **Phase A → B ADR:** `docs/decisions/2026-04-27-phase-a-to-b-transition.md`
- **Mart architecture commitment:** `docs/discovery/coverage-matrix.md` + `.claude/rules/mart-naming.md` Rule 2
- **Phase B artifacts (on main):** `2-dbt/models/staging/fanbasis/`, `2-dbt/models/warehouse/{facts,bridges}/`, `2-dbt/models/marts/{lead_journey,revenue_detail,sales_activity_detail,speed_to_lead_detail}.sql`
- **Local dev loop:** `2-dbt/scripts/local-ci.sh` + `2-dbt/profiles.yml` (`dev_local` / `ci_local`) + `2-dbt/README.md` "Local CI" section.
- **Data-engineer agent + LAW skills:** `~/.claude/agents/data-engineer.md`. Pairing rule: `.claude/rules/use-data-engineer-agent.md`.
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

- Last regen: 2026-04-29 evening (post-#118 through #130, full session capture).
- WORKLOG: skipped — all session output captured by the 13 PR descriptions (#118–#130), the bq-ingest.md empirical anchor (PR #122 + corrected by #130), the worktree-cleanup investigation log at `.tmp/worktree-salvage-2026-04-29/`, and the WIF/Y1 architectural changes documented inline in this regen. Per `.claude/rules/worklog.md` routing table: PR descriptions + rule-file edits + this regen replace a WORKLOG entry.
- Earlier _meta entries (PR #107/#108 morning, Step 4 deploy, reconciliation sweep) — narratives carried by their respective PR descriptions and rule files.
- Recurring drift pattern (caught twice this session — PR #122 + PR #130): claiming "live revision" or "fixed end-to-end" without verifying via `gcloud run services describe`. Future-Claude: before writing such claims, run `gcloud run services describe bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d --format='value(status.traffic[0].revisionName)'` and quote the output.
