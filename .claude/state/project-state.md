# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-30 early morning UTC (post-PR-#133 merge + Y3 audit PR #132 multi-revision in flight)._

## Where we are

- **`bq-ingest` live revision is `00083-5kk`** but **main is ahead** as of 2026-04-30 00:23 UTC. PR #133 merged retires the dead HTTP analytical surface (`/ask`, `/query`, `/query/catalog`) plus `analyst.py` + `warehouse_queries.py` (698 lines deleted). No Cloud Build trigger or workflow auto-deploys this service; the next `gcloud run deploy bq-ingest --source services/bq-ingest …` retires the live routes. No traffic to break — surfaces verified dead via 30-day log scan before retirement.
- **`bq-ingest` revision `00083-5kk`** still includes PRs #118 + #119 + #127 (GHL contacts ingest fully fixed). `00093-xiv` retained at 0% as rollback.
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

- **2026-04-29/30 late evening (latest)** — Y3 marts.sql retirement scoped via PR #132 (audit doc, three revision passes after multi-persona `/ce-doc-review`); PR #133 merged retiring dead HTTP analytical surface (`/ask`, `/query`, `/query/catalog`, `analyst.py`, `warehouse_queries.py`, `google-genai` dep — 698 lines). Audit revealed: (1) David's account has read 17 of the originally-DROP-classified tables in the last 27 days — "confirmed orphan" classification was wrong on framework grounds, not data; (2) `Marts.bridge_identity_contact_payment` is scope-different from dbt's `Core.bridge_identity_contact_payment` (357 vs 3,844 rows, 3 vs 5 match tiers) — not byte-equivalent, repoint needs overlap-tier reconciliation; (3) Marts pipeline is healthy post-Y1 (was misread as broken earlier in session). Y3 itself is gated on Batch 0 verification + David's disposition on ~26 tables.
- **2026-04-29 evening** — Closed the dbt-prod-fully-broken-since-consolidation arc. **Thirteen PRs total** (#118–#130). Four exposed cascading post-U2 consolidation residue: PR #126 (WIF cutover; sa-transform via OIDC), PR #127 (corrected GHL contacts filter to `operator: range` with inner `{gt, lt}`), PR #128 (repointed dbt sources at `raw_ghl_v2` views), PR #129 (Y1 schema cutover: lowercase → capitalized STG/Core/Marts). PR #130 post-Y1 cleanup (snapshot routing + revision-name drift correction).
- **2026-04-29 morning** — Merged PRs #107/#108/#110/#111/#112/#113/#114/#115/#116/#117. Redeployed `bq-ingest` to `00087-zah` (later superseded by `00093-xiv` → `00083-5kk` in evening). Closed `/ingest-fathom` timeout via env-var removal. Removed legacy `Marts.mrt_speed_to_lead_*` writes from `ghl_models.sql` (PR #117).

## Open threads

- **🟡 Live deploy of bq-ingest** — main is ahead of revision `00083-5kk` after PR #133. Next `gcloud run deploy bq-ingest --source services/bq-ingest …` retires `/ask` + `/query` + `/query/catalog` from the live service. No traffic to break.
- **🟡 Decommission `GCP_SA_KEY_PROD` secret + revoke `dbt-prod@dee-data-ops-prod` keys** — secret is dormant post-WIF cutover. Recommend deleting after the 2026-04-30 08:00 UTC scheduled nightly confirms WIF stability under cron context. `gh secret delete GCP_SA_KEY_PROD` (irreversible).
- **🟡 Drop orphan lowercase `staging`/`warehouse`/`snapshots` datasets** — partial materialization from the failed PR-#128 deploy at 19:48 UTC 2026-04-29. Currently same row counts as Y1 capitalized targets (both VIEWS over the same source data), but FUTURE risk: when staging logic next changes, only `STG.*` updates while `staging.*` definitions stay frozen. Drop with `bq rm -r --dataset` once confirmed nothing reads from lowercase names.
- **2026-04-30 08:00 UTC scheduled nightly = first compound verification.** Yesterday's green run was workflow_dispatch-triggered. Cron context may surface subtle differences (env, working directory). If red: most likely failure surface is something cron-context exposes that workflow_dispatch didn't.
- **Stripe staging `safe_cast` rule violation** (NEW from PR #120 review). `stg_stripe__charges.sql:28-30` uses `safe_cast(... as int64)` for amount columns. Per `staging.md` SAFE_CAST should only appear in WHERE clauses. Stripe is historical-only (frozen data); fix is a small follow-up PR with careful CI build.
- **dabi NUMERIC rendering verification** (NEW post-PR-#120). First dabi pipeline run after Y1 will surface whether NUMERIC columns render as expected. If not, mart-layer `format()` cast is the right place.
- **Multi-page contacts unit test** (NEW post-PR-#119/#127). bq-ingest has no test infra; reviewer recommended a stub-mocked test that returns two pages with `searchAfter` cursors and asserts page-2 body shape (and the `operator: range` filter shape, given the operator has been wrong twice).
- **Mart cleanup post-#123 — PR-1 in flight (PR #135).** Multi-PR refactor scoped at `docs/plans/2026-04-30-mart-collapse-fct-sks-plan.md`. PR-1 widens `fct_calls_booked` with `selected_opportunity_id` (canonical mart-side join axis) — strictly additive, no consumer change yet. PR-2 collapses `sales_activity_detail.closer_and_outcome` behind a parity gate; PR-3 retires the gate. **PR-2 blocked on David answering Q3.1 (closer_name semantics: booking-time vs current vs both), Q3.2 (lead_journey current pipeline state), Q3.3 (outcome-column drift acceptance) in plan §8.** `lead_journey.latest_opportunity` is intentionally out of scope — answers a contact-grain "current state" question the booking-grain fact does not.
- **Y3 audit landed (PR #132)** — `marts.sql` retirement scope is defined: 22 confirmed-orphan + 5 cascade-hold + 1 parity-gated + 2 superseded + 15 PORT, post-#133 redistribution noted via banner. **Blocked on Batch 0 verification:** (1) `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` 27-day scan against the 19 confirmed-orphan + 9 ex-warehouse_queries.py-only PORT tables; (2) David disposition on the ~17 tables with his ad-hoc reads + the 9 newly-PORT-pending tables; (3) bridge overlap-tier reconciliation (`altimate-data-parity` against Core filtered to 3 overlapping tiers). Multi-day to multi-week depending on `mart_master_lead_wide` 1:1-vs-split fork (Open Q 2). Trigger to start: dabi reaches a natural checkpoint OR a `marts.sql` failure incident. Memory anchor: David ran query for 17 tables already; raw data captured in PR #132 thread.
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

- Last regen: 2026-04-30 early morning UTC (post-PR-#133 merge + PR #132 multi-revision audit in flight).
- WORKLOG: skipped — Y3 session output is captured by PR #132 (audit doc, three commits with detailed revision history) + PR #133 (description + commit message documenting the dead-surface retirement) + this regen for the cross-cutting state changes. Per `.claude/rules/worklog.md` routing table: PR descriptions + audit-doc artifact + this regen replace a WORKLOG entry.
- Earlier _meta entries (PR #107/#108 morning, Step 4 deploy, reconciliation sweep, PR #118-#130 evening) — narratives carried by their respective PR descriptions and rule files.
- Recurring drift pattern (caught three times across recent sessions — PR #122, PR #130, then 2026-04-29 Y3 investigation): claiming "live revision" or "X is broken" without verifying via `gcloud run services describe` / `__TABLES__` semantics / etc. Today's specific lesson: VIEWs report `numRows: 0` in `__TABLES__` as a metadata artifact — `SELECT COUNT(*)` against the view is the actual count signal. Future-Claude: when reasoning about Marts table state, query the table directly; do not infer from `__TABLES__` alone.
- Recurring audit-completeness pattern (caught three times in PR #132's review passes): each round of `/ce-doc-review` revealed structural data-flow gaps the prior round missed (R1: cascade dependencies + analyst.py runtime consumer; R2: 3 more cascade refs + path-B HOLD contradiction + bridge scope-difference; R3: David-as-ad-hoc-consumer pattern via `INFORMATION_SCHEMA.JOBS_BY_PROJECT`). Lesson: static-grep audits of analytical pipelines miss human-operator reads; INFO_SCHEMA scan should be the FIRST verification step on any "what is consumed?" question, not the last.
