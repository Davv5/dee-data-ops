# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-29 mid-day (post PR-#118/#119/#120 merges + bq-ingest redeploy)._

## Where we are

- **`bq-ingest` runs revision `bq-ingest-00093-xiv` at 100% traffic.** Deployed 2026-04-29 mid-day from `services/bq-ingest/` after PR #118 + #119 merged; tagged URL `https://pagination-fix---bq-ingest-mjxxki4snq-uc.a.run.app` retained for diagnostic continuity. `/routes` parity check passed byte-equal (22 routes) against prior revision before promotion. Old `bq-ingest-00087-zah` retained at 0% as rollback. **Earlier "fixed end-to-end" claim in the prior regen was premature** — code on main is not the same as live; always redeploy after a behavioral PR (operational-health.md "stale-deploy drift").
- **GHL contacts ingest fixed in code AND live.** PR #118 swapped the `/contacts/search` filter from `gte`+ISO to `gt`+epoch_ms. PR #119 fixed page-2+ pagination via `searchAfter` array cursoring (matches the vendor contract from compete-iq's production client). Live proof-of-life is the next hourly `ghl-hourly-ingest` execution (HH:20 UTC).
- **Fanbasis money columns are NUMERIC in code** (PR #120). `stg_fanbasis__{transactions,refunds}` cast amounts as NUMERIC; `fct_payments` Stripe arm also flipped for UNION-ALL type-symmetry; `release_gate_revenue_detail` thresholds use `numeric '0.05'`/`numeric '0.10'` literals. CI dbt build passed; **takes effect on next prod dbt build** (typically nightly).
- **bq-ingest consolidation Steps 1–4 shipped** (PRs #100/#102/#104 + 2026-04-28 deploy). `dee-data-ops` is the production deploy origin; `gtm-lead-warehouse` is no longer load-bearing.
- **`bq-ingest` requires authenticated invocation.** `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" ...`
- **GHL transition snapshots remain LIVE.** `Core.fct_pipeline_stage_snapshots` compounds daily at 07:00 UTC. First usable transition signal in ~5 days.
- **Phase B layer-build still on main** (PRs #84/#86/#88/#90/#92): Fanbasis staging, identity-contact-payment bridge, payments/refunds rename, revenue_detail net-of-refunds. Local-CI tooling (PRs #94/#95) remains the dev-loop bypass.
- **`ask-corpus` v2 engine** lives on main (PR #74).
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline.
- **Methodology corpus engine v2** is on main (11/13 units shipped, `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md`). **GTM source-port plan paused** (`docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`) — U4a+ resumes when GHL trusted-copy decision lands.
- **Headline metric (locked 2026-04-19):** unchanged.

## Last 3 decisions

- **2026-04-29 mid-day (latest)** — Three PRs shipped closing two long-standing tech-debt threads: PR #118 + #119 (GHL contacts 422 + page-2+ pagination via searchAfter array — full vendor-contract alignment with compete-iq's production pattern), and PR #120 (Fanbasis money → NUMERIC, including Stripe-arm UNION ALL type-symmetry and release-gate threshold hardening). All three landed via the producer + adversarial-reviewer pairing pattern from `.claude/rules/use-data-engineer-agent.md`. Investigation of the `ghl-identity-sync` chained-command thread found it intentional and benign (single GHL API call per 2hr to refresh `custom_field_definitions`); thread closed without code change.
- **2026-04-29 morning** — Merged PRs #107/#108/#110/#111/#112/#113/#114/#115/#116/#117. Redeployed `bq-ingest` to revision `00087-zah`. Closed `/ingest-fathom` timeout via env-var removal. Added `model.ghl` to `run_marts_with_dependencies` (PR #115). Removed legacy `Marts.mrt_speed_to_lead_*` writes from `ghl_models.sql` (PR #117).
- **2026-04-28 late-night** — Reconciliation sweep + `pivot-discipline.md` rule. Closed 7 stale plan-debt artifacts caused by un-walked pivots; new rule requires same-session walk of superseded docs when a pivot memory is saved.

## Open threads

- **`*_RUN_MODELS_AFTER_INCREMENTAL` audit closed.** Both env vars removed from bq-ingest service; PR #115 routes GHL refresh through marts Job context; PR #117 makes `marts.sql` the canonical owner of `mrt_speed_to_lead_*`. The earlier "GHL contacts freshness gap" caveat is now closed by PR #118+#119.
- **Stripe staging `safe_cast` rule violation** (NEW, surfaced from PR #120 review). `stg_stripe__charges.sql:28-30` uses `safe_cast(... as int64)` for amount columns. Per `staging.md` SAFE_CAST should only appear in WHERE clauses. Stripe is historical-only (frozen data) so blast radius is bounded; fix is a small follow-up PR with a careful CI build to surface any silent NULLs that would now hard-fail.
- **dabi NUMERIC rendering verification** (NEW post-PR-#120). First dabi pipeline run after merge will surface whether NUMERIC columns render as expected in dashboards (e.g., trailing-zero noise). If they need polish, a mart-layer `format()` cast is the right place.
- **Multi-page contacts unit test** (NEW post-PR-#119). bq-ingest has no test infra; reviewer recommended a stub-mocked test that returns two pages with `searchAfter` cursors and asserts page-2 body shape. Worth a separate small PR.
- **`pipeline-marts-hourly` operating notes (corrections from PR #115 review).** (a) Timeout is 3600s per `jobs.yaml:623`. (b) `run_marts_with_dependencies` is fail-fast — calendly/fathom/ghl raising aborts marts. (c) Wall-clock cadence shifted: marts Job at `:50` is now the only writer of `mrt_speed_to_lead_daily`. (d) Future verification should record `dependencies['model.ghl']` count to confirm rewrite.
- **Step 5 (optional)** — Cloud Build trigger watching `services/bq-ingest/**`. Reframed as deploy provenance, not stale-clone defense.
- **Step 6 (after a few days of clean operation)** — archive `heidyforero1/gtm-lead-warehouse` + delete stale local clones at `~/Documents/{fanbasis-ingest,gtm,gtm-lead-warehouse}`.
- **bq-ingest pre-existing deferred follow-ups (per audit §"Deferred follow-ups"):** Cloud Run Jobs image rebuild path, `1-raw-landing/` consolidation, secret hygiene (pin all to versions, rename `Secret`), orphan SQL audit (5 spec-only files), services/ polyrepo precedent.
- **Snapshot architecture follow-ups:** (a) extend `Core.fct_ghl_opportunities` upstream to surface `assigned_to_user_id`; (b) once `Core.fct_pipeline_stage_snapshots` has ≥2 daily partitions, swap GHL freshness signal from `MAX(_ingested_at)` to `MAX(snapshot_date)`.
- **Calendly drain follow-ups (deferred from PR #110 review):** (a) advisory lock on concurrent drain Jobs; (b) `mode` column on `calendly_backfill_state`; (c) FAILED runs are silently abandoned by `_find_drainable_run_id`.
- **Vestigial v2-cutover artifacts** — Cloud Run Jobs (`ghl-incremental-v2`, `calendly-incremental-v2`, `ghl-backfill-v2`) not invoked by active schedulers; produce false signals. Plus shell scripts that reference them: `run_canary_parity_check.sh`, `cutover_schedulers_to_v2.sh`. Delete or label as a single cleanup pass.
- **Pre-existing stale PRs in dee-data-ops:** #50 + #44 (Metabase, both predate dabi pivot). Close or evaluate.
- **Mart cleanup post-#123** — `sales_activity_detail.sql:127-156` and `lead_journey.sql:77-95` use divergent opp-selection rules ("latest by created_at" with broken time filter; "latest by updated_at"). PR #123 picked one canonical rule at the fact layer (active opp at booking time + `opportunity_id desc` tiebreaker). Follow-up PR should collapse the marts to consume `fct_calls_booked.{assigned_user_sk,pipeline_stage_sk}` directly, retiring the inline selection logic in both marts.
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
- **bq-ingest production:** Cloud Run service `bq-ingest`, revision `bq-ingest-00093-xiv` (deployed 2026-04-29 mid-day, includes PR #118 + #119). URL: `https://bq-ingest-mjxxki4snq-uc.a.run.app`. Tagged URL `https://pagination-fix---bq-ingest-mjxxki4snq-uc.a.run.app` resolves to the same revision.
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

- Last regen: 2026-04-29 mid-day (post PR-#118/#119/#120 merges).
- WORKLOG: skipped — all session output captured by the three PR descriptions (#118, #119, #120) and the worktree-cleanup investigation log lives in `/Users/david/Documents/data ops/.tmp/worktree-salvage-2026-04-29/`. Per `.claude/rules/worklog.md` routing table: PR descriptions capture shipped work, this regen captures present state; both replace a WORKLOG entry.
- Earlier _meta entries (PR #107/#108 morning, Step 4 deploy, reconciliation sweep) — narratives carried by their respective PR descriptions and rule files.
