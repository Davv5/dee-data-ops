# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-05-01 mid-day UTC — **PR #146 merged + verified end-to-end.** Ayaan Menon promoted `unknown`→`SDR` + `Marts.rpt_rep_scorecard_week.rep_weeks` UNION expanded from 2 arms to 5 (added `rpt_call_to_booking_rate_week`, `fct_speed_to_lead`, `rpt_setter_unbooked_conversion_week` + regex guard) + `rep_role` CASE extended with SDR-signal branch. **6 SDRs newly visible** in scorecard (Ayaan + Aariz/Blagoj/Boipelo/Marco/Mitchell/Stanley); Aariz reportable per-rep for the first time at 2.4% pct_within_sla, Blagoj leads at 9.4%. **`Marts.lead_journey.assigned_sdr_name` 4 → 5 distinct SDRs** (+128 contacts attributed to Ayaan). Ayaan's 160 fct_speed_to_lead events now 100% attributed (was 0/160). Headline metric invariance verified: org-wide `pct_within_5m` = 1.06 → 1.06._

## Where we are

- **`bq-ingest` live revision is `00089-w7c`** as of 2026-05-01 13:18 UTC (PR #146 deploy). Ships marts.sql with: (1) Ayaan in `ghl_user_name_map` (line 1583) + `static_ghl_user_seed` (line 2776); (2) `rep_weeks` UNION expanded to 5 arms (lines 5743-5783); (3) `rep_role` CASE extended with SDR-signal branch (line 5870). Cloud Run Jobs runtime stack rebuilt: 15 jobs redeployed with `fanbasis-python-runner:latest`. Rollback: `update-traffic --to-revisions=bq-ingest-00088-tsd=100`.
- **`Marts.rpt_rep_scorecard_week` post-PR-#146:** 16 distinct reps × 211 week-rep pairs (was 9 × 69). Label distribution: 9 `closer` (preserved), 8 `setter` (6 new SDRs + Ayaan + Houssam in 2 weeks where he made outbound activity but no Fathom won-deal — correct per-week semantic). 0 `setter+closer` (af arm contributes 0 distinct names today). 0 regex-guard leakage.
- **dbt prod auth runs via Workload Identity Federation** (PR #126). Workflows authenticate to `sa-transform@project-41542e21-470f-4589-96d` via OIDC; pool `github-actions`, provider `github-oidc`. The `GCP_SA_KEY_PROD` secret is dormant.
- **dbt prod schemas are STG / Core / Marts** (capitalized — Y1 cutover, PR #129). dbt-built tables coexist alongside bq-ingest's inline-SQL writers in those datasets (different table names, no conflict).
- **GHL sources read from `raw_ghl_v2` views** (PR #128). Per-entity views over `Raw.ghl_objects_raw` filtered by `entity_type` and aliasing `ingested_at AS _ingested_at`.
- **`fct_calls_booked.{assigned_user_sk,pipeline_stage_sk,booking_time_opportunity_id}` are wired and flowing** (PRs #123/#135/#138). `assigned_user_sk` non-NULL = 10/5,487 (0.18%); 99.82% NULL is legitimate LEFT JOIN orphan case (historical/deleted users not in `/users/search`).
- **Cloud project provenance map added 2026-05-01:** `docs/discovery/cloud-project-provenance-map.md` labels `project-41542e21-470f-4589-96d` as current, `dee-data-ops-prod` as legacy prod/rollback, and `dee-data-ops` as legacy dev/raw. Local `gcloud` default still points at `dee-data-ops-prod`; always pass `--project` / `--project_id` explicitly.
- **Duplicate data audit added 2026-05-01:** `docs/discovery/duplicate-data-audit-2026-05-01.md` confirms current project is active, legacy prod marts/warehouse are parity archives, legacy `dee-data-ops.raw_*` is large raw-history candidate data, and `dee-data-ops-prod` legacy jobs are still scheduled. Do not delete or pause legacy assets before source-by-source comparison.
- **Legacy runtime audit added 2026-05-01:** `docs/discovery/legacy-runtime-audit-2026-05-01.md` classifies `dee-data-ops-prod` jobs. `ghl-hot` writes legacy messages/conversations and should stay temporarily; `ghl-cold` is a pause candidate after ID comparison; `calendly-poll` is highest-priority pause/migration candidate because it is every-minute and showing quota/timeouts. No legacy jobs were changed.
- **Data-layer reset map added 2026-05-01:** `docs/discovery/current-data-layer-truth-map.md` is now the first read before mart/dashboard work. It corrects stale guidance: dbt `speed_to_lead_detail` and `sales_activity_detail` were deleted in PR #142; Fanbasis staging/facts now exist; dashboard v1 may consume bq-ingest Speed-to-Lead report tables temporarily, but dbt remains the durable data layer.
- **`bq-ingest` requires authenticated invocation.** `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" ...`
- **GHL transition snapshots remain LIVE.** `Core.fct_pipeline_stage_snapshots` compounds daily at 07:00 UTC.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline.
- **Headline metric (locked 2026-04-19):** unchanged. Org-wide `pct_within_5m` invariance verified post-deploy: 1.06 → 1.06.

## Last 3 decisions

- **2026-05-01 mid-day (latest)** — **PR #146 merged + deployed: Ayaan SDR promotion + rep_weeks UNION expansion.** Single PR coupled two changes after pre-merge adversarial review found the seed-only fix wouldn't surface Ayaan in `rpt_rep_scorecard_week` (root cause: `rpt_appt_funnel_week.setter_name` resolves via Calendly slug → email, only 4 closer slugs hardcoded, no SDR resolution path). Diff: 46 add / 3 del across `2-dbt/seeds/ghl_sdr_roster.csv` + `services/bq-ingest/sql/marts.sql`. CE adversarial rounds 1+2 (round 1 caught the P1 `rep_weeks` gap → user chose Option B; round 2 surfaced 7 P2 advisories), altimate-sql-review 1 MEDIUM (REGEXP_CONTAINS function-on-column, intentional defense, accepted). Deploy chain: dbt-deploy auto-fired on merge → bq-ingest service deploy `00088-tsd` → `00089-w7c` (manual) → Cloud Run Jobs runtime stack rebuild (15 jobs) → `pipeline-marts-hourly-wq4mp` execution. Verified end-to-end via 9 verification queries against `Marts.*`: 7/7 expected SDRs surface as `setter`, 0 regex-guard leakage, 0 closer-label regressions, headline metric invariance holds.
- **2026-04-30 late evening** — **Closed Bug #1 (Aariz seed swap) + Bug #2 (fct_outreach cascade) end-to-end** via PRs #141/#142/#143/#144. SDR-attribution chain fully repaired.
- **2026-04-30 late afternoon** — **Closed the `assigned_user_sk` 100% NULL arc end-to-end** via PRs #135/#136/#137/#138 + bq-ingest deploys `00099-jez`/`00101-xas`/`00103-rav`. Code fix for `/users/search` (companyId param + no-page param + explicit limit=100). PR-2 of mart-collapse remains blocked on David answering Q3.1/Q3.2/Q3.3 in plan §8.

## Open threads

- ~~**🟡 Ayaan Menon numerator decision**~~ — RESOLVED 2026-05-01 via PR #146 (Option A + Option B coupled). Verified post-deploy: Ayaan's 160 fct_speed_to_lead events 100% attributed; `lead_journey.assigned_sdr_name` 4 → 5 distinct SDRs (+128 contacts); `rpt_rep_scorecard_week` 9 → 16 distinct reps with all 7 expected SDRs as `rep_role='setter'`.
- **🟡 `rpt_appt_funnel_week.setter_name` 100% 'unknown' in production** — 690 rows, 0 resolved. Slug → email → `dim_team_members` chain at `marts.sql:5604-5607` is silently broken upstream (`slug_to_email` has only 4 closer slugs hardcoded; LEFT JOIN to dim_team_members.email isn't matching). The PR #146 sp arm partially compensates (cosmetically restores rep coverage on the dashboard), masking the upstream regression. Independent of this PR; needs root-cause investigation.
- **🟡 `rpt_call_to_booking_rate_week` + `rpt_setter_unbooked_conversion_week` produce 0 rows in production** — separate pipeline issue surfaced by PR #146 round-2 review. Likely cause: contact_id NULL or `Core.fct_ghl_outbound_calls`/`fct_ghl_conversations` missing data for the relevant grain. Net effect today: `total_dials` column in `rpt_rep_scorecard_week` is 0 for everyone; dial counts surface in `Marts.lead_journey.number_of_dials` instead (aariz=1,417, Blagoj=908, Marco=674, Boipelo=438).
- **🟡 TODO(rep-role-cleanup) at marts.sql:5863** — when the af arm comes back online (per #1 above), existing CASE branches 1/2 will silently re-label Houssam/Jordan/Kevin/Jake from `'closer'` → `'setter+closer'`/`'setter'`. Time-bomb queued by PR #146; recommend creating a GitHub issue NOW so the regression has a tracking surface.
- **🟡 Cross-tree seed drift** — dbt seed has 16 users; bq-ingest hardcoded seeds have 14 (post-PR #146). No automated parity test. Bug #1's lesson called for `unique(team_member_key)` + `relationships()` tests on these seeds; still not built. Blocker: `Marts.dim_team_members` is a `CREATE OR REPLACE TABLE` in `marts.sql:2643`, not `ref()`-able from dbt — needs a `sources.yml` block first.
- **🟡 Upstream COALESCE inconsistency at marts.sql:5386** — `rpt_setter_unbooked_conversion_week.setter_name` uniquely uses triple-fallback `COALESCE(tm.display_name, cw.owner_id, 'unknown')`. Other reports use `COALESCE(tm.display_name, 'unknown')`. The regex guard in PR #146 (marts.sql:5778) is downstream patch; deleting `cw.owner_id, ` from line 5386 obviates the regex (~3-line follow-up PR).
- **🟡 Case-mismatch in rep_name** — `'aariz menon'` lowercase vs `'Ayaan Menon'` Title Case coexist in `Marts.rpt_rep_scorecard_week.rep_name` post-PR-#146. Forward join hazard if any future model joins on rep_name case-sensitively. Soft pin (GHL-verbatim casing) is intentional per `marts.sql:1565-1568` + `2748-2751` and PR #141 anchor.
- **🟡 Pivot-debt: Metabase authoring tree (`3-bi/metabase/`) references retired marts.** Surfaced 2026-04-30 by adversarial review of PR #142. `3-bi/metabase/authoring/dashboards/speed_to_lead.py` has 8 hardcoded `FROM dee-data-ops-prod.marts.speed_to_lead_detail` references. Recommended: banner-archive the entire `3-bi/metabase/` tree per `.claude/rules/pivot-discipline.md`.
- **🟡 GHL users `includeDeleted=true` for historical SDR attribution.** `assigned_user_sk` 99.82% NULL on `fct_calls_booked` is mostly historical bookings whose user_id isn't returned by `/users/search`. Decision deferred until a downstream consumer surfaces the need.
- **🟡 Decommission `GCP_SA_KEY_PROD` secret + revoke `dbt-prod@dee-data-ops-prod` keys** — secret is dormant post-WIF cutover. `gh secret delete GCP_SA_KEY_PROD` (irreversible).
- **🟡 Drop orphan lowercase `staging`/`warehouse`/`snapshots` datasets** — partial materialization from PR #128 deploy. Drop with `bq rm -r --dataset` once confirmed nothing reads from lowercase names.
- **Stripe staging `safe_cast` rule violation** (PR #120 review). `stg_stripe__charges.sql:28-30` uses `safe_cast(... as int64)` for amount columns. Per `staging.md`, SAFE_CAST should only appear in WHERE clauses. Stripe is historical-only; small follow-up PR.
- ~~**dabi NUMERIC rendering verification**~~ — superseded by the click-around dashboard product plan (`docs/plans/2026-05-01-001-feat-dashboard-product-plan.md`).
- **Multi-page contacts unit test** (post-PRs #119/#127). bq-ingest has no test infra; reviewer recommended a stub-mocked test for `searchAfter` cursor + `operator: range` filter shape.
- **🟡 Mart-collapse plan stale after PR #142.** `docs/plans/2026-04-30-mart-collapse-fct-sks-plan.md` targets dbt `sales_activity_detail`, which no longer exists on `main`. Do not execute PR-2 from that plan without rewriting it against current code.
- **Y3 audit landed (PR #132)** — `marts.sql` retirement scope defined: 22 confirmed-orphan + 5 cascade-hold + 1 parity-gated + 2 superseded + 15 PORT. Blocked on Batch 0 verification + David disposition on ~26 tables.
- **2 GHL opportunities** (out of thousands) have Ayaan as `$.assignedTo` — `Marts.mart_master_lead_wide.closer_name` flipped from `'owner_id:eWA0YcbNP3rklPwRFFwM'` → `'Ayaan Menon'` for those rows post-PR-#146. Trivial scope but real role-conflation (SDR Ayaan now appears as `closer_name` for those 2 opps).
- **`pipeline-marts-hourly` operating notes** (PR #115 review). Timeout 3600s; `run_marts_with_dependencies` is fail-fast; marts Job at `:50` is the only writer of `mrt_speed_to_lead_daily` (post-#117).
- **Step 5 (optional)** — Cloud Build trigger watching `services/bq-ingest/**`. Deploy provenance.
- **Step 6** — archive `heidyforero1/gtm-lead-warehouse` + delete stale local clones at `~/Documents/{fanbasis-ingest,gtm,gtm-lead-warehouse}`.
- **bq-ingest pre-existing deferred follow-ups:** Cloud Run Jobs image rebuild path, `1-raw-landing/` consolidation, secret hygiene, orphan SQL audit, services/ polyrepo precedent.
- **Snapshot architecture follow-ups:** (a) extend `Core.fct_ghl_opportunities` upstream to surface `assigned_to_user_id`; (b) once `Core.fct_pipeline_stage_snapshots` has ≥2 daily partitions, swap GHL freshness signal from `MAX(_ingested_at)` to `MAX(snapshot_date)`.
- **Calendly drain follow-ups (PR #110 review):** advisory lock; `mode` column on `calendly_backfill_state`; FAILED-run handling.
- **Vestigial v2-cutover artifacts** — `ghl-incremental-v2`, `calendly-incremental-v2`, `ghl-backfill-v2` Jobs + scripts. Delete or label.
- **Pre-existing stale PRs:** #50 + #44 (Metabase, both predate dabi pivot). Close or evaluate.
- **GHL trusted-copy decision** — single named blocker for several Tier B / refresh marts.
- **Fathom → GHL contact join key** — attendee email reliability. Affects future `fct_calls_held`.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` + `QUALITY_PARITY_FLOOR=0.6` ship provisional.
- **Strategic-Reset-paused threads:** Typeform `form_id` upstream gap, GHL `messages`/`tasks` 0-row upstream (GHL `users` resolved 2026-04-30 via PR #138), Fathom transcript landing, Stripe staleness.
- **Week-0 client asks** — unchanged.

## Where to look

- **bq-ingest production:** Cloud Run service `bq-ingest`, revision `bq-ingest-00089-w7c` (deployed 2026-05-01 13:18 UTC; PR #146 marts.sql with rep_weeks expansion). URL: `https://bq-ingest-535993952532.us-central1.run.app`. Rollback: `bq-ingest-00088-tsd` (Bug #1 fix, prior latest); `bq-ingest-00103-rav` (PR #138 era).
- **Service deploy command (durable):** `gcloud run deploy bq-ingest --source services/bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d --memory=1024Mi`.
- **Cloud Run Jobs runtime stack deploy (separate image):** `cd services/bq-ingest && ops/scripts/deploy_runtime_stack.sh` rebuilds `fanbasis-python-runner:latest` AND updates 15 Jobs from `ops/cloud/jobs.yaml`. Required for marts.sql to land in `pipeline-marts-hourly`.
- **WIF setup:** Pool `github-actions`, provider `github-oidc` in project `project-41542e21-470f-4589-96d`. SA `sa-transform@project-41542e21-470f-4589-96d`.
- **Y1 schema cutover:** dbt prod targets `STG`/`Core`/`Marts` (capitalized) datasets. Snapshots → `Core`. Seeds → `STG`.
- **`raw_ghl_v2` views:** Per-entity views over `Raw.ghl_objects_raw`; created out-of-band 2026-04-29.
- **bq-ingest dependency audit:** `docs/discovery/bq-ingest-dependency-audit.md`
- **Operational-health rule:** `.claude/rules/operational-health.md`
- **bq-ingest service rules:** `.claude/rules/bq-ingest.md`
- **Live snapshot table:** `project-41542e21-470f-4589-96d.Core.fct_pipeline_stage_snapshots`, partitioned by `snapshot_date`.
- **Current data-layer truth map:** `docs/discovery/current-data-layer-truth-map.md` — first read before marts/dashboard work.
- **Cloud project provenance map:** `docs/discovery/cloud-project-provenance-map.md` — first read before GCP/BigQuery/Cloud Run/Scheduler work.
- **Duplicate data audit:** `docs/discovery/duplicate-data-audit-2026-05-01.md` — read before proposing migration/deletion/pausing of legacy projects.
- **Legacy runtime audit:** `docs/discovery/legacy-runtime-audit-2026-05-01.md` — read before touching `ghl-hot`, `ghl-cold`, or `calendly-poll`.
- **Canonical roadmap:** `docs/discovery/gold-layer-roadmap.md` (stale in places; refresh against the truth map before executing)
- **Phase A → B ADR:** `docs/decisions/2026-04-27-phase-a-to-b-transition.md`
- **Mart architecture commitment:** `docs/discovery/coverage-matrix.md` + `.claude/rules/mart-naming.md` Rule 2
- **Phase B artifacts (on main):** `2-dbt/models/staging/fanbasis/`, `2-dbt/models/warehouse/{facts,bridges}/`, `2-dbt/models/marts/{lead_journey,revenue_detail}.sql`. dbt `sales_activity_detail` and `speed_to_lead_detail` were retired in PR #142; Speed-to-Lead v1 currently consumes bq-ingest `Marts.*` report tables.
- **Local dev loop:** `2-dbt/scripts/local-ci.sh` + `2-dbt/profiles.yml` (`dev_local` / `ci_local`).
- **Data-engineer agent + LAW skills:** `~/.claude/agents/data-engineer.md`. Pairing rule: `.claude/rules/use-data-engineer-agent.md`.
- **Corpus engine v2:** `.claude/skills/ask-corpus/scripts/` + `SKILL.md`
- **Fast operating loop:** `docs/runbooks/operator-fast-loop.md`
- **Memories (auto-loaded):** see `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `.claude/rules/*.md`
- **Routing rule for end-of-session:** `.claude/rules/worklog.md`
- **Pivot-discipline rule:** `.claude/rules/pivot-discipline.md`
- **Full history:** `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-05-01 mid-day UTC (PR #146 merged + verified end-to-end; bq-ingest service deploy `00088-tsd` → `00089-w7c` + Cloud Run Jobs runtime stack rebuild + `pipeline-marts-hourly-wq4mp` execution + 9 verification queries against `Marts.*`).
- WORKLOG: skipped — session output fully captured by PR #146 description (commit `fb20c66`), CLAUDE.local.md roster-gaps update, and this regen. Per `.claude/rules/worklog.md` routing table: PR description + CLAUDE.local.md edit + project-state regen replace a WORKLOG entry.
- New empirical anchor (2026-05-01 PR #146 verification): **`rep_role` is per-rep-per-week granularity, not per-person.** Houssam Bentouati appears as `'closer'` in 10 weeks AND `'setter'` in 2 weeks of `Marts.rpt_rep_scorecard_week` post-PR-#146 because the new branch 4 of the rep_role CASE fires when a rep has `cb`/`sp`/`ub` signal but no `af`/`cl` signal in that week. This is correct per-week semantic (closers occasionally also do outbound work in some weeks), not a bug. Pre-deploy prediction (15 reps + 6 setters) was off by +1 rep + +2 setter labels because (a) Ayaan's name only resolves post-`dim_team_members` refresh which is post-deploy; (b) Houssam's per-week granularity surfaces in both buckets. Future-Claude reviewing rep_scorecard outputs should not assume one-row-per-person; it's one-row-per-(person, week, role).
- New empirical anchor (2026-05-01 PR #146 deploy chain): the bq-ingest `pipeline-marts-hourly` Cloud Run Job uses a **separate image** (`fanbasis-python-runner:latest`) from the `bq-ingest` Cloud Run Service. Service deploy via `gcloud run deploy bq-ingest --source services/bq-ingest` updates ONLY the service; Cloud Run Jobs require `cd services/bq-ingest && ops/scripts/deploy_runtime_stack.sh` to ALSO rebuild fanbasis-python-runner AND re-deploy all 15 jobs from `ops/cloud/jobs.yaml`. **Future-Claude shipping marts.sql changes via the bq-ingest service deploy MUST also run the runtime stack script** — otherwise `pipeline-marts-hourly` continues to use the old image with the old marts.sql. Auto-deploy on merge ships marts.sql to the SERVICE only, not the JOB; the JOB image needs the manual rebuild script.
- Recurring drift pattern (caught five times now): claiming "live revision" or "X is broken/wired" without verifying via `gcloud run services describe` / row-count queries. Today's catch: project-state header claimed `bq-ingest live revision is 00103-rav` but actual was `00088-tsd` (Bug #1 fix overwrote it without state regen). Verify revision before assuming.
- Earlier _meta entries — narratives carried by their respective PR descriptions and rule files.
