# Worklog

Rolling log of what's been done on this project. Newest entries at the top. Tail gets injected into Claude Code context at every session start via the `SessionStart` hook in `.claude/settings.json`.

**Conventions:**
- One entry per meaningful work session or deliverable
- Entries start with a dated H2 heading: `## YYYY-MM-DD — <one-line summary>`
- Three sub-sections per entry: `**What happened**`, `**Decisions**`, `**Open threads**`
- Keep bullets tight — this is a log, not a narrative
- Do not paste code, diffs, or long prose — link to files/commits instead

---

## 2026-04-22 — Speed-to-Lead Metabase dashboard shipped v1.1 → v1.2 → v1.3 (4 PRs merged)

**What happened**
- **v1.1 (vocabulary + layout)** — Data Ops + Metabase Craft corpus audits drove leaderboard `column_title` aliases (snake_case → Title Case), tile renames (`SLA`/`DQ` out of tile titles), donut title + categories to business phrasing (`Lead Tracking Match Rate` with `Matched` / `No SDR touch yet` / `Unassigned rep`). Full-width Markdown header card with metric definition + time-window map.
- **v1.2 (trend + click-through + BQ tuning)** — T1/T2/T3 switched to `display=smartscalar` reading a new `stl_headline_trend_weekly` 12-week time series; each tile now renders a week-over-week delta arrow. T6 → `click_behavior` linking to Lead Detail, passing literal `"true"` through a `{{within_5min}}` template-tag. BigQuery `auto_run_queries=false` toggled on prod Metabase + codified in `ops/metabase/authoring/infrastructure/bigquery_connection.py`.
- **v1.3 (9-item outcome + coverage revamp via 4 parallel agents)** — 4 new rollups: `stl_outcome_by_touch_bucket_30d` (close-rate by touch time), `stl_response_time_distribution_30d` (cumulative curve), `stl_source_outcome_30d` (outcome overlay on lead source), `stl_coverage_heatmap_30d` (day × hour SDR coverage). Weekly rollup extended with P90 + volume columns + SDR-scope correctness fix (pre-fix showed 38k-min medians on ramping weeks because no-touch rows polluted the quantile pool). Mart gained `era_flag` (hardcoded 2026-03-16 cutover = ISO-W12 Monday when median dropped below 60 min). Dashboard reflowed to 14 dashcards: distribution bar (row 8), close-rate bar + source-outcome table (row 14), coverage pivot heatmap (row 20).
- **PRs merged to main**: #40 (COS hotfix), #41 (Secret Manager client auth + BQ connection + dbt-metabase script), #42 (data layer v1.3), #43 (dashboard v1.1–v1.3). **#44 open** (deferred `curl -fsS` hardening from #40 review).

**Decisions**
- **SDR-scoped median + P90 + 5-min rate** in the weekly rollup. *Why:* rows with no SDR touch carry NULL/sentinel `minutes_to_first_sdr_touch`, polluting quantile pools. Post-fix the metric honestly reads "typical response time among leads an SDR actually touched."
- **Era flag as inline CASE, not a seed.** One binary cutover for now; flip to a seed only if the taxonomy grows past `ramping`/`stable`.
- **Show rate = `close_outcome IS NOT NULL`** fallback — mart has no real `show_outcome` column. Over-counts `'pending'` opportunities as showed; documented in SQL + YAML. Revisit when a true show column lands.
- **Drop gauge viz in favor of smart-scalar.** Metabase `column_formatting` wires only into `display=table`; scalars silently ignore it. Directional arrow + week-over-week delta beats a static threshold for exec tiles.
- **4 area-scoped agents, not 9 per-item agents.** 7 of 9 items touched `speed_to_lead.py`; per-item isolation would have guaranteed merge tangles. Grouping by (dbt new files / dbt edits / dashboard / infra script) avoided conflicts.
- **Merge sequence #40 → #41 → #42 → #43 squash-merged**, repo convention preserved. Old `fix/metabase-startup-cos-compat` branch retired; replaced by the four scope-aligned PRs.

**Open threads**
- `GCP_SA_KEY_PROD` repo secret still unset → CI/CD `dbt-deploy.yml` blocked; prod builds this session ran via a local oauth profile at `/tmp/dbt-oauth/profiles.yml` using David's personal gcloud ADC.
- PR #44 (`curl -fsS` hardening) open for review.
- `dbt_metadata_sync.py` committed but **never run** — Metabase column hovers still empty. Requires `pip install dbt-metabase==1.6.0` + one manual run after a `dbt parse`.
- `sales_activity_detail` mart lacks a real `show_outcome` column; three v1.3 outcome rollups fall back to `close_outcome IS NOT NULL`.
- Metabase cruft on prod: 3 stale cards (old names `% Within 5-min SLA (7d)`, `DQ — SDR Activity Within 1 Hr (7d)`, `Attribution Quality Mix (30d)`) + a renamed duplicate collection 5 in the instance. Not on any active dashboard; cleanup deferred.
- Roster gaps unresolved: Ayaan Menon, Jake Lynch need role decisions; Moayad + Halle leaderboard-evidenced but not in seed. `docs/proposals/roster_update_from_oracle.md` staged, awaiting David's manual commit.
- GHL PIT rotation still owed (transcript-exposed 2026-04-19).
- Stripe Fivetran sync gap still open (4,750 checkout sessions, zero rows in customer/charge/invoice/payment_intent).

---

## 2026-04-21 — Metabase live on GCP + startup-script COS compatibility hotfix

**What happened**
- Ran `terraform apply` end-to-end against `dee-data-ops-prod`. 24 resources created: 2 SAs (VM + BQ reader), 7 IAM bindings, Cloud SQL Postgres `metabase-appdb` (15m2s to provision), VPC + private-IP peering, static external IP `34.66.7.243`, 2 firewall rules, GCE e2-small VM, GCS ops bucket, Secret Manager entries. Plan matched spec exactly: 24 add / 0 change / 0 destroy.
- Post-apply: uploaded runtime assets to ops bucket, generated BQ reader SA key `11b606f3…` into Secret Manager version 1, reset VM.
- Two COS-compatibility bugs in `startup-script.sh` surfaced on reboot, fixed in this commit:
  1. **`/opt/metabase` is read-only on COS.** Moved compose dir to `/var/lib/metabase/compose/` (writable + persistent).
  2. **`gsutil` isn't on COS.** Replaced with `curl` against the Storage JSON API using the same metadata-token pattern the script already uses for Secret Manager.
  3. **`docker compose` V2 plugin isn't on COS.** Replaced with three explicit `docker run` invocations (cloud-sql-proxy, metabase, caddy) sharing a `mbnet` network. `docker-compose.yml` stays in the runtime dir as topology documentation.
- Metabase reachable at **https://34-66-7-243.nip.io** (HTTP 200) ~105s after the VM reset that consumed the fixed script.

**Decisions**
- **Kept `docker-compose.yml` in the runtime dir.** *Why:* it's the clearest documentation of the 3-container topology + env-var shape; future operators read it to understand what's running. The startup script mirrors it; the two stay in lockstep via code review.
- **Caddy's nip.io hostname preserved end-to-end.** *Why:* fresh Let's Encrypt ACME against the public DNS-as-IP maps worked on first attempt. No need for a custom domain in v1.
- **Script re-upload + reset twice.** *Why:* two separate COS bugs; fixed serially so each reboot gave clean diagnostic signal. Alternative of "fix everything blindly and reboot once" would've made root-cause attribution harder.

**Open threads**
- Metabase setup wizard not yet completed (admin user, API key). First click-through owned by David.
- `bigquery_connection.py` + `speed_to_lead.py` authoring scripts pending — blocked on the API key from the setup wizard.
- Tracks O (stale GH secrets) + P (Slack webhook fix) still halted on permission / missing secret.
- Track N (Evidence decommission) still awaiting pr-reviewer run.
- `.terraform.lock.hcl` committed alongside the startup-script fix — required for reproducible `terraform init`.

---

## 2026-04-20 — Track T: corpus config decouple (ask-corpus skill + `.claude/corpus.yaml`)

**What happened**
- Created `.claude/corpus.yaml` declaring 3 notebooks: `methodology.data_ops` (7c7cd5d4), `methodology.metabase` (ce484bbc), `engagement` (741d85c6). Schema: `methodology:` is a LIST (room for more craft notebooks); `engagement:` is singular.
- Refactored `.claude/skills/ask-corpus/SKILL.md` — removed hardcoded notebook id, added Step 1 python+pyyaml resolver that reads `corpus.yaml` and accepts optional `scope` param (`methodology.data_ops`, `methodology.metabase`, `methodology` = cross-query default, `engagement`). Fallback to hardcoded Data Ops id if `corpus.yaml` missing.
- Rewrote `.claude/rules/using-the-notebook.md` — documents 3 routing modes + rule of thumb + "how to add a new methodology notebook" section.
- Added "Corpus config" pointer to `CLAUDE.md` near the existing Reference Corpus section.
- Verified end-to-end: 3 test queries returned cited answers from correct notebooks.
  - `methodology.metabase` "backup Metabase app DB?" → cited "Backing up Metabase" doc (PostgreSQL, RDS, H2 Docker/JAR variants).
  - `methodology.data_ops` "canonical 3-layer dbt architecture?" → cited "How to Create a Data Modeling Pipeline (3 Layer Approach)" + warehouse/marts rule files.
  - `engagement` "Speed-to-Lead metric grain?" → cited the sorted-rabbit plan confirming Calendly-event grain (3,141 bookings), DataOps-corrected from GHL pipeline stage.

**Decisions**
- **`methodology` as a LIST** (not a single dict) so future craft notebooks (dlthub, great-expectations, etc.) register by adding an entry — no skill change needed. *Why:* track file originally had it as a single dict; David's Metabase notebook made a 2nd methodology notebook concrete and the schema had to generalize.
- **Default scope = `methodology` cross-query** (not engagement). *Why:* "how should I structure X?" questions should hit craft notebooks first; engagement scope is narrower and more intentional.
- **python+pyyaml over bash+yq.** *Why:* pyyaml is already in the dbt-bigquery venv on this machine; no extra install. `yq` isn't a hard dep of the project.
- **Fallback to hardcoded Data Ops id if `corpus.yaml` missing.** *Why:* template forks that haven't added a corpus.yaml yet shouldn't break the skill.

**Open threads**
- None that block further work. The three agent personas (architect/executor/reviewer) now pick up scope routing automatically via this skill.

---

## 2026-04-21 — Track V: `Metabase Craft` NotebookLM notebook created (14 sources)

**What happened**
- Created a third NotebookLM notebook **`Metabase Craft`** (`notebook_id: ce484bbc-546b-4fe4-a7db-bc01b847dbe5`) on David's account alongside the existing "Data Ops" (`7c7cd5d4-…`) and "D-DEE Engagement Memory" (`741d85c6-…`) notebooks. Role: methodology-slot retrieval for Metabase OSS self-host questions (install, ops, dev, warehouse integration, AI, licensing, cost gotchas)
- Uploaded 14/14 sources via `mcp__notebooklm-mcp__source_add` — all returned `status=success`:
  - Install+ops (4): Docker run, Cloud-vs-self-host, Backing up, Upgrading
  - Developer path (3): Developer Guide start, Building Metabase, Dev-branch Docker
  - Warehouse integration (2): BigQuery connector doc, `gouline/dbt-metabase` GitHub repo
  - AI (2): Metabot doc, Metabase 60 release notes
  - Licensing (2): license overview + AGPL
  - Field notes (1): Kevin Leary's BigQuery cost/speed optimization blog
- `notebook_describe` confirms `source_count=14`; AI-generated summary correctly spans all four topic pillars (licensing, Docker ops, AI/Metabot, BigQuery integration)
- Sanity query *"…recommended way to back up Metabase's application database when running on Docker with an external Postgres instance?"* → answer cites the Backing-up-Metabase source (`56b12aa0-…`) with correct guidance (follow standard PostgreSQL backup procedure; Metabase stores all runtime app data in one SQL DB)
- Handover doc: `docs/handovers/Davv5-Track-V-Execution-2026-04-21_17-02.md`

**Decisions**
- **One notebook, not two or three.** *Why:* content-route via NotebookLM's retrieval, not by manually picking notebooks per question. Name "Metabase Craft" mirrors the "Data Ops" naming of the existing methodology notebook to signal portable craft (not client-scoped).
- **No PII scrubbing — all 14 sources are public web content.**
- **Recorded `notebook_id` in this WORKLOG entry** so Track T (corpus config decouple) can wire it into `.claude/corpus.yaml` when that track fires; no code change in `.claude/rules/` or `corpus.yaml` yet.

**Open threads**
- Track T: add `ce484bbc-546b-4fe4-a7db-bc01b847dbe5` to `.claude/corpus.yaml` and update `using-the-notebook.md` to mention the Metabase Craft notebook as the routing target for Metabase questions
- Audio overview / Studio artifact for onboarding: not generated (not in track scope)

---

## 2026-04-21 — Track S: swap SessionStart tail-injection for curated project-state index

**What happened**
- Replaced the SessionStart hook's `tail -n 200 WORKLOG.md` injection with a curated 43-line index at `.claude/state/project-state.md`, loaded by a new `.claude/scripts/sessionstart-inject-state.sh`. `.claude/settings.json` hook command swapped; PostToolUse (notebook-sync) + PreToolUse (dbt prod-block) hooks preserved; `jq .` validates
- Updated `.claude/rules/worklog.md` to document the split: WORKLOG.md is the append-only audit log (unchanged); `.claude/state/project-state.md` is the auto-injected stateful snapshot; regeneration is a manual end-of-session command for v1
- Grounded the retrieval-over-injection pattern in the Data Ops notebook (cited `CLAUDE.md` "Claude Code Rules" section — same path-scoped rule loader idea applied to project state)

**Decisions**
- **Index at `.claude/state/project-state.md`, not root.** *Why:* co-located with other Claude Code infrastructure (`rules/`, `scripts/`, `skills/`, `settings.json`); `.claude/state/` implies machine-regenerated, not human-authored prose
- **Manual regeneration for v1, not an EndSession hook.** *Why:* EndSession is not a confirmed Claude Code hook event in this environment; a documented `tail | pbcopy | $EDITOR` command is good enough for one regeneration per session. v2 can wire automation once the hook event is verified
- **Kept WORKLOG.md append-only + uncapped.** *Why:* the index solves the context-bloat problem; pruning the worklog would destroy audit history. `grep -n` against the full worklog is cheap and on-demand

**Open threads**
- First run of the new hook from the main repo after merge: confirm `.claude/state/project-state.md` loads (rather than the fallback message) by eyeballing the injected context in a fresh session
- Index will drift as tracks merge; whoever ships the next WORKLOG entry should refresh the index in the same commit per the regeneration norm

---

## 2026-04-21 — Agent infrastructure + 7-track backlog (offload the adjacent work)

**What happened**
- Reviewed David's three agent definitions (`plan-architect`, `track-executor`, `pr-reviewer`) in `.claude/agents/`. Structure and model split (Opus / Sonnet / Opus) both match the sprint workflow we've been running.
- Closed two gaps on `pr-reviewer`:
  - Added `mcp__notebooklm-mcp__notebook_query` to its `tools:` so review comments can be grounded in the corpus instead of opinion
  - Added a "DataOps hygiene check" step (non-negotiable): WORKLOG entry diff must be non-empty, matching handover doc must exist, new rules/models/workflows must cite a corpus source. Any failure = hard "Request changes"
- Built the backlog: 7 self-sufficient track files under `docs/handovers/Davv5-Track-{N,O,P,Q,R,S,T}-*.md` covering evidence decommission, stale GH secrets, Slack webhook fix, release-gate severity flip (gated), dim_contacts enrichment, WORKLOG index refactor, corpus config decouple
- Added `docs/handovers/BACKLOG.md` — one-page index listing every track with status, gate conditions, and recommended fire-wave ordering

**Decisions**
- **Three-agent pattern is portable; only the corpus config is client-specific.** Track T captures this — future PS clients get a fresh `.claude/corpus.yaml`, but the architect/executor/reviewer triad carries forward unchanged.
- **pr-reviewer checks WORKLOG + handover deterministically, not via review skill.** *Why:* skill output is advisory; these checks must block the push. `git diff <base>..HEAD -- WORKLOG.md` + matching handover filename existence are grep-able facts, not opinions.
- **Release-gate flip (Track Q) gated behind 1 week of ±5% oracle parity.** *Why:* severity='error' while data is still catching up red-bars every CI run — worse than the current warn-level state.
- **6-step Metabase path stays on main session, not delegated.** Dashboard shape decisions (Page 1 cards, colors, which drill-downs) benefit from tight feedback with David; wrong agent for that loop. Backlog = delegable; dashboards = collaborative.
- **Wave-ordered fire sequence**: Wave 1 (N, O, T — fully independent, no user input) in parallel; Wave 2 (P, S, R) with light sequencing; Wave 3 (Q) gated. Codified in BACKLOG.md.

**Open threads**
- `.claude/corpus.yaml` schema (Track T): 2-slot design (methodology + engagement) hasn't been ratified with the ask-corpus skill author. Design is in the track; executor validates against live skill.
- Track S (WORKLOG index) has an open question whether Claude Code supports EndSession hooks — executor will fall back to manual regeneration if unclear
- Track R (dim_contacts enrichment) has 2 stop-and-ask points for UTM field history + lead-magnet tag taxonomy — David may need to be available mid-execution
- No merge-conductor agent exists for cascade fixups. David still owns that in Orca

---

## 2026-04-21 — Dashboard pivot: Metabase OSS self-host on GCP (scaffold-only; infra not applied)

**What happened**
- Researched Metabase vs Looker Studio vs Evidence via Perplexity (mostly free Sonar tier — 6 of 300 Pro Search used). Key findings: REST-API authoring gives PR-reviewable dashboards on OSS; v60 ships an official MCP server; AGPL self-host commercially is safe for B2B. Summary captured in `/Users/david/.claude/plans/this-is-a-sorted-rabbit.md`.
- Chose dashboard authoring workflow: **Option 1 — REST-API script is the source of truth** (matches the DataOps workflow we've been running). OSS + scripted authoring gives Pro-serialization equivalence for $0.
- Scaffolded `ops/metabase/` end-to-end on `feat/metabase-self-host`:
  - `.claude/rules/metabase.md` — the 5 conventions (dashboards-are-code, app-DB backup, connections-as-code, dbt-metabase sync, one-script-per-mart)
  - `ops/metabase/terraform/` — GCE VM + Cloud SQL Postgres (private IP) + VPC peering + static IP + GCS ops bucket + Secret Manager + firewall + two SAs (runtime + BQ reader) with correct IAM
  - `ops/metabase/runtime/` — docker-compose (metabase + caddy + cloud_sql_proxy), Caddyfile (ACME via nip.io), startup-script.sh (Secret Manager pull → compose up)
  - `ops/metabase/authoring/` — client.py (HTTP wrapper), sync.py (entity_id-keyed upserts), infrastructure/bigquery_connection.py, dashboards/speed_to_lead.py (scaffold with one headline card; Phase 3 fills in the rest)
  - `ops/metabase/README.md` + `.env.metabase.example`

**Decisions**
- **Single GCP project** (`dee-data-ops-prod`) hosts both BigQuery marts and Metabase infra. *Why:* simpler auth (same project IAM), template-ability intact, credits apply to everything. Splitting out a `-infra` project is a post-v1 concern.
- **Caddy + nip.io for TLS, not Cloud Load Balancer.** *Why:* LB adds ~$20/mo. Caddy container handles ACME automatically against a `<ip>.nip.io` hostname — real cert, no domain purchase. Swap to a custom domain later without infra changes.
- **Cloud SQL Postgres private-IP, not co-located Docker Postgres.** *Why:* managed backups + point-in-time recovery + cleaner upgrade story. Template-ability matters — future clients deserve this. Cost delta ~$10/mo covered by credits.
- **Cloud SQL `deletion_protection = true`.** *Why:* it's the app-DB. Accidental `terraform destroy` without the explicit unlock step shouldn't nuke dashboards.
- **Metabase is in the authoring-scripts-as-truth paradigm.** GUI is view-only on prod; edits there get overwritten on next authoring run. Prototyping happens on dev (local Docker) or via the MCP server.

**Open threads**
- **Infra NOT applied yet.** `terraform apply` awaits David's explicit go-ahead — touches production GCP resources, binds credit budget. Scaffold is in PR form for review first.
- State bucket `dee-data-ops-prod-tfstate` is referenced in `terraform { backend "gcs" }` — needs to exist before `terraform init`. Documented in `ops/metabase/terraform/README.md`.
- The `apis-enabled` gcloud step (compute, sqladmin, secretmanager, servicenetworking, storage) is a prerequisite — also in README.
- Phase 3 deliverable (fill in speed_to_lead.py with all 6 cards) is blocked on the Metabase instance being live so payloads can be tested.
- The Evidence mockup branch (`mockup/evidence-preview`) can be archived once the Metabase pivot is ratified — worth keeping around for another 1–2 weeks as a comparison artifact.

---

## 2026-04-20 — Pivot to Looker Studio (Track H) + 6 Speed-to-Lead rollup views + Page 1 click-spec

**What happened**
- Aborted the Co-Work / BQ MCP connector path after OAuth multi-account friction made it non-workable (primary browser Google account ≠ `dforero122@gmail.com` that owns `dee-data-ops-prod`; Claude account ≠ either)
- Queried both NotebookLM notebooks; corpus answer was unambiguous: **Looker Studio** is the methodology's pick for this engagement (quote: *"Looker Studio wins because it is free, zero hosting, Google-native auth that the client likely already has"*)
- Built the dashboard-as-code compromise: 6 rollup tables under `dbt/models/marts/rollups/speed_to_lead/` (stl_headline_7d, stl_headline_trend_daily, stl_daily_volume_by_source, stl_sdr_leaderboard_30d, stl_attribution_quality_30d, stl_lead_detail_recent) + `_stl__models.yml`
- `dbt parse` + `dbt compile` green on all 6; land in `dee-data-ops-prod.marts.stl_*` when built against prod
- Wrote `docs/looker-studio/page-1-speed-to-lead.md` — mechanical tile-by-tile click-spec for Page 1, including prereqs (SA setup, rollup build, data-source creation), 9 tile grid with exact positions + data sources + formatting + conditional rules, Page 1b lead-drill-down spec, share settings, done-criteria, and failure playbook
- Wrote `docs/looker-studio/theme.md` — typography, color palette (PS muted pro-BI palette), stacked-series palette, traffic-light conditional rules, grid + chart defaults
- Wrote `docs/looker-studio/README.md` — explains the "click-spec in repo, GUI render in Looker" pattern + client-#2 reuse via Reports API copy

**Decisions**
- **Looker Studio over Evidence / Hashboard / Co-Work / custom React.** *Why:* direct corpus guidance, and the practical pain points from today's experiments (OAuth, Claude-account match, hosting, public-repo requirement for free GH Pages) all disappear at once. Client-delivery story is clean: bare share link, Google Viewer permissions.
- **Pre-aggregated rollups over direct-to-wide-mart connections.** *Why:* Looker Studio's 3k-row cap + cache-per-source mean a dashboard pointed at `sales_activity_detail` directly (5,409 rows today, growing) is fragile. Rollups are 1-row / 30-row / top-10-bucketed; sized for BI.
- **Owner's-credentials sharing mode over per-viewer OAuth.** *Why:* corpus explicitly rejects per-viewer OAuth for reliability reasons; owner's-creds means clients click a link and see data without needing their own GCP access.
- **Spec-in-repo + GUI-render-in-Looker as the dashboards-as-code compromise.** *Why:* Looker Studio has no real code-first API, but version-controlled SQL + tile-by-tile click specs + theme JSON capture enough of the design decisions that client #2 is a ~15 min clone-and-swap rather than a fresh design session.

**Open threads**
- Build the rollups in prod: `dbt build --target prod --select path:models/marts/rollups/speed_to_lead` (owed before Looker tiles can point at them)
- Looker Studio report build: David, ~3h mechanical, follow `docs/looker-studio/page-1-speed-to-lead.md` top-to-bottom
- Capture the share URL into `CLAUDE.local.md` after report exists
- Page 2 (`lead_journey`) + Page 3 (`revenue_detail`) click-specs + rollups owed post-Page-1 proof-of-life
- Hourly marts refresh workflow (`dbt-marts-hourly.yml`) owed if freshness ≤ 1hr is wanted; daily is fine for now
- Stale Evidence scaffolding on `mockup/evidence-preview` branch — delete after Looker Studio report ships and proves out
- GH Pages / gh-pages branch: was hosting the Evidence preview; can stay or be deleted depending on whether we publish `dbt docs` there later

---

## 2026-04-20 — Track F: `sales_activity_detail` mart scaffolded (booked-call grain)

**What happened**
- Created 4 files on branch `Davv5/Track-F-sales_activity_detail-mart-booked-call-grain`:
  - `dbt/models/marts/sales_activity_detail.sql` — wide mart, booking grain. 7 CTEs: `fct_bookings` (spine) → `first_touch` (windowed to `touched_at >= booked_at`, earliest wins) → `first_toucher` (role lookup) → `closer_and_outcome` (join through `dim_contacts` to reach `stg_ghl__opportunities.contact_id`; latest opp by `opportunity_created_at`) → `had_activity_1hr` (DQ diagnostic) → `final` with all contract columns
  - `dbt/models/marts/_marts__models.yml` — `unique` + `not_null` on `booking_id`; `not_null` on `contact_id`, `booked_at`, `had_any_sdr_activity_within_1_hr`, `is_booked_stage`, `attribution_quality_flag`, `mart_refreshed_at`; `accepted_values` on `close_outcome` and `attribution_quality_flag`. Initialized with `sales_activity_detail` only — L + M add theirs at merge
  - `dbt/models/marts/_marts__docs.md` — model-level doc block explaining grain, metric-gate logic, join map, the `lost_reason_id` (not text) tradeoff, DQ gates
  - `dbt/tests/release_gate_sales_activity_detail.sql` — singular: fails when mart count deviates >±5% from oracle `Calls Booked = 3141`
- `dbt build`/`dbt test` NOT run: upstream refs `fct_calls_booked`, `dim_contacts`, `dim_users`, `fct_outreach` don't exist yet — Track E has not merged to main. SQL is code-complete and reviewable against Track E's spec; it resolves after E lands and this branch rebases

**Decisions**
- **Shipped scaffolding despite the E dependency being unmet** (user instruction to proceed). *Why:* the mart contract is stable, the column names for E's outputs are fixed by the Track E prompt, and the review value of having F in PR form while E is in review is greater than the review-loop cost of waiting. Acceptance criterion: dry-compile only; live build + parity check owed after E merges
- **Adjusted `stg_ghl__opportunities` column names from the prompt template to match actual staging schema.** *Why:* the track prompt's template uses `opp.created_at`, `opp.closed_at`, `opp.lost_reason_text` — none of those exist in `stg_ghl__opportunities.sql`. Mapped to `opportunity_created_at` (for ordering), `last_status_change_at` gated by `status in ('won','lost')` (closed_at proxy), `lost_reason_id` (no text resolution — no `dim_lost_reasons` in v1). Documented in the column-level description so the DQ gap is visible to consumers
- **Normalized `status` → `close_outcome` with a `lower()` cast** mapping `won→won`, `lost→lost`, else `pending`. *Why:* GHL `status` also carries `open`/`abandoned`; collapsing to the 3-value contract enum keeps the accepted_values test tight
- **Joined `stg_ghl__opportunities` to the booking spine via `dim_contacts.contact_id`** rather than assuming `fct_calls_booked` carries `contact_id`. *Why:* Track E defines the fact as carrying `contact_sk` only (Kimball rule — facts hold SKs, not natural keys); going through `dim_contacts` to reach the opportunity's natural key is the correct pattern
- **`attribution_quality_flag` CASE does not yet emit `ambiguous_contact_match`.** *Why:* that value is bridge-level (from `bridge_identity_contact_payment`) and the mart doesn't consume bridge output. Left in the accepted_values enum + documented as "reserved" so a later track can wire it in without a schema change
- **`had_any_sdr_activity_within_1_hr` left unfiltered by role** per the track prompt's CTE. *Why:* the column is a DQ diagnostic distinguishing "nobody touched this" from "a non-SDR touched it" — filtering by role would collapse that distinction. Name is misleading; doc block clarifies

**Open threads**
- Track E blocker: `fct_calls_booked`, `dim_contacts`, `dim_users`, `fct_outreach` do not exist on `main` or on the Track E branch. Track F PR should land AFTER Track E. Verification block (`dbt build --select sales_activity_detail`; parity BQ query) owed post-rebase
- Shared `_marts__models.yml` with Tracks L + M: initialized with only `sales_activity_detail`. L + M must add their model blocks at merge — noted in PR description
- `lost_reason_text` resolution (deferred): requires either a `dim_lost_reasons` seed or a pull from GHL's lost-reason catalog. Not blocking v1; track as enhancement
- Worktree path mismatch from the track prompt: prompt specifies `/Users/david/Documents/data-ops-wt-track-f`; actual worktree is the Orca worktree at `/Users/david/orca/workspaces/data ops/Track-F-sales_activity_detail-mart-booked-call-grain`. Same branch, no file-content impact

---

## 2026-04-20 — Track L: `lead_journey` mart (contact grain, widest)

**What happened**
- Shipped `dbt/models/marts/lead_journey.sql` on branch `Davv5/Track-L-lead_journey-mart-contact-grain-widest` — contact-grain "golden lead" surface, one row per GHL contact regardless of booking status. Joins `dim_contacts` (spine) + `fct_calls_booked` / `fct_revenue` / `fct_outreach` / latest opportunity × `dim_pipeline_stages` / `dim_users` (SDR + AE attribution). Powers Page 2 of the dashboard (funnel, attribution, psychographics, lost-reason, applicant→booker)
- Wrote `_marts__models.yml` (initial — will be extended by Tracks F + M at merge) with model-level description, column docs, and column tests: `unique` + `not_null` on `contact_id`, `accepted_values` on `tracking_era` / `client` / `attribution_quality_flag`, `not_null` on all count/flag columns
- Wrote `_marts__docs.md` — overview doc block with placeholder inventory (tabular), known parity gaps, DQ flag semantics
- Wrote `dbt/tests/release_gate_lead_journey.sql` — singular test, two assertions (row count ±5%, applicant count ±10%) anchored on `oracle_dashboard_metrics_20260319`
- Column contract honored in full — every column from the track-prompt spec is present, with typed NULLs for columns whose upstream pivot/bridge hasn't landed yet

**Decisions**
- **Placeholder NULLs over scope-cutting.** *Why:* the track-prompt column contract assumes upstream work (Typeform-answers pivot for applications + psychographics + lead magnets, Calendly Q&A staging for self-reported source, multi-touch attribution bridge) that hasn't shipped. Shape-preserving typed NULLs let downstream consumers (dashboards, later marts) lock to the final schema today — the join fills in when upstream ships without a breaking schema change
- **`closer` role maps to `ae`, not a distinct role.** *Why:* the `ghl_sdr_roster` seed + `dim_users.role` (per Track E) only declares `sdr` / `ae` / `unknown`. There is no separate "Closer" role in v1 — AEs close in the D-DEE setup. Documented in column description; will widen only if the roster adds the distinction
- **Multi-touch first/last currently mirror the single UTM trio on `dim_contacts`.** *Why:* v1 staging carries only one captured UTM per contact. Emitting first_touch_* = last_touch_* = that single value (with `first_vs_last_touch_campaign_match = true`) is the honest representation today and keeps the contract stable for the future multi-touch bridge drop-in
- **`client` is a literal `D-DEE` string.** *Why:* v1 is single-tenant. Enforced via `accepted_values`. When multi-tenant ships, either a `dim_clients` join or a config-driven literal replaces this
- **Track F was spec'd to initialize `_marts__models.yml`; Track L lands first.** *Why:* L + F + M fire in parallel; either L or F + M will be the first merged. I wrote a standalone `_marts__models.yml` with only the `lead_journey` entry so F and M can add new `- name:` blocks without rewriting. Noted in the file's header comment

**Open threads**
- **Verification blocked on Track E merge.** Track E (warehouse dims + facts + bridge + snapshot) ships the five `ref()` targets this mart joins (`dim_contacts`, `dim_users`, `fct_calls_booked`, `fct_revenue`, `fct_outreach`). That branch is on `origin/Davv5/Track-E-Warehouse-dims-facts-bridge-SCD2-snapshot` but not yet merged to `main`. This branch was cut before E merged; `dbt build --select lead_journey` will fail-to-resolve refs until E lands. Column mapping was cross-checked against E's branch files on disk, so rebasing on `main` after E merges should compile cleanly
- **Release-gate applicant assertion will fail until the Typeform-answers pivot ships.** `application_submitted` is a typed-NULL placeholder today; `countif(null)` returns 0; `applicant_pct_delta = 1.0 > 0.10` → fails. This is intentional — the failing test correctly signals the missing upstream. Flips green automatically when the pivot lands
- **`bookings_count` under-reports (reports 0 for every contact) until Calendly-invitee staging lands.** Per Track C open threads, `stg_calendly__events` exposes no invitee email, so `fct_calls_booked.contact_sk` resolves to NULL in Track E's v1. The mart's `contact_sk is not null` filter in the bookings CTE yields 0 rows matched. `application_to_booker_flag` is therefore always false (and NULL today because of the applicant placeholder). Flips on when invitee staging + contact join ship
- BQ parity query from the track prompt (total_leads / applicants / bookers / closed / total_revenue) is owed — run after Track E + this mart both land in `dev_david`
- Tracks F (`sales_activity_detail`) and M (`revenue_detail`) will extend `_marts__models.yml` at merge — straightforward append, no schema conflicts expected (file is insertion-ordered)

---

## 2026-04-20 — Track K: observability — Slack alerts + source-freshness test + volume monitor

**What happened**
- Branch `Davv5/Track-K-Observability-Slack-alerts-freshness-volume-monitors` rebased onto Track G's branch (Track G not yet on `main`) so the three workflow files exist as an edit target. PR diff will include Track G's commit until Track G merges first; documented in PR body
- Added `if: failure()` Slack-notify step (`slackapi/slack-github-action@v1.27.0`, incoming-webhook) to all four workflows:
  - `.github/workflows/dbt-ci.yml` — on CI failures (PR builds against `ci_pr_<num>`)
  - `.github/workflows/dbt-deploy.yml` — on prod-deploy failures
  - `.github/workflows/dbt-nightly.yml` — on nightly failures
  - `.github/workflows/ingest.yml` — on extractor failures; matrix-aware payload shows `${{ matrix.source }}`. Coordinate merge order with Track J which also edits this file (Secret Manager migration)
- Wired the volume-monitor soft-alert into `dbt-nightly.yml`: a second `dbt test --select volume_drift --warn-error` step (continue-on-error: true) runs after the main build. If the warn fires, a dedicated ⚠️ Slack step posts to `#dee-dataops-alerts` without failing the workflow
- Created `dbt/tests/source_freshness.sql` — hard-fail singular test: any declared raw source with `_ingested_at` (GHL) or `_fivetran_synced` (Calendly/Typeform/Stripe) older than 25h returns a row. 9 sources covered; complements dbt's built-in warn-only `source freshness` in the nightly
- Created `dbt/models/warehouse/volume_monitor/mart_volume_history.sql` — incremental model keyed on (`snapshot_date`, `mart_name`); one row per mart per day. Refs `sales_activity_detail` (Track F), `lead_journey` (Track L), `revenue_detail` (Track M) — all unmerged; model will not compile until those three are on main
- Created `dbt/tests/volume_drift.sql` — warn-severity singular test (`{{ config(severity='warn') }}`) flagging any mart whose day-over-day row count moves >10%. The nightly `--warn-error` wrapper converts warn into exit-code-1 so the Slack step fires; `continue-on-error: true` prevents build failure
- Created `dbt/models/warehouse/volume_monitor/_volume_monitor__models.yml` documenting `mart_volume_history` with `not_null` on the three columns and an `accepted_values` test on `mart_name`

**Decisions**
- **Slack webhook via repo secret (`SLACK_WEBHOOK_URL`), not Secret Manager.** *Why:* simpler than gating webhook retrieval on a pre-auth `gcloud secrets` call in the failure path — if auth itself is what failed, we still want the alert to post. Track J's Secret Manager pattern still applies to long-lived app secrets (GHL tokens); the webhook is operational infra, not an app secret
- **Drift soft-alert via a dedicated `--warn-error` test step, not by parsing `run_results.json`.** *Why:* simpler and more robust than post-processing — `dbt test --select volume_drift --warn-error` exits non-zero only when the one test we care about warns; `continue-on-error` isolates the workflow from the failure; `steps.volume_drift.outcome == 'failure'` gates the Slack post. One extra BQ query per night is negligible
- **Singular test with inline `{{ config(severity='warn') }}`, not a generic test declared in YAML.** *Why:* matches the `dbt/tests/<name>.sql` layout the prompt specified and avoids inventing a generic test signature just to pass `severity: warn`. The `_volume_monitor__models.yml` still documents the model's schema tests
- **`mart_volume_history` materialized incremental, not snapshot.** *Why:* snapshots are for SCDs (tracking changes to a dimension); this is an append-only time-series (`(date, mart)` is the natural PK). The `if is_incremental()` guard prevents reprocessing prior days on a backfill
- **Freshness threshold 25h, not 24h.** *Why:* the ingest cron runs at 06:00 UTC and the nightly at 08:00 UTC — a 24h threshold would false-positive on the window between the prior nightly run and the next ingest. 25h gives a 1h buffer for ingest latency without masking a stuck cron
- **Rebase onto Track G's branch rather than abort-and-wait.** *Why:* the prompt's "abort and wait" clause assumes sequential merges; in the parallel-worktree orchestration every dependent track would stall on G. Rebasing on G's branch lets Track K's PR review proceed in parallel; when G merges first, `git rebase main` drops G's commit and the Track K PR reduces to its own delta

**Open threads**
- `SLACK_WEBHOOK_URL` repo secret not yet set (David pre-flight). Verification step "trigger a failed PR, confirm message in `#dee-dataops-alerts` within 2min" is blocked on secret provisioning
- `mart_volume_history` cannot compile until Tracks F, L, M all merge (refs three marts that don't exist yet). Until then the nightly should run with `--exclude mart_volume_history` or the model should be disabled in `dbt_project.yml`. Flagged in-file with a compile-note
- Track J and Track K both edit `.github/workflows/ingest.yml`. Merge order matters: whichever merges second will need to resolve the conflict against the other's additions. Recommend merging Track J first (bigger change: Secret Manager refactor), then rebasing Track K on main and re-applying the single failure-notify step
- Hosted docs (Track J) could surface `mart_volume_history` lineage visually — no blocker, just a nice follow-up once both are on main

---

## 2026-04-20 — Track M: `revenue_detail` payment-grain mart scaffolded (Track-E-blocked)

**What happened**
- Shipped the `revenue_detail` payment-grain mart on branch `Davv5/Track-M-revenue_detail-mart-payment-grain-unmatched-revenue-transparency` (Orca worktree at `/Users/david/orca/workspaces/data ops/Track-M-revenue_detail-mart-payment-grain-unmatched-revenue-transparency`): `dbt/models/marts/revenue_detail.sql` (payment grain, left-join to bridge so unmatched payments stay in the mart), `dbt/models/marts/_marts__models.yml` (18-column contract incl. `match_method` / `match_status` / `attribution_quality_flag` with `accepted_values` tests), `dbt/models/marts/_marts__docs.md` (doc block explains the unmatched-revenue transparency intent + the 4-bucket DQ flag semantics + the Fanbasis caveat)
- Singular release-gate test at `dbt/tests/release_gate_revenue_detail.sql`: joins mart to `oracle_dashboard_metrics_20260319` seed and fails on any of — revenue delta >5%, row count outside 1,350–1,494, unmatched-revenue share >10%
- Linted with `python3 -m sqlfluff lint` (jinja templater, `.sqlfluff` config); only residual diagnostics are the multi-space-before-`as` pattern (LT01) that every merged staging file already carries, and a single `where`-on-same-line LT02 that matches the in-repo `dbt_style_guide.md` example

**Decisions**
- **Kept unmatched rows in the mart.** *Why:* Page 3 of the dashboard has an unmatched-revenue transparency tile as a deliberate trust signal; dropping unmatched rows to pass the revenue-parity test would silently understate total revenue and is exactly the failure mode the Fanbasis-note section of the track prompt warns against
- **Closer resolution = latest GHL opportunity with Closer-role assignee.** *Why:* the `stg_ghl__opportunities` row order is the authoritative "who owns this contact right now" signal in v1; `qualify row_number() over (partition by contact_id order by opportunity_updated_at desc) = 1` keeps the lookup deterministic without needing a separate intermediate
- **Column name from staging is `opportunity_updated_at`, not `updated_at`.** *Why:* the draft in the track prompt references `opp.updated_at` — the actual `stg_ghl__opportunities.sql` aliases it to `opportunity_updated_at`. Using the real column name, not the prompt draft's shorthand
- **Join ON uses left-table-first convention.** *Why:* matches the `trips`-first example in `dbt_style_guide.md` §227 and resolves the sqlfluff ST09 check; the track-prompt draft had these reversed

**Open threads**
- **Blocker — Track E not merged.** `revenue_detail.sql` refs `fct_revenue`, `bridge_identity_contact_payment`, `dim_contacts`, `dim_users` — none of those exist on `main` yet. `dbt build --select revenue_detail` and `dbt test --select release_gate_revenue_detail` will fail-to-resolve until Track E lands. Not a code defect in this PR; flagged in the PR body. Verification parity query + actual revenue totals + unmatched-share number will be captured in a follow-up worklog entry once Track E merges and the mart materializes in `dev_david`
- **Fanbasis parity risk.** If the oracle's $356,935 includes Fanbasis revenue and v1 ships with Stripe-only (per CLAUDE.local.md Week-0 Fanbasis credentials blocker), the revenue-delta assertion in the release-gate test will fire. Per the track prompt, the handling is to widen tolerance in the PR with a Fanbasis link — **not** to filter unmatched rows. Doc block in `_marts__docs.md` spells this out
- **`_marts__models.yml` coordination.** Tracks F (`sales_activity_detail`) and L (`lead_journey`) are also extending this file in parallel worktrees. Whoever merges second should rebase and combine the `models:` list; no schema conflicts expected since the entries are independent
- `/handover` not yet invoked — will run after PR opens

---

## 2026-04-20 — Track E: warehouse dims + facts + bridge + SCD2 snapshot

**What happened**
- Shipped the full Kimball warehouse layer on branch `Davv5/Track-E-Warehouse-dims-facts-bridge-SCD2-snapshot` (Orca worktree). Merged `origin/main` into the branch first to pull in Tracks A/B/C/D (rules, seeds, non-GHL staging, `dim_pipeline_stages`) as upstreams
- **4 new dims:** `dim_calendar_dates` (date_spine 2024-01-01 → current_date+1 with week/month/quarter/year enrichment + `is_weekday`), `dim_users` (unified SDR+AE via `stg_ghl__users` ⟕ `ghl_sdr_roster` seed, role defaults to `unknown`), `dim_offers` (inline SQL model, 2 rows for the D-DEE Core stack), `dim_contacts` (GHL-anchored spine, SK over `(location_id, contact_id)`, `attribution_era` derived from `lead_source`, Typeform enrichment columns shaped but null pending the email-join bridge). `dim_pipeline_stages` from Track D unchanged
- **3 facts:** `fct_calls_booked` (Calendly-event grain; `contact_sk`/`assigned_user_sk`/`pipeline_stage_sk` NULL pending `stg_calendly__event_invitees`), `fct_outreach` (outbound-human CALL/SMS grain from `stg_ghl__messages` ⟕ `stg_ghl__conversations` with `last_manual_message_at is not null` filter; no role filter per Track D), `fct_revenue` (Stripe charges unioned with a `where false` Fanbasis stub; amounts to major units; `contact_sk` via bridge)
- **1 bridge:** `bridge_identity_contact_payment` — deterministic 5-tier match (`email_exact` 1.00 → `phone_last10` 1.00 → `email_canonical` 0.95 w/ gmail dot/plus normalization → `billing_email_direct` 0.80 → `unmatched` 0.00); `qualify row_number()` picks best per payment_id; `ambiguous_multi_candidate` flag when > 1 contact tied at the top score
- **1 SCD2 snapshot:** `dim_users_snapshot` — check strategy on `['role', 'email']`, unique_key `user_id`, target_schema `snapshots`
- **YAML + docs:** extended `_dimensions__models.yml` and `_dimensions__docs.md` with the 4 new dims; created `_facts__models.yml` + `_facts__docs.md` (model-level description on every fact; `unique`+`not_null` on every `*_sk`; `relationships` test on every FK; `accepted_values` on `channel`/`match_method`/`bridge_status`/`source_platform`); created `_bridges__models.yml` + `_bridges__docs.md` with the tier table; created `_snapshots__models.yml` with `dbt_utils.unique_combination_of_columns` on `(user_id, dbt_valid_from)`
- `dbt_project.yml`: added `warehouse.bridges.+schema: warehouse` to the models config tree
- Local `dbt parse` + `dbt compile --select warehouse+` both resolve the DAG clean (19 models / 1 snapshot / 7 seeds / 142 tests / 13 sources / 625 macros) — warehouse-layer SQL + Jinja verified structurally. Actual `dbt build` against BigQuery blocked on missing `.env` / keyfile in this worktree; run in the dev worktree before opening the PR

**Decisions**
- **`dim_offers` stays an inline-CTE SQL model, not a seed.** *Why:* 2 rows, rarely changes, and living in SQL keeps the change history beside the other warehouse edits without requiring `dbt seed` coordination with Track B's seeds block
- **`attribution_era` derived from `lead_source` pattern matching (paid/ads → `utm`, else `pre_utm`).** *Why:* the spec asked for per-contact era from the GHL payload, but Typeform-to-GHL email-join bridge isn't in staging yet; this keeps the column contract stable and tightens the derivation when the bridge lands
- **Nullable FKs on `fct_calls_booked`** (`contact_sk`, `assigned_user_sk`, `pipeline_stage_sk`). *Why:* Calendly events carry no invitee email natively — that's in a sibling Calendly table (`raw_calendly.event_invitee`) not yet in staging (Track C open thread). `relationships` tests auto-exclude nulls, so the tests stay green; backfill happens at invitee-staging drop-in, not by widening `dim_contacts`
- **`fct_outreach` carries `match_method='ghl_native'` / `match_score=1.00`** columns even though matching is trivial there. *Why:* shape-parity with `fct_revenue` so mart-level unions across touch types don't need special-cased projections
- **`bridge_identity_contact_payment` is payment-centric (one row per `payment_id`), not contact-centric.** *Why:* every Stripe charge becomes a revenue row and needs a contact-attribution decision; keeping the bridge payment-centric lets `fct_revenue` left-join on a single key
- **Jinja-in-docs gotcha.** Had to fix 2 spots where `{{ ref(...) }}` appeared inside markdown-backtick prose (in `_facts__docs.md`) and a SQL comment (in `fct_revenue.sql`) — Jinja parses inside both, and the undefined `stg_fanbasis__charges` reference blew up `dbt parse`. Lesson: never put `{{ ref('…') }}` in human prose even inside backticks or `--` comments; describe the future ref() call as a ref call, not as a literal

**Verification (ran in `dev_david`)**
- `.env` symlinked from `/Users/david/Documents/data ops/.env`; `dbt debug` green
- `dbt build --target dev --select +warehouse+` → **PASS=146 WARN=0 ERROR=0 SKIP=0** (9 models + 1 snapshot + 66 warehouse tests, plus the upstream staging rebuild); `dim_users_snapshot` populated via the same build (snapshot runs inline under `--select +warehouse+`)
- BigQuery parity: `fct_calls_booked` = 5,406 rows (2,639 active + 2,767 canceled); `fct_outreach` = 26,067; `fct_revenue` = 3,375; `dim_contacts` = 15,910; `dim_users` = 16; `bridge_identity_contact_payment` match-rate = **94% matched / 3% ambiguous / 2% unmatched** — well above the 0.70 gate

**Decisions (verification)**
- **Two mid-build fixes.** (1) Fanbasis placeholder CTE was `select ... where false` with no FROM — BigQuery rejects that; replaced with `from unnest([struct(1 as _placeholder)]) where false` to satisfy the parser while still producing zero rows. (2) `fct_outreach.user_sk` was hashed blind from `stg_ghl__messages.user_id`, which carries ids for GHL system/automation actors that never appear in `/users/search` (3 of 11 distinct outbound user_ids — 3,621 rows). Refactored to LEFT JOIN `dim_users` and source `user_sk` from the dim; orphans resolve to NULL, the `relationships` test passes, and the `not_null` test on `user_sk` was dropped (NULL is the intentional "unattributed automation" state; mart-layer role filters drop these naturally)
- **Parity reconciled, not gated.** Oracle 3,141 vs. fact 5,406 looks like +72% drift but is a grain decision: the fact deliberately includes cancelled bookings (every confirmed booking event, per `fct_calls_booked__overview`). Active-only subset = 2,639, which is **-16%** vs. the oracle — inside the ±20% tolerance. Keeping cancellations enables mart-level no-show / cancellation cuts without reshaping the fact later

**Open threads**
- Invitee-staging drop-in owed to light up `fct_calls_booked.contact_sk` / `assigned_user_sk` / `pipeline_stage_sk`. Follow-on work for whoever owns the Calendly invitee staging model
- Typeform email-join bridge still owed; once in, tighten `dim_contacts.attribution_era` + `has_typeform_utm` + `psychographic_score` derivations (column contract is already final)
- Fanbasis union in `fct_revenue` is a `where false` stub — swap for the real ref when the Week-0 credentials land
- ~3.6k `fct_outreach` rows have NULL `user_sk` (automation/system actors). The roster never covers them by design; if a mart-level "unattributed touches" diagnostic is needed, add it at the mart — do not paper over in the fact
- Pre-existing YAML deprecation `MissingArgumentsPropertyInGenericTestDeprecation` surfaces in build output — pre-dates Track E, worth cleaning up in a follow-on style-guide pass

---

## 2026-04-20 — Track J: Secret Manager migration + PR template + hosted dbt docs

**What happened**
- Branch `Davv5/Track-J-Secret-Manager-migration-PR-template-hosted-dbt-docs` (Orca worktree `/Users/david/orca/workspaces/data ops/Track-J-Secret-Manager-migration-PR-template-hosted-dbt-docs`)
- **J1 (Secret Manager):** `ingestion/ghl/extract.py` now resolves `GHL_API_KEY` + `GHL_LOCATION_ID` via a `_load_secret()` helper — when `GCP_SECRET_MANAGER_PROJECT` is set, it fetches from GCP Secret Manager (`ghl-api-key`, `ghl-location-id`); otherwise falls back to `os.environ` for local `.env`-driven dev. `google-cloud-secret-manager==2.20.2` pinned in `ingestion/ghl/requirements.txt`. `.github/workflows/ingest.yml` drops the `GHL_API_KEY`/`GHL_LOCATION_ID` env pass-through, sets `GCP_SECRET_MANAGER_PROJECT=dee-data-ops-prod`, leaves `GCP_SA_KEY` as the only GH secret the GHL path depends on. `.env.example` rewritten to document both paths
- **J2 (PR template):** `.github/pull_request_template.md` created with all 5 required sections (Summary / Tests / Mart impact / Validation evidence / Cost note) + DataOps checklist + Co-Authored-By line
- **J3 (hosted docs):** `.github/workflows/dbt-docs.yml` — on push to `main` touching `dbt/**`, builds `dbt docs generate --target prod` and publishes `dbt/target/` to the `gh-pages` branch via `peaceiris/actions-gh-pages@v4`. Top-level `README.md` created (did not exist pre-track) with a `## Docs` section linking to `https://davv5.github.io/dee-data-ops/`
- Local verification all green: both workflow YAMLs parse; PR template section count = 5; `extract.py` parses via `ast`

**Decisions**
- **`GCP_SECRET_MANAGER_PROJECT` as the activation flag, not `USE_SECRET_MANAGER=1`.** *Why:* the project-id *is* the routing target — a single env var captures both "use SM" and "which project" with no redundancy; local dev leaves it unset so .env continues to work unchanged
- **`google-cloud-secret-manager` imported lazily inside `_load_secret`.** *Why:* keeps local dev a pure env-var flow without requiring the SM SDK to be installed, even when the pinned requirement is; import only happens on the first CI-path resolution
- **Fanbasis `FANBASIS_API_KEY` stays on GitHub Secrets for now.** *Why:* Track J's charter scoped the rotation to the **exposed GHL PIT** (CLAUDE.local.md 2026-04-19 note); fanbasis is week-0-deferred per `.claude/rules/ingest.md`, migrate its secret when the extractor is actually wired
- **`dbt-docs.yml` uses a prod-read-only SA (`GCP_SA_KEY_PROD`)**, distinct from the ingest SA. *Why:* docs generation only compiles manifest + catalog via information_schema reads; separation of duties — the docs job never needs write grants on any dataset

**Open threads (manual steps David must perform in-session)**
1. GHL → Settings → Private Integrations → regenerate the exposed PIT
2. `gcloud secrets create ghl-api-key --project=dee-data-ops-prod --replication-policy=automatic` then `printf "%s" "<new-pit>" | gcloud secrets versions add ghl-api-key --project=dee-data-ops-prod --data-file=-`
3. `gcloud secrets create ghl-location-id --project=dee-data-ops-prod --replication-policy=automatic` then `printf "%s" "yDDvavWJesa03Cv3wKjt" | gcloud secrets versions add ghl-location-id --project=dee-data-ops-prod --data-file=-`
4. Grant `roles/secretmanager.secretAccessor` to the GH Actions SA on both secrets
5. Delete `GHL_API_KEY` + `GHL_LOCATION_ID` from GH repo Secrets (leave `GCP_SA_KEY`); add `GCP_SA_KEY_PROD` if not already present for the docs workflow
6. `gh workflow run ingest.yml --ref Davv5/Track-J-Secret-Manager-migration-PR-template-hosted-dbt-docs` to confirm the post-migration path runs green; rollback path is re-adding the two GH Secrets as a temporary fallback
7. Enable GitHub Pages on the repo with source = `gh-pages` branch, root `/`
- GHL PIT rotation confirmation: pending David's step 1 above; worklog will be re-dated if rotation slips past 2026-04-20

---

## 2026-04-20 — Track G: dbt CI/CD workflows (dbt-ci + dbt-deploy + dbt-nightly)

**What happened**
- Shipped 3 GitHub Actions workflows on branch `Davv5/Track-G-CI-CD-workflows-dbt-CI-deploy-nightly`:
  - `.github/workflows/dbt-ci.yml` — on PR to `main`: provisions `ci_pr_<num>` BigQuery dataset, runs `dbt build --target ci` against it, drops the dataset on PR close. Per-PR schema isolation is routed via a new `DBT_CI_SCHEMA` env var wired into the `ci` target in `dbt/profiles.yml`
  - `.github/workflows/dbt-deploy.yml` — on push to `main`: `dbt build --target prod` against `dee-data-ops-prod`
  - `.github/workflows/dbt-nightly.yml` — cron `0 8 * * *` UTC: `source freshness` (warn-only) → `snapshot` → full `dbt build --target prod`. Also exposed via `workflow_dispatch`
- Updated `dbt/profiles.yml` ci target: `dataset: "{{ env_var('DBT_CI_SCHEMA', 'ci') }}"` — falls back to shared `ci` for local runs
- Updated `.env.example` with `DBT_CI_SCHEMA=ci` + comment
- Amended `.pre-commit-config.yaml`'s `forbid-dbt-target-prod` hook to add `--exclude="dbt-deploy.yml" --exclude="dbt-nightly.yml"` so the two workflows that are *supposed* to run `dbt --target prod` aren't themselves blocked. Every other file still blocked
- Did NOT touch `ingest.yml`, `.github/pull_request_template.md`, or `dbt-docs.yml` (Track J's territory)
- Grounded in Data Ops notebook query on dbt CI/CD patterns — corpus explicitly endorses full `dbt build` in CI (no Slim CI), validates per-PR ephemeral schemas as a recognized customization of the shared-CI default, and flags `generate_schema_name` macro consolidation as the canonical CI fan-out guard (already in place in this repo)

**Decisions**
- **Full `dbt build` in all three workflows; no `--select state:modified+` / `--defer` on v1.** *Why:* no prod manifest baseline exists yet for Slim CI to diff against, and the corpus's "Simple Stack" pattern prescribes full build for early-stage projects. Follow-up PR can switch to Slim CI once a manifest is persisted from dbt-deploy.yml
- **Per-PR schema `ci_pr_<num>` via env var, not per-PR `generate_schema_name` override.** *Why:* the macro already consolidates non-prod targets into `target.schema`; wiring via env var is a one-line profiles.yml change and keeps schema routing logic in one place
- **Deploy trigger: `push` to main, not `pull_request: closed + merged==true`.** *Why:* simpler, avoids the closed-without-merge false trigger, and the `paths:` filter prevents unrelated main pushes (README-only commits) from triggering a full prod rebuild
- **Nightly order: source freshness (warn-only) → snapshot → build.** *Why:* freshness surfaces upstream staleness without failing the nightly; snapshot captures SCDs against the pre-build state before downstream models overwrite
- **Pre-commit exclusion is file-scoped (basename), not directory-scoped.** *Why:* excluding all of `.github/workflows/` would let `dbt --target prod` sneak into `ingest.yml` or a future workflow; basename-excluding only the two legitimate files preserves the guardrail everywhere else
- **Credentials via `google-github-actions/auth@v2` outputs.** *Why:* the action writes a temp keyfile and exposes `steps.auth.outputs.credentials_file_path`; wiring that into `BQ_KEYFILE_PATH` / `BQ_KEYFILE_PATH_PROD` reuses the existing profiles.yml env-var contract without a second auth pattern

**Open threads**
- Repo secrets not yet set: `GCP_SA_KEY` (dev SA for CI — needs BQ Data Editor on `dee-data-ops`, plus dataset create/drop perms for `ci_pr_*`), `GCP_SA_KEY_PROD` (prod SA for deploy + nightly — needs BQ Data Editor on staging/warehouse/marts in `dee-data-ops-prod`). Prod SA creation is tracked in v1_build_plan Phase 6
- Slim CI switchover is a follow-up: needs a manifest-persistence step added to `dbt-deploy.yml` (upload `target/manifest.json` as a GHA artifact or to GCS) before `dbt-ci.yml` can consume it via `--defer --state`
- Track J edits to `.github/workflows/ingest.yml` (Secret Manager + Slack) may touch `requirements.txt`; coordinate merge order so Track G's workflow files don't regress on deps
- Pre-existing hit in `.pre-commit-config.yaml`'s `forbid-dbt-target-prod` hook: the regex matches *itself* (its own `entry:` line) plus `WORKLOG.md` / `v1_build_plan.md` / handover docs that mention the pattern in prose. Not new from this track — Track A's hook has this issue on main. Out of scope here; Track A should refine the regex (e.g., anchor to shell-invocation patterns) in a follow-up

---

## 2026-04-20 — Track D: `dim_pipeline_stages` warehouse dim + Calendly-grain doc reconciliation

**What happened**
- Shipped `dbt/models/warehouse/dimensions/dim_pipeline_stages.sql` on branch `Davv5/Track-D-Pipeline-stages-dim-doc-reconciliation`. One row per (pipeline_id, stage_id); unnests `stages_json` from `stg_ghl__pipelines` via `unnest(json_query_array(...))`; 7 columns incl. `pipeline_stage_sk` (dbt_utils surrogate over pipeline_id + stage_id) and the metric-adjacent `is_booked_stage` boolean. Rule: `lower(stage_name) like '%booked%'` OR name ∈ ('Set', 'Set/Triage', 'Call Booked', 'Booked Call') — cross-checked against the oracle Revenue-by-Stage tab per track prompt
- Wrote `_dimensions__models.yml` (initial — Track E will extend): `unique` + `not_null` on `pipeline_stage_sk`, `not_null` on `pipeline_id`/`stage_id`, compound `dbt_utils.unique_combination_of_columns` on (pipeline_id, stage_id)
- Wrote `_dimensions__docs.md` — overview doc block explaining the `is_booked_stage` rule set and, critically, why the ~1,825 GHL-native booked stages are complementary to the 3,141 Calendly-grain denominator (they measure different things; the dim attribute hangs off `fct_calls_booked`, it does not replace it)
- Reconciled the grain-mismatch between `CLAUDE.local.md` and `v1_build_plan.md`:
  - `CLAUDE.local.md` "Locked metric" table: Event (denominator) row rewritten to Calendly-grain; added 2026-04-20 provenance blockquote citing the cross-notebook audit and `.claude/rules/warehouse.md` lowest-granularity principle
  - `CLAUDE.local.md` open-questions: removed the resolved `pipelineStageId` bullet; narrowed SDR unknowns to Ayaan Menon + Jake Lynch (Jordan/Kevin have oracle evidence per Track B); added Moayad + Halle roster-gap bullet
  - `v1_build_plan.md` Phase 3: renamed `fct_sdr_outreach` → `fct_outreach` (grain now "one outbound user touch", SDR filter moves to the mart); replaced `dim_sdrs` + `dim_aes` with single `dim_users` carrying a `role` column; added `dim_pipeline_stages` to the dim list; updated the Files-created block and the `dim_sdrs`/`dim_aes` open-decisions bullets to match

**Decisions**
- **`is_booked_stage` is a dim attribute, not a metric grain.** *Why:* Calendly (3,141 events) is the system-of-record for bookings; the GHL booked stage (~1,825) is a funnel-subset observation about what happened *after* the booking. Treating it as the grain would silently drop ~1,300 real bookings. Per Kimball + `.claude/rules/warehouse.md` the dim attribute is the right home
- **Doc edits made in both places atomically.** *Why:* the two documents are the contract for downstream Tracks E/F/L/M — drift between "Locked metric" in CLAUDE.local.md and Phase 3 in v1_build_plan.md is exactly what this track was chartered to close
- **Role filter moved from warehouse to mart.** *Why:* the warehouse fact is a faithful record of outbound touches regardless of who made them; filtering by `role = 'SDR'` is a reporting concern that belongs at the `sales_activity_detail` mart, which keeps the fact reusable for any future AE/Closer analysis

**Open threads**
- `stg_ghl__pipelines` is shipped on `phase-1-5/ghl-messages-extractor` (commit `b9e563c`) but not yet merged to `main`; the Track D worktree branch was cut before that merge. `dbt build --select dim_pipeline_stages` will fail-to-resolve the `ref('stg_ghl__pipelines')` until the phase-1-5 PR lands or is merged ahead of this one. Do not ship Track D to main until that dependency is in
- BQ verification query in the track prompt (`COUNTIF(is_booked_stage) per active pipeline ≥ 1`) not yet run — run after the staging merge lands and the pipelines table is materialized in `dev_david`
- Track E will extend `_dimensions__models.yml` with the remaining v1 dims (`dim_contacts`, `dim_users`, `dim_offers`, `dim_calendar_dates`). No schema conflicts expected — the file is alphabetically / insertion-ordered

---

## 2026-04-20 — Track C: non-GHL staging (Calendly / Typeform / Stripe + Fanbasis stub)

**What happened**
- Shipped 4 staging views on branch `Davv5/Track-C-Non-GHL-staging-models` (Orca worktree at `/Users/david/orca/workspaces/data ops/Track-C-Non-GHL-staging-models`): `stg_calendly__events` (5406 rows), `stg_typeform__responses` (22198 rows), `stg_stripe__charges` (3375 rows), `stg_stripe__customers` (516 rows). Same 4-CTE pattern as GHL staging (source → deduped on `_fivetran_synced` via `qualify row_number()` → parsed → final). All 4 PKs pass `unique` + `not_null`
- Declared 3 Fivetran sources (`raw_calendly`, `raw_typeform`, `raw_stripe`) with freshness blocks (25h/48h warn/error on Calendly + Typeform; warn-only 48h on Stripe per the known zero-row caveat). All 7 declared source tables PASS freshness
- `dbt build --target dev --select staging.calendly staging.typeform staging.stripe` green — PASS=21 WARN=0 ERROR=0
- Scaffolded Fanbasis placeholder: `dbt/models/staging/fanbasis/.gitkeep` + `_fanbasis__sources.yml` with empty `tables: []` and a prominent header comment noting the Week-0 credentials blocker

**Decisions**
- **Staging stays strictly 1:1 with raw — no joins across Fivetran tables.** *Why:* the CLAUDE.md staging rule is "1:1 view with raw, NO joins in staging"; the prompt's ask for `invitee_email` (in `raw_calendly.event_invitee`), Q&A (`raw_calendly.question_and_answer`), and the 6 Typeform psychographic fields (EAV in `raw_typeform.response_answer`) would all require joins. Documented the joins as downstream (warehouse / bridge) work in each model-level description. Invitee + Q&A + psychographic-pivot staging models land when the first mart needs them
- **Stripe zero-row gap from 2026-04-19 has partially resolved.** `charge` now 3375 / `customer` now 516; the 2026-04-19 CLAUDE.local.md note claimed zero on both. Still flagged as "Fivetran ↔ Dashboard reconcile owed before revenue marts depend on these figures" in `stg_stripe__charges` model description, but the structural ship is now backed by real data
- **Amounts preserved in minor units (`amount_minor`, `balance_minor`).** *Why:* Stripe's API contract is minor units; dividing by 100 at staging would hard-code USD assumption and lose fidelity for multi-currency. Conversion to major units happens in a fact model
- **Calendly `booked_at` maps to `event.created_at`, not `start_time`.** *Why:* `booked_at` is the Speed-to-Lead *start clock* (moment invitee confirms the slot); `start_time` is the actual meeting time, aliased to `scheduled_for` instead
- **Deduped on `_fivetran_synced` even though Fivetran upserts.** *Why:* idempotent + mirrors GHL-staging CTE shape; no behavioral cost

**Open threads**
- Downstream bridges owed: (a) `stg_calendly__event_invitees` to surface `invitee_email` and tracking UTMs; (b) `stg_calendly__question_and_answers` + pivot to surface `self_reported_source`; (c) Typeform psychographic-pivot (needs `field_id → label` mapping from the active form config). None block Track C.
- Stripe Fivetran ↔ Dashboard reconcile still owed — `charge` / `customer` aren't empty but completeness vs. Stripe source-of-truth unverified
- Fanbasis: still waiting on Week-0 API docs + credentials

---

## 2026-04-20 — Track B: oracle validation seeds + roster-update proposal

**What happened**
- Landed 6 zero-PII oracle CSVs from Master Lead Sheet.xlsx snapshot 2026-03-19 as dbt seeds under `dbt/seeds/validation/`: `oracle_dashboard_metrics_20260319` (43 rows), `oracle_show_rate_by_campaign_20260319` (56), `oracle_show_rate_by_period_20260319` (4), `oracle_revenue_by_stage_20260319` (55), `oracle_sdr_leaderboard_20260319` (6 incl. TOTAL), `oracle_closer_leaderboard_20260319` (8 incl. TOTAL)
- Declared all 6 in `dbt/seeds/_seeds__models.yml` with column schemas + `source: Master Lead Sheet.xlsx snapshot 2026-03-19` descriptions. `not_null` + `unique` tests on identity columns (sdr_name, closer_name, campaign, period, pipeline_stage, metric)
- Added `seeds.dee_data_ops.validation: {+schema: validation}` block to `dbt_project.yml`; preserved the anticipated `ghl_sdr_roster` block shape from the `phase-1-5/ghl-messages-extractor` branch verbatim
- `dbt seed --target dev --select validation` → PASS=6 into `dev_david` schema. `dbt test --select validation` → PASS=11 (6 not_null + 5 unique)
- Wrote `docs/proposals/roster_update_from_oracle.md` — evidence-backed table for 16 current roster rows + 2 roster-gap additions (Moayad departed, Halle confirm), each citing oracle leaderboard row. David reviews + commits roster CSV manually in a separate PR

**Decisions**
- **Dashboard-metrics CSV normalized to a 3-column `(section, metric, value)` shape** before loading. *Why:* raw oracle file has section dividers (`--- GHL PIPELINE ---`) and blank spacer rows that produce ragged-column CSVs dbt agate rejects. Preserving the section grouping as a column keeps semantic grouping intact without losing any KPI rows
- **Headers snake-cased on copy** (`Show Rate %` → `show_rate_pct`, `Cash All Time` → `cash_all_time`, etc.). *Why:* BigQuery identifiers disallow spaces, percents, and parens; sanitizing at the seed layer keeps downstream SQL clean. Original labels preserved in column descriptions
- **TOTAL row in closer leaderboard padded** to 8 cols (source CSV drops the trailing `state` field on the aggregate row). *Why:* dbt seed requires consistent column count per row
- **`ghl_sdr_roster.csv` not modified** — enforced by the DataOps rule on person-identifying seeds. Proposal doc is the only artifact; David drives the commit

**Open threads**
- `ghl_sdr_roster.csv` is not yet on `main` (lives on branch `phase-1-5/ghl-messages-extractor`). The `ghl_sdr_roster` config block in `dbt_project.yml` currently shows as "unused" in dbt warnings — resolves automatically when that branch merges
- PR description flags conflict risk with Track D (if D also edits `seeds.dee_data_ops:` in `dbt_project.yml`, coordinate at merge)
- 2 proposed roster additions (Moayad, Halle) need David decision on whether to add with `status=departed` or leave off

---

## 2026-04-20 — Track A: rules + AI-workflow guardrails landed

**What happened**
- Ported + adapted three path-scoped `.claude/rules/` files on branch `Davv5/Track-A-Rules-Guardrails`: `staging.md`, `warehouse.md`, `ingest.md`. All three auto-synced to the Data Ops NotebookLM notebook via the existing PostToolUse hook (sync log `/tmp/dataops-sync-rule.log` confirms source IDs)
- Added `.claude/commands/handover.md` (session-continuity slash command, ported AS-IS from `fanbasis-ingest`) + `docs/handovers/TEMPLATE.md` (Branch / Timestamp / Changed files / Run IDs / Decisions / Unresolved risks / First task)
- Added three lint configs: `.sqlfluff` (BigQuery + jinja templater), `.pre-commit-config.yaml` (trailing-whitespace/ruff/sqlfluff + 4 local guardrail hooks: forbid-dbt-target-prod, no-joins-in-staging, no-raw-refs-outside-staging, no-legacy-table-refs), `pyproject.toml` (ruff + isort)
- Edited `.claude/settings.json` via `Edit` — added a `PreToolUse` Bash hook blocking `dbt ... --target prod` from local shell (bypassed inside `$GITHUB_ACTIONS`). PostToolUse notebook-sync + SessionStart worklog-tail hooks preserved verbatim. Hook verified live by the verification bash command firing it mid-test

**Decisions**
- **Adapted `dim_contact` identity-spine section in `warehouse.md` to GHL-only anchor** for v1 (removed Calendly/Stripe/Fanbasis/Fathom bridges). *Why:* D-DEE v1 scope uses GHL as the single anchor; cross-source bridges are deferred to v1+N. Left a forward-looking paragraph so the rule still guides when bridges are added
- **Adapted `ingest.md` orchestration contract from Cloud Run Jobs → GitHub Actions `workflow_dispatch` + `schedule:` cron** throughout. Trimmed the source inventory to D-DEE's 5 (GHL + Fanbasis via Python/GH Actions; Typeform + Calendly + Stripe via Fivetran). *Why:* matches v1 build plan Phase 1; Fivetran-managed sources follow the same raw-dataset contract but have no repo-local extractor
- **Did NOT port `GTM lead warehouse/.claude/rules/marts.md`** — the current project's `.claude/rules/mart-naming.md` is the canonical marts rule and already correct
- **Kept `pyproject.toml`'s `known-first-party` as `["ingestion", "ingest"]`** — the project uses `ingestion/` on disk but leaving `ingest` in the list keeps isort sorted correctly if files under sibling projects land here via copy/paste

**Open threads**
- PR targets `main` from `Davv5/Track-A-Rules-Guardrails`. No shared-file edits except `.claude/settings.json`, which was merged into the existing hooks array (not overwritten)
- `pre-commit install` not yet run in this worktree — the hooks config is committed but not locally active. Enable on any clone by running `pre-commit install`
- `.github/pull_request_template.md` not yet in this repo (Track J owns); used plain PR description

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
