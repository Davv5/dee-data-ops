# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-28 late evening (post-PR #100 bq-ingest dependency audit + auth-posture fix in production)._

## Where we are

- **`bq-ingest` is no longer publicly invokable.** PR #100's audit caught `allUsers → roles/run.invoker` on the Cloud Run service — anyone could have hit `/refresh-models` or `/snapshot-pipeline-stages` and triggered BigQuery writes. Removed in-session after verifying the 8 Cloud Scheduler entries authenticate via OIDC (`sa-scheduler@`) and that no Calendly webhook subscription targets the service (Calendly subs both point at Zapier hooks). Both Cloud Run-generated URLs verified closed (unauth=403, auth=200).
- **Step 1 of the bq-ingest consolidation is done.** PR #100 lands `docs/discovery/bq-ingest-dependency-audit.md` (417 lines, two rounds of multi-persona doc review). Step 2 (the actual `git mv` into `services/bq-ingest/`) is unblocked.
- **Audit surfaced 7 deferred follow-ups** beyond Step 2 itself — Cloud Run Jobs image rebuild, `1-raw-landing/` consolidation, SQL resolution cleanup (6 of 9 modules use a broken `parent / "sql"` default), secret hygiene (4 of 5 secrets resolve via `latest`; one is named literally `Secret`), orphan SQL audit, services/ polyrepo precedent. Tracked in §"Deferred follow-ups" of the audit doc.
- **GHL transition snapshots remain LIVE.** `Core.fct_pipeline_stage_snapshots` compounds daily at 07:00 UTC. First usable transition signal in ~6 days.
- **`bq-ingest` service** still on revision `bq-ingest-00076-wtl`, Python 3.13, 1024Mi. The audit captured the full env-var spec, secret refs, and IAM policy — these are the inputs Step 4's parity check uses.
- **Operational-health rule lives on main.** `.claude/rules/operational-health.md` distinguishes pausable vs non-pausable work; loud freshness gate replaces `|| true` in `dbt-nightly.yml`.
- **Phase B layer-build still on main** (PRs #84/#86/#88/#90/#92): Fanbasis staging, identity-contact-payment bridge, payments/refunds rename, revenue_detail net-of-refunds. Local-CI tooling (PRs #94/#95) remains the dev-loop bypass when GH Actions degrades.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **bq-ingest consolidation:** `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md` — Step 1 (audit) shipped via PR #100. Audit at `docs/discovery/bq-ingest-dependency-audit.md`. **Next: Step 2 (the `git mv` into `services/bq-ingest/`) — fresh-context session, ~60 min.**
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-28 late evening** — PR #100 merged (audit + auth-posture fix). Two doc-review rounds (R1: 16 actionable findings + 5 FYI applied LFG; R2: caught the Calendly webhook regression my R1 fix nearly introduced + 11 other corrections). Audit reframed Step 5 (Cloud Build trigger) from "required" to "optional-but-recommended" — David is the sole operator, so the original "stale clone" threat model was wrong; right rationale is deploy provenance / build reproducibility.
- **2026-04-28 evening** — Brought GHL transition snapshots online via four-PR hotfix chain on `heidyforero1/gtm-lead-warehouse`. Production now on revision `00076-wtl`. Companion: PR #97 lands operational-health rule + loud freshness gate.
- **2026-04-28 evening** — Operational-health rule articulates the principle that emerged from the audit: extractor-pause silently destroyed data on operational pipelines (snapshot 404s, OOMs, vestigial Cloud Run Jobs). Pausable vs not-pausable distinction codified. Memory: `feedback_dont_dismiss_high_leverage_under_pause.md`.

## Open threads

- **Step 2 of the bq-ingest consolidation** (top of list) — `git mv` into `services/bq-ingest/`. Pre-flight checklist in the audit doc (§"Pre-flight checklist for Step 2") covers IAM, scheduler URI continuity, Python version triad, sub-dir build-context constraint.
- **bq-ingest deferred follow-ups (per audit §"Deferred follow-ups"):** Cloud Run Jobs image rebuild path, `1-raw-landing/` consolidation, SQL resolution cleanup (6 broken-default modules), secret hygiene (pin all to versions, rename `Secret`), orphan SQL audit (5 spec-only files), services/ polyrepo precedent. None block Step 2.
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
- **bq-ingest source (today):** `heidyforero1/gtm-lead-warehouse` repo. Local working copy: `~/Documents/gtm-lead-warehouse` (current main; **other local clones at `fanbasis-ingest` are stale — do not deploy from them**, deletion sequenced as Step 6 of consolidation)
- **bq-ingest production:** Cloud Run service `bq-ingest` in `project-41542e21-470f-4589-96d`, region `us-central1`, revision `bq-ingest-00076-wtl`. URL: `https://bq-ingest-mjxxki4snq-uc.a.run.app`. **Now requires authenticated invocation:** `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" /routes`.
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

- Last regen: 2026-04-28 late evening (post-PR #100 bq-ingest dependency audit + auth-posture fix in production).
- WORKLOG: skipped. Today's session is captured by destinations: PR #100 description + audit doc body cover the audit + auth-fix narrative; doc-review process value is in the PR comment thread; the audit's §"Deferred follow-ups" is the durable tracker for what didn't get done. No residual narrative needs WORKLOG.
