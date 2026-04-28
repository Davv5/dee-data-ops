# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-28 night (post-PR #104 bq-ingest Step 3 pointer updates)._

## Where we are

- **Steps 1, 2, and 3 of the bq-ingest consolidation are done.** PR #104 merged at `dcc857a` — pointer updates: 5 absolute paths in `services/bq-ingest/ops/README.md` rewritten relative; `services/bq-ingest/pyproject.toml` exclusions cleaned; `.claude/rules/ingest.md` frontmatter extended to cover `services/bq-ingest/**`; new path-scoped rule `.claude/rules/bq-ingest.md` ported the two facts from dropped per-source CLAUDE.md files (Fathom split-runtime guardrail; Raw → Core → Marts ordering); RUNBOOK.md retitled with Python 3.13 vs py311 split note + heritage paragraph flagging stale Looker Studio + prod-target sections; `.env.example` extended with bq-ingest local-dev secrets (Calendly, Stripe, Typeform, Fathom, BQ_LOCATION); operational-health.md "Where the bq-ingest source lives" rewritten — `services/bq-ingest/` is canonical and the Step-4 deploy invariant (`--source services/bq-ingest`, NOT `--source .`) is locked in. Two reviewer rounds: 5 fixes pushed (5f19005), 2 items deferred to Step 4 with rationale. **Step 4 (first deploy from new home + `/routes` parity check) is unblocked.**
- **`bq-ingest` is no longer publicly invokable.** PR #100's audit caught `allUsers → roles/run.invoker` on the Cloud Run service — removed after verifying the 8 Cloud Schedulers authenticate via OIDC (`sa-scheduler@`) and Calendly subs target Zapier hooks. URLs verified closed (unauth=403, auth=200).
- **Step-4 punch list (now at the top of Open threads):** deploy command MUST be `--source services/bq-ingest` (not `--source .`); `ops/env/triage/` directory missing in BOTH source and dest (audit miss; triage scripts exit non-zero on missing config); 6 CWD-fragile shell scripts; `ops/runner/tasks.py:71` dispatches `backfill.calendly_invitee_drain` to a non-existent module (daily 03:45 UTC scheduler may already be silently failing); `app.py:42` returns stale `service: 'fanbasis-ingest'` brand string (intentionally kept for Step 4 byte-equal parity check, then flipped). Full thread: PR #102 comments + PR #104 round-2 review summary.
- **Audit's pre-existing 7 deferred follow-ups** still tracked in §"Deferred follow-ups" of `docs/discovery/bq-ingest-dependency-audit.md`.
- **GHL transition snapshots remain LIVE.** `Core.fct_pipeline_stage_snapshots` compounds daily at 07:00 UTC. First usable transition signal in ~6 days.
- **`bq-ingest` service** still on revision `bq-ingest-00076-wtl`, Python 3.13, 1024Mi, deployed from the OLD repo path. The audit captured the full env-var spec, secret refs, and IAM policy — these are the inputs Step 4's parity check uses.
- **Operational-health rule lives on main.** `.claude/rules/operational-health.md` distinguishes pausable vs non-pausable work; loud freshness gate replaces `|| true` in `dbt-nightly.yml`.
- **Phase B layer-build still on main** (PRs #84/#86/#88/#90/#92): Fanbasis staging, identity-contact-payment bridge, payments/refunds rename, revenue_detail net-of-refunds. Local-CI tooling (PRs #94/#95) remains the dev-loop bypass when GH Actions degrades.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **bq-ingest consolidation:** `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md` — Steps 1, 2, 3 shipped (PRs #100, #102, #104). **Next: Step 4 (first deploy from `services/bq-ingest/` with `/routes` parity check against the live revision).** Then Step 5 (optional Cloud Build trigger), Step 6 (archive `gtm-lead-warehouse` + delete stale local clones).
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-28 night (latest)** — PR #104 merged (Step 3 pointer updates). Two parallel reviewers (adversarial + project-standards) found 6 issues; 5 fixed in commit 5f19005, 2 deferred to Step 4 with rationale (the `service: 'fanbasis-ingest'` brand string would break byte-equal parity if changed early; full RUNBOOK rewrite is mechanical doc work that doesn't gate Step 4). New path-scoped rule `.claude/rules/bq-ingest.md` ports the two facts from dropped per-source CLAUDE.md files.
- **2026-04-28 evening** — PR #102 merged (Step 2 code move, 91 net files). Adversarial-review pass dropped 9 per-source `CLAUDE.md` files that would have auto-loaded broken doc paths; round-2 review confirmed the call and surfaced the Step 3 + Step 4 punch lists. Step 2 stayed cleanly mechanical.
- **2026-04-28 late evening** — PR #100 merged (audit + auth-posture fix). Two doc-review rounds; audit reframed Step 5 (Cloud Build trigger) from "required" to "optional-but-recommended" — David is the sole operator, so the original "stale clone" threat model was wrong; right rationale is deploy provenance / build reproducibility.

## Open threads

- **Step 4 of the bq-ingest consolidation (next)** — first deploy from `services/bq-ingest/`. **Deploy command MUST be `gcloud run deploy bq-ingest --source services/bq-ingest …`** (not `--source .` — Buildpack autodetection scans the source dir for `requirements.txt`, which doesn't exist at the dee-data-ops monorepo root). Pre-existing latent bugs to address before/during: missing `ops/env/triage/` directory (audit miss); 6 CWD-fragile shell scripts (`typeform_pipeline.py:383`, `manage_schedulers_from_manifest.py:145`, `run_data_quality.sh:7`, `run_master_lead_reliability_gate.sh:12`, `run_identity_gap_analysis.sh:12`, `run_ingestion_parity.sh:12`); `ops/runner/tasks.py:71` dispatches `backfill.calendly_invitee_drain` to a non-existent module (the daily 03:45 UTC scheduler is probably silently failing today — verify before redeploying); `app.py:42` returns stale `service: 'fanbasis-ingest'` (flip after parity check completes).
- **Step 4 RUNBOOK follow-ups** — `services/bq-ingest/RUNBOOK.md` heritage paragraph flags two stale sections for rewrite: Looker Studio core rule #3 (BI direction is dabi per memory `project_bi_direction_dabi.md`) and the Phase-3 prod-target invocation paragraph (contradicts the `.claude/settings.json` hook block). Rewrite is mechanical doc work; sequenced after Step 4 ships.
- **bq-ingest pre-existing deferred follow-ups (per audit §"Deferred follow-ups"):** Cloud Run Jobs image rebuild path, `1-raw-landing/` consolidation, SQL resolution cleanup (6 broken-default modules; one of them — `mart_models.py`'s default `sql_file_path` — was surfaced as a latent bug by PR #104 review and is now documented in `.claude/rules/bq-ingest.md`), secret hygiene (pin all to versions, rename `Secret`), orphan SQL audit (5 spec-only files), services/ polyrepo precedent.
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
- **Full history:** `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-04-28 night (post-PR #104 bq-ingest Step 3 pointer updates).
- WORKLOG: skipped. Session shipped PR #104 with a thorough description and a round-2 review-finding comment (`4338668269`); the new rule `.claude/rules/bq-ingest.md` is its own self-documenting artifact. PR + rule + review comment cover the audit-log surface; routing rule "Code that shipped → PR description ✓" + "New convention or pattern → `.claude/rules/*.md` ✓" both apply.
