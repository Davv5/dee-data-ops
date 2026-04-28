---
paths: [".github/workflows/**", "1-raw-landing/**", "2-dbt/models/staging/**", "docs/runbooks/**"]
---

# Operational health — what to watch and what doesn't count

Load when working on ingest scheduling, CI/CD workflows, source freshness, or runbook content. Complements `ingest.md` (build-time contract) and `staging.md` (transform-time contract) with a third concern: **detecting silent decay in deployed extractors.**

## The principle: pausable vs not-pausable extractor work

Strategic pauses are a real tool — "we're not building new extractors this sprint" is a defensible steering decision. But the pause must NOT extend to operating *existing* pipelines that are silently breaking.

| Pausable (it's a steering call) | NOT pausable (it's silent data loss) |
|---|---|
| Adding a new extractor for a vendor we don't yet ingest | A scheduled ingest job that's failing every run |
| Filling a known schema gap (e.g. Typeform `form_id` upstream) | A deployed image missing routes its scheduler invokes |
| Vendor-API expansion (richer endpoints on an existing source) | A snapshot table that's been empty since creation because no one notices |
| Feature flags / behaviour changes inside an extractor | Ingestion succeeding but write semantics dropping data (MERGE-by-id when append was needed) |
| Documentation, refactors, naming hygiene | Memory-bound runs OOM-ing intermittently and the failures look like normal hiccups |

**Anti-anchor:** "this is in a paused work bucket" is not a tiebreaker. Re-evaluate each ask against the operational-health column above.

This rule was written 2026-04-28 after a routine `fct_opportunity_stage_transitions` audit surfaced three silent failures the Strategic Reset extractor pause had been hiding. Memory: `feedback_dont_dismiss_high_leverage_under_pause.md`.

## False-signal patterns (don't trust these)

These look like health signals but lie. Always cross-check.

1. **Cloud Run Job last-execution date.** Many of our schedulers do NOT invoke Cloud Run Jobs — they POST directly to the `bq-ingest` Flask service. Jobs like `ghl-incremental-v2` / `calendly-incremental-v2` show last-run `2026-04-19` and look "dead" but are vestigial. The real signal is in the service request logs.
2. **Cloud Scheduler `state: ENABLED`.** Means the scheduler is firing the trigger, not that the work is succeeding. A scheduler firing 200 OK against a route that doesn't exist looks identical to one firing successfully.
3. **`lastAttemptTime` showing a recent timestamp.** Same caveat — it's a "we tried" signal, not a "downstream succeeded" signal.
4. **HTTP 200 from `bq-ingest:/ingest-*`.** The endpoint returns 200 if Python ran without raising. It does NOT tell you whether new rows actually landed in raw — silent MERGE-by-id can succeed on every call while writing zero new rows because no upstream entity changed.

## True signals (use these)

| Signal | Interpretation |
|---|---|
| `MAX(_ingested_at)` per `raw_<source>.<source>__<obj>_raw` table | The only reliable freshness signal; everything else is a proxy |
| `dbt source freshness` against the configured `warn_after` / `error_after` blocks in `_<source>__sources.yml` | What CI gates on |
| `bq-ingest:/routes` (added in PR #1 of the originating `gtm-lead-warehouse` repo, now in `services/bq-ingest/app.py`) | Confirms which routes the running image actually serves — catches stale-deploy drift |
| Service-level structured logs (search for `ERROR` severity in `bq-ingest`) | Surfaces OOMs, BigQuery quota errors, vendor-API 5xx — the failures that don't reach the HTTP response |

## The 2026-04-28 incident — patterns that should not recur

Three failures discovered during one fact-table audit:

1. **Stale-deploy drift.** `bq-ingest` was deployed from `~/Documents/fanbasis-ingest` (local clone, app.py mtime 2026-04-13) while GitHub HEAD on `gtm-lead-warehouse` had newer routes (incl. `/snapshot-pipeline-stages`). Daily snapshot scheduler hit 404 silently for at least 2 days; `Core.fct_pipeline_stage_snapshots` had been empty since the table was created.
   - **Defense:** always `gcloud run deploy --source` from a fresh `git pull` of GitHub HEAD. After every deploy, `curl /routes` and grep for the routes you expect.
2. **Silent semantic data loss.** `snapshot_pipeline_stages_daily` SQL had `WHERE LOWER(status) NOT IN ('lost','abandoned','won')` — exactly the close transitions we most want for funnel analysis.
   - **Defense:** every WHERE clause in a snapshot/fact insert is a candidate for silent data loss. Code review should flag any filter that could exclude a load-bearing event class.
3. **Memory-bound intermittent OOM.** `bq-ingest` ran with 512Mi for `run_models()` calls that polled BQ jobs to completion; gunicorn worker overhead crossed the limit. Both Fathom and Calendly pipelines hit it.
   - **Defense:** Cloud Run service memory should match the largest payload it processes; OOMs that look "intermittent" are usually deterministic on the right input. Bump first, investigate second if recurrence persists.
4. **Buildpack Python version drift on first redeploy.** Restoring the snapshot route required `gcloud run deploy --source .`. The current GCP universal builder (`universal_builder_20260414_RC00`) ships only Python 3.13 / 3.14; previous prod ran 3.11. Without a pin, the redeploy autodetected 3.14 and the service boot-failed on a protobuf C-extension incompatibility. Rolled back, pinned `.python-version` to 3.13, redeployed.
   - **Defense:** any service deployed via buildpacks needs an explicit Python pin in the source tree. Verify after any builder upgrade that the pinned version is still in the supported list — the catalog drifts faster than the pin does.

### Where the bq-ingest source lives (today + planned)

**This repo, at `services/bq-ingest/`** — code consolidated from `heidyforero1/gtm-lead-warehouse@515c89a` via PR #102 (Step 2 of the consolidation plan, merged 2026-04-28). The full plan is at `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md` and the dependency audit at `docs/discovery/bq-ingest-dependency-audit.md`.

**Production still serves from the OLD repo** until Step 4 (first deploy from `services/bq-ingest/` with `/routes` parity check). The live revision `bq-ingest-00076-wtl` (gen 84) was deployed 2026-04-28 from `gtm-lead-warehouse` via Buildpack. When Step 4 ships, the deploy command MUST be `gcloud run deploy bq-ingest --source services/bq-ingest …` (NOT `--source .` — Buildpack autodetection won't find `requirements.txt` at the dee-data-ops monorepo root). Sister rule for service-side conventions: `.claude/rules/bq-ingest.md`.

## Freshness gates in CI

`dbt-nightly.yml` runs `dbt source freshness --target prod` once per day at 08:00 UTC. Today the step is `|| true` — warn-only, won't fail the workflow. **That choice is now superseded.** The intent ("a stale source shouldn't fail the whole nightly") was made before we had evidence that silent decay goes undetected for 9+ days.

Going forward:
- `dbt source freshness` runs **without** `|| true` — a source past its `error_after` threshold fails the nightly. GitHub Actions failure notification reaches David's inbox.
- Per-source `error_after` is set in the source YAML. Default: 36h for daily-cadence sources, 6h for hourly-cadence sources. Tighter than necessary on purpose — better to tune up after a false alarm than miss a real one.
- The check covers the contract from `ingest.md` §6 ("Logged: every run inserts into `ops.ingest_runs`"), so logs are the diagnostic surface; the gate is the alarm.

## When you're tempted to skip a freshness check

Don't. The 2026-04-28 incident was three failures stacked because nobody was looking. The cost of a false alarm (5 minutes diagnosing) is dwarfed by the cost of N days of silent loss.

If a source legitimately runs less often (e.g. weekly Fanbasis backfill), encode that in `error_after` — don't disable the check.

## Lessons learned

- *(Populate as new operational issues surface.)*
