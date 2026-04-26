# Session Handover — Track W: scheduler migration (GHL extractor → Cloud Run Jobs + Cloud Scheduler, 1-min hot / 15-min cold)

**Branch:** `Davv5/Track-W-Live-By-Default-Scheduler-Migration-GHL`
**Timestamp:** `2026-04-22_20-15` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Session goal

Move the GHL custom extractor off GitHub Actions cron (5-minute minimum — can't hit the 1-min freshness target) onto **Cloud Run Jobs + Cloud Scheduler** in `dee-data-ops-prod`, split by endpoint heat:

| Endpoint group | Cadence | Endpoints |
|---|---|---|
| **Hot** | every 1 min | `conversations`, `messages` |
| **Cold** | every 15 min | `contacts`, `opportunities`, `users`, `pipelines` |

The existing `.github/workflows/ingest.yml` GHA workflow retains `workflow_dispatch` as a manual backstop / emergency backfill path but loses its `schedule:` cron for GHL. Fanbasis (week-0-deferred, not in v1 critical path) stays on the GHA matrix unchanged.

**When this ships:** a GHL webhook event that lands in the vendor's system at T+0 shows up in `raw_ghl.conversations` / `raw_ghl.messages` by T+60s, giving Track Y (incremental dbt + 2-min builder) the fresh raw data it needs to hit the ~1-min end-to-end STL freshness SLA.

## Explicit corpus deviation flag

This track **intentionally violates** `.claude/rules/ingest.md` as it stands today. That rule says:

> "GitHub Actions `workflow_dispatch` + `schedule:` cron is the **only** production trigger path for custom-source ingest. Not Airflow, not Dagster, not a local cron."
> (source: `.claude/rules/ingest.md`, Data Ops notebook; confirmed via `ask-corpus` query 2026-04-22)

The Data Ops corpus further explicitly warns against near-real-time ambitions:

> "Real-time sounds nice, but will it really change business decisions? Probably not."
> (source: *The Starter Guide for Modern Data*, Data Ops notebook)

**Why we're deviating:** the headline STL metric is a 5-minute SLA measurement. A dashboard that shows this metric on 5-minute-stale data is structurally incapable of catching a live 5-minute-SLA miss. 1-min freshness is a product requirement, not an engineering preference. David has signed off.

**Mandatory mitigation:** this track MUST edit `.claude/rules/ingest.md` in the same PR to carve a narrow, documented exception — Cloud Run Jobs + Cloud Scheduler are allowed specifically for **sub-5-min-cadence custom extractors where the freshness requirement is dashboard-load-bearing**. Daily / hourly sources still use the GHA cron path. The PostToolUse rule-sync hook will upsert the revised rule into the Data Ops notebook automatically.

## Changed files (expected)

```
ingestion/ghl/Dockerfile                             — created — slim python:3.11 image, runs extract.py
ingestion/ghl/.dockerignore                          — created — exclude .venv, __pycache__, tests
ingestion/ghl/extract.py                             — edited — accept --endpoints CSV flag + concurrency-guard via BQ advisory lock
ingestion/ghl/README.md                              — edited — document Cloud Run Job invocation + fallback GHA path
ops/cloud-run/ghl-extractor/terraform/main.tf        — created — Cloud Run Job + Cloud Scheduler jobs (hot + cold) + IAM bindings
ops/cloud-run/ghl-extractor/terraform/variables.tf   — created — project_id, region, image tag, SA email
ops/cloud-run/ghl-extractor/terraform/outputs.tf     — created — job names, scheduler job names
ops/cloud-run/ghl-extractor/terraform/README.md      — created — apply/destroy/rollback runbook
ops/cloud-run/ghl-extractor/build-and-push.sh        — created — docker build + gcloud artifacts docker push to AR repo
.github/workflows/ingest.yml                         — edited — drop `schedule:` from GHL matrix row; keep workflow_dispatch; Fanbasis cron unchanged
.github/workflows/cloud-run-deploy-ghl.yml           — created — on-merge-to-main, build image + push + `gcloud run jobs update` for hot+cold
.claude/rules/ingest.md                              — edited — carve Cloud Run Jobs exception for sub-5-min cadence extractors
.claude/rules/live-by-default.md                     — created IF Track Z has not landed yet; ELSE append a "scheduler" subsection
docs/runbooks/ghl-cloud-run-extractor.md             — created — how to roll back, pause scheduler, inspect logs
WORKLOG.md                                           — edited — dated entry with Cloud Run Job URLs, Scheduler job names, rollback procedure
docs/handovers/Davv5-Track-W-Execution-<timestamp>.md — created — handover doc
```

## Tasks (ordered)

### Pre-flight (no infra changes yet)

- [x] Read `.claude/rules/ingest.md` end-to-end. Understand what you're carving the exception *against*.
- [x] Read `ingestion/ghl/extract.py` — note: secrets already come from GCP Secret Manager via `_load_secret()` when `GCP_SECRET_MANAGER_PROJECT` env var is set (Track J delivered this). Cloud Run Jobs set that env var; no re-plumbing of secrets.
- [x] Read `ingestion/ghl/README.md` for the current run contract.
- [x] `ask-corpus scope: methodology.data_ops` the question: *"What's the minimum safe staging pattern when migrating a production ingest job from one scheduler to another? How do we avoid the two schedulers racing and double-ingesting while cutting over?"* — captured in WORKLOG entry.
- [x] **STOP checkpoint surfaced.** Endpoint split matches track "Decisions already made" and David's authorization. Hot: conversations + messages; cold: contacts + opportunities + users + pipelines. Proceeding per track plan.

### Containerize the extractor

- [x] Write `ingestion/ghl/Dockerfile` (also added `ingestion/__init__.py` + `ingestion/ghl/__init__.py` for `python -m` module invocation)
- [x] Write `ingestion/ghl/.dockerignore`
- [x] Add `--endpoints` CSV CLI flag + `--since` override to `extract.py`
- [x] Add BQ advisory lock (`raw_ghl._job_locks`) via MERGE + try/finally. Lock only engaged on Cloud Run path (`GCP_SECRET_MANAGER_PROJECT` env var present).
- [ ] **MANUAL CHECKPOINT W1: David smoke-tests the container in dev:**
  ```bash
  docker build -t ghl-extractor:dev -f ingestion/ghl/Dockerfile .
  docker run --rm \
    -e GCP_PROJECT_ID_DEV=dee-data-ops \
    -e GOOGLE_APPLICATION_CREDENTIALS=/sa.json \
    -v ~/sa-dev.json:/sa.json \
    ghl-extractor:dev --endpoints conversations --since 2026-04-22T00:00:00Z --dry-run
  ```
  Confirm output shows `{"endpoint": "conversations", "cursor": ..., "would_load": ...}`.
- [ ] **STOP — David confirms** container works before pushing to Artifact Registry.

### Provision infra (prod, staged)

- [x] Write `ops/cloud-run/ghl-extractor/build-and-push.sh`
- [x] Write Terraform: `main.tf`, `variables.tf`, `outputs.tf`, `README.md`
- [ ] **MANUAL CHECKPOINT W2: David runs `terraform plan`** (after container smoke-test passes):
  ```bash
  cd ops/cloud-run/ghl-extractor/terraform
  terraform init
  # If AR repo already exists from Track J:
  terraform import google_artifact_registry_repository.ingest \
    projects/dee-data-ops-prod/locations/us-central1/repositories/ingest
  terraform plan
  ```
  Confirm plan only touches resources under `ops/cloud-run/ghl-extractor/`. Share plan output before applying.
- [x] Secret Manager IAM already set (David fixed `ingest@dee-data-ops.iam.gserviceaccount.com` on `ghl-api-key` + `ghl-location-id` in `dee-data-ops-prod` this session — no action needed per track pre-context).
- [ ] **STOP — David confirms** `terraform plan` output before running `apply`.
- [ ] `terraform apply` — David runs. Capture Cloud Run Job URLs + Scheduler job names into WORKLOG.

### Cut over

- [ ] **MANUAL CHECKPOINT W3 (dual-run observation):** after `terraform apply`, let Cloud Run Jobs run for 15 minutes. Query:
  ```sql
  SELECT EXTRACT(MINUTE FROM _ingested_at) AS minute, COUNT(*) AS rows
  FROM `dee-data-ops.raw_ghl.conversations`
  WHERE _ingested_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
  GROUP BY 1 ORDER BY 1;
  ```
  Expect 10+ distinct minute values. Confirm no `_job_locks` deadlocks or error logs.
- [x] Edited `.github/workflows/ingest.yml`: added `if: matrix.source != 'ghl' || github.event_name == 'workflow_dispatch'` guard. GHL skips on cron; runs on `workflow_dispatch`. Fanbasis cron unchanged.
- [x] Write `.github/workflows/cloud-run-deploy-ghl.yml` (CD loop on merge to main touching `ingestion/ghl/**`)
- [ ] **STOP — David confirms the dual-run window looks clean** before this PR merges (which activates the `if:` guard on the GHA path).

### Documentation + rule updates

- [x] Edit `.claude/rules/ingest.md` — added "Near-real-time exception (Cloud Run Jobs)" subsection under **Orchestration**:
  ```markdown
  ### Near-real-time exception (Cloud Run Jobs)

  Custom extractors whose freshness SLA is **sub-5-minute AND dashboard-load-bearing**
  may run on Cloud Run Jobs + Cloud Scheduler instead of GitHub Actions cron.
  As of 2026-04-22 this applies ONLY to `ingestion/ghl/` hot endpoints
  (conversations, messages) at 1-min cadence, with cold endpoints
  (contacts, opportunities, users, pipelines) at 15-min cadence.

  All other custom extractors stay on the GHA path. The GHA workflow is
  retained as a manual backstop via `workflow_dispatch` — flip to it for
  emergency backfill if Cloud Scheduler is paused.

  Exception criteria (ALL must hold):
  - Dashboard tile SLA measures a sub-5-min event
  - Source API supports polling at the target cadence without exceeding rate limits
  - Concurrency guard exists (BQ advisory lock, no file locks)
  - Terraform-managed (not clicked in the GCP console)
  ```
- [ ] If `.claude/rules/live-by-default.md` already exists (Track Z may have landed first), append a short "Scheduler" section pointing at this rule. If it doesn't exist yet, defer that documentation to Track Z — the two rules don't cross-depend for execution correctness.
- [ ] Write `docs/runbooks/ghl-cloud-run-extractor.md`:
  - How to pause (`gcloud scheduler jobs pause ghl-hot --location=us-central1`)
  - How to manually trigger a one-off run (`gcloud run jobs execute ghl-hot --region=us-central1 --args="--endpoints=conversations,messages,--since=2026-04-22T00:00:00Z"`)
  - How to roll back to the GHA cron path (re-enable the `schedule:` in ingest.yml + pause the scheduler jobs — the GHA path stays dormant; no code changes needed)
  - How to read logs (`gcloud run jobs executions list --job=ghl-hot --region=us-central1 --limit=20`)
- [x] Appended WORKLOG entry per `.claude/rules/worklog.md`. Note: Cloud Run Job URLs + Scheduler job names will be added after `terraform apply` (pending David's confirmation). Dual-run observation summary will be added after W3 checkpoint.
- [x] Commit locally (do NOT push; do NOT open PR — that's `pr-reviewer`'s job per `.claude/rules/agents.md`).

## Decisions already made

- **Cloud Run Jobs over Airflow/Dagster/dbt Cloud.** Cloud Run is the lightest way to get sub-5-min cadence on a containerized Python job in GCP. No standing cost between runs (vs. an always-on Airflow scheduler). Same GCP-native auth as everything else in the repo. (source: deviation from `.claude/rules/ingest.md`; rationale documented in WORKLOG + in the rule exception itself.)
- **Same SA (`ingest@dee-data-ops.iam.gserviceaccount.com`) in both paths.** No new principal. Re-use the Secret Manager IAM bindings Track J already granted.
- **Destination project stays `dee-data-ops`** (not `-prod`). The extractor writes to `raw_ghl` in the dev project; dbt reads cross-project from prod. This matches the Track J secrets architecture — do NOT move the raw dataset in this track.
- **Concurrency guard lives in BigQuery, not Redis/Memorystore/GCS.** Already have BQ; a 2-minute advisory-lock table is trivial. Adding another backing store for one lock is scope creep.
- **1-min hot / 15-min cold split.** Conversations + messages drive the STL 5-minute metric; contacts/opportunities/users/pipelines drive identity and pipeline-stage enrichment which doesn't move on the minute. Pulling all six at 1-min cadence is wasted API budget (GHL rate limit: 100 req/10s).
- **GHA workflow retained as backstop, not deleted.** If Cloud Scheduler is paused or Cloud Run is down, `gh workflow run ingest.yml` still pulls GHL manually. Cost of keeping this path: zero.
- **Dedup stays downstream in staging.** Append-only raw + dedup in `stg_ghl__*` views is the existing contract (`.claude/rules/ingest.md` §3). Do NOT change this.
- **Fanbasis stays on GHA.** Week-0-deferred, not in the v1 STL critical path, and the weekly cadence doesn't justify Cloud Run.

## Open questions

- **Cloud Run Jobs concurrency-on-scheduler-overlap:** if a 1-min scheduled run starts at T and the prior run hasn't finished by T+60s, Cloud Run will queue the new execution. The BQ advisory lock we're adding will short-circuit the new execution to exit 0, but execution queuing will pile up if the extractor chronically runs >60s. **Pick sensible default:** set Cloud Run Job `max_instances = 1` AND the BQ lock expires at 2 min. If executions queue, alarm fires (existing Slack monitor for unusual `raw_ghl._job_locks` row count). David reviews if this happens.
- **Billing alarm:** 1-min cadence = 43,200 Cloud Run Job executions/month per hot endpoint group. At Cloud Run Jobs pricing that's a few dollars/month; trivial. **No decision needed.** Note the projected monthly cost in the WORKLOG entry.
- **Artifact Registry repo `ingest` — does it exist?** Track J *may* have created it for a different reason. **Pick sensible default:** `terraform import` it if it exists, create it if it doesn't. Either outcome is correct.
- **Does the `GCP_PROJECT_ID_DEV` env var name stay?** The variable is misleadingly named (extract.py writes to the "dev" project even in prod — that's the raw-landing architecture). **Pick sensible default:** leave the name alone in THIS track. Renaming it is a different concern and would touch GHA + Secret Manager + Terraform. Track the rename in BACKLOG.md as a followup.

## Done when

- `ghl-hot` Cloud Run Job fires every minute in `dee-data-ops-prod`, pulling conversations + messages.
- `ghl-cold` Cloud Run Job fires every 15 min, pulling contacts + opportunities + users + pipelines.
- `raw_ghl.conversations._ingested_at` shows new rows landing at 1-minute cadence (confirm via `SELECT EXTRACT(MINUTE FROM _ingested_at), COUNT(*) FROM raw_ghl.conversations WHERE _ingested_at > current_timestamp() - INTERVAL 15 MINUTE GROUP BY 1`).
- GHA `ingest.yml` no longer runs GHL on schedule (confirm the next 06:00 UTC cron skips it).
- `workflow_dispatch` manual-backstop path still works (`gh workflow run ingest.yml` fires the Fanbasis row; GHL manual re-run via an `if` override if David ever wants it).
- `.claude/rules/ingest.md` documents the Cloud Run exception; the PostToolUse hook syncs it to the Data Ops notebook (check `/tmp/dataops-sync-rule.log`).
- WORKLOG entry includes the projected monthly Cloud Run cost, the Cloud Run Job URLs, and the dual-run observation summary.
- Handover doc produced.
- Commit sits locally, ready for `pr-reviewer`.

## Dependencies

- **Prerequisite:** none — this track is the upstream end of the live-by-default chain. Track Y (dbt incremental + 2-min builder) *consumes* this track's output but can execute in parallel as long as Track Y uses dev data for validation.
- **Blocks:** Track Z's end-to-end STL freshness tile will show stale data until this track ships. Track Y's 2-min dbt builder will appear to work (the models rebuild incrementally) but will show no real improvement in end-to-end freshness until this track lands.

## Manual-verification checkpoints (recap)

1. After Pre-flight — David confirms endpoint split.
2. After containerize — David confirms dev-run container works before push to AR.
3. After terraform plan — David reads the plan, confirms before apply.
4. After terraform apply + dual-run window — David confirms no row duplication before the GHA cron gets disabled for GHL.

## Context links

- `.claude/rules/ingest.md` — the rule being amended
- `.claude/rules/worklog.md` — WORKLOG format
- `.claude/rules/agents.md` — branch/worktree/commit conventions, `pr-reviewer` gate
- Data Ops notebook: `7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a`
- `ingestion/ghl/extract.py` — existing extractor (no rewrite; wrapping only)
- Track J handover (Secret Manager migration): `docs/handovers/Davv5-Track-J-Secret-Manager-migration-PR-template-hosted-dbt-docs-*`
- Track Y (dbt incremental + 2-min builder) — parallel-safe, runs next
- Track Z (Metabase live-by-default) — depends on Tracks W+Y for end-to-end freshness to actually be live
