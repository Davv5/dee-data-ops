# bq-ingest — Runbook

Daily ops, incident response, backfills, validation gates. Written for the sole operator and any future collaborator picking up a broken pipeline at 2am.

**Python version note:** the service pins `3.13` in `.python-version`, while the rest of the dee-data-ops repo (root `pyproject.toml`, `AGENTS.md`) targets `py311`. The split is intentional — GCP Cloud Run universal builder `universal_builder_20260414_RC00` dropped 3.11, so bq-ingest's Buildpack deploys must use 3.13. See `.claude/rules/operational-health.md` for the originating incident. The dbt project under `2-dbt/` and the `1-raw-landing/` extractors stay on 3.11 since they don't deploy via Cloud Run.

**Heritage:** this code was consolidated from `heidyforero1/gtm-lead-warehouse` into `services/bq-ingest/` per `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md` (PR #102). Some references below still describe the old repo's deploy origin and will be updated as Step 4 lands.

## Core rules (non-negotiable)

1. **Safe change order: Raw → Core → Marts.** Never skip steps. Never touch a mart without first refreshing the upstream Core.
2. **All Python runs in Cloud Run Jobs.** Not local shell. (Dev loops + debug queries are fine locally; production schedules are not.)
3. **Looker Studio reports read from `Marts` only.** Point charts at `Raw` or wide `Core` only for debugging.
4. **Before any release, the validation gates pass.** See § Validation gates below.
5. **Never commit directly to `main`.** Always branch → PR → CI → merge.

## Daily rhythm

1. **Check Cloud Run + Scheduler succeeded** for GHL, Calendly, Fanbasis (and Fathom if enabled).
   ```bash
   gcloud scheduler jobs list --location=us-central1
   gcloud run jobs executions list --job=pipeline-run --region=us-central1 --limit=5
   ```
2. **Warehouse healthcheck** runs hourly (`warehouse-healthcheck-hourly`, cron `58 * * * *` America/New_York).
   - Endpoint: `POST /healthcheck-warehouse` on `app.py`
   - Local: `./ops/scripts/run_warehouse_healthcheck.sh`
   - Green = 200; red = 500 with failing check identified.
3. **After any model-affecting change** — refresh Core (per source) then run Marts:
   ```bash
   ./ops/scripts/run_mart_models.sh     # or POST /refresh-marts on app.py
   ```
4. **Spot check** → `sql/mart_validation.sql` (row counts, `match_status`, top `attribution_gap_reason`).

## When someone says "this number is wrong"

Five-step debug loop:

1. **Name the metric.** Point to `docs/guides/MART_METRIC_DICTIONARY.md`.
2. **Name the grain.** Lead vs payment vs monthly campaign row — they don't mix.
3. **Check freshness.** `mart_refreshed_at` on the Mart table. If stale, check upstream `_ingested_at` in Raw.
4. **Check joins.**
   - Payments: `fct_fanbasis_payment_line.match_status`, `match_method`, `bridge_status`. Unmatched/ambiguous identities are the #1 issue.
   - Leads: `dim_golden_contact.attribution_gap_reason` and `campaign_reporting`.
5. **Reproduce in BigQuery.** Filter one email or one `golden_contact_key` and trace GHL → Calendly bridge → Fanbasis dim. The identity spine either matched or didn't — diagnostics tell you which.

## Validation gates (run before any release)

All exit non-zero on failure, so they compose in shell pipelines.

```bash
ops/scripts/run_ingestion_parity.sh                # Source parity — must PASS on ghl_contacts
ops/scripts/run_phase1_release_gate.sh             # Revenue match — hard blocks, no soft failures
ops/scripts/run_master_lead_reliability_gate.sh    # Lead-field confidence distribution
ops/scripts/run_mart_models.sh                     # Refresh Core → Marts (legacy runner)
ops/scripts/run_warehouse_healthcheck.sh           # Freshness + row-count + critical-table presence
ops/scripts/validate_marts.sh                      # Mart-layer schema + cardinality checks
```

Canonical manifest of what runs where: `ops/cloud/jobs.yaml`.

**Phase 3 cutover:** `run_mart_models.sh` flips from imperative SQL runner to `dbt build --target prod --select marts` — after the 14-day FARM_FINGERPRINT parity window proves the dbt ports are byte-for-byte equivalent. Legacy runner retires only after parity gate passes.

## Incident response

### Scheduled job failed

1. **Identify the job.**
   ```bash
   gcloud run jobs executions list --job=<job-name> --region=us-central1 --limit=10
   gcloud run jobs executions describe <execution-id> --region=us-central1
   ```
2. **Read logs.**
   ```bash
   gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="<job-name>"' --limit=50 --freshness=1h --format='value(textPayload)'
   ```
3. **Common causes + fixes.**
   - API rate limit → back off, retry via `gcloud run jobs execute <job-name>`.
   - Auth token expired → rotate via `ops/scripts/rotate_api_key_secret.sh <secret-name> <env-var> <job-name>`. Secret Manager stores latest; job binding points at `:latest`.
   - Schema drift upstream → check `_ingested_at` on most recent Raw row; if JSON shape changed, add a staging-layer `coalesce` or cast.
   - Stale checkpoint → `select * from Ops.ingest_checkpoints where source='<source>'`; reset if pointer is wrong.

### Mart looks wrong

Walk the identity spine. Every mart attribution leak has a named diagnostic — see `ARCHITECTURE.md` § "Known attribution leaks."

### Looker dashboard blank

1. Check `mart_refreshed_at` on the mart powering the dashboard.
2. If stale, force a refresh: `POST /refresh-marts` on `app.py` or `./ops/scripts/run_mart_models.sh`.
3. If fresh but empty, check Looker's date filter (UTC vs local-time confusion is common).

## Backfills

Detailed per-source guidance: `docs/runbooks/BACKFILL_RUNBOOK.md`.

Hot path:

```bash
# GHL comprehensive backfill (all entities — scheduled daily at 03:07 UTC)
gcloud run jobs execute ghl-comprehensive-backfill --region=us-central1

# Typeform backfill (scheduled daily at 05:15 UTC)
gcloud run jobs execute typeform-backfill --region=us-central1

# Per-source date-windowed backfill (generic pattern)
gcloud run jobs execute <source>-backfill --region=us-central1 --args=--since=2026-01-01
```

Backfill state lives in `Ops.ingest_checkpoints`. If a backfill job dies mid-run, restart — the job resumes from the checkpoint.

## Identity coverage checkpoint

Before adding new matcher methods (new email canonicalization, new phone rule, etc.) run the identity gap analysis. Add matchers **only** when the gap analysis shows material upside on unmatched revenue or unmatched customers.

```bash
./ops/scripts/run_identity_gap_analysis.sh
```

Details: `docs/runbooks/IDENTITY_RESOLUTION_RUNBOOK.md`.

## Per-source guardrails

- **Calendly** — `docs/runbooks/CALENDLY_GUARDRAILS.md`. Webhook idempotency, invitee dedupe, object gates, run caps.
- **GHL comprehensive backfill** — `docs/runbooks/GHL_COMPREHENSIVE_BACKFILL.md`. Multi-entity backfill sequencing (messages, notes, tasks, call logs, forms).
- **Fathom enrichment lane** — `docs/runbooks/CLOUD_RUN_SETUP.md`. Vertex AI LLM analysis; separate deploy from main ingest lane.

## Cloud baseline capture (before destructive ops)

Before deleting a Cloud Run Job, scheduler, or dataset — capture the current state so you can diff / recover:

```bash
./ops/scripts/capture_cloud_baseline.sh
```

This writes a timestamped snapshot of `gcloud run jobs list`, `gcloud scheduler jobs list`, and `bq ls`. Keep it until the destructive op is proven safe in production for at least one daily cycle.

## Branch + PR workflow

Full detail: `docs/runbooks/PR_WORKFLOW.md`.

Hot path:

```bash
git checkout main && git pull origin main
git checkout -b feature/<short-description>
# ... make changes ...
git add <specific files>
git commit -m "<imperative tense, what changed, why>"
git push -u origin feature/<short-description>
gh pr create
```

Branch protection on `main`: 1 review required; `ci` status check required (added Phase 4). No direct pushes.

## Agent execution rules

Full detail: `docs/runbooks/AGENT_EXECUTION_RULES.md`. The two rules you cannot break:

1. **No `python ingest/<source>/extract.py` from local shell against production BigQuery.** Ingest scripts run in Cloud Run Jobs only.
2. **No `dbt build --target prod` from local shell.** Production deploys through GitHub Actions only. (Phase 1 adds a `.claude/settings.json` hook that blocks this command pattern.)

## Looker Studio operations

- Data source: BigQuery, dataset `Marts` (same GCP project as Core).
- Service account needs `BigQuery Data Viewer` on `Marts` + job user on the project.
- Prefer pre-built mart tables over blended Looker sources. Business logic goes in SQL, not in Studio calculated fields.

## Dashboard specs (design-time references)

- `docs/dashboards/executive/SPEC.md`
- `docs/dashboards/marketing/SPEC.md`
- `docs/dashboards/sales/SPEC.md`
- `docs/dashboards/operations/SPEC.md`

## Escalation

You're the sole GTM engineer. Escalation path = stop shipping, capture the state, wait for business-hours debug. The cost of a broken mart is a delayed decision; the cost of a bad fix at 2am is a silent corruption that nobody notices until month-close.
