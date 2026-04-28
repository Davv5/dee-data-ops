# bq-ingest dependency audit

**Status:** Discovery output. Created 2026-04-28 as Step 1 of the consolidation plan in [`docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`](../plans/2026-04-28-bq-ingest-consolidation-plan.md).

**Purpose:** before moving the bq-ingest source tree from `heidyforero1/gtm-lead-warehouse` into `dee-data-ops/services/bq-ingest/`, map every load-bearing edge (Python imports, SQL file references, env vars, secrets, Cloud Run service spec) so the move is mechanical and the deploy-from-new-home succeeds on the first try.

**Inputs audited:**
- Local clone `~/Documents/gtm-lead-warehouse`, `main` @ `515c89a` (hotfix: cast assigned_to_user_id to NULL in snapshot SELECT, PR #4)
- Live Cloud Run service `bq-ingest` in `project-41542e21-470f-4589-96d` / `us-central1`, revision `bq-ingest-00076-wtl` (generation 84)

## Top-level inventory

| Path in `gtm-lead-warehouse/` | Size | Verdict | Destination |
|---|---|---|---|
| `app.py` | 12 KB | **MOVE** | `services/bq-ingest/app.py` |
| `sources/` | 416 KB / 32 .py | **MOVE** | `services/bq-ingest/sources/` |
| `ops/` | 188 KB / 6 .py + 17 .sh + 2 .yaml | **MOVE** | `services/bq-ingest/ops/` |
| `sql/` | 528 KB / 25 .sql | **MOVE** (preserve tree) | `services/bq-ingest/sql/` |
| `enrichment/` | 80 KB / 3 .py + own Dockerfile | **DROP** — separate sub-project, zero references from bq-ingest | (split to its own repo if still useful, otherwise delete) |
| `dbt/` | 44 KB | **DROP** — `2-dbt/` in dee-data-ops is canonical | — |
| `requirements.txt`, `pyproject.toml`, `.python-version`, `.dockerignore` | small | **MOVE** | `services/bq-ingest/` (root of service) |
| `.env.example` | 2.2 KB | **MERGE** into dee-data-ops `.env.example` | — |
| `RUNBOOK.md`, `ARCHITECTURE.md`, `CLEANUP_PLAN.md`, `README.md` | mixed | **MERGE / DROP** per plan §"What does NOT move" | service-scoped runbook lands at `services/bq-ingest/RUNBOOK.md` |
| `.claude/`, `.sqlfluff`, `.pre-commit-config.yaml` | small | **DROP** — duplicates dee-data-ops tooling | — |

Total to move: ~1.2 MB, 41 Python files (matches the plan's estimate). The 80 KB enrichment/ pile and 44 KB dbt/ pile drop.

## Python import map

`grep -rEn "^(from|import) (sources|ops|enrichment|app)" --include="*.py"` across the tree returns 33 in-tree edges. All use the absolute `sources.X.Y` and `ops.runner.tasks` patterns rooted at the package directory. Edge summary:

**`app.py` imports from:**
- `ops.runner.tasks` → `run_task`
- `sources.calendly.calendly_pipeline` → 4 names
- `sources.fanbasis.fanbasis_pipeline` → multi-name (paren import)
- `sources.fathom.fathom_pipeline` → 3 names
- `sources.ghl.ghl_pipeline` → 4 names (incl. `snapshot_pipeline_stages_daily`)
- `sources.shared.{analyst, data_quality, phase1_release_gate, warehouse_healthcheck, warehouse_queries}` → 6 names total
- `sources.typeform.typeform_pipeline` → 3 names

`app.py` does **not** import `sources.stripe`, `sources.identity`, or `sources.marts`. Those are reached via `ops/runner/cli.py` (the manifest-job path) instead.

**`ops/runner/cli.py` imports from:**
- `ops.runner.tasks` → 3 names
- `sources.{calendly, fanbasis, fathom, ghl, stripe, typeform}.X_pipeline` (paren imports of pipeline functions)
- `sources.shared.warehouse_healthcheck` → `run_healthcheck`

**Cross-source edges** (sources/ talking to itself):
- `sources/marts/mart_models.py` lazy-imports `sources.ghl.ghl_pipeline.run_models` and `sources.typeform.typeform_pipeline.run_models` (inside a function — used to drive consolidated mart refresh).
- `sources/shared/phase1_release_gate.py` imports `sources.shared.warehouse_healthcheck.run_healthcheck`.
- All `*_backfill.py` files import their sibling `*_pipeline.py`.

**Edges that touch `enrichment/`:** zero. `grep -rEn "from enrichment|import enrichment" --include="*.py"` returns no results. The only mention of `enrichment` in any .py is in `enrichment/fathom/test_google_auth.py` (a script inside enrichment itself). `enrichment/fathom/README.md` confirms: *"This folder contains the transcript-analysis enrichment workflow, separate from core warehouse ingest."*

**`sys.path` manipulation** (the hidden-import risk flagged in the plan): exactly **one** occurrence:

```python
# ops/scripts/cloud_python_dispatch.py:7-10
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
```

After the move, `parents[2]` walks `services/bq-ingest/ops/scripts/cloud_python_dispatch.py` → `…/scripts/` → `…/ops/` → `services/bq-ingest/`, which is the correct package root. **No code change required** as long as the tree moves as a unit.

## SQL file inventory

25 SQL files under `sql/`. Three categories:

**Referenced by Python** (executed by `*_pipeline.run_models()`):

| SQL file | Python referrer(s) |
|---|---|
| `sql/calendly_models.sql` | `sources/calendly/calendly_pipeline.py` |
| `sql/data_quality_tests.sql` | `sources/shared/data_quality.py` |
| `sql/fathom_models.sql` | `sources/fathom/fathom_pipeline.py` |
| `sql/ghl_models.sql` | `sources/ghl/ghl_pipeline.py` |
| `sql/marts.sql` | `sources/marts/mart_models.py` |
| `sql/models.sql` | `sources/{fanbasis, typeform, marts}/*_pipeline.py` |
| `sql/phase1_release_gate.sql` | `sources/shared/phase1_release_gate.py` |
| `sql/stripe_models.sql` | `sources/stripe/stripe_pipeline.py` |
| `sql/typeform_models.sql` | `sources/typeform/typeform_pipeline.py` |

**Referenced by bash scripts in `ops/scripts/`**:

| SQL file | Bash referrer |
|---|---|
| `sql/identity_gap_analysis.sql` | `ops/scripts/run_identity_gap_analysis.sh` |
| `sql/ingestion_parity_validation.sql` | `ops/scripts/run_ingestion_parity.sh` |
| `sql/master_lead_reliability_gate.sql` | `ops/scripts/run_master_lead_reliability_gate.sh` |

**No detected executor** (move-and-flag — likely hand-run validators):

- `sql/calendly_guardrails.sql`
- `sql/calendly_validation.sql`
- `sql/fathom_validation.sql`
- `sql/identity_bridge_kpi.sql`
- `sql/mart_validation.sql`
- `sql/speed_to_lead_validation.sql`
- `sql/dims/dim_offer.sql`
- `sql/validate/fathom_outcomes_week.sql`
- `sql/validate/lead_magnet_url_fields.sql`
- `sql/validate/mart_master_lead_wide.sql`
- `sql/validate/operations_kpi_panel.sql`
- `sql/validate/semantic_foundation_terms.sql`
- `sql/validate/typeform_core_tables.sql`

13 files in this pile. Recommendation: move them (preserve `sql/` tree as a unit so `Path(__file__).parent / "sql" / "X.sql"` patterns continue to resolve), but flag in the post-move PR that they have no executor wired up. Either resurrect a runner or delete in a follow-up; not a migration blocker.

## Cloud Run service spec snapshot

Captured via `gcloud run services describe bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d --format=yaml`.

**Identity:**
- Service: `bq-ingest`
- Project: `project-41542e21-470f-4589-96d`
- Region: `us-central1`
- Latest revision: `bq-ingest-00076-wtl` (generation 84, ready 2026-04-28T17:17:19Z)
- Service account: `id-sa-ingest@project-41542e21-470f-4589-96d.iam.gserviceaccount.com`
- Image: `us-central1-docker.pkg.dev/project-41542e21-470f-4589-96d/cloud-run-source-deploy/bq-ingest@sha256:693a8cb…`
- URL: `https://bq-ingest-mjxxki4snq-uc.a.run.app`

**Build path:** `--source` (Buildpacks). The `run.googleapis.com/build-source-location` annotation points at a `gs://run-sources-…/services/bq-ingest/1777396503.626197-….zip` upload — i.e. the source was uploaded from a local clone, **not** built from a Cloud Build trigger watching a git repo. This is the failure mode the consolidation is designed to eliminate.

**Resources:** cpu=1000m, memory=1024Mi (post-bump 2026-04-28 — clears the 512Mi OOMs flagged in the plan). containerConcurrency=80, maxScale=3, timeoutSeconds=300.

**Buildpack image:** `us-central1-docker.pkg.dev/serverless-runtimes/google-24/run/universal:public-image-next` (the universal builder where 3.11 was dropped 2026-04-14; `.python-version` pin of 3.13 is load-bearing).

**Plain env vars** (52 total — full list in the YAML capture, not duplicated here). Notable groups:
- BigQuery: `GCP_PROJECT_ID`, `BQ_DATASET=Raw`, plus per-source `BQ_*_RAW_TABLE` / `BQ_*_STATE_TABLE`
- GHL: `GHL_OBJECT_TYPES`, `GHL_LOCATION_ID=yDDvavWJesa03Cv3wKjt`, `GHL_PAGE_LIMIT`, `GHL_INCREMENTAL_LOOKBACK_HOURS`, `GHL_ENABLE_OUTBOUND_CALL_LOGS`, `GHL_ENDPOINT_OUTBOUND_CALL_LOGS=/conversations/messages/export`, `GHL_METHOD_OUTBOUND_CALL_LOGS=GET`, `GHL_PAGINATION_OUTBOUND_CALL_LOGS=none`
- Calendly: `CALENDLY_API_BASE`, `CALENDLY_OBJECT_TYPES`, `CALENDLY_ENABLE_PHASE1/2/3` (3 disabled), `CALENDLY_MAX_PAGES_*` per object, `CALENDLY_WEBHOOK_REQUIRE_SIGNATURE=false`
- Fathom: `FATHOM_API_BASE`, `FATHOM_WORKSPACE_ID=default`, `FATHOM_OBJECT_TYPES=calls`, `FATHOM_INTERNAL_EMAIL_DOMAINS=fanbasis.com`
- Fanbasis: `FANBASIS_PAGE`, `FANBASIS_PER_PAGE`, `BQ_TXN_TABLE=fanbasis_transactions_txn_raw`
- Runtime: `GUNICORN_CMD_ARGS=--timeout 180 --graceful-timeout 180`, `*_RUN_MODELS_AFTER_INCREMENTAL=true` flags

**Secret refs** (5 total, all `valueFrom.secretKeyRef`):

| env var | secret name | version |
|---|---|---|
| `FANBASIS_API_KEY` | `Secret` | `latest` |
| `GHL_ACCESS_TOKEN` | `GhlAccessToken` | `latest` |
| `FATHOM_API_KEY` | `FathomApiKey` | `latest` |
| `CALENDLY_API_KEY` | `CalendlyApiKey` | `latest` |
| `TYPEFORM_API_KEY` | `typeform-api-key` | `2` |

Per the plan's risk note, these survive the migration as-is — they're attached to the service revision, not the source. The service account `id-sa-ingest@project-41542e21-470f-4589-96d` already has access to all five. **No secret rotation needed for the move.** Only sanity-check: confirm IAM `secretmanager.secretAccessor` on `id-sa-ingest@…` for each of the five secrets before redeploying from the new home. (One-shot: `for s in Secret GhlAccessToken FathomApiKey CalendlyApiKey typeform-api-key; do gcloud secrets get-iam-policy "$s" --project=project-41542e21-470f-4589-96d --flatten="bindings[].members" --filter="bindings.members:id-sa-ingest" --format="value(bindings.role)"; done` — five rows of `roles/secretmanager.secretAccessor` is the expected output.)

**`.env.example` diff:** the gtm-lead-warehouse `.env.example` has an extra `GUNICORN_CMD_ARGS` block plus per-source ingestion knobs (`GHL_*`, `CALENDLY_*`, `FATHOM_*`, `FANBASIS_*`, `TYPEFORM_*`) that the dee-data-ops `.env.example` does not document. Step 3 (pointer updates) needs to merge these into `dee-data-ops/.env.example` so a fresh dev environment can run the service locally.

## Cloud Build trigger — recommended for Step 5

The plan marks Step 5 (Cloud Build path-watch trigger) as "optional but recommended." **Recommendation: treat it as required, not optional.** Without it, the consolidation reduces but does not eliminate the failure mode — anyone could still clone dee-data-ops to `~/Documents/dee-data-ops-staging-clone-2/` and `gcloud run deploy --source` from there. The point of consolidation is that the canonical source-of-truth for what's deployed lives at a single git ref; a path-watching trigger on `services/bq-ingest/**` enforces that.

**Setup outline** (defers actual config to Step 5's PR):
1. Connect GitHub repo `Davv5/dee-data-ops` to Cloud Build (one-time, IAM-mediated).
2. Create trigger: branch=`^main$`, included files=`services/bq-ingest/**`, build config=inline `cloudbuild.yaml` that runs `gcloud run deploy bq-ingest --source services/bq-ingest --region us-central1 --memory 1024Mi`.
3. The trigger uses the existing build SA `run-build-sa@project-41542e21-470f-4589-96d`; verify it has `run.developer` on the bq-ingest service.

**Risk if deferred:** no behavioral risk, but the consolidation's stated goal ("kill the repo-fork hazard at the root") is only partially met. Stale-clone-deploy is still possible from `~/Documents/data ops/` itself.

## Pre-flight checklist for Step 2 (the actual code move)

Before opening the code-move PR, confirm:

- [ ] `services/` directory does not yet exist in dee-data-ops. (Confirmed 2026-04-28 — `ls services/` → "no services/ dir yet".)
- [ ] No open PR in dee-data-ops touches the planned destination paths. (Confirmed 2026-04-28 — open PRs are #50 and #44, both Metabase work.)
- [ ] `id-sa-ingest@project-41542e21-470f-4589-96d` retains `secretmanager.secretAccessor` on all 5 secrets. (Verify with the one-shot above.)
- [ ] `.python-version` (`3.13`) and `requirements.txt` (`requirements.txt`) both move with the source. The buildpack reads `.python-version` at build time — without it, the universal builder defaults to whichever Python it last shipped, which is the issue PR #4 was hot-fixing.
- [ ] `dee-data-ops/.gitignore` doesn't exclude `services/**` or `*.zip` patterns that would interfere with `gcloud run deploy --source`. (Confirm by reading.)

## What this audit explicitly did not check (deferred to Step 4)

- Whether the Cloud Run revision's running image actually hits `/routes` and matches a route inventory derived from `app.py`. That's a deploy-time parity check, not a pre-move audit.
- Whether the Cloud Scheduler / Cloud Run Jobs (`ops/cloud/jobs.yaml`) need to be re-pointed at the new image after the consolidated deploy. Spot check: `jobs.yaml` references the existing image `cloud-run-source-deploy/fanbasis-python-runner:latest` for the manifest-driven jobs path — that image is independent of `bq-ingest` (separate Cloud Run Job, separate deploy lifecycle). The bq-ingest Cloud Run **service** consolidation does not affect the Cloud Run **Jobs** image. Verify in Step 4 that the post-move deploy doesn't accidentally rebuild the jobs image.

## Conclusion

The migration is mechanical. No hidden imports, no `sys.path` traps that survive the move, no secret rotation, no env-var schema break. `enrichment/` cleanly drops. `sql/` has 13 orphan files but they move with the tree (preserve `Path(__file__).parent / "sql"` resolution) and get a follow-up cleanup pass.

The one judgment call is the Cloud Build trigger. This audit recommends moving it from "optional" to "required" in Step 5; otherwise Step 6's archival of `gtm-lead-warehouse` doesn't actually remove the stale-local-clone hazard the consolidation exists to fix.

Step 2 (the code move PR) is unblocked.
