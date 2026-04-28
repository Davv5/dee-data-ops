# bq-ingest dependency audit

**Status:** Discovery output. Created 2026-04-28 as Step 1 of the consolidation plan in [`docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`](../plans/2026-04-28-bq-ingest-consolidation-plan.md). Revised 2026-04-28 evening following multi-persona doc review (PR #100).

**Purpose:** before moving the bq-ingest source tree from `heidyforero1/gtm-lead-warehouse` into `dee-data-ops/services/bq-ingest/`, map every load-bearing edge (Python imports, SQL file references, env vars, secrets, Cloud Run service spec) so the move is mechanical *for Step 2 specifically* and the deploy-from-new-home succeeds on the first try. Non-mechanical follow-ups surfaced by this audit are itemized in §"Deferred follow-ups" so they don't disappear into "we'll get to it."

**Inputs audited:**
- Local clone `~/Documents/gtm-lead-warehouse`, `main` @ `515c89a` (hotfix: cast assigned_to_user_id to NULL in snapshot SELECT, PR #4)
- Live Cloud Run service `bq-ingest` in `project-41542e21-470f-4589-96d` / `us-central1`, revision `bq-ingest-00076-wtl` (generation 84)
- Live Cloud Run IAM policy on the service (relevant to auth-posture finding below)

## Top-level inventory

| Path in `gtm-lead-warehouse/` | Size | Verdict | Destination |
|---|---|---|---|
| `app.py` | 12 KB | **MOVE** | `services/bq-ingest/app.py` |
| `sources/` | 416 KB / 32 .py | **MOVE** | `services/bq-ingest/sources/` |
| `ops/` | 188 KB / 6 .py + 17 .sh + 2 .yaml + 1 Dockerfile | **MOVE** | `services/bq-ingest/ops/` |
| `sql/` | 528 KB / 25 .sql | **MOVE** (preserve tree) | `services/bq-ingest/sql/` |
| `enrichment/` | 80 KB / 3 .py + own Dockerfile | **DROP** — separate sub-project, zero references from bq-ingest | (split to its own repo if still useful, otherwise delete) |
| `dbt/` | 44 KB | **DROP** — `2-dbt/` in dee-data-ops is canonical | — |
| `requirements.txt`, `pyproject.toml`, `.python-version` | small | **MOVE** | `services/bq-ingest/` (root of service) |
| `.env.example` | 2.2 KB | **MERGE** into dee-data-ops `.env.example` | — |
| `RUNBOOK.md`, `ARCHITECTURE.md`, `CLEANUP_PLAN.md`, `README.md` | mixed | **MERGE / DROP** per plan §"What does NOT move" | service-scoped runbook lands at `services/bq-ingest/RUNBOOK.md` |
| `.claude/`, `.sqlfluff`, `.pre-commit-config.yaml` | small | **DROP** — duplicates dee-data-ops tooling | — |

**Two Dockerfiles exist in the source tree** — call this out explicitly so neither is overlooked:

- `enrichment/fathom/Dockerfile` — drops with the rest of `enrichment/`.
- `ops/cloud/pipeline-runner/Dockerfile` — moves with `ops/`. This is the source of the `fanbasis-python-runner:latest` image referenced by `ops/cloud/jobs.yaml`. The bq-ingest **service** deploy uses Buildpacks (no Dockerfile); the Cloud Run **Jobs** deploy uses this Dockerfile. Different lifecycles, both must be considered post-move.

Total to move: ~1.2 MB, 39 Python files (32 in `sources/`, 6 in `ops/`, plus `app.py`). The 80 KB enrichment/ pile (3 .py) and 44 KB dbt/ pile drop. The plan's pre-audit estimate of 41 was slightly high — the 39 figure is post-audit ground truth.

## Coexistence with `1-raw-landing/`

This audit consolidates the bq-ingest **service**. It does NOT consolidate `1-raw-landing/{ghl,calendly,fanbasis}/` — a parallel ingestion stack already living in dee-data-ops with its own `Dockerfile`, `extract.py`, and dedicated GitHub Actions workflows (`cloud-run-deploy-ghl.yml`, `cloud-run-deploy-calendly.yml`) that build and deploy to Cloud Run Jobs in **`dee-data-ops-prod`** — a different GCP project than bq-ingest's `project-41542e21-470f-4589-96d`.

**Post-move state**, the repo holds two independent paths for the same sources, in two GCP projects:

| Path | Source code | Deploy target | Trigger |
|---|---|---|---|
| `services/bq-ingest/sources/ghl/` | (after Step 2) | Cloud Run **service** `bq-ingest` in `project-41542e21-470f-4589-96d` | Flask routes (HTTP), Cloud Scheduler hits |
| `1-raw-landing/ghl/` | (today) | Cloud Run **Job** in `dee-data-ops-prod` | GH Actions on push |

**Scope decision** (made deliberately rather than by archaeology): this audit treats bq-ingest as the consolidating canonical surface for incremental + webhook ingestion, and treats `1-raw-landing/` as a separate concern owned by the dee-data-ops-prod project's batch-job lifecycle. Whether to fold `1-raw-landing/` into `services/` (making it `services/raw-landing-ghl/` etc.) or keep it separate is **out of scope for this consolidation plan** but should be tracked as a follow-up before the next ingestion source lands.

The risk this section guards against is the consolidation looking complete after Step 6 while the same class of bug (deploy from a stale source path) still applies to `1-raw-landing/`.

## Python import map

`grep -rEn "^(from|import) (sources|ops|enrichment|app)" --include="*.py"` across the tree returns 33 in-tree edges. All use the absolute `sources.X.Y` and `ops.runner.tasks` patterns rooted at the package directory.

**Caveat: the grep is anchored.** It catches top-of-line static imports but misses (a) lazy imports inside function bodies and (b) `importlib.import_module` calls. The audit confirms both kinds are present in this codebase:

- `sources/marts/mart_models.py:32-33` lazy-imports `sources.ghl.ghl_pipeline.run_models` and `sources.typeform.typeform_pipeline.run_models` inside a function. Catalogued under "cross-source edges" below.
- `ops/runner/tasks.py:17` does `importlib.import_module(module_name)` driven by the `PYTHON_TARGET` env var. The TASK_REGISTRY at `tasks.py:70-78` maps short keys (`backfill.calendly`, `pipeline.full`, `pipeline.marts_refresh_hourly`, etc.) to `sources.X.Y:main`. The Cloud Run **Jobs** in `ops/cloud/jobs.yaml` invoke this dispatch path.

Both resolve correctly post-move because the package path stays `sources.<x>.<y>`, but the implication for **Step 4 parity** is: a `/routes` curl against the Cloud Run service is necessary but not sufficient. Step 4 must also exercise `python3 -m ops.runner.cli run backfill.<each-source>` against the new tree, since the dispatch surface is hidden from the static grep.

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

**Edges that touch `enrichment/`:** zero. `grep -rEn "from enrichment|import enrichment" --include="*.py"` returns no results. The only mention of `enrichment` in any .py is inside `enrichment/fathom/test_google_auth.py` (a script inside enrichment itself). `enrichment/fathom/README.md` confirms: *"This folder contains the transcript-analysis enrichment workflow, separate from core warehouse ingest."*

**`sys.path` manipulation** (the hidden-import risk flagged in the plan): exactly **one** occurrence:

```python
# ops/scripts/cloud_python_dispatch.py:7-10
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
```

After the move, `parents[2]` walks `services/bq-ingest/ops/scripts/cloud_python_dispatch.py` → `…/scripts/` → `…/ops/` → `services/bq-ingest/`, which is the correct package root. **No code change required** as long as the tree moves as a unit.

## SQL file inventory

25 SQL files under `sql/`. Resolution patterns first, then the per-file inventory.

### How `*_pipeline.run_models()` actually finds its SQL — important caveat

The Step 1 plan asserted that `Path(__file__).parent / "sql" / "X.sql"` patterns continue to resolve as long as the tree moves as a unit. **This is wrong for 7 of the 9 SQL-emitting modules.** Verified against current source:

- **Two modules resolve repo-root `sql/` correctly today** via `parents[2]`:
  - `sources/ghl/ghl_pipeline.py:1041-1045` has the safe pattern: `candidate = Path(__file__).parent / "sql" / "ghl_models.sql"; if candidate.exists() … else: parents[2] / "sql" / "ghl_models.sql"`. The fallback is the only branch that resolves.
  - `sources/shared/phase1_release_gate.py` uses `parents[2] / "sql" / "phase1_release_gate.sql"` directly.

- **Seven modules use the broken default** `Path(__file__).resolve().parent / "sql" / "X.sql"`, which evaluates to `sources/<source>/sql/X.sql` — a directory that does not exist on disk:
  - `sources/calendly/calendly_pipeline.py:1969`
  - `sources/fathom/fathom_pipeline.py:652`
  - `sources/fanbasis/fanbasis_pipeline.py:332`
  - `sources/marts/mart_models.py:39, 51` (the second is `sql/dims/`)
  - `sources/stripe/stripe_pipeline.py:342`
  - `sources/typeform/typeform_pipeline.py:387`
  - `sources/shared/data_quality.py:26` (this one has an env-var override `DQ_SQL_FILE` — the only env-var override of this kind in the codebase)

**Implication.** Either (a) the corresponding `/refresh-X-models` routes are silently inert in production today (every call returns ok with `statements_executed=0`), or (b) the deployed Cloud Run service is invoking these via an unenumerated mechanism the audit's env-var capture missed. The audit's earlier framing — "no code change required" — overstates the safety of the move; the real claim is "the move doesn't make a pre-existing latent bug worse."

**Step 4 must baseline behavior, not assume it.** Before declaring parity post-move, exercise each scheduled route against the *current* production revision (`bq-ingest-00076-wtl`) and capture which return success-with-side-effects vs success-with-zero-statements vs FileNotFoundError. The new revision must match that baseline, not a green `/routes` listing. This is the same class of failure as the operational-health rule's worked example #2 (the `/snapshot-pipeline-stages` 404 that the route-listing didn't catch).

### File inventory

**Referenced by Python** (executed by `*_pipeline.run_models()`):

| SQL file | Python referrer(s) | Resolution OK? |
|---|---|---|
| `sql/ghl_models.sql` | `sources/ghl/ghl_pipeline.py` | ✅ parents[2] fallback |
| `sql/phase1_release_gate.sql` | `sources/shared/phase1_release_gate.py` | ✅ parents[2] direct |
| `sql/data_quality_tests.sql` | `sources/shared/data_quality.py` | ⚠️ via `DQ_SQL_FILE` env override only |
| `sql/calendly_models.sql` | `sources/calendly/calendly_pipeline.py` | ❌ broken default |
| `sql/fathom_models.sql` | `sources/fathom/fathom_pipeline.py` | ❌ broken default |
| `sql/marts.sql` | `sources/marts/mart_models.py` | ❌ broken default |
| `sql/models.sql` | `sources/{fanbasis, typeform, marts}/*_pipeline.py` | ❌ broken default |
| `sql/stripe_models.sql` | `sources/stripe/stripe_pipeline.py` | ❌ broken default |
| `sql/typeform_models.sql` | `sources/typeform/typeform_pipeline.py` | ❌ broken default |

**Referenced by bash scripts in `ops/scripts/`**:

| SQL file | Bash referrer |
|---|---|
| `sql/identity_gap_analysis.sql` | `ops/scripts/run_identity_gap_analysis.sh` |
| `sql/ingestion_parity_validation.sql` | `ops/scripts/run_ingestion_parity.sh` |
| `sql/master_lead_reliability_gate.sql` | `ops/scripts/run_master_lead_reliability_gate.sh` |

### "No Python/bash executor" — but several are operator-run from runbooks

The audit's earlier "13 orphan files" framing was too aggressive. Re-grepping `docs/`, `RUNBOOK.md`, and runbook trees in the source repo surfaces operator-run patterns. Re-classified:

**Operator-run (preserve, sequence runbook updates with the move):**
- `sql/mart_validation.sql` — referenced from `RUNBOOK.md` (mart spot-check)
- `sql/calendly_guardrails.sql` — referenced from `docs/runbooks/CALENDLY_GUARDRAILS.md`
- `sql/calendly_validation.sql` — referenced from `docs/sources/calendly/INDEX.md`
- `sql/fathom_validation.sql` — referenced from `docs/sources/fathom/INDEX.md`
- `sql/identity_bridge_kpi.sql` — referenced from `docs/sources/identity/INDEX.md`
- `sql/speed_to_lead_validation.sql` — referenced from runbook docs
- `sql/validate/mart_master_lead_wide.sql` — referenced from `docs/AUDIT.md`
- `sql/validate/operations_kpi_panel.sql` — referenced from `docs/AUDIT.md`

**Spec-only / no observed executor (audit before drop):**
- `sql/dims/dim_offer.sql`
- `sql/validate/fathom_outcomes_week.sql`
- `sql/validate/lead_magnet_url_fields.sql`
- `sql/validate/semantic_foundation_terms.sql`
- `sql/validate/typeform_core_tables.sql`

Move the whole `sql/` tree as a unit (preserving the broken `parent / "sql"` resolution at the same level of brokenness — a no-op for the move). When Step 3's pointer-update PR rewrites `RUNBOOK.md` and the source-INDEX docs, every reference to `sql/<file>.sql` must be re-rooted to `services/bq-ingest/sql/<file>.sql`. If David doesn't recognize a file in the "spec-only" pile and can confirm no operator runs it, that's the deletion candidate list for the post-move cleanup.

## Cloud Run service spec snapshot

Captured via `gcloud run services describe bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d --format=yaml`.

**Identity:**
- Service: `bq-ingest`
- Project: `project-41542e21-470f-4589-96d`
- Region: `us-central1`
- Latest revision: `bq-ingest-00076-wtl` (generation 84, ready 2026-04-28T17:17:19Z)
- Runtime service account: `id-sa-ingest@project-41542e21-470f-4589-96d.iam.gserviceaccount.com`
- Image: `us-central1-docker.pkg.dev/project-41542e21-470f-4589-96d/cloud-run-source-deploy/bq-ingest@sha256:693a8cb…`

### Auth posture — finding flagged for immediate remediation

Verified `gcloud run services get-iam-policy bq-ingest`:

```yaml
bindings:
- members:
  - allUsers
  - serviceAccount:sa-scheduler@project-41542e21-470f-4589-96d.iam.gserviceaccount.com
  role: roles/run.invoker
```

**The service is publicly invokable.** Anyone on the internet can curl `https://bq-ingest-mjxxki4snq-uc.a.run.app/refresh-models`, `/snapshot-pipeline-stages`, `/run-data-quality`, etc. and trigger BigQuery write operations. This is independent of the migration but the audit must call it out:

- Either there's a deliberate reason the service allows unauthenticated invocations (likely none — `sa-scheduler@` is the only intended caller and it has its own binding), OR
- The `allUsers` binding is a leftover from an earlier test/debug iteration that was never tightened.

**Recommendation:** before the consolidation lands, restrict to authenticated callers. The Cloud Scheduler entries in `ops/cloud/jobs.yaml` already use `oidcToken.serviceAccountEmail`, so removing `allUsers` doesn't break the scheduled paths. One-shot: `gcloud run services remove-iam-policy-binding bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d --member='allUsers' --role='roles/run.invoker'`. Verify schedulers still fire; verify `/healthcheck` from a curl with `Authorization: Bearer $(gcloud auth print-identity-token)` continues to work; un-binding allUsers is reversible if anything breaks.

This is **out of scope for the bq-ingest source-tree move itself** but is the highest-priority finding the audit surfaced. Do not let it disappear into the consolidation work.

### Build path

`--source` (Buildpacks). The `run.googleapis.com/build-source-location` annotation points at a `gs://run-sources-…/services/bq-ingest/<timestamp>.zip` upload — i.e. the source was uploaded from a local clone, **not** built from a Cloud Build trigger watching a git repo. This is one of the failure modes the consolidation is designed to address.

### Resources + buildpack

cpu=1000m, memory=1024Mi (post-bump 2026-04-28 — clears the 512Mi OOMs flagged in the plan). containerConcurrency=80, maxScale=3, timeoutSeconds=300.

Buildpack image: `us-central1-docker.pkg.dev/serverless-runtimes/google-24/run/universal:public-image-next` (the universal builder where 3.11 was dropped 2026-04-14; `.python-version` pin of 3.13 is load-bearing).

### Plain env vars (52 total)

Notable groups (full list in the YAML capture, not duplicated here):
- BigQuery: `GCP_PROJECT_ID`, `BQ_DATASET=Raw`, plus per-source `BQ_*_RAW_TABLE` / `BQ_*_STATE_TABLE`
- GHL: `GHL_OBJECT_TYPES`, `GHL_LOCATION_ID=yDDvavWJesa03Cv3wKjt`, `GHL_PAGE_LIMIT`, `GHL_INCREMENTAL_LOOKBACK_HOURS`, `GHL_ENABLE_OUTBOUND_CALL_LOGS`, `GHL_ENDPOINT_OUTBOUND_CALL_LOGS=/conversations/messages/export`, `GHL_METHOD_OUTBOUND_CALL_LOGS=GET`, `GHL_PAGINATION_OUTBOUND_CALL_LOGS=none`
- Calendly: `CALENDLY_API_BASE`, `CALENDLY_OBJECT_TYPES`, `CALENDLY_ENABLE_PHASE1/2/3` (3 disabled), `CALENDLY_MAX_PAGES_*`, `CALENDLY_WEBHOOK_REQUIRE_SIGNATURE=false`
- Fathom: `FATHOM_API_BASE`, `FATHOM_WORKSPACE_ID=default`, `FATHOM_OBJECT_TYPES=calls`, `FATHOM_INTERNAL_EMAIL_DOMAINS=fanbasis.com`
- Fanbasis: `FANBASIS_PAGE`, `FANBASIS_PER_PAGE`, `BQ_TXN_TABLE=fanbasis_transactions_txn_raw`
- Runtime: `GUNICORN_CMD_ARGS=--timeout 180 --graceful-timeout 180`, `*_RUN_MODELS_AFTER_INCREMENTAL=true` flags

`.env.example` diff: the gtm-lead-warehouse `.env.example` has an extra `GUNICORN_CMD_ARGS` block plus per-source ingestion knobs that the dee-data-ops `.env.example` does not document. Step 3 (pointer updates) merges these into `dee-data-ops/.env.example`.

### Secret refs (5 total)

| env var | secret name | version |
|---|---|---|
| `FANBASIS_API_KEY` | `Secret` | `latest` |
| `GHL_ACCESS_TOKEN` | `GhlAccessToken` | `latest` |
| `FATHOM_API_KEY` | `FathomApiKey` | `latest` |
| `CALENDLY_API_KEY` | `CalendlyApiKey` | `latest` |
| `TYPEFORM_API_KEY` | `typeform-api-key` | `2` |

Per the plan's risk note, these survive the migration as-is — they're attached to the service revision, not the source. The runtime SA `id-sa-ingest@…` already has access to all five.

**Recommendation: pin all five to current versions during the migration**, mirroring the TYPEFORM pattern. Four of five resolve via `latest`, which means the next deploy after a key rotation auto-resolves to a brand-new credential with no validation gate between rotation and production. The migration is the natural moment to standardize since the service spec is being rewritten anyway. Add a per-secret rotation runbook entry: (1) create new version, (2) validate in dev, (3) update service spec to the pinned version, (4) redeploy.

The secret named literally `Secret` (Fanbasis API key) is operationally fragile (poor grep-ability, easy to misread as the noun). The migration is also the natural rename window: create `FanbasisApiKey`, grant access, update spec, deploy, deprecate `Secret`. Treat as low-priority cleanup, not a Step 2 blocker.

## Cloud Build trigger — recommended for Step 5 (optional-but-recommended, not required)

The plan marks Step 5 (Cloud Build path-watch trigger) as "optional but recommended." Earlier this audit argued for promoting it to *required*; the rationale was wrong. The original framing ("anyone could clone dee-data-ops to a stale path") assumes a multi-operator threat model, but David is the sole operator on this engagement (per CLAUDE.md). The actual repo-fork hazard the consolidation kills is the literal existence of a separate `heidyforero1/gtm-lead-warehouse` repo — not a class of "stale clone" problem.

**The trigger's real value proposition** for this engagement:
1. **Deploy provenance**: every revision's image SHA is traceable to a specific git ref via the build annotation.
2. **Build reproducibility**: `gcloud run deploy --source` from a local checkout is replaced by a deterministic CI build.
3. **Removes "deploy from local clone" as a class of action** (which is a separate goal from "defends against multiple humans clones").

These are good reasons. None of them require treating Step 5 as a blocker for Step 6 archival. **Step 5 stays optional-but-recommended; Step 6 (archive `gtm-lead-warehouse`) does not depend on Step 5.**

### Tightened trigger config (when Step 5 ships)

The minimal config from the plan was:
```
gcloud run deploy bq-ingest --source services/bq-ingest --region us-central1 --memory 1024Mi
```

Tightened version:
```
gcloud run deploy bq-ingest \
  --source services/bq-ingest \
  --region us-central1 \
  --memory 1024Mi \
  --service-account=id-sa-ingest@project-41542e21-470f-4589-96d.iam.gserviceaccount.com \
  --no-traffic
```

Plus, in the trigger spec itself:
- `includedFiles: services/bq-ingest/**`
- `ignoredFiles: services/bq-ingest/**/*.md, services/bq-ingest/RUNBOOK.md` — suppress no-op deploys on docs-only PRs that would reset cached state.
- A concurrency clause to serialize racing merges (single in-flight deploy at a time).

`--no-traffic` is critical for the first auto-trigger run: a docs-only PR catching the trigger, or a buggy revert merging to main, should produce a built-but-not-promoted revision. Manual `gcloud run services update-traffic bq-ingest --to-latest` after curl-verifying `/routes` and route behavior promotes it.

**Do NOT add `--clear-secrets` or `--update-secrets`** unless re-specifying all five secret refs explicitly. Omitting both flags preserves existing refs; adding `--clear-secrets` to "be explicit" wipes them.

### Deploy posture — accepted

The trigger fires on every merge to `main` touching `services/bq-ingest/**`. `CLAUDE.local.md` notes branch protection on `main` is deferred to Phase 6 (GH Free tier limitation). The current pre-merge gate is David's own review. Per the engagement's solo-operator pre-authorization (CLAUDE.md), this is the deliberate posture: David's merge IS the deploy approval.

The `--no-traffic` flag above adds a second checkpoint between merge and 100% traffic, so the posture is: merge → build → revision created at 0% traffic → manual promote after curl-verification. This preserves the "human in the loop" property even with auto-build.

If/when branch protection lands at Phase 6, revisit whether the manual-promote step can be relaxed.

## Deferred follow-ups (surfaced by this audit, not in scope for Step 2)

These are concerns the audit surfaced that don't block the Step 2 code move but must not disappear into "we'll get to it." Each gets its own GH issue or its own line in the Step 2 PR description:

1. **Auth posture**: remove `allUsers → roles/run.invoker` from bq-ingest IAM. (Highest priority — independent of the migration.)
2. **Cloud Run Jobs image rebuild**: the `fanbasis-python-runner:latest` image (built from `ops/cloud/pipeline-runner/Dockerfile`) builds from the same source tree the service uses. After the move, the build pipeline producing this image must point at the new path — otherwise the next jobs rebuild fails, or worse, succeeds off the archived `gtm-lead-warehouse` repo and silently re-introduces the stale-clone hazard. Either add a parallel CB trigger for the Jobs image (Step 5b) OR document the rebuild procedure in the new RUNBOOK.md.
3. **Jobs manifest path coexistence**: `ops/cloud/jobs.yaml` carries both `jobs[]` (Cloud Run Jobs) and `schedulers[]` (Cloud Scheduler). The schedulers' URI continuity is covered in pre-flight below. The jobs[] image is a separate scope decision: in scope for a future consolidation pass, out of scope for this plan.
4. **`1-raw-landing/` consolidation**: deferred to a separate plan. See §"Coexistence with `1-raw-landing/`."
5. **SQL resolution cleanup**: 7 of 9 `*_pipeline.run_models()` modules use the broken `parent / "sql"` default. Fix the defaults to follow the ghl/phase1 `parents[2]` pattern OR use env-var overrides. Not a Step 2 blocker; the migration preserves current behavior (broken or working) per-module. Track as cleanup ticket.
6. **Secret hygiene**: pin all 5 secret refs to versions, rename `Secret` → `FanbasisApiKey`. Cleanup, not blocker.
7. **Orphan SQL audit**: 5 spec-only SQL files (after the runbook re-classification above) need a kill/keep decision per file. Cleanup, not blocker.

## Pre-flight checklist for Step 2

Before opening the code-move PR, confirm:

- [ ] `services/` directory does not yet exist in dee-data-ops. (Confirmed 2026-04-28 — `ls services/` → "no services/ dir yet".)
- [ ] No open PR in dee-data-ops touches the planned destination paths. (Confirmed 2026-04-28 — open PRs are #50 and #44, both Metabase work; #100 is this audit.)
- [ ] `dee-data-ops/.gitignore` doesn't exclude `services/**` or `*.zip` patterns that would interfere with `gcloud run deploy --source`.

**Runtime SA secret access** (existing check, retained):

- [ ] `id-sa-ingest@project-41542e21-470f-4589-96d` retains `secretmanager.secretAccessor` on all 5 secrets:
  ```
  for s in Secret GhlAccessToken FathomApiKey CalendlyApiKey typeform-api-key; do
    gcloud secrets get-iam-policy "$s" \
      --project=project-41542e21-470f-4589-96d \
      --flatten="bindings[].members" \
      --filter="bindings.members:id-sa-ingest" \
      --format="value(bindings.role)"
  done
  ```
  Five rows of `roles/secretmanager.secretAccessor` is the expected output.

**Build SA permissions** (new — required for Step 4 deploy and Step 5 trigger):

- [ ] Step 4 deployer (David's user account) has `roles/run.developer` on the bq-ingest service, `roles/iam.serviceAccountUser` on `id-sa-ingest@…` (to attach the runtime SA to a new revision), and `roles/storage.objectAdmin` on the `gs://run-sources-*` bucket.
- [ ] Step 5 build SA `run-build-sa@project-41542e21-470f-4589-96d` has the same three roles, plus `roles/artifactregistry.writer` on the `cloud-run-source-deploy` AR repo (for image push).
  ```
  gcloud projects get-iam-policy project-41542e21-470f-4589-96d \
    --flatten="bindings[].members" \
    --filter="bindings.members:run-build-sa" \
    --format="table(bindings.role)"
  ```

**Cloud Scheduler URI continuity** (new):

- [ ] Enumerate every Cloud Scheduler job whose `httpTarget.uri` references a `bq-ingest-*.run.app` URL. Use the live API, not `ops/cloud/jobs.yaml` (which may be stale):
  ```
  gcloud scheduler jobs list --location=us-central1 --project=project-41542e21-470f-4589-96d --format=json \
    | jq -r '.[] | select(.httpTarget.uri | contains("bq-ingest")) | "\(.name)\t\(.httpTarget.uri)"'
  ```
  Confirm post-redeploy URIs match. Investigate the jobs.yaml inconsistency: most entries use `bq-ingest-mjxxki4snq-uc.a.run.app`, fathom-hourly-ingest uses `bq-ingest-535993952532.us-central1.run.app`. Cloud Run generates both forms; one may be a stale alias from a prior service iteration.

**Python version triad reconciliation** (new):

- [ ] Three Python versions exist in the source tree:
  - `.python-version` → `3.13` (Cloud Run Buildpacks runtime)
  - `pyproject.toml` → `target-version = "py311"` (Ruff lint target)
  - `ops/cloud/pipeline-runner/Dockerfile` → `FROM python:3.11-slim` (Cloud Run Jobs runtime)
  
  Decide before move: (a) bring all three to 3.13, OR (b) document the pipeline-runner Jobs runtime as a deliberate divergence in `services/bq-ingest/RUNBOOK.md` and pin Ruff to py311.
- [ ] `pyproject.toml`'s `[tool.ruff.lint.isort] known-first-party = ["ingest"]` references a package that doesn't exist (actual roots are `sources`, `ops`). Correct to `["sources", "ops"]` post-move.

**Buildpack version pin** (new — defends against the next 3.11-style drop):

- [ ] `.python-version` (`3.13`) moves with the source. The buildpack reads it at build time — without it, the universal builder defaults to whichever Python it last shipped, which is the issue PR #4 was hot-fixing.

## What this audit explicitly did not check (deferred to Step 4)

- Behavioral parity between the current revision (`bq-ingest-00076-wtl`) and the post-move revision. `/routes` parity is necessary but **not sufficient** — `/routes` lists registered handlers, not behavior. Per §"How `*_pipeline.run_models()` actually finds its SQL," Step 4 must also exercise each `/refresh-*` route and `python3 -m ops.runner.cli run backfill.<source>` against the new tree, capturing baseline `statements_executed` / `rows_written` / `ok=true` semantics from the current revision and matching against the new one.

## Layout precedent — services/ as a polyrepo bet

`services/bq-ingest/` is the first service-shaped directory in this repo. Today the repo is "the dbt project plus thin glue"; after this consolidation it becomes "a polyrepo: dbt project + Python service + (per the deferred-follow-up #2) a Cloud Run Jobs deploy lifecycle."

`services/<name>/` is a deliberate convention picked over the alternatives (per-service repos, workspace tooling like uv workspaces or pnpm). Naming it now sets the precedent that future services (a Fanbasis extractor, a webhook receiver, a re-folded `1-raw-landing/`) will inherit. If a second service stresses this convention — e.g., shared library code emerges between `services/bq-ingest/` and a future `services/fanbasis-extractor/` — that's the moment to revisit (introduce `services/_shared/`, or move to a workspace setup, or split repos). For now, `services/bq-ingest/` stands alone.

## Conclusion

The Step 2 code move is mechanical: 39 Python files plus support assets move as a unit, the single `sys.path.insert` survives, the dynamic import dispatch survives, and no env vars or secret refs need re-specification at deploy time. Step 2 is unblocked.

The audit also surfaced material non-mechanical concerns that the Step 2 framing was quietly hiding:

- **The auth-posture finding** (`allUsers → run.invoker` on bq-ingest) is independent of the migration but the highest-priority remediation the audit produced.
- **The SQL-path-resolution claim was wrong** for 7 of 9 modules. The migration preserves current behavior (broken or working) per-module; what's needed is a baseline measurement at Step 4 and a cleanup pass after.
- **`1-raw-landing/` is a parallel ingestion stack in a different GCP project** the audit's earlier framing didn't acknowledge. Scope for this plan is bq-ingest only; the separate consolidation is tracked as a deferred follow-up.
- **The Cloud Build trigger** (Step 5) stays optional-but-recommended, with deploy provenance / build reproducibility as the rationale (not stale-clone defense). Step 6 doesn't depend on it.
- **Pre-flight is now broader** than 5 items: secret access, build-SA IAM (3 roles for the user, 4 for the build SA), Cloud Scheduler URI continuity, Python version triad reconciliation, buildpack version pin.

Step 2 PR description should call out the deferred follow-ups list (§"Deferred follow-ups") so they survive into the consolidated repo's issue tracker rather than dying with this audit's branch.
