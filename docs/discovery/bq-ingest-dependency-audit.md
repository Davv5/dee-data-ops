# bq-ingest dependency audit

**Status:** Discovery output. Created 2026-04-28 as Step 1 of the consolidation plan in [`docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`](../plans/2026-04-28-bq-ingest-consolidation-plan.md). Revised twice on 2026-04-28 evening following multi-persona doc review rounds 1 and 2 (PR #100). Round 2 caught a P0 regression introduced by Round 1's auth fix (Calendly webhook surface) — verified resolved.

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

**Post-move state**, the repo holds two independent deploy paths for the same sources, in two GCP projects:

| Path | Source code | Deploy target | Trigger |
|---|---|---|---|
| `services/bq-ingest/sources/ghl/` | (after Step 2) | Cloud Run **service** `bq-ingest` in `project-41542e21-470f-4589-96d` | Flask routes (HTTP), Cloud Scheduler hits |
| `1-raw-landing/ghl/` | (today) | Cloud Run **Job** in `dee-data-ops-prod` | GH Actions on push |

**Independent at the deploy layer, entangled at the dbt-source layer.** The two paths land into different `Raw.*` tables/schemas, but `2-dbt/` reads from both. The `_ghl__sources.yml` file documents the GHL `Raw` tables as "populated by GTM's `bq-ingest` Cloud Run service + `ghl-incremental-v2` Cloud Run Job" — i.e., the dbt staging layer joins data from both writers. **Consequence for Step 4 parity:** a green dbt run after the bq-ingest move is NOT proof that bq-ingest itself is healthy; if bq-ingest stops writing while `1-raw-landing/`'s job continues, downstream models keep working against possibly-stale rows from the parallel writer, masking the breakage. Step 4's parity check must observe `bq-ingest` write activity directly (per-route response codes, BigQuery write timestamps on the bq-ingest-owned tables), not infer health from dbt.

**Scope decision** (made deliberately rather than by archaeology): this audit treats bq-ingest as the consolidating canonical surface for incremental + webhook ingestion, and treats `1-raw-landing/` as a separate concern owned by the dee-data-ops-prod project's batch-job lifecycle. Whether to fold `1-raw-landing/` into `services/` (making it `services/raw-landing-ghl/` etc.) or keep it separate is **out of scope for this consolidation plan** but should be tracked as a follow-up before the next ingestion source lands.

The risk this section guards against is the consolidation looking complete after Step 6 while the same class of bug (deploy from a stale source path) still applies to `1-raw-landing/`, and a future operator reading dbt as the source of truth missing the cross-project entanglement.

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

The Step 1 plan asserted that `Path(__file__).parent / "sql" / "X.sql"` patterns continue to resolve as long as the tree moves as a unit. **The reality is more textured — there are four distinct resolution patterns across the 9 SQL-emitting modules**, only two of which work today and one of which depends on process CWD. Verified against current source:

- **Two modules resolve repo-root `sql/` correctly today** via `parents[2]`:
  - `sources/ghl/ghl_pipeline.py:1041-1045` — safe pattern: try `parent / "sql" / "ghl_models.sql"`, fall back to `parents[2] / "sql" / "ghl_models.sql"`. The fallback branch is the only one that resolves.
  - `sources/shared/phase1_release_gate.py:17-20` — `os.getenv("PHASE1_RELEASE_GATE_SQL_FILE", str(Path(__file__).resolve().parents[2] / "sql" / "phase1_release_gate.sql"))`. Default is parents[2]; an env-var override is also accepted.

- **Six modules use the broken default** `Path(__file__).resolve().parent / "sql" / "X.sql"`, which evaluates to `sources/<source>/sql/X.sql` — a directory that does not exist on disk:
  - `sources/calendly/calendly_pipeline.py:1969`
  - `sources/fathom/fathom_pipeline.py:652`
  - `sources/fanbasis/fanbasis_pipeline.py:332`
  - `sources/marts/mart_models.py:39, 51` (the second is `sql/dims/`)
  - `sources/stripe/stripe_pipeline.py:342`

- **One module has an env-var override over the broken default**: `sources/shared/data_quality.py:26` — `os.getenv("DQ_SQL_FILE", str(Path(__file__).resolve().parent / "sql" / "data_quality_tests.sql"))`. If `DQ_SQL_FILE` is set in production, this works; otherwise it hits the broken default.

- **One module uses a CWD-relative bare literal**: `sources/typeform/typeform_pipeline.py:383` — `sql_file = "sql/typeform_models.sql"`. Resolves correctly only when the process CWD is the package root. Cloud Run Buildpacks happens to set CWD to `/workspace`, where the SQL file lands at the same level — so it works in production. Any local invocation from a different directory raises `FileNotFoundError` deterministically (different failure mode than the broken-default's silent zero-statements).

**Two known env-var overrides** appear in production: `DQ_SQL_FILE` and `PHASE1_RELEASE_GATE_SQL_FILE`. The audit's env-var capture lists neither in §"Plain env vars (52 total)" notable groups — the captured set may not reflect the full set of overrides actually in service spec. Step 4 baseline must capture the complete env-var spec, not just the subset the audit highlighted.

**Implication.** Either (a) the corresponding `/refresh-X-models` routes for the 6 broken-default modules are silently inert in production today (every call returns ok with `statements_executed=0`), (b) deeper env-var overrides exist that the audit didn't enumerate, or (c) some routes raise FileNotFoundError and the failure is being absorbed by Flask's default error handler. The audit's earlier framing — "no code change required" — overstates the safety of the move; the real claim is "the move doesn't make a pre-existing latent bug worse."

**Step 4 must baseline behavior, not assume it** — and the baseline IS the broken state for 6 of 9 modules. Before declaring parity post-move:

1. Capture the complete service env-var spec via `gcloud run services describe bq-ingest --format='value(spec.template.spec.containers[0].env)'`.
2. Exercise each scheduled route against the *current* production revision (`bq-ingest-00076-wtl`) and record per-route: status code, `statements_executed`, `rows_written`, time-to-respond. Authenticated curl: `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" https://bq-ingest-mjxxki4snq-uc.a.run.app/<route>`.
3. The new revision must match per-route. A route that returns `statements_executed=0` today should still return `statements_executed=0` post-move — not a regression, not a "fix." Repairing the broken defaults is deferred follow-up #5; codifying the broken state as parity-success creates a false-confidence artifact, so Step 4's report must explicitly distinguish "matches baseline (broken)" from "matches baseline (working)" per-route.

This is the same class of failure as the operational-health rule's worked example #2 (the `/snapshot-pipeline-stages` 404 that the route-listing didn't catch).

### File inventory

**Referenced by Python** (executed by `*_pipeline.run_models()`):

| SQL file | Python referrer(s) | Resolution OK? |
|---|---|---|
| `sql/ghl_models.sql` | `sources/ghl/ghl_pipeline.py` | ✅ parents[2] fallback |
| `sql/phase1_release_gate.sql` | `sources/shared/phase1_release_gate.py` | ✅ parents[2] (env-overridable via `PHASE1_RELEASE_GATE_SQL_FILE`) |
| `sql/data_quality_tests.sql` | `sources/shared/data_quality.py` | ⚠️ broken default; works only if `DQ_SQL_FILE` env-set |
| `sql/calendly_models.sql` | `sources/calendly/calendly_pipeline.py` | ❌ broken default |
| `sql/fathom_models.sql` | `sources/fathom/fathom_pipeline.py` | ❌ broken default |
| `sql/marts.sql` | `sources/marts/mart_models.py` | ❌ broken default |
| `sql/models.sql` | `sources/{fanbasis, typeform, marts}/*_pipeline.py` | ❌ broken default (fanbasis, marts) |
| `sql/stripe_models.sql` | `sources/stripe/stripe_pipeline.py` | ❌ broken default |
| `sql/typeform_models.sql` | `sources/typeform/typeform_pipeline.py` | ⚠️ CWD-relative literal `"sql/typeform_models.sql"` — works in Buildpack runtime, FileNotFoundError otherwise |

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

### Auth posture — found and fixed during the audit

Initial state (verified via `gcloud run services get-iam-policy bq-ingest` on 2026-04-28):

```yaml
bindings:
- members:
  - allUsers
  - serviceAccount:sa-scheduler@project-41542e21-470f-4589-96d.iam.gserviceaccount.com
  role: roles/run.invoker
```

**The service was publicly invokable.** Anyone on the internet could curl `https://bq-ingest-mjxxki4snq-uc.a.run.app/refresh-models`, `/snapshot-pipeline-stages`, `/run-data-quality`, etc. and trigger BigQuery write operations. The `allUsers` binding was almost certainly a leftover from an earlier test/debug iteration that was never tightened — `sa-scheduler@` is the only intended caller and already had its own binding.

**Action taken (2026-04-28):**

Verified the 8 Cloud Scheduler entries hitting `bq-ingest` all use OIDC auth via `sa-scheduler@…`, so removing `allUsers` doesn't break the scheduled paths. Then:

```
gcloud run services remove-iam-policy-binding bq-ingest \
  --region us-central1 --project project-41542e21-470f-4589-96d \
  --member='allUsers' --role='roles/run.invoker'
```

Post-state verified:

```yaml
bindings:
- members:
  - serviceAccount:sa-scheduler@project-41542e21-470f-4589-96d.iam.gserviceaccount.com
  role: roles/run.invoker
```

End-to-end check after IAM propagation, **on both Cloud Run-generated URLs** (the service has two: `bq-ingest-mjxxki4snq-uc.a.run.app` and `bq-ingest-535993952532.us-central1.run.app`; IAM is service-scoped, so removing `allUsers` closes both — verified):

| URL | unauth | with `Authorization: Bearer $(gcloud auth print-identity-token)` |
|---|---|---|
| `bq-ingest-mjxxki4snq-uc.a.run.app/routes` | 403 | 200 (full 22-route listing) |
| `bq-ingest-535993952532.us-central1.run.app/routes` | 403 | 200 |

The next scheduled scheduler fires (next 5–60 min) implicitly verify the OIDC path against the closed surface.

**Calendly webhook check (Round-2 doc-review caught this risk).** App.py exposes `/webhooks/calendly` (line 328, POST) and the env capture shows `CALENDLY_WEBHOOK_REQUIRE_SIGNATURE=false` — meaning if Calendly were posting to this route, the previous design relied on `allUsers` to admit the request, since Calendly cannot present a Google OIDC token. Verified via Calendly API:

- 30-day Cloud Run logs: 1 hit on `/webhooks/calendly` (2026-04-02, status 200) — almost certainly a manual test.
- Live `GET /webhook_subscriptions` against the Calendly org: 2 active subscriptions, **both pointing at Zapier hooks** (`hooks.zapier.com/...`); none target `bq-ingest`.

The `/webhooks/calendly` route is dead code today. Closing the surface is safe. If a future engagement needs Calendly webhooks pointing back at this service, the right design is HMAC signature validation (flip `CALENDLY_WEBHOOK_REQUIRE_SIGNATURE=true` and add the secret) plus a per-route public-access exception via a Cloud Load Balancer or split service — not re-opening `allUsers` invoker on the whole service.

**Originally framed as out of scope for the source-tree move and the highest-priority deferred finding; resolved in the same session.**

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

`--no-traffic` is **permanent in the trigger spec**, not a one-time first-deploy precaution. Every merge produces a built-but-not-promoted revision; manual `gcloud run services update-traffic bq-ingest --to-latest` after curl-verifying `/routes` and route behavior promotes it. The trigger delivers auto-build + manual-promote — not auto-deploy.

This is the deliberate steady-state posture for this engagement until branch protection on `main` lands (deferred to Phase 6 — GH Free tier limitation per `CLAUDE.local.md`). At Phase 6, revisit whether to drop `--no-traffic` from the trigger spec and rely on branch protection as the pre-merge gate.

**Sub-directory build context.** `gcloud run deploy --source services/bq-ingest` uploads only the `services/bq-ingest/` subtree to Cloud Build — NOT the dee-data-ops repo root. `.python-version`, `requirements.txt`, `pyproject.toml` MUST live at `services/bq-ingest/` root for the buildpack to find them; files at the dee-data-ops repo root are invisible to the build. The Step 2 inventory enforces this (those files are listed as MOVE → `services/bq-ingest/` root); restating here so a reader skipping straight to Step 5 doesn't miss the constraint.

**Do NOT add `--clear-secrets` or `--update-secrets`** unless re-specifying all five secret refs explicitly. Omitting both flags preserves existing refs; adding `--clear-secrets` to "be explicit" wipes them.

### Deploy posture — accepted

The trigger fires on every merge to `main` touching `services/bq-ingest/**`. The current pre-merge gate is David's own review. Per the engagement's solo-operator pre-authorization (`CLAUDE.md`), this is the deliberate posture: David's merge IS the deploy approval; `--no-traffic` adds a second checkpoint between merge and 100% traffic.

merge → build → revision created at 0% traffic → manual promote after curl-verification. Auto-build + manual-promote, not auto-deploy.

## Deferred follow-ups (surfaced by this audit, not in scope for Step 2)

These are concerns the audit surfaced that don't block the Step 2 code move but must not disappear into "we'll get to it." Each gets its own GH issue or its own line in the Step 2 PR description:

1. ~~**Auth posture**: remove `allUsers → roles/run.invoker` from bq-ingest IAM.~~ **DONE 2026-04-28** — see §"Auth posture — found and fixed during the audit."
2. **Cloud Run Jobs image rebuild**: the `fanbasis-python-runner:latest` image (built from `ops/cloud/pipeline-runner/Dockerfile`) builds from the same source tree the service uses. After the move, the build pipeline producing this image must point at the new path — otherwise the next jobs rebuild fails, or worse, succeeds off the archived `gtm-lead-warehouse` repo and silently re-introduces the stale-clone hazard. Either add a parallel Cloud Build trigger for the Jobs image (sequenced alongside Step 5's bq-ingest service trigger — same trigger PR, different `includedFiles` glob targeting the Jobs image's source path) OR document the rebuild procedure in the new RUNBOOK.md.
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
- [ ] Step 5 build SA `run-build-sa@project-41542e21-470f-4589-96d` has `roles/run.builder` (the aggregate role Cloud Run's Buildpacks pipeline uses — bundles run.developer, iam.serviceAccountUser-on-runtime-SA, storage.objectAdmin, and artifactregistry.writer internally). Verified 2026-04-28 — current binding is `roles/run.builder`, deploys succeed.
  ```
  gcloud projects get-iam-policy project-41542e21-470f-4589-96d \
    --flatten="bindings[].members" \
    --filter="bindings.members:run-build-sa" \
    --format="value(bindings.role)"
  ```
  Expected output: `roles/run.builder`. If the binding ever degrades to a narrower role, Cloud Build trigger deploys will fail at attach-runtime-SA time.

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
- [ ] `pyproject.toml`'s lint config is internally inconsistent: `[tool.ruff.lint.isort] known-first-party = ["ingest"]` references a non-existent package, but `[tool.ruff] extend-exclude` already lists `sources`, `sql`, `ops`, `enrichment`, `app.py` — meaning Ruff lints none of the service code today, so the `known-first-party` value is unused. Pick: (a) **remove** sources/ops/app.py from extend-exclude AND set `known-first-party = ["sources", "ops"]` (start linting the service surface); OR (b) **document** that lint is intentionally disabled on the service code while it migrates and leave both fields untouched. Don't half-fix it.

**Buildpack version pin** (new — defends against the next 3.11-style drop):

- [ ] `.python-version` (`3.13`) moves with the source. The buildpack reads it at build time — without it, the universal builder defaults to whichever Python it last shipped, which is the issue PR #4 was hot-fixing.

## What this audit explicitly did not check (deferred to Step 4)

- Behavioral parity between the current revision (`bq-ingest-00076-wtl`) and the post-move revision. `/routes` parity is necessary but **not sufficient** — `/routes` lists registered handlers, not behavior. Per §"How `*_pipeline.run_models()` actually finds its SQL," Step 4 must also exercise each `/refresh-*` route and `python3 -m ops.runner.cli run backfill.<source>` against the new tree, capturing baseline `statements_executed` / `rows_written` / `ok=true` semantics from the current revision and matching against the new one. **Preconditions for the local CLI invocation:** (a) CWD must be the package root (typeform's CWD-relative literal raises FileNotFoundError otherwise), (b) ADC or a service-account keyfile with BigQuery + Secret Manager read access (`gcloud auth application-default login` plus runtime SA impersonation, or download an `id-sa-ingest@…` keyfile temporarily), (c) all 5 vendor-API env vars sourced from the local shell (`gcloud secrets versions access latest --secret=<name>` per secret) — production runs read these from Secret Manager bindings, but the local CLI reads them from `os.getenv` directly.

## Layout precedent — services/ as a polyrepo bet

`services/bq-ingest/` is the first service-shaped directory in this repo. Today the repo is "the dbt project plus thin glue"; after this consolidation it becomes "a polyrepo: dbt project + Python service + (per the deferred-follow-up #2) a Cloud Run Jobs deploy lifecycle."

`services/<name>/` is a deliberate convention picked over the alternatives (per-service repos, workspace tooling like uv workspaces or pnpm). Naming it now sets the precedent that future services (a Fanbasis extractor, a webhook receiver, a re-folded `1-raw-landing/`) will inherit. If a second service stresses this convention — e.g., shared library code emerges between `services/bq-ingest/` and a future `services/fanbasis-extractor/` — that's the moment to revisit (introduce `services/_shared/`, or move to a workspace setup, or split repos). For now, `services/bq-ingest/` stands alone.

## Conclusion

The Step 2 code move is mechanical: 39 Python files plus support assets move as a unit, the single `sys.path.insert` survives, the dynamic import dispatch survives, and no env vars or secret refs need re-specification at deploy time. Step 2 is unblocked.

The audit also surfaced material non-mechanical concerns that the Step 2 framing was quietly hiding:

- **The auth-posture finding** (`allUsers → run.invoker` on bq-ingest) was independent of the migration but the highest-priority remediation the audit produced — **fixed in this session**, verified unauth=403 / auth=200.
- **The SQL-path-resolution claim was wrong** for 7 of 9 modules. The migration preserves current behavior (broken or working) per-module; what's needed is a baseline measurement at Step 4 and a cleanup pass after.
- **`1-raw-landing/` is a parallel ingestion stack in a different GCP project** the audit's earlier framing didn't acknowledge. Scope for this plan is bq-ingest only; the separate consolidation is tracked as a deferred follow-up.
- **The Cloud Build trigger** (Step 5) stays optional-but-recommended, with deploy provenance / build reproducibility as the rationale (not stale-clone defense). Step 6 doesn't depend on it.
- **Pre-flight is now broader** than 5 items: secret access, build-SA IAM (3 roles for the user, 4 for the build SA), Cloud Scheduler URI continuity, Python version triad reconciliation, buildpack version pin.

Step 2 PR description should call out the deferred follow-ups list (§"Deferred follow-ups") so they survive into the consolidated repo's issue tracker rather than dying with this audit's branch.
