# bq-ingest consolidation into dee-data-ops

**Status:** Planning. Created 2026-04-28 after the operational-health audit (`feedback_dont_dismiss_high_leverage_under_pause.md` memory; `.claude/rules/operational-health.md`).

**Why this exists:** the production `bq-ingest` Cloud Run service is currently deployed from `heidyforero1/gtm-lead-warehouse` — a repo David does not actively push to. Multiple stale local clones (`~/Documents/fanbasis-ingest`, `~/Documents/gtm`, `~/Documents/gtm-lead-warehouse`) all point to it. Today's audit surfaced three silent failures (`/snapshot-pipeline-stages` 404, terminal-state filter, 512Mi OOM) that none of the in-place defenses would have caught because nobody was looking at that repo. Consolidating into `dee-data-ops` (the active push target) kills the repo-fork hazard at the root.

## End state

- bq-ingest source lives at `services/bq-ingest/` in `dee-data-ops`.
- Cloud Run deploys originate from there: `gcloud run deploy bq-ingest --source services/bq-ingest --region us-central1 --project project-41542e21-470f-4589-96d`.
- `heidyforero1/gtm-lead-warehouse` archived (not deleted — kept for git history).
- Stale local clones (`fanbasis-ingest`, `gtm`, `gtm-lead-warehouse`) deleted from `~/Documents/`.
- All operational-health references and runbooks point to the new home.

## What moves

From `gtm-lead-warehouse/`:

| Source | Destination | Notes |
|---|---|---|
| `app.py` | `services/bq-ingest/app.py` | Flask entrypoint, ~330 lines |
| `sources/` | `services/bq-ingest/sources/` | Per-source pipeline modules: `ghl/`, `calendly/`, `fathom/`, `fanbasis/`, `typeform/`, `stripe/`, `identity/`, `marts/`, `shared/` |
| `ops/` | `services/bq-ingest/ops/` | Runner CLI + scripts |
| `enrichment/` | `services/bq-ingest/enrichment/` | If still referenced — audit before move |
| `sql/` | `services/bq-ingest/sql/` | SQL files referenced by `*_pipeline.run_models()` |
| `requirements.txt` | `services/bq-ingest/requirements.txt` | |
| `pyproject.toml` | `services/bq-ingest/pyproject.toml` | Ruff config |
| `.python-version` | `services/bq-ingest/.python-version` | Pinned 3.13 (3.11 dropped from current GCP buildpack) |
| `.dockerignore` if present | `services/bq-ingest/.dockerignore` | |

Total: ~1.2 MB, 41 Python files.

## What does NOT move

- `gtm-lead-warehouse/dbt/` — redundant with `2-dbt/`. dee-data-ops's dbt project is canonical.
- `gtm-lead-warehouse/CLAUDE.md` — the dee-data-ops `CLAUDE.md` already covers the engagement; the service-specific bits roll into a new `services/bq-ingest/CLAUDE.md` (or path-scoped rule).
- `gtm-lead-warehouse/.github/workflows/*.yml` if any — dee-data-ops's CI is canonical; if there's a workflow specific to bq-ingest deployment it gets its own file under `dee-data-ops/.github/workflows/bq-ingest-deploy.yml`.
- `gtm-lead-warehouse/.claude/`, `.sqlfluff`, `.pre-commit-config.yaml` — duplicates of dee-data-ops tooling.

## Pre-migration audit (do before code move)

1. **What does `enrichment/` contain and is it still referenced?** `app.py` doesn't import from it directly; check whether any pipeline module does. If unreferenced, drop.
2. **What's in `gtm-lead-warehouse/sql/` (528 KB) and which paths reference it?** `run_models()` in each pipeline reads a SQL file path; map every reference to confirm what's load-bearing.
3. **What env vars does the service rely on?** Check Cloud Run service spec (`gcloud run services describe bq-ingest --format=yaml`) and ensure dee-data-ops `.env.example` covers them.
4. **Cloud Build trigger:** there isn't one today (deploys are local `gcloud run deploy --source`). Adding a trigger that watches `services/bq-ingest/**` on dee-data-ops would replace the stale-local-clone risk with a real CI/CD pipeline. Optional but recommended at this step.

## Migration sequence (one PR each, mergeable independently)

1. **Audit + dependency map.** Output: a list of every file in `gtm-lead-warehouse` and its in/out edges. No code change.
2. **Code move.** `git mv` (or copy + delete) the load-bearing tree into `services/bq-ingest/`. Run lint + import tests locally to confirm the package imports cleanly. No deploy.
3. **Pointer updates.** Touch every doc / runbook / rule that references `gtm-lead-warehouse`: `.claude/rules/operational-health.md`, `.claude/state/project-state.md`, `docs/runbooks/*`, `WORKLOG.md` if relevant. PR body lists each file changed.
4. **First deploy from new home.** `gcloud run deploy bq-ingest --source services/bq-ingest ...`. Use the same `--memory=1024Mi` flag. Verify `/routes` parity against the gtm-lead-warehouse-deployed revision before routing traffic. If it fails, traffic stays on the previous revision.
5. **Cloud Build trigger** (optional but recommended). GitHub trigger on `dee-data-ops` watching `services/bq-ingest/**` paths, deploying on merge to `main`. Removes the local-clone deploy step entirely.
6. **Archive `gtm-lead-warehouse` and delete stale local clones.** Only after several days of clean operation from the new home.

## Risks

- **Hidden imports.** Python's import resolution is forgiving — a module under `gtm-lead-warehouse/sources/shared/` could be referenced via `from sources.shared.X import Y` or via `sys.path` manipulation in some script. Check for `sys.path.insert` and explicit path manipulation before assuming the move is mechanical.
- **SQL file paths.** `Path(__file__).resolve().parent / "sql" / "..."` patterns in pipeline files are relative to the file location. Moving the tree as a unit preserves them; moving piecemeal breaks them.
- **Secret references.** Cloud Run env-var refs to Secret Manager (`secretKeyRef: GhlAccessToken`, etc.) survive the migration as-is — they're attached to the service revision, not to the source. But ensure the service account `id-sa-ingest@project-41542e21-470f-4589-96d` has access to all the same secrets when deploying from the new location.
- **Buildpack Python version.** Pinned to 3.13 (3.11 was dropped from GCP universal builder `universal_builder_20260414_RC00`). The `.python-version` file moves with the source — verify it lands in `services/bq-ingest/`.

## Time estimate

- Audit + dependency map: 30 min
- Code move + local lint pass: 60 min
- Pointer updates: 30 min
- First deploy + parity check: 30 min
- Cloud Build trigger (optional): 30 min

**Total: 2.5–3 hours for an end-to-end fresh-context session.** Worth its own dedicated session — do not interleave with unrelated work.

## After migration

- Update `.claude/rules/operational-health.md` worked-example #2 to point at the new path.
- Update `feedback_dont_dismiss_high_leverage_under_pause.md` memory body if the trigger conditions need refining based on what we learn.
- The two referenced "memory bump command" / "redeploy command" snippets in PR descriptions become single-line wrappers in `services/bq-ingest/RUNBOOK.md`.
