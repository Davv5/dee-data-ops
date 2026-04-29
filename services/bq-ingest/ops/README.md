# Ops Layout

- `env/` -> Cloud Run job/service env var files (`*.env`)
- `scripts/` -> operational helpers (for example secret rotation)
- `env/triage/` -> source-specific triage config files for generic pipeline diagnostics

Example:
- [rotate_api_key_secret.sh](scripts/rotate_api_key_secret.sh)

## Cloud Python Jobs

Canonical runtime manifest:

```bash
ops/cloud/jobs.yaml
```

Deploy shared runtime image + managed jobs from manifest:

```bash
ops/scripts/deploy_runtime_stack.sh
```

Deploy managed jobs only (no image build):

```bash
ops/scripts/deploy_jobs_from_manifest.sh
```

Run ad-hoc Python callable in cloud:

```bash
ops/scripts/run_cloud_python.sh fathom_pipeline:run_models --async
```

Scheduler canary cutover + rollback:

```bash
ops/scripts/cutover_schedulers_to_v2.sh
ops/scripts/rollback_last_scheduler_cutover.sh
```

## Pipeline Triage Pattern

Use [run_pipeline_triage.sh](scripts/run_pipeline_triage.sh) for fast, repeatable incident diagnosis across sources.

Passive checks (no ingest call):

```bash
ops/scripts/run_pipeline_triage.sh --source typeform
```

Active checks (includes ingest smoke call):

```bash
ops/scripts/run_pipeline_triage.sh --source typeform --smoke
```

Run all source configs:

```bash
ops/scripts/run_pipeline_triage_all.sh
```

What it checks:
- Cloud Run revision health and startup errors
- Secret availability and newline formatting guardrails
- Env-to-secret wiring for the source auth key
- Raw/Core row-count health
- Scheduler status and latest attempt logs
- Optional ingest endpoint smoke result

Automatic execution on failure:
- [validate_marts.sh](scripts/validate_marts.sh) auto-runs `run_pipeline_triage_all.sh` when validation fails.
- [run_pipeline.sh](scripts/run_pipeline.sh) auto-runs `run_pipeline_triage_all.sh` on script failure (`ERR` trap).

Optional toggles:
- `AUTO_TRIAGE_ON_FAIL=false` to disable automatic triage on failure.
- `AUTO_TRIAGE_SMOKE_ON_FAIL=true` to include ingest smoke calls during automatic triage.

Release gate:
- `ops/scripts/run_phase1_release_gate.sh` runs the Phase 1 deterministic revenue release gate and exits non-zero on any hard failure.
- `pipeline.full` now fail-closes on the same Phase 1 release gate in the scheduled Cloud Run job path.

To add a per-source triage config:
1. Copy [ops/env/triage/_template.env](env/triage/_template.env) to `ops/env/triage/<source>.env`.
2. Fill in source endpoint, tables, and optional scheduler/secret fields. The template documents every variable.
3. Run `ops/scripts/run_pipeline_triage.sh --source <name>`.

`run_pipeline_triage_all.sh` iterates over every `*.env` in the directory (excluding `_template.env`). Until at least one source-specific config exists, the all-script exits non-zero with "No triage source configs found" — that's by design; the validate-marts ERR-trap path needs at least one configured source to do useful diagnosis.
