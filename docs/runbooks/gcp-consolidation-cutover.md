# GCP Consolidation Cutover Runbook

_Step-by-step for Phase 1 (U1–U5) of `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`. Owns: David. Written 2026-04-23, updated as steps complete._

**Target project:** `project-41542e21-470f-4589-96d` (GTM's GCP, billed to `0114FD-8EC797-A11084`).
**Retiring projects:** `dee-data-ops-prod`, `dee-data-ops` (decommissioned in U14 after 30-day soak).

---

## Preflight (U1) — checklist

- [x] **Cloud Run Jobs inventoried** — 26 jobs, all `us-central1`. See `docs/preflight/gtm-gcp-inventory.md` §3.
- [x] **Cloud Run Services inventoried** — `bq-ingest` (active ingest router), `gtm-warehouse-mcp-phase0`. See §4.
- [x] **Cloud Scheduler jobs inventoried** — 19 jobs all `us-central1`. 4 have non-OK status codes. See §5.
- [x] **Secret Manager inventoried** — 10 secrets. No service-account keyfile for Merge CI yet. See §6.
- [x] **Raw-data freshness snapshotted** — GHL legacy 3-day stale, GHL Phase-2 4-day stale and partial, Calendly Phase-2 empty, Stripe ~50 days stale, Typeform + Calendly legacy + Fathom + Fanbasis fresh. See §7.
- [x] **GHL staleness diagnosed** — `bq-ingest` service-level regression (scheduler returns 0, but no rows land). Recommend accept-as-is for U2–U4 and repair in parallel. See §13.
- [x] **Merge staging schema compatibility assessed** (static review — dbt compile deferred to U2). GHL: column renames needed (`id`→`entity_id`, `payload`→`payload_json`) + table-name `identifier:` overrides. Calendly: shim needed (Phase-2 empty). Stripe/Typeform/Fathom: shims already scoped for U3. See §11.
- [x] **IAM posture drafted for Merge CI** — new SA `merge-dbt-ci@...` suggested; role list in §12.
- [x] **Fathom transcript baseline captured** — 0% coverage of 1,157 calls. U6 starting baseline. See §9.
- [ ] **David reviewed inventory + signed off on GHL-staleness path, SA provisioning, and Calendly-shim addition.** *(Awaiting.)*

**Exit condition for U1:** all checkboxes above. Then U2 proceeds.

---

## U2 — Retarget Merge's dbt profile / env / CI

_(Not started. Will be fleshed out when U1 sign-off lands.)_

Outline:

1. Create service account `merge-dbt-ci@project-41542e21-470f-4589-96d.iam.gserviceaccount.com` with roles from preflight §12.
2. Generate keyfile; store as a new Secret Manager entry (`merge-dbt-ci-key`) in `project-41542e21-...`; also store as the GitHub repo secret `GCP_SA_KEY_PROD` (currently unset — this simultaneously unblocks the existing CI/CD `dbt-deploy.yml` gap flagged in project-state).
3. Update `dbt/profiles.yml`:
   - `dev.project` → `project-41542e21-470f-4589-96d` (default via `GCP_PROJECT_ID_DEV` env var).
   - `ci.project` → same.
   - `prod.project` → same.
   - Keep `generate_schema_name` routing unchanged.
4. Add a throwaway `dev_gtm` target variant (explicit `project:` literal) for quick single-model compile/diagnosis runs that bypass env-var flipping.
5. Update `.env.example`: `GCP_PROJECT_ID_DEV`, `GCP_PROJECT_ID_PROD`, `BQ_KEYFILE_PATH`, `BQ_KEYFILE_PATH_PROD`.
6. Update `.github/workflows/dbt-deploy.yml`: swap secret reference, add project override, preserve per-PR ephemeral schema behavior.
7. Update `CLAUDE.md`: replace `dee-data-ops-prod` / `dee-data-ops` references with `project-41542e21-...`; add a short "decommissioned" note pointing at the cutover runbook.
8. Update `.claude/state/project-state.md` once `dbt debug` green on all three targets.

**Verify-by-dry-run:** from a fresh checkout, `dbt debug --target dev` green from laptop, `dbt debug --target prod` green from CI. No `dbt build` yet — that's U4.

---

## U3 — Staging shims (Stripe / Typeform / Fathom, plus Calendly and column-rename work for GHL)

_(Not started.)_

Scope addition from U1 preflight: **also shim Calendly** (Phase-2 empty) and **rewrite GHL staging sources** (column renames). The plan's original U3 scope covered Stripe/Typeform/Fathom only — the Calendly + GHL work is added here, not deferred, to keep U4 parity tractable.

---

## U4 — Replay + cutover parity (HARD GATE)

_(Not started. Parity SQL gets written first — characterize, then replay.)_

Parity thresholds from plan §U4:
- `fct_speed_to_lead_touch`: 15,283 ± small incremental tolerance.
- `speed_to_lead_detail`: same.
- Headline metric from `stl_headline_7d`: **exact** match (± 0).
- `fct_revenue` sum: exact match.
- `dim_contacts` row count: within 0.1%.

David sign-off required before U5 starts.

---

## U5 — Repoint Metabase

_(Not started. Favor the "add new BQ database entry + clone dashboard" path over in-place edit, for rollback safety.)_

---

## Rollback paths

| Step | Rollback |
|---|---|
| U2 profile retarget | revert PR; CI falls back to `dee-data-ops-prod` |
| U3 shims | SQL-only — revert staging models |
| U4 replay | leave `dee-data-ops-prod.*` untouched; parity tables in consolidated project can be dropped |
| U5 Metabase repoint | revert BQ-database-id reference on the dashboard; `dee-data-ops-prod` BQ entry stays registered in Metabase until U14 |
| U6+ | per-unit; documented inline once those units land |

---

## Links

- Active plan: `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`
- U1 data snapshot: `docs/preflight/gtm-gcp-inventory.md`
- Project state: `.claude/state/project-state.md`
- Memory: `project_gcp_consolidation_decision.md`
