# GTM GCP Preflight Inventory — `project-41542e21-470f-4589-96d`

_Snapshot captured 2026-04-23 (evening) under U1 of `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`._

This is a one-time read-only audit. Nothing here mutates state. Queries and commands are reproducible from a fresh shell with `gcloud auth login` against `dforero122@gmail.com` and the current project unset or pointed anywhere else — every command passes `--project project-41542e21-470f-4589-96d` explicitly.

---

## 1. Project basics

- **Project ID:** `project-41542e21-470f-4589-96d`
- **Project number:** `535993952532`
- **Display name:** "My First Project" (not renamed)
- **Billing account:** `0114FD-8EC797-A11084` (shared with `dee-data-ops` / `dee-data-ops-prod`)
- **Region:** all workloads in `us-central1`. No jobs found in `us-east1` / `us-west1` / `us-east4`.

## 2. BigQuery datasets

`bq ls --project_id=project-41542e21-470f-4589-96d`:

| Dataset | Purpose (inferred) | Notes |
|---|---|---|
| `Raw` | Legacy single-blob `<source>_objects_raw` + support tables | 33 tables — live extractors write here for stripe/typeform/fathom/fanbasis; partially retired for GHL/Calendly |
| `raw_ghl` | Phase-2 per-object `ghl__<obj>_raw` | 14 tables; **PARTIAL — only 6/14 have rows** |
| `raw_calendly` | Phase-2 per-object `calendly__<obj>_raw` | 6 tables; **ALL EMPTY — Phase-2 scaffolded but no data landing** |
| `STG` | Legacy dbt staging views | 31 views — GTM's own dbt project output |
| `Core` | Legacy dbt warehouse (dims/facts/bridges) | 56 tables — GTM's own dbt project output; includes `bqml_fathom_sales_call_classifier` |
| `Marts` | Legacy dbt marts + Looker rpt_* tables | 50 tables — GTM's own dbt project output |
| `Ops` | Sync state, gate results, identity overrides | 4 tables |
| `ops` | Ingest checkpoints + runs | 2 tables (lowercase — distinct from `Ops`) |
| `dev_dshah` | GTM-side dev staging views | 13 views |

## 3. Cloud Run Jobs (26 total, all in `us-central1`)

Raw `gcloud run jobs list`:

```
Incremental (live):           ghl-incremental-v2, calendly-incremental-v2,
                              calendly-invitee-drain
Backfill (daily scheduled):   ghl-backfill-v2, ghl-backfill,
                              ghl-call-log-backfill(-v2),
                              ghl-comprehensive-backfill(-v2),
                              ghl-form-submissions-backfill(-v2),
                              ghl-identity-sync,
                              calendly-backfill, calendly-backfill-v2,
                              calendly-invitee-backfill-v2,
                              fathom-backfill, fathom-llm-analysis,
                              stripe-backfill, typeform-backfill,
                              fanbasis-backfill
DQ / orchestration:           dq-tests, pipeline-marts-hourly,
                              pipeline-run, pipeline-run-v2,
                              python-runner, python-runner-v2
```

**Observation:** `-v2` variants exist for GHL + Calendly + pipeline orchestration — partial Phase-2 migration in flight. Non-v2 jobs are likely retired but still deployed.

## 4. Cloud Run Services (2 total)

| Service | URL | Status |
|---|---|---|
| `bq-ingest` | https://bq-ingest-mjxxki4snq-uc.a.run.app | Ready=True |
| `gtm-warehouse-mcp-phase0` | https://gtm-warehouse-mcp-phase0-mjxxki4snq-uc.a.run.app | Ready=True |

**`bq-ingest`** is the central ingest router — Cloud Scheduler POSTs to `/ingest-ghl`, `/ingest-fathom`, etc. This is distinct from the Cloud Run **Jobs** with the same names. The schedulers in §5 point at this service, not the jobs.

## 5. Cloud Scheduler (19 jobs, all `us-central1`)

Only `us-central1` has scheduler jobs. Status and last attempt:

| Scheduler | Schedule | Last attempt | HTTP code | Target |
|---|---|---|---|---|
| ghl-hourly-ingest | 20 * * * * | 2026-04-23 18:21 | 0 (OK) | `bq-ingest/ingest-ghl` |
| calendly-hourly-ingest | 33 * * * * | 2026-04-23 17:33 | 0 | (service) |
| fathom-hourly-ingest | 17 * * * * | 2026-04-23 18:19 | **13** | `bq-ingest/ingest-fathom` |
| typeform-hourly-ingest | 43 * * * * | 2026-04-23 17:43 | 0 | (service) |
| stripe-hourly-backfill | 25 * * * * | 2026-04-23 18:25 | 0 | (service) |
| fanbasis-hourly-ingest | 0 * * * * | 2026-04-23 18:00 | 0 | (service) |
| fanbasis-hourly-models-refresh | 10 * * * * | 2026-04-23 18:10 | 0 | (service) |
| marts-hourly-refresh | 50 * * * * | 2026-04-23 17:50 | 0 | (service) |
| warehouse-healthcheck-hourly | 58 * * * * | 2026-04-23 17:58 | **13** | (service) |
| ghl-identity-sync-2h | 0 */2 * * * | 2026-04-23 18:00 | 0 | (service) |
| pipeline-daily-refresh | 0 2 * * * | 2026-04-23 02:00 | **2** | (service) |
| calendly-backfill-daily | 0 3 * * * | 2026-04-23 03:00 | 0 | (service) |
| calendly-invitee-drain-daily | 45 3 * * * | 2026-04-23 03:45 | 0 | (service) |
| fathom-backfill-daily | 0 4 * * * | 2026-04-23 04:00 | 0 | (service) |
| fanbasis-backfill-daily | 0 5 * * * | 2026-04-23 05:00 | 0 | (service) |
| typeform-backfill-daily | 15 5 * * * | 2026-04-23 05:15 | 0 | (service) |
| dq-tests-daily | 30 5 * * * | 2026-04-23 05:30 | 0 | (service) |
| ghl-comprehensive-backfill-daily | 7 3 * * * | 2026-04-23 03:07 | 0 | (service) |
| ghl-pipeline-stage-snapshot-daily | 0 7 * * * | 2026-04-23 11:00 | **5** | (service) |

Non-zero codes are gRPC/HTTP status codes from the scheduler's last attempt:
- `13` = INTERNAL — `bq-ingest` threw 5xx
- `5` = NOT_FOUND — target route missing
- `2` = UNKNOWN — catch-all

## 6. Secret Manager (10 secrets)

| Secret | Created |
|---|---|
| CalendlyApiKey | 2026-04-02 |
| FathomApiKey | 2026-04-02 |
| FathomApiKeyLegacySales | 2026-04-03 |
| GhlAccessToken | 2026-04-02 |
| Secret | 2026-04-02 (uninformative name) |
| StripeApiKey | 2026-04-02 |
| fathom-gemini-api-key | 2026-04-02 |
| gtm-warehouse-mcp-token | 2026-04-03 |
| gtm-wiki-github-token | 2026-04-08 |
| typeform-api-key | 2026-04-07 |

**Gaps for Merge CI on this project:**
- No service-account keyfile secret exists — U2 will need one (name TBD; `dbt-ci-sa-key` suggested).
- Fanbasis API key not in Secret Manager (maybe in env of a Cloud Run Job — not blocking this plan; Fanbasis is deferred).

## 7. Raw-data freshness (the live state that matters)

### Legacy single-blob (`Raw.*`)

| Table | Rows | max_ingested_at / max_event_ts |
|---|---|---|
| `Raw.ghl_objects_raw` (all entity_types) | 68,314 | 2026-04-20 17:20 (3 days stale) |
| `Raw.calendly_objects_raw` (scheduled_events, event_invitees, event_types) | 10,882 | 2026-04-23 17:33 (fresh) |
| `Raw.stripe_objects_raw` (9 object_types) | 7,619 | 2026-03-04 00:38 (~50 days stale — stripe-backfill failing) |
| `Raw.typeform_objects_raw` (responses, forms) | 5,085 | 2026-04-23 15:59 (responses fresh) |
| `Raw.fathom_calls_raw` | 1,157 | 2026-04-23 18:17 (fresh) |
| `Raw.fathom_call_intelligence` | **0** | n/a — table empty |
| `Raw.fanbasis_transactions_txn_raw` | 455 | 2026-04-23 07:27 (fresh) |
| `Raw.calendly_webhook_events_raw` | 1 | 2026-04-02 (one-off artifact) |

### Phase-2 per-object (`raw_ghl.*`, `raw_calendly.*`)

| Table | Rows | max _ingested_at |
|---|---|---|
| `raw_ghl.ghl__contacts_raw` | 15,888 | 2026-04-19 14:33 (4 days stale) |
| `raw_ghl.ghl__conversations_raw` | 101 | 2026-04-19 14:34 (4 days stale, **also very low row count**) |
| `raw_ghl.ghl__opportunities_raw` | 25,959 | 2026-04-19 14:33 (4 days stale) |
| `raw_ghl.ghl__outbound_call_logs_raw` | 100 | 2026-04-19 14:26 (stale, low count) |
| `raw_ghl.ghl__messages_raw` | 0 | — |
| `raw_ghl.ghl__notes_raw` | 0 | — |
| `raw_ghl.ghl__tasks_raw` | 0 | — |
| `raw_ghl.ghl__users_raw` | 0 | — |
| `raw_calendly.calendly__scheduled_events_raw` | 0 | — |
| `raw_calendly.calendly__event_invitees_raw` | 0 | — |
| (other `raw_calendly.*` 4 tables) | 0 | — |

**Interpretation:**
- Phase-2 for GHL half-happened: 6 entity_types migrated; 4 entity_types never landed; all frozen since 2026-04-19 14:33.
- Phase-2 for Calendly is **scaffolded but empty**. The plan's assumption that "Merge's staging should need no changes — GTM already has them in per-object shape" (§"Approach") is **wrong for Calendly** — the table names exist but the data is still in `Raw.calendly_objects_raw`.
- Legacy `Raw.ghl_objects_raw` is also stale (2026-04-20 17:20) — the v2 path and the v1 path both stopped around the same window. Root cause for both is in `bq-ingest` service (not the Jobs).

## 8. Cloud Run Jobs — recent execution health

Last 3 executions by job (abbreviated):

- **`ghl-incremental-v2`** — last 4 succeeded, all clustered on 2026-04-19 (13:25 → 14:31). **No executions since 2026-04-19 14:31** despite scheduler firing hourly. Suggests either the scheduler switched targets away from this Job (to the service), or the Job was disabled at the source-side.
- **`stripe-backfill`** — currently running (Unknown); last 2 (16:25 and 17:25 today) **FAILED** (container error).
- **`typeform-backfill`** — last 3 runs (2026-04-21, 22, 23 at 05:15 daily) all **FAILED** (exit code 2).
- **`fathom-backfill`** — last 3 runs (2026-04-21, 22, 23 at 04:00 daily) all **FAILED** (exit code 1).
- **`fathom-llm-analysis`** — last success **2026-04-03**; dead since. `Raw.fathom_call_intelligence` is empty.

## 9. Fathom transcript baseline (for U6)

```
SELECT entity_type, COUNT(*) AS row_count, MAX(partition_date) AS max_partition,
  MAX(ingested_at) AS max_ingested,
  SUM(CASE WHEN JSON_VALUE(payload_json, '$.transcript') IS NOT NULL
           AND LENGTH(JSON_VALUE(payload_json, '$.transcript')) > 10 THEN 1 ELSE 0 END) AS with_transcript
FROM `project-41542e21-470f-4589-96d.Raw.fathom_calls_raw`
GROUP BY entity_type
```

Result:

| entity_type | rows | max_partition | max_ingested | with_transcript |
|---|---|---|---|---|
| calls | 1,157 | 2026-04-23 | 2026-04-23 18:17 | **0** |

**Confirmed:** 0% transcript coverage. Fathom-hourly-ingest lands metadata (title, times, participant counts, `is_revenue_relevant`, `classification_*`) into top-level columns, but the `$.transcript` field in `payload_json` is absent. This is the known stuck point. Root cause diagnosis is scoped to U6 — candidates are (a) extractor only calls the `/meetings` endpoint not `/transcripts`, (b) Fathom API requires an async transcript job, (c) auth scope missing.

## 10. Stripe object-type inventory (for U7 sizing)

```
SELECT object_type, COUNT(*), MAX(event_ts) FROM `...Raw.stripe_objects_raw` GROUP BY 1
```

| object_type | rows | max_event_ts |
|---|---|---|
| charges | 3,375 | 2026-03-02 15:51 |
| balance_transactions | 1,934 | 2026-03-04 00:38 |
| invoices | 966 | 2026-02-28 17:12 |
| customers | 516 | 2026-02-07 01:16 |
| subscriptions | 420 | 2026-02-07 01:17 |
| products | 185 | 2026-02-18 15:36 |
| prices | 181 | 2026-02-18 15:36 |
| refunds | 21 | 2025-07-12 18:54 |
| disputes | 21 | 2026-02-14 03:22 |

Stripe is **uniformly stale across every object_type** — roughly 50 days behind. The failing `stripe-backfill` job is the direct cause. This is separate from the GHL/Phase-2 freeze.

## 11. Schema compatibility — Merge staging vs. GTM raw

Test-compiling a Merge staging model against GTM's project requires (a) dbt CLI installed in a `.venv` on this repo (not present) and (b) a `dev_gtm` target in `dbt/profiles.yml` (not present — adding one is U2 scope). Static review suffices for U1's "record compatible / incompatible per model" criterion.

### GHL — **INCOMPATIBLE, fixable at staging layer**

Merge's `_ghl__sources.yml` declares `database: dee-data-ops`, `schema: raw_ghl`, and table names `contacts`, `opportunities`, `conversations`, `users`, `messages`, `notes`, `outbound_call_logs`, `tasks`. It expects columns `(id, _ingested_at, payload)`.

GTM's `raw_ghl` tables are named `ghl__<obj>_raw` with columns `(entity_id, location_id, partition_date, event_ts, updated_at_ts, _ingested_at, source, payload_json, backfill_run_id, is_backfill)`.

Differences:
- **Dataset:** `dee-data-ops.raw_ghl` → `project-41542e21-470f-4589-96d.raw_ghl` (U2 cutover).
- **Table names:** `contacts` → `ghl__contacts_raw`, etc. Fix via source-YAML `identifier:` per table.
- **Column names:** `id` → `entity_id`, `payload` → `payload_json`. Two fix options:
  (a) add view layer inside `raw_ghl` renaming columns (more code, zero impact on staging SQL), or
  (b) update each `stg_ghl__*.sql` `source` CTE to `SELECT entity_id AS id, SAFE.TO_JSON_STRING(payload_json) AS payload, _ingested_at FROM {{ source(...) }}`.
  Option (b) is less code, more local change; pick in U3.
- **Row coverage:** `messages`, `notes`, `tasks`, `users` return zero rows — GHL Phase-2 migration is partial. Either (i) shim from `Raw.ghl_objects_raw` filtered by `entity_type`, or (ii) accept the gap and document. Decide in U3.

### Calendly — **INCOMPATIBLE, needs shim (Phase-2 not landed)**

Merge's `_calendly__sources.yml` expects `raw_calendly.{event, event_invitee, scheduled_events, invitees, invitee_no_shows, question_and_answer}` — a mix of Fivetran + Cloud Run poller naming. GTM's `raw_calendly` has 6 tables all matching the expected `calendly__<obj>_raw` suffix pattern but **zero rows**. Data is in `Raw.calendly_objects_raw` (entity_types: `scheduled_events`, `event_invitees`, `event_types`).

**Must shim during Phase-1.** The plan's "Merge's staging works against GTM's per-object raw for GHL/Calendly — verify during U1" assumption does not hold for Calendly; only GHL works (modulo column renames above).

### Stripe — **Known shim-required per plan (U3)**

GTM has only `Raw.stripe_objects_raw` — no `raw_stripe` dataset. Merge expects `raw_stripe.{charge, customer, ...}`. U3 plan already calls for a JSON-decoding shim keyed on `object_type`.

### Typeform — **Known shim-required per plan (U3)**

GTM has only `Raw.typeform_objects_raw` (entity_types `responses`, `forms`) — no `raw_typeform`. Merge expects `raw_typeform.{response, response_answer, form}`. U3 plan already calls for the shim.

### Fathom — **Known shim-required per plan (U6)**

GTM has `Raw.fathom_calls_raw` (calls only; empty `fathom_call_intelligence`). Merge has no fathom staging yet; U6 creates it.

### Fanbasis — **Out of scope per plan §Scope Boundaries**

GTM has `Raw.fanbasis_transactions_txn_raw` fresh; Merge has fanbasis staging but it's noted as broken on both sides and deferred.

## 12. IAM posture for Merge CI (to be provisioned in U2)

Merge CI does not yet have a service account on this project. What it needs (minimum privilege):

- `roles/bigquery.jobUser` at the project level (to run queries).
- `roles/bigquery.dataEditor` scoped per-dataset on `staging`, `warehouse`, `marts`, plus any dev/ci datasets created at cutover. **Do not** grant `dataEditor` on `Raw` — dbt should read raw, not write it.
- `roles/bigquery.dataViewer` on `Raw`, `raw_ghl`, `raw_calendly` so staging models can read.
- `roles/secretmanager.secretAccessor` on just the secrets CI actually needs (TBD — minimally the keyfile secret).
- No `run.admin` / `run.invoker` needed for dbt CI itself — that's extractor territory.

**Recommend creating a new SA** (`merge-dbt-ci@project-41542e21-470f-4589-96d.iam.gserviceaccount.com` or similar) rather than reusing the existing GTM SA which has broader permissions than dbt needs.

## 13. GHL staleness diagnosis (answering the preflight question)

The plan flagged GHL 3-day stale. Confirmed and refined:

- **Legacy path (`Raw.ghl_objects_raw`)** last wrote 2026-04-20 17:20 (~3 days stale). Responsible writer is the `bq-ingest` Cloud Run **service**, invoked by the scheduler.
- **Phase-2 path (`raw_ghl.ghl__*_raw`)** last wrote 2026-04-19 14:33 (~4 days stale). Responsible writer appears to be the `ghl-incremental-v2` Cloud Run **Job**, which has had no executions since 2026-04-19 14:31 despite scheduler firing hourly.
- Scheduler `ghl-hourly-ingest` fires every hour at :20 with status code 0 — it's hitting `bq-ingest/ingest-ghl` and getting 200s, but no rows are landing. The service is accepting the call but dropping writes.
- Scheduler codes `13` (INTERNAL) also present on `fathom-hourly-ingest` and `warehouse-healthcheck-hourly` — same `bq-ingest` service, suggests the service is partially broken.

**Diagnosis class:** `bq-ingest` service-level regression, not a quota or auth issue. Not fixable from Merge's side; belongs to GTM's repo. Options for U1 sign-off:
- **Fix-in-place during cutover:** ask David to redeploy `bq-ingest` from the GTM repo; gate on fresh rows landing.
- **Accept-as-is:** proceed with cutover against stale data; note that Speed-to-Lead parity at U4 will be against yesterday's data vs today's data. Tolerance bands in U4 parity SQL must account for this.
- **Spin off:** separate "GTM ingest repair" session before U2 begins.

Recommended path is **accept-as-is for U2–U4** (retarget + shims + parity) — the locked metric only cares that Merge's dbt reproduces `dee-data-ops-prod`'s output given identical raw. The raw-freshness issue is orthogonal to cutover parity. Fix the ingest service in parallel, not in series.

---

## Sign-off checklist (for David, before U2 starts)

- [ ] Read this inventory.
- [ ] Decide GHL staleness handling: (A) fix-in-place first, (B) accept-as-is (recommended), (C) spin off.
- [ ] Approve provisioning of `merge-dbt-ci@` service account + IAM bindings listed in §12, or nominate a different SA identity.
- [ ] Confirm that Calendly's Phase-2 scaffolding being empty is acceptable (U3 gets a Calendly shim on top of what the plan already covers).
- [ ] Note Stripe's 50-day staleness is a pre-existing GTM bug, not introduced by this plan — tracked for U7/U8 but not cutover-blocking.

Once signed off, U2 (dbt profile retarget) can proceed.
