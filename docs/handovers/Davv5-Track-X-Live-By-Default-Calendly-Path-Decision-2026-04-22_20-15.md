# Session Handover — Track X: Calendly path (Fivetran tier bump vs. custom poller)

**Branch:** `Davv5/Track-X-Live-By-Default-Calendly-Path`
**Timestamp:** `2026-04-22_20-15` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## ✅ DECISION LOCKED — 2026-04-22

**David chose Option B (custom Calendly poller).** Reason: saves the Fivetran tier-bump cost and consolidates all near-real-time sources on the Cloud Run + Scheduler pattern established by Track W.

Executor: follow the **Option B** path only. Ignore the Option A execution section — it is retained for audit only and must not be run.

---

## Original decision memo (pre-lock)

Calendly is the *second source* feeding the Speed-to-Lead mart (the denominator:
Calendly bookings with SDR touch within 5 min). Today it ingests via **Fivetran
free tier at daily cadence** — incompatible with the 1-min freshness target.

This track has two mutually exclusive execution paths:

| Option | Cadence achievable | Cost | Code change | Ops burden |
|---|---|---|---|---|
| **A — Fivetran Standard tier bump** | 15-min sync floor | ~$120/mo (est. — David confirms) | None | None |
| **B — Custom Calendly poller (Cloud Run Job, same pattern as GHL)** | 1-min | Few $/mo Cloud Run | ~300 LOC extractor + TF | One more custom extractor to operate |

**Plan-architect recommendation:** **Option B (custom poller)**, for three reasons:

1. **Pattern consistency.** Track W ships Cloud Run + Scheduler for GHL. Adding a second custom source on the same rails costs marginal design complexity — Terraform already modularized, Dockerfile template reusable, Secret Manager IAM pattern already proven. Avoiding a two-rail world (some near-real-time via Fivetran, some via Cloud Run) is worth real money.
2. **Freshness ceiling.** Fivetran Standard's 15-min floor puts a hard ceiling on the STL end-to-end SLA at ~15 min — which *contradicts the product requirement* ("within ~1 min of landing"). Option A doesn't actually ship the stated goal; it ships a compromise we'd then immediately need to re-do.
3. **Disposability.** A custom Calendly poller is ~300 lines of Python following the same extractor skeleton `.claude/rules/ingest.md` already documents. If D-DEE later adopts a different scheduling system (e.g., Acuity, Chili Piper), the poller pattern ports. A Fivetran connector doesn't.

**When Option A still makes sense:**
- If David wants the 15-min cadence to be the *actual* answer for Calendly specifically (e.g., he's decided the 5-min SLA measurement only needs Calendly at booking-event granularity, and bookings don't arrive at 1-min burstiness).
- If Calendly's API rate limits or auth complexity make a polling path materially worse than Fivetran's managed connector.
- If Fivetran-managed-Calendly is pinning some other downstream Fivetran feature (HubSpot sync, etc. — not present in D-DEE today, but worth a 30-second check).

The rest of this document assumes **Option B**. If David picks A, skip to the "Option A execution path" section near the end.

---

## Session goal (Option B — custom Calendly poller)

Replace the Fivetran-managed Calendly connector with a repo-local Python
extractor that mirrors the GHL pattern: Cloud Run Job in `dee-data-ops-prod`,
Cloud Scheduler at 1-min cadence, writes append-only to `raw_calendly.*`
with `_ingested_at TIMESTAMP`. Retire the Fivetran connector at the end,
after a dual-run overlap window proves the new path matches.

When this ships: a Calendly booking created at T+0 lands in
`raw_calendly.event` by T+60s. Downstream staging (`stg_calendly__*`) and the
`sales_activity_detail` mart inherit that freshness for free once Track Y
(dbt incremental) lands.

## Corpus deviation flag

Same flag as Track W: moving a source off Fivetran into custom Python means
owning the poll cadence, auth refresh, rate-limit handling, and schema
drift that Fivetran otherwise absorbs. The Data Ops corpus'
`ingest.md` rule treats Fivetran sources as opaque — "orchestration is
configured in the Fivetran dashboard" — and doesn't contemplate
swap-to-custom migrations. Document the migration reasoning in the
WORKLOG entry.

## Changed files (expected, Option B)

```
ingestion/calendly/extract.py                                — created — incremental extractor, skeleton per .claude/rules/ingest.md
ingestion/calendly/requirements.txt                          — created — requests, google-cloud-bigquery, google-cloud-secret-manager
ingestion/calendly/Dockerfile                                — created — slim python:3.11 image
ingestion/calendly/.dockerignore                             — created
ingestion/calendly/README.md                                 — created — auth model, endpoint list, pagination handling
ops/cloud-run/calendly-extractor/terraform/main.tf           — created — Cloud Run Job + Cloud Scheduler (1-min cadence)
ops/cloud-run/calendly-extractor/terraform/variables.tf      — created
ops/cloud-run/calendly-extractor/terraform/outputs.tf        — created
ops/cloud-run/calendly-extractor/terraform/README.md         — created
dbt/models/staging/calendly/stg_calendly__event.sql          — edited — switch source column from `_fivetran_synced` to `_ingested_at`; dedupe by event_uri keep max(_ingested_at)
dbt/models/staging/calendly/stg_calendly__invitee.sql        — edited — same pattern (if file exists; else list in "new sources" below)
dbt/models/staging/calendly/_calendly__sources.yml           — edited — freshness block threshold updated to warn 1h / error 4h (Track Y also touches this; coordinate)
dbt/tests/source_freshness.sql                               — edited — swap `_fivetran_synced` to `_ingested_at` on raw_calendly.*
.claude/rules/ingest.md                                      — edited — update v1 source inventory table: Calendly Pipeline column flips from "Fivetran managed" to "Python extractor + Cloud Run"
.github/workflows/cloud-run-deploy-calendly.yml              — created — on-merge CD for the Calendly image
docs/runbooks/calendly-cloud-run-extractor.md                — created
WORKLOG.md                                                   — edited — dated entry incl. Fivetran-connector-retire timestamp + dual-run overlap row-count reconciliation
docs/handovers/Davv5-Track-X-Execution-<timestamp>.md        — created
```

**Fivetran-side change (NOT in repo):** at the very end of the cutover, **PAUSE** (do not delete) the Calendly Fivetran connector via the Fivetran UI. Keep the connection definition intact as an emergency rollback path for 30 days. Delete after the 30-day observation window is clean.

## Tasks (Option B, ordered)

### Pre-flight

- [ ] `ask-corpus scope: methodology.data_ops` the question: *"When migrating a source from a managed connector (Fivetran) to a custom polling extractor, what's the safe cutover pattern? How do we reconcile row counts between the two during overlap?"* — capture the answer in WORKLOG.
- [ ] Read `.claude/rules/ingest.md` — note the v1 source inventory table (line ~25) and the extractor skeleton (line ~80). Match the new Calendly extractor to that skeleton.
- [ ] Read `ingestion/ghl/extract.py` — the Calendly extractor should mirror its structure (watermark table `raw_calendly._sync_state`, token-bucket throttle, secret loading, `_ingested_at` stamping).
- [ ] Read `dbt/models/staging/calendly/` (whatever staging models exist today) — note every column that references `_fivetran_synced` — all will need to switch to `_ingested_at` after cutover.
- [ ] List the current Calendly schema Fivetran is landing: `bq ls --format=prettyjson dee-data-ops-prod:raw_calendly`. Expect at minimum `event`, `invitee`. If anything else is present (webhook, routing_form_submission, etc.) that dbt consumes, the extractor needs to cover it too — enumerate and confirm with David.
- [ ] **STOP — David confirms** the table list to cover and whether he wants 1-min cadence (recommended) or something looser (e.g., 5-min, if Calendly API rate limits bite).

### Build the extractor (mirrors GHL)

- [ ] Create a Calendly API token (personal access token or OAuth2 app token) + store in GCP Secret Manager as `calendly-api-key` in `dee-data-ops-prod`. Grant `roles/secretmanager.secretAccessor` to `ingest@dee-data-ops.iam.gserviceaccount.com`.
- [ ] Write `ingestion/calendly/extract.py` following the `.claude/rules/ingest.md` skeleton:
  - `_load_secret("CALENDLY_API_KEY")` — reuse the Secret Manager pattern from `ingestion/ghl/extract.py`.
  - Watermark per endpoint in `raw_calendly._sync_state` (timestamp column, same shape as GHL's).
  - For `events`: Calendly `/scheduled_events` supports `min_start_time` + `max_start_time` filters and `updated_at` sort — poll by `updated_at_after` cursor.
  - For `invitees`: Calendly `/scheduled_events/{uuid}/invitees` fans out from the `events` pull. Mirror the GHL conversations → messages fan-out pattern.
  - Rate limits: Calendly's documented limit is modest (check their docs at poll-time; if <100 req/min at 1-min cadence we're fine). Token-bucket throttle.
  - Append-only insert to `raw_calendly.<object>` with `_ingested_at = current_timestamp()` in UTC.
  - Same BQ advisory-lock concurrency guard pattern Track W defined for GHL.
  - `--endpoints` CSV flag, `--since ISO-8601` backfill flag.
- [ ] Write `ingestion/calendly/requirements.txt` (copy from `ingestion/ghl/` with version bumps as needed).
- [ ] Write `ingestion/calendly/Dockerfile` (identical template to GHL; only the COPY paths change).
- [ ] Write `ingestion/calendly/README.md` — the auth-token rotation procedure and the endpoint list.
- [ ] Local smoke-test against dev BQ project: `docker run ... calendly-extractor:dev --since 2026-04-22T00:00:00Z`. Confirm a row lands in `dee-data-ops:raw_calendly.event` with a non-null `_ingested_at`.
- [ ] **STOP — David confirms** the dev-run output looks correct.

### Provision Cloud Run infra

- [ ] Copy Track W's `ops/cloud-run/ghl-extractor/terraform/` directory to `ops/cloud-run/calendly-extractor/terraform/` and edit:
  - Image name: `calendly-extractor` (Artifact Registry repo can be the shared `ingest` repo Track W created).
  - Job names: `calendly-poll` (single job; no hot/cold split — Calendly has far less data than GHL).
  - Scheduler: every 1 min.
  - SA: same `ingest@dee-data-ops.iam.gserviceaccount.com`.
- [ ] **STOP — David runs `terraform plan` and confirms.**
- [ ] `terraform apply`. Capture the Cloud Run Job URL + Scheduler name.
- [ ] Write `.github/workflows/cloud-run-deploy-calendly.yml` (same template as Track W's; triggered on changes to `ingestion/calendly/**`).

### Dual-run cutover

- [ ] Let both paths run in parallel for **at least 24 hours** (longer than Track W's GHL window because Calendly is lower-volume and edge cases take longer to surface). During this window both Fivetran and Cloud Run are writing to `raw_calendly.*`:
  - Fivetran writes with `_fivetran_synced` only.
  - Cloud Run writes with `_ingested_at` only.
  - BOTH will write rows; dedup in staging needs to handle this — see the staging edits below.
- [ ] Edit `dbt/models/staging/calendly/stg_calendly__event.sql` (and any peer staging models):
  - Dedupe by event URI (or equivalent PK), keeping the row with max(`coalesce(_ingested_at, _fivetran_synced)`). This works for both sources during the overlap.
  - Add a column `_source_path` = `if(_ingested_at is not null, 'cloud_run', 'fivetran')` — useful for reconciliation queries.
- [ ] Run a reconciliation query at end of the 24h window:
  ```sql
  SELECT
    _source_path,
    COUNT(*) as row_count,
    COUNT(DISTINCT event_uri) as unique_events
  FROM {{ ref('stg_calendly__event') }}
  WHERE event_created_at >= '2026-<cutover-day>'
  GROUP BY 1
  ```
  Expect cloud_run to have ≥ Fivetran's row count (Fivetran is daily-synced; Cloud Run is 1-min-synced, so CR should be ≥). If CR is missing events Fivetran caught, halt cutover and debug.
- [ ] **STOP — David confirms** reconciliation looks clean before pausing Fivetran.

### Retire Fivetran

- [ ] **In Fivetran UI:** pause (do NOT delete) the Calendly connector. Note the Fivetran connector ID in the WORKLOG for 30-day rollback.
- [ ] Edit `dbt/models/staging/calendly/stg_calendly__event.sql` — remove the coalesce logic, keep only `_ingested_at`. Same for peer staging models.
- [ ] Edit `dbt/tests/source_freshness.sql`:
  ```sql
  select 'raw_calendly.event', max(_ingested_at) from {{ source('raw_calendly', 'event') }}
  ```
  (swap out `_fivetran_synced`).
- [ ] Edit `dbt/models/staging/calendly/_calendly__sources.yml`:
  - Update freshness block to `warn_after: {count: 1, period: hour}`, `error_after: {count: 4, period: hour}` for Calendly sources. **Note:** Track Y may also touch this file for a similar retune — coordinate merge order (see Dependencies section).
- [ ] Edit `.claude/rules/ingest.md` v1 source inventory table: Calendly row's Pipeline column flips from "Fivetran managed connector" to "Python extractor + Cloud Run Jobs".
- [ ] Append WORKLOG entry: cloud run job URL, scheduler name, Fivetran connector ID paused, 24h reconciliation summary.
- [ ] Write `docs/runbooks/calendly-cloud-run-extractor.md` (same template as Track W's GHL runbook).
- [ ] Run `/handover`.
- [ ] Commit locally.

---

## Option A execution path (Fivetran tier bump, skip if Option B was chosen)

If David picks A, the track collapses to:

### Tasks (Option A)

- [ ] Upgrade Fivetran to Standard tier in the Fivetran billing UI. Record the monthly cost in WORKLOG.
- [ ] In the Calendly connector settings, set the sync frequency to 15 minutes (the Standard tier floor).
- [ ] Edit `dbt/models/staging/calendly/_calendly__sources.yml` freshness block to `warn_after: {count: 30, period: minute}`, `error_after: {count: 2, period: hour}` — Calendly is now 15-min-synced; STL freshness ceiling is the 15-min bound and we accept it.
- [ ] Update the STL scope doc (`client_v1_scope_speed_to_lead.md` if it references the SLA) to note: "Calendly tile freshness ceiling: 15 min; GHL tiles: 1 min. Mixed-freshness dashboards carry a per-tile 'data as of' stamp."
- [ ] Update `.claude/rules/live-by-default.md` (Track Z's rule) — add a caveat: "Mixed-cadence sources are acceptable when a vendor API doesn't support sub-X-min polling; per-tile freshness stamps are required to prevent user confusion."
- [ ] Append WORKLOG + `/handover` + local commit.

### Option A decisions

- If David picks A, **the stated 1-min end-to-end freshness goal cannot be fully met for Calendly-derived tiles**. It's still met for GHL-derived tiles (Track W delivers that). David accepts the asymmetry.
- No code change required. Entire track is ~30 min of work + billing.

---

## Decisions already made (both options)

- **Do NOT delete the Fivetran connector immediately.** Pause it, wait 30 days for rollback insurance, then delete. No cost delta once paused on Standard tier (confirm with Fivetran billing).
- **Staging logic tolerates both sources during overlap.** Coalesce(`_ingested_at`, `_fivetran_synced`) + dedup by PK. Rip the coalesce out only after Fivetran is paused.
- **Raw dataset name stays `raw_calendly`.** Don't rename to `raw_calendly_custom` or similar — churn for no gain, and breaks every existing staging/mart lineage.
- **One Calendly job, not hot/cold split.** Calendly data volume is ~1-2 orders of magnitude below GHL. A single 1-min-cadence job covers all endpoints.

## Open questions

- **Option A vs B decision:** David, this is the only hard blocker. Everything above assumes B; A collapses to a 30-min billing change. Pick before execution starts.
- **Calendly API rate limits at 1-min cadence (Option B only):** the Calendly docs state rate limits per token/endpoint but don't publish a precise minute-budget. **Pick sensible default:** implement token-bucket throttle at 60 req/min with exponential backoff on 429. If we hit 429s in practice, raise cadence to 2-min.
- **Does the 24h dual-run overlap produce any genuinely different row counts?** If CR catches events Fivetran missed (e.g., deleted/edited invitees Fivetran purged), the reconciliation query will reveal it. **Pick sensible default:** CR becomes source of truth; Fivetran-only rows get logged but not migrated.
- **Webhook path (deferred):** Calendly supports webhooks, which would give true-real-time. Explicitly deferred per the scope — polling only for this track. Track backlog note: revisit if 1-min polling misses enough events to move the STL metric.

## Done when (Option B)

- `raw_calendly.event._ingested_at` shows rows landing at 1-min cadence.
- Fivetran Calendly connector is **paused** (not deleted).
- `stg_calendly__event` reads from `_ingested_at`, no longer references `_fivetran_synced`.
- `dbt/tests/source_freshness.sql` uses `_ingested_at` for Calendly rows.
- `.claude/rules/ingest.md` v1 source inventory table reflects the new pipeline.
- 24h reconciliation summary logged to WORKLOG.
- Handover doc + local commit.

## Done when (Option A)

- Fivetran Standard tier active; Calendly connector configured at 15-min sync.
- Freshness thresholds updated in `_calendly__sources.yml`.
- Scope doc notes the mixed-freshness reality.
- WORKLOG + handover + local commit.

## Dependencies

- **Hard dependency on Track W:** Option B reuses Track W's Cloud Run pattern, `ingest` AR repo, deploy-on-push workflow template, SA IAM, and concurrency-guard code. Option B should execute *after* Track W lands so the pattern is proven; starting Option B in parallel is possible but forces merging two Cloud Run Terraform modules in one window.
- **Soft dependency on Track Y:** freshness-threshold edits in `_calendly__sources.yml` overlap with Track Y's retune of the same file. **Merge order:** Track W → Track X → Track Y, OR land Track Y first and let X rebase (simpler if X touches only the Calendly row and Y touches only the GHL rows, which is the recommended split).
- **Independent of Track Z:** Z's Metabase defaults don't depend on which source path was picked — it applies to both.

## Manual-verification checkpoints (recap, Option B)

1. After Pre-flight — David confirms table list + cadence.
2. After dev-run container test — David confirms output.
3. After `terraform plan` — David confirms.
4. After 24h dual-run — David confirms reconciliation before pausing Fivetran.

## Context links

- `.claude/rules/ingest.md` — extractor skeleton + v1 source inventory
- `docs/handovers/Davv5-Track-W-Live-By-Default-Scheduler-Migration-GHL-2026-04-22_20-15.md` — the GHL Cloud Run pattern this track mirrors
- Data Ops notebook: `7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a`
- Calendly API docs: `https://developer.calendly.com/api-docs/` (enumerate endpoints + auth model before writing extractor)
- Fivetran billing / tier page: `https://www.fivetran.com/pricing` (for Option A cost anchor)
