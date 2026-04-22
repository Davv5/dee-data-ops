# Session Handover — Track D: Speed-to-Lead Metabase caching config + weekly digest scaffold

**Branch:** `Davv5/Track-D-STL-Metabase-Admin-Config`
**Timestamp:** `2026-04-22_17-12` (executed)
**Author:** track-executor (Claude Sonnet 4.6)
**PR:** not yet opened

---

## Session goal

Ship two Metabase admin-config changes on the self-hosted OSS v0.60.1 instance:
1. Enable server-wide query-result caching + 6-hour TTL on the Speed-to-Lead dashboard.
2. Set up a weekly Monday 06:00 ET email subscription of the Speed-to-Lead dashboard.

**Partial ship:** Item 2 is scaffolded but not activated. SMTP is not configured on
the instance. The subscription script is committed with a guard that exits 1 with
bootstrap instructions. David must complete SMTP setup before the pulse can be created.

## Changed files

```
ops/metabase/authoring/infrastructure/caching_config.py       — created — idempotent caching config; checks toggle (read-only), sets per-dashboard TTL
ops/metabase/authoring/infrastructure/dashboard_subscriptions.py — created — idempotent pulse upsert with SMTP guard + timezone assert
ops/metabase/.env.metabase.example                             — edited  — added STL_WEEKLY_DIGEST_RECIPIENTS= placeholder
.claude/rules/metabase.md                                      — edited  — two "Lessons learned" bullets (caching read-only gotcha + subscription SMTP prereq)
WORKLOG.md                                                     — edited  — dated entry: 2026-04-22 Track D partial ship
docs/handovers/Davv5-Track-D-Execution-2026-04-22_17-12.md    — created — this file
```

**NOT touched:**
- `ops/metabase/authoring/dashboards/speed_to_lead.py` — Track A/B/C territory
- Any `dbt/**` file
- Production secrets, GH Actions secrets

## Commands run / verification outputs

**Pre-flight BQ connection check (green):**
```
BigQuery connection: dee-data-ops-prod (id=2)
Before:
  auto_run_queries             = False
  details.include-user-id-and-hash = False
After:
  auto_run_queries             = False
  details.include-user-id-and-hash = False
PUT fired: False
State matches desired
```

**SMTP probe result (SMTP NOT configured):**
```
email-configured? = False
email-smtp-host = None
email-smtp-port = None
email-smtp-username = None
email-smtp-password = None
```
Note: `/api/email` returns 404 on OSS v0.60.1. SMTP status read from `GET /api/setting` list.

**caching_config.py first run:**
```
Server-wide caching settings:
  enable-query-caching: have=False want=True -- READ-ONLY (cannot PUT via API on v0.60.1)

  ACTION REQUIRED: Set MB_ENABLE_QUERY_CACHING=true in ops/metabase/runtime/docker-compose.yml, ...

Dashboard 'Speed-to-Lead' (id=3):
  cache_ttl before: None  want: 21600
  PUT fired.
  cache_ttl after:  21600

ACTION NEEDED: enable-query-caching is still OFF.
```

**caching_config.py second run (idempotency confirmed):**
```
Dashboard 'Speed-to-Lead' (id=3):
  cache_ttl before: 21600  want: 21600
  ok (no PUT).
  cache_ttl after:  21600
```

**dashboard_subscriptions.py SMTP guard test:**
```
ERROR: SMTP is not configured on this Metabase instance.
email-configured? = False
...
Exit code: 1
```

**Metabase rule sync:**
```
Synced .claude/rules/metabase.md → Data Ops notebook, source ID 65c7f876 at 17:33 local
```

## Decisions made

- **`enable-query-caching` is read-only on v0.60.1** — confirmed by HTTP 500 "read-only
  setting" from `PUT /api/setting/enable-query-caching`. Fix is `MB_ENABLE_QUERY_CACHING=true`
  env var in docker-compose.yml. Script warns rather than errors so the dashboard TTL still
  ships without blocking.
- **Per-dashboard `cache_ttl = 21600 s` SET and persisted** — the PUT to
  `/dashboard/3` succeeded and the value persisted on re-read. The corpus suggested this
  was Pro-only; empirically it works on OSS v0.60.1. The 6-hour TTL aligns with the
  daily dbt prod refresh cadence (source: *"Caching query results"*, Metabase Learn,
  source d6a8e3ae).
- **SMTP skip = partial ship per David's pre-execution authorization.** `dashboard_subscriptions.py`
  scaffolded with `check_smtp()` guard (exits 1), timezone assertion (`report-timezone =
  America/New_York`), and recipient reading from `STL_WEEKLY_DIGEST_RECIPIENTS` env var.
  Script is ready to run once SMTP is configured.
- **`report-timezone` pre-change value: `null`** — the instance was using system default
  (UTC). The subscription script will assert `America/New_York` before creating the pulse,
  so `schedule_hour = 6` fires at 06:00 ET, not 06:00 UTC (~01:00-02:00 ET).
- **Dashboard id=3 is the primary target** — collection 6, `public_uuid = 163abd8d-b16a-4f88-95b9-881a506aa461`,
  view_count = 53. id=2 (collection 5, no public UUID, view_count = 2) is the older copy.
  The `_find_dashboard` helper prefers the copy with a public_uuid.
- **Recipient: `mannyshah4344@gmail.com`** (David's personal email, pre-launch staging).
  Added to local `.env.metabase` (gitignored). Placeholder added to `.env.metabase.example`.

## Unresolved risks / open threads

- **ACTION REQUIRED — server restart for caching toggle:**
  1. SSH into the GCE VM running Metabase.
  2. Edit `ops/metabase/runtime/docker-compose.yml` — add `MB_ENABLE_QUERY_CACHING: "true"` to the environment block.
  3. `docker compose down && docker compose up -d`
  4. Re-run `python -m ops.metabase.authoring.infrastructure.caching_config` — should now print `enable-query-caching: have=True want=True -- ok`.
  5. Verify second public-share page load (https://34-66-7-243.nip.io/public/dashboard/163abd8d-b16a-4f88-95b9-881a506aa461) hits cache — zero new `stl_*` scans in GCP BQ job history within 2 minutes.

- **ACTION REQUIRED — SMTP bootstrap for subscription delivery:**
  Full steps in `ops/metabase/authoring/infrastructure/dashboard_subscriptions.py` docstring (SMTP BOOTSTRAP section):
  1. Choose provider (SendGrid recommended — free tier 100 emails/day).
  2. Store creds in GCP Secret Manager (`dee-data-ops-prod`): `metabase-smtp-host`, `metabase-smtp-port`, `metabase-smtp-user`, `metabase-smtp-pass`.
  3. Configure SMTP in Metabase Admin UI (Admin → Settings → Email) — one-time bootstrap, not via API (same pattern as BQ connection).
  4. Verify with "Send test email" in Admin UI.
  5. Run `python -m ops.metabase.authoring.infrastructure.dashboard_subscriptions` — should create the pulse and print schedule/recipients/cards.

- **BQ cache-hit verification** — deferred until after server restart (enable-query-caching OFF means no cache hits currently). Add WORKLOG note when complete.

- **Dashboard footer** — does not yet mention the 6-hour cache window. Pick up on next `speed_to_lead.py` edit (Track A/B/C territory).

- **Monitor first Monday digest** — 2026-04-28 06:00 ET (after SMTP bootstrap + pulse creation). Check Admin → Troubleshooting → Tasks if email doesn't arrive.

## First task for next session

Complete the server-restart task above (MB_ENABLE_QUERY_CACHING env var) then run
`caching_config.py` to confirm the toggle flips. Then bootstrap SMTP and run
`dashboard_subscriptions.py` to create the pulse.

## Context links

- `ops/metabase/authoring/infrastructure/caching_config.py` — the new caching script
- `ops/metabase/authoring/infrastructure/dashboard_subscriptions.py` — the new subscription script
- `ops/metabase/authoring/infrastructure/bigquery_connection.py` — canonical idempotent-assert pattern
- `ops/metabase/authoring/client.py` — MetabaseClient
- `.claude/rules/metabase.md` — updated Lessons learned section
- Track D plan: `docs/handovers/Davv5-Track-D-stl-metabase-admin-config-2026-04-22_17-12.md`
- WORKLOG entry: `## 2026-04-22 — Track D: Speed-to-Lead Metabase caching config + subscription scaffold (partial ship)`
