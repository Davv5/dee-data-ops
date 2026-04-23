# Session Handover — Track Z: Metabase live-by-default (cache_ttl=0, dashboard auto-refresh 60s, freshness tile, `live-by-default.md` rule)

**Branch:** `Davv5/Track-Z-Live-By-Default-Metabase-Defaults-And-Rule`
**Timestamp:** `2026-04-22_20-15` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Session goal

Flip Metabase's authoring defaults so new Speed-to-Lead tiles (and any
future dashboard built on live marts) render ≤60s-fresh data without
per-tile configuration:

1. `ops/metabase/authoring/sync.py` → new questions default to
   `cache_ttl=0` (no question-level cache) so incremental mart refreshes
   land on the dashboard immediately.
2. `ops/metabase/authoring/dashboards/speed_to_lead.py` → dashboard
   auto-refreshes every 60s; existing STL tiles get their `cache_ttl`
   reset to 0 via a one-off authoring run.
3. Add an **end-to-end freshness tile** at the top of the STL dashboard:
   "Data as of: <ingest lag in minutes>" — sourced from
   `timestamp_diff(current_timestamp(), max(_ingested_at), minute)` in
   `raw_ghl.conversations`. Makes regressions visible instantly.
4. Author a new `.claude/rules/live-by-default.md` rule documenting the
   defaults, the acceptable-deviation cases (heavy aggregations that
   *should* cache), and linking to Tracks W/X/Y for the upstream
   freshness chain.

When this ships (stacked on Tracks W + Y): a GHL event landing at T+0
shows up in the STL dashboard in ≤ ~90s, and the "Data as of" tile
makes that lag visible and auditable.

## Changed files (expected)

```
ops/metabase/authoring/sync.py                              — edited — upsert_card + upsert_dashboard default cache_ttl=0
ops/metabase/authoring/dashboards/speed_to_lead.py          — edited — add auto_apply_filters + dashboard-level auto_refresh 60s; add freshness tile at row 0; reset cache_ttl=0 on all STL cards on next run
ops/metabase/authoring/infrastructure/caching_config.py     — edited — redefine DASHBOARD_CACHE_TTL_SEC baseline from 21600 (6h) to a PER-DASHBOARD lookup + flag live-mode dashboards to 0
.claude/rules/live-by-default.md                            — created — portable rule: live-by-default defaults + when to deviate
.claude/rules/metabase.md                                   — edited — amend the "Lessons learned" caching paragraph to reflect live-by-default rollups case
docs/runbooks/metabase-live-dashboard-setup.md              — created — how to ship a new live-by-default dashboard (checklist for downstream engineers)
WORKLOG.md                                                  — edited — dated entry
docs/handovers/Davv5-Track-Z-Execution-<timestamp>.md       — created
```

**No change to:**
- `ops/metabase/runtime/docker-compose.yml` (the server-level `MB_ENABLE_QUERY_CACHING` env var stays `true` — we want caching enabled at the server level so per-dashboard / per-question overrides work; we just set `cache_ttl=0` on live dashboards to bypass it).
- The STL SQL queries themselves. Tiles point at the existing `stl_*` marts; they don't change. Materialization cadence is Track Y's concern.

## Tasks (ordered)

### Pre-flight

- [x] Read `ops/metabase/authoring/sync.py` — identify the current `upsert_card` payload (line ~35-68). Note: no `cache_ttl` key today; Metabase defaults to `null` which means "use server default".
- [x] Read `ops/metabase/authoring/dashboards/speed_to_lead.py` in full — identify every `upsert_card` call (there are ~12; each tile is one) and the `upsert_dashboard` call.
- [x] Read `ops/metabase/authoring/infrastructure/caching_config.py` end-to-end — note the **OSS limitation finding** in the file's docstring: `enable-query-caching` is a read-only setting via REST API on OSS v0.60.1, but per-dashboard `cache_ttl` via dashboard PUT endpoint DOES work. Confirm this finding still holds at execution time by running `caching_config.py` first and reading its output.
- [x] Read `.claude/rules/metabase.md` — specifically the "Lessons learned" caching paragraph at the end. That paragraph says "6h for dbt-prod-daily rollups"; this track replaces that guidance for live rollups specifically.
- [x] `ask-corpus scope: methodology` the query: corpus confirmed auto-refresh is URL-fragment only on all Metabase OSS versions — no REST API key exists for `refresh_period`. Captured in WORKLOG + live-by-default.md Lessons Learned.
- [x] **STOP resolved as "pick sensible default" per Open Questions:** corpus answer was definitive (URL fragment only). Proceeding with URL-fragment fallback path — documented in rule + runbook + sync.py docstring. No REST API change needed or possible.

### Flip the defaults in `sync.py`

- [x] Edit `ops/metabase/authoring/sync.py` → `upsert_card`:
  - Add a new keyword argument `cache_ttl: int | None = 0` to the function signature. Default `0` = no cache (live).
  - Add `"cache_ttl": cache_ttl` to the `payload` dict. Metabase's card PUT/POST accepts this key.
  - Add `cache_ttl` as a keyword argument to `upsert_dashboard` too (same default of `0`).
- [x] Corpus confirmed `cache_ttl=0` = "do not cache" (explicit bypass); `null` = "use server default". Keeping `0` as the default per the decision already made. Documented in sync.py docstring and live-by-default.md Lessons Learned.
- [x] Deviation pattern documented in sync.py docstring and live-by-default.md "When to deviate" section. Override: `upsert_card(..., cache_ttl=3600)`.

### Update `caching_config.py` for per-dashboard lookup

- [x] Edit `ops/metabase/authoring/infrastructure/caching_config.py`:
  - Replaced single `DASHBOARD_CACHE_TTL_SEC = 21600` with typed dict `DASHBOARD_CACHE_TTL_SEC: dict[str, int | None]` containing Speed-to-Lead and Speed-to-Lead Lead Detail at 0.
  - Added `DEFAULT_CACHE_TTL_SEC: int | None = 0` for dashboards not in the dict.
  - Updated `main()` to iterate all named dashboards idempotently with per-dashboard TTL lookup. LookupError continues (skips missing dashboard, logs a warning).

### Add the dashboard auto-refresh + freshness tile

- [x] In `ops/metabase/authoring/dashboards/speed_to_lead.py`:
  - `refresh_period=60` via REST API: NOT POSSIBLE on Metabase OSS — corpus confirmed this is URL-fragment only (`#refresh=60`). No code change to `upsert_dashboard` for this. Documented in `upsert_dashboard` docstring, `live-by-default.md` Lessons Learned, and `metabase.md`.
  - Added `freshness_tile` at row 0 (6×2, top-left). SQL: `timestamp_diff(current_timestamp(), max(_ingested_at), minute)` from `raw_ghl.conversations`. `cache_ttl=0` explicit.
  - All existing rows shifted down by 2 (header → row 2, T1 hero → row 4, ..., footer → row 41).
  - Dashboard layout comment updated to v1.7.
- [x] Track E coordination done: Track E's "Data refreshed" footer tile is at row 39 in the pre-Z layout (row 41 post-shift). Track Z tile is at row 0. Zero overlap. Code comments on freshness_tile cross-reference the footer tile and explain the distinction. `freshness_tile["id"]` added to `kept_ids` in orphan cleanup.

### Author the `live-by-default.md` rule

- [x] Create `.claude/rules/live-by-default.md`:
  ```markdown
  ---
  paths:
    - "dbt/models/marts/rollups/**"
    - "ops/metabase/authoring/dashboards/**"
    - "ops/metabase/authoring/sync.py"
    - "ops/metabase/authoring/infrastructure/caching_config.py"
  ---

  # Live-by-default conventions

  As of 2026-04 the Speed-to-Lead panel ships with ~60s end-to-end
  freshness (raw landing → dashboard render). The defaults below are
  encoded so NEW rollups and dashboards inherit that behavior without
  per-model / per-tile configuration.

  ## dbt (see `dbt-marts-rollups.md` for detail)

  - Rollups under `marts/rollups/**` default to `materialized: incremental`,
    `incremental_strategy: insert_overwrite`, `on_schema_change: append_new_columns`
    via `dbt_project.yml`.
  - 2-minute Cloud Run Job rebuilds `stl_* sales_activity_detail` on prod.
  - Nightly full-refresh (08:00 UTC) reconciles any drift.

  ## Custom-source ingestion (see `ingest.md` for detail)

  - Near-real-time sources (sub-5-min SLA, dashboard-load-bearing) run on
    Cloud Run Jobs + Cloud Scheduler.
  - Hot endpoints: 1-min cadence. Cold endpoints: 15-min cadence.
  - GHA cron remains the default for daily-or-coarser sources + as manual
    backstop for the NRT sources.

  ## Metabase

  - `upsert_card` defaults `cache_ttl=0` (no per-question cache). A question
    returns the latest query every render.
  - Dashboards default to `refresh_period=60` (auto-refresh every 60 seconds).
  - Every live-by-default dashboard carries a top-of-page "Data freshness"
    tile sourced from `raw_<source>._ingested_at` so ingest-pipeline
    regressions are immediately visible.
  - Server-level caching stays ON (`MB_ENABLE_QUERY_CACHING=true`) so
    per-dashboard overrides work — live dashboards set `cache_ttl=0`;
    daily-cadence dashboards set `cache_ttl=21600`.

  ## When to deviate

  **Cache the tile (non-zero `cache_ttl`) when:**
  - The underlying SQL does heavy aggregation over a large window
    (> 10M rows scanned) AND the answer genuinely changes on daily cadence.
  - A third-party tile (e.g., a public-share link to a non-authed audience)
    needs to absorb traffic without re-querying BQ each render.
  - The underlying mart is built on a `table` materialization refreshed
    only in the nightly workflow (not in the 2-min builder).

  **Accept slower-than-1-min cadence when:**
  - The source vendor doesn't support sub-5-min polling (e.g., Fivetran
    free/standard tier).
  - The API rate limit doesn't allow 1-min cadence without bursting 429s.
  - The tile's SLA is inherently coarser than minute-level (monthly
    reporting, daily ops reviews).

  Decisions to deviate are recorded in the relevant track's WORKLOG entry
  with a one-line rationale.

  ## Upstream chain

  - Track W (2026-04-22): GHL extractor → Cloud Run Jobs (1-min hot / 15-min cold)
  - Track X (2026-04-22): Calendly extractor → Cloud Run Jobs (1-min)
  - Track Y (2026-04-22): dbt incremental + 2-min Cloud Run builder + freshness-threshold retune
  - Track Z (2026-04-22): THIS rule + Metabase defaults + freshness tile

  ## Lessons learned

  (Populate as live-by-default issues surface — cache poisoning, stale
   refreshes, Cloud Run cold starts, etc.)
  ```
- [x] The PostToolUse hook syncs this file into the Data Ops notebook automatically. Check `/tmp/dataops-sync-rule.log` after save to confirm.

### Amend `metabase.md`

- [x] Edited `.claude/rules/metabase.md` Lessons Learned caching paragraph: updated to live-by-default (cache_ttl=0 for live rollups, 21600 for daily-cadence). Added auto-refresh OSS gotcha (URL-fragment only). Kept OSS read-only toggle gotcha unchanged. Pulses/subscriptions paragraph unchanged.

### Runbook

- [x] Created `docs/runbooks/metabase-live-dashboard-setup.md` — 9-step checklist. Covers mart pre-req, script authoring, `cache_ttl=0` inheritance, URL-fragment auto-refresh setup (with OSS caveat), freshness tile template, `caching_config.py` dict update, deploy + verify steps, one-off reset path.

### Wrap

- [x] WORKLOG entry appended (2026-04-22 evening).
- [x] Local commit. Commit hash: `eb02415`.

## Decisions already made

- **`cache_ttl=0` as the new default** over `cache_ttl=null`. `0` is explicit and self-documenting; `null` is ambiguous ("is this live or is this server-default?"). If OSS v0.60.1 silently interprets `0` as "server default," fall back to `null` and note in Lessons Learned.
- **Top-of-dashboard freshness tile is live-by-default convention**, not a Speed-to-Lead-specific tile. Every future live dashboard gets one via the runbook checklist.
- **Two freshness tiles on the STL dashboard (raw lag + rollup computed_at)** — they measure different things; both are valuable. Don't collapse them into one.
- **Server-level `MB_ENABLE_QUERY_CACHING=true` stays on.** Per-dashboard overrides depend on it being enabled server-side; turning it off would break the few dashboards that legitimately want caching (future daily-cadence dashboards).
- **`caching_config.py` becomes a dashboard-name → TTL lookup**, not a single constant. Future daily-cadence dashboards can be added with `21600` without touching the live defaults.
- **Track E coordination:** Track Z's freshness tile goes at ROW 0 (top); Track E's "Data refreshed" tile stays at ROW 39 (footer). No conflict. Code comments in both tiles cross-reference the other.
- **Rule scope:** `.claude/rules/live-by-default.md` cross-references three existing rules (`dbt-marts-rollups.md`, `ingest.md`, `metabase.md`). Not a replacement for any; a coherence-bearing index.

## Open questions

- **Does Metabase OSS v0.60.1 accept `refresh_period=60` via dashboard PUT payload?** The exact key name varies between Metabase versions. If it doesn't, the fallback is the URL-fragment `?refresh=60` path — users have to load the dashboard with the fragment, and shared links / embeds need to carry it. **Pick sensible default:** try API payload first; if it fails, document the URL-fragment fallback in the runbook + add a Metabase setup step to configure the default dashboard URL with the fragment. **STOP and ask David** if neither path works — that's a real limitation worth knowing before launch.
- **Caching-config.py one-off run — should it run as part of the deploy workflow, or manually?** **Pick sensible default:** manually for v1. It's idempotent, cheap to run, and running it in CI without observability is riskier than a conscious manual run. Document the manual step in the runbook.
- **Should the freshness tile read from `raw_ghl.conversations` (the hottest source) or from a union of raw_ghl + raw_calendly?** **Pick sensible default:** `raw_ghl.conversations` — it's the highest-cadence source in Track W; if this lags, everything downstream lags. Adding Calendly via UNION is easy later if Calendly-only regressions become a pattern.
- **What if Track Y is not yet deployed when this lands?** Track Z can ship standalone: Metabase defaults flip, freshness tile appears. Tile will show whatever lag the current (nightly or GHA-5min) path produces — ugly but accurate. Once Track Y lands, the tile auto-improves to ~1 min. No rework needed in Track Z.
- **What if Track W is not yet deployed?** Same — Metabase defaults still flip; freshness tile shows the old cadence's lag until Track W ships. The tile makes the pre-live state visible, which is actually desirable.

## Done when

- `upsert_card` in `sync.py` defaults to `cache_ttl=0`.
- `upsert_dashboard` in `sync.py` defaults to `cache_ttl=0` + `refresh_period=60` (or equivalent OSS-supported key).
- `caching_config.py` uses the dashboard-name → TTL dict; running it sets Speed-to-Lead cards to `cache_ttl=0`.
- The Speed-to-Lead dashboard has a top-row "Data freshness" tile reading from `raw_ghl.conversations._ingested_at`.
- `.claude/rules/live-by-default.md` exists, syncs to the Data Ops notebook, and documents the chain (W → X → Y → Z).
- `.claude/rules/metabase.md`'s caching paragraph is updated for live-by-default.
- Runbook `docs/runbooks/metabase-live-dashboard-setup.md` documents the new-dashboard checklist.
- Running `python -m ops.metabase.authoring.dashboards.speed_to_lead` against the prod Metabase instance succeeds and produces the new defaults on all STL tiles + the new freshness tile.
- WORKLOG + handover + local commit.

## Dependencies

- **Coordinates with Track E (PR #50, currently open):** both touch `dashboards/speed_to_lead.py` and both add a freshness-like tile. Different rows, different purposes — **merge Track E first, then Track Z rebases to add the top-row tile without disturbing Track E's footer tile**. If this is infeasible for some reason, Track Z waits until Track E merges, then opens its PR (low risk of conflict).
- **Independent of Tracks W / X / Y in execution order:** Track Z can be authored, committed, and merged without any of them shipping. Its effectiveness depends on W+Y for end-to-end freshness to actually be live — but the tile, the auto-refresh, and the rule are all valuable on their own.
- **Parallel-safe with Tracks W / X / Y:** zero file overlap with any of them. (Track Y touches `caching_config.py` only incidentally — this track's edits are the canonical ones; Y should not touch it.)

## Manual-verification checkpoints

1. After Pre-flight — David confirms the refresh_period REST API path works on OSS (or accepts URL-fragment fallback).
2. After `sync.py` + `caching_config.py` edits — David eyeballs a dev-Metabase test run to confirm cache_ttl=0 persists on new cards.
3. After the freshness tile lands — David confirms the tile placement doesn't collide with Track E's footer tile.
4. After full STL script run against prod — David loads the dashboard in a browser, confirms auto-refresh ticks, confirms freshness tile reads <2 min at steady state (post Tracks W+Y) or current-cadence lag (pre those tracks).

## Context links

- `.claude/rules/metabase.md` — the 5-rule Metabase convention this track amends
- `.claude/rules/mart-naming.md` — dashboard-facing naming (no change, but referenced in the new `live-by-default.md`)
- Metabase Craft notebook: `ce484bbc-546b-4fe4-a7db-bc01b847dbe5` (query via `ask-corpus scope: methodology.metabase`)
- Metabase Learn notebook: for authoring/SQL/visualization questions
- Track W handover — upstream ingest freshness
- Track X handover — Calendly source path
- Track Y handover — dbt incremental + 2-min builder
- Track E (PR #50) — the "Data refreshed" footer tile to coordinate with
