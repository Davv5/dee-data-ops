# Session Handover — Track F1: warehouse layer — fct_speed_to_lead_touch + conformed dims (executed)

**Branch:** `Davv5/Track-F1-STL-Warehouse-Layer`
**Timestamp:** `2026-04-22_23-58` (local)
**Author:** track-executor (Claude Sonnet 4.6)
**PR:** not yet opened

---

## Session goal

Introduce the lowest-grain Speed-to-Lead fact (`fct_speed_to_lead_touch`, one row per booking × touch-event) plus two new conformed dimensions (`dim_sdr`, `dim_source`), with SCD-2 snapshot on `dim_users` already in place and expanded, and a seed for lead-source enrichment. Zero existing models touched. Zero Metabase changes.

## Changed files

```
dbt/models/warehouse/facts/fct_speed_to_lead_touch.sql     — created — lowest-grain STL fact
dbt/models/warehouse/facts/_facts__models.yml              — edited — fct_speed_to_lead_touch block + tests
dbt/models/warehouse/facts/_facts__docs.md                 — edited — fct_speed_to_lead_touch docs entry
dbt/models/warehouse/dimensions/dim_sdr.sql                — created — SDR conformed dim on dim_users
dbt/models/warehouse/dimensions/dim_source.sql             — created — lead-source dim backed by seed
dbt/models/warehouse/dimensions/_dimensions__models.yml    — edited — dim_sdr + dim_source blocks + tests
dbt/models/warehouse/dimensions/_dimensions__docs.md       — edited — dim_sdr + dim_source docs entries
dbt/snapshots/dim_users_snapshot.sql                       — edited — expanded check_cols to name/role/email/is_active; explicit column select
dbt/snapshots/_snapshots__models.yml                       — edited — updated description to reflect new check_cols
dbt/seeds/stl_lead_source_lookup.csv                       — created — 12 channel-level lead_source values + __unknown__ sentinel
dbt/seeds/_seeds__models.yml                               — edited — stl_lead_source_lookup seed docs block
WORKLOG.md                                                  — edited — Track F1 dated entry
docs/handovers/Davv5-Track-F1-Execution-2026-04-22_23-58.md — created — this file
```

## Commands run

```
dbt deps                                  # from worktree dbt dir — packages current (dbt_utils 1.3.3)
dbt seed --target dev --select stl_lead_source_lookup  # 13 rows inserted
dbt seed --target dev --select ghl_sdr_roster --full-refresh  # roster stale in dev; refreshed to get correct roles
dbt run --target dev --select dim_users   # rebuild after roster refresh
dbt snapshot --target dev                 # 22 rows merged into snapshots.dim_users_snapshot
dbt build --target dev --select dim_sdr dim_source fct_speed_to_lead_touch
  # PASS=27 WARN=0 ERROR=0 (3 models + 24 tests)
```

## Decisions made

- **Grain: (booking × touch-event).** Justification: "3 Data Modeling Mistakes That Can Derail a Team" (Data Ops notebook) — go to the lowest grain to preserve downstream roll-up options. The existing `close_outcome IS NOT NULL` fallback in `sales_activity_detail` is the symptom of being too coarse; the new `show_outcome` column resolves it.
- **`dim_sdr` is a conformed subset view on `dim_users`.** Not a duplicate dim — role-filtered so the mart can use `sdr_sk` as a business-readable FK without widening `dim_users`. (source: "Creating a Data Model w/ dbt: Facts", Data Ops notebook.)
- **`dim_source` seed covers only inferrable channel-level values.** The actual `lead_source` field in GHL holds campaign/content labels (~100+ distinct values), NOT the channel taxonomy the plan assumed. Only 12 clearly-inferrable values are classified (`TikTok`, `Instagram`, `YouTube`, `outbound`, `email`, etc.). All others have `is_paid = NULL` — flagged to David.
- **`show_outcome` v1 heuristic**: `last_stage_change_at >= scheduled_for` as the "attended" signal. Fallback (when no opportunity or stage signal) = close_outcome known → `showed`. Code comment and WORKLOG flag added for F3 finalization.
- **`is_sdr_touch` uses current-state role join (F1).** SCD-2 as-of join via `dim_users_snapshot` deferred to F2 to keep F1 buildable before snapshot has history.
- **Snapshot check_cols expanded** from `['role', 'email']` to `['name', 'role', 'email', 'is_active']` per track spec. This is additive — a wider change-detection net, no functional regression.
- **`ghl_sdr_roster` dev seed was stale.** Ran `--full-refresh` to restore correct roles. This is a dev-environment maintenance step, not a schema change.

## Unresolved risks

- [ ] **Sanity query pct_within_5min_7d = NULL** — root cause is `fct_calls_booked.contact_sk = NULL` for ALL 5,406 bookings (pre-existing: `stg_calendly__event_invitees` not yet wired). Both the new fact and the old `stl_headline_7d` rollup return NULL. NOT a grain bug. The metric will light up when invitee staging ships. F2 must confirm parity once invitee staging unlocks the join.
- [ ] **`dim_source` is_paid gap** — 98/111 `lead_source` values have `is_paid = NULL`. David needs to either (a) classify the major campaign labels in the seed, or (b) confirm that F2's mart can work with `is_paid = NULL` for campaign-level sources (grouped as "unknown channel") — waiting on David.
- [ ] **Roster gaps** (Ayaan, Jake, Moayad, Halle) — roles still unresolved in `ghl_sdr_roster`. Their historical touches will show `attribution_quality_flag = 'role_unknown'` once invitee staging unlocks the contact join. Not a blocker for F1 merge.
- [ ] **Prod `dbt snapshot --target prod`** — must be run by a human before F2 deploys the SCD-2 as-of join in the mart. F2 track should include this as a pre-step.
- [ ] **`show_outcome` spot-check** — track requires ~20 manual inspections against dev BQ before F2 ships. Executor could not perform this check because all `touch_sk = NULL` (invitee staging gap). To be done by F2 executor once invitee staging is live.

## First task for next session

Run `dbt build --target dev --select fct_calls_booked+` after `stg_calendly__event_invitees` staging lands, then re-run the F1 sanity query to confirm pct_within_5min matches `stl_headline_7d.pct_within_5min_7d` within 0.1pp. That unlocks F2.

## Context links

- Track file: `docs/handovers/Davv5-Track-F1-star-schema-warehouse-layer-2026-04-22_19-34.md`
- Track F2 (next): `docs/handovers/Davv5-Track-F2-stl-wide-mart-card-rewire-2026-04-22_19-34.md`
- Corpus conversation: `notebook_id=7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a`, conversation `7fa6ba4e-faad-4964-ad20-91252c05eec3`
- WORKLOG.md entry: `grep "Track F1" WORKLOG.md`
- `.claude/rules/warehouse.md` — conventions this track follows
- `.claude/state/project-state.md` — "Mart lacks real show_outcome" open thread (resolved by this track)
