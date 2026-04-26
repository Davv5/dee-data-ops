# Staging Models — Inventory

**Sprint supporting artifact.** Model-centric view. Pairs with `source-inventory.md` (source-centric) and `gap-analysis.md` (delta).

Last regenerated: 2026-04-24 — verified by `find 2-dbt/models/staging -name "*.sql"` against the working tree.

**Legend:** ✅ Healthy · 🟡 Usable with caveats · 🔴 Present but compiles empty / broken · ⚪ Placeholder / not a model

---

## The 13 staging models

| # | Model | Source | Status | Row-count health | Notes |
|---|---|---|---|---|---|
| 1 | `stg_calendly__events` | Calendly | 🟡 | Fresh | Feeds `fct_calls_booked` + `fct_speed_to_lead_touch`. U3 blob-shim. |
| 2 | `stg_calendly__event_invitees` | Calendly | 🟡 | Fresh | Via U3 shim. Narrow coverage; no cancel/no-show signal extracted. |
| 3 | `stg_calendly__event_types` | Calendly | 🟡 | Fresh | Exists with no downstream consumer. |
| 4 | `stg_fathom__calls` | Fathom | 🟡 | Fresh (metadata only) | Transcripts 0% populated in raw; staging cannot expose what's not there. Top-level columns (`is_revenue_relevant`, `classification_*`) are queryable. |
| 5 | `stg_ghl__contacts` | GHL | 🟡 | 3–4 days stale | Core identity backbone. `bq-ingest` regression is the root cause of staleness. |
| 6 | `stg_ghl__conversations` | GHL | 🔴 | Stale AND 92% undercount on Phase-2 path | 101 rows Phase-2 vs 1,314 legacy blob. Staging points at Phase-2; current numerators are unreliable. |
| 7 | `stg_ghl__messages` | GHL | 🔴 | 0 rows upstream | Staging file compiles empty. Phase-2 extractor never wrote this entity. |
| 8 | `stg_ghl__opportunities` | GHL | 🟡 | 3–4 days stale | Pipeline + stage data. Central to Speed-to-Lead's pipeline-stage dim. |
| 9 | `stg_ghl__pipelines` | GHL | 🟡 | Stale | Feeds `dim_pipeline_stages`. |
| 10 | `stg_ghl__users` | GHL | 🔴 | 0 rows upstream | Staging file compiles empty. SDR identity kept in seed (`2-dbt/seeds/ghl_sdr_roster.csv`) because GHL user fields don't distinguish SDR/AE. |
| 11 | `stg_stripe__charges` | Stripe | 🔴 | ~50-day stale | U3 blob-shim. Stripe account banned; stale-by-design. |
| 12 | `stg_stripe__customers` | Stripe | 🔴 | ~50-day stale | Same as above. |
| 13 | `stg_typeform__responses` | Typeform | 🟡 | Fresh hourly | `form_id` is NULL (extractor gap; `not_null` test lifted in U3, restore at U9). Backfill failing 3+ days. |

**Total:** 13 `.sql` staging models across 5 sources.

---

## Model count by source (what's on disk today)

| Source | Staging models | Expected based on raw entities | Delta |
|---|---:|---:|---|
| Calendly | 3 | 3 | aligned |
| Fathom | 1 | 1 (transcripts absent, so nothing to model) | aligned |
| GHL | 6 | 6 built, 4 compile empty | 4 hollow models |
| Stripe | 2 | 9 object_types in raw | **7 unmodelled** (customers, charges are done; balance_transactions, invoices, subscriptions, products, prices, disputes, refunds are not) |
| Typeform | 1 | 2 (responses + forms) | **1 unmodelled** (`stg_typeform__forms`) |
| **Fanbasis** | **0** | **≥1** (transactions landing fresh) | **entire source unmodelled** |
| **Total** | **13** | | see gap-analysis.md |

Stripe's 7-model gap is deliberately deferred (account banned, historical-only). Everything else is a real gap.

---

## Materialization + conventions check

All 13 models confirmed materialized as `view` (conforms to Part 1 / Layer 2 rule: "Staging models MUST be materialized as `view`"). No joins. Prefix and double-underscore naming (`stg_<source>__<table>`) is consistent across all files.

---

## YAML coverage

All 5 source directories have `_<source>__sources.yml` and (except Fanbasis) `_<source>__models.yml`:

- `_calendly__sources.yml` + `_calendly__models.yml`
- `_fanbasis__sources.yml` (placeholder, `tables: []`) — **no models file**
- `_fathom__sources.yml` + `_fathom__models.yml`
- `_ghl__sources.yml` + `_ghl__models.yml`
- `_stripe__sources.yml` + `_stripe__models.yml`
- `_typeform__sources.yml` + `_typeform__models.yml`

The missing `_fanbasis__models.yml` is the visible tell for the Fanbasis gap (see `gap-analysis.md` §1).

---

## Models that exist but compile empty (watch list)

Three staging models read from raw tables that have 0 rows. They pass schema validation but return nothing:

- `stg_ghl__messages` — raw table `raw_ghl.ghl__messages_raw` never written by extractor.
- `stg_ghl__users` — raw table `raw_ghl.ghl__users_raw` never written by extractor.
- (`stg_ghl__tasks` and `stg_ghl__notes` do **not** exist as staging models despite raw tables being present and empty — they were skipped at U3.)

**Decision pending during sprint:** keep these hollow models, delete them, or repoint them at the legacy blob via shim (same approach as Calendly/Stripe/Fathom/Typeform). See `gap-analysis.md` §2.

---

## Cross-reference

- Source details (freshness, row counts, extractor status): `docs/discovery/source-inventory.md`
- What's missing vs what should exist: `docs/discovery/gap-analysis.md`
- Layer rules these models conform to: `docs/methodology.md` § Layer 2
