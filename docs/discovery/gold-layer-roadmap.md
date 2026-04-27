# Gold-Layer Roadmap — Data Discovery Sprint

**Purpose:** Ordered list of the work Phase B has to do, classified against the existing warehouse + mart scaffold. This page closes Phase A when David accepts it.

Last updated: 2026-04-27.

This page connects to:

- `docs/discovery/coverage-matrix.md` — which sources answer which questions today
- `docs/discovery/business-area-map.md` — the 13 business questions and priorities
- `2-dbt/models/warehouse/` and `2-dbt/models/marts/` — the existing scaffold this roadmap is grounded against
- `.claude/rules/warehouse.md` and `.claude/rules/mart-naming.md` — the modeling rules this roadmap conforms to

---

## What changed from the first draft

The first draft of this page proposed 14 net-new marts to build. Audit against the actual `2-dbt/models/` tree showed that draft was wrong on two counts:

1. **Most of the wide marts I proposed already exist.** `lead_journey`, `revenue_detail`, `sales_activity_detail`, and `speed_to_lead_detail` already cover contact-grain, payment-grain, booking-grain, and locked-metric-grain reporting. Per `.claude/rules/mart-naming.md` Rule 2 ("fewer, wider marts over many narrow ones"), the existing layer is correctly shaped. Adding parallel wide marts would have duplicated work and violated the rule.
2. **`fct_revenue` is already a Stripe + Fanbasis union fact** with a placeholder Fanbasis CTE waiting for staging. See `2-dbt/models/warehouse/facts/fct_revenue.sql:40–66`. The architectural decision — one revenue fact per business process, unioned across platforms — was made when the warehouse layer landed. A separate `fct_fanbasis_transactions` fact would have violated `.claude/rules/warehouse.md` ("Facts: `fct_<event>.sql` … one per business process, not per source platform").

This revised roadmap is grounded against the existing scaffold and the corpus modeling rules.

---

## The architectural anchor

The single load-bearing fact for Phase B is the placeholder CTE in `fct_revenue.sql`:

```sql
fanbasis_payments as (
    -- Placeholder: structural union-parity stub until the Fanbasis
    -- extractor + staging ship. Zero-row-producing `where false` keeps
    -- this model green without mocking payment volume.
    select cast(null as string) as payment_id,
           'fanbasis'           as source_platform,
           ...
    from unnest([struct(1 as _placeholder)])
    where false
),
```

The whole revenue half of Phase B is "build `stg_fanbasis__transactions` and replace this CTE." Once that lands, `fct_revenue` automatically carries Fanbasis rows, `revenue_detail` automatically reports Fanbasis revenue, and `lead_journey` automatically marks Fanbasis-paying contacts as `has_any_payment_flag = true`. No new fact, no new mart — just fill the hole.

---

## Existing scaffold (what's already shipped)

| Layer | Models |
|---|---|
| **Staging** | 13 models across Calendly / Fathom / GHL / Stripe / Typeform. **Fanbasis: zero models** — the gap. |
| **Warehouse facts** | `fct_calls_booked`, `fct_outreach`, `fct_revenue` (Stripe live + Fanbasis placeholder), `fct_speed_to_lead_touch` |
| **Warehouse dims** | `dim_calendar_dates`, `dim_contacts`, `dim_offers`, `dim_pipeline_stages`, `dim_sdr`, `dim_source`, `dim_users` |
| **Warehouse bridges** | `bridge_identity_contact_payment` |
| **Marts** | `speed_to_lead_detail` (locked-metric, shipped 2026-04-23), `revenue_detail` (payment-grain), `lead_journey` (contact-grain "golden lead"), `sales_activity_detail` (booked-call grain), plus the `marts/rollups/speed_to_lead/` directory |

The mart layer is wider and more complete than the first-draft roadmap assumed. Most reporting questions have a wide mart already pointing at them; what's missing is upstream completeness (Fanbasis staging) and a few net-new facts/dims.

---

## Phase B work, classified

Each row from the first draft is reclassified into one of four categories:

- **🟢 Net-new build** — genuinely missing model that has to be authored.
- **🟡 Fill the placeholder** — model exists but is gated on a single upstream staging build.
- **🔵 Rollup of existing wide mart** — wide mart already covers the surface; what's missing is a period-grain scorecard.
- **⚪ Already shipped** — exists and works; widens automatically when an upstream gap closes.

| Original rank | Original mart name | Reclassification | What's actually needed |
|---:|---|---|---|
| 1 | `fct_fanbasis_transactions` | 🟡 Fill the placeholder | Build `stg_fanbasis__transactions`. Replace the placeholder CTE in `fct_revenue.sql:40–66`. **No new fact.** |
| 2 | `mart_setter_performance` | 🔵 Rollup | `lead_journey` already does contact-grain SDR attribution. Build a weekly setter-scorecard rollup if needed. |
| 3 | `fct_calls_held` | 🟢 Net-new build | Genuinely missing. `stg_fathom__calls` exists; no Fathom fact yet. |
| 4 | `mart_lead_to_paid` | ⚪ Already shipped | `lead_journey` is the lead → paid mart. Auto-widens when Rank 1 lands. |
| 5 | `dim_typeform_form` | 🟢 Net-new build | Genuinely missing. Needs `/forms` extractor pull + staging + dim. |
| 6 | `fct_opportunity_stage_transitions` | 🟢 Net-new build | Genuinely missing. Confirm GHL emits stage-transition events first. |
| 7 | `mart_show_rate` | 🔵 Rollup | `lead_journey` already carries `showed_calls_count` + `cancelled_bookings_count`. Build a weekly show-rate rollup. |
| 8 | `mart_lead_quality_by_source` | 🔵 Rollup | `dim_source` + UTM flow already in `dim_contacts` → `lead_journey`. Build a per-form / per-source quality rollup. |
| 9 | `fct_fanbasis_refunds` | 🟢 Net-new build (recast) | Per warehouse rule, this is `fct_refunds` (per business process), not Fanbasis-specific. Depends on Fanbasis refund/chargeback shape. |
| 10 | `mart_pipeline_velocity` | 🔵 Rollup | Downstream of Rank 6. Build as a rollup of `fct_opportunity_stage_transitions`. |
| 11 | `mart_closer_performance` | 🔵 Rollup | `revenue_detail` does closer attribution at payment grain. Build a weekly closer-scorecard rollup. |
| 12 | `mart_retention_cohorts` | 🟢 Net-new build | Depends on Fanbasis customer/subscription entities (vendor-support). |
| 13 | `mart_attribution_multi_touch` | ⚪ Already shipped (placeholder) | `lead_journey` carries `first_touch_*` / `last_touch_*` placeholders ready for a multi-touch bridge. Build the bridge; the mart auto-widens. |
| 14 | `mart_call_content_themes` | 🟢 Net-new build | Depends on Rank 3 + Fathom transcripts (vendor-support). |

---

## Phase B build sequence

### Phase B.1 — Fill the placeholder (highest leverage)

**Single ticket: `stg_fanbasis__transactions` + wire into `fct_revenue`.**

- Build `stg_fanbasis__transactions` view from `raw_fanbasis.transactions` matching the column shape on lines 17–37 of `fct_revenue.sql` (the Stripe CTE).
- Fill the `_fanbasis__models.yml` placeholder.
- Replace the `where false` placeholder CTE in `fct_revenue.sql:40–66` with `select … from {{ ref('stg_fanbasis__transactions') }}`.
- Verify: `revenue_detail` rows count for `source_platform = 'fanbasis'` goes from 0 → real number. `lead_journey.has_any_payment_flag = true` for Fanbasis-matched contacts.

This single ticket flips Q2 / Q4 / Q5 / Q8 / Q9 / Q11 from blocked toward usable. It is the highest-leverage move on the whole roadmap.

**Open question worth confirming before ticket starts:** does `bridge_identity_contact_payment` already handle Fanbasis `payment_id` shape, or does it need a Fanbasis-aware match-method addition? Read the bridge before starting Phase B.1.

### Phase B.2 — Net-new facts and dims (parallel)

Three independent tickets, all unblocked today:

- **`fct_calls_held`** (Rank 3) — grain: one row per Fathom call. PK: `fathom_call_id`. Reads `stg_fathom__calls` + joins to `dim_contacts` via attendee email. **Open question:** is the Fathom-attendee-email → GHL contact join reliable enough? Confirm before ticket starts.
- **`dim_typeform_form`** (Rank 5) — grain: one row per Typeform form. PK: `typeform_form_id`. Needs a one-time `/forms` endpoint pull + `stg_typeform__forms` + dim.
- **`fct_opportunity_stage_transitions`** (Rank 6) — grain: one row per opportunity stage transition. PK: `_surrogate_key(opportunity_id, transition_at, to_stage_id)`. **Confirm first** that GHL emits stage-transition events with timestamps in the raw landing.

### Phase B.3 — Rollup marts on top of existing wide marts

These are period-grain scorecards on top of `lead_journey` / `revenue_detail`. None require new facts; all require business agreement on the period grain (default proposal: weekly).

- `setter_scorecard_weekly` (Rank 2) — rollup of `lead_journey` by `assigned_sdr_name × period_start`.
- `show_rate_weekly` (Rank 7) — rollup of `lead_journey` by `assigned_sdr_name × period_start`.
- `lead_quality_by_source` (Rank 8) — rollup of `lead_journey` by `first_touch_source × period_start`. Better when Rank 5 lands (form-level granularity).
- `closer_scorecard_weekly` (Rank 11) — rollup of `revenue_detail` by `closer_name × period_start`.
- `pipeline_velocity` (Rank 10) — rollup of `fct_opportunity_stage_transitions` by `opportunity_id`. Depends on Phase B.2 Rank 6.

Per `.claude/rules/mart-naming.md` Rule 2, several of these may consolidate further once we see how dashboards consume them. Default to "ship one, see what's missing, then decide whether the second one is real" rather than scaffolding all five up front.

### Phase B.4 — Net-new with vendor-support gates (deferred)

- `fct_refunds` (recast Rank 9) — depends on Fanbasis refund/chargeback shape investigation.
- `retention_cohorts` (Rank 12) — depends on Fanbasis subscription/customer entities.
- `call_content_themes` (Rank 14) — depends on Rank 3 + Fathom transcripts (plan tier / vendor support).

These stay in the roadmap as known-pending; no Phase B ticket starts until the vendor-support question resolves.

### Phase B.5 — Multi-touch attribution

Recast Rank 13. The wide mart slot already exists (`lead_journey.first_touch_*` / `last_touch_*` are placeholders). Phase B work is "build the multi-touch bridge upstream" — not a new mart. Defer until Phase B.2 Rank 5 lands and the form-level attribution surface is known.

---

## The blocker that gates the most marts

Per the coverage matrix and the per-mart classifications above, **the GHL trusted-copy decision gates more downstream work than any other single item**:

- All four existing wide marts (`speed_to_lead_detail`, `revenue_detail`, `lead_journey`, `sales_activity_detail`) consume GHL-side dimensions whose freshness is gated on the trusted-copy decision.
- Phase B.3 rollup marts inherit this dependency.
- Phase B.2 `fct_opportunity_stage_transitions` (Rank 6) is gated on it directly.

Fanbasis is the highest-leverage *build*; GHL trusted-copy is the highest-leverage *decision*. The two are independent — Phase B.1 can ship without GHL trust resolved, and the GHL decision can be made without waiting on Fanbasis. Run them in parallel.

---

## Open decisions still owed

These are not blockers to closing Phase A, but they're load-bearing for Phase B execution:

1. **GHL trusted-copy decision** — Phase-2 path vs legacy blob. Gates ~7 of the existing/proposed marts.
2. **Fanbasis bridge match-method** — does `bridge_identity_contact_payment` need a Fanbasis-aware match strategy, or does the existing email-based match work? Read the bridge before Phase B.1 starts.
3. **Fathom → GHL contact join key** — attendee email reliability. Affects Phase B.2 Rank 3.
4. **GHL stage-transition event presence** — does raw landing carry stage-change events with timestamps? Affects Phase B.2 Rank 6.
5. **Period grain for rollup marts** — proposal: weekly. Daily is too noisy, monthly too coarse for coaching. Phase B.3 default unless contradicted.
6. **Fanbasis refund/chargeback shape** — vendor-support investigation. Gates Phase B.4 `fct_refunds`.

---

## What this means for closing Phase A

Phase A closes when David accepts this roadmap. The classification + Phase B sequence above are the deliverable. After acceptance:

1. This file gets committed and PR'd against `main` (per `CLAUDE.md` — never commit to `main` directly).
2. Phase B.1 (the Fanbasis staging ticket) becomes the next branch / next ticket.
3. Phase B.2 net-new builds enter parallel queue; Phase B.3 rollups enter the queue ordered behind Phase B.1 + Phase B.2 dependencies.
4. The strategic-reset plan (`docs/plans/2026-04-24-strategic-reset.md`) is marked complete; the paused GTM-source-port plan resumes (or is superseded) on its own track.

When each new model is scaffolded, cite the row in the classification table by its original-rank number in the model's `description:` block so the design rationale stays traceable from SQL back to discovery.
