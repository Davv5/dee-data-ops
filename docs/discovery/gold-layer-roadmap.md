# Gold-Layer Roadmap — Data Discovery Sprint

> **STALE WARNING — 2026-05-01 reset.**
> This roadmap contains useful reasoning but is not current execution truth.
> It predates PR #142 retiring dbt `speed_to_lead_detail` and
> `sales_activity_detail`, and it predates Fanbasis staging/fact work now
> present on `main`. Read `docs/discovery/current-data-layer-truth-map.md`
> before using this roadmap for any build.

**Purpose:** Ranked list of Gold-layer marts (dashboard-ready tables) to build or refresh next, derived from the coverage matrix and scored against a value / feasibility / strategic-anchor rubric. This is the final Discovery Sprint artifact — when this page is approved, Phase A (docs-only Discovery) ends and Phase B (Layer Build) is authorized.

Last updated: 2026-04-27.

This page connects to:

- `docs/discovery/source-inventory.md` — what raw data is live and what is broken
- `docs/discovery/business-area-map.md` — the 13 business questions that drive everything below
- `docs/discovery/coverage-matrix.md` — which sources can answer which questions today
- `2-dbt/models/marts/` — where the four already-shipped marts live; the four `_detail` marts named below are existing artifacts, not new builds
- `2-dbt/models/warehouse/` — where `fct_*` / `dim_*` deps named below live or will be built

---

## How to read this page

The "next dashboard build list" the coverage matrix promised. Two things to internalize before the table:

1. **Most of the marts in this roadmap already exist as production tables.** The 2026-04-22 Speed-to-Lead refactor (Tracks F1/F2/F3) collapsed eleven narrow rollups into four wide marts — `speed_to_lead_detail`, `sales_activity_detail`, `lead_journey`, `revenue_detail`. Those marts ship today. The roadmap below is mostly "refresh on unblocker," not "build from scratch." Net-new entries (`customer_retention_detail`, `call_intelligence_detail`) are flagged explicitly.
2. **Marts use business-friendly singular names without `fct_` / `dim_` prefixes** (per `.claude/rules/mart-naming.md`). Warehouse-layer facts and dims keep Kimball naming and appear only in the data-deps column.

Status legend on the rank table:

- **Shipped** — mart exists in production, refresh-only work needed
- **Refresh** — exists but needs a meaningful re-anchor (e.g. Stripe → Fanbasis pivot, trusted-GHL-copy rewire)
- **New** — net-new mart, not yet scaffolded

---

## Scoring rubric

Rubric used: `mart-roadmap-rank` skill default (no `--rubric` override supplied).

Each candidate mart scores on three dimensions, summed for total rank order (higher = build sooner):

| Dimension | Range | Weight | What it measures |
|---|---:|---:|---|
| Value | 0–3 | ×3 | How many business questions does this mart unblock? Weighted by question priority (P0 = 3, P1 = 2, P2 = 1). Cap at 3. |
| Feasibility | 0–3 | ×2 | How much of the mart's data deps are ✅ or 🟡 in the coverage matrix today? 3 = all live, 2 = mostly live, 1 = mostly blocked, 0 = fully blocked. |
| Strategic anchor | 0–2 | ×2 | Does this mart sit on the locked metric / oracle number / explicit client request? 2 = yes (locked), 1 = adjacent, 0 = neither. |

Tie-break: P0 question count, then rubric pins (none here), then alphabetical mart name.

---

## Ranked roadmap

| Rank | Mart name | Grain | PK | Purpose (1 line) | Business area | Rationale | Data deps | Blockers |
|---:|---|---|---|---|---|---|---|---|
| 1 | `speed_to_lead_detail` (Shipped) | One row per (Calendly booking × outbound touch event); bookings with no SDR touch emit one row with `touched_at = NULL` | `speed_to_lead_touch_id` (surrogate hash of `booking_sk` × `coalesce(touch_sk, 'no-touch')`) | Powers the headline Speed-to-Lead metric and every Page-1 Metabase card | Funnel — Speed-to-Lead (Q1) | Locked headline metric anchor for the engagement; already feeds 15/15 Page-1 cards in production | `fct_speed_to_lead_touch`, `fct_calls_booked`, `fct_outreach`, `dim_contacts`, `dim_users`, `dim_pipeline_stages`, `dim_calendar_dates`, `stg_ghl__opportunities` | `client-decision`: pick trusted GHL conversations copy (legacy blob 1,314 rows vs Phase-2 101 rows). Numerator may shift on rewire; current mart runs on whichever copy `stg_ghl__conversations` resolves to today. |
| 2 | `lead_journey` (Refresh) | One row per GHL contact (booked or not) — widest mart in the project, ~15,598 rows vs oracle 6,113 applicants | `contact_id` (natural key from `dim_contacts`) | Full-funnel "golden lead" view: applicant → booker → paid, with attribution, lost-reason, multi-touch, lead-quality flags | Funnel — Lead → Paid (Q2), Attribution (Q4, Q10, Q11), Funnel — Stage analysis (Q6) | Two P0 questions covered (Q2, Q4) plus three P1/P2; widest dashboard surface; the mart that converts the matrix's three unblocking moves into visible dashboards | `dim_contacts`, `fct_calls_booked` (agg), `fct_outreach` (agg), `fct_revenue` (agg, will pivot to `fct_payments`), `dim_pipeline_stages`, `stg_ghl__opportunities`, `stg_calendly__events`, `stg_typeform__responses`, future `stg_typeform__forms` | `dbt-staging`: `stg_fanbasis__transactions` does not exist; current `fct_revenue` is Stripe-only. `extractor-build` (U9): Typeform `form_id` is NULL on every response. `dbt-staging`: `stg_typeform__forms` does not exist. `client-decision`: trusted GHL copy. |
| 3 | `revenue_detail` (Refresh) | One row per payment event (currently Stripe charge + Fanbasis payment placeholder) — ~1,423 rows vs oracle | `payment_id` | Full revenue surface — Page 3 of the dashboard; the table finance and GTM both query | Funnel — Lead → Paid (Q2), Net Revenue (Q8), Retention (Q9, partial), Multi-touch attribution (Q11) | Single most leverage refresh on the matrix: pivot from Stripe-only to Fanbasis-primary unblocks every "did they pay" question. Stripe is banned; Fanbasis is the live revenue source per `project_stripe_historical_only.md`. | New `stg_fanbasis__transactions`, `dim_contacts`, `dim_offers`, existing `stg_stripe__charges` (downgraded to historical-only), eventual `stg_fanbasis__refunds` for Q8 | `dbt-staging`: build `stg_fanbasis__transactions` (the headline matrix unblocker — biggest single move that flips cells from blocked to working). `dbt-staging` then `dbt-warehouse`: retarget `_fanbasis__sources.yml` from `dee-data-ops` to `project-41542e21-470f-4589-96d`, then introduce `fct_payments` (or rename `fct_revenue`). `vendor-support`: confirm Fanbasis refunds/chargebacks shape (separate rows vs in-place updates) before Q8 can land. |
| 4 | `sales_activity_detail` (Refresh) | One row per Calendly booking event (~3,141 rows vs oracle) | `booking_id` (surrogate from `fct_calls_booked.booking_sk`) | Booked-call grain Speed-to-Lead and SDR / closer / cycle-time / show-rate cuts; preserves Page-1 sibling tiles after the F2/F3 refactor | Funnel — Setter performance (Q3), Conversion — Win rate (Q5) | One P0 (Q3) and one P1 (Q5); the SDR-leaderboard surface; refresh shares the trusted-GHL-copy gate with `speed_to_lead_detail` so the two refresh together | `fct_calls_booked`, `fct_outreach`, `dim_contacts`, `dim_users`, `dim_pipeline_stages`, `dim_calendar_dates`, `stg_ghl__opportunities`, `seeds/ghl_sdr_roster.csv` | `client-decision`: trusted GHL copy (shared with rank 1). `client-access`: roster gaps — Ayaan / Jake roles `unknown`; Moayad (departed) and Halle (active Closer) missing entirely (per `docs/proposals/roster_update_from_oracle.md`). `vendor-support` then `extractor-build` if `stg_ghl__users` should ever land non-empty (Q5 close-rate cuts currently fall back on the seed). |
| 5 | `customer_retention_detail` (New) | One row per Fanbasis customer × cohort month, with churn-event lifecycle columns (signup, first paid, last paid, churn flag, plan changes) | Surrogate key on `(fanbasis_customer_id, cohort_month)` — composite natural key is unsafe because plan changes and reactivation reuse the customer id | Cohort retention and churn analysis tied to acquisition path | Retention (Q9) | Q9 is currently 100% blocked on the matrix. Even after Fanbasis transactions land for Q2/Q8, retention needs a separate customer-/subscription-grain feed that has not been inspected yet. Rank below the four refresh marts because the underlying entity has not been confirmed to exist in raw. | New `stg_fanbasis__customers`, new `stg_fanbasis__subscriptions` (or whatever the entity turns out to be called), `dim_contacts` (for the GHL bridge), `dim_calendar_dates`, optional `stg_stripe__customers` for historical bridge | `vendor-support`: confirm Fanbasis carries customer / subscription / plan entities at all; current preflight only inspected `Raw.fanbasis_transactions_txn_raw`. `extractor-build`: if entities exist but are not landing, add to the Fanbasis extractor. `dbt-staging` and `dbt-warehouse` follow once shape is known. `client-decision`: whether historical Stripe customers must bridge to current Fanbasis customers for blended-retention reporting. |
| 6 | `call_intelligence_detail` (New) | One row per Fathom call with transcript-derived signals (objections, talk tracks, sentiment, classification) | `fathom_call_id` (natural key) | Conversation intelligence — what is happening on sales calls and how it ties to revenue and refunds | Conversation Intelligence (Q12) | Q12 is the only P2 question that requires net-new infrastructure rather than a refresh. Ranked last because Fathom transcripts are missing on 100% of 1,157 calls and the LLM-analysis pipeline (`Raw.fathom_call_intelligence`) has been dead since 2026-04-03. Until at least one of those is repaired, the mart cannot be materialized. | `stg_fathom__calls` (exists, metadata only), new `stg_fathom__transcripts`, `dim_users`, optional `stg_fanbasis__transactions` for revenue-per-call | `vendor-support`: Fathom transcript plan tier + API access — root-cause diagnosis is scoped to U6 post-sprint. `client-decision`: whether to upgrade the Fathom plan if the gap is plan-tier rather than scope. `extractor-build`: extend `fathom-hourly-ingest` to call `/transcripts` (or async transcript-job pattern). Separate question: revive `fathom-llm-analysis` Cloud Run Job, or re-implement classification in dbt once transcripts land. |

---

## Per-mart notes

### 1. `speed_to_lead_detail` (rank 1, score 17)

- **Grain:** one row per (Calendly booking × outbound touch event); bookings with no SDR touch emit one row with `touched_at = NULL`. Per Joshua Kim's _AE — The Order in which I Model Data_ (Step 2), grain is the most load-bearing decision; this mart's grain is fact-source-of-truth for the locked headline metric and is frozen as the BI-layer contract (column order frozen per `_marts__models.yml`).
- **PK:** `speed_to_lead_touch_id`, a surrogate hash of `booking_sk` and `coalesce(touch_sk, 'no-touch')`. Surrogate over natural because the no-touch fallback row has no natural touch identity, and the hash gives uniqueness regardless.
- **Business questions served:** Q1 (How fast do we contact booked leads?). Strategic anchor for the engagement.
- **Downstream dashboards (expected):** Page 1 of the D-DEE Speed-to-Lead dashboard (already wired, 15/15 cards live).
- **Why this rank:** value 3 (Q1 is P0 and unique to this mart), feasibility 2 (Calendly is fresh; the only blocker is the GHL-trusted-copy decision, and the mart already runs in production against the current copy), strategic anchor 2 (locked headline metric — the only mart in the project carrying this score). Total 17. The mart already ships; this rank is mostly about ordering when the trusted-copy rewire actually fires, not about new build work.

### 2. `lead_journey` (rank 2, score 15)

- **Grain:** one row per GHL contact, whether booked or not (~15,598 rows vs oracle 6,113 applicants). Per Kim Step 2, the contact-grain choice was made because the front-of-funnel applicant→booker conversion (~51%) is meaningless at booking grain — every applicant who never booked is a row that has to exist. This mart deliberately carries typed-NULL placeholder columns for upstream that has not yet shipped (psychographics, lead magnets, self-reported source) so the schema stays stable through refresh waves.
- **PK:** `contact_id` (the GHL contact natural key, surfaced as `dim_contacts.contact_id`). Natural over surrogate because the GHL contact id is universally referenced by every downstream system (Calendly, Typeform, Fanbasis bridge) and a surrogate would force every join to round-trip through `dim_contacts`.
- **Business questions served:** Q2 (lead → paid), Q4 (forms and channels — attribution columns), Q6 (pipeline stuck — current pipeline state), Q10 (bad lead sources — DQ flags), Q11 (multi-touch attribution).
- **Downstream dashboards (expected):** Page 2 of the D-DEE dashboard (funnel, attribution, psychographic, applicant→booker conversion, lost-reason, multi-touch). Currently feeds Page 2 with the Stripe-only revenue tail.
- **Why this rank:** value 3 (covers two P0 questions plus three P1/P2; tie-break wins over the rank-3/4 marts on P0 question count), feasibility 1 (multiple blockers — Fanbasis missing, Typeform `form_id` NULL, Typeform forms reference table missing, GHL trusted-copy decision pending), strategic anchor 2 (the widest dashboard surface and the explicit beneficiary of two of the matrix's three unblocking moves). Total 15. Refresh, not new build.

### 3. `revenue_detail` (rank 3, score 15)

- **Grain:** one row per payment event (Stripe charge today; Fanbasis payment after refresh). Unmatched rows stay visible (not quarantined) so the Page-3 unmatched-revenue tile stays honest. Per Kim Step 2, the payment-event grain (rather than transaction-line or invoice grain) was chosen because the analytical question is "where is the money coming from per dollar," and one payment is the unit users intuitively count.
- **PK:** `payment_id` (natural key). Source-namespaced internally (`stripe_charge_<id>` vs `fanbasis_payment_<id>`) so the pivot from Stripe-primary to Fanbasis-primary does not require a PK rename.
- **Business questions served:** Q2 (lead → paid, paid side), Q8 (refunds and chargebacks — once the Fanbasis refunds entity lands), Q9 (retention, partially — payment-grain churn signals; full retention belongs to `customer_retention_detail`), Q11 (multi-touch attribution, paid side).
- **Downstream dashboards (expected):** Page 3 of the D-DEE dashboard (revenue, refunds, attribution by paid customer). Currently powered by Stripe historical data only.
- **Why this rank:** value 3 (Q2 P0 plus three secondaries), feasibility 1 (`stg_fanbasis__transactions` does not exist; the matrix flagged this as the single largest blocker-flip on the page), strategic anchor 2 (Fanbasis pivot is matrix unblocking move #2; revenue is the engagement's most-asked-about surface after Speed-to-Lead). Total 15. Refresh, not new build — the mart already exists; the work is rewiring its grain source from Stripe to Fanbasis.

### 4. `sales_activity_detail` (rank 4, score 15)

- **Grain:** one row per Calendly booking event (~3,141 rows vs oracle). Per Kim Step 2, the booking grain (rather than touch grain) was chosen because the Page-1 SDR / closer / cycle-time / show-rate cuts all aggregate naturally to the booking, and the lower-grain `speed_to_lead_detail` already exists for touch-level detail. Two siblings, two grains, by design (per `mart-naming.md` Rule 2 — fewer wider marts, but a separate mart when grain genuinely differs).
- **PK:** `booking_id`, a surrogate from `fct_calls_booked.booking_sk`. Surrogate because Calendly events can be rescheduled or recreated with the same external id, and the surrogate freezes the analytical row identity.
- **Business questions served:** Q3 (setter performance), Q5 (closer win rate — `close_outcome` and `cycle_time_booking_to_close_min` columns).
- **Downstream dashboards (expected):** Page 1 of the dashboard (sibling to `speed_to_lead_detail`); SDR leaderboard tiles; closer win-rate tiles.
- **Why this rank:** value 3 (Q3 P0 plus Q5 P1; capped from a raw 5), feasibility 2 (Calendly is fresh; `dim_users` falls back to the roster seed when GHL users is empty; the only material blocker is the same trusted-GHL-copy decision that gates rank 1), strategic anchor 1 (adjacent to the locked metric — drives the same Page-1 surface). Total 15. Tie-break loses to `revenue_detail` alphabetically (r < s) given equal P0 question count.

### 5. `customer_retention_detail` (rank 5, score 6) — **net-new**

- **Grain:** one row per Fanbasis customer × cohort month, with churn-event lifecycle columns. Per Kim Step 2, the customer × cohort-month grain (rather than per-event) is necessary because the analytical question — "which acquisition paths or sales patterns lead to better long-term customers?" — requires cohort comparability over time, not raw event counts.
- **PK:** surrogate on `(fanbasis_customer_id, cohort_month)`. Composite natural key is unsafe because reactivation and plan changes can produce multiple lifecycles per customer; surrogate gives unambiguous row identity. Per `data-modeling-process.md` Maxim 2, do not paper over a duplicate-customer grain bug downstream with `QUALIFY ROW_NUMBER`.
- **Business questions served:** Q9 (retention and churn).
- **Downstream dashboards (expected):** new dashboard page; not yet scoped. Likely "Customer cohorts" page added after the rank 1–4 refresh wave finishes.
- **Why this rank:** value 2 (one P1 question), feasibility 0 (Fanbasis customer / subscription entity not yet inspected — could turn out to be missing entirely from the live extractor, which would push feasibility to 0 and require an upstream fix before the mart is even drawable), strategic anchor 0 (no locked metric or oracle number on retention yet). Total 6. The first task here is not the mart — it is a 10-minute BigQuery payload audit (per source-inventory quick win #2) to confirm what Fanbasis is actually sending.

### 6. `call_intelligence_detail` (rank 6, score 3) — **net-new**

- **Grain:** one row per Fathom call with transcript-derived signals. Per Kim Step 2, the call grain (rather than utterance / sentence grain) was chosen because every dashboard cut anchors to a call (closer × call, offer × call, revenue per call). Sentence-grain analysis can live in a downstream warehouse model if and when transcripts arrive.
- **PK:** `fathom_call_id` (natural key, the Fathom recording id).
- **Business questions served:** Q12 (what is happening on sales calls).
- **Downstream dashboards (expected):** new dashboard surface — closer coaching, objection-frequency, talk-track quality. Not on the current Metabase plan; ranked here as the call-out for "this question is still in scope but is gated entirely on vendor / extractor work."
- **Why this rank:** value 1 (one P2 question), feasibility 0 (transcripts are missing on 100% of 1,157 calls; the LLM-analysis pipeline that would have produced derived signals is dead since 2026-04-03), strategic anchor 0. Total 3. Sequenced last because every dependency is upstream of dbt — extractor change, plan-tier decision, or both.

---

## Build sequence

Marts grouped by readiness so Phase B builds in dependency order, not score order.

### Tier A — production-ready or ready-to-build today

Marts with no 🔴 dependency on the coverage matrix. Build (or refresh trivially) first.

- `speed_to_lead_detail` — already shipped. The "build" is a re-anchor against whichever GHL conversations copy David picks. Until the trusted-copy decision is made, leave the production mart on its current source — it powers 15/15 Page-1 cards live.
- `sales_activity_detail` — already shipped. Same trusted-GHL-copy gate as rank 1; refresh together. Roster gaps (Ayaan, Jake, Moayad, Halle) are the only other open thread and they are owned by David, not blocked on a vendor.

### Tier B — gated on one named blocker (we own the fix)

Marts whose deps include a single 🔴 cell that is `dbt-staging` or `dbt-warehouse` work — i.e. ours to write, not waiting on a vendor or the client.

- `revenue_detail` — blocker: `stg_fanbasis__transactions` does not exist. Path: (1) audit `Raw.fanbasis_transactions_txn_raw` payload shape (10-min BQ query — quick win #2 from source inventory); (2) retarget `_fanbasis__sources.yml` from `dee-data-ops` to `project-41542e21-470f-4589-96d`; (3) scaffold `stg_fanbasis__transactions` via the `staging-scaffold` skill; (4) add or rename `fct_payments` at warehouse via `warehouse-fct-scaffold`; (5) refresh the mart's grain source from `fct_revenue` (Stripe) to `fct_payments` (Fanbasis primary, Stripe historical). Q8 (refunds) waits on a separate `vendor-support` thread (Fanbasis refunds entity shape) and is a follow-on, not part of this Tier B unit.
- `lead_journey` — blockers: `stg_fanbasis__transactions` (shared with rank 3 — solving once unblocks both), `stg_typeform__forms` (does not exist; `staging-scaffold` for the `forms` entity_type already in raw), Typeform `form_id` (U9 work; placeholder column stays NULL until then, mart still compiles), trusted GHL copy (shared with Tier A). Practical sequence: refresh after rank 3 lands so the Fanbasis dependency only has to be wired once.

### Tier C — gated on multiple blockers including vendor-support

Marts whose deps include a 🔴 cell that requires vendor support, an extractor change, or a client decision we cannot make on dbt's behalf.

- `customer_retention_detail` — blocker: Fanbasis customer / subscription entity not yet inspected; may not exist in the live extractor at all. First action is the 10-min Fanbasis payload audit (same inspection feeding Tier B), not mart work. Defer until after the audit confirms entity presence.
- `call_intelligence_detail` — blockers: Fathom transcript plan-tier / API-scope question (vendor-support, scoped to U6 post-sprint), and the dead `fathom-llm-analysis` Cloud Run Job (separate architectural decision). Defer until U6 produces a transcript-coverage diagnosis.

---

## What this means for Phase B

Phase A (Discovery Sprint, docs-only) ends when this roadmap is approved. Phase B (Layer Build) starts with the Tier A and Tier B work above.

Sequenced first invocations of the build-phase skills:

1. **`staging-scaffold`** for `stg_fanbasis__transactions` — the highest-leverage single move on the matrix, prerequisite for the rank-3 refresh and a co-prerequisite for the rank-2 refresh.
2. **`warehouse-fct-scaffold`** for `fct_payments` (or rename `fct_revenue` to keep historical lineage intact) — declares the grain explicitly per the data-modeling-process LAW, then physicalises against the new staging.
3. **`mart-collapse`** to refresh `revenue_detail` against `fct_payments`. Stripe stays available as historical; Fanbasis becomes the primary grain source.
4. **`staging-scaffold`** for `stg_typeform__forms`, then refresh `lead_journey` to surface form-level attribution (the `form_id` value stays NULL on responses until U9; the forms reference table can ship today).
5. The trusted-GHL-copy decision then triggers refresh of `speed_to_lead_detail` and `sales_activity_detail` together.

Not invoked from this roadmap: Tier C marts wait on out-of-sprint work (Fanbasis vendor support, Fathom transcript repair). They re-enter the build queue when their blockers move.

Per `.claude/rules/use-data-engineer-agent.md`, all the above skills should be invoked through the `data-engineer` subagent so the LAW pattern fires (Altimate `sql-review` on every staging / warehouse / mart write; baseline schema-test append).

---

## What this roadmap intentionally does not include

- **No mart for Q13 (Slack governance).** Slack is not a data source. This is documented closure, not a deferred mart.
- **No standalone dim or warehouse-fct entries.** The four existing marts plus the two net-new ones above are already the right "fewer wider marts" surface per `mart-naming.md` Rule 2; the warehouse-layer fcts and dims they consume show up in the data-deps column rather than as their own ranked rows.
- **No re-listing of the eleven retired `stl_*` rollups** (collapsed into `speed_to_lead_detail` in the F1/F2/F3 refactor). The wide-mart pattern is the design; do not re-introduce per-dashboard narrow tables (`mart-naming.md` Rule 2, Kim Step 4).
- **No new Stripe mart.** Stripe is banned (per `project_stripe_historical_only.md`); historical data folds into `revenue_detail` as the secondary source after the Fanbasis pivot. A blended `revenue_history` mart was considered and rejected — `revenue_detail` already does the blend at row level via source-namespaced PKs.

---

## Source citations

- Joshua Kim, _AE — The Order in which I Model Data_ — Medium, April 2026 (Step 2: grain is the most load-bearing decision; Step 4: silver-layer scalability; Maxim 2: `QUALIFY ROW_NUMBER` is a symptom).
- "How to Create a Data Modeling Pipeline (3 Layer Approach)" — Data Ops notebook (3-layer pattern; "fewer wider marts").
- "3 Data Modeling Mistakes That Can Derail a Team" — Data Ops notebook (parity-gated dual-source deprecation pattern; basis for the F1/F2/F3 refactor and the refresh-on-unblocker sequencing here).
