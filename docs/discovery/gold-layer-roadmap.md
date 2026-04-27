# Gold-Layer Roadmap — Data Discovery Sprint

**Sprint artifact 4 (final).** This is the ordered list of dashboard-ready marts to build next. It ends Phase 1 (Discovery) and authorizes Phase 2 (Layer Build).

Last updated: 2026-04-27.

This page is fed by — and must be re-run when materially changed:

- `docs/discovery/source-inventory.md` — what data we have and whether it is fresh.
- `docs/discovery/business-area-map.md` — the 13 business questions plus named-owner column (currently `client-decision` pending Week-0 confirmation).
- `docs/discovery/coverage-matrix.md` — the question × source readiness grid plus the **mart architecture commitment**: one wide mart per playbook chapter, grain-split only when a genuinely different grain emerges.

---

## Scoring rubric (default, baked into `mart-roadmap-rank`)

Each candidate mart scored on three dimensions, summed for total rank order (higher = build sooner):

| Dimension | Range | Weight | What it measures |
|---|---:|---:|---|
| **Value** | 0–3 | ×3 | How many business questions does this mart unblock, weighted by priority (P0 = 3, P1 = 2, P2 = 1). Cap at 3. |
| **Feasibility** | 0–3 | ×2 | How much of the mart's data deps are ✅ or 🟡 in the coverage matrix today. 3 = all live, 2 = mostly live, 1 = mostly blocked, 0 = fully blocked. |
| **Strategic anchor** | 0–2 | ×2 | Does this mart sit on the locked metric / explicit client request. 2 = locked, 1 = adjacent, 0 = neither. |

Tie-break: P0 question count, then alphabetical mart name. No `--rubric` overrides applied for this run.

**Owner column:** the area map's `Owner` column resolves each business question to one canonical role at D-DEE / Precision Scaling (SDR Manager, Sales Manager, Marketing Lead, Finance Lead, Sales Operations, D-DEE Leadership, David / Operations). The roles are inferred from the playbook chapter and matter for two downstream decisions: (a) which audience each Phase-2 dashboard is routed to, and (b) the eventual schema-per-audience split called out in `.claude/rules/mart-naming.md` Rule 5 (do not split prematurely — single `marts` schema today).

---

## Ranked roadmap

| Rank | Mart name | Grain | PK | Purpose | Business area | Rationale | Data deps | Blockers |
|---:|---|---|---|---|---|---|---|---|
| 1 | `speed_to_lead_detail` | one row per Calendly booking × SDR touch event | `(booking_id, touch_id)` surrogate | Time-to-first-touch on every booked call, sliced by SDR / pipeline / lead source. | Funnel — Speed-to-Lead | Locked metric; already shipped (Track F refactor 2026-04-22). Extend in place rather than rebuild. | `stg_calendly__events`, `stg_ghl__conversations`, `stg_ghl__contacts`, `stg_ghl__pipelines`, `ghl_sdr_roster.csv` (seed), `dim_pipeline_stages` | GHL conversations 92% undercount (Phase-2 vs legacy); GHL trusted-copy decision still owed. |
| 2 | `funnel_booking_detail` | one row per Calendly booking | `booking_id` | Booking-grain wide table for setter performance, no-show / show rate, and stage-stuck diagnosis. Sister mart to `speed_to_lead_detail` (different grain). | Funnel — Setter / Show-rate / Stage analysis | Highest-value new mart; deps mostly 🟡 today. Q3 (setter) is the cleanest single-blocker entry point (gated only on GHL trusted-copy decision). | `stg_calendly__events`, `stg_calendly__event_invitees`, `stg_ghl__opportunities`, `stg_ghl__contacts`, `dim_pipeline_stages`, `ghl_sdr_roster.csv` | GHL trusted-copy decision (legacy blob vs Phase-2); Calendly cancellation / no-show status fields not yet in staging; GHL stage-history adequacy unverified. |
| 3 | `attribution_detail` | one row per contact (with payment outcome columns) | `contact_id` | Lead-to-paid wide table covering form attribution, multi-touch, lead quality, and revenue follow-through per contact. | Attribution | Highest absolute value (4 questions span P0→P2) but feasibility is the matrix's worst. Fanbasis staging is the precondition for every "did they pay" cell. | `stg_fanbasis__transactions` *(does not exist yet)*, `stg_typeform__responses`, `stg_typeform__forms` *(does not exist yet)*, `stg_ghl__contacts`, `stg_ghl__opportunities`, `stg_calendly__events` | Fanbasis has zero staging models today (`_fanbasis__sources.yml` is a placeholder + still points at deprecated pre-U2 project); Typeform `form_id` missing on responses; GHL stale. |
| 4 | `revenue_event_detail` | one row per Fanbasis payment event (charge / refund / chargeback) | `transaction_id` (Fanbasis natural key) | Net-revenue surface — gross paid, refunds, chargebacks, attributable to closer / offer / cohort. | Net Revenue | Strategic anchor: Fanbasis is the live revenue source per memory `project_stripe_historical_only.md`. Blocked until raw Fanbasis is modeled in dbt. | `stg_fanbasis__transactions` *(does not exist yet)*, `stg_fanbasis__refunds` *(unverified)*, `stg_ghl__opportunities`, `stg_ghl__contacts` | Fanbasis staging absent; Fanbasis refund / chargeback shape unverified — Week-0 ask owed by D-DEE. |
| 5 | `closer_call_outcome` | one row per Fathom call × outcome | `call_id` | Closer-grain wide table — call → did they close → did they pay. Powers closer leaderboard, win-rate by offer, and call-content cross-cuts (when transcripts land). | Conversion — Win rate | Multiple blockers across GHL users, Fanbasis staging, and (eventually) Fathom transcripts. Worth ranking but not buildable mid-sprint. | `stg_fathom__calls`, `stg_ghl__opportunities`, `stg_ghl__users` *(0 rows upstream)*, `stg_fanbasis__transactions` *(does not exist yet)* | GHL `users` 0 rows upstream; Fanbasis staging absent; Fathom transcripts 0% coverage (only matters if call-content is in scope for v1). |
| 6 | `customer_lifecycle_detail` | one row per Fanbasis customer | `customer_id` | Retention / churn — cohort, MRR, churn date, acquisition path. | Retention | Doubly blocked: Fanbasis customer/subscription entity shape is unverified, and Stripe → Fanbasis customer bridge is undecided for historical retention. | `stg_fanbasis__customers` *(unverified)*, `stg_fanbasis__subscriptions` *(unverified)*, `stg_ghl__contacts`, `stg_stripe__customers` *(historical bridge, optional)* | Fanbasis customer / subscription entity shape unverified; historical-bridge decision pending; Stripe banned (historical-only). |
| 7 | `call_intelligence_detail` | one row per Fathom call | `call_id` | Conversation intelligence — transcript, sentiment, classification, objection tagging. | Conversation Intelligence | P2 priority and fully blocked. Roadmap entry holds a placeholder so the chapter is not lost. | `stg_fathom__calls`, `stg_fathom__transcripts` *(does not exist; transcripts not landing)*, `stg_fathom__intelligence` *(0 rows; LLM analysis pipeline dead since 2026-04-03)* | Fathom transcripts 0% coverage; Fathom LLM analysis pipeline dead 3+ weeks. Both upstream of dbt. |

---

## Per-mart notes

### 1. `speed_to_lead_detail` (already shipped)

- **Primary audience:** SDR Manager.
- **Grain:** one row per Calendly booking × SDR touch event. Lowest-grain rationale: every Speed-to-Lead question (median latency, p95 latency, % under-5-min) is a different aggregation of the same underlying touch sequence — pre-aggregating to booking-grain would lose the touch-level cuts. (source: "AE — The Order in which I Model Data" Step 2, Joshua Kim, Data Ops notebook; lowest-grain commitment in `.claude/rules/warehouse.md`.)
- **PK:** `(booking_id, touch_id)` surrogate. Composite because a booking can have many touches; touch-level uniqueness is required for parity-test stability.
- **Questions served:** Q1 (locked metric).
- **Downstream dashboards:** v1 Speed-to-Lead dashboard (Metabase, retiring) → BI surface in Phase 2.
- **Action:** **extend in place.** Add columns for setter performance (Q3) and show / no-show status (when the matching Calendly fields land). Q6 (stage stuck) does not extend this mart — different grain — see Mart 2.
- **Reference:** Track F refactor (2026-04-22) consolidated 11 narrow rollups into this single wide mart; the parity test `stl_headline_parity` stayed green across the deprecation window. Pattern documented in `.claude/rules/mart-naming.md` "Lessons Learned."

### 2. `funnel_booking_detail` (Tier B — gated on one named blocker)

- **Primary audience:** SDR Manager (Q3, Q7); Sales Operations as secondary consumer for Q6's stage-stuck slice.
- **Grain:** one row per Calendly booking. Distinct from Mart 1 (which is touch-grain). Justified-by-grain split per the architecture commitment in `coverage-matrix.md`.
- **PK:** `booking_id` (Calendly natural key — `event_uuid`).
- **Questions served:** Q3 (setter performance), Q6 (stage stuck — joined to opportunity-stage timestamps), Q7 (no-show rescue).
- **Downstream dashboards:** SDR leaderboard, no-show rescue dashboard, pipeline-velocity dashboard.
- **Single named blocker (per the matrix):** GHL trusted-copy decision. The mart's setter-performance columns hinge on `stg_ghl__conversations`; the legacy blob has 1,314 rows, Phase-2 has 101, and we cannot pick one without a `client-decision`. Resolving this single decision unlocks the build.
- **Optional gates (do not block first cut):** Calendly cancellation / no-show status fields not yet landed in staging (Q7 partial coverage), and GHL stage-history adequacy unverified (Q6 partial coverage). The first cut can ship Q3-only, then extend.

### 3. `attribution_detail` (Tier C — Fanbasis-blocked)

- **Primary audience:** Marketing Lead (Q4, Q10, Q11); D-DEE Leadership for Q2's revenue-attribution cut.
- **Grain:** one row per contact, with payment outcome and source-attribution columns. Contact-grain rather than touchpoint-grain because the questions ("which forms / channels / ads make money") aggregate touchpoints up to the contact and ask about the paid outcome.
- **PK:** `contact_id` (GHL natural key, since GHL is the identity backbone per the source inventory).
- **Questions served:** Q2 (lead → paid), Q4 (forms / channels), Q10 (lead quality), Q11 (multi-touch).
- **Downstream dashboards:** marketing attribution dashboard, lead-source quality scorecard, multi-touch pathing report.
- **Blockers:** (a) Fanbasis has zero dbt staging today — `_fanbasis__sources.yml` is a placeholder still pointing at the deprecated pre-U2 project; (b) Typeform responses do not carry `form_id` reliably (lifted in U3, restore in U9); (c) GHL is stale until the trusted-copy decision lands. Three named blockers — Tier C until at least the Fanbasis blocker resolves.
- **Highest-leverage unlock (per the source inventory's quick wins #1):** retarget `_fanbasis__sources.yml` to `project-41542e21-470f-4589-96d` and add `stg_fanbasis__transactions`. Two hours of dbt work flips the Q2 column from 🔴 to 🟡 across the entire Attribution chapter.

### 4. `revenue_event_detail` (Tier C — Fanbasis-blocked)

- **Primary audience:** Finance Lead.
- **Grain:** one row per Fanbasis payment event (`charge`, `refund`, `chargeback`). Event-grain rather than transaction-grain because refunds and chargebacks need to net against the original charge in BI without a fan-out join.
- **PK:** `transaction_id` (Fanbasis natural key, scoped by event_type).
- **Questions served:** Q8 (refunds / chargebacks).
- **Downstream dashboards:** net-revenue dashboard, refund-by-offer / refund-by-closer report.
- **Blockers:** Fanbasis transactions staging absent + Fanbasis refund / chargeback entity shape unverified (Week-0 ask). Both block independently.
- **Strategic anchor:** Fanbasis is D-DEE's only live revenue source per memory `project_stripe_historical_only.md` (Stripe is banned, historical-only). This mart is the precondition for any "current revenue" claim on a dashboard.

### 5. `closer_call_outcome` (Tier C — multi-blocker)

- **Primary audience:** Sales Manager.
- **Grain:** one row per Fathom call × outcome. Outcome dimension covers (booked → showed → closed → paid). Call-grain rather than opportunity-grain because the closer-coaching question ("which closer wins which calls") is anchored to the call event.
- **PK:** `call_id` (Fathom natural key).
- **Questions served:** Q5 (closer win rate). Q12 lives in a separate mart (different value frame — call content vs. call outcome).
- **Downstream dashboards:** closer leaderboard, win-rate-by-offer dashboard.
- **Blockers:** GHL `users` 0 rows upstream (closer identity); Fanbasis staging absent (paid outcome); Fathom transcripts 0% (only blocks the content cross-cut, not the win-rate columns). Multi-blocker by definition.

### 6. `customer_lifecycle_detail` (Tier C — deeply blocked)

- **Primary audience:** D-DEE Leadership.
- **Grain:** one row per Fanbasis customer.
- **PK:** `customer_id` (Fanbasis natural key, once verified).
- **Questions served:** Q9 (churn / retention).
- **Downstream dashboards:** retention curve dashboard, cohort MRR dashboard.
- **Blockers:** Fanbasis customer / subscription entity shape entirely unverified — we don't yet know whether Fanbasis exposes recurring-customer data at all. Plus Stripe → Fanbasis customer bridge is a separate `client-decision` if historical retention must blend.
- **Discovery follow-up:** during the Fanbasis API audit (a quick win in the source inventory), explicitly probe whether `customer` and `subscription` entities are accessible. If they aren't, this mart's grain is unclear and it should drop off the roadmap until an extractor change lands.

### 7. `call_intelligence_detail` (Tier C — deeply blocked)

- **Primary audience:** Sales Manager (with a coaching-lead secondary consumer once transcripts land).
- **Grain:** one row per Fathom call.
- **PK:** `call_id` (Fathom natural key).
- **Questions served:** Q12 (call-content analysis — objections, talk tracks, sentiment).
- **Downstream dashboards:** call-content scorecard, objection-frequency dashboard, coaching-flag report.
- **Blockers:** Fathom transcripts 0% coverage across all 1,157 calls; Fathom LLM analysis pipeline dead since 2026-04-03. Both blockers are upstream of dbt — a U6 extractor concern.
- **Roadmap status:** placeholder so the Conversation Intelligence chapter is not lost. Do not promote without an explicit ranking decision against the unblocked marts above.

---

## Build sequence (3-tier)

### Tier A — ready to build today (blockers list empty)

- **`speed_to_lead_detail`** — already shipped. Action: **extend in place**, not rebuild. Add Q3 setter-performance columns once the GHL trusted-copy decision lands; add Q7 show/no-show columns once Calendly status fields are staged.

### Tier B — gated on one named blocker

- **`funnel_booking_detail`** — single blocker is the GHL trusted-copy decision (`client-decision`). First cut can ship Q3-only the day that decision lands. Q6 and Q7 extend the same mart in subsequent passes.

### Tier C — gated on multiple blockers

- **`attribution_detail`** — Fanbasis staging + Typeform `form_id` + GHL trusted-copy. The Fanbasis staging unblock is the highest-leverage move on the entire matrix per the source inventory's quick wins.
- **`revenue_event_detail`** — Fanbasis staging + Fanbasis refund / chargeback shape verification.
- **`closer_call_outcome`** — GHL users + Fanbasis staging + (optionally) Fathom transcripts.
- **`customer_lifecycle_detail`** — Fanbasis customer / subscription verification + historical-bridge decision.
- **`call_intelligence_detail`** — Fathom transcript landing + LLM analysis pipeline revival. Both upstream of dbt.

---

## What this means for Phase 2

1. **Re-run this rank** when (a) the GHL trusted-copy decision lands, (b) any 🔴 cell in the matrix flips to 🟡, or (c) the playbook chapters expand (e.g., a new business question from D-DEE that does not fit any existing chapter). Owner roles are inferred from the chapter, so a new chapter introduces a new owner row implicitly.

2. **First Phase-2 work order** (no new mart needed yet): extend `speed_to_lead_detail` with the columns Q3 / Q6 / Q7 will eventually consume. Pure additive change; existing parity test guards the Q1 metric.

3. **First new mart to build** when the GHL trusted-copy decision lands: `funnel_booking_detail`. Invocation order:
   - `warehouse-fct-scaffold` for the underlying booking-grain fact (e.g. `fct_bookings`).
   - `mart-collapse` to produce the wide mart from the fact + dim_contacts + dim_pipeline_stages + ghl_sdr_roster seed.

4. **Highest-leverage staging unlock** (independent of any mart decision): retarget `_fanbasis__sources.yml` to `project-41542e21-470f-4589-96d` and add `stg_fanbasis__transactions`. This single change moves three Tier-C marts (`attribution_detail`, `revenue_event_detail`, `closer_call_outcome`) closer to buildable. Invocation: `staging-scaffold` skill against `Raw.fanbasis_transactions_txn_raw`.

5. **Do not build** `customer_lifecycle_detail` or `call_intelligence_detail` until their respective discovery follow-ups (Fanbasis customer/subscription audit; Fathom transcript root-cause) close. They stay on the roadmap as named placeholders so the chapters are not forgotten.

---

## Rank summary (one-line cheat sheet)

```
1. speed_to_lead_detail        Funnel              SHIPPED — extend in place
2. funnel_booking_detail       Funnel              Tier B (GHL trusted-copy)
3. attribution_detail          Attribution         Tier C (Fanbasis + form_id + GHL)
4. revenue_event_detail        Net Revenue         Tier C (Fanbasis × 2)
5. closer_call_outcome         Conversion          Tier C (GHL users + Fanbasis)
6. customer_lifecycle_detail   Retention           Tier C (Fanbasis customer)
7. call_intelligence_detail    Conv. Intelligence  Tier C (Fathom transcripts)
```
