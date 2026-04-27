# Coverage Matrix - Data Discovery Sprint

**Purpose:** This page answers the question: "For each business question on the map, which data sources can answer it today, which sources we still need, and what is missing?"

Last updated: 2026-04-25.

This page connects to:

- `docs/discovery/business-area-map.md` - the 13 business questions used as the rows below
- `docs/discovery/source-shapes.md` - the field-level builder spec behind every "Needed" cell on this page
- `docs/discovery/source-inventory.md` - what data we have and whether it is fresh
- `docs/discovery/staging-models.md` - which raw data has been cleaned up enough for dbt to use
- `docs/discovery/gap-analysis.md` - what is missing or broken

---

## How to read this page

This is not a dashboard plan. It is the bridge between the Business Question Map and the next dashboard build list. For each business question, this page shows what each confirmed data source contributes.

Each cell uses three lines:

- **Today** - what we can actually answer with this source right now.
- **Needed** - what this source still has to deliver before the question is fully answerable.
- **Missing** - the specific data gap or freshness problem that is blocking us.

Sources that are not relevant to a given question say `not used`. We do not pad those cells.

Each question also carries a **blocker class** line under its headline status. The tag names what kind of action gets the question unblocked - whether the work is in your hands today or waiting on someone else. The full taxonomy lives in `source-shapes.md`; the short version:

- `client-access` waits on D-DEE for credentials or settings.
- `vendor-support` waits on the data vendor (docs, plan tier, support fix).
- `extractor-build` is yours to build in `1-raw-landing/`.
- `dbt-staging` is yours to build in `2-dbt/models/staging/`.
- `dbt-warehouse` is yours to build in `2-dbt/models/warehouse/`.
- `client-decision` waits on D-DEE to choose between options.

### Confirmed data sources used as columns

- **Calendly** - booking events. The system of record for "a lead booked a call."
- **GHL** - GoHighLevel CRM. Contacts, conversations, opportunities, pipeline stages, users (SDRs, closers).
- **Fanbasis** - the live payment processor. The only current source of paid-customer truth.
- **Typeform** - form responses. Where a lead enters the funnel from marketing.
- **Fathom** - call recording and transcripts. Sales call analysis.
- **Stripe** - historical-only payments. Stripe is banned at D-DEE; useful for old history only.

Slack is intentionally excluded from this matrix. Current evidence says Slack is an alert destination, not a data source. We will revisit only if David confirms Slack data should be reported on.

### Playbook chapter tags

Each question is also tagged with the analytics playbook chapter it belongs to. This makes it easier to crib templates from the published canon (RevOps, Gong, HubSpot, Amplitude) when the build list is written:

- **Funnel** - Speed-to-Lead, lead → paid, where leads stuck (Q1, Q2, Q3, Q6)
- **Attribution** - which form, channel, content, or ad created the customer (Q4, Q10, Q11)
- **Conversion** - closer win rate, no-show rescue (Q5, Q7)
- **Net Revenue** - refunds, chargebacks (Q8)
- **Retention** - customers stick around vs churn (Q9)
- **Conversation Intelligence** - what is happening on calls (Q12)

### Status shorthand used in the headline grid

- ✅ **Working** - the question can be answered with this source today.
- 🟡 **Partly** - the source contributes but is stale, partial, or undercounted.
- 🔴 **Blocked** - the source is needed but not yet usable.
- ⚪ **Not used** - this source is not part of answering this question.

---

## Mart architecture commitment

Per `.claude/rules/mart-naming.md` Rule 2 ("Fewer, wider marts over many narrow ones"), the gold-layer roadmap that consumes this matrix will build **one wide mart per playbook chapter**, not one mart per business question.

This locks the unit-of-analysis the rank skill (`mart-roadmap-rank`) operates on. The six chapters tagged in the section above — Funnel, Attribution, Conversion, Net Revenue, Retention, Conversation Intelligence — each map to **one** wide mart. Multiple business questions feed the same mart via slice-and-dice in the BI tool. Concretely:

- **Funnel** → one mart, currently shipped as `speed_to_lead_detail` at booking-touch grain (Q1). Q3 (setter performance), Q6 (stage analysis), and Q7 (no-show / show rate, also tagged Conversion) extend the same mart family.
- **Attribution** → one mart spanning Q4 (forms), Q10 (lead quality), Q11 (multi-touch). All three collapse into one wide attribution table; they don't get separate marts.
- **Conversion** → one mart at the call-outcome grain spanning Q5 (closer win rate) and Q7's call-side coverage.
- **Net Revenue** → one mart spanning Q8 (refunds / chargebacks), gated on Fanbasis cleanup.
- **Retention** → one mart spanning Q9, gated on Fanbasis customer/subscription shape.
- **Conversation Intelligence** → one mart spanning Q12, gated on Fathom transcripts.

The exception is **grain**. A second mart in the same chapter is justified only when a genuinely different grain emerges (booking-grain vs. payment-transaction grain, e.g., the Funnel chapter could later split into a booking-grain mart and a stage-transition-grain mart if Q6 demands it). Default: **one mart per chapter, evaluate grain split only when the mart is being built.**

Q2 (lead → paid) deliberately straddles Funnel and Attribution. The roadmap will resolve which mart owns Q2; the leading candidate is Attribution, since "did this lead pay" is the canonical attribution outcome.

> "I typically don't like to create Marts tables one to one for each report... I think that can get a little messy and create a lot of fluff in your data that becomes outdated."
> — *"How to Create a Data Modeling Pipeline (3 Layer Approach)"*, Data Ops notebook (cited in `mart-naming.md` Rule 2)

---

## Coverage at a glance

| # | Business question | Playbook chapter | Calendly | GHL | Fanbasis | Typeform | Fathom | Stripe |
|---|---|---|---|---|---|---|---|---|
| Q1 | How fast do we contact booked leads? | Funnel - Speed-to-Lead | ✅ | 🟡 | ⚪ | ⚪ | ⚪ | ⚪ |
| Q2 | Which leads turn into paid customers? | Funnel - Lead → Paid | 🟡 | 🟡 | 🔴 | 🟡 | ⚪ | 🟡 |
| Q3 | Which setters are creating bookings, and where are handoffs breaking? | Funnel - Setter performance | 🟡 | 🟡 | ⚪ | ⚪ | ⚪ | ⚪ |
| Q4 | Which forms and channels bring good leads? | Attribution | 🟡 | 🟡 | 🔴 | 🟡 | ⚪ | 🟡 |
| Q5 | Which closers turn calls into customers? | Conversion - Win rate | 🟡 | 🟡 | 🔴 | ⚪ | 🟡 | ⚪ |
| Q6 | Where do leads get stuck in the sales process? | Funnel - Stage analysis | 🟡 | 🟡 | ⚪ | ⚪ | ⚪ | ⚪ |
| Q7 | Which booked calls no-show, and what rescue follow-up works? | Conversion - Show rate | 🟡 | 🟡 | ⚪ | ⚪ | ⚪ | ⚪ |
| Q8 | Where are refunds and chargebacks coming from? | Net Revenue | ⚪ | 🟡 | 🔴 | ⚪ | ⚪ | 🟡 |
| Q9 | Which customers stick around, and which ones churn? | Retention | ⚪ | 🟡 | 🔴 | ⚪ | ⚪ | 🟡 |
| Q10 | Which lead sources create bad or unusable leads? | Attribution - Lead quality | 🟡 | 🟡 | ⚪ | 🟡 | ⚪ | ⚪ |
| Q11 | Which content, ads, or funnels actually make money? | Attribution - Multi-touch | 🟡 | 🟡 | 🔴 | 🟡 | ⚪ | 🟡 |
| Q12 | What is happening on sales calls? | Conversation Intelligence | ⚪ | 🟡 | ⚪ | ⚪ | 🔴 | ⚪ |
| Q13 | Should Slack be used as data, or only for alerts? | (governance question) | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ | ⚪ |

Read the grid as: "for this row's question, here is what each source is contributing right now." Detail for every coloured cell is in the per-question sections below.

---

## Per-question coverage

### Q1. How fast do we contact booked leads?

- **Playbook chapter:** Funnel - Speed-to-Lead
- **Priority:** Start here (P0)
- **Headline status:** Partly working
- **Blocker class:** `client-decision` (which GHL copy is trusted) → `dbt-staging`
- **Decision it unlocks:** Which setter, pipeline, or lead source needs attention because leads are not getting contacted fast enough.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Booking events with timestamps drive the denominator (lead booked a call). Calendly is fresh. | Nothing additional. The grain is correct. | Nothing material. |
| **GHL** | First outbound human SDR touch (CALL or SMS) on the contact, via `lastManualMessageDate`. SDR identity comes from `ghl_sdr_roster.csv` seed. | Restored freshness on the GHL conversations table; pick the trusted GHL copy. | Many rows are missing in the current GHL conversations table compared with the older copy. |
| **Fanbasis** | Not used. | Not used. | Not used. |
| **Typeform** | Not used. | Not used. | Not used. |
| **Fathom** | Not used. | Not used. | Not used. |
| **Stripe** | Not used. | Not used. | Not used. |

---

### Q2. Which leads turn into paid customers?

- **Playbook chapter:** Funnel - Lead → Paid
- **Priority:** Start here (P0)
- **Headline status:** Blocked
- **Blocker class:** `vendor-support` (Fanbasis API docs) → `extractor-build` → `dbt-staging`; plus `client-decision` for GHL trusted copy
- **Decision it unlocks:** Which campaigns, forms, offers, and closers are creating real revenue.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Booking event marks the funnel midpoint (lead → booked → paid). | A reliable join key from booking back to the GHL contact, then forward to the Fanbasis customer. | Nothing on the Calendly side itself. |
| **GHL** | Contact and opportunity stage progression. | Freshness restored; trusted GHL copy chosen; opportunity-status history. | Stale data; pipeline-stage transition history not yet confirmed adequate. |
| **Fanbasis** | Transactions are landing in raw, but there is no cleaned-up Fanbasis transactions table for dbt to use. | A staging model for Fanbasis transactions, then a customer-level join key to GHL. | Cleaned-up Fanbasis transactions table - this is the v2 revenue blocker. |
| **Typeform** | Response counts. | Reliable form ID on every response so we can attribute revenue to the form. | Form ID is missing on the cleaned-up response table. |
| **Fathom** | Not used. | Not used. | Not used. |
| **Stripe** | Old transactions only - useful for historical revenue lookback only. Stripe is banned at D-DEE. | A bridge from Stripe customer IDs to current Fanbasis customer IDs if historical revenue must blend with current. | Stripe is banned; data is historical-only. |

---

### Q3. Which setters are creating bookings, and where are handoffs breaking?

- **Playbook chapter:** Funnel - Setter performance
- **Priority:** Start here (P0)
- **Headline status:** Partly working to blocked
- **Blocker class:** `client-decision` (GHL trusted copy) → `vendor-support` (GHL users / tasks / messages return rows) → `extractor-build`
- **Decision it unlocks:** Where setter coaching, staffing, or follow-up rules should change.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Booking events with invitee details. Calendly is fresh. | Nothing additional. | Nothing material. |
| **GHL** | Contacts and opportunities exist; SDR identity comes from the repo seed. | Trusted GHL copy chosen; users, tasks, and messages tables filled in for handoff analysis. | GHL conversations are undercounted; users, tasks, and messages tables are empty in source. Some setter and closer roles still missing from the seed (Ayaan, Jake roles unknown; Moayad and Halle missing entirely). |
| **Fanbasis** | Not used. | Not used. | Not used. |
| **Typeform** | Not used. | Not used. | Not used. |
| **Fathom** | Not used. | Not used. | Not used. |
| **Stripe** | Not used. | Not used. | Not used. |

---

### Q4. Which forms and channels bring good leads?

- **Playbook chapter:** Attribution
- **Priority:** Start here (P0)
- **Headline status:** Partly working
- **Blocker class:** `extractor-build` (Typeform forms reference + form_id passthrough) → `dbt-staging`; plus `client-decision` (GHL trusted copy) and `vendor-support` (Fanbasis docs for the "good = paid" definition)
- **Decision it unlocks:** Which lead sources should get more spend, more attention, or less attention.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Booking event tied to a contact - the activation step in the funnel. | Source attribution carried through to the booking. | Calendly does not carry the original form ID by default; depends on Typeform → GHL → Calendly pass-through. |
| **GHL** | Contacts and opportunities by lead source field (where populated). | Freshness restored; trusted GHL copy chosen; consistent UTM and source field hygiene. | Stale data; lead-source field hygiene not yet audited. |
| **Fanbasis** | Not used directly, but blocks the "good = paid" definition. | Cleaned-up Fanbasis transactions to define "good leads" as those that paid. | Same Fanbasis blocker as Q2. |
| **Typeform** | Response counts and form titles. | Reliable form ID per response; mapping from Typeform form to GHL contact. | Form ID is missing on the cleaned-up table; no Typeform forms reference table yet. |
| **Fathom** | Not used. | Not used. | Not used. |
| **Stripe** | Old paid-customer history available for backwards attribution lookback only. | Same Stripe → Fanbasis bridge as Q2 if historical paid customers must be attributed. | Stripe is banned; historical-only. |

---

### Q5. Which closers turn calls into customers?

- **Playbook chapter:** Conversion - Win rate
- **Priority:** Next (P1)
- **Headline status:** Partly working
- **Blocker class:** `vendor-support` (Fanbasis docs, Fathom transcript plan tier, GHL users endpoint) → `client-decision` (Fathom plan upgrade if needed) → `extractor-build`
- **Decision it unlocks:** Which closers need coaching, which offers are converting, and where sales calls are breaking down.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Call event (the moment of the closing call). | Closer identity carried through from the booking. | Closer identity is in GHL, not Calendly; depends on the GHL join. |
| **GHL** | Opportunities by owner; closer roster from the seed. | Trusted GHL copy chosen; users table populated; closer-stage transition history. | GHL users table is empty; some closer roles still missing from the roster seed. |
| **Fanbasis** | Not used directly, but blocks the "won = paid" outcome. | Cleaned-up Fanbasis transactions table to mark which deals actually paid. | Same Fanbasis blocker as Q2. |
| **Typeform** | Not used. | Not used. | Not used. |
| **Fathom** | Call basics exist (title, date, attendee, length). | Transcripts available for at least the closing-call cohort. | Transcripts missing for all calls. |
| **Stripe** | Not used. | Not used. | Not used. |

---

### Q6. Where do leads get stuck in the sales process?

- **Playbook chapter:** Funnel - Stage analysis
- **Priority:** Next (P1)
- **Headline status:** Partly working
- **Blocker class:** `client-decision` (GHL trusted copy) → `vendor-support` (confirm GHL emits stage transition events) → `extractor-build` if event-stream needed
- **Decision it unlocks:** Which sales stage or owner is slowing the pipeline down.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Booking event as the entry stage. | Nothing additional from Calendly. | Nothing material. |
| **GHL** | Opportunities and pipeline stages exist. | Stage history (timestamps for each stage transition) on every opportunity; trusted GHL copy. | GHL is stale; we still need to confirm we have enough stage history to measure time stuck per step. |
| **Fanbasis** | Not used. | Not used. | Not used. |
| **Typeform** | Not used. | Not used. | Not used. |
| **Fathom** | Not used. | Not used. | Not used. |
| **Stripe** | Not used. | Not used. | Not used. |

---

### Q7. Which booked calls no-show, and what rescue follow-up works?

- **Playbook chapter:** Conversion - Show rate
- **Priority:** Next (P1)
- **Headline status:** Partly working
- **Blocker class:** `dbt-staging` (Calendly status / cancellation fields landing) → `vendor-support` (GHL tasks / messages return rows) → `extractor-build`
- **Decision it unlocks:** Where the team should add reminders, rescue calls, or follow-up capacity.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Booking events are fresh and complete. | No-show, cancel, and reschedule status fully pulled into the cleaned-up tables. | Status fields are not fully landed in staging yet. |
| **GHL** | Conversation rows exist for some follow-ups. | Trusted GHL copy chosen; tasks and messages populated for rescue-cadence analysis. | GHL follow-up data is stale and incomplete; tasks and messages are empty in source. |
| **Fanbasis** | Not used. | Not used. | Not used. |
| **Typeform** | Not used. | Not used. | Not used. |
| **Fathom** | Not used (no-show by definition has no call). | Not used. | Not used. |
| **Stripe** | Not used. | Not used. | Not used. |

---

### Q8. Where are refunds and chargebacks coming from?

- **Playbook chapter:** Net Revenue
- **Priority:** Next (P1)
- **Headline status:** Blocked
- **Blocker class:** `vendor-support` (Fanbasis refund / chargeback shape - the single most important docs question on the matrix) → `extractor-build` → `dbt-staging`
- **Decision it unlocks:** Which offers, cohorts, or closers are creating refund risk.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Not used. | Not used. | Not used. |
| **GHL** | Contacts and opportunities to attribute refunds back to the closer or offer. | Trusted GHL copy chosen; freshness restored. | Same GHL gaps as Q2. |
| **Fanbasis** | Transactions are landing, but we do not yet know whether refunds and chargebacks are landing as separate rows or as updates. | Confirmation that refund and chargeback events arrive in the raw landing; cleaned-up refunds/chargebacks table. | Refunds and chargebacks structure in Fanbasis is unverified. |
| **Typeform** | Not used. | Not used. | Not used. |
| **Fathom** | Not used (refund analysis pulls in calls only as a downstream cross-cut, see Q12). | Not used. | Not used. |
| **Stripe** | Old refund and dispute data exists historically. | A cleaned-up Stripe disputes table for the historical lookback if needed. | Stripe is banned; historical-only. Disputes data is stale and not fully cleaned up. |

---

### Q9. Which customers stick around, and which ones churn?

- **Playbook chapter:** Retention
- **Priority:** Next (P1)
- **Headline status:** Blocked
- **Blocker class:** `vendor-support` (Fanbasis customer / subscription entity shape) → `extractor-build` → `dbt-warehouse`; plus `client-decision` if historical Stripe-to-Fanbasis bridge is needed
- **Decision it unlocks:** Which acquisition paths or sales patterns lead to better long-term customers.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Not used. | Not used. | Not used. |
| **GHL** | Contact and opportunity history for cohort definition (when did they convert, via which path). | Trusted GHL copy chosen; freshness. | Same GHL gaps as Q2. |
| **Fanbasis** | Transactions are landing; subscription or recurring-customer structure is unverified. | Confirmation of whether Fanbasis carries subscription, plan, or repeat-customer data; cleaned-up customers table. | Subscription / customer entity not yet inspected in Fanbasis. |
| **Typeform** | Not used. | Not used. | Not used. |
| **Fathom** | Not used. | Not used. | Not used. |
| **Stripe** | Old subscription data exists historically. | Bridge from Stripe customers to current Fanbasis customers if historical retention must be measured. | Stripe is banned; historical-only. |

---

### Q10. Which lead sources create bad or unusable leads?

- **Playbook chapter:** Attribution - Lead quality
- **Priority:** Next (P1)
- **Headline status:** Partly working
- **Blocker class:** `client-decision` (broader DQ rules beyond the no-phone OR lost-status filter; GHL trusted copy) → `extractor-build` (Typeform form_id) → `dbt-staging`
- **Decision it unlocks:** Which leads should be filtered, deprioritized, or treated differently.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Booking event - the indicator of "lead engaged enough to book." | Nothing additional. | Nothing material. |
| **GHL** | Contacts and opportunities; one agreed DQ filter applied today (no phone OR opportunity status = lost). | Trusted GHL copy; broader lead-quality rules agreed with the client beyond the current DQ filter. | Stale GHL data; only one DQ rule encoded; tag-based filters intentionally excluded per source-owner decision. |
| **Fanbasis** | Not used directly, though "bad" can be sharpened with paid-vs-not after Fanbasis lands. | Cleaned-up Fanbasis transactions to harden the "bad lead = booked but never paid" definition. | Same Fanbasis blocker as Q2. |
| **Typeform** | Response counts by form. | Reliable form ID per response so we can flag specific forms as low-quality producers. | Form ID gap (same as Q4). |
| **Fathom** | Not used. | Not used. | Not used. |
| **Stripe** | Not used. | Not used. | Not used. |

---

### Q11. Which content, ads, or funnels actually make money?

- **Playbook chapter:** Attribution - Multi-touch
- **Priority:** Later (P2)
- **Headline status:** Blocked to partly working
- **Blocker class:** `vendor-support` (Fanbasis docs) → `extractor-build` (Typeform form_id + UTM passthrough audit, GHL campaign-field hygiene) → `dbt-staging`; plus `client-decision` (GHL trusted copy)
- **Decision it unlocks:** Which funnel paths deserve more spend and which should be cut.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Booking event. | UTM or campaign attribution carried through from Typeform/GHL into the booking row. | Pass-through fidelity not yet audited end to end. |
| **GHL** | Contacts and opportunities by source / UTM where populated. | Trusted GHL copy; campaign and UTM hygiene; freshness. | Some campaign fields are placeholders today. |
| **Fanbasis** | Not used directly, but blocks the "made money = paid" outcome. | Cleaned-up Fanbasis transactions joined back to the campaign / form / ad on the contact. | Same Fanbasis blocker as Q2. |
| **Typeform** | Response counts by form title. | Form ID and any UTM fields captured at response time. | Form ID gap (same as Q4); UTM fields on Typeform responses not yet audited. |
| **Fathom** | Not used. | Not used. | Not used. |
| **Stripe** | Old paid-customer revenue available for historical lookback only. | Bridge to current Fanbasis customers if historical campaigns must be measured. | Stripe is banned; historical-only. |

---

### Q12. What is happening on sales calls?

- **Playbook chapter:** Conversation Intelligence
- **Priority:** Later (P2)
- **Headline status:** Blocked
- **Blocker class:** `vendor-support` (Fathom transcript plan tier + API access) → `client-decision` (plan upgrade if needed) → `extractor-build` → `dbt-staging`
- **Decision it unlocks:** Which objections, talk tracks, or call habits are tied to revenue or refunds.

| Source | Today | Needed | Missing |
|---|---|---|---|
| **Calendly** | Not used directly. | Not used. | Not used. |
| **GHL** | Opportunity and closer identity to attribute call outcomes back to people and offers. | Trusted GHL copy; freshness. | Same GHL gaps as Q5. |
| **Fanbasis** | Not used directly (could be cross-cut to revenue per call). | Same cleaned-up Fanbasis transactions if revenue per call is required. | Same Fanbasis blocker as Q2. |
| **Typeform** | Not used. | Not used. | Not used. |
| **Fathom** | Call basics (title, date, attendees, length). | Transcripts on every call so we can analyze content. | Transcripts missing for all calls - this is the call-content blocker. |
| **Stripe** | Not used. | Not used. | Not used. |

---

### Q13. Should Slack be used as data, or only for alerts?

- **Playbook chapter:** (governance question, not an analytics chapter)
- **Priority:** Later (P2)
- **Headline status:** Not started
- **Blocker class:** `client-decision` (David confirms Slack data matters or it stays out of the matrix entirely)
- **Decision it unlocks:** Whether Slack should be a data source on the next matrix at all.

This question is intentionally outside the source matrix. Current evidence says Slack is an alert destination, not a data source. We will not add a Slack column unless David specifically confirms Slack messages or client communications must be reported on. If that confirmation comes, this question gets a row in the next iteration of this matrix and Slack becomes the seventh column.

---

## Source readiness summary

How each confirmed source contributes across all 13 questions, and the single biggest gap to close.

### Calendly

- **Unlocks:** Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q10, Q11 (booking grain or booking-attached attribution).
- **Today:** Calendly is fresh and reliable. Booking events are clean.
- **Biggest gap:** No-show, cancel, and reschedule status fields are not fully landed in the cleaned-up tables. Fixing this unblocks Q7 directly and tightens Q1, Q5, and Q11.

### GHL

- **Unlocks:** Every question except Q13.
- **Today:** Contacts, opportunities, and basic conversations exist. SDR roster comes from a repo seed. The Speed-to-Lead metric is live on the v1 dashboard.
- **Biggest gap:** We are running on two GHL data copies, neither fully trusted. Until the trusted copy is chosen, every GHL-heavy question is partly working at best. Choosing the trusted copy is the single highest-leverage move on this matrix.

### Fanbasis

- **Unlocks:** Q2, Q4, Q5, Q8, Q9, Q11 (everything that ends in "did they pay").
- **Today:** Transactions are landing in raw. There is no cleaned-up Fanbasis transactions table for dbt to use, and refunds, chargebacks, customers, and subscriptions are not yet inspected.
- **Biggest gap:** Missing cleaned-up Fanbasis transactions table. This is the v2 revenue blocker and the precondition for any "did they pay" answer.

### Typeform

- **Unlocks:** Q2, Q4, Q10, Q11 (lead source attribution).
- **Today:** Responses are fresh.
- **Biggest gap:** No reliable form ID on cleaned-up responses, and no Typeform forms reference table. Without form ID, we can count leads but cannot tell which form they came from.

### Fathom

- **Unlocks:** Q5, Q12 (anything that requires call content).
- **Today:** Call basics exist (title, date, attendees, length).
- **Biggest gap:** Transcripts are missing for all calls. Without transcripts, conversation intelligence is impossible and closer coaching analysis stays surface-level.

### Stripe

- **Unlocks:** Q2, Q4, Q8, Q9, Q11 - historical lookback only. Stripe is banned at D-DEE.
- **Today:** Old transactions and disputes exist as historical reference.
- **Biggest gap:** Not Stripe itself; it is banned and frozen. The gap is the absence of a bridge from Stripe customers to current Fanbasis customers. If historical revenue must blend with current, we need that bridge before any blended revenue dashboard can ship.

---

## What this means for the next build

The matrix points at three unblocking moves before any new dashboard work starts. Each ties back to a playbook chapter so the build list can crib templates from published canon.

### 1. Pick the trusted GHL copy

Affects every question except Q13. Until this is decided, every GHL-heavy answer is partly working.

- **Playbook chapter unlocked:** all four Funnel-chapter questions (Q1, Q2, Q3, Q6) plus Conversion (Q5, Q7) and Attribution (Q4, Q10, Q11).
- **Closest published reference:** RevOps pipeline-hygiene playbooks (Pavilion, HubSpot Sales Hub docs) on owner / stage / source field discipline.

### 2. Land cleaned-up Fanbasis transactions

Affects every "did they pay" question (Q2, Q4, Q5, Q8, Q9, Q11). This is the single change that flips the most cells from blocked to working.

- **Playbook chapter unlocked:** Funnel - Lead → Paid, Net Revenue, Retention.
- **Closest published reference:** Stripe's own dispute analytics documentation for refund and chargeback shape, ChartMogul's net revenue retention framing for customer retention.

### 3. Add a Typeform forms reference table and lock down form ID

Low-cost helper. Unlocks attribution rows that the GHL and Fanbasis fixes cannot reach on their own.

- **Playbook chapter unlocked:** Attribution (Q4, Q10, Q11).
- **Closest published reference:** HubSpot multi-touch attribution playbooks; Bizible / 6sense lead-source quality literature.

Everything else - Fathom transcripts (Q5, Q12), Stripe-to-Fanbasis customer bridge (Q9 historical), Slack as data (Q13) - waits for an explicit ranking decision in the next dashboard build list. None of those three blocks more than two questions on the current matrix.

---

## Next page to write

The next discovery page is the dashboard build list itself. Use this matrix as the input. The build list should:

- Take the three unblocking moves above as ordered prerequisites.
- Group dashboards by playbook chapter (Funnel, Attribution, Conversion, Net Revenue, Retention, Conversation Intelligence) so each dashboard maps to a published genre with cribbable templates.
- Name the specific dashboard-ready table behind each dashboard before the dashboard work begins.

That page would live at `docs/discovery/dashboard-build-list.md` (proposed) and would be the artifact that ends Phase A and authorizes Phase B build work.
