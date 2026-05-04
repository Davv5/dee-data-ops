{% docs lead_journey %}

# lead_journey

**Grain:** one row per GHL contact (whether booked or not).
**Page:** Page 2 of the D-DEE Speed-to-Lead dashboard.

## Why it exists

Contact-grain "golden lead" surface. Answers the full-funnel attribution questions:
applicant → booker conversion, psychographic segment → close rate, lost-reason
breakdown, pipeline-stage distribution, multi-touch first-vs-last.

## What it's built from

- `dim_contacts` — row source; every GHL contact gets a row
- Aggregated `fct_calls_booked`, `fct_outreach`, `fct_payments` by contact_sk
- `dim_pipeline_stages` for current pipeline state
- `stg_ghl__opportunities` for current status + lost_reason
- `stg_calendly__events` for self-reported source

## Front-of-funnel metric (David correction)

At D-DEE, `application_submitted` is a lead-magnet gate, not a post-booking step.
The meaningful metric is **applicant → booker conversion rate** (oracle ≈ 51%:
3,141 bookers of 6,113 applicants), not "booked but didn't apply."

## Placeholders

Some columns ship as typed NULLs pending upstream Typeform-answers pivot /
bridge work (psychographics, lead magnets, self-reported source, engagement
score, first-vs-last-touch match). Schema stays stable; joins fill in when
upstream ships.

## Release gate

Singular test `release_gate_lead_journey.sql` asserts:
- row count within ±5% of oracle 15,598 contacts
- applicant count within ±10% of oracle 6,113 applications

{% enddocs %}


{% docs fanbasis_missing_ghl_contacts %}

# fanbasis_missing_ghl_contacts

Operator queue for Fanbasis buyers who have paid revenue but still do not
resolve to a GHL contact. This mart exists because the right fix for the final
Fanbasis identity gap is source depth and CRM hygiene, not weaker fuzzy
matching.

**Grain:** one row per Fanbasis customer/email identity with paid Fanbasis
payments and NULL `fct_payments.contact_sk`.

Inputs:

- `stg_fanbasis__transactions` — paid transaction identity from checkout
- `stg_fanbasis__customers` — customer directory identity from
  `/public-api/customers`
- `stg_fanbasis__subscribers` — subscriber/customer id, product, and status
  from `/public-api/subscribers`
- `dim_contacts` — GHL contact presence check by email and phone

`recommended_action` is the operator field:

1. `create_ghl_contact` — Fanbasis buyer exists, but no GHL email/phone match
2. `repair_identity_bridge` — GHL has exactly one candidate, but bridge did not
   attach it
3. `review_duplicate_ghl_contacts` — GHL has multiple candidates

The `suggested_ghl_contact_payload_json` field is a draft payload for review.
It is intentionally not pushed automatically into GHL from dbt.

{% enddocs %}

{% docs lead_magnet_detail %}

# lead_magnet_detail

**Grain:** one row per GHL opportunity.
**Primary key:** `opportunity_id`.

## Why it exists

Lead magnets are represented in GHL as pipelines. This mart turns those
pipeline opportunities into a business-facing analysis surface: which magnets
create volume, get worked, book calls, and turn into revenue.

## What it's built from

- `stg_ghl__opportunities` — row source
- `dim_pipeline_stages` — lead-magnet / stage names and booked-stage flag
- `lead_magnet_pipeline_taxonomy` — human-maintained GHL pipeline taxonomy and clean
  reporting names
- `dim_contacts` — contact identity and UTM context
- `dim_users` — current assigned user
- `fct_outreach` — calls and SMS inside the opportunity window
- `fct_calls_booked` — bookings inside the opportunity window plus direct
  `booking_time_opportunity_id` counts
- `fct_payments` + `fct_refunds` — net revenue inside the opportunity window

## Attribution window

About 45% of contacts have opportunities in more than one pipeline, so joining
all contact revenue to every pipeline would over-credit lead magnets. This mart
uses an opportunity window:

`opportunity_created_at <= event timestamp < next_opportunity_created_at`

That assigns follow-up, bookings, and revenue to the most recent lead-magnet
opportunity before the event. The `is_first_opportunity_for_contact` and
`is_latest_opportunity_for_contact` flags support first-touch and last-touch
views without changing the mart grain.

## Taxonomy

Raw GHL pipeline names are not all the same business object. Some are true
lead magnets, some are launches, some are waitlists, and some are sales
operating pipelines. The mart keeps the raw `lead_magnet_name` and adds
taxonomy fields:

- `lead_magnet_reporting_name`
- `lead_magnet_category`
- `lead_magnet_offer_type`
- `is_true_lead_magnet`
- `is_launch`
- `is_waitlist`
- `is_sales_pipeline`
- `include_in_lead_magnet_dashboard`

## Quality flags

`attribution_quality_flag` buckets every row:

1. `clean` — contact and pipeline are mapped, and the contact only appears in one magnet
2. `multi_magnet_contact` — same contact appears in multiple magnets; use window or first/latest flags
3. `contact_not_matched` — opportunity did not join to `dim_contacts`
4. `pipeline_not_mapped` — opportunity pipeline/stage did not join to `dim_pipeline_stages`

{% enddocs %}


{% docs lead_magnet_buyer_detail %}

# lead_magnet_buyer_detail

**Grain:** one row per matched paid contact.
**Primary key:** `contact_sk`.

## Why it exists

`lead_magnet_detail` is opportunity-grain. This mart is buyer-grain. It answers
the dashboard questions that should not be counted at transaction grain:

- how many people bought
- first purchase revenue
- total collected revenue
- payment-plan behavior
- latest known magnet before first purchase
- days from magnet to purchase
- bookings before purchase

## What it's built from

- `fct_payments` — paid matched payments by contact
- `fct_refunds` — net-of-refunds adjustment for Fanbasis payments
- `lead_magnet_detail` — first known magnet and latest prior magnet context
- `fct_calls_booked` — active/canceled bookings before first purchase
- `dim_contacts` — buyer identity and UTM context

## Attribution language

The latest prior magnet is **not** a source-system payment field. It is the
latest known GHL opportunity before the buyer's first purchase timestamp. Use
that label in dashboard UI: "Latest known magnet before first purchase."

The mart keeps uncovered buyers visible:

1. `latest_prior_magnet` — buyer has a known magnet before first purchase
2. `purchase_before_first_magnet` — buyer has a later known magnet, but bought first
3. `no_known_magnet` — buyer has no GHL opportunity/magnet
4. `missing_taxonomy` — prior magnet exists but taxonomy is missing
5. `uncategorized_offer_type` — prior magnet exists but offer type is still generic

## Booking status caveat

Calendly booking status currently supports `active` and `canceled`. This mart
does not claim show/no-show truth.

{% enddocs %}


{% docs canceled_booking_recovery_detail %}

# canceled_booking_recovery_detail

**Grain:** one row per canceled Calendly booking.
**Primary key:** `canceled_booking_sk`.

## Why it exists

Canceled bookings are not automatically lost demand at D-DEE. Triagers and
hosts can cancel for qualification, rescheduling, wrong-calendar cleanup, or
duplicate hygiene. This mart follows the next observable event after a canceled
booking so the Revenue Funnel can separate true leakage from recovered demand.

It answers:

- how many canceled bookings later rebooked
- whether the later booking likely showed
- whether Fathom gives stronger recorded-call evidence
- whether the contact later bought
- whether host/triager cancellations behave differently from invitee cancellations

## What it's built from

- `fct_calls_booked` — canceled booking spine and next active booking
- `stg_calendly__events` — cancellation reason and cancellation actor
- `stg_calendly__event_invitees` — no-show marker for the later active booking
- `stg_fathom__calls` — recorded-call evidence near the later active booking time
- `revenue_funnel_detail` — first purchase after cancellation and buyer revenue

## Show language

`has_likely_show_after_cancel` is intentionally called a **show signal**, not
attendance truth. It is true when the next active booking is due and was not
marked no-show. `has_fathom_show_evidence` is stronger because it requires a
revenue-relevant Fathom call within 15 minutes of the next active booking's
scheduled start.

## Revenue crediting

A buyer can have more than one canceled booking. `total_net_revenue_after_cancel`
answers this row's local question: did this cancellation precede the buyer's
first purchase? `credited_net_revenue_after_first_cancel` credits buyer revenue
only to the first canceled booking for the contact so dashboard totals do not
duplicate recovered revenue.

{% enddocs %}


{% docs revenue_funnel_detail %}

# revenue_funnel_detail

**Grain:** one row per matched paid contact.
**Primary key:** `contact_sk`.

## Why it exists

`revenue_detail` is the payment-reconciliation table. It keeps every payment,
including unmatched rows, at transaction grain. `revenue_funnel_detail` is the
buyer-journey table: one row per paid buyer with the best available story of
how money was created.

It answers:

- what the buyer bought
- whether the buyer looks like a payment-plan buyer
- which lead magnet was latest before first purchase
- whether the buyer booked before buying
- whether the buyer was touched or reached before buying
- which closer or sales-call owner is most defensibly tied to the revenue
- which setter or first-touch rep is most defensibly tied to the path

## What it's built from

- `lead_magnet_buyer_detail` — buyer-grain source of truth and magnet attribution
- `lead_magnet_detail` — latest-prior opportunity owner and window metrics
- `fct_payments` + `fct_refunds` — paid payments, product, payment-plan signal, net revenue
- `fct_outreach` — pre-purchase call/SMS path
- `fct_calls_booked` — latest booking before first purchase
- `stg_fathom__calls` — recorded-call evidence near bookings and purchases
- `stg_fathom__call_invitees` — buyer-email matches to Fathom calendar invitees
- `stg_calendly__event_memberships` — Calendly host/calendar account evidence
- `operator_identity_aliases` — manual aliases for operator emails not in GHL users
- `dim_users` — operator labels

## Attribution language

`credited_closer_*` is the primary Revenue Credit surface. It is not a
commission rule. It is a buyer-grain operating read that chooses the strongest
available closer / sales-call evidence in this order:

1. closer-role owner of the latest prior opportunity
2. closer-role Fathom recorder on a revenue-relevant call where the buyer's
   email appears as an external calendar invitee before purchase
3. closer-role Fathom recorder on a revenue-relevant call within 15 minutes of
   the latest booking before purchase
4. closer-role booking owner
5. closer self-introduction in the Fathom transcript on a team-account recording
6. closer-role Calendly host on the latest booking before purchase
7. non-closer owner / Fathom recorder / booking owner fallback
8. Calendly host account for Fathom team-account recordings, marked low confidence
9. Fathom team-account recorder fallback, marked low confidence
10. latest booking Calendly host fallback, marked low confidence when it is not a known closer
11. unassigned

Direct Fathom contact-email matching is intentionally separate from the
booking-time match. It catches sales calls where the buyer was on the Fathom
calendar invite but Calendly/GHL did not tie the booking cleanly. Team recorder
accounts are first checked against the matched Calendly scheduled-event host.
If that host is still a shared calendar account, it stays low confidence instead
of being treated as an individual closer.
When the recording itself is a shared Fathom/team account, transcript evidence
can upgrade the row only when the internal speaker says a rostered closer's
first name in a direct self-introduction pattern such as "my name is Ethan".
Transcript-only aliases, such as Jaden/Jayden, live in
`operator_speech_aliases` and are marked medium confidence. Plain name mentions
are intentionally ignored.
Calendly event titles can also upgrade a row when the latest booking title ends
with a rostered parenthetical operator name, such as "Brand Scaling Blueprint
Access Call (Hammad)". This is stronger than a shared host account but still
kept below direct Fathom / GHL ownership evidence.
Generic Calendly host fallback is intentionally low confidence for shared
calendar accounts such as Mind of Dee / Manny; it means "we know the booking
account," not "we know the human closer."

`credited_setter_*` stays separate. It credits the first successful pre-purchase
call when available, then first touch. This lets the dashboard distinguish
"who handled/closed the sales path" from "who first worked the buyer."

`best_available_operator_*` is not a commission rule. It is an operating
diagnostic that chooses the most concrete path evidence in this order:

1. first successful call before purchase
2. first human touch before purchase
3. owner of the latest prior opportunity/magnet
4. rostered operator named in the latest booking title
5. booking-time owner
6. unassigned

Use the source column beside the name so a user can see why a person was
credited.

## Payment-plan language

`is_payment_plan_buyer` is an inferred operating signal, not a source-system
contract. It turns true when a buyer has more than one paid payment, Fanbasis
`auto_renew` payments, or a product name that looks like split pay / deposit /
balance / payment-plan language. This is enough for funnel operations, but
finance-grade installment schedules need a future Fanbasis subscription or
plan-change source.

`payment_plan_truth_status` is the guardrail label for that exact gap. The
current Fanbasis extractor lands completed transactions from
`/public-api/checkout-sessions/transactions`; it does not yet land
`/public-api/subscribers` or checkout-session subscription rows, so the mart can
say “cash collected” and “auto-renew signal” but not “remaining balance owed.”
The Fanbasis API docs expose subscriber/subscription endpoints; landing those is
the next source-layer step before this can become receivables truth.

## Quality flags

`revenue_funnel_quality_flag` keeps messy rows visible:

1. `clean` — usable buyer journey row
2. `missing_taxonomy` — latest prior magnet exists but taxonomy is missing
3. `uncategorized_offer_type` — latest prior magnet has only generic taxonomy
4. `negative_net_revenue` — refunds exceed net revenue for the buyer
5. `contact_not_matched` — contact id did not survive the buyer contract
6. `no_known_magnet` — buyer has no known pre-purchase magnet

{% enddocs %}


{% docs customer_retention_detail %}

# customer_retention_detail

**Grain:** one row per matched paid contact per calendar month.
**Primary key:** `customer_retention_sk`.

## Why it exists

`revenue_funnel_detail` tells the buyer story once. `customer_retention_detail`
turns that buyer into a month-by-month operating surface: when they first paid,
whether they paid again, whether a refund hit later, what LTV looked like over
time, and what source/product/operator context those retained dollars belong to.

It answers:

- which cohorts created repeat paid months
- which products produced multi-payment or auto-renew behavior
- which lead magnets and operators drove customers with higher LTV
- where refunds changed the customer value curve
- which Fanbasis buyers have subscriber/customer evidence versus cash-only rows

## What it's built from

- `revenue_funnel_detail` — buyer identity, product family, lead-magnet, closer,
  setter, and lifetime revenue context
- `fct_payments` — paid payment events; this is the cash spine
- `fct_refunds` — Fanbasis refund events by refund month
- `stg_fanbasis__transactions` — Fanbasis customer ids attached to payment rows
- `stg_fanbasis__subscribers` — subscriber/subscription status evidence
- `stg_fanbasis__customers` — Fanbasis customer-directory evidence

## Truth boundary

This mart does **not** claim full churn or remaining-balance truth yet. Payment
activity is treated as audited cash activity. Fanbasis subscriber rows are used
as lifecycle evidence, especially for current active/completed/failed/onetime
status, but they are not used to invent unpaid future receivables.

`retention_state` is month-specific:

1. `new_paid_month` — first purchase month
2. `repeat_paid_month` — later month with a paid payment
3. `refund_only_month` — refund activity without same-month paid cash
4. `active_subscriber_current_month_no_payment` — current month has active
   Fanbasis subscriber evidence but no paid payment yet
5. `post_latest_payment_month` — month after latest paid activity
6. `observed_gap_month` — between purchase months with no payment/refund signal

`customer_lifecycle_status` is current best evidence, not a historical monthly
SCD. If a buyer has an active Fanbasis subscriber record today, prior months
will carry that current lifecycle label while `retention_state` remains the
month-specific activity label.

## Payment-plan health

`repeat_payment_type` separates repeat cash into buckets a human can act on:

1. Fanbasis auto-renew / installment evidence
2. Fanbasis subscription installment evidence
3. multi-product repeat or upsell
4. same-product multi-payment
5. single-payment / no-repeat-yet states

`payment_plan_health_status` is the operator layer. It translates lifecycle and
payment timing into actions such as failed-plan recovery, active-plan due/no
payment yet, active-plan not-yet-due, completed-plan paid off, one-time upsell
candidate, or historical Stripe product repair.

Expected-payment dates are heuristic. They use the latest Fanbasis subscription
payment frequency when present, defaulting to 30 days for subscription rows
without a frequency. They are not a receivables ledger and should not be used as
finance-grade balance truth.

## Quality flags

`retention_quality_flag` keeps caveats visible:

1. `clean` — usable customer-month row
2. `missing_product_family` — product still falls into Unknown / historical Stripe
3. `no_subscriber_record` — Fanbasis cash exists but no subscriber row matched
4. `negative_lifetime_value` — refunds exceed net revenue for the buyer

{% enddocs %}


{% docs collection_contract_evidence_detail %}

# collection_contract_evidence_detail

**Grain:** one row per current matched paid customer.
**Primary key:** `collection_contract_evidence_sk`.

## Why it exists

Manual collections changed the retention question. We still do not have a
finance-grade promised contract value or remaining balance source, but we do
have two strong evidence streams:

- payment rows that prove how much cash was collected and what product was sold
- Fathom transcript snippets where the sales call mentions payment terms

This mart puts those two streams together without pretending transcript amounts
are a receivables ledger.

## What it's built from

- `customer_retention_detail` — current customer row, collected cash, product,
  collection motion, and operator next action
- `revenue_funnel_detail` — candidate Fathom sales-call ids and closer/setter
  attribution
- `stg_fathom__transcript_segments` — raw transcript snippets searched for
  amount + payment-context language

## Truth boundary

`lifetime_net_revenue_after_refunds`, `upfront_collected_net_revenue`, and
`post_first_collected_net_revenue` are payment facts.

`payment_terms_evidence_text`, `mentioned_payment_amounts_text`, and
`largest_mentioned_payment_amount` are transcript evidence. Use them to review
what was discussed on the call. Do not use them as contract value or balance
owed until a proper promise/contract source lands.

`contract_evidence_status` keeps coverage plain:

1. `transcript_payment_terms_found` — candidate sales-call transcript has
   payment-term snippets
2. `sales_call_found_no_payment_terms` — sales call exists, but no matching
   payment-term snippet was found
3. `no_sales_call_transcript` — no candidate Fathom sales call was available

{% enddocs %}


{% docs customer_action_queue %}

# customer_action_queue

**Grain:** one row per matched customer x action surface.
**Primary key:** `customer_action_id`.

## Why it exists

This is the bridge from dashboards to a future action-handler agent. Revenue,
Retention, Contract Terms, and the operator review ledger all had useful action
logic, but each lived in its own surface. This mart makes one readable queue:
who needs attention, why, how much money is at stake, which route to use, what
source table produced the signal, and whether a human already handled it.

## Product boundary

Expose this queue as its own Action Queue tab/workspace. Do not blend it into
Revenue, Retention, or Customer 360 as another chart section. Speed-to-Lead,
Lead Magnets, Revenue, Retention, and Customer 360 remain truth/exploration
surfaces. `customer_action_queue` is an execution surface where actions can be
reviewed, routed, completed, and written back to app-owned ledgers.

## What it's built from

- `revenue_funnel_detail` — revenue/data/product/attribution action candidates
- `customer_retention_detail` — recovery, renewal, upsell, watchlist, and
  manual-collection action candidates
- `collection_contract_evidence_detail` — contract-terms review candidates
- `operator_action_reviews` — app-owned review ledger for fixed/wont-fix state
- `contract_terms_reviews` — app-owned confirmed contract terms ledger

The app-owned ledgers are declared as `marts_app` sources because they are
mutable human decisions, not dbt-derived source truth.

## Truth boundary

`revenue_credit_*` is the strongest known revenue attribution. It is not the
same thing as current follow-up ownership.

`current_owner_name` is intentionally `Unknown` and `current_owner_source` is
`not_modeled_yet` until a real assignment/ownership source exists. The queue is
therefore safe for a future agent to read without inventing accountability.

`is_action_open` respects the operator review ledger. `fixed` and `wont_fix`
rows close the action unless the review has expired. Confirmed contract terms
also close the contract-terms review action even if the separate review row is
missing.

{% enddocs %}


{% docs revenue_detail %}

# revenue_detail

**Grain:** one row per payment event (Stripe charge, Fanbasis payment).
**Page:** Page 3 of the D-DEE Speed-to-Lead dashboard.

## Why it exists

Finance + GTM both need to see the full revenue number broken out by attribution.
If we silently drop payments that can't be matched to a contact, the mart total
is lower than Stripe's dashboard — eroding trust. Every payment stays visible;
`attribution_quality_flag` calls out the uncertain rows.

## What it's built from

- `fct_payments` — row source (union of Stripe + Fanbasis; either arm can be empty during ingest outages)
- `bridge_identity_contact_payment` — left-join for `contact_sk`, `match_method`,
  `match_score`, `bridge_status`; unmatched and payment-identity-only payments
  survive with NULL contact_sk
- `fct_refunds` — pre-aggregated per `(source_platform, parent_payment_id)` and left-joined
  to expose `refunds_total_amount`, `refunds_total_amount_net`, `refunds_count`,
  and `net_amount_after_refunds`
- `dim_contacts` — left-join for campaign / first-touch / last-touch / lead-magnet
  attribution when matched
- `stg_ghl__opportunities` + `dim_users` — latest Closer per contact when matched

## Net-of-refunds asymmetry

The two payment sources arrive with different refund semantics, so
`net_amount_after_refunds` branches by `source_platform`:

- **Stripe** rows return `net_amount` directly. `stg_stripe__charges` already
  computes `amount_captured_minor - amount_refunded_minor` at staging, so
  `fct_payments.net_amount` is already net of refunds for Stripe.
- **Fanbasis** (and any future non-Stripe arm) returns
  `net_amount - refunds_total_amount`. Fanbasis's `net_amount` is net of fees
  only, so the refund is subtracted here.

Mechanical branching keeps the math correct even if `fct_refunds` extends
to Stripe later — no doc-only future-Claude trap. Singular test
`revenue_detail_refunds_parity.sql` asserts the mart's refund total matches
`fct_refunds` exactly per source_platform regardless.

## DQ flag semantics

`attribution_quality_flag` buckets every row:

1. `unmatched` — bridge could not link the payment to any contact
2. `ambiguous_contact_match` — bridge matched to more than one candidate
3. `payment_identity_only` — payment has source identity, but no CRM contact row
4. `role_unknown` — matched to a contact but no Closer-role attribution available
5. `clean` — everything above is resolved

## Release gate

Singular test `release_gate_revenue_detail.sql` asserts:
- `sum(net_amount)` within ±5% of oracle $356,935.16
- row count within ±5% of 1,423
- unmatched revenue share ≤10% (soft threshold — transparency signal)

{% enddocs %}
