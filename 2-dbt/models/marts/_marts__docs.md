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
- which operator is most defensibly tied to the path

## What it's built from

- `lead_magnet_buyer_detail` — buyer-grain source of truth and magnet attribution
- `lead_magnet_detail` — latest-prior opportunity owner and window metrics
- `fct_payments` + `fct_refunds` — paid payments, product, payment-plan signal, net revenue
- `fct_outreach` — pre-purchase call/SMS path
- `fct_calls_booked` — latest booking before first purchase
- `dim_users` — operator labels

## Attribution language

`best_available_operator_*` is not a commission rule. It is an operating
diagnostic that chooses the most concrete path evidence in this order:

1. first successful call before purchase
2. first human touch before purchase
3. owner of the latest prior opportunity/magnet
4. booking-time owner
5. unassigned

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
  `match_score`, `bridge_status`; unmatched payments survive with NULL contact_sk
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
