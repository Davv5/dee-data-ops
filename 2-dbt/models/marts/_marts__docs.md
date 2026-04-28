{% docs sales_activity_detail %}

# sales_activity_detail

**Grain:** one row per Calendly booking event.
**Page:** Page 1 of the D-DEE Speed-to-Lead dashboard.

## Why it exists

The canonical Speed-to-Lead surface. Answers "% of booked calls where a human SDR
made first outbound contact within 5 min" and all supporting SDR / closer / cycle-time
/ show-rate cuts.

## What it's built from

- `fct_calls_booked` — Calendly booking events (grain row source)
- `fct_outreach` — outbound user touches (all roles; role filter applied HERE at the mart layer)
- `dim_contacts`, `dim_users`, `dim_pipeline_stages`, `dim_calendar_dates`
- `stg_ghl__opportunities` for closer attribution + close outcome + lost reason

## Speed-to-Lead metric columns

The SDR-role filter is applied at this mart (not the warehouse) per
`.claude/rules/mart-naming.md` and the DataOps-corrected metric definition:

- `minutes_to_first_sdr_touch` — NULL when `first_toucher_role != 'SDR'`
- `is_within_5_min_sla` — boolean, the headline metric
- `had_any_sdr_activity_within_1_hr` — DQ diagnostic

## DQ flag semantics

`attribution_quality_flag` buckets every row:

1. `no_sdr_touch` — no outbound SDR touch found after booking
2. `role_unknown` — first toucher's role is unknown in the seed
3. `pre_utm_era` — contact is in the pre-UTM era (attribution fields unreliable)
4. `ambiguous_contact_match` — bridge matched to multiple contacts
5. `clean` — everything above is resolved

## Release gate

Singular test `release_gate_sales_activity_detail.sql` asserts row count within
±5% of oracle 3,141 Calendly bookings (2026-03-19 snapshot).

{% enddocs %}


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
3. `role_unknown` — matched to a contact but no Closer-role attribution available
4. `clean` — everything above is resolved

## Release gate

Singular test `release_gate_revenue_detail.sql` asserts:
- `sum(net_amount)` within ±5% of oracle $356,935.16
- row count within ±5% of 1,423
- unmatched revenue share ≤10% (soft threshold — transparency signal)

{% enddocs %}
