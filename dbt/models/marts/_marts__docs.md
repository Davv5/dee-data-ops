{% docs revenue_detail %}

# revenue_detail

**Grain:** one row per payment event (Stripe charge, Fanbasis payment).
**Page:** Page 3 of the D-DEE speed-to-lead dashboard — revenue by campaign,
revenue by closer, payment method + card country, and the
**unmatched-revenue transparency tile**.

## Why it exists

Finance and GTM both need to see the full revenue number broken out by
attribution. If we silently drop payments that cannot be matched to a
contact, the dashboard shows a revenue total lower than Stripe and
erodes trust in every downstream number. This mart keeps every payment
visible — matched and unmatched — and surfaces a per-row
`attribution_quality_flag` so the dashboard can call out exactly which
dollars are attributed cleanly and which aren't.

## What it's built from

- `fct_revenue` — union of Stripe + Fanbasis payments at the payment grain.
  The primary row source; nothing here gets filtered out.
- `bridge_identity_contact_payment` — left-join on `payment_id` to carry
  `contact_sk`, `match_method`, `match_score`, and `bridge_status` onto
  every payment. Unmatched payments survive with NULL contact_sk.
- `dim_contacts` — left-join on `contact_sk` to pull campaign /
  first-touch / last-touch / lead-magnet attribution when matched.
- `stg_ghl__opportunities` + `dim_users` — resolve the latest Closer per
  contact (most recently updated opportunity, Closer role only).

## DQ flag semantics

`attribution_quality_flag` buckets every row into exactly one of four
states, checked in this order:

1. `unmatched` — bridge could not link the payment to any contact.
2. `ambiguous_contact_match` — bridge matched to more than one candidate.
3. `role_unknown` — matched to a contact but no Closer-role
   opportunity exists for that contact yet.
4. `clean` — matched and attributed end-to-end.

## Release-gate test

`release_gate_revenue_detail.sql` (singular test in `dbt/tests/`) asserts
three invariants jointly against `oracle_dashboard_metrics_20260319`:

- Revenue total within ±5% of the oracle `Total Revenue (USD)`.
- Row count within ±5% of the oracle `Paying Customers` proxy (1,423 —
  1,350 to 1,494).
- Unmatched revenue share under 10% — hard stop on silent attribution
  decay.

## Known v1 caveat — Fanbasis

`fct_revenue` is a union of Stripe + Fanbasis; Fanbasis is zero-rows in
v1 pending Week-0 API credentials (see `CLAUDE.local.md`). If the
revenue-parity assertion fires with a shortfall that lines up with
Fanbasis volume, the tolerance should be widened in the PR with a note
linking the Fanbasis blocker — **not** by filtering unmatched rows out
of the mart.
{% docs mart_lead_journey_overview %}

# `lead_journey` — contact-grain golden-lead surface

One row per GHL contact, whether they ever booked a call or not.
Powers Page 2 of the D-DEE dashboard:

- **Funnel** — lead → applicant → booker → shower → buyer
- **Attribution** — first-touch / last-touch / self-reported source
- **Psychographics** — Typeform-sourced quiz answers
- **Lost reason** — latest opportunity status + lost_reason
- **Applicant → booker conversion** — `application_to_booker_flag`

Expected row count: ~15,598 (anchored on `dim_contacts`, which is 1:1
with GHL contacts per the v1 single-anchor identity spine rule).

## Placeholder inventory

Columns that ship as typed NULLs today until their upstream bridge
or pivot lands. Contract stays stable; the join fills in when
upstream ships.

| Column | Upstream owed |
|---|---|
| `application_submitted`, `application_date` | Typeform-answers pivot |
| `lead_magnet_first_engaged`, `lead_magnet_history` | GHL-tag pivot |
| `age`, `business_stage`, `investment_range`, `core_struggle`, `emotional_goal_value`, `current_situation` | Typeform-answers pivot |
| `self_reported_source`, `self_reported_vs_utm_match` | `stg_calendly__event_questions_and_answers` |
| `engagement_score` | Not surfaced on GHL opportunity endpoint |

## Known parity gaps (v1)

- `bookings_count` will report 0 for every contact until
  `fct_calls_booked.contact_sk` resolves (Calendly invitee staging —
  Track C open thread). The oracle says 3,141 contacts have at
  least one booking; v1 will under-report until that bridge lands.
- `application_submitted` is NULL today — `release_gate_lead_journey`
  fails on the applicant-count assertion until the Typeform pivot
  ships. This is expected and correctly signals the dependency.

## DQ flag semantics

`attribution_quality_flag` values:

- `clean` — every signal present and mutually consistent
- `pre_utm_era` — the contact's `attribution_era = pre_utm`
- `no_sdr_touch` — the contact is a booker but no SDR touch is
  attributed (`fct_outreach` row count = 0 for SDR-role users)
{% docs sales_activity_detail %}

**Grain:** one row per Calendly booking event (~3,141 rows, ±5% of oracle).

**Purpose:** primary Speed-to-Lead reporting surface. Wide, denormalized, dashboard-ready.

**Core metric logic:**
- Numerator columns (`minutes_to_first_sdr_touch`, `is_within_5_min_sla`) populate only when
  the first outbound touch after booking was made by a user with `role = 'SDR'`. That role
  filter lives at the mart — `fct_outreach` remains a faithful record of every outbound
  touch regardless of role, so the same fact powers AE / Closer analyses downstream.
- `had_any_sdr_activity_within_1_hr` is a DQ diagnostic — ANY user activity within 60 min of
  booking — designed to separate "SLA missed" from "SLA not applicable" when the SDR numerator
  is null.

**Join map:**
- `fct_calls_booked` is the spine.
- `dim_contacts` on `contact_sk` — supplies identity, UTM attribution, and era.
- `dim_users` twice: once via `assigned_user_sk` (assignment), once via `fct_outreach.user_sk` (first toucher).
- `fct_outreach` windowed to `touched_at >= booked_at`, picking first by `row_number()`.
- `stg_ghl__opportunities` is joined on `contact_id` (not booking); most recent opp by
  `opportunity_created_at` wins. Surfaces closer + outcome + `lost_reason_id` +
  `last_stage_change_at`.
- `dim_pipeline_stages` on `pipeline_stage_sk` — supplies human-readable pipeline/stage
  names and the `is_booked_stage` boolean.

**Why `lost_reason` carries the id, not the text:** GHL returns `lostReasonId` only; a
`dim_lost_reasons` lookup is not in v1 scope. The id preserves the signal; a future track
can widen when the lost-reason catalog is available.

**Known DQ gates:**
- `attribution_quality_flag` is never null — see the column description for the enum.
- The release-gate test (`dbt/tests/release_gate_sales_activity_detail.sql`) fails when the
  mart row count deviates more than ±5% from the oracle `Calls Booked` seed (3,141).

{% enddocs %}
