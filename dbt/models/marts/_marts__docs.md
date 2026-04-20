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

{% enddocs %}
