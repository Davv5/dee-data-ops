{% docs fct_calls_booked__overview %}

The Speed-to-Lead denominator. One row per Calendly booking event —
every confirmed booking, regardless of whether it later landed on a
GHL opportunity or made it to a booked pipeline stage. Per
`dim_pipeline_stages__overview`, Calendly (~3,141 events) and the
GHL booked-stage count (~1,825) measure different things; the fact
here is authoritative on "did a booking event happen?"

### Pending join axes

- `contact_sk`: needs `stg_calendly__event_invitees` (invitee email)
  to email-join `dim_contacts`. Owed in Track C follow-on.
- `assigned_user_sk` / `pipeline_stage_sk`: resolve via the booked
  opportunity in GHL. Needs the invitee-email → opportunity match
  first (same dependency).

Until those land, the three FK columns emit NULL and the
`relationships` tests auto-exclude nulls. When they ship, widen the
join here — do not widen `dim_contacts`.

{% enddocs %}

{% docs fct_outreach__overview %}

The Speed-to-Lead numerator source. One row per outbound human
CALL/SMS — `stg_ghl__messages` filtered to outbound TYPE_CALL /
TYPE_SMS, restricted to conversations with a non-null
`last_manual_message_at` (GHL's native "a human touched this"
signal). No role filter at this layer — the SDR filter lives in the
mart so the same fact powers AE and Closer leaderboards without
duplicating facts.

### Why no role filter here

Per Track D's grain-audit decisions: the warehouse fact is a faithful
record of outbound touches regardless of who made them; filtering
by `role = 'SDR'` is a reporting concern that belongs at the
`sales_activity_detail` mart. Keeps the fact reusable.

### Contact / user matching

Both are GHL-native joins (the message row carries `contact_id` +
`user_id` directly), so `match_method` is always `ghl_native` and
`match_score` is always 1.00. Those columns exist only for
shape-parity with `fct_revenue`, where matching is tiered.

{% enddocs %}

{% docs fct_revenue__overview %}

One row per payment event, unioned across Stripe and Fanbasis.

### Union shape

- **Stripe side:** `stg_stripe__charges`, one row per charge. Amounts
  converted from minor units (cents) to major units here — staging
  preserves Stripe's minor-unit contract for multi-currency fidelity,
  warehouse normalizes to major units for mart-friendly aggregation.
- **Fanbasis side:** zero-row `where false` stub. The extractor is
  still blocked on Week-0 credentials. Structural parity is preserved
  so `dbt build` is green today; when the real staging model lands,
  swap the CTE body for a ref to the forthcoming Fanbasis staging model
  without touching the downstream union or the final projection.

### Contact attribution

Flows through `bridge_identity_contact_payment`. When the bridge
returns `unmatched`, `contact_sk` is NULL and `bridge_status` /
`match_method` carry the diagnostic. Mart-level revenue rollups
should decide explicitly whether to include unmatched revenue (it is
real money) or exclude it (it can't be attributed to a SDR / AE).

{% enddocs %}
