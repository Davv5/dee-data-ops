{% docs bridge_identity_contact_payment__overview %}

Payment-centric identity bridge: every payment (Stripe charge + Fanbasis
transaction) gets exactly one row, with the best-available match to
`dim_contacts` + diagnostic metadata. `fct_payments` picks `contact_sk`
up by left-joining this bridge on `(source_platform, payment_id)`.

### Why a composite PK

The bridge unions Stripe + Fanbasis. A bare `payment_id` join is
ambiguous because the two providers issue ids in independent
namespaces — there is no guarantee a Stripe `ch_*` id can never
collide with a Fanbasis transaction id. Keying on
`(source_platform, payment_id)` makes the join deterministic and
matches `fct_payments.payment_sk`, which is already hashed over the
same pair.

Stripe at D-DEE is historical-only (memory
`project_stripe_historical_only.md`); Fanbasis is the live forward-going
contributor. Both arms ride the same tier ladder so the match-rate
target applies to the unioned bridge, not per-source.

### Why a bridge (not widened columns on dim_contacts or fct_payments)

Per `.claude/rules/warehouse.md` — multi-source identity resolution
resolves to GHL's `contact_id` *upstream* of `dim_contacts`, not by
widening the dim's PK. Keeping the matching logic in a named bridge
model isolates it, makes the tier table testable, and lets the
`relationships` test on `fct_payments.contact_sk` work unchanged.

### Tiers (priority order)

| Tier | Rule | Score | `bridge_status` when selected |
|---|---|---|---|
| 1. `email_exact` | `payment.email_norm = dim_contacts.email_norm` | 1.00 | `matched` |
| 2. `phone_last10` | last 10 digits of phone equal | 1.00 | `matched` |
| 3. `email_canonical` | gmail dot/plus normalized on both sides | 0.95 | `matched` |
| 4. `stripe_customer_email` | Stripe charge failed billing identity, but linked Stripe customer email resolves to CRM | 0.90 | `matched` |
| 5. `stripe_customer_phone` | Stripe charge failed billing identity, but linked Stripe customer phone resolves to CRM | 0.90 | `matched` |
| 6. `fanbasis_conversation_email` | Fanbasis email matches a historical GHL conversation email for exactly one contact | 0.88 | `matched` |
| 7. `fanbasis_unique_crm_name` | Fanbasis buyer name matches exactly one CRM contact full name | 0.82 | `matched` |
| 8. `billing_email_direct` | payment has email but no CRM contact — payment-only fallback | 0.80 | `payment_identity_only` |
| 9. `unmatched` | no email, no phone, no CRM match | 0.00 | `unmatched` |

`ambiguous_multi_candidate` is surfaced when > 1 distinct `contact_sk`
tied at the highest observed score for a payment. The `qualify
row_number()` still picks one, but the diagnostic flag fires so mart
owners can decide whether to trust the pick or suppress the row.

### Source-side identity columns

| `source_platform` | Email column | Phone column |
|---|---|---|
| `stripe` | `stg_stripe__charges.billing_email`; fallback `stg_stripe__customers.email` | `stg_stripe__charges.billing_phone`; fallback `stg_stripe__customers.phone` |
| `fanbasis` | `stg_fanbasis__transactions.fan_email`; fallback `stg_ghl__conversations.contact_email` | `stg_fanbasis__transactions.fan_phone` |

Fanbasis also exposes `fan.name`. The bridge only uses it after email,
phone, Stripe-customer, and GHL-conversation identity fail, and only when
the normalized full name resolves to exactly one CRM contact.

### Gmail canonical normalization

For emails in `gmail.com` / `googlemail.com`: drop dots from the
local part, drop everything after `+`, then lowercase. Applied
symmetrically on both sides of the join.

```
rich.from.clothes+ddee@gmail.com   →  richfromclothes@gmail.com
richfromclothes@gmail.com           →  richfromclothes@gmail.com
```

Non-Gmail providers pass through unchanged — treating them as
case-sensitive is the safer default given provider-side variance.

### Target match rate

≥ 70% per source — a **tuning trigger**, not a ship gate. Enforced by
`bridge_match_rate_floor.sql` (severity = `warn`); a new payment
processor can legitimately land below 70% during a contact-backfill
catch-up window.

Hard ship gates remain:

- `bridge_payment_count_parity` — every staging payment row appears in
  the bridge (no silent drops)
- `dbt_utils.unique_combination_of_columns` — composite PK
  `(source_platform, payment_id)` is unique

If the warn fires and persists past a backfill window, retune the tier
set or backfill upstream contact data before treating that source's
`fct_payments` rows as report-grade — and escalate to David.

{% enddocs %}

{% docs bridge_stripe_payment_product_repair__overview %}

Historical Stripe direct charges frequently omit product identity: no invoice,
no charge description, no metadata, and no Stripe customer id. This bridge
keeps those rows from becoming permanent `Unknown / historical Stripe` by
repairing product labels with traceable evidence.

### Repair priority

1. **GHL opportunity context** — nearby opportunity pipeline/stage/name
   mentions a known product family.
2. **Calendly booking context** — nearby booked/scheduled call event name
   mentions a known product family.
3. **Stripe amount pattern** — no context match exists, but the historical
   charge amount matches a known repeated Stripe product pattern.

`fct_payments` only uses this bridge when source Stripe product fields are
missing. Invoice line products, invoice descriptions, and charge descriptions
remain higher authority than repairs.

{% enddocs %}
