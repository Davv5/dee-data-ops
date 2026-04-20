{% docs bridge_identity_contact_payment__overview %}

Payment-centric identity bridge: every Stripe charge gets exactly one
row, with the best-available match to `dim_contacts` + diagnostic
metadata. `fct_revenue` picks `contact_sk` up by left-joining this
bridge on `payment_id`.

### Why a bridge (not widened columns on dim_contacts or fct_revenue)

Per `.claude/rules/warehouse.md` — multi-source identity resolution
resolves to GHL's `contact_id` *upstream* of `dim_contacts`, not by
widening the dim's PK. Keeping the matching logic in a named bridge
model isolates it, makes the tier table testable, and lets the
`relationships` test on `fct_revenue.contact_sk` work unchanged.

### Tiers (priority order)

| Tier | Rule | Score | `bridge_status` when selected |
|---|---|---|---|
| 1. `email_exact` | `payment.email_norm = dim_contacts.email_norm` | 1.00 | `matched` |
| 2. `phone_last10` | last 10 digits of phone equal | 1.00 | `matched` |
| 3. `email_canonical` | gmail dot/plus normalized on both sides | 0.95 | `matched` |
| 4. `billing_email_direct` | payment has email but no CRM contact — payment-only fallback | 0.80 | `matched` |
| 5. `unmatched` | no email, no phone, no CRM match | 0.00 | `unmatched` |

`ambiguous_multi_candidate` is surfaced when > 1 distinct `contact_sk`
tied at the highest observed score for a payment. The `qualify
row_number()` still picks one, but the diagnostic flag fires so mart
owners can decide whether to trust the pick or suppress the row.

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

≥ 70%. If `bridge_status = 'matched'` / `count(*)` drops below that
threshold on the Stripe side, the tier set needs retuning before
`fct_revenue` is trusted for reporting. Escalate to David before
shipping a mart that depends on `contact_sk` resolution at lower
rates.

{% enddocs %}
