-- Failed/unpaid Stripe attempts may exist in the fact for auditability, but
-- they must never contribute revenue to downstream marts.

select
    payment_id,
    gross_amount,
    net_amount,
    source_presentment_net_amount,
    is_paid
from {{ ref('fct_payments') }}
where source_platform = 'stripe'
  and not coalesce(is_paid, false)
  and (
      coalesce(gross_amount, 0) != 0
      or coalesce(net_amount, 0) != 0
      or coalesce(source_presentment_net_amount, 0) != 0
  )
