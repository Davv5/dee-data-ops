-- Billing-email direct is a payment-only identity fallback, not a CRM contact
-- match. This test protects the dashboard health contract from drifting back
-- to the old "matched with NULL contact_sk" shape.

select
    source_platform,
    payment_id,
    contact_sk,
    match_method,
    bridge_status
from {{ ref('bridge_identity_contact_payment') }}
where (
        match_method = 'billing_email_direct'
        and (
            bridge_status != 'payment_identity_only'
            or contact_sk is not null
        )
    )
    or (
        bridge_status = 'payment_identity_only'
        and (
            match_method != 'billing_email_direct'
            or contact_sk is not null
        )
    )
