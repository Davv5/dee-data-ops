-- Deterministic, tiered email/phone match between payments (Stripe +
-- Fanbasis) and `dim_contacts`. One row per `(source_platform,
-- payment_id)` — best-match picked via `qualify row_number() over
-- (partition by source_platform, payment_id order by match_score desc)`.
-- Tiers and scores (per the corpus audit gap-fix brief):
--     1. email_exact           — score 1.00
--     2. email_canonical       — score 0.95  (gmail dot/plus normalized)
--     3. phone_last10          — score 1.00
--     4. stripe_customer_email — score 0.90  (Stripe customer object fallback)
--     5. stripe_customer_phone — score 0.90  (Stripe customer object fallback)
--     6. fanbasis_conversation_email — score 0.88
--     7. fanbasis_unique_crm_name    — score 0.82
--     8. billing_email_direct        — score 0.80  (payment-only fallback;
--        bridge_status = payment_identity_only because no CRM contact exists)
--     9. unmatched                   — score 0.00
-- Keeping the bridge payment-centric (not contact-centric) means every
-- charge / Fanbasis transaction gets exactly one row here; `fct_payments`
-- left-joins on `(source_platform, payment_id)` to pick up `contact_sk`
-- + bridge metadata. Stripe is historical-only at D-DEE per memory
-- `project_stripe_historical_only.md`; Fanbasis is the live-going arm.

with

stripe_payments as (

    select
        charges.charge_id                                         as payment_id,
        'stripe'                                                  as source_platform,
        nullif(lower(trim(charges.billing_email)), '')            as email_norm,
        regexp_replace(coalesce(charges.billing_phone, ''), r'[^0-9]', '') as phone_digits,
        nullif(lower(trim(stripe_customers.email)), '')           as fallback_email_norm,
        regexp_replace(coalesce(stripe_customers.phone, ''), r'[^0-9]', '') as fallback_phone_digits,
        cast(null as string)                                      as fan_name_norm
    from {{ ref('stg_stripe__charges') }} as charges
    left join {{ ref('stg_stripe__customers') }} as stripe_customers
        on charges.customer_id = stripe_customers.customer_id
    where charges.charge_id is not null

),

fanbasis_subscribers as (

    select
        customer_id,
        nullif(customer_email_norm, '')                            as customer_email_norm,
        customer_phone_digits,
        subscription_updated_at
    from {{ ref('stg_fanbasis__subscribers') }}
    where customer_id is not null
    qualify row_number() over (
        partition by customer_id
        order by subscription_updated_at desc nulls last, subscriber_id
    ) = 1

),

fanbasis_customers as (

    select
        customer_email_norm,
        customer_phone_digits,
        last_transaction_at
    from {{ ref('stg_fanbasis__customers') }}
    where customer_email_norm is not null
    qualify row_number() over (
        partition by customer_email_norm
        order by last_transaction_at desc nulls last, customer_id
    ) = 1

),

fanbasis_payments as (

    select
        fanbasis_transactions.payment_id,
        'fanbasis'                                                as source_platform,
        coalesce(
            nullif(lower(trim(fanbasis_transactions.fan_email)), ''),
            fanbasis_subscribers.customer_email_norm,
            fanbasis_customers.customer_email_norm
        )                                                         as email_norm,
        coalesce(
            nullif(regexp_replace(coalesce(fanbasis_transactions.fan_phone, ''), r'[^0-9]', ''), ''),
            nullif(fanbasis_subscribers.customer_phone_digits, ''),
            nullif(fanbasis_customers.customer_phone_digits, ''),
            ''
        )                                                         as phone_digits,
        cast(null as string)                                      as fallback_email_norm,
        cast(null as string)                                      as fallback_phone_digits,
        nullif(
            regexp_replace(
                regexp_replace(
                    lower(trim(fanbasis_transactions.fan_name)),
                    r'[^a-z0-9 ]',
                    ''
                ),
                r'\s+',
                ' '
            ),
            ''
        )                                                         as fan_name_norm
    from {{ ref('stg_fanbasis__transactions') }} as fanbasis_transactions
    left join fanbasis_subscribers
        on fanbasis_transactions.fan_id = fanbasis_subscribers.customer_id
    left join fanbasis_customers
        on lower(trim(fanbasis_transactions.fan_email))
           = fanbasis_customers.customer_email_norm
    where fanbasis_transactions.payment_id is not null

),

payments as (

    select * from stripe_payments
    union all
    select * from fanbasis_payments

),

payments_canonicalized as (

    select
        payments.*,

        -- Gmail-canonical email: drop `+tag`, drop dots in local part
        -- when domain is gmail.com / googlemail.com.
        case
            when payments.email_norm is null then null
            when split(payments.email_norm, '@')[safe_offset(1)] in (
                'gmail.com', 'googlemail.com'
            )
            then concat(
                replace(
                    split(
                        split(payments.email_norm, '@')[safe_offset(0)],
                        '+'
                    )[safe_offset(0)],
                    '.', ''
                ),
                '@gmail.com'
            )
            else payments.email_norm
        end                                                       as email_canonical,

        -- Stripe-only fallback identity from `stg_stripe__customers`.
        -- These fields rescue historical Stripe charges where charge
        -- billing details are incomplete but the customer object carries
        -- the same buyer identity.
        case
            when payments.fallback_email_norm is null then null
            when split(payments.fallback_email_norm, '@')[safe_offset(1)] in (
                'gmail.com', 'googlemail.com'
            )
            then concat(
                replace(
                    split(
                        split(payments.fallback_email_norm, '@')[safe_offset(0)],
                        '+'
                    )[safe_offset(0)],
                    '.', ''
                ),
                '@gmail.com'
            )
            else payments.fallback_email_norm
        end                                                       as fallback_email_canonical,

        case
            when length(payments.phone_digits) >= 10
            then right(payments.phone_digits, 10)
            else null
        end                                                       as phone_last10,

        case
            when length(payments.fallback_phone_digits) >= 10
            then right(payments.fallback_phone_digits, 10)
            else null
        end                                                       as fallback_phone_last10

    from payments

),

contacts as (

    select
        contact_sk,
        contact_id,
        location_id,
        nullif(
            regexp_replace(
                regexp_replace(
                    lower(trim(
                        coalesce(
                            contact_name,
                            concat(
                                coalesce(first_name, ''),
                                ' ',
                                coalesce(last_name, '')
                            )
                        )
                    )),
                    r'[^a-z0-9 ]',
                    ''
                ),
                r'\s+',
                ' '
            ),
            ''
        )                                                         as contact_name_norm,
        nullif(email_norm, '')                                    as contact_email_norm,
        regexp_replace(coalesce(phone, ''), r'[^0-9]', '')        as contact_phone_digits
    from {{ ref('dim_contacts') }}

),

contacts_canonicalized as (

    select
        contacts.*,

        case
            when contacts.contact_email_norm is null then null
            when split(contacts.contact_email_norm, '@')[safe_offset(1)] in (
                'gmail.com', 'googlemail.com'
            )
            then concat(
                replace(
                    split(
                        split(
                            contacts.contact_email_norm,
                            '@'
                        )[safe_offset(0)],
                        '+'
                    )[safe_offset(0)],
                    '.', ''
                ),
                '@gmail.com'
            )
            else contacts.contact_email_norm
        end                                                       as contact_email_canonical,

        case
            when length(contacts.contact_phone_digits) >= 10
            then right(contacts.contact_phone_digits, 10)
            else null
        end                                                       as contact_phone_last10

    from contacts

),

conversation_contacts as (

    select distinct
        contacts_canonicalized.contact_sk,
        nullif(lower(trim(conversations.contact_email)), '')       as conversation_email_norm
    from {{ ref('stg_ghl__conversations') }} as conversations
    inner join contacts_canonicalized
        on conversations.contact_id = contacts_canonicalized.contact_id
       and conversations.location_id = contacts_canonicalized.location_id
    where conversations.contact_email is not null
        and conversations.contact_id is not null
        and conversations.location_id is not null

),

conversation_contacts_canonicalized as (

    select
        conversation_contacts.*,
        case
            when conversation_contacts.conversation_email_norm is null then null
            when split(conversation_contacts.conversation_email_norm, '@')[safe_offset(1)] in (
                'gmail.com', 'googlemail.com'
            )
            then concat(
                replace(
                    split(
                        split(
                            conversation_contacts.conversation_email_norm,
                            '@'
                        )[safe_offset(0)],
                        '+'
                    )[safe_offset(0)],
                    '.',
                    ''
                ),
                '@gmail.com'
            )
            else conversation_contacts.conversation_email_norm
        end                                                       as conversation_email_canonical
    from conversation_contacts

),

conversation_email_candidate_contacts as (

    select distinct
        conversation_email_norm,
        conversation_email_canonical,
        contact_sk
    from conversation_contacts_canonicalized
    where conversation_email_norm is not null

),

conversation_email_candidates as (

    select
        conversation_email_norm,
        conversation_email_canonical,
        contact_sk
    from (
        select
            conversation_email_candidate_contacts.*,
            count(*) over (
                partition by
                    conversation_email_norm,
                    conversation_email_canonical
            )                                                       as candidate_count
        from conversation_email_candidate_contacts
    )
    where candidate_count = 1

),

crm_name_candidate_contacts as (

    select distinct
        contact_name_norm,
        contact_sk
    from contacts_canonicalized
    where contact_name_norm is not null
        and regexp_contains(contact_name_norm, r' ')
        and length(contact_name_norm) >= 6

),

crm_name_candidates as (

    select
        contact_name_norm,
        contact_sk
    from (
        select
            crm_name_candidate_contacts.*,
            count(*) over (
                partition by contact_name_norm
            )                                                       as candidate_count
        from crm_name_candidate_contacts
    )
    where candidate_count = 1

),

-- Tier 1: email_exact
tier_email_exact as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        contacts_canonicalized.contact_sk,
        'email_exact'                                             as match_method,
        1.00                                                      as match_score
    from payments_canonicalized
    inner join contacts_canonicalized
        on payments_canonicalized.email_norm
           = contacts_canonicalized.contact_email_norm
    where payments_canonicalized.email_norm is not null

),

-- Tier 2: email_canonical (gmail dot/plus normalized)
tier_email_canonical as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        contacts_canonicalized.contact_sk,
        'email_canonical'                                         as match_method,
        0.95                                                      as match_score
    from payments_canonicalized
    inner join contacts_canonicalized
        on payments_canonicalized.email_canonical
           = contacts_canonicalized.contact_email_canonical
    where payments_canonicalized.email_canonical is not null
        and payments_canonicalized.email_norm
            != contacts_canonicalized.contact_email_norm

),

-- Tier 3: phone_last10
tier_phone_last10 as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        contacts_canonicalized.contact_sk,
        'phone_last10'                                            as match_method,
        1.00                                                      as match_score
    from payments_canonicalized
    inner join contacts_canonicalized
        on payments_canonicalized.phone_last10
           = contacts_canonicalized.contact_phone_last10
    where payments_canonicalized.phone_last10 is not null

),

-- Tier 4: stripe_customer_email — charge billing identity failed, but
-- the linked Stripe customer object has an email that resolves to CRM.
tier_stripe_customer_email as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        contacts_canonicalized.contact_sk,
        'stripe_customer_email'                                   as match_method,
        0.90                                                      as match_score
    from payments_canonicalized
    inner join contacts_canonicalized
        on payments_canonicalized.fallback_email_norm
           = contacts_canonicalized.contact_email_norm
        or payments_canonicalized.fallback_email_canonical
           = contacts_canonicalized.contact_email_canonical
    where payments_canonicalized.source_platform = 'stripe'
        and payments_canonicalized.fallback_email_norm is not null

),

-- Tier 5: stripe_customer_phone — final deterministic Stripe customer
-- fallback before falling back to payment-only email visibility.
tier_stripe_customer_phone as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        contacts_canonicalized.contact_sk,
        'stripe_customer_phone'                                   as match_method,
        0.90                                                      as match_score
    from payments_canonicalized
    inner join contacts_canonicalized
        on payments_canonicalized.fallback_phone_last10
           = contacts_canonicalized.contact_phone_last10
    where payments_canonicalized.source_platform = 'stripe'
        and payments_canonicalized.fallback_phone_last10 is not null

),

-- Tier 6: fanbasis_conversation_email — the Fanbasis purchase email
-- matches a historical GHL conversation email for exactly one contact.
-- This rescues alternate-email buyers whose current contact email differs.
tier_fanbasis_conversation_email as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        conversation_email_candidates.contact_sk,
        'fanbasis_conversation_email'                              as match_method,
        0.88                                                       as match_score
    from payments_canonicalized
    inner join conversation_email_candidates
        on payments_canonicalized.email_norm
           = conversation_email_candidates.conversation_email_norm
        or payments_canonicalized.email_canonical
           = conversation_email_candidates.conversation_email_canonical
    where payments_canonicalized.source_platform = 'fanbasis'
        and payments_canonicalized.email_norm is not null

),

-- Tier 7: fanbasis_unique_crm_name — the Fanbasis buyer name matches
-- exactly one CRM contact full name. Kept below email/phone/conversation
-- identity and above payment-only fallback because it produces a CRM
-- contact, but the source key is still weaker than email or phone.
tier_fanbasis_unique_crm_name as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        crm_name_candidates.contact_sk,
        'fanbasis_unique_crm_name'                                 as match_method,
        0.82                                                       as match_score
    from payments_canonicalized
    inner join crm_name_candidates
        on payments_canonicalized.fan_name_norm
           = crm_name_candidates.contact_name_norm
    where payments_canonicalized.source_platform = 'fanbasis'
        and payments_canonicalized.fan_name_norm is not null

),

-- Tier 8: billing_email_direct — payment has a billing_email but no CRM
-- contact exists. Emits a row with contact_sk = null so the bridge is a
-- complete payment catalog; match_score reflects the weaker grounding.
tier_billing_email_direct as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        cast(null as string)                                      as contact_sk,
        'billing_email_direct'                                    as match_method,
        0.80                                                      as match_score
    from payments_canonicalized
    left join contacts_canonicalized
        on payments_canonicalized.email_canonical
           = contacts_canonicalized.contact_email_canonical
        or payments_canonicalized.phone_last10
           = contacts_canonicalized.contact_phone_last10
    where payments_canonicalized.email_norm is not null
        and contacts_canonicalized.contact_sk is null

),

-- All matched (and billing-direct) tiers — the union that defines
-- "this payment was accounted for by a higher-confidence tier."
-- tier_unmatched below is the anti-join of payments against this set,
-- guaranteeing the docstring promise: every payment gets exactly one
-- bridge row, regardless of which non-NULL identity columns it carries.
matched_or_billing as (

    select * from tier_email_exact
    union all
    select * from tier_email_canonical
    union all
    select * from tier_phone_last10
    union all
    select * from tier_stripe_customer_email
    union all
    select * from tier_stripe_customer_phone
    union all
    select * from tier_fanbasis_conversation_email
    union all
    select * from tier_fanbasis_unique_crm_name
    union all
    select * from tier_billing_email_direct

),

-- Tier 9: unmatched — every payment that did not appear in any prior
-- tier. Anti-join shape (not per-row predicates) so structural gaps in
-- the upstream tier predicates cannot silently drop payments from the
-- bridge. Covers the no-email-no-phone case, the phone-only-no-match
-- case, and any future shape that fails to satisfy tiers 1-8.
tier_unmatched as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        cast(null as string)                                      as contact_sk,
        'unmatched'                                               as match_method,
        0.00                                                      as match_score
    from payments_canonicalized
    where not exists (
        select 1
        from matched_or_billing
        where matched_or_billing.source_platform
                  = payments_canonicalized.source_platform
          and matched_or_billing.payment_id
                  = payments_canonicalized.payment_id
    )

),

all_tiers as (

    select * from matched_or_billing
    union all
    select * from tier_unmatched

),

-- Detect ambiguous multi-candidate: same payment matched to > 1 distinct
-- contact after applying the same score + method-precedence ordering used by
-- `best_match`. This keeps true duplicate-contact ties visible without
-- flagging an email-exact winner as ambiguous merely because a lower-priority
-- phone match points at a different CRM contact.
candidate_scores as (

    select
        source_platform,
        payment_id,
        contact_sk,
        match_method,
        match_score,
        case match_method
            when 'email_exact'           then 1
            when 'phone_last10'          then 2
            when 'email_canonical'       then 3
            when 'stripe_customer_email' then 4
            when 'stripe_customer_phone' then 5
            when 'fanbasis_conversation_email' then 6
            when 'fanbasis_unique_crm_name'    then 7
            when 'billing_email_direct'        then 8
            else 9
        end                                                       as method_precedence,
        max(match_score) over (
            partition by source_platform, payment_id
        )                                                         as best_score
    from all_tiers
    where match_method != 'unmatched'
        and match_method != 'billing_email_direct'
        and contact_sk is not null

),

top_candidate_scores as (

    select *
    from candidate_scores
    where match_score = best_score

),

best_candidate_methods as (

    select
        source_platform,
        payment_id,
        best_score,
        min(method_precedence)                                    as best_method_precedence
    from top_candidate_scores
    group by 1, 2, 3

),

candidate_counts as (

    select
        top_candidate_scores.source_platform,
        top_candidate_scores.payment_id,
        top_candidate_scores.best_score,
        best_candidate_methods.best_method_precedence,
        count(distinct top_candidate_scores.contact_sk)           as distinct_candidate_count
    from top_candidate_scores
    inner join best_candidate_methods
        on top_candidate_scores.source_platform = best_candidate_methods.source_platform
       and top_candidate_scores.payment_id      = best_candidate_methods.payment_id
       and top_candidate_scores.best_score      = best_candidate_methods.best_score
       and top_candidate_scores.method_precedence
           = best_candidate_methods.best_method_precedence
    group by 1, 2, 3, 4

),

best_match as (

    select
        all_tiers.source_platform,
        all_tiers.payment_id,
        all_tiers.contact_sk,
        all_tiers.match_method,
        all_tiers.match_score
    from all_tiers
    qualify row_number() over (
        partition by all_tiers.source_platform, all_tiers.payment_id
        order by
            all_tiers.match_score desc,
            case all_tiers.match_method
                when 'email_exact'           then 1
                when 'phone_last10'          then 2
                when 'email_canonical'       then 3
                when 'stripe_customer_email' then 4
                when 'stripe_customer_phone' then 5
                when 'fanbasis_conversation_email' then 6
                when 'fanbasis_unique_crm_name'    then 7
                when 'billing_email_direct'        then 8
                else 9
            end,
            all_tiers.contact_sk
    ) = 1

),

final as (

    select
        best_match.source_platform,
        best_match.payment_id,
        best_match.contact_sk,
        best_match.match_method,
        best_match.match_score,

        case
            when best_match.match_method = 'unmatched'
                then 'unmatched'
            when best_match.match_method = 'billing_email_direct'
                then 'payment_identity_only'
            when candidate_counts.distinct_candidate_count > 1
              and best_match.match_score = candidate_counts.best_score
                then 'ambiguous_multi_candidate'
            else 'matched'
        end                                                       as bridge_status

    from best_match
    left join candidate_counts
        on best_match.source_platform = candidate_counts.source_platform
       and best_match.payment_id      = candidate_counts.payment_id

)

select * from final
