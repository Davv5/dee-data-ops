-- Deterministic, tiered email/phone match between payments (Stripe +
-- Fanbasis) and `dim_contacts`. One row per `(source_platform,
-- payment_id)` — best-match picked via `qualify row_number() over
-- (partition by source_platform, payment_id order by match_score desc)`.
-- Tiers and scores (per the corpus audit gap-fix brief):
--     1. email_exact           — score 1.00
--     2. email_canonical       — score 0.95  (gmail dot/plus normalized)
--     3. phone_last10          — score 1.00
--     4. billing_email_direct  — score 0.80  (payment-only fallback)
--     5. unmatched             — score 0.00  (bridge_status = 'unmatched')
-- Keeping the bridge payment-centric (not contact-centric) means every
-- charge / Fanbasis transaction gets exactly one row here; `fct_revenue`
-- left-joins on `(source_platform, payment_id)` to pick up `contact_sk`
-- + bridge metadata. Stripe is historical-only at D-DEE per memory
-- `project_stripe_historical_only.md`; Fanbasis is the live-going arm.

with

stripe_payments as (

    select
        charge_id                                                 as payment_id,
        'stripe'                                                  as source_platform,
        lower(trim(billing_email))                                as email_norm,
        regexp_replace(coalesce(billing_phone, ''), r'[^0-9]', '') as phone_digits
    from {{ ref('stg_stripe__charges') }}
    where charge_id is not null

),

fanbasis_payments as (

    select
        payment_id                                                as payment_id,
        'fanbasis'                                                as source_platform,
        lower(trim(fan_email))                                    as email_norm,
        regexp_replace(coalesce(fan_phone, ''), r'[^0-9]', '')    as phone_digits
    from {{ ref('stg_fanbasis__transactions') }}
    where payment_id is not null

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

        case
            when length(payments.phone_digits) >= 10
            then right(payments.phone_digits, 10)
            else null
        end                                                       as phone_last10

    from payments

),

contacts as (

    select
        contact_sk,
        contact_id,
        location_id,
        email_norm                                                as contact_email_norm,
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

-- Tier 4: billing_email_direct — payment has a billing_email but no CRM
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

-- Tier 5: unmatched — payment has no email, no phone, and no CRM match.
tier_unmatched as (

    select
        payments_canonicalized.source_platform,
        payments_canonicalized.payment_id,
        cast(null as string)                                      as contact_sk,
        'unmatched'                                               as match_method,
        0.00                                                      as match_score
    from payments_canonicalized
    left join contacts_canonicalized
        on payments_canonicalized.email_canonical
           = contacts_canonicalized.contact_email_canonical
        or payments_canonicalized.phone_last10
           = contacts_canonicalized.contact_phone_last10
    where contacts_canonicalized.contact_sk is null
        and (
            payments_canonicalized.email_norm is null
            or payments_canonicalized.email_norm = ''
        )
        and (
            payments_canonicalized.phone_last10 is null
        )

),

all_tiers as (

    select * from tier_email_exact
    union all
    select * from tier_email_canonical
    union all
    select * from tier_phone_last10
    union all
    select * from tier_billing_email_direct
    union all
    select * from tier_unmatched

),

-- Detect ambiguous multi-candidate: same payment matched to > 1 distinct
-- contact at the *same* highest score.
candidate_counts as (

    select
        source_platform,
        payment_id,
        max(match_score)                                          as best_score,
        count(distinct contact_sk)                                as distinct_candidate_count
    from all_tiers
    where match_method != 'unmatched'
        and match_method != 'billing_email_direct'
    group by 1, 2

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
        order by all_tiers.match_score desc
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
