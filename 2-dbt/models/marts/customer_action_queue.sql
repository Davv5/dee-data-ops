{{
    config(
        materialized = 'table',
    )
}}

-- Unified customer action queue.
--
-- Grain: one row per customer x action surface. This is the bridge between
-- dashboard truth and a future action-handler agent: one table says who needs
-- attention, why, how much money is at stake, where the evidence came from,
-- and whether a human has already handled it.

with

revenue_buyers as (

    select * from {{ ref('revenue_funnel_detail') }}

),

retention_customers as (

    select *
    from {{ ref('customer_retention_detail') }}
    qualify row_number() over (
        partition by contact_sk
        order by activity_month desc
    ) = 1

),

contract_evidence as (

    select * from {{ ref('collection_contract_evidence_detail') }}

),

latest_reviews as (

    select
        area,
        queue_name,
        coalesce(contact_sk, entity_id)                                as contact_sk,
        action_bucket,
        lower(review_status)                                           as review_status,
        review_note,
        reviewed_by,
        reviewed_at,
        expires_at
    from {{ source('marts_app', 'operator_action_reviews') }}
    where entity_type = 'customer'
        and lower(review_status) in ('open', 'reviewed', 'fixed', 'wont_fix')
    qualify row_number() over (
        partition by
            area,
            queue_name,
            coalesce(contact_sk, entity_id),
            action_bucket
        order by reviewed_at desc
    ) = 1

),

latest_contract_terms_reviews as (

    select
        contact_sk,
        review_status,
        promised_contract_value,
        upfront_agreed_amount,
        balance_expected_amount,
        review_confidence,
        terms_source_note,
        reviewed_by,
        reviewed_at
    from {{ source('marts_app', 'contract_terms_reviews') }}
    where review_status = 'confirmed'
    qualify row_number() over (
        partition by contact_sk
        order by reviewed_at desc
    ) = 1

),

revenue_actions as (

    select
        revenue_buyers.contact_sk,
        revenue_buyers.contact_id,
        coalesce(
            nullif(revenue_buyers.contact_name, ''),
            nullif(revenue_buyers.email_norm, ''),
            nullif(revenue_buyers.phone, ''),
            'Unknown buyer'
        )                                                           as customer_display_name,
        revenue_buyers.email_norm,
        revenue_buyers.phone,

        'revenue'                                                    as action_area,
        'revenue_action_queue'                                       as queue_name,
        case
            when revenue_buyers.revenue_funnel_quality_flag = 'negative_net_revenue'
                then 'data_risk'
            when revenue_buyers.revenue_funnel_quality_flag in (
                    'missing_taxonomy',
                    'uncategorized_offer_type',
                    'contact_not_matched',
                    'no_known_magnet'
                )
                then 'data_risk'
            when revenue_buyers.top_product_family = 'Unknown / historical Stripe'
                then 'product_cleanup'
            when revenue_buyers.credited_closer_source = 'unassigned'
                or revenue_buyers.credited_closer_confidence in ('low', 'missing')
                then 'attribution_gap'
            when revenue_buyers.payment_plan_truth_status in (
                    'fanbasis_auto_renew_cash_only',
                    'name_inferred_plan_cash_only'
                )
                then 'payment_plan_review'
            else 'open_customer'
        end                                                         as action_bucket,
        case
            when revenue_buyers.revenue_funnel_quality_flag in (
                    'negative_net_revenue',
                    'missing_taxonomy',
                    'uncategorized_offer_type',
                    'contact_not_matched',
                    'no_known_magnet'
                )
                then 'Data risk'
            when revenue_buyers.top_product_family = 'Unknown / historical Stripe'
                then 'Product cleanup'
            when revenue_buyers.credited_closer_source = 'unassigned'
                or revenue_buyers.credited_closer_confidence in ('low', 'missing')
                then 'Attribution gap'
            when revenue_buyers.payment_plan_truth_status in (
                    'fanbasis_auto_renew_cash_only',
                    'name_inferred_plan_cash_only'
                )
                then 'Payment-plan review'
            else 'Open customer'
        end                                                         as action_label,
        case
            when revenue_buyers.revenue_funnel_quality_flag = 'negative_net_revenue'
                then 'Negative net revenue'
            when revenue_buyers.revenue_funnel_quality_flag = 'contact_not_matched'
                then 'Buyer is not matched to a clean GHL contact'
            when revenue_buyers.revenue_funnel_quality_flag = 'missing_taxonomy'
                then 'Lead magnet taxonomy is missing'
            when revenue_buyers.revenue_funnel_quality_flag = 'uncategorized_offer_type'
                then 'Lead magnet offer type is uncategorized'
            when revenue_buyers.revenue_funnel_quality_flag = 'no_known_magnet'
                then 'No known magnet before purchase'
            when revenue_buyers.top_product_family = 'Unknown / historical Stripe'
                then 'Unknown historical Stripe product'
            when revenue_buyers.credited_closer_source = 'unassigned'
                then 'No revenue credit assigned'
            when revenue_buyers.credited_closer_confidence in ('low', 'missing')
                then 'Low-confidence revenue credit'
            when revenue_buyers.payment_plan_truth_status in (
                    'fanbasis_auto_renew_cash_only',
                    'name_inferred_plan_cash_only'
                )
                then 'Payment plan truth needs review'
            else 'Open customer'
        end                                                         as action_reason,
        case
            when revenue_buyers.revenue_funnel_quality_flag = 'negative_net_revenue'
                then 1
            when revenue_buyers.revenue_funnel_quality_flag in (
                    'missing_taxonomy',
                    'uncategorized_offer_type',
                    'contact_not_matched',
                    'no_known_magnet'
                )
                then 2
            when revenue_buyers.top_product_family = 'Unknown / historical Stripe'
                then 3
            when revenue_buyers.credited_closer_source = 'unassigned'
                then 4
            when revenue_buyers.credited_closer_confidence in ('low', 'missing')
                then 5
            when revenue_buyers.payment_plan_truth_status in (
                    'fanbasis_auto_renew_cash_only',
                    'name_inferred_plan_cash_only'
                )
                then 6
            else 9
        end                                                         as priority_rank,
        'audit_first'                                                as recommended_channel,
        'Audit first'                                                as recommended_channel_label,
        'revenue_funnel_detail'                                      as source_table,
        cast(revenue_buyers.contact_sk as string)                    as source_record_id,
        revenue_buyers.first_purchase_at                             as source_event_at,
        revenue_buyers.total_net_revenue_after_refunds               as money_at_stake,

        revenue_buyers.top_product_by_net_revenue,
        revenue_buyers.top_product_family,
        revenue_buyers.latest_prior_lead_magnet_name,
        revenue_buyers.latest_prior_lead_magnet_offer_type,
        revenue_buyers.credited_closer_name                          as revenue_credit_name,
        revenue_buyers.credited_closer_source                        as revenue_credit_source,
        revenue_buyers.credited_closer_confidence                    as revenue_credit_confidence,
        revenue_buyers.credited_setter_name,
        revenue_buyers.credited_setter_source,
        'Unknown'                                                    as current_owner_name,
        'not_modeled_yet'                                            as current_owner_source,

        revenue_buyers.revenue_funnel_quality_flag,
        cast(null as string)                                         as retention_quality_flag,
        cast(null as string)                                         as payment_plan_health_status,
        cast(null as string)                                         as collection_health_status,
        revenue_buyers.payment_plan_truth_status,
        revenue_buyers.pre_purchase_funnel_path,
        cast(null as string)                                         as contract_evidence_status,
        cast(null as numeric)                                        as promised_contract_value,
        cast(null as numeric)                                        as upfront_agreed_amount,
        cast(null as numeric)                                        as balance_expected_amount,
        cast(null as string)                                         as review_confidence,
        false                                                        as has_confirmed_contract_terms,
        cast(null as timestamp)                                      as confirmed_contract_terms_at

    from revenue_buyers
    where revenue_buyers.revenue_funnel_quality_flag != 'clean'
        or revenue_buyers.credited_closer_source = 'unassigned'
        or revenue_buyers.credited_closer_confidence in ('low', 'missing')
        or revenue_buyers.top_product_family = 'Unknown / historical Stripe'
        or revenue_buyers.payment_plan_truth_status in (
            'fanbasis_auto_renew_cash_only',
            'name_inferred_plan_cash_only'
        )

),

retention_actions as (

    select
        retention_customers.contact_sk,
        retention_customers.contact_id,
        coalesce(
            nullif(retention_customers.contact_name, ''),
            nullif(retention_customers.email_norm, ''),
            nullif(retention_customers.phone, ''),
            'Unknown customer'
        )                                                           as customer_display_name,
        retention_customers.email_norm,
        retention_customers.phone,

        'retention'                                                  as action_area,
        'retention_worklist'                                         as queue_name,
        case
            when retention_customers.retention_operator_next_action in (
                    'review_manual_collection',
                    'confirm_repeat_or_upsell',
                    'monitor_manual_collection'
                )
                then retention_customers.collection_health_status
            else retention_customers.payment_plan_health_status
        end                                                         as action_bucket,
        case
            when retention_customers.retention_operator_next_action in (
                    'review_manual_collection',
                    'confirm_repeat_or_upsell',
                    'monitor_manual_collection'
                )
                then case retention_customers.collection_health_status
                    when 'manual_collection_stale_review'
                        then 'Manual collection stale'
                    when 'collection_call_no_payment_review'
                        then 'Collection call, no pay'
                    when 'plan_named_collection_review'
                        then 'Plan-named cash only'
                    when 'repeat_or_upsell_review'
                        then 'Repeat or upsell review'
                    when 'manual_collection_recently_collected'
                        then 'Manual collection current'
                    else initcap(replace(retention_customers.collection_health_status, '_', ' '))
                end
            when retention_customers.payment_plan_health_status = 'failed_plan_recovery_needed'
                then 'Failed plan recovery'
            when retention_customers.payment_plan_health_status = 'active_plan_due_no_payment_yet'
                then 'Active plan due, no payment'
            when retention_customers.payment_plan_health_status = 'active_plan_not_yet_due'
                then 'Active plan not yet due'
            when retention_customers.payment_plan_health_status = 'completed_plan_paid_off'
                then 'Completed / paid off'
            when retention_customers.payment_plan_health_status = 'repeat_payment_observed'
                then 'Repeat payment observed'
            when retention_customers.payment_plan_health_status = 'one_time_upsell_candidate'
                then 'One-time upsell candidate'
            when retention_customers.payment_plan_health_status = 'historical_stripe_product_review'
                then 'Historical Stripe product repair'
            when retention_customers.payment_plan_health_status = 'review_negative_value'
                then 'Review negative value'
            else initcap(replace(retention_customers.payment_plan_health_status, '_', ' '))
        end                                                         as action_label,
        case retention_customers.retention_operator_next_action
            when 'recover_failed_payment' then 'Recover failed payment'
            when 'collect_due_payment' then 'Collect due payment'
            when 'watch_next_due_date' then 'Watch next due date'
            when 'monitor_active_plan' then 'Monitor active plan'
            when 'upsell_completed_customer' then 'Upsell completed customer'
            when 'upsell_one_time_customer' then 'Upsell one-time customer'
            when 'repair_historical_product' then 'Repair historical product'
            when 'review_refund_or_chargeback' then 'Review refund / chargeback'
            when 'review_manual_collection' then 'Review manual collection'
            when 'confirm_repeat_or_upsell' then 'Confirm repeat / upsell'
            when 'monitor_manual_collection' then 'Monitor manual collection'
            when 'monitor_repeat_customer' then 'Monitor repeat customer'
            else 'Monitor'
        end                                                         as action_reason,
        case
            when retention_customers.payment_plan_health_status = 'failed_plan_recovery_needed'
                then 1
            when retention_customers.payment_plan_health_status = 'active_plan_due_no_payment_yet'
                then 2
            when retention_customers.payment_plan_health_status = 'review_negative_value'
                then 3
            when retention_customers.payment_plan_health_status = 'historical_stripe_product_review'
                then 4
            when retention_customers.payment_plan_health_status = 'completed_plan_paid_off'
                then 5
            when retention_customers.payment_plan_health_status = 'one_time_upsell_candidate'
                then 6
            when retention_customers.collection_health_status in (
                    'manual_collection_stale_review',
                    'collection_call_no_payment_review',
                    'plan_named_collection_review',
                    'repeat_or_upsell_review'
                )
                then 6
            when retention_customers.payment_plan_health_status = 'active_plan_not_yet_due'
                then 7
            when retention_customers.payment_plan_health_status = 'repeat_payment_observed'
                then 8
            else 9
        end                                                         as priority_rank,
        case
            when retention_customers.retention_operator_next_action in (
                    'recover_failed_payment',
                    'collect_due_payment',
                    'upsell_completed_customer',
                    'upsell_one_time_customer'
                )
                and retention_customers.phone is not null
                then 'call_text'
            when retention_customers.retention_operator_next_action in (
                    'recover_failed_payment',
                    'collect_due_payment',
                    'upsell_completed_customer',
                    'upsell_one_time_customer'
                )
                and retention_customers.email_norm is not null
                then 'email'
            when retention_customers.retention_operator_next_action in (
                    'recover_failed_payment',
                    'collect_due_payment',
                    'upsell_completed_customer',
                    'upsell_one_time_customer'
                )
                then 'missing_contact_route'
            when retention_customers.retention_operator_next_action in (
                    'watch_next_due_date',
                    'monitor_active_plan',
                    'monitor_manual_collection',
                    'monitor_repeat_customer'
                )
                then 'monitor'
            else 'audit_first'
        end                                                         as recommended_channel,
        case
            when retention_customers.retention_operator_next_action in (
                    'recover_failed_payment',
                    'collect_due_payment',
                    'upsell_completed_customer',
                    'upsell_one_time_customer'
                )
                and retention_customers.phone is not null
                then 'Call + text'
            when retention_customers.retention_operator_next_action in (
                    'recover_failed_payment',
                    'collect_due_payment',
                    'upsell_completed_customer',
                    'upsell_one_time_customer'
                )
                and retention_customers.email_norm is not null
                then 'Email'
            when retention_customers.retention_operator_next_action in (
                    'watch_next_due_date',
                    'monitor_active_plan',
                    'monitor_manual_collection',
                    'monitor_repeat_customer'
                )
                then 'Monitor'
            when retention_customers.retention_operator_next_action in (
                    'recover_failed_payment',
                    'collect_due_payment',
                    'upsell_completed_customer',
                    'upsell_one_time_customer'
                )
                then 'Missing contact route'
            else 'Audit first'
        end                                                         as recommended_channel_label,
        'customer_retention_detail'                                  as source_table,
        retention_customers.customer_retention_sk                    as source_record_id,
        retention_customers.latest_purchase_at                       as source_event_at,
        retention_customers.lifetime_net_revenue_after_refunds       as money_at_stake,

        retention_customers.top_product_by_net_revenue,
        retention_customers.top_product_family,
        retention_customers.latest_prior_lead_magnet_name,
        retention_customers.latest_prior_lead_magnet_offer_type,
        retention_customers.credited_closer_name                     as revenue_credit_name,
        retention_customers.credited_closer_source                   as revenue_credit_source,
        retention_customers.credited_closer_confidence               as revenue_credit_confidence,
        retention_customers.credited_setter_name,
        retention_customers.credited_setter_source,
        'Unknown'                                                    as current_owner_name,
        'not_modeled_yet'                                            as current_owner_source,

        retention_customers.revenue_funnel_quality_flag,
        retention_customers.retention_quality_flag,
        retention_customers.payment_plan_health_status,
        retention_customers.collection_health_status,
        retention_customers.payment_plan_truth_status,
        retention_customers.pre_purchase_funnel_path,
        cast(null as string)                                         as contract_evidence_status,
        cast(null as numeric)                                        as promised_contract_value,
        cast(null as numeric)                                        as upfront_agreed_amount,
        cast(null as numeric)                                        as balance_expected_amount,
        cast(null as string)                                         as review_confidence,
        false                                                        as has_confirmed_contract_terms,
        cast(null as timestamp)                                      as confirmed_contract_terms_at

    from retention_customers
    where retention_customers.payment_plan_health_status in (
            'failed_plan_recovery_needed',
            'active_plan_due_no_payment_yet',
            'one_time_upsell_candidate',
            'completed_plan_paid_off',
            'historical_stripe_product_review',
            'active_plan_not_yet_due',
            'repeat_payment_observed',
            'review_negative_value'
        )
        or retention_customers.collection_health_status in (
            'manual_collection_stale_review',
            'collection_call_no_payment_review',
            'plan_named_collection_review',
            'repeat_or_upsell_review'
        )

),

contract_terms_actions as (

    select
        contract_evidence.contact_sk,
        contract_evidence.contact_id,
        coalesce(
            nullif(contract_evidence.contact_name, ''),
            nullif(contract_evidence.email_norm, ''),
            nullif(contract_evidence.phone, ''),
            'Unknown customer'
        )                                                           as customer_display_name,
        contract_evidence.email_norm,
        contract_evidence.phone,

        'contract_terms'                                             as action_area,
        'retention_worklist'                                         as queue_name,
        'contract_terms_review'                                      as action_bucket,
        'Contract terms review'                                      as action_label,
        'Confirm promised value, upfront amount, expected balance, and confidence from transcript evidence'
                                                                        as action_reason,
        6                                                            as priority_rank,
        'audit_first'                                                as recommended_channel,
        'Audit first'                                                as recommended_channel_label,
        'collection_contract_evidence_detail'                        as source_table,
        contract_evidence.collection_contract_evidence_sk            as source_record_id,
        contract_evidence.latest_payment_terms_call_at               as source_event_at,
        contract_evidence.lifetime_net_revenue_after_refunds         as money_at_stake,

        contract_evidence.top_product_by_net_revenue,
        contract_evidence.top_product_family,
        contract_evidence.latest_prior_lead_magnet_name,
        contract_evidence.latest_prior_lead_magnet_offer_type,
        contract_evidence.credited_closer_name                       as revenue_credit_name,
        contract_evidence.credited_closer_source                     as revenue_credit_source,
        contract_evidence.credited_closer_confidence                 as revenue_credit_confidence,
        contract_evidence.credited_setter_name,
        contract_evidence.credited_setter_source,
        'Unknown'                                                    as current_owner_name,
        'not_modeled_yet'                                            as current_owner_source,

        cast(null as string)                                         as revenue_funnel_quality_flag,
        cast(null as string)                                         as retention_quality_flag,
        cast(null as string)                                         as payment_plan_health_status,
        contract_evidence.collection_health_status,
        cast(null as string)                                         as payment_plan_truth_status,
        cast(null as string)                                         as pre_purchase_funnel_path,
        contract_evidence.contract_evidence_status,
        latest_contract_terms_reviews.promised_contract_value,
        latest_contract_terms_reviews.upfront_agreed_amount,
        latest_contract_terms_reviews.balance_expected_amount,
        latest_contract_terms_reviews.review_confidence,
        latest_contract_terms_reviews.contact_sk is not null         as has_confirmed_contract_terms,
        latest_contract_terms_reviews.reviewed_at                    as confirmed_contract_terms_at

    from contract_evidence
    left join latest_contract_terms_reviews
        on contract_evidence.contact_sk = latest_contract_terms_reviews.contact_sk
    where contract_evidence.contract_evidence_status = 'transcript_payment_terms_found'

),

action_candidates as (

    select * from revenue_actions
    union all
    select * from retention_actions
    union all
    select * from contract_terms_actions

),

reviewed_actions as (

    select
        action_candidates.*,
        latest_reviews.review_status                                 as ledger_review_status,
        latest_reviews.review_note,
        latest_reviews.reviewed_by,
        latest_reviews.reviewed_at,
        latest_reviews.expires_at
    from action_candidates
    left join latest_reviews
        on action_candidates.action_area = latest_reviews.area
       and action_candidates.queue_name = latest_reviews.queue_name
       and action_candidates.contact_sk = latest_reviews.contact_sk
       and action_candidates.action_bucket = latest_reviews.action_bucket

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'reviewed_actions.contact_sk',
            'reviewed_actions.action_area',
            'reviewed_actions.queue_name',
            'reviewed_actions.action_bucket'
        ]) }}                                                        as customer_action_id,

        reviewed_actions.contact_sk,
        reviewed_actions.contact_id,
        reviewed_actions.customer_display_name,
        reviewed_actions.email_norm,
        reviewed_actions.phone,
        reviewed_actions.phone is not null                           as has_phone,
        reviewed_actions.email_norm is not null                      as has_email,

        reviewed_actions.action_area,
        reviewed_actions.queue_name,
        reviewed_actions.action_bucket,
        reviewed_actions.action_label,
        reviewed_actions.action_reason,
        reviewed_actions.priority_rank,
        case
            when reviewed_actions.priority_rank <= 2 then 'high'
            when reviewed_actions.priority_rank <= 6 then 'medium'
            else 'low'
        end                                                         as priority_label,
        reviewed_actions.recommended_channel,
        reviewed_actions.recommended_channel_label,

        reviewed_actions.source_table,
        reviewed_actions.source_record_id,
        reviewed_actions.source_event_at,
        date(reviewed_actions.source_event_at, 'America/New_York')    as source_event_date,
        reviewed_actions.money_at_stake,

        reviewed_actions.top_product_by_net_revenue,
        reviewed_actions.top_product_family,
        reviewed_actions.latest_prior_lead_magnet_name,
        reviewed_actions.latest_prior_lead_magnet_offer_type,
        reviewed_actions.revenue_credit_name,
        reviewed_actions.revenue_credit_source,
        reviewed_actions.revenue_credit_confidence,
        reviewed_actions.credited_setter_name,
        reviewed_actions.credited_setter_source,
        reviewed_actions.current_owner_name,
        reviewed_actions.current_owner_source,

        reviewed_actions.revenue_funnel_quality_flag,
        reviewed_actions.retention_quality_flag,
        reviewed_actions.payment_plan_health_status,
        reviewed_actions.collection_health_status,
        reviewed_actions.payment_plan_truth_status,
        reviewed_actions.pre_purchase_funnel_path,
        reviewed_actions.contract_evidence_status,
        reviewed_actions.promised_contract_value,
        reviewed_actions.upfront_agreed_amount,
        reviewed_actions.balance_expected_amount,
        reviewed_actions.review_confidence,
        reviewed_actions.has_confirmed_contract_terms,
        reviewed_actions.confirmed_contract_terms_at,

        case
            when reviewed_actions.has_confirmed_contract_terms
                then 'fixed'
            else coalesce(reviewed_actions.ledger_review_status, 'open')
        end                                                         as review_status,
        reviewed_actions.review_note,
        reviewed_actions.reviewed_by,
        reviewed_actions.reviewed_at,
        reviewed_actions.expires_at,
        case
            when reviewed_actions.has_confirmed_contract_terms
                then false
            when reviewed_actions.ledger_review_status in ('fixed', 'wont_fix')
                and (
                    reviewed_actions.expires_at is null
                    or reviewed_actions.expires_at > current_timestamp()
                )
                then false
            else true
        end                                                         as is_action_open,
        current_timestamp()                                          as mart_refreshed_at

    from reviewed_actions

)

select * from final
