-- Grain: one row per current matched paid customer (`contact_sk`).
-- This mart answers "what did we collect, what did they buy, and did the
-- sales-call transcript mention payment terms?" without inventing remaining
-- contract value. Follows the mart-layer truth boundary from
-- "How to Create a Data Modeling Pipeline (3 Layer Approach)".

with

current_customers as (

    select * from {{ ref('customer_retention_detail') }}
    where is_current_month

),

revenue_funnel as (

    select * from {{ ref('revenue_funnel_detail') }}

),

transcript_segments as (

    select * from {{ ref('stg_fathom__transcript_segments') }}
    where segment_text is not null

),

candidate_calls as (

    select
        contact_sk,
        email_norm                                                    as buyer_email_norm,
        contact_email_fathom_call_id                                  as call_id,
        contact_email_fathom_scheduled_start_at                       as call_at,
        contact_email_fathom_recorded_by_name                         as recorded_by_name,
        contact_email_fathom_user_name                                as attributed_user_name,
        contact_email_fathom_user_role                                as attributed_user_role,
        contact_email_fathom_is_revenue_relevant                      as is_revenue_relevant,
        'buyer_email_fathom_call'                                     as candidate_call_source,
        1                                                             as candidate_call_priority
    from revenue_funnel
    where contact_email_fathom_call_id is not null

    union all

    select
        contact_sk,
        email_norm                                                    as buyer_email_norm,
        latest_booking_fathom_call_id                                 as call_id,
        latest_booking_fathom_scheduled_start_at                      as call_at,
        latest_booking_fathom_recorded_by_name                        as recorded_by_name,
        latest_booking_fathom_user_name                               as attributed_user_name,
        latest_booking_fathom_user_role                               as attributed_user_role,
        latest_booking_fathom_is_revenue_relevant                     as is_revenue_relevant,
        'latest_booking_fathom_call'                                  as candidate_call_source,
        2                                                             as candidate_call_priority
    from revenue_funnel
    where latest_booking_fathom_call_id is not null

    union all

    select
        contact_sk,
        email_norm                                                    as buyer_email_norm,
        transcript_closer_call_id                                     as call_id,
        contact_email_fathom_scheduled_start_at                       as call_at,
        contact_email_fathom_recorded_by_name                         as recorded_by_name,
        transcript_closer_user_name                                   as attributed_user_name,
        transcript_closer_user_role                                   as attributed_user_role,
        true                                                          as is_revenue_relevant,
        'transcript_closer_call'                                      as candidate_call_source,
        3                                                             as candidate_call_priority
    from revenue_funnel
    where transcript_closer_call_id is not null

),

deduped_candidate_calls as (

    select * from candidate_calls
    qualify row_number() over (
        partition by contact_sk, call_id
        order by
            candidate_call_priority,
            is_revenue_relevant desc,
            call_at desc
    ) = 1

),

ranked_candidate_calls as (

    select
        deduped_candidate_calls.*,
        row_number() over (
            partition by contact_sk
            order by
                candidate_call_priority,
                is_revenue_relevant desc,
                call_at desc,
                call_id
        )                                                           as candidate_call_sequence
    from deduped_candidate_calls

),

scoped_candidate_calls as (

    select * from ranked_candidate_calls
    where candidate_call_sequence <= 3

),

payment_terms_segments as (

    select
        scoped_candidate_calls.contact_sk,
        scoped_candidate_calls.buyer_email_norm,
        scoped_candidate_calls.call_id,
        scoped_candidate_calls.call_at,
        scoped_candidate_calls.candidate_call_source,
        scoped_candidate_calls.candidate_call_priority,
        scoped_candidate_calls.recorded_by_name,
        scoped_candidate_calls.attributed_user_name,
        scoped_candidate_calls.attributed_user_role,
        scoped_candidate_calls.is_revenue_relevant,
        transcript_segments.segment_index,
        transcript_segments.segment_offset_seconds,
        transcript_segments.speaker_name,
        transcript_segments.speaker_email_norm,
        transcript_segments.segment_text
    from scoped_candidate_calls
    inner join transcript_segments
        on scoped_candidate_calls.call_id = transcript_segments.call_id
    where regexp_contains(
            lower(transcript_segments.segment_text),
            r'(\$\s*[0-9]|\b[0-9][0-9,]*(?:\.[0-9]{1,2})?\s*(?:dollars|bucks|usd)\b|\b[0-9][0-9,]*(?:\.[0-9]{1,2})?\s+(?:payment|deposit|balance|today|down)\b)'
        )
        and regexp_contains(
            lower(transcript_segments.segment_text),
            r'(we charge|we[^a-z0-9]ll|we will|i can send|i[^a-z0-9]ll send|send (you|the).{0,40}payment|payment link|checkout|invoice|take action|decision.{0,40}call|lock in|get started with|start working with us|start with|put down|down payment|remaining balance|full payment|split|installment|payment plan|program (is|costs|cost|would be|goes)|price (is|on|for)|charge (you|today)|get the.{0,40}payment done|payment option|deposit)'
        )
        and not regexp_contains(
            lower(transcript_segments.segment_text),
            r'(\bi paid\b|paid this|already spending|spent|i.?m not going to pay|i am not going to pay|not going to pay|i don.?t pay|insurance|costs you so far)'
        )
        and (
            transcript_segments.speaker_email_norm is null
            or transcript_segments.speaker_email_norm != scoped_candidate_calls.buyer_email_norm
        )

),

payment_terms_amount_mentions as (

    select distinct
        payment_terms_segments.contact_sk,
        payment_terms_segments.call_id,
        payment_terms_segments.segment_index,
        amount_text,
        safe_cast(replace(amount_text, ',', '') as numeric)           as mentioned_amount
    from payment_terms_segments,
        unnest(array_concat(
            regexp_extract_all(
                lower(payment_terms_segments.segment_text),
                r'\$\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)'
            ),
            regexp_extract_all(
                lower(payment_terms_segments.segment_text),
                r'\b([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:dollars|bucks|usd)\b'
            ),
            regexp_extract_all(
                lower(payment_terms_segments.segment_text),
                r'\b(?:payment|deposit|balance|remaining|cost|price|investment)\s+(?:with\s+|of\s+|is\s+|was\s+|the\s+)?([0-9][0-9,]*(?:\.[0-9]{1,2})?)\b'
            ),
            regexp_extract_all(
                lower(payment_terms_segments.segment_text),
                r'\b([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s+(?:payment|deposit|balance|today)\b'
            ),
            regexp_extract_all(
                lower(payment_terms_segments.segment_text),
                r'\b(?:put down|start with|started with|get started with)\s+([0-9][0-9,]*(?:\.[0-9]{1,2})?)\b'
            )
        )) as amount_text
    where amount_text is not null

),

plausible_payment_amount_mentions as (

    select distinct
        contact_sk,
        mentioned_amount
    from payment_terms_amount_mentions
    where mentioned_amount between 50 and 10000

),

payment_terms_summary as (

    select
        contact_sk,
        count(distinct call_id)                                      as payment_terms_calls_count,
        count(*)                                                     as payment_terms_snippets_count,
        min(call_at)                                                 as first_payment_terms_call_at,
        max(call_at)                                                 as latest_payment_terms_call_at,
        string_agg(
            concat(
                '[',
                candidate_call_source,
                ' #',
                cast(segment_index as string),
                '] ',
                segment_text
            ),
            '\n---\n'
            order by
                candidate_call_priority,
                call_at desc,
                segment_index
            limit 5
        )                                                           as payment_terms_evidence_text
    from payment_terms_segments
    group by 1

),

payment_amount_summary as (

    select
        contact_sk,
        count(*)                                                     as mentioned_payment_amounts_count,
        max(mentioned_amount)                                        as largest_mentioned_payment_amount,
        string_agg(
            concat('$', cast(mentioned_amount as string)),
            ', '
            order by mentioned_amount
            limit 20
        )                                                           as mentioned_payment_amounts_text
    from plausible_payment_amount_mentions
    group by 1

),

candidate_call_summary as (

    select
        contact_sk,
        count(distinct call_id)                                      as candidate_sales_calls_count,
        string_agg(
            concat(candidate_call_source, ': ', call_id),
            ', '
            order by
                candidate_call_priority,
                call_at desc,
                call_id
            limit 3
        )                                                           as candidate_sales_call_ids
    from scoped_candidate_calls
    group by 1

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'current_customers.contact_sk'
        ]) }}                                                        as collection_contract_evidence_sk,
        current_customers.contact_sk,
        current_customers.contact_id,
        current_customers.email_norm,
        current_customers.contact_name,
        current_customers.phone,

        current_customers.first_purchase_at,
        current_customers.latest_purchase_at,
        current_customers.first_payment_id,
        current_customers.latest_payment_id,
        current_customers.lifetime_paid_payments_count,
        current_customers.lifetime_stripe_payments_count,
        current_customers.lifetime_fanbasis_payments_count,
        current_customers.lifetime_fanbasis_auto_renew_payments_count,
        current_customers.lifetime_purchased_product_count,

        current_customers.first_purchase_product,
        current_customers.first_purchase_product_family,
        current_customers.latest_purchase_product,
        current_customers.lifetime_purchased_products,
        current_customers.top_product_by_net_revenue,
        current_customers.top_product_family,
        current_customers.top_product_net_revenue,
        current_customers.top_product_payments_count,

        current_customers.upfront_collected_net_revenue,
        current_customers.post_first_collected_net_revenue,
        current_customers.lifetime_net_revenue_after_refunds,
        current_customers.post_first_collected_net_revenue_share,
        current_customers.average_net_revenue_per_payment,

        current_customers.post_first_paid_payments_count,
        current_customers.first_post_first_payment_at,
        current_customers.latest_post_first_payment_at,
        current_customers.post_first_purchase_collection_bookings_count,
        current_customers.latest_collection_booking_at,
        current_customers.latest_collection_booking_name,
        current_customers.collection_motion_type,
        current_customers.collection_health_status,
        current_customers.retention_operator_next_action,

        current_customers.latest_prior_lead_magnet_name,
        current_customers.latest_prior_lead_magnet_category,
        current_customers.latest_prior_lead_magnet_offer_type,
        current_customers.credited_closer_name,
        current_customers.credited_closer_source,
        current_customers.credited_closer_confidence,
        current_customers.credited_setter_name,
        current_customers.credited_setter_source,

        coalesce(candidate_call_summary.candidate_sales_calls_count, 0)
                                                                        as candidate_sales_calls_count,
        candidate_call_summary.candidate_sales_call_ids,
        coalesce(payment_terms_summary.payment_terms_calls_count, 0)     as payment_terms_calls_count,
        coalesce(payment_terms_summary.payment_terms_snippets_count, 0)  as payment_terms_snippets_count,
        coalesce(payment_amount_summary.mentioned_payment_amounts_count, 0)
                                                                        as mentioned_payment_amounts_count,
        payment_amount_summary.mentioned_payment_amounts_text,
        payment_amount_summary.largest_mentioned_payment_amount,
        payment_terms_summary.first_payment_terms_call_at,
        payment_terms_summary.latest_payment_terms_call_at,
        payment_terms_summary.payment_terms_evidence_text,
        coalesce(payment_terms_summary.payment_terms_calls_count, 0) > 0
                                                                        as has_payment_terms_transcript_evidence,
        case
            when payment_terms_summary.payment_terms_calls_count > 0
                then 'transcript_payment_terms_found'
            when coalesce(candidate_call_summary.candidate_sales_calls_count, 0) > 0
                then 'sales_call_found_no_payment_terms'
            else 'no_sales_call_transcript'
        end                                                           as contract_evidence_status,
        'Collected cash and purchased product are payment facts. Transcript amounts are evidence snippets only; they are not remaining-balance truth.'
                                                                        as contract_evidence_truth_note,

        current_timestamp()                                           as mart_refreshed_at

    from current_customers
    left join candidate_call_summary
        on current_customers.contact_sk = candidate_call_summary.contact_sk
    left join payment_terms_summary
        on current_customers.contact_sk = payment_terms_summary.contact_sk
    left join payment_amount_summary
        on current_customers.contact_sk = payment_amount_summary.contact_sk

)

select * from final
