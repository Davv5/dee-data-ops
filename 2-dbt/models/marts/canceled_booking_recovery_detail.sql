{{
    config(
        materialized = 'table',
    )
}}

-- Grain: one row per canceled Calendly booking.
--
-- Canceled does not always mean lost at D-DEE: host/triager cancellations
-- can be reschedules, qualification cleanup, or calendar hygiene. This mart
-- follows what happened after the canceled booking and keeps show evidence
-- separate from revenue evidence.

with

bookings_enriched as (

    select
        bookings.booking_sk,
        bookings.contact_sk,
        bookings.calendly_event_id,
        bookings.booked_at,
        bookings.scheduled_for,
        bookings.cancelled_at,
        lower(coalesce(bookings.event_status, 'unknown'))             as event_status,

        nullif(trim(events.cancel_reason), '')                        as cancel_reason,
        events.cancelled_by,
        coalesce(events.cancelled_by_type, invitees.canceler_type, 'unknown')
                                                                        as cancelled_by_type,

        invitees.invitee_status,
        invitees.no_show_created_at

    from {{ ref('fct_calls_booked') }} as bookings
    left join {{ ref('stg_calendly__events') }} as events
        on bookings.calendly_event_id = events.event_id
    left join {{ ref('stg_calendly__event_invitees') }} as invitees
        on bookings.calendly_event_id = invitees.event_id

),

canceled as (

    select
        *,
        row_number() over (
            partition by coalesce(contact_sk, booking_sk)
            order by coalesce(cancelled_at, booked_at), booking_sk
        )                                                            as canceled_booking_sequence
    from bookings_enriched
    where event_status in ('canceled', 'cancelled')

),

next_active as (

    select
        canceled.booking_sk                                          as canceled_booking_sk,
        bookings_enriched.booking_sk                                 as next_active_booking_sk,
        bookings_enriched.booked_at                                  as next_active_booked_at,
        bookings_enriched.scheduled_for                              as next_active_scheduled_for,
        bookings_enriched.no_show_created_at                         as next_active_no_show_created_at

    from canceled
    inner join bookings_enriched
        on canceled.contact_sk = bookings_enriched.contact_sk
       and canceled.booking_sk != bookings_enriched.booking_sk
       and bookings_enriched.event_status = 'active'
       and bookings_enriched.booked_at > canceled.booked_at
    qualify row_number() over (
        partition by canceled.booking_sk
        order by bookings_enriched.booked_at, bookings_enriched.booking_sk
    ) = 1

),

fathom_by_next_active as (

    select
        next_active.canceled_booking_sk,
        count(fathom.call_id)                                       as fathom_calls_near_next_booking,
        countif(fathom.call_id is not null and fathom.is_revenue_relevant)
                                                                      as revenue_fathom_calls_near_next_booking

    from next_active
    left join {{ ref('stg_fathom__calls') }} as fathom
        on abs(
            timestamp_diff(
                fathom.scheduled_start_at,
                next_active.next_active_scheduled_for,
                minute
            )
        ) <= 15
    group by 1

),

buyer_after_cancel as (

    select
        canceled.booking_sk                                          as canceled_booking_sk,
        revenue.contact_sk                                           as buyer_contact_sk,
        revenue.first_purchase_at,
        revenue.total_net_revenue_after_refunds

    from canceled
    left join {{ ref('revenue_funnel_detail') }} as revenue
        on canceled.contact_sk = revenue.contact_sk
       and revenue.first_purchase_at >= coalesce(canceled.cancelled_at, canceled.booked_at)

),

final as (

    select
        canceled.booking_sk                                          as canceled_booking_sk,
        canceled.contact_sk,
        canceled.calendly_event_id,
        canceled.booked_at,
        canceled.scheduled_for,
        canceled.cancelled_at,
        canceled.cancel_reason,
        canceled.cancelled_by,
        canceled.cancelled_by_type,
        canceled.canceled_booking_sequence,

        next_active.next_active_booking_sk,
        next_active.next_active_booked_at,
        next_active.next_active_scheduled_for,
        timestamp_diff(
            next_active.next_active_booked_at,
            coalesce(canceled.cancelled_at, canceled.booked_at),
            hour
        )                                                            as hours_to_rebook,
        next_active.next_active_no_show_created_at is not null        as next_active_marked_no_show,

        coalesce(fathom_by_next_active.fathom_calls_near_next_booking, 0)
                                                                      as fathom_calls_near_next_booking,
        coalesce(fathom_by_next_active.revenue_fathom_calls_near_next_booking, 0)
                                                                      as revenue_fathom_calls_near_next_booking,

        buyer_after_cancel.first_purchase_at                          as first_purchase_after_cancel_at,
        coalesce(buyer_after_cancel.total_net_revenue_after_refunds, 0)
                                                                      as total_net_revenue_after_cancel,
        case
            when canceled.canceled_booking_sequence = 1
                and buyer_after_cancel.buyer_contact_sk is not null
                then buyer_after_cancel.total_net_revenue_after_refunds
            else 0
        end                                                           as credited_net_revenue_after_first_cancel,

        next_active.next_active_booking_sk is not null                as has_rebooked_after_cancel,
        coalesce(
            next_active.next_active_scheduled_for < current_timestamp(),
            false
        )                                                             as next_active_is_due,
        coalesce((
            next_active.next_active_scheduled_for < current_timestamp()
            and next_active.next_active_no_show_created_at is null
        ), false)                                                     as has_likely_show_after_cancel,
        coalesce(fathom_by_next_active.revenue_fathom_calls_near_next_booking, 0) > 0
                                                                      as has_fathom_show_evidence,
        buyer_after_cancel.buyer_contact_sk is not null               as had_purchase_after_cancel,

        case
            when canceled.contact_sk is null
                then 'contact_not_matched'
            when buyer_after_cancel.buyer_contact_sk is not null
                then 'bought_after_cancel'
            when coalesce(fathom_by_next_active.revenue_fathom_calls_near_next_booking, 0) > 0
                then 'fathom_show_after_cancel'
            when next_active.next_active_scheduled_for >= current_timestamp()
                then 'rebooked_pending'
            when next_active.next_active_no_show_created_at is not null
                then 'rebooked_no_show'
            when next_active.next_active_booking_sk is not null
                then 'likely_show_after_cancel'
            else 'not_recovered_yet'
        end                                                           as recovery_outcome,

        current_timestamp()                                           as mart_refreshed_at

    from canceled
    left join next_active
        on canceled.booking_sk = next_active.canceled_booking_sk
    left join fathom_by_next_active
        on canceled.booking_sk = fathom_by_next_active.canceled_booking_sk
    left join buyer_after_cancel
        on canceled.booking_sk = buyer_after_cancel.canceled_booking_sk

)

select * from final
