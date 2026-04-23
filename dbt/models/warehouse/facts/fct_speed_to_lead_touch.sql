{{
    config(
        materialized='table',
        partition_by={
            'field': 'booked_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

-- Grain: one row per (booking_sk, touch_sk). Bookings with zero outbound
-- human SDR touches emit ONE row with touch_sk = NULL — this preserves
-- denominator counts for booking-level rollups (% of bookings with ≥1 touch).
--
-- Justification for going to this grain: "If you have the option to go to a
-- lower granularity and you can still achieve the same result I tend to
-- recommend doing that — it gives you more options... because you can then
-- roll things up." (source: "3 Data Modeling Mistakes That Can Derail a Team",
-- Data Ops notebook.) The mart-layer fallback `close_outcome IS NOT NULL` in
-- `sales_activity_detail` is exactly the "missing out on the ability to report
-- on data because you didn't go low enough" failure-mode that source calls out.
-- Resolution: this fact carries a real `show_outcome` column per touch row,
-- resolving the DQ gap noted in .claude/state/project-state.md.
--
-- show_outcome derivation (v1): Calendly event_status + GHL opportunity
-- last_stage_change_at signal. Heuristic: if the opportunity stage changed
-- AFTER scheduled_for, we treat that as "attended" (the rep progressed the
-- stage because the call happened). This is a v1 approximation; the fallback
-- for rows where no opportunity exists or stage_change is absent is documented
-- below with a code comment and flagged in WORKLOG "Open threads" for F3
-- finalization.
--
-- SCD-2 note: is_sdr_touch uses dim_users.role at query time (current-state
-- join). For historical as-of joins use dim_users_snapshot with the
-- dbt_valid_from / dbt_valid_to window. F2 will layer in the SCD join for the
-- mart; F1 uses current-state role to keep the fact buildable before the
-- snapshot has history.

with

bookings as (

    select
        booking_sk,
        contact_sk,
        booked_at,
        scheduled_for,
        event_status,
        cancelled_at
    from {{ ref('fct_calls_booked') }}

),

touches as (

    select
        touch_sk,
        contact_sk,
        user_sk,
        touched_at,
        channel
    from {{ ref('fct_outreach') }}

),

users as (

    select
        user_sk,
        user_id,
        name,
        role,
        is_active
    from {{ ref('dim_users') }}

),

contacts as (

    select
        contact_sk,
        contact_id,
        lead_source,
        attribution_era
    from {{ ref('dim_contacts') }}

),

-- Pick the opportunity closest to (and after) booked_at for the contact.
-- If none after, fall back to most recent before (legacy import coverage).
-- Mirrors the closer_and_outcome CTE in sales_activity_detail.
opportunities as (

    select
        contact_id,
        status,
        last_status_change_at,
        last_stage_change_at,
        lost_reason_id,
        opportunity_created_at
    from {{ ref('stg_ghl__opportunities') }}

),

calendar as (

    select
        date_sk,
        date_day
    from {{ ref('dim_calendar_dates') }}

),

dim_sdr as (

    select
        sdr_sk,
        user_sk
    from {{ ref('dim_sdr') }}

),

dim_src as (

    select
        source_sk,
        lead_source
    from {{ ref('dim_source') }}

),

-- Left-join touches to bookings on contact match + touched AFTER booked
-- This is the same axis as the first_touch CTE in sales_activity_detail.
booking_touches as (

    select
        bookings.booking_sk,
        bookings.contact_sk,
        bookings.booked_at,
        bookings.scheduled_for,
        bookings.event_status,
        bookings.cancelled_at,
        touches.touch_sk,
        touches.user_sk,
        touches.touched_at,
        touches.channel
    from bookings
    left join touches
        on touches.contact_sk = bookings.contact_sk
       and touches.touched_at >= bookings.booked_at

),

-- Resolve opportunity per booking (dedup to one row per booking_sk)
booking_opp as (

    select
        bookings.booking_sk,
        opps.status                         as opp_status,
        opps.last_status_change_at,
        opps.last_stage_change_at,
        opps.lost_reason_id,
        opps.opportunity_created_at
    from bookings
    left join contacts c
        on c.contact_sk = bookings.contact_sk
    left join opportunities opps
        on opps.contact_id = c.contact_id
    qualify row_number() over (
        partition by bookings.booking_sk
        order by opps.opportunity_created_at desc
    ) = 1

),

-- Compute had_any_sdr_activity_within_1_hr at booking grain (repeated per row)
had_activity_1hr as (

    select
        bt.booking_sk,
        count(bt.touch_sk) > 0              as had_any_sdr_activity_within_1_hr
    from booking_touches bt
    inner join users u
        on u.user_sk = bt.user_sk
       and lower(u.role) = 'sdr'
    where bt.touched_at between bt.booked_at
                             and timestamp_add(bt.booked_at, interval 60 minute)
    group by bt.booking_sk

),

enriched as (

    select
        bt.booking_sk,
        bt.contact_sk,
        bt.booked_at,
        bt.scheduled_for,
        bt.event_status,
        bt.cancelled_at,
        bt.touch_sk,
        bt.user_sk,
        bt.touched_at,
        bt.channel,

        u.role                                                  as toucher_role,

        -- is_sdr_touch: current-state role join (v1; SCD-2 as-of join in F2)
        lower(u.role) = 'sdr'                                   as is_sdr_touch,

        -- is_first_touch: TRUE on the earliest is_sdr_touch row per booking
        case
            when lower(u.role) = 'sdr' and bt.touched_at is not null
                then row_number() over (
                    partition by bt.booking_sk, (lower(u.role) = 'sdr')
                    order by bt.touched_at asc
                ) = 1
            else false
        end                                                     as is_first_touch,

        bo.opp_status,
        bo.last_status_change_at,
        bo.last_stage_change_at,
        bo.lost_reason_id,

        coalesce(ha.had_any_sdr_activity_within_1_hr, false)    as had_any_sdr_activity_within_1_hr,

        c.lead_source,
        c.attribution_era,

        ds.sdr_sk,
        src.source_sk

    from booking_touches bt
    left join users u           on u.user_sk = bt.user_sk
    left join booking_opp bo    on bo.booking_sk = bt.booking_sk
    left join had_activity_1hr ha on ha.booking_sk = bt.booking_sk
    left join contacts c        on c.contact_sk = bt.contact_sk
    left join dim_sdr ds        on ds.user_sk = bt.user_sk
    left join dim_src src       on src.lead_source = c.lead_source

),

final as (

    select
        -- PK: coalesce touch_sk so null-touch rows still get a unique SK
        {{ dbt_utils.generate_surrogate_key([
            'booking_sk', "coalesce(touch_sk, 'no-touch')"
        ]) }}                                                   as speed_to_lead_touch_sk,

        booking_sk,
        touch_sk,
        contact_sk,
        user_sk,
        sdr_sk,

        coalesce(
            source_sk,
            -- __unknown__ row in dim_source handles NULLs
            (
                select source_sk from dim_src
                where lead_source = '__unknown__'
                limit 1
            )
        )                                                       as source_sk,

        -- date_sk: join on date of booking
        cal.date_sk,

        booked_at,
        touched_at,

        case
            when touched_at is not null
                then timestamp_diff(touched_at, booked_at, minute)
        end                                                     as minutes_to_touch,

        is_first_touch,
        is_sdr_touch,
        channel,

        -- is_within_5_min_sla: first SDR touch that arrived within 5 min
        case
            when is_first_touch and is_sdr_touch and touched_at is not null
                then timestamp_diff(touched_at, booked_at, minute) < 5
            else false
        end                                                     as is_within_5_min_sla,

        had_any_sdr_activity_within_1_hr,
        event_status,
        cancelled_at,

        -- show_outcome: v1 heuristic from Calendly event_status + GHL stage signal
        -- Fallback (v1 approximation): when no opportunity exists for a booking,
        -- or when last_stage_change_at is absent, we cannot apply the stage-change
        -- heuristic. We fall back to `close_outcome IS NOT NULL` as a weak
        -- "showed" signal. This is documented here so F3 can find and finalize it.
        -- OPEN THREAD: validate heuristic accuracy against ~20 manual inspections
        -- before F2 ships (flagged in WORKLOG "Open threads").
        case
            when lower(event_status) in ('canceled', 'cancelled')
                then 'cancelled'
            when event_status = 'active'
                 and current_timestamp() < scheduled_for
                then 'pending'
            when event_status = 'active'
                 and current_timestamp() >= scheduled_for
                 and last_stage_change_at is not null
                 and last_stage_change_at >= scheduled_for
                then 'showed'
            when event_status = 'active'
                 and current_timestamp() >= scheduled_for
                 and last_stage_change_at is null
                -- v1 fallback: no stage signal available; treat close outcome as
                -- showed proxy. See code comment above re: F3 finalization.
                and lower(opp_status) in ('won', 'lost')
                then 'showed'
            else 'no_show'
        end                                                     as show_outcome,

        -- close_outcome: mirrors closer_and_outcome CTE in sales_activity_detail
        case
            when lower(opp_status) = 'won'  then 'won'
            when lower(opp_status) = 'lost' then 'lost'
            else 'pending'
        end                                                     as close_outcome,

        case
            when lower(opp_status) in ('won', 'lost')
                then last_status_change_at
        end                                                     as closed_at,

        lost_reason_id                                          as lost_reason,

        -- attribution_quality_flag: verbatim from sales_activity_detail lines 225-234
        case
            when touched_at is null and toucher_role is null
                then 'no_sdr_touch'
            when toucher_role is null
              or lower(toucher_role) = 'unknown'
                then 'role_unknown'
            when attribution_era = 'pre_UTM'
                then 'pre_utm_era'
            else 'clean'
        end                                                     as attribution_quality_flag,

        attribution_era,

        -- era_flag: inline CASE matching sales_activity_detail lines 236-239
        case
            when date(booked_at) < date '2026-03-16' then 'ramping'
            else 'stable'
        end                                                     as era_flag,

        current_timestamp()                                     as mart_refreshed_at

    from enriched
    left join calendar cal
        on cal.date_day = date(booked_at)

)

select * from final
