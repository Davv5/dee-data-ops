

-- era_flag: inline CASE on booked_at using a hardcoded 2026-03-16 cutover
-- (Monday of ISO-W12, the week the median first-touch time dropped below 60 min).
-- Option B chosen over a seed — one binary cutover, no business-editable nuance,
-- no seed-maintenance overhead, no extra DAG node / join per row. Flip to a seed
-- only if the era taxonomy grows beyond ramping/stable.
with

fct_bookings as (
    select
        booking_sk,
        contact_sk,
        assigned_user_sk,
        pipeline_stage_sk,
        booked_at,
        scheduled_for,
        event_status,
        cancelled_at
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_calls_booked`
),

contacts as (
    -- dim_contacts doesn't expose multi-touch attribution columns yet (Track E
    -- ships single-touch utm_* only). Stub multi-touch + lead-magnet fields as
    -- NULL so the mart keeps its column contract; Looker tiles will render
    -- empty for these until the upstream enrichment lands.
    select
        dc.contact_sk,
        dc.contact_id,
        dc.email,
        dc.contact_name,
        dc.lead_source,
        dc.attribution_era,
        cast(null as string) as first_touch_campaign,
        cast(null as string) as first_touch_source,
        cast(null as string) as first_touch_medium,
        cast(null as string) as last_touch_campaign,
        cast(null as string) as last_touch_source,
        cast(null as string) as last_touch_medium,
        cast(null as string) as lead_magnet_first_engaged,
        cast(null as string) as lead_source_self_reported,
        cast('Dee' as string) as client  -- v1 is single-tenant; widen when multi-tenant ships
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_contacts` as dc
),

users as (
    select
        user_sk,
        user_id,
        name,
        role
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_users`
),

opportunities as (
    select
        contact_id,
        assigned_user_id,
        status,
        last_status_change_at,
        last_stage_change_at,
        lost_reason_id,
        opportunity_created_at
    from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__opportunities`
),

outreach as (
    select
        contact_sk,
        user_sk,
        touched_at,
        channel,
        message_id
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_outreach`
),

stages as (
    select
        pipeline_stage_sk,
        pipeline_name,
        stage_name,
        is_booked_stage
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_pipeline_stages`
),

assigned as (
    select
        user_sk,
        name as assigned_user_name,
        role as assigned_user_role
    from users
),

first_touch as (
    select
        b.booking_sk,
        o.user_sk,
        o.touched_at,
        o.channel
    from fct_bookings b
    left join outreach o
      on o.contact_sk = b.contact_sk
     and o.touched_at >= b.booked_at
    qualify row_number() over (
        partition by b.booking_sk
        order by o.touched_at asc
    ) = 1
),

first_toucher as (
    select
        user_sk,
        name as first_toucher_name,
        role as first_toucher_role
    from users
),

-- Pick the opportunity whose opportunity_created_at is closest to (and after) booked_at.
-- If none exists after, fall back to the most recent before — covers legacy imports.
closer_and_outcome as (
    select
        b.booking_sk,
        u.name as closer_name,
        u.role as closer_role,
        case
            when lower(opp.status) = 'won'  then 'won'
            when lower(opp.status) = 'lost' then 'lost'
            else 'pending'
        end as close_outcome,
        case
            when lower(opp.status) in ('won', 'lost')
                then opp.last_status_change_at
        end as closed_at,
        opp.lost_reason_id as lost_reason,
        opp.last_stage_change_at
    from fct_bookings b
    left join contacts c_opp
      on c_opp.contact_sk = b.contact_sk
    left join opportunities opp
      on opp.contact_id = c_opp.contact_id
    left join users u
      on u.user_id = opp.assigned_user_id
    qualify row_number() over (
        partition by b.booking_sk
        order by opp.opportunity_created_at desc
    ) = 1
),

had_activity_1hr as (
    select
        b.booking_sk,
        count(o.message_id) > 0 as had_any_sdr_activity_within_1_hr
    from fct_bookings b
    left join outreach o
      on o.contact_sk = b.contact_sk
     and o.touched_at between b.booked_at
                          and timestamp_add(b.booked_at, interval 60 minute)
    group by b.booking_sk
),

final as (
    select
        b.booking_sk                                as booking_id,
        c.contact_id,
        c.email,
        c.contact_name                              as full_name,
        b.booked_at,
        b.scheduled_for,
        b.event_status,
        b.cancelled_at,

        au.assigned_user_name,
        au.assigned_user_role,

        ftr.first_toucher_name,
        ftr.first_toucher_role,
        ft.touched_at                               as first_outbound_touch_at,
        ft.channel                                  as first_touch_channel,

        case
            when ftr.first_toucher_role = 'SDR' and ft.touched_at is not null
                then timestamp_diff(ft.touched_at, b.booked_at, minute)
        end                                         as minutes_to_first_sdr_touch,
        case
            when ftr.first_toucher_role = 'SDR' and ft.touched_at is not null
                then timestamp_diff(ft.touched_at, b.booked_at, minute) < 5
        end                                         as is_within_5_min_sla,
        coalesce(ha.had_any_sdr_activity_within_1_hr, false)
                                                    as had_any_sdr_activity_within_1_hr,

        co.closer_name,
        co.closer_role,
        co.close_outcome,
        case
            when co.close_outcome = 'won'
                then timestamp_diff(co.closed_at, b.booked_at, minute)
        end                                         as cycle_time_booking_to_close_min,
        co.lost_reason,

        c.first_touch_campaign,
        c.first_touch_source,
        c.first_touch_medium,
        c.last_touch_campaign,
        c.last_touch_source,
        c.last_touch_medium,

        c.lead_source,
        s.pipeline_name,
        s.stage_name,
        coalesce(s.is_booked_stage, false)          as is_booked_stage,
        case
            when co.last_stage_change_at is not null
                then date_diff(current_date(), date(co.last_stage_change_at), day)
        end                                         as days_since_stage_change,

        case
            when ft.touched_at is null and ftr.first_toucher_role is null
                then 'no_sdr_touch'
            when ftr.first_toucher_role is null
              or lower(ftr.first_toucher_role) = 'unknown'
                then 'role_unknown'
            when c.attribution_era = 'pre_UTM'
                then 'pre_utm_era'
            else 'clean'
        end                                         as attribution_quality_flag,
        c.attribution_era,
        case
            when date(b.booked_at) < date '2026-03-16' then 'ramping'
            else 'stable'
        end                                         as era_flag,
        c.client,

        current_timestamp()                         as mart_refreshed_at

    from fct_bookings b
    left join contacts c           on c.contact_sk = b.contact_sk
    left join assigned au          on au.user_sk = b.assigned_user_sk
    left join first_touch ft       on ft.booking_sk = b.booking_sk
    left join first_toucher ftr    on ftr.user_sk = ft.user_sk
    left join closer_and_outcome co on co.booking_sk = b.booking_sk
    left join stages s             on s.pipeline_stage_sk = b.pipeline_stage_sk
    left join had_activity_1hr ha  on ha.booking_sk = b.booking_sk
)

select * from final