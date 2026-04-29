-- Grain: one row per GHL contact (the v1 identity spine). Contact-grain
-- "golden lead" surface — every lead appears, booked or not. Powers
-- Page 2 of the dashboard (funnel / attribution / psychographics /
-- lost reason / applicant → booker conversion).
--
-- Upstream dependencies (Track E): dim_contacts, dim_users,
-- dim_pipeline_stages, fct_calls_booked, fct_payments, fct_outreach.
--
-- Placeholder columns — shape-preserving NULLs carry columns whose
-- upstream hasn't shipped yet. This lets dashboards / schemas lock
-- against the final contract today; the join gets filled in when the
-- upstream bridge lands. Placeholders:
--   * application_submitted / application_date / lead_magnet_* —
--     owed by a Typeform-answers pivot model (applications + magnets
--     live in Typeform, not GHL contact attributes).
--   * Psychographic columns (age, business_stage, investment_range,
--     core_struggle, emotional_goal_value, current_situation) —
--     same pivot.
--   * self_reported_source / self_reported_vs_utm_match — Calendly
--     questions_and_answers staging owed (today's `stg_calendly__
--     events` exposes no Q&A).
--   * engagement_score — not surfaced on the GHL opportunity endpoint.
--   * first_touch_* vs last_touch_* — a single-touch today
--     (`dim_contacts` carries one utm_source/medium/campaign trio);
--     the multi-touch attribution bridge will widen later. Both
--     first_touch_* and last_touch_* currently mirror the single
--     captured UTM so the schema is stable.
--
-- Known gap on bookings_count today: `fct_calls_booked.contact_sk`
-- resolves to NULL until Calendly invitee staging lands (Track C open
-- thread). Until then every contact reports 0 bookings through the
-- fact join; oracle parity on bookers will not pass. Documented in
-- the release-gate test.

with

contacts as (

    select * from `project-41542e21-470f-4589-96d`.`Core`.`dim_contacts`

),

bookings as (

    select
        contact_sk,
        count(*)                                                as bookings_count,
        max(booked_at)                                          as latest_booking_at,
        countif(lower(coalesce(event_status, '')) not in ('canceled', 'cancelled', 'no_show', 'no-show'))
                                                                as showed_calls_count,
        countif(lower(coalesce(event_status, '')) in ('canceled', 'cancelled'))
                                                                as cancelled_bookings_count
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_calls_booked`
    where contact_sk is not null
    group by contact_sk

),

revenue as (

    select
        contact_sk,
        count(*)                                                as payment_count,
        min(transaction_date)                                   as first_payment_at,
        max(transaction_date)                                   as latest_payment_at,
        sum(net_amount)                                         as total_net_revenue
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_payments`
    where contact_sk is not null
    group by contact_sk

),

-- Latest opportunity per contact, joined to dim_pipeline_stages for
-- pipeline + stage display names and the is_booked_stage flag. Uses
-- `qualify row_number()` to keep exactly one opportunity per contact
-- (the most recently updated).
latest_opportunity as (

    select
        opp.contact_id,
        opp.assigned_user_id                                    as closer_user_id,
        opp.status                                              as opportunity_status,
        opp.lost_reason_id,
        opp.last_stage_change_at,
        opp.opportunity_updated_at,
        stages.pipeline_name,
        stages.stage_name,
        coalesce(stages.is_booked_stage, false)                 as is_booked_stage
    from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__opportunities` opp
    left join `project-41542e21-470f-4589-96d`.`Core`.`dim_pipeline_stages` stages
        on  stages.pipeline_id = opp.pipeline_id
        and stages.stage_id    = opp.pipeline_stage_id
    qualify row_number() over (
        partition by opp.contact_id
        order by opp.opportunity_updated_at desc
    ) = 1

),

-- SDR attribution: the user_sk that touched a contact most often,
-- restricted to users whose roster role is `SDR`. Ties broken by
-- touch count. fct_outreach carries NULL user_sk for GHL system /
-- automation actors — naturally dropped by the role-filtered join
-- below.
sdr_touches as (

    select
        o.contact_sk,
        u.name                                                  as sdr_name,
        count(*)                                                as touch_count
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_outreach` o
    inner join `project-41542e21-470f-4589-96d`.`Core`.`dim_users` u
        on u.user_sk = o.user_sk
    where u.role = 'SDR'
    group by o.contact_sk, u.name

),

assigned_sdr as (

    select
        contact_sk,
        sdr_name                                                as assigned_sdr_name
    from sdr_touches
    qualify row_number() over (
        partition by contact_sk
        order by touch_count desc, sdr_name
    ) = 1

),

-- Closer attribution: the assigned_user on the latest opportunity,
-- joined to dim_users and filtered to role = 'Closer' per the 5-role
-- taxonomy (SDR / Setter / Triager / DM_Setter / Closer / Owner /
-- unknown) declared in dim_users.
assigned_closer as (

    select
        lo.contact_id,
        u.name                                                  as assigned_closer_name
    from latest_opportunity lo
    inner join `project-41542e21-470f-4589-96d`.`Core`.`dim_users` u
        on u.user_id = lo.closer_user_id
    where u.role = 'Closer'

),

dial_count as (

    select
        contact_sk,
        count(*)                                                as number_of_dials
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_outreach`
    where channel = 'call'
    group by contact_sk

),

final as (

    select
        contacts.contact_id,
        contacts.email_norm                                     as email_canonical,
        contacts.contact_name                                   as full_name,
        contacts.phone,
        'D-DEE'                                                 as client,
        contacts.timezone,
        contacts.contact_created_at                             as opt_in_date,
        contacts.attribution_era                                as tracking_era,

        -- Front-of-funnel (placeholders — Typeform-answers pivot owed)
        cast(null as bool)                                      as application_submitted,
        cast(null as timestamp)                                 as application_date,
        cast(null as string)                                    as lead_magnet_first_engaged,
        cast(null as string)                                    as lead_magnet_history,

        -- Funnel aggregates
        coalesce(bookings.bookings_count, 0)                    as bookings_count,
        bookings.latest_booking_at,
        coalesce(bookings.showed_calls_count, 0)                as showed_calls_count,
        coalesce(bookings.cancelled_bookings_count, 0)          as cancelled_bookings_count,
        -- application_to_booker_flag — NULL today because
        -- application_submitted is NULL; flips on automatically when
        -- the Typeform pivot ships and the `bool AND bool` resolves.
        (cast(null as bool) and coalesce(bookings.bookings_count, 0) > 0)
                                                                as application_to_booker_flag,

        -- Current pipeline state
        latest_opportunity.pipeline_name                        as current_pipeline_name,
        latest_opportunity.stage_name                           as current_stage_name,
        coalesce(latest_opportunity.is_booked_stage, false)     as is_booked_stage,
        (lower(coalesce(latest_opportunity.opportunity_status, '')) in ('won', 'closed_won'))
                                                                as is_closed_stage,
        case
            when latest_opportunity.last_stage_change_at is null then cast(null as int64)
            else date_diff(current_date(), date(latest_opportunity.last_stage_change_at), day)
        end                                                     as days_since_stage_change,
        cast(null as int64)                                     as engagement_score,

        -- Outcome
        (lower(coalesce(latest_opportunity.opportunity_status, '')) in ('won', 'closed_won'))
                                                                as closed_won_flag,
        latest_opportunity.lost_reason_id                       as lost_reason,
        (revenue.payment_count is not null)                     as has_any_payment_flag,
        revenue.first_payment_at,
        revenue.latest_payment_at,
        coalesce(revenue.total_net_revenue, 0)                  as total_net_revenue,

        -- Multi-touch — single-touch today, first/last mirror the
        -- one UTM trio carried on dim_contacts. When the multi-touch
        -- bridge lands, widen here.
        contacts.utm_campaign                                   as first_touch_campaign,
        contacts.utm_source                                     as first_touch_source,
        contacts.utm_medium                                     as first_touch_medium,
        contacts.utm_campaign                                   as last_touch_campaign,
        contacts.utm_source                                     as last_touch_source,
        contacts.utm_medium                                     as last_touch_medium,
        -- Trivially true today because first/last are the same single
        -- captured UTM; flips on as soon as multi-touch widens.
        true                                                    as first_vs_last_touch_campaign_match,

        -- Self-reported (placeholder — Calendly Q&A staging owed)
        cast(null as string)                                    as self_reported_source,
        cast(null as bool)                                      as self_reported_vs_utm_match,

        -- Psychographic (placeholders — Typeform-answers pivot owed)
        cast(null as int64)                                     as age,
        cast(null as string)                                    as business_stage,
        cast(null as string)                                    as investment_range,
        cast(null as string)                                    as core_struggle,
        cast(null as string)                                    as emotional_goal_value,
        cast(null as string)                                    as current_situation,

        -- Team attribution
        assigned_sdr.assigned_sdr_name,
        assigned_closer.assigned_closer_name,
        coalesce(dial_count.number_of_dials, 0)                 as number_of_dials,

        -- DQ
        case
            when contacts.attribution_era = 'pre_utm'           then 'pre_utm_era'
            when assigned_sdr.assigned_sdr_name is null
             and coalesce(bookings.bookings_count, 0) > 0       then 'no_sdr_touch'
            else 'clean'
        end                                                     as attribution_quality_flag,
        contacts.attribution_era,
        current_timestamp()                                     as mart_refreshed_at

    from contacts
    left join bookings
        on bookings.contact_sk = contacts.contact_sk
    left join revenue
        on revenue.contact_sk = contacts.contact_sk
    left join latest_opportunity
        on latest_opportunity.contact_id = contacts.contact_id
    left join assigned_sdr
        on assigned_sdr.contact_sk = contacts.contact_sk
    left join assigned_closer
        on assigned_closer.contact_id = contacts.contact_id
    left join dial_count
        on dial_count.contact_sk = contacts.contact_sk

)

select * from final