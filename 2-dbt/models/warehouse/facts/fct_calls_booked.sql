-- Grain: one row per Calendly booking event (`stg_calendly__events.event_id`).
-- Speed-to-Lead denominator source — every confirmed booking, including
-- bookings that never landed on a GHL opportunity.
--
-- contact_sk resolution: `stg_calendly__event_invitees.invitee_email_norm`
-- → `dim_contacts.email_norm` → `contact_sk`. Bookings whose invitee email
-- doesn't match a GHL contact resolve to NULL contact_sk (the relationships
-- test auto-excludes NULLs). Do not widen `dim_contacts` here.
--
-- assigned_user_sk + pipeline_stage_sk are *diagnostic* attribution — they
-- represent which SDR GHL believes owns the booking, NOT the headline
-- Speed-to-Lead numerator (which sources first-touch identity from
-- raw_ghl.conversations / fct_outreach, independent of opportunity state).
-- The selected opportunity is the most-recent one for the contact whose
-- `opportunity_created_at <= booked_at` ("active opp at booking time"),
-- with `opportunity_id desc` as the deterministic tiebreaker on ties.
-- If no opp pre-existed the booking, both SKs are NULL. The strict `<=`
-- boundary is sub-second sensitive: if the GHL "Booked" workflow creates
-- an opp T+50ms after the Calendly booking event, the fact selects the
-- *prior* opp on this contact (or NULL) — by design, since this column
-- represents the SDR who owned the contact *before* the booking, not the
-- workflow-created opp. The fact's selection rule provides the canonical
-- axis to *eliminate* three divergent rules currently coexisting in marts
-- (sales_activity_detail uses "latest opp by created_at" with a broken time
-- filter; lead_journey uses "latest opp by updated_at"). Mart collapse to
-- consume this axis ships in PR-2 / PR-3 — see plan doc below.
--
-- booking_time_opportunity_id projects the opportunity_id of the picked opp so
-- marts can join back to stg_ghl__opportunities on a single deterministic
-- axis without re-implementing the selection rule. NULL on the same
-- condition as assigned_user_sk / pipeline_stage_sk (no pre-booking opp).
-- Mart collapse plan: `docs/plans/2026-04-30-mart-collapse-fct-sks-plan.md`.

with

events as (

    select * from {{ ref('stg_calendly__events') }}

),

invitees as (

    select * from {{ ref('stg_calendly__event_invitees') }}

),

contacts as (

    select
        contact_sk,
        contact_id,
        email_norm
    from {{ ref('dim_contacts') }}

),

opportunities as (

    select
        opportunity_id,
        contact_id,
        assigned_user_id,
        pipeline_id,
        pipeline_stage_id,
        opportunity_created_at
    from {{ ref('stg_ghl__opportunities') }}

),

users as (

    select
        user_id,
        user_sk
    from {{ ref('dim_users') }}

),

pipeline_stages as (

    select
        pipeline_id,
        stage_id,
        pipeline_stage_sk
    from {{ ref('dim_pipeline_stages') }}

),

-- Resolve booking → contact_id, plus the active opp at booking time.
booking_contact as (

    select
        events.event_id,
        events.booked_at,
        contacts.contact_sk,
        contacts.contact_id
    from events
    left join invitees
        on invitees.event_id = events.event_id
    left join contacts
        on contacts.email_norm = invitees.invitee_email_norm

),

opportunity_at_booking as (

    select
        booking_contact.event_id,
        opportunities.opportunity_id,
        opportunities.assigned_user_id,
        opportunities.pipeline_id,
        opportunities.pipeline_stage_id
    from booking_contact
    left join opportunities
        on opportunities.contact_id = booking_contact.contact_id
       and opportunities.opportunity_created_at <= booking_contact.booked_at
    qualify row_number() over (
        partition by booking_contact.event_id
        order by opportunities.opportunity_created_at desc,
                 opportunities.opportunity_id desc
    ) = 1

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['events.event_id']) }}
                                                                as booking_sk,

        booking_contact.contact_sk                              as contact_sk,
        users.user_sk                                           as assigned_user_sk,
        pipeline_stages.pipeline_stage_sk                       as pipeline_stage_sk,
        opportunity_at_booking.opportunity_id                   as booking_time_opportunity_id,

        events.event_id                                         as calendly_event_id,
        events.event_type_id,
        events.event_name,

        events.booked_at,
        events.scheduled_for,
        events.scheduled_until,

        events.status                                           as event_status,

        case
            when lower(events.status) in ('canceled', 'cancelled')
            then events.updated_at
            else null
        end                                                     as cancelled_at,

        events.location_type,
        events.active_invitee_count,
        events.is_deleted

    from events
    left join booking_contact
        on booking_contact.event_id = events.event_id
    left join opportunity_at_booking
        on opportunity_at_booking.event_id = events.event_id
    left join users
        on users.user_id = opportunity_at_booking.assigned_user_id
    left join pipeline_stages
        on pipeline_stages.pipeline_id = opportunity_at_booking.pipeline_id
       and pipeline_stages.stage_id    = opportunity_at_booking.pipeline_stage_id

)

select * from final
