with

source as (

    select * from {{ source('raw_calendly', 'event') }}

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by uri
        order by _fivetran_synced desc
    ) = 1

),

parsed as (

    select
        uri                                                             as event_id,

        event_type_uri                                                  as event_type_id,
        name                                                            as event_name,
        status,
        location_type,

        start_time                                                      as scheduled_for,
        end_time                                                        as scheduled_until,
        created_at                                                      as booked_at,
        updated_at,

        cancel_reason,
        canceled_by                                                     as cancelled_by,
        canceler_type                                                   as cancelled_by_type,

        invitees_active                                                 as active_invitee_count,
        invitees_limit                                                  as invitee_limit,

        coalesce(_fivetran_deleted, false)                              as is_deleted,

        _fivetran_synced                                                as _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
