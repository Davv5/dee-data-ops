-- Fathom stores calendar invitees as a nested array inside each call payload.
-- This staging view extracts that source-shaped child entity without applying
-- business attribution logic.

with

source as (

    select
        entity_id                                                       as call_id,
        workspace_id,
        team_id,
        event_ts,
        updated_at_ts,
        ingested_at                                                     as _ingested_at,
        backfill_run_id,
        is_backfill,
        payload_json
    from {{ source('raw_fathom', 'fathom_calls_raw') }}
    where entity_type = 'calls'

),

invitees as (

    select
        source.call_id,
        source.workspace_id,
        source.team_id,
        source.event_ts,
        source.updated_at_ts,
        source._ingested_at,
        source.backfill_run_id,
        source.is_backfill,

        nullif(lower(trim(json_value(invitee, '$.email'))), '')
                                                                        as participant_email_norm,
        nullif(json_value(invitee, '$.email'), '')                      as participant_email,
        nullif(json_value(invitee, '$.name'), '')                       as participant_name,
        nullif(json_value(invitee, '$.email_domain'), '')               as participant_email_domain,
        safe_cast(json_value(invitee, '$.is_external') as bool)         as is_external,
        nullif(json_value(invitee, '$.response_status'), '')            as response_status,
        invitee                                                         as participant_json

    from source,
        unnest(ifnull(json_query_array(source.payload_json, '$.calendar_invitees'), []))
            as invitee

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'call_id',
            'participant_email_norm',
            'participant_name',
            'cast(is_external as string)'
        ]) }}                                                          as call_invitee_sk,

        call_id,
        workspace_id,
        team_id,
        participant_email_norm,
        participant_email,
        participant_name,
        participant_email_domain,
        is_external,
        response_status,
        event_ts,
        updated_at_ts,
        _ingested_at,
        backfill_run_id,
        is_backfill,
        participant_json

    from invitees
    qualify row_number() over (
        partition by
            call_id,
            participant_email_norm,
            participant_name,
            is_external
        order by _ingested_at desc
    ) = 1

)

select * from final
