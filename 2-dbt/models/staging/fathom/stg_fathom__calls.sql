-- U3 blob-shim (2026-04-23): 1:1 staging view on `Raw.fathom_calls_raw`.
-- Mixes top-level extracted columns (workspace_id, team_id, title,
-- ended_at_ts, classification_*, is_revenue_relevant, is_internal_only,
-- external_participant_count) with JSON-decoded payload fields (recorder,
-- meeting_title, urls, transcript).
--
-- Transcript payload note: Fathom stores transcript as a nested JSON array,
-- not a scalar string. `stg_fathom__transcript_segments` is the analysis-ready
-- child model; this view keeps the raw transcript JSON for call-grain
-- observability.
--
-- TODO: retire when `raw_fathom.fathom__calls_raw` (U6) is populated.

with

source as (

    select
        entity_id                                                       as call_id,

        workspace_id,
        team_id,
        title                                                           as extracted_title,

        event_ts,
        ended_at_ts,
        updated_at_ts,

        external_participant_count,
        is_internal_only,
        is_revenue_relevant,

        classification_label,
        classification_confidence,
        classification_reason,

        json_value(payload_json, '$.meeting_title')                     as meeting_title,
        json_value(payload_json, '$.recorded_by.email')                 as recorded_by_email,
        json_value(payload_json, '$.recorded_by.name')                  as recorded_by_name,
        json_value(payload_json, '$.recorded_by.team')                  as recorded_by_team,

        safe_cast(json_value(payload_json, '$.recording_start_time') as timestamp) as recording_start_at,
        safe_cast(json_value(payload_json, '$.recording_end_time') as timestamp)   as recording_end_at,
        safe_cast(json_value(payload_json, '$.scheduled_start_time') as timestamp) as scheduled_start_at,
        safe_cast(json_value(payload_json, '$.scheduled_end_time') as timestamp)   as scheduled_end_at,

        json_value(payload_json, '$.share_url')                         as share_url,
        json_value(payload_json, '$.url')                               as fathom_url,
        json_value(payload_json, '$.transcript_language')               as transcript_language,

        json_query(payload_json, '$.transcript')                        as transcript,
        array_length(json_query_array(payload_json, '$.transcript'))     as transcript_segment_count,

        ingested_at                                                     as _ingested_at
    from {{ source('raw_fathom', 'fathom_calls_raw') }}

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by call_id
        order by _ingested_at desc
    ) = 1

),

final as (

    select * from deduped

)

select * from final
