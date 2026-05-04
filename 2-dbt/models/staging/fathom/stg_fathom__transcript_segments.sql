-- Fathom transcript is a nested array on each raw call payload. Keep this
-- source-shaped and call-grain-safe: one row per transcript segment.

with

source as (

    select
        entity_id                                                       as call_id,
        transcript_segment,
        segment_index,
        ingested_at                                                     as _ingested_at
    from {{ source('raw_fathom', 'fathom_calls_raw') }},
        unnest(
            ifnull(json_query_array(payload_json, '$.transcript'), [])
        ) as transcript_segment with offset as segment_index

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'call_id',
            'cast(segment_index as string)'
        ]) }}                                                          as transcript_segment_sk,
        call_id,
        segment_index,

        safe_cast(
            json_value(transcript_segment, '$.timestamp') as float64
        )                                                             as segment_offset_seconds,
        nullif(json_value(transcript_segment, '$.speaker.display_name'), '')
                                                                        as speaker_name,
        nullif(
            lower(trim(
                json_value(
                    transcript_segment,
                    '$.speaker.matched_calendar_invitee_email'
                )
            )),
            ''
        )                                                             as speaker_email_norm,
        nullif(json_value(transcript_segment, '$.text'), '')           as segment_text,
        transcript_segment                                             as segment_json,
        _ingested_at
    from source

)

select * from final
