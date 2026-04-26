-- U3 blob-shim (2026-04-23): source CTE filters `Raw.typeform_objects_raw`
-- by `entity_type = 'responses'` and JSON-decodes the payload into the
-- column shape downstream expects. Body of the model (deduped / parsed /
-- final) is unchanged from the Fivetran-shape version.
--
-- Known gap: `form_id` is not carried in the GTM extractor output (neither
-- as a top-level column nor inside payload_json). Staging emits NULL for
-- now; real fix is the U9 Phase-2 Typeform extractor rewrite.
--
-- TODO: retire when `raw_typeform.typeform__responses_raw` (U9) is populated.

with

source as (

    select
        json_value(payload_json, '$.response_id')                       as id,

        -- GAP: form_id not present in blob payload or extracted columns.
        -- Flagged in _typeform__sources.yml; fix belongs to U9.
        cast(null as string)                                            as form_id,

        json_value(payload_json, '$.token')                             as token,

        json_value(payload_json, '$.response_type')                     as response_type,
        json_value(payload_json, '$.landing_id')                        as landing_id,

        safe_cast(json_value(payload_json, '$.landed_at') as timestamp)    as landed_at,
        safe_cast(json_value(payload_json, '$.submitted_at') as timestamp) as submitted_at,
        cast(null as timestamp)                                         as staged_at,

        json_value(payload_json, '$.hidden.utm_source')                 as hidden_utm_source,
        json_value(payload_json, '$.hidden.utm_medium')                 as hidden_utm_medium,
        json_value(payload_json, '$.hidden.utm_campaign')               as hidden_utm_campaign,

        json_value(payload_json, '$.metadata.user_agent')               as metadata_user_agent,
        json_value(payload_json, '$.metadata.platform')                 as metadata_platform,
        json_value(payload_json, '$.metadata.referer')                  as metadata_referer,
        json_value(payload_json, '$.metadata.browser')                  as metadata_browser,
        json_value(payload_json, '$.metadata.network_id')               as metadata_network_id,

        safe_cast(json_value(payload_json, '$.calculated.score') as int64) as calculated_score,

        ingested_at                                                     as _fivetran_synced
    from {{ source('raw_typeform', 'typeform_objects_raw') }}
    where entity_type = 'responses'

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by id
        order by _fivetran_synced desc
    ) = 1

),

parsed as (

    select
        id                                                              as response_id,
        form_id,
        token                                                           as response_token,

        response_type,
        landing_id,

        landed_at,
        submitted_at,
        staged_at,

        hidden_utm_source,
        hidden_utm_medium,
        hidden_utm_campaign,

        metadata_user_agent                                             as user_agent,
        metadata_platform                                               as platform,
        metadata_referer                                                as referrer,
        metadata_browser                                                as browser,
        metadata_network_id                                             as network_id,

        calculated_score,

        _fivetran_synced                                                as _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
