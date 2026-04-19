with source as (

    select
        id,
        _ingested_at,
        payload
    from {{ source('ghl', 'opportunities') }}

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by id
        order by _ingested_at desc
    ) = 1

),

parsed as (

    select
        id                                                          as opportunity_id,

        json_value(payload, '$.name')                               as opportunity_name,
        json_value(payload, '$.status')                             as status,
        json_value(payload, '$.source')                             as lead_source,

        json_value(payload, '$.pipelineId')                         as pipeline_id,
        json_value(payload, '$.pipelineStageId')                    as pipeline_stage_id,

        json_value(payload, '$.contactId')                          as contact_id,
        json_value(payload, '$.assignedTo')                         as assigned_user_id,
        json_value(payload, '$.locationId')                         as location_id,
        json_value(payload, '$.lostReasonId')                       as lost_reason_id,

        cast(json_value(payload, '$.monetaryValue') as numeric)     as monetary_value,
        cast(json_value(payload, '$.forecastProbability') as numeric) as forecast_probability,
        cast(json_value(payload, '$.effectiveProbability') as numeric) as effective_probability,

        cast(json_value(payload, '$.createdAt') as timestamp)       as opportunity_created_at,
        cast(json_value(payload, '$.updatedAt') as timestamp)       as opportunity_updated_at,
        cast(json_value(payload, '$.lastStatusChangeAt') as timestamp) as last_status_change_at,
        cast(json_value(payload, '$.lastStageChangeAt') as timestamp)  as last_stage_change_at,

        _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
