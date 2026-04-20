with source as (

    select
        id,
        _ingested_at,
        payload
    from {{ source('ghl', 'pipelines') }}

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
        id                                                              as pipeline_id,

        json_value(payload, '$.name')                                   as pipeline_name,
        json_value(payload, '$.locationId')                             as location_id,

        cast(json_value(payload, '$.showInFunnel') as bool)             as show_in_funnel,
        cast(json_value(payload, '$.showInPieChart') as bool)           as show_in_pie_chart,

        json_query(payload, '$.stages')                                 as stages_json,

        _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
