-- U3 column-rename (2026-04-23): see stg_ghl__contacts.sql header.
with source as (

    select
        entity_id                                                       as id,
        _ingested_at,
        to_json_string(payload_json)                               as payload
    from `project-41542e21-470f-4589-96d`.`raw_ghl_v2`.`ghl__pipelines_raw`

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