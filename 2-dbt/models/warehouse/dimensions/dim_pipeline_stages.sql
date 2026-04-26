with pipelines as (

    select
        pipeline_id,
        pipeline_name,
        show_in_funnel,
        stages_json
    from {{ ref('stg_ghl__pipelines') }}

),

stages_unnested as (

    select
        pipeline_id,
        pipeline_name,
        show_in_funnel,
        stage_json
    from pipelines,
    unnest(json_query_array(stages_json)) as stage_json

),

parsed as (

    select
        pipeline_id,
        pipeline_name,
        show_in_funnel,

        json_value(stage_json, '$.id')                          as stage_id,
        json_value(stage_json, '$.name')                        as stage_name,
        cast(json_value(stage_json, '$.position') as int64)     as stage_position

    from stages_unnested

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['pipeline_id', 'stage_id']) }}
                                                                as pipeline_stage_sk,

        pipeline_id,
        pipeline_name,
        stage_id,
        stage_name,
        stage_position,

        case
            when lower(stage_name) like '%booked%'                              then true
            when stage_name in ('Set', 'Set/Triage', 'Call Booked', 'Booked Call') then true
            else false
        end                                                     as is_booked_stage,

        show_in_funnel

    from parsed

)

select * from final
