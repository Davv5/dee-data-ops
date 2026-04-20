with

source as (

    select * from {{ source('raw_typeform', 'response') }}

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
