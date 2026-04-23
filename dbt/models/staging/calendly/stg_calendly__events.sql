-- Track X (2026-04-22): dual-run overlap update.
--
-- During the overlap window both Fivetran (raw_calendly.event) and the Cloud
-- Run poller (raw_calendly.scheduled_events) write Calendly events. This view
-- unions both sources and deduplicates by uri, keeping the row with the most
-- recent effective timestamp (coalesce(_ingested_at, _fivetran_synced)). A
-- `_source_path` column identifies which pipeline wrote each row — used for
-- 24h reconciliation queries before pausing Fivetran.
--
-- After Fivetran is paused: remove the union + coalesce, keep only the
-- scheduled_events source and _ingested_at. (Track X runbook step.)
--
-- Corpus grounding: append-only + staging dedupe is the idempotency contract;
-- both sources sharing raw_calendly.* means overlap is handled here, not at
-- ingest time. Source: ".claude/rules/ingest.md", Data Ops notebook.

with

fivetran_source as (

    select
        uri,
        event_type_uri,
        name,
        status,
        location_type,
        start_time,
        end_time,
        created_at,
        updated_at,
        cancel_reason,
        canceled_by,
        canceler_type,
        invitees_active,
        invitees_limit,
        coalesce(_fivetran_deleted, false)                              as is_deleted,
        cast(null as timestamp)                                         as _ingested_at,
        cast(_fivetran_synced as timestamp)                             as _fivetran_synced,
        'fivetran'                                                      as _source_path
    from {{ source('raw_calendly', 'event') }}

),

poller_source as (

    -- Cloud Run poller writes payload-as-JSON; extract fields with JSON_VALUE.
    -- Column list mirrors fivetran_source for UNION compatibility.
    select
        json_value(payload, '$.uri')                                    as uri,
        json_value(payload, '$.event_type.uri')                         as event_type_uri,
        json_value(payload, '$.name')                                   as name,
        json_value(payload, '$.status')                                 as status,
        json_value(payload, '$.location.type')                          as location_type,
        cast(json_value(payload, '$.start_time') as timestamp)          as start_time,
        cast(json_value(payload, '$.end_time') as timestamp)            as end_time,
        cast(json_value(payload, '$.created_at') as timestamp)          as created_at,
        cast(json_value(payload, '$.updated_at') as timestamp)          as updated_at,
        json_value(payload, '$.cancellation.reason')                    as cancel_reason,
        json_value(payload, '$.cancellation.cancelled_by')              as canceled_by,
        json_value(payload, '$.cancellation.canceler_type')             as canceler_type,
        safe_cast(json_value(payload, '$.invitees_counter.active') as int64) as invitees_active,
        safe_cast(json_value(payload, '$.invitees_counter.limit') as int64)  as invitees_limit,
        false                                                           as is_deleted,
        _ingested_at,
        cast(null as timestamp)                                         as _fivetran_synced,
        'cloud_run'                                                     as _source_path
    from {{ source('raw_calendly', 'scheduled_events') }}

),

combined as (

    select * from fivetran_source
    union all
    select * from poller_source

),

deduped as (

    -- Dedup by uri: keep the row with the most recent effective timestamp.
    -- coalesce prefers _ingested_at (Cloud Run) over _fivetran_synced (Fivetran),
    -- which is correct — the poller has higher cadence and more recent data.
    select *
    from combined
    qualify row_number() over (
        partition by uri
        order by coalesce(_ingested_at, _fivetran_synced) desc
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

        is_deleted,

        coalesce(_ingested_at, _fivetran_synced)                        as _ingested_at,
        _source_path
    from deduped

),

final as (

    select * from parsed

)

select * from final
