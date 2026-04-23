-- Track X (2026-04-22): dual-run overlap update.
--
-- During the dual-run overlap window both Fivetran (raw_calendly.event) and the Cloud
-- Run poller (raw_calendly.scheduled_events) write Calendly events. This view
-- unions both sources and deduplicates by uri, keeping the row with the most
-- recent effective timestamp (coalesce(_ingested_at, _fivetran_synced)). A
-- `_source_path` column identifies which pipeline wrote each row -- used for
-- 24h reconciliation queries before pausing Fivetran.
--
-- After Fivetran is paused: remove the union + coalesce, keep only the
-- scheduled_events source and _ingested_at. (Track X runbook step.)
--
-- Corpus grounding: append-only + staging dedupe is the idempotency contract;
-- both sources sharing raw_calendly.* means overlap is handled here, not at
-- ingest time. Source: ".claude/rules/ingest.md", Data Ops notebook.
--
-- Track X regression fix (2026-04-23): `raw_calendly.scheduled_events` is
-- created by the Cloud Run poller on its first successful write (WRITE_APPEND
-- + CREATE_IF_NEEDED in ingestion/calendly/extract.py). Before that write,
-- the table does not exist -- which broke the prod `dbt build` immediately
-- after the Track X merge with:
--   Not found: Table dee-data-ops:raw_calendly.scheduled_events was not
--   found in location US
--
-- Fix: gate the poller_source CTE behind `adapter.get_relation()`. When the
-- poller table exists, staging unions both pipelines (dual-run behavior as
-- designed). When it does not exist yet, staging falls back to the Fivetran
-- source alone -- same shape as pre-Track X, but emitting the same output
-- columns (`_source_path`, nullable `_ingested_at`) so downstream models
-- don't care. Once the poller has its first successful run, subsequent dbt
-- invocations pick it up automatically with no staging-model edit needed.

{% set poller_relation = adapter.get_relation(
    database=source('raw_calendly', 'scheduled_events').database,
    schema=source('raw_calendly', 'scheduled_events').schema,
    identifier=source('raw_calendly', 'scheduled_events').identifier
) %}

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

){%- if poller_relation is not none %},

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

){%- endif %},

combined as (

    select * from fivetran_source
    {%- if poller_relation is not none %}
    union all
    select * from poller_source
    {%- endif %}

),

deduped as (

    -- Dedup by uri: keep the row with the most recent effective timestamp.
    -- coalesce prefers _ingested_at (Cloud Run) over _fivetran_synced (Fivetran),
    -- which is correct -- the poller has higher cadence and more recent data.
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
