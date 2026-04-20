-- Hard-fail singular test — any declared raw source >25h stale trips dbt test.
-- Complements `dbt source freshness` (warn-only in nightly) with a blocking
-- check that can gate CI / prod deploys. Column refs:
--   GHL sources use `_ingested_at` (custom extractor).
--   Fivetran sources use `_fivetran_synced`.

with
sources as (
    select 'raw_ghl.contacts'      as source_name, max(_ingested_at) as last_ingest from {{ source('ghl', 'contacts') }}
    union all
    select 'raw_ghl.conversations',                 max(_ingested_at) from {{ source('ghl', 'conversations') }}
    union all
    select 'raw_ghl.messages',                      max(_ingested_at) from {{ source('ghl', 'messages') }}
    union all
    select 'raw_ghl.opportunities',                 max(_ingested_at) from {{ source('ghl', 'opportunities') }}
    union all
    select 'raw_ghl.pipelines',                     max(_ingested_at) from {{ source('ghl', 'pipelines') }}
    union all
    select 'raw_ghl.users',                         max(_ingested_at) from {{ source('ghl', 'users') }}
    union all
    select 'raw_calendly.event',                    max(_fivetran_synced) from {{ source('raw_calendly', 'event') }}
    union all
    select 'raw_typeform.response',                 max(_fivetran_synced) from {{ source('raw_typeform', 'response') }}
    union all
    select 'raw_stripe.charge',                     max(_fivetran_synced) from {{ source('raw_stripe', 'charge') }}
)

select *
from sources
where timestamp_diff(current_timestamp(), last_ingest, hour) > 25
