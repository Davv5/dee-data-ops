-- U3 sanity test: each blob-shim staging model decodes at least a minimum
-- row count from its upstream blob filter. Prevents a silent-zero regression
-- where (for example) a wrong `object_type` / `entity_type` literal leaves
-- every stg model empty.
--
-- Thresholds are ground-truth row counts captured at U1 preflight
-- (2026-04-23). They are floors, not ceilings — new data landing is fine.
--
-- Excluded:
--  - stg_ghl__messages / stg_ghl__users (0 rows in raw, known upstream gap)
--  - stg_typeform__responses zero-check relies on the blob too, included
--
-- Returns zero rows when all shims are producing data; any row = regression.

with counts as (
    select 'stg_stripe__charges'    as model, count(*) as n, 3000  as floor from {{ ref('stg_stripe__charges') }}
    union all
    select 'stg_stripe__customers',              count(*), 500          from {{ ref('stg_stripe__customers') }}
    union all
    select 'stg_typeform__responses',            count(*), 4000         from {{ ref('stg_typeform__responses') }}
    union all
    select 'stg_fathom__calls',                  count(*), 1000         from {{ ref('stg_fathom__calls') }}
    union all
    select 'stg_calendly__events',               count(*), 3000         from {{ ref('stg_calendly__events') }}
    union all
    select 'stg_calendly__event_invitees',       count(*), 3000         from {{ ref('stg_calendly__event_invitees') }}
    union all
    select 'stg_calendly__event_types',          count(*), 1            from {{ ref('stg_calendly__event_types') }}
    union all
    select 'stg_ghl__contacts',                  count(*), 10000        from {{ ref('stg_ghl__contacts') }}
    union all
    select 'stg_ghl__opportunities',             count(*), 20000        from {{ ref('stg_ghl__opportunities') }}
    union all
    select 'stg_ghl__pipelines',                 count(*), 20           from {{ ref('stg_ghl__pipelines') }}
    union all
    select 'stg_ghl__conversations',             count(*), 1            from {{ ref('stg_ghl__conversations') }}
)

select *
from counts
where n < floor
