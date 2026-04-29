-- Hard-fail singular test — any declared raw source >25h stale trips dbt test.
-- Complements `dbt source freshness` (warn-only in nightly) with a blocking
-- check that can gate CI / prod deploys.
--
-- Post-U3 (2026-04-23): GHL and Stripe sources are temporarily excluded
-- from the hard-fail list during the consolidation window.
--
--  - GHL: `bq-ingest` Cloud Run service is partially broken (U1 preflight
--    §13); Phase-2 `raw_ghl.*` last wrote 2026-04-19 14:33 and has not
--    recovered. Fix belongs to GTM's repo and is a U4b precondition, not
--    a U3 blocker. Accepted-as-is for U2–U4a per preflight sign-off.
--  - Stripe: `Raw.stripe_objects_raw` has been ~50 days stale since at
--    least 2026-03-04 (U1 preflight §10 — `stripe-backfill` Cloud Run
--    Job failing daily). Tracked in U7/U8; not cutover-blocking.
--
-- Calendly and Typeform blob sources ARE included — GTM's extractors
-- write to them hourly and both were fresh at U1 preflight. Re-add GHL
-- to this list when `bq-ingest` is repaired and the raw catches up.

with
sources as (
    select 'raw_calendly'                          as source_name,
           max(ingested_at)                        as last_ingest
    from `project-41542e21-470f-4589-96d`.`Raw`.`calendly_objects_raw`
    where entity_type = 'scheduled_events'
    union all
    select 'raw_typeform',
           max(ingested_at)
    from `project-41542e21-470f-4589-96d`.`Raw`.`typeform_objects_raw`
    where entity_type = 'responses'
)

select *
from sources
where timestamp_diff(current_timestamp(), last_ingest, hour) > 25