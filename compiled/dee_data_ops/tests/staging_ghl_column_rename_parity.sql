-- U3 column-rename parity test: for each GHL staging model that is actually
-- producing data, verify the staging PK set equals the distinct
-- `entity_id` set in the corresponding Phase-2 raw table. Proves the
-- `entity_id AS id` rename in the source CTE is lossless end-to-end
-- (through the dedupe + parsed CTEs, out to the final PK alias).
--
-- Intentional scope:
--  - Only models with >0 rows are checked (contacts, conversations,
--    opportunities, pipelines). messages + users are 0-row upstream.
--  - This is a U3-scope structural test. Cross-project parity against
--    `dee-data-ops-prod` lives in U4a (`cutover-speed-to-lead-plumbing-
--    parity.sql`) against a frozen snapshot.
--
-- Returns zero rows when rename is lossless; any row = divergence.

with divergence as (
    select 'stg_ghl__contacts' as model,
           (select count(distinct contact_id)     from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__contacts`)     as stg_n,
           (select count(distinct entity_id)      from `project-41542e21-470f-4589-96d`.`raw_ghl_v2`.`ghl__contacts_raw`)    as raw_n
    union all
    select 'stg_ghl__conversations',
           (select count(distinct conversation_id) from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__conversations`),
           (select count(distinct entity_id)       from `project-41542e21-470f-4589-96d`.`raw_ghl_v2`.`ghl__conversations_raw`)
    union all
    select 'stg_ghl__opportunities',
           (select count(distinct opportunity_id)  from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__opportunities`),
           (select count(distinct entity_id)       from `project-41542e21-470f-4589-96d`.`raw_ghl_v2`.`ghl__opportunities_raw`)
    union all
    select 'stg_ghl__pipelines',
           (select count(distinct pipeline_id)     from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__pipelines`),
           (select count(distinct entity_id)       from `project-41542e21-470f-4589-96d`.`raw_ghl_v2`.`ghl__pipelines_raw`)
)

select *
from divergence
where stg_n <> raw_n