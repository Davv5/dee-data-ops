-- Every opportunity-bearing GHL pipeline should be classified before it powers
-- lead-magnet reporting. New uncategorized pipelines should fail loudly so the
-- dashboard does not compare unlike funnels by accident.

with

missing_taxonomy as (

    select distinct
        lead_magnet_id,
        lead_magnet_name
    from {{ ref('lead_magnet_detail') }}
    where taxonomy_confidence = 'missing_taxonomy'

)

select *
from missing_taxonomy
