-- Singular test: (booking_id, touch_key) must be unique in speed_to_lead_detail.
-- touch_key = coalesce(cast(touched_at as string), 'no-touch') so that
-- no-touch rows (touched_at = NULL) are treated as a single unique row per
-- booking, not as a group of duplicates.
--
-- Returns rows on failure (any duplicate combination). Zero rows = grain is clean.

select
    booking_id,
    coalesce(cast(touched_at as string), 'no-touch') as touch_key,
    count(*)                                         as n
from `project-41542e21-470f-4589-96d`.`Marts`.`speed_to_lead_detail`
group by
    booking_id,
    coalesce(cast(touched_at as string), 'no-touch')
having count(*) > 1