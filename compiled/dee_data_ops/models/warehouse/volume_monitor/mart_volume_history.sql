

-- Daily row-count snapshot per mart. One row per (mart, day). Feeds the
-- volume_drift warn-test which compares today's count against prior-day.
-- Marts refd here ship from Tracks F (sales_activity_detail), L (lead_journey),
-- M (revenue_detail); until all three are on main, this model will fail to
-- compile and should be excluded from CI with --exclude mart_volume_history.

with
snapshot as (
    select current_date() as snapshot_date, 'sales_activity_detail' as mart_name, count(*) as row_count
    from `project-41542e21-470f-4589-96d`.`Marts`.`sales_activity_detail`
    union all
    select current_date(), 'lead_journey', count(*)
    from `project-41542e21-470f-4589-96d`.`Marts`.`lead_journey`
    union all
    select current_date(), 'revenue_detail', count(*)
    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_detail`
)

select *
from snapshot

where snapshot_date > (select coalesce(max(snapshot_date), date '1900-01-01') from `project-41542e21-470f-4589-96d`.`warehouse`.`mart_volume_history`)
