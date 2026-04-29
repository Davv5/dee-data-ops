-- One-row rollup exposing the latest booking timestamp on sales_activity_detail
-- plus the run timestamp of the dbt build. Feeds the "Data as of HH:MM"
-- freshness number card on the Speed-to-Lead dashboard.
--
-- Grain: exactly 1 row. Materialized as a table so Metabase's result cache +
-- BigQuery's native result cache both get a stable identity to key off.
--
-- Why max(booked_at), not current_timestamp(): per Metabase Learn
-- "BI Dashboard Visualization Best Practices" (corpus: Metabase Learn
-- notebook), a freshness card should show "the latest value of a timestamp
-- column that represents the data's natural reporting cadence" — NOT the
-- query execution time. current_timestamp() would be a tautology (it is
-- always "now"). max(booked_at) tells the viewer the latest event the
-- mart has seen, which is what "data is current as of X" really means.


select
    max(booked_at)      as last_booking_at,
    current_timestamp() as run_ts
from `project-41542e21-470f-4589-96d`.`Marts`.`sales_activity_detail`