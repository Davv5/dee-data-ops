{{ config(severity='warn') }}

-- Warn-severity singular test: any mart's daily row count >10% off prior-day
-- baseline returns a row. Severity=warn means it does NOT fail the build —
-- the nightly workflow runs this test separately with --warn-error to detect
-- the warn and post a Slack soft-alert. Skips the first day (prior_count null).

with
daily as (
    select
        mart_name,
        snapshot_date,
        row_count,
        lag(row_count) over (partition by mart_name order by snapshot_date) as prior_count
    from {{ ref('mart_volume_history') }}
),

drift as (
    select
        mart_name,
        snapshot_date,
        row_count,
        prior_count,
        safe_divide(abs(row_count - prior_count), prior_count) as pct_drift
    from daily
    where snapshot_date = current_date()
      and prior_count is not null
)

select *
from drift
where pct_drift > 0.10
