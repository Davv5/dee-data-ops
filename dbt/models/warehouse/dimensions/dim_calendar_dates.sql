with

date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="date_add(current_date(), interval 1 day)"
    ) }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['date_day']) }}   as date_sk,

        cast(date_day as date)                                 as date_day,

        extract(year      from date_day)                      as year_number,
        extract(quarter   from date_day)                      as quarter_number,
        extract(month     from date_day)                      as month_number,
        extract(week      from date_day)                      as week_number,
        extract(day       from date_day)                      as day_of_month,
        extract(dayofweek from date_day)                      as day_of_week,
        extract(dayofyear from date_day)                      as day_of_year,

        format_date('%A',    date_day)                        as day_name,
        format_date('%B',    date_day)                        as month_name,
        format_date('%Y-%m', date_day)                        as year_month,
        format_date('%Y-Q%Q', date_day)                       as year_quarter,

        date_trunc(date_day, week(monday))                    as week_start_date,
        date_trunc(date_day, month)                           as month_start_date,
        date_trunc(date_day, quarter)                         as quarter_start_date,
        date_trunc(date_day, year)                            as year_start_date,

        case
            when extract(dayofweek from date_day) in (1, 7) then false
            else true
        end                                                   as is_weekday

    from date_spine

)

select * from final
