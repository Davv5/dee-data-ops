with

date_spine as (

    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
    
    

    )

    select *
    from unioned
    where generated_number <= 852
    order by generated_number



),

all_periods as (

    select (
        

        datetime_add(
            cast( cast('2024-01-01' as date) as datetime),
        interval row_number() over (order by generated_number) - 1 day
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= date_add(current_date(), interval 1 day)

)

select * from filtered



),

final as (

    select
        to_hex(md5(cast(coalesce(cast(date_day as string), '_dbt_utils_surrogate_key_null_') as string)))   as date_sk,

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