{{ config(
    materialized='table',
    tags=['static']
) }}

with dates as (
    select
        date_add('day', n, date '2017-01-01') as date
    from unnest(sequence(0, 5000)) as t(n)
)

, final as (
    select
        date
    ,   year(date) as year
    ,   month(date) as month
    ,   day(date) as day
    ,   week_of_year(date) as week_of_year
    ,   quarter(date) as quarter
    ,   day_of_week(date) as day_of_week
    ,   case 
            when day_of_week(date) in (6,7) then true
            else false
        end as is_weekend
    ,   date_trunc('month', date) as month_start
    ,   date_trunc('month', date) + interval '1' month - interval '1' day as month_end
    ,   case 
            when date = date_trunc('month', date) then true
            else false
        end as is_month_start
    ,   case 
            when date = (date_trunc('month', date) + interval '1' month - interval '1' day) then true
            else false
        end as is_month_end
)

select * from final