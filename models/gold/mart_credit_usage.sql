with base as (
    select
        company_id
    ,   plan_id
    ,   credits
    ,   created_at
    from {{ ref('fact_bookings') }}
    where created_at >= date_add('month', -3, current_date)
)

, agg as (
    select
        company_id
    ,   plan_id
    ,   sum(credits) as total_credits_used
    from base
    group by 1,2
)

, plans as (
    select
        plan_id
    ,   plan_type
    ,   monthly_credits
    from {{ ref('dim_plans') }}
)

, final as (
    select
        a.company_id
    ,   a.plan_id
    ,   p.plan_type
    ,   a.total_credits_used
    ,   p.monthly_credits
    ,   a.total_credits_used / p.monthly_credits as usage_rate
    ,   avg(a.total_credits_used / p.monthly_credits) over () as overall_avg
    from agg a
    left join plans p using (plan_id)
)