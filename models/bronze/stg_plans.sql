with source as (
    select * from {{ source('raw', 'plans') }}
)

, final as (
    select
        plan_id
    ,   plan_name
    ,   plan_type
    ,   monthly_credits
    ,   plan_value
    ,   created_at
    from source
)

select * from final