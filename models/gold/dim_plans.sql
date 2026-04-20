{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ ref('stg_plans') }}
)

, final as (
    select
        plan_id
    ,   plan_name
    ,   plan_type
    ,   monthly_credits
    ,   plan_value
    from source
)

select * from final