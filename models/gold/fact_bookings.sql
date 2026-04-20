{{ config(
    materialized='incremental',
    unique_key='booking_id',
    incremental_strategy='merge'
) }}

with bookings as (
    select *
    from {{ ref('stg_bookings') }}
    {{ incremental_filter('created_at') }}
)

, final as (
    select
        booking_id
    ,   space_id
    ,   company_id
    ,   user_id
    ,   plan_id
    ,   booking_type_id
    ,   status_id
    ,   credits
    ,   credits_in_money
    ,   check_in_datetime
    ,   check_out_datetime
    ,   created_at
    from bookings
)

select * from final