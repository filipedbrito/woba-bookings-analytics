with source as (
    select * from {{ source('raw', 'bookings') }}
)

, clean as (
    select
        booking_id
    ,   space_id
    ,   user_id
    ,   company_id
    ,   plan_id
    ,   booking_type_id
    ,   status_id
    ,   credits
    ,   credits_in_money
    ,   check_in_datetime
    ,   check_out_datetime
    ,   is_cancelled
    ,   is_no_show
    ,   created_at
    from 
        source
)

select * from clean