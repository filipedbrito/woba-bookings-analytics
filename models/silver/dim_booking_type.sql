{{ config(
    materialized='table'
) }}

with source as (
    select 
        distinct booking_type_id
    from {{ ref('stg_bookings') }}
)

, final as (
    select
        booking_type_id
    ,   case
            when booking_type_id = 1 then 'on_demand'
            when booking_type_id = 2 then 'scheduled'
            when booking_type_id = 3 then 'recurring'
            else 'unknown'
        end as booking_type_name
    from source
)

select * from final