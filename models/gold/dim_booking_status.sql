{{ config(
    materialized='table'
) }}

with source as (
    select 
        distinct status_id
    from {{ ref('stg_bookings') }}
)

, final as (
    select
        status_id
    ,   case 
            when status_id = 1 then 'scheduled'
            when status_id = 2 then 'confirmed'
            when status_id = 3 then 'cancelled'
            when status_id = 4 then 'no_show'
            else 'unknown'
        end as status_name
    from source
)

select * from final