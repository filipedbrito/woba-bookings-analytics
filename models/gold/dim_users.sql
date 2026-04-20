{{ config(
    materialized='table'
) }}

with source as (
    select distinct
        user_id
    from {{ ref('stg_bookings') }}
)

, final as (
    select
        user_id
    from source
)

select * from final