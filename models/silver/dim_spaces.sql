{{ config(
    materialized='incremental',
    unique_key='space_id',
    incremental_strategy='merge'
) }}

with source as (
    select * from {{ ref('stg_spaces') }}
)

, final as (
    select
        space_id
    ,   space_name
    ,   category
    ,   tier
    ,   total_seats
    ,   current_timestamp as updated_at
    from 
        source
)

select * from final