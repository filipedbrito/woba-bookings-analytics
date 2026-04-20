{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ ref('stg_companies') }}
)

, final as (
    select
        company_id
    ,   company_name
    ,   industry
    ,   employee_count
    from source
)

select * from final