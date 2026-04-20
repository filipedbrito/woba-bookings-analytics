with source as (
    select * from {{ source('bronze', 'companies') }}
)

, final as (
    select
        company_id
    ,   company_name
    ,   industry
    ,   employee_count
    ,   created_at
    from source
)

select * from final