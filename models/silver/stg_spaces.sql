with source as (
    select * from {{ source('bronze', 'spaces') }}
)

, final as (
    select
        space_id
    ,   space_name
    ,   category
    ,   tier
    ,   total_seats
    ,   created_at
    from source
)

select * from final