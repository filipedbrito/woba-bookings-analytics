select *
from {{ ref('fact_bookings') }}
where is_cancelled = true
  and check_in_datetime is not null