-- active_users ≤ eligible_users

select *
from {{ ref('fct_7d_active_users_daily') }}
where active_users > eligible_users