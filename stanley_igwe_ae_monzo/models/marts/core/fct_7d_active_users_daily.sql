with calendar as (

    select distinct activity_date
    from {{ ref('stg_account_transactions') }}

),

eligible_users as (

    select
        activity_date,
        count(distinct user_id) as eligible_users
    from {{ ref('int_user_open_status_daily') }}
    group by 1

),

active_users as (

    select
        u.activity_date,
        count(distinct u.user_id) as active_users
    from {{ ref('int_user_daily_activity') }} u
    where u.is_active = true
      and u.activity_date >= date_sub(u.activity_date, interval 6 day)
    group by 1

)

select
    c.activity_date,
    e.eligible_users,
    a.active_users,
    safe_divide(a.active_users, e.eligible_users) as active_rate_7d
from calendar c
left join eligible_users e
    on c.activity_date = e.activity_date
left join active_users a
    on c.activity_date = a.activity_date
order by c.activity_date
