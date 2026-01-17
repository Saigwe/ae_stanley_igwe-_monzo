with calendar as (

    select distinct activity_date
    from {{ ref('stg_account_transactions') }}

),

eligible_users as (

    select
        activity_date,
        count(distinct user_id) as eligible_users
    from {{ ref('int_user_open_status_daily') }}
    group by activity_date

),

user_activity_spine as (

    select
        c.activity_date,
        u.user_id,
        coalesce(a.is_active, false) as is_active
    from calendar c
    join (
        select distinct user_id
        from {{ ref('int_user_open_status_daily') }}
    ) u
        on true
    left join {{ ref('int_user_daily_activity') }} a
        on a.user_id = u.user_id
       and a.activity_date = c.activity_date

),

rolling_activity as (

    select
        activity_date,
        user_id,

        max(cast(is_active as int64)) over (
            partition by user_id
            order by activity_date
            rows between 6 preceding and current row
        ) as is_active_7d

    from user_activity_spine

),

active_users as (

    select
        activity_date,
        count(distinct user_id) as active_users
    from rolling_activity
    where is_active_7d = 1
    group by activity_date

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
