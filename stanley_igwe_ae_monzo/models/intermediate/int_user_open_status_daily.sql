with account_states as (

    select
        account_id,
        user_id,
        date(valid_from) as valid_from_date,
        date(coalesce(valid_to, timestamp '9999-12-31')) as valid_to_date
    from {{ ref('accounts_history') }}
    where is_open = true

),

dates as (

    select distinct activity_date
    from {{ ref('stg_account_transactions') }}

),

user_open_by_day as (

    select distinct
        a.user_id,
        d.activity_date
    from account_states a
    join dates d
        on d.activity_date >= a.valid_from_date
       and d.activity_date < a.valid_to_date

)

select
    user_id,
    activity_date,
    true as has_open_account
from user_open_by_day
