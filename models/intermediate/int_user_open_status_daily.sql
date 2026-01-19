with account_states as (

  select
    account_id,
    user_id,
    date(valid_from) as valid_from_date,
    date(coalesce(valid_to, timestamp '9999-12-31')) as valid_to_date
  from {{ ref('dim_accounts_history') }}
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
  from account_states as a
  inner join dates as d
    on
      a.valid_from_date <= d.activity_date
      and a.valid_to_date > d.activity_date

)

select
  user_id,
  activity_date,
  true as has_open_account
from user_open_by_day
