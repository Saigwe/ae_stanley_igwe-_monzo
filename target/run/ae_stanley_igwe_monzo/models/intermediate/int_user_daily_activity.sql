

  create or replace view `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_user_daily_activity`
  OPTIONS()
  as with transactions as (

  select
    account_id,
    activity_date,
    transactions_num
  from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`stg_account_transactions`

),

accounts as (

  select
    account_id,
    user_id
  from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts`
),

user_activity as (

  select
    a.user_id,
    t.activity_date,
    sum(t.transactions_num) as transactions_num
  from transactions as t
  inner join accounts as a
    on t.account_id = a.account_id
  group by 1, 2

)

select
  user_id,
  activity_date,
  transactions_num,
  transactions_num > 0 as is_active
from user_activity;

