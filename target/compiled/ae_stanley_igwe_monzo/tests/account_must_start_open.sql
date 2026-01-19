-- Meaning: Every account must start life as open.

select
  account_id
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts_history`
group by account_id
having min(valid_from) != min(case when account_status = 'open' then valid_from end)