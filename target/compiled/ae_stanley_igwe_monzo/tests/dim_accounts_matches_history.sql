-- dim_accounts matches history’s current state

select
  d.account_id
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts` d
join `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts_history` h
  on d.account_id = h.account_id
where h.is_current
  and d.account_status != h.account_status