-- User cannot be eligible without an open account

select *
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_user_open_status_daily`
where has_open_account = true
  and open_accounts_count = 0