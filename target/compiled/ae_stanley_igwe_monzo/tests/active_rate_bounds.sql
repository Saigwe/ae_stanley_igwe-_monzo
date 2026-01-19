-- active_rate_7d is between 0 and 1

select *
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`fct_7d_active_users_daily`
where active_rate_7d < 0
   or active_rate_7d > 1