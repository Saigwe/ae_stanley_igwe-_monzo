
    
    



select user_id
from (select * from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_account_lifecycle_events` where event_type = 'created') dbt_subquery
where user_id is null


