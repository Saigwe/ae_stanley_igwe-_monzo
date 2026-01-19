
  
    

    create or replace table `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts`
      
    
    

    
    OPTIONS()
    as (
      select
  account_id,
  user_id,
  account_type,
  account_status,
  is_open,
  valid_from as status_updated_at
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts_history`
where is_current = true
    );
  