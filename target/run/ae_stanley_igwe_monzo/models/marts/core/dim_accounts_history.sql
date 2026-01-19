
  
    

    create or replace table `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts_history`
      
    
    

    
    OPTIONS()
    as (
      select
  account_id,
  user_id,
  account_type,
  account_status,
  valid_from,
  valid_to,
  account_status = 'open' as is_open,
  valid_to is null as is_current
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_account_state_transitions`
    );
  