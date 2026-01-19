
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- User cannot be eligible without an open account

select *
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_user_open_status_daily`
where has_open_account = true
  and open_accounts_count = 0
  
  
      
    ) dbt_internal_test