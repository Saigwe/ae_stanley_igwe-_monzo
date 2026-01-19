
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_open_account
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_user_open_status_daily`
where has_open_account is null



  
  
      
    ) dbt_internal_test