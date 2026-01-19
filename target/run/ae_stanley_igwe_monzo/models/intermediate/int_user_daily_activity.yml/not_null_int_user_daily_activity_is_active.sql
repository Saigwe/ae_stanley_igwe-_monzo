
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_active
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_user_daily_activity`
where is_active is null



  
  
      
    ) dbt_internal_test