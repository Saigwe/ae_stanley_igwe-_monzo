
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transactions_num
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_user_daily_activity`
where transactions_num is null



  
  
      
    ) dbt_internal_test