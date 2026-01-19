
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select activity_date
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_user_open_status_daily`
where activity_date is null



  
  
      
    ) dbt_internal_test