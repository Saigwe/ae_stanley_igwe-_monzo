
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select eligible_users
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`fct_7d_active_users_daily`
where eligible_users is null



  
  
      
    ) dbt_internal_test