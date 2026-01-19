
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reopened_at
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`stg_account_reopened`
where reopened_at is null



  
  
      
    ) dbt_internal_test