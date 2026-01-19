
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select activity_date
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`stg_account_transactions`
where activity_date is null



  
  
      
    ) dbt_internal_test