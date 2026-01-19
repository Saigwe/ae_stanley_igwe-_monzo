
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select closed_at
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`stg_account_closed`
where closed_at is null



  
  
      
    ) dbt_internal_test