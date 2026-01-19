
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status_updated_at
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts`
where status_updated_at is null



  
  
      
    ) dbt_internal_test