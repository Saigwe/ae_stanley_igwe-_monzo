
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_open
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts`
where is_open is null



  
  
      
    ) dbt_internal_test