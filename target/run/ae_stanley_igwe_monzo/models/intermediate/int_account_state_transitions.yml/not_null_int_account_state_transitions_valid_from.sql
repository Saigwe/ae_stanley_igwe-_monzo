
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valid_from
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_account_state_transitions`
where valid_from is null



  
  
      
    ) dbt_internal_test