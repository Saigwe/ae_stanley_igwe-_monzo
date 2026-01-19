
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select account_id
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_account_state_transitions`
where account_id is null



  
  
      
    ) dbt_internal_test