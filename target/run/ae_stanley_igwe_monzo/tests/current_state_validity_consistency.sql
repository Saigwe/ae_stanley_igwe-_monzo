
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- valid_to is null iff is_current = true

select *
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts_history`
where
  (is_current and valid_to is not null)
  or
  (not is_current and valid_to is null)
  
  
      
    ) dbt_internal_test