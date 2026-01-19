
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Exactly one current row per account

select
  account_id
from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`dim_accounts_history`
where is_current
group by account_id
having count(*) != 1
  
  
      
    ) dbt_internal_test