
    
    

with dbt_test__target as (

  select account_id as unique_field
  from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`stg_account_created`
  where account_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


