
    
    

with dbt_test__target as (

  select activity_date as unique_field
  from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`fct_7d_active_users_daily`
  where activity_date is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


