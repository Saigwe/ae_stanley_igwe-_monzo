
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`int_account_lifecycle_events`
    group by event_type

)

select *
from all_values
where value_field not in (
    'created','closed','reopened'
)


