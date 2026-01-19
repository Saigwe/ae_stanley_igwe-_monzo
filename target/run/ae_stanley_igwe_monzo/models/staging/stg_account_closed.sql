

  create or replace view `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`stg_account_closed`
  OPTIONS()
  as select
  account_id_hashed as account_id,
  closed_ts as closed_at
from `analytics-take-home-test`.`monzo_datawarehouse`.`account_closed`;

