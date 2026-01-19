

  create or replace view `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`stg_account_transactions`
  OPTIONS()
  as select
  account_id_hashed as account_id,
  date as activity_date,
  transactions_num
from `analytics-take-home-test`.`monzo_datawarehouse`.`account_transactions`;

