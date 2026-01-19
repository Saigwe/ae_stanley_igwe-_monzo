

  create or replace view `analytics-take-home-test`.`AE_Stan_Igwe_Task_azigw`.`stg_account_created`
  OPTIONS()
  as select
  account_id_hashed as account_id,
  user_id_hashed as user_id,
  account_type,
  created_ts as created_at
from `analytics-take-home-test`.`monzo_datawarehouse`.`account_created`;

