select
  account_id_hashed as account_id,
  user_id_hashed as user_id,
  account_type,
  created_ts as created_at
from `analytics-take-home-test`.`monzo_datawarehouse`.`account_created`