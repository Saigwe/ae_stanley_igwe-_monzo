select
  account_id_hashed as account_id,
  date as activity_date,
  transactions_num
from `analytics-take-home-test`.`monzo_datawarehouse`.`account_transactions`