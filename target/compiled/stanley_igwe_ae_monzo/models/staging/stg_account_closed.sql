select
  account_id_hashed as account_id,
  closed_ts as closed_at
from `analytics-take-home-test`.`monzo_datawarehouse`.`account_closed`