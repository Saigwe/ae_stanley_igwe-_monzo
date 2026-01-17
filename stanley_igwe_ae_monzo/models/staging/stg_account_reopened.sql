select
    account_id_hashed as account_id,
    reopened_ts as reopened_at
from {{ source('monzo_datawarehouse', 'account_reopened') }}
