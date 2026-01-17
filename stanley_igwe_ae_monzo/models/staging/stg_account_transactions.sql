select
    account_id_hashed as account_id,
    date as activity_date,
    transactions_num
from {{ source('monzo_datawarehouse', 'account_transactions') }}
