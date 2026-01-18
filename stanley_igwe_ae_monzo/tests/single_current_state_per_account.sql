-- Exactly one current row per account

select
  account_id
from {{ ref('dim_accounts_history') }}
where is_current
group by account_id
having count(*) != 1