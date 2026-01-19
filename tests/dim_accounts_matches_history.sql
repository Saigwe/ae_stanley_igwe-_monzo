-- dim_accounts matches history’s current state

select
  d.account_id
from {{ ref('dim_accounts') }} d
join {{ ref('dim_accounts_history') }} h
  on d.account_id = h.account_id
where h.is_current
  and d.account_status != h.account_status