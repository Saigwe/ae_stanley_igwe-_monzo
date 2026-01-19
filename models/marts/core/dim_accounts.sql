select
  account_id,
  user_id,
  account_type,
  account_status,
  is_open,
  valid_from as status_updated_at
from {{ ref('dim_accounts_history') }}
where is_current = true
