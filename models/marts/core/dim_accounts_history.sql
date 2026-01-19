select
  account_id,
  user_id,
  account_type,
  account_status,
  valid_from,
  valid_to,
  account_status = 'open' as is_open,
  valid_to is null as is_current
from {{ ref('int_account_state_transitions') }}
