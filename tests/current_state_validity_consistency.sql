-- valid_to is null iff is_current = true

select *
from {{ ref('dim_accounts_history') }}
where
  (is_current and valid_to is not null)
  or
  (not is_current and valid_to is null)