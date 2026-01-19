-- Expected result: 0 rows.
-- What this proves: SCD logic is sound.

select
  account_id
from {{ ref('dim_accounts_history') }}
qualify
  count(*) over (
    partition by account_id
    order by valid_from
    rows between current row and 1 following
  ) > 1
  and valid_to > lead(valid_from) over (
    partition by account_id
    order by valid_from
  )