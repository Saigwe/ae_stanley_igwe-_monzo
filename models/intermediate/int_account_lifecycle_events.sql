with created as (

  select
    account_id,
    user_id,
    account_type,
    created_at as event_at,
    'created' as event_type
  from {{ ref('stg_account_created') }}

),

closed as (

  select
    account_id,
    cast(null as string) as user_id,
    cast(null as string) as account_type,
    closed_at as event_at,
    'closed' as event_type
  from {{ ref('stg_account_closed') }}

),

reopened as (

  select
    account_id,
    cast(null as string) as user_id,
    cast(null as string) as account_type,
    reopened_at as event_at,
    'reopened' as event_type
  from {{ ref('stg_account_reopened') }}

),

unioned as (

  select * from created
  union all
  select * from closed
  union all
  select * from reopened

),

ordered as (

  select
    account_id,
    user_id,
    account_type,
    event_type,
    event_at,

    row_number() over (
      partition by account_id
      order by event_at, event_type
    ) as event_sequence

  from unioned

)

select
  account_id,
  user_id,
  account_type,
  event_type,
  event_at,
  event_sequence
from ordered
