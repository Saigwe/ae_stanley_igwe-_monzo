with events as (

  select
    account_id,
    user_id,
    account_type,
    event_type,
    event_at,
    event_sequence
  from {{ ref('int_account_lifecycle_events') }}

),

states as (

  select
    account_id,

    -- user_id and account_type only exist on creation
    first_value(user_id ignore nulls)
      over (
        partition by account_id
        order by event_sequence
      ) as user_id,

    first_value(account_type ignore nulls)
      over (
        partition by account_id
        order by event_sequence
      ) as account_type,

    {{ account_status_from_event('event_type') }} as account_status,

    event_at as valid_from,

    lead(event_at) over (
      partition by account_id
      order by event_sequence
    ) as valid_to

  from events

)

select
  account_id,
  user_id,
  account_type,
  account_status,
  valid_from,
  valid_to
from states
