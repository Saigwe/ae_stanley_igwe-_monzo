{% macro account_status_from_event(event_type) %}
  case
    when {{ event_type }} in ('created', 'reopened') then 'open'
    when {{ event_type }} = 'closed' then 'closed'
  end
{% endmacro %}
