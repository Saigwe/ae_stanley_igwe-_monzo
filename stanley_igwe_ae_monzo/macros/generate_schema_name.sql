{% macro generate_schema_name(custom_schema_name, node) %}

  {% if target.name == 'dev' %}
    {{ target.schema }}_{{ 
        target.user 
        or env_var('USERNAME', 'dev') 
        or env_var('USER', 'dev') 
    }}
  {% else %}
    {{ custom_schema_name or target.schema }}
  {% endif %}

{% endmacro %}
