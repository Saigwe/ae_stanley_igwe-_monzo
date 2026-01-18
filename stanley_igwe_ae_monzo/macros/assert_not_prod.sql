{% macro assert_not_prod() %}
  {% if target.name == 'prod' %}
    {{ exceptions.raise_compiler_error(
       "Direct prod runs are blocked. Use CI."
    ) }}
  {% endif %}
{% endmacro %}
