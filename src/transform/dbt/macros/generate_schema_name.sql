{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema | lower }}
  {%- else -%}
    {{ custom_schema_name | lower }}
  {%- endif -%}
{%- endmacro %}
