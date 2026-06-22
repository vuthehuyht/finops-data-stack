{%- macro current_batch_date() -%}
  '{{ var("batch_date") }}'::DATE
{%- endmacro -%}
