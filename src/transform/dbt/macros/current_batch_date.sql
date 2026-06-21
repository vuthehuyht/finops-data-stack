{%- macro current_batch_date() -%}
  TO_DATE('{{ var("batch_date") }}')
{%- endmacro -%}
