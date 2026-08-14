{%- macro latest_source(source_name, source_unique_keys) -%}
(
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY {{ source_unique_keys | join(', ') }} 
        ORDER BY _CONATA_LOADED_AT DESC
      ) AS _conata_rn
    FROM {{ source_name }}
    {%- if is_incremental() and var('partition_key', none) is not none %}
    WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
    {%- endif %}
  )
  WHERE _conata_rn = 1
)
{%- endmacro -%}
