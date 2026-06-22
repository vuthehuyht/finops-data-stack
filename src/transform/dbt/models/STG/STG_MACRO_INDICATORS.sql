{{
  config(
    materialized='incremental',
    unique_key=['INDICATOR_NAME', 'REPORT_DATE'],
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  INDICATOR_NAME::VARCHAR(256) AS INDICATOR_NAME,
  REPORT_DATE::DATE AS REPORT_DATE,
  VALUE::NUMERIC(18, 4) AS VALUE,
  UNIT::VARCHAR(256) AS UNIT,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_MACRO_INDICATORS') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
