{{
  config(
    materialized='incremental',
    unique_key=['RATE_TYPE', 'DATE'],
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  RATE_TYPE::VARCHAR(256) AS RATE_TYPE,
  DATE::DATE AS DATE,
  RATE_VALUE::NUMERIC(18, 4) AS RATE_VALUE,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_INTEREST_RATES') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
