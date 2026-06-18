{{
  config(
    materialized='incremental',
    unique_key=['RATE_TYPE', 'DATE'],
    incremental_strategy='merge'
  )
}}

SELECT
  RATE_TYPE::VARCHAR(256) AS RATE_TYPE,
  DATE::DATE AS DATE,
  RATE_VALUE::NUMERIC(18, 4) AS RATE_VALUE,
  BATCH_DATE::DATE AS BATCH_DATE,
  _CONATA_SOURCE::VARCHAR(256) AS _CONATA_SOURCE,
  _CONATA_SOURCE_ROW_NUMBER::INTEGER AS _CONATA_SOURCE_ROW_NUMBER,
  _CONATA_PARTITION_KEY::VARCHAR(256) AS _CONATA_PARTITION_KEY,
  _CONATA_LOADED_AT::TIMESTAMP AS _CONATA_LOADED_AT
FROM {{ source('RAW', 'RAW_INTEREST_RATES') }}
{% if is_incremental() %}
  -- Lọc theo partition key khi chạy incremental
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
