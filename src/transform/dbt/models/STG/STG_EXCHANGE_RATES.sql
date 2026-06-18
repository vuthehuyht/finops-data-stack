{{
  config(
    materialized='incremental',
    unique_key=['PAIR', 'DATE'],
    incremental_strategy='merge'
  )
}}

SELECT
  PAIR::VARCHAR(256) AS PAIR,
  DATE::DATE AS DATE,
  EXCHANGE_RATE::NUMERIC(18, 4) AS EXCHANGE_RATE,
  BATCH_DATE::DATE AS BATCH_DATE,
  _CONATA_SOURCE::VARCHAR(256) AS _CONATA_SOURCE,
  _CONATA_SOURCE_ROW_NUMBER::INTEGER AS _CONATA_SOURCE_ROW_NUMBER,
  _CONATA_PARTITION_KEY::VARCHAR(256) AS _CONATA_PARTITION_KEY,
  _CONATA_LOADED_AT::TIMESTAMP AS _CONATA_LOADED_AT
FROM {{ source('RAW', 'RAW_EXCHANGE_RATES') }}
{% if is_incremental() %}
  -- Lọc theo partition key khi chạy incremental
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
