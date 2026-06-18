{{
  config(
    materialized='incremental',
    unique_key=['COMMODITY_NAME', 'DATE'],
    incremental_strategy='merge'
  )
}}

SELECT
  COMMODITY_NAME::VARCHAR(256) AS COMMODITY_NAME,
  DATE::DATE AS DATE,
  PRICE::NUMERIC(18, 4) AS PRICE,
  BATCH_DATE::DATE AS BATCH_DATE,
  _CONATA_SOURCE::VARCHAR(256) AS _CONATA_SOURCE,
  _CONATA_SOURCE_ROW_NUMBER::INTEGER AS _CONATA_SOURCE_ROW_NUMBER,
  _CONATA_PARTITION_KEY::VARCHAR(256) AS _CONATA_PARTITION_KEY,
  _CONATA_LOADED_AT::TIMESTAMP AS _CONATA_LOADED_AT
FROM {{ source('RAW', 'RAW_COMMODITIES_PRICE') }}
{% if is_incremental() %}
  -- Lọc theo partition key khi chạy incremental
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
