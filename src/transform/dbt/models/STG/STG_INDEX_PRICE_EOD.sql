{{
  config(
    materialized='incremental',
    unique_key=['INDEX_NAME', 'TRADING_DATE'],
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  INDEX_NAME::VARCHAR(256) AS INDEX_NAME,
  TRADING_DATE::DATE AS TRADING_DATE,
  "OPEN"::NUMERIC(18, 4) AS "OPEN",
  HIGH::NUMERIC(18, 4) AS HIGH,
  LOW::NUMERIC(18, 4) AS LOW,
  CLOSE::NUMERIC(18, 4) AS CLOSE,
  VOLUME::NUMERIC(18, 4) AS VOLUME,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_INDEX_PRICE_EOD') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
