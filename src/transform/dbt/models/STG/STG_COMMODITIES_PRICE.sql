{{
  config(
    materialized='incremental',
    unique_key=['COMMODITY_NAME', 'DATE'],
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  COMMODITY_NAME::VARCHAR(256) AS COMMODITY_NAME,
  DATE::DATE AS DATE,
  PRICE::NUMERIC(18, 4) AS PRICE,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_COMMODITIES_PRICE') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
