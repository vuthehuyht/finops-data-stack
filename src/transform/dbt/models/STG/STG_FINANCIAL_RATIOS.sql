{{
  config(
    materialized='incremental',
    unique_key=['TICKER', 'DATE'],
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  TICKER::VARCHAR(256) AS TICKER,
  DATE::DATE AS DATE,
  SHARES_OUTSTANDING::NUMERIC(18, 4) AS SHARES_OUTSTANDING,
  MARKET_CAP::NUMERIC(18, 4) AS MARKET_CAP,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_FINANCIAL_RATIOS') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
