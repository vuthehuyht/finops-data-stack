{{
  config(
    materialized='incremental',
    unique_key=['TICKER', 'PERIOD', 'YEAR'],
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  TICKER::VARCHAR(256) AS TICKER,
  PERIOD::VARCHAR(256) AS PERIOD,
  YEAR::INTEGER AS YEAR,
  TOTAL_ASSETS::NUMERIC(18, 4) AS TOTAL_ASSETS,
  CURRENT_ASSETS::NUMERIC(18, 4) AS CURRENT_ASSETS,
  CASH::NUMERIC(18, 4) AS CASH,
  INVENTORY::NUMERIC(18, 4) AS INVENTORY,
  TOTAL_LIABILITIES::NUMERIC(18, 4) AS TOTAL_LIABILITIES,
  SHORT_TERM_DEBT::NUMERIC(18, 4) AS SHORT_TERM_DEBT,
  LONG_TERM_DEBT::NUMERIC(18, 4) AS LONG_TERM_DEBT,
  EQUITY::NUMERIC(18, 4) AS EQUITY,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_BALANCE_SHEET') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
