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
  CFO::NUMERIC(18, 4) AS CFO,
  CFI::NUMERIC(18, 4) AS CFI,
  CFF::NUMERIC(18, 4) AS CFF,
  NET_CASH_FLOW::NUMERIC(18, 4) AS NET_CASH_FLOW,
  CAPEX::NUMERIC(18, 4) AS CAPEX,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_CASHFLOW_STATEMENT') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
