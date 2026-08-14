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
  NULLIF(NULLIF(LOWER(CFO), 'nan'), '')::NUMERIC(38, 4) AS CFO,
  NULLIF(NULLIF(LOWER(CFI), 'nan'), '')::NUMERIC(38, 4) AS CFI,
  NULLIF(NULLIF(LOWER(CFF), 'nan'), '')::NUMERIC(38, 4) AS CFF,
  NULLIF(NULLIF(LOWER(NET_CASH_FLOW), 'nan'), '')::NUMERIC(38, 4) AS NET_CASH_FLOW,
  NULLIF(NULLIF(LOWER(CAPEX), 'nan'), '')::NUMERIC(38, 4) AS CAPEX,
  {{ datacore_common_metadata() }}
FROM {{ latest_source(source("RAW", "RAW_CASHFLOW_STATEMENT"), ['TICKER','PERIOD','YEAR']) }}
