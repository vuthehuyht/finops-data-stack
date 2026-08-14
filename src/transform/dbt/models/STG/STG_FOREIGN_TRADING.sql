{{
  config(
    materialized='incremental',
    unique_key=['TICKER', 'TRADING_DATE'],
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  TICKER::VARCHAR(256) AS TICKER,
  TRADING_DATE::DATE AS TRADING_DATE,
  BUY_VOL::NUMERIC(18, 4) AS BUY_VOL,
  SELL_VOL::NUMERIC(18, 4) AS SELL_VOL,
  BUY_VAL::NUMERIC(18, 4) AS BUY_VAL,
  SELL_VAL::NUMERIC(18, 4) AS SELL_VAL,
  NET_VAL::NUMERIC(18, 4) AS NET_VAL,
  {{ datacore_common_metadata() }}
FROM {{ latest_source(source("RAW", "RAW_FOREIGN_TRADING"), ['TICKER','TRADING_DATE']) }}
