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
  NULLIF(NULLIF(LOWER(TRADING_DATE), 'nan'), '')::DATE AS TRADING_DATE,
  NULLIF(NULLIF(LOWER(BUY_VOL), 'nan'), '')::NUMERIC(38, 4) AS BUY_VOL,
  NULLIF(NULLIF(LOWER(SELL_VOL), 'nan'), '')::NUMERIC(38, 4) AS SELL_VOL,
  NULLIF(NULLIF(LOWER(BUY_VAL), 'nan'), '')::NUMERIC(38, 4) AS BUY_VAL,
  NULLIF(NULLIF(LOWER(SELL_VAL), 'nan'), '')::NUMERIC(38, 4) AS SELL_VAL,
  NULLIF(NULLIF(LOWER(NET_VAL), 'nan'), '')::NUMERIC(38, 4) AS NET_VAL,
  {{ datacore_common_metadata() }}
FROM {{ latest_source(source("RAW", "RAW_FOREIGN_TRADING"), ['TICKER','TRADING_DATE']) }}
