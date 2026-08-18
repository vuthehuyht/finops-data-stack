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
  "open"::NUMERIC(38, 4) AS "OPEN",
  NULLIF(NULLIF(LOWER(HIGH), 'nan'), '')::NUMERIC(38, 4) AS HIGH,
  NULLIF(NULLIF(LOWER(LOW), 'nan'), '')::NUMERIC(38, 4) AS LOW,
  NULLIF(NULLIF(LOWER(CLOSE), 'nan'), '')::NUMERIC(38, 4) AS CLOSE,
  NULLIF(NULLIF(LOWER(VOLUME), 'nan'), '')::NUMERIC(38, 4) AS VOLUME,
  NULLIF(NULLIF(LOWER(VALUE), 'nan'), '')::NUMERIC(38, 4) AS VALUE,
  COALESCE(
    NULLIF(NULLIF(LOWER(ADJUSTED_CLOSE), 'nan'), '')::NUMERIC(38, 4),
    NULLIF(NULLIF(LOWER(CLOSE), 'nan'), '')::NUMERIC(38, 4)
  ) AS ADJUSTED_CLOSE,
  {{ datacore_common_metadata() }}
FROM {{ latest_source(source("RAW", "RAW_STOCK_PRICE_EOD"), ['TICKER','TRADING_DATE']) }}
