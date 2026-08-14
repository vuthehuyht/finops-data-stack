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
  "OPEN"::NUMERIC(38, 4) AS "OPEN",
  NULLIF(NULLIF(LOWER(HIGH), 'nan'), '')::NUMERIC(38, 4) AS HIGH,
  NULLIF(NULLIF(LOWER(LOW), 'nan'), '')::NUMERIC(38, 4) AS LOW,
  NULLIF(NULLIF(LOWER(CLOSE), 'nan'), '')::NUMERIC(38, 4) AS CLOSE,
  NULLIF(NULLIF(LOWER(VOLUME), 'nan'), '')::NUMERIC(38, 4) AS VOLUME,
  {{ datacore_common_metadata() }}
FROM {{ latest_source(source("RAW", "RAW_INDEX_PRICE_EOD"), ['INDEX_NAME','TRADING_DATE']) }}
