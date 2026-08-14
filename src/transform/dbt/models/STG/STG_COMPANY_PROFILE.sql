{{
  config(
    materialized='incremental',
    unique_key='TICKER',
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  TICKER::VARCHAR(256) AS TICKER,
  COMPANY_NAME::VARCHAR(512) AS COMPANY_NAME,
  INDUSTRY::VARCHAR(256) AS INDUSTRY,
  EXCHANGE::VARCHAR(256) AS EXCHANGE,
  NULLIF(NULLIF(LOWER(OUTSTANDING_SHARE), 'nan'), '')::NUMERIC(38, 4) AS OUTSTANDING_SHARE,
  DESCRIPTION::VARCHAR(65535) AS DESCRIPTION,
  {{ datacore_common_metadata() }}
FROM {{ latest_source(source("RAW", "RAW_COMPANY_PROFILE"), ['TICKER']) }}
