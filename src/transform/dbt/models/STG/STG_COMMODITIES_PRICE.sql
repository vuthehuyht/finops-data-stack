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
  NULLIF(NULLIF(LOWER(DATE), 'nan'), '')::DATE AS DATE,
  NULLIF(NULLIF(LOWER(PRICE), 'nan'), '')::NUMERIC(38, 4) AS PRICE,
  {{ datacore_common_metadata() }}
FROM {{ latest_source(source("RAW", "RAW_COMMODITIES_PRICE"), ['COMMODITY_NAME','DATE']) }}
