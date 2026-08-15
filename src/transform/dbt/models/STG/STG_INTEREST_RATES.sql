{{
  config(
    materialized='incremental',
    unique_key=['RATE_TYPE', 'DATE'],
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  RATE_TYPE::VARCHAR(256) AS RATE_TYPE,
  NULLIF(NULLIF(LOWER(DATE), 'nan'), '')::DATE AS DATE,
  NULLIF(NULLIF(LOWER(RATE_VALUE), 'nan'), '')::NUMERIC(38, 4) AS RATE_VALUE,
  {{ datacore_common_metadata() }}
FROM {{ latest_source(source("RAW", "RAW_INTEREST_RATES"), ['RATE_TYPE','DATE']) }}
