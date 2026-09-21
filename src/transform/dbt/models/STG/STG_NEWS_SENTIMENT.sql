{{
  config(
    materialized='incremental',
    unique_key='ARTICLE_ID',
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  ARTICLE_ID::VARCHAR(256) AS ARTICLE_ID,
  SENTIMENT_SCORE::NUMERIC(18, 4) AS SENTIMENT_SCORE,
  SENTIMENT_LABEL::VARCHAR(32) AS SENTIMENT_LABEL,
  MODEL_VERSION::VARCHAR(256) AS MODEL_VERSION,
  {{ datacore_common_metadata() }}
FROM {{ latest_source(source("RAW", "RAW_NEWS_SENTIMENT"), ['ARTICLE_ID']) }}
