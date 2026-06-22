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
  TICKER::VARCHAR(256) AS TICKER,
  PUBLISH_TIME::TIMESTAMP AS PUBLISH_TIME,
  TITLE::VARCHAR(256) AS TITLE,
  SUMMARY::VARCHAR(256) AS SUMMARY,
  CONTENT::VARCHAR(256) AS CONTENT,
  SOURCE::VARCHAR(256) AS SOURCE,
  URL::VARCHAR(256) AS URL,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_NEWS_ARTICLES') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
