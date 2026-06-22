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
  DESCRIPTION::VARCHAR(65535) AS DESCRIPTION,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_COMPANY_PROFILE') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
