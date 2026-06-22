{{
  config(
    materialized='incremental',
    unique_key='EVENT_ID',
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  EVENT_ID::VARCHAR(256) AS EVENT_ID,
  TICKER::VARCHAR(256) AS TICKER,
  EVENT_TYPE::VARCHAR(256) AS EVENT_TYPE,
  CASE
    WHEN EX_RIGHT_DATE = '' OR EX_RIGHT_DATE = 'None' OR EX_RIGHT_DATE IS NULL THEN NULL
    ELSE EX_RIGHT_DATE::DATE
  END AS EX_RIGHT_DATE,
  CASE
    WHEN RECORD_DATE = '' OR RECORD_DATE = 'None' OR RECORD_DATE IS NULL THEN NULL
    ELSE RECORD_DATE::DATE
  END AS RECORD_DATE,
  EVENT_DETAILS::VARCHAR(256) AS EVENT_DETAILS,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_CORPORATE_EVENTS') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
