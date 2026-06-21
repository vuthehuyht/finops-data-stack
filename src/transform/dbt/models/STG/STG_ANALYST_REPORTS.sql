{{
  config(
    materialized='incremental',
    unique_key='REPORT_ID',
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  REPORT_ID::VARCHAR(256) AS REPORT_ID,
  TICKER::VARCHAR(256) AS TICKER,
  BROKERAGE_FIRM::VARCHAR(256) AS BROKERAGE_FIRM,
  PUBLISH_DATE::DATE AS PUBLISH_DATE,
  RECOMMENDATION::VARCHAR(256) AS RECOMMENDATION,
  TARGET_PRICE::NUMERIC(18, 4) AS TARGET_PRICE,
  REPORT_PDF_URL::VARCHAR(256) AS REPORT_PDF_URL,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_ANALYST_REPORTS') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
