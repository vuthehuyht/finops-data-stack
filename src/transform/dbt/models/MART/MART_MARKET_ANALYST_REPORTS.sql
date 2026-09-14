{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY'],
    unique_key='REPORT_ID'
  )
}}

-- Market-wide / macro analyst commentary from FireAnt that isn't tied to a
-- specific ticker (STG_ANALYST_REPORTS.TICKER IS NULL). Ticker-specific reports
-- are aggregated into MART_STOCK_SENTIMENT_SCORES instead.

SELECT
  REPORT_ID::VARCHAR(256) AS REPORT_ID,
  BROKERAGE_FIRM::VARCHAR(256) AS BROKERAGE_FIRM,
  PUBLISH_DATE::DATE AS PUBLISH_DATE,
  TITLE::VARCHAR(256) AS TITLE,
  DESCRIPTION::VARCHAR(8000) AS DESCRIPTION,
  FILE_NAME::VARCHAR(256) AS FILE_NAME,
  {{ datacore_common_metadata() }}
FROM {{ ref('STG_ANALYST_REPORTS') }}
WHERE TICKER IS NULL
{% if is_incremental() %}
  AND BATCH_DATE <= {{ current_batch_date() }}
{% endif %}
