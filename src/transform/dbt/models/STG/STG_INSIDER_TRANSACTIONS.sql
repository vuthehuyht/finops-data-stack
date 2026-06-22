{{
  config(
    materialized='incremental',
    unique_key='TRANSACTION_ID',
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  TRANSACTION_ID::VARCHAR(256) AS TRANSACTION_ID,
  TICKER::VARCHAR(256) AS TICKER,
  INSIDER_NAME::VARCHAR(256) AS INSIDER_NAME,
  POSITION::VARCHAR(256) AS POSITION,
  ACTION::VARCHAR(256) AS ACTION,
  REGISTERED_VOL::NUMERIC(18, 4) AS REGISTERED_VOL,
  EXECUTED_VOL::NUMERIC(18, 4) AS EXECUTED_VOL,
  DATE_START::DATE AS DATE_START,
  DATE_END::DATE AS DATE_END,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_INSIDER_TRANSACTIONS') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
