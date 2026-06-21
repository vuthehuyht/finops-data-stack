{{
  config(
    materialized='incremental',
    unique_key=['PAIR', 'DATE'],
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY']
  )
}}

SELECT
  PAIR::VARCHAR(256) AS PAIR,
  CASE
    WHEN DATE = '' OR DATE = 'None' OR DATE IS NULL THEN NULL
    ELSE DATE::DATE
  END AS DATE,
  CASE
    WHEN EXCHANGE_RATE = '' OR EXCHANGE_RATE = 'None' OR EXCHANGE_RATE IS NULL THEN NULL
    ELSE EXCHANGE_RATE::NUMERIC(18, 4)
  END AS EXCHANGE_RATE,
  {{ datacore_common_metadata() }}
FROM {{ source('RAW', 'RAW_EXCHANGE_RATES') }}
{% if is_incremental() %}
  -- Filter by partition key for incremental run
  WHERE _CONATA_PARTITION_KEY = '{{ var("partition_key") }}'
{% endif %}
