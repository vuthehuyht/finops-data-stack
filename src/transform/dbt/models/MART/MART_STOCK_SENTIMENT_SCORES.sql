{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_exclude_columns=['DATACORE_CREATE_DATETIME', 'DATACORE_CREATE_PROGRAM', 'DATACORE_CREATE_BY'],
    unique_key=['TICKER', 'DATE']
  )
}}

-- Aggregates news volume, analyst coverage, and corporate event flags per ticker per day.
-- News sentiment comes from STG_NEWS_SENTIMENT (NLP batch scoring, src/nlp/).

WITH NEWS_DAILY AS (
  SELECT
    N.TICKER,
    N.PUBLISH_TIME::DATE AS DATE,
    COUNT(*) AS NEWS_COUNT,
    -- NULL for a ticker/day whose articles have not been scored yet
    AVG(S.SENTIMENT_SCORE) AS AVG_SENTIMENT_SCORE
  FROM {{ ref('STG_NEWS_ARTICLES') }} AS N
  LEFT JOIN {{ ref('STG_NEWS_SENTIMENT') }} AS S
    ON N.ARTICLE_ID = S.ARTICLE_ID
  {% if is_incremental() %}
    WHERE N.BATCH_DATE <= {{ current_batch_date() }}
  {% endif %}
  GROUP BY 1, 2
),

NEWS_WITH_VELOCITY AS (
  SELECT
    TICKER,
    DATE,
    NEWS_COUNT,
    AVG_SENTIMENT_SCORE,
    -- Rolling 7-day average sentiment
    AVG(AVG_SENTIMENT_SCORE) OVER (
      PARTITION BY TICKER ORDER BY DATE
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS SENTIMENT_MOMENTUM_7D,
    -- news_velocity: today's count vs 30-day average
    AVG(NEWS_COUNT) OVER (
      PARTITION BY TICKER ORDER BY DATE
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS AVG_NEWS_COUNT_30D
  FROM NEWS_DAILY
),

ANALYST_REPORTS_CLEAN AS (
  -- FireAnt only exposes free-text title/description, not a structured
  -- recommendation or target price, so both are extracted from DESCRIPTION
  -- with regex heuristics (see macros/analyst_report_signals.sql).
  SELECT
    TICKER,
    PUBLISH_DATE,
    {{ clean_report_text('DESCRIPTION') }} AS REPORT_TEXT
  FROM {{ ref('STG_ANALYST_REPORTS') }}
  WHERE
    TICKER IS NOT NULL
    {% if is_incremental() %}
      AND BATCH_DATE <= {{ current_batch_date() }}
    {% endif %}
),

ANALYST_LATEST AS (
  -- Analyst coverage and extracted signals per ticker per day.
  SELECT
    TICKER,
    PUBLISH_DATE AS DATE,
    COUNT(*) AS ANALYST_REPORT_COUNT,
    SUM(CASE WHEN {{ analyst_is_buy('REPORT_TEXT') }} THEN 1 ELSE 0 END)::INTEGER AS ANALYST_BUY_COUNT,
    AVG({{ analyst_target_price('REPORT_TEXT') }}) AS AVG_ANALYST_TARGET_PRICE
  FROM ANALYST_REPORTS_CLEAN
  GROUP BY 1, 2
),

CORPORATE_EVENTS_DAILY AS (
  -- Flag days with corporate events (dividend ex-date, rights issue, etc.)
  SELECT
    TICKER,
    COALESCE(EX_RIGHT_DATE, RECORD_DATE, BATCH_DATE) AS DATE,
    COUNT(*) AS EVENT_COUNT,
    SUM(CASE
      WHEN UPPER(EVENT_TYPE) LIKE '%DIVIDEND%' OR UPPER(EVENT_TYPE) LIKE '%CHIA_CO_TUC%'
        THEN 1
      ELSE 0
    END) AS DIVIDEND_EVENT_COUNT
  FROM {{ ref('STG_CORPORATE_EVENTS') }}
  {% if is_incremental() %}
    WHERE BATCH_DATE <= {{ current_batch_date() }}
  {% endif %}
  GROUP BY 1, 2
),

-- Generate the date spine from the news table as the primary anchor
TICKER_DATES AS (
  SELECT DISTINCT
    TICKER,
    DATE
  FROM NEWS_DAILY
  UNION DISTINCT
  SELECT DISTINCT
    TICKER,
    DATE
  FROM ANALYST_LATEST
)

SELECT
  TD.TICKER::VARCHAR(256) AS TICKER,
  TD.DATE::DATE AS DATE,
  -- News signals
  COALESCE(N.NEWS_COUNT, 0)::INTEGER AS NEWS_COUNT,
  N.AVG_SENTIMENT_SCORE::NUMERIC(38, 4) AS DAILY_NEWS_SENTIMENT_SCORE,
  N.SENTIMENT_MOMENTUM_7D::NUMERIC(38, 4) AS SENTIMENT_MOMENTUM_7D,
  -- news_velocity: ratio of today's count to 30-day rolling average
  CASE
    WHEN N.AVG_NEWS_COUNT_30D > 0
      THEN N.NEWS_COUNT / N.AVG_NEWS_COUNT_30D
  END::NUMERIC(38, 4) AS NEWS_VELOCITY,
  -- Analyst signals
  COALESCE(A.ANALYST_REPORT_COUNT, 0)::INTEGER AS ANALYST_REPORT_COUNT,
  COALESCE(A.ANALYST_BUY_COUNT, 0)::INTEGER AS ANALYST_BUY_COUNT,
  A.AVG_ANALYST_TARGET_PRICE::NUMERIC(38, 4) AS AVG_ANALYST_TARGET_PRICE,
  -- Corporate event flags
  COALESCE(E.EVENT_COUNT, 0)::INTEGER AS CORPORATE_EVENT_COUNT,
  COALESCE(E.DIVIDEND_EVENT_COUNT, 0)::INTEGER AS DIVIDEND_EVENT_COUNT,
  {{ datacore_common_metadata() }}
FROM TICKER_DATES AS TD
LEFT JOIN NEWS_WITH_VELOCITY AS N
  ON
    TD.TICKER = N.TICKER
    AND TD.DATE = N.DATE
LEFT JOIN ANALYST_LATEST AS A
  ON
    TD.TICKER = A.TICKER
    AND TD.DATE = A.DATE
LEFT JOIN CORPORATE_EVENTS_DAILY AS E
  ON
    TD.TICKER = E.TICKER
    AND TD.DATE = E.DATE
