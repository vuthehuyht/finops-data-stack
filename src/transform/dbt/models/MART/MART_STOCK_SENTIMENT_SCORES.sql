{{
  config(
    materialized='table',
    unique_key=['TICKER', 'DATE']
  )
}}

-- Aggregates news volume, analyst coverage, and corporate event flags per ticker per day.
-- NLP sentiment scores are a placeholder (NULL) until an NLP pipeline populates them
-- in the Silver layer. The model is designed to accept them as soon as they exist.

WITH NEWS_DAILY AS (
  SELECT
    TICKER,
    PUBLISH_TIME::DATE AS DATE,
    COUNT(*) AS NEWS_COUNT,
    -- Placeholder: sentiment score would come from NLP enrichment in STG layer
    NULL::NUMERIC(18, 4) AS AVG_SENTIMENT_SCORE
  FROM {{ ref('STG_NEWS_ARTICLES') }}
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

ANALYST_LATEST AS (
  -- Most recent analyst report per ticker per day.
  -- FireAnt only exposes free-text title/description, not a structured
  -- recommendation or target price, so those signals stay NULL placeholders
  -- until an NLP pass extracts them from DESCRIPTION.
  SELECT
    TICKER,
    PUBLISH_DATE AS DATE,
    COUNT(*) AS ANALYST_REPORT_COUNT,
    NULL::INTEGER AS ANALYST_BUY_COUNT,
    NULL::NUMERIC(18, 4) AS AVG_ANALYST_TARGET_PRICE
  FROM {{ ref('STG_ANALYST_REPORTS') }}
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
  N.AVG_SENTIMENT_SCORE::NUMERIC(18, 4) AS DAILY_NEWS_SENTIMENT_SCORE,
  N.SENTIMENT_MOMENTUM_7D::NUMERIC(18, 4) AS SENTIMENT_MOMENTUM_7D,
  -- news_velocity: ratio of today's count to 30-day rolling average
  CASE
    WHEN N.AVG_NEWS_COUNT_30D > 0
      THEN N.NEWS_COUNT / N.AVG_NEWS_COUNT_30D
  END::NUMERIC(18, 4) AS NEWS_VELOCITY,
  -- Analyst signals
  COALESCE(A.ANALYST_REPORT_COUNT, 0)::INTEGER AS ANALYST_REPORT_COUNT,
  COALESCE(A.ANALYST_BUY_COUNT, 0)::INTEGER AS ANALYST_BUY_COUNT,
  A.AVG_ANALYST_TARGET_PRICE::NUMERIC(18, 4) AS AVG_ANALYST_TARGET_PRICE,
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
