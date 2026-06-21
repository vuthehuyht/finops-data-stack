{{
  config(
    materialized='table',
    unique_key=['TICKER', 'DATE']
  )
}}

-- Aggregates news volume, analyst coverage, and corporate event flags per ticker per day.
-- NLP sentiment scores are a placeholder (NULL) until an NLP pipeline populates them
-- in the Silver layer. The model is designed to accept them as soon as they exist.

WITH news_daily AS (
    SELECT
        TICKER,
        PUBLISH_TIME::DATE AS DATE,
        COUNT(*)                                       AS news_count,
        -- Placeholder: sentiment score would come from NLP enrichment in STG layer
        NULL::NUMERIC(18, 4)                           AS avg_sentiment_score
    FROM {{ ref('STG_NEWS_ARTICLES') }}
    GROUP BY 1, 2
),

news_with_velocity AS (
    SELECT
        TICKER,
        DATE,
        news_count,
        avg_sentiment_score,
        -- Rolling 7-day average sentiment
        AVG(avg_sentiment_score) OVER (
            PARTITION BY TICKER ORDER BY DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sentiment_momentum_7d,
        -- news_velocity: today's count vs 30-day average
        AVG(news_count) OVER (
            PARTITION BY TICKER ORDER BY DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_news_count_30d
    FROM news_daily
),

analyst_latest AS (
    -- Most recent analyst report per ticker per day
    SELECT
        TICKER,
        PUBLISH_DATE          AS DATE,
        COUNT(*)              AS analyst_report_count,
        -- Number of BUY recommendations on the day
        SUM(CASE WHEN UPPER(RECOMMENDATION) IN ('BUY', 'OUTPERFORM', 'OVERWEIGHT') THEN 1 ELSE 0 END)
                              AS analyst_buy_count,
        AVG(TARGET_PRICE)     AS avg_analyst_target_price
    FROM {{ ref('STG_ANALYST_REPORTS') }}
    GROUP BY 1, 2
),

corporate_events_daily AS (
    -- Flag days with corporate events (dividend ex-date, rights issue, etc.)
    SELECT
        TICKER,
        COALESCE(EX_RIGHT_DATE, RECORD_DATE, BATCH_DATE) AS DATE,
        COUNT(*)                                AS event_count,
        SUM(CASE WHEN UPPER(EVENT_TYPE) LIKE '%DIVIDEND%' OR UPPER(EVENT_TYPE) LIKE '%CHIA_CO_TUC%'
                 THEN 1 ELSE 0 END)             AS dividend_event_count
    FROM {{ ref('STG_CORPORATE_EVENTS') }}
    GROUP BY 1, 2
),

-- Generate the date spine from the news table as the primary anchor
ticker_dates AS (
    SELECT DISTINCT TICKER, DATE FROM news_daily
    UNION
    SELECT DISTINCT TICKER, DATE FROM analyst_latest
)

SELECT
    td.TICKER::VARCHAR(256)              AS TICKER,
    td.DATE::DATE                        AS DATE,
    -- News signals
    COALESCE(n.news_count, 0)::INTEGER   AS NEWS_COUNT,
    n.avg_sentiment_score::NUMERIC(18, 4) AS DAILY_NEWS_SENTIMENT_SCORE,
    n.sentiment_momentum_7d::NUMERIC(18, 4) AS SENTIMENT_MOMENTUM_7D,
    -- news_velocity: ratio of today's count to 30-day rolling average
    CASE WHEN n.avg_news_count_30d > 0
        THEN n.news_count / n.avg_news_count_30d
    END::NUMERIC(18, 4)                  AS NEWS_VELOCITY,
    -- Analyst signals
    COALESCE(a.analyst_report_count, 0)::INTEGER AS ANALYST_REPORT_COUNT,
    COALESCE(a.analyst_buy_count, 0)::INTEGER    AS ANALYST_BUY_COUNT,
    a.avg_analyst_target_price::NUMERIC(18, 4)   AS AVG_ANALYST_TARGET_PRICE,
    -- Corporate event flags
    COALESCE(e.event_count, 0)::INTEGER          AS CORPORATE_EVENT_COUNT,
    COALESCE(e.dividend_event_count, 0)::INTEGER AS DIVIDEND_EVENT_COUNT,
    CURRENT_TIMESTAMP::TIMESTAMP                 AS DATACORE_CREATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_CREATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)                      AS DATACORE_CREATE_BY,
    CURRENT_TIMESTAMP::TIMESTAMP                 AS DATACORE_UPDATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_UPDATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)                      AS DATACORE_UPDATE_BY,
    '{{ var("batch_date") }}'::DATE              AS BATCH_DATE
FROM ticker_dates td
LEFT JOIN news_with_velocity n    ON td.TICKER = n.TICKER AND td.DATE = n.DATE
LEFT JOIN analyst_latest     a    ON td.TICKER = a.TICKER AND td.DATE = a.DATE
LEFT JOIN corporate_events_daily e ON td.TICKER = e.TICKER AND td.DATE = e.DATE
