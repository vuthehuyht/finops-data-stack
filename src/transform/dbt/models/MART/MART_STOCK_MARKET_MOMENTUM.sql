{{
  config(
    materialized='table',
    unique_key=['TICKER', 'TRADING_DATE']
  )
}}

WITH stock_prices AS (
    SELECT
        TICKER,
        TRADING_DATE,
        CLOSE,
        ADJUSTED_CLOSE,
        -- Lag prices for momentum & moving average calculations
        LAG(ADJUSTED_CLOSE, 1)   OVER (PARTITION BY TICKER ORDER BY TRADING_DATE) AS prev_close,
        LAG(ADJUSTED_CLOSE, 20)  OVER (PARTITION BY TICKER ORDER BY TRADING_DATE) AS close_20d_ago,
        LAG(ADJUSTED_CLOSE, 30)  OVER (PARTITION BY TICKER ORDER BY TRADING_DATE) AS close_30d_ago,
        LAG(ADJUSTED_CLOSE, 50)  OVER (PARTITION BY TICKER ORDER BY TRADING_DATE) AS close_50d_ago,
        LAG(ADJUSTED_CLOSE, 90)  OVER (PARTITION BY TICKER ORDER BY TRADING_DATE) AS close_90d_ago,
        LAG(ADJUSTED_CLOSE, 200) OVER (PARTITION BY TICKER ORDER BY TRADING_DATE) AS close_200d_ago
    FROM {{ ref('STG_STOCK_PRICE_EOD') }}
),

daily_returns AS (
    SELECT
        TICKER,
        TRADING_DATE,
        CLOSE,
        ADJUSTED_CLOSE,
        close_20d_ago,
        close_30d_ago,
        close_50d_ago,
        close_90d_ago,
        close_200d_ago,
        -- Daily log return for volatility calculation
        CASE
            WHEN prev_close > 0 THEN (ADJUSTED_CLOSE / prev_close) - 1
        END AS daily_return
    FROM stock_prices
),

stock_metrics AS (
    SELECT
        TICKER,
        TRADING_DATE,
        CLOSE,
        ADJUSTED_CLOSE,
        -- Price momentum
        CASE WHEN close_30d_ago  > 0 THEN (ADJUSTED_CLOSE / close_30d_ago)  - 1 END AS price_momentum_1m,
        CASE WHEN close_90d_ago  > 0 THEN (ADJUSTED_CLOSE / close_90d_ago)  - 1 END AS price_momentum_3m,
        -- Rolling moving averages
        AVG(ADJUSTED_CLOSE) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS moving_average_20d,
        AVG(ADJUSTED_CLOSE) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS moving_average_50d,
        AVG(ADJUSTED_CLOSE) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS moving_average_200d,
        -- 30-day realised volatility (std dev of daily returns)
        STDDEV(daily_return) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_30d,
        -- Average daily return over 20 trading days (~1 month)
        AVG(daily_return) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_return_1m
    FROM daily_returns
),

vnindex_daily AS (
    SELECT
        TRADING_DATE,
        CLOSE,
        LAG(CLOSE, 1) OVER (ORDER BY TRADING_DATE) AS prev_close
    FROM {{ ref('STG_INDEX_PRICE_EOD') }}
    WHERE INDEX_NAME = 'VNINDEX'
),

vnindex_returns AS (
    SELECT
        TRADING_DATE,
        CASE WHEN prev_close > 0 THEN (CLOSE / prev_close) - 1 END AS vnindex_daily_return
    FROM vnindex_daily
),

vnindex_1m AS (
    SELECT
        TRADING_DATE,
        -- Average daily VNINDEX return over 20 trading days
        AVG(vnindex_daily_return) OVER (
            ORDER BY TRADING_DATE
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vnindex_return_1m
    FROM vnindex_returns
)

SELECT
    s.TICKER::VARCHAR(256)            AS TICKER,
    s.TRADING_DATE::DATE              AS TRADING_DATE,
    s.price_momentum_1m::NUMERIC(18, 6) AS PRICE_MOMENTUM_1M,
    s.price_momentum_3m::NUMERIC(18, 6) AS PRICE_MOMENTUM_3M,
    s.volatility_30d::NUMERIC(18, 6)    AS VOLATILITY_30D,
    -- Relative strength = stock monthly return minus VNINDEX monthly return
    (s.avg_return_1m - v.vnindex_return_1m)::NUMERIC(18, 6) AS RELATIVE_STRENGTH_VS_VNINDEX,
    s.moving_average_20d::NUMERIC(18, 4)  AS MOVING_AVERAGE_20D,
    s.moving_average_50d::NUMERIC(18, 4)  AS MOVING_AVERAGE_50D,
    s.moving_average_200d::NUMERIC(18, 4) AS MOVING_AVERAGE_200D,
    CURRENT_TIMESTAMP::TIMESTAMP      AS DATACORE_CREATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_CREATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)           AS DATACORE_CREATE_BY,
    CURRENT_TIMESTAMP::TIMESTAMP      AS DATACORE_UPDATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_UPDATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)           AS DATACORE_UPDATE_BY,
    '{{ var("batch_date") }}'::DATE   AS BATCH_DATE
FROM stock_metrics s
LEFT JOIN vnindex_1m v ON s.TRADING_DATE = v.TRADING_DATE
