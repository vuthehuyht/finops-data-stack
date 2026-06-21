{{
  config(
    materialized='table',
    unique_key=['TICKER', 'TRADING_DATE']
  )
}}

-- Aggregates insider transactions, proprietary desk activity, and foreign flow
-- signals at the Ticker + Date granularity.
-- Insider transactions span a date range (DATE_START to DATE_END); they are
-- attributed to DATE_START for simplicity.

WITH foreign_flow AS (
    SELECT
        TICKER,
        TRADING_DATE,
        BUY_VOL,
        SELL_VOL,
        BUY_VAL,
        SELL_VAL,
        NET_VAL,
        -- 10-day rolling foreign buy ratio
        SUM(BUY_VAL) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS buy_val_10d,
        SUM(BUY_VAL + SELL_VAL) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS total_trading_val_10d,
        -- 1-month (20-day) net foreign flow for momentum
        SUM(NET_VAL) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS net_foreign_val_1m,
        LAG(SUM(NET_VAL) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 20) OVER (PARTITION BY TICKER ORDER BY TRADING_DATE) AS net_foreign_val_1m_prev
    FROM {{ ref('STG_FOREIGN_TRADING') }}
),

proprietary_flow AS (
    SELECT
        TICKER,
        TRADING_DATE,
        NET_VAL AS prop_net_val,
        BUY_VOL AS prop_buy_vol,
        SELL_VOL AS prop_sell_vol,
        -- 5-day rolling sum of proprietary net value
        SUM(NET_VAL) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS prop_net_val_5d
    FROM {{ ref('STG_PROPRIETARY_TRADING') }}
),

-- Correlation between prop and foreign net value over 10 days
prop_foreign_joined AS (
    SELECT
        p.TICKER,
        p.TRADING_DATE,
        p.prop_net_val,
        f.NET_VAL AS foreign_net_val
    FROM proprietary_flow p
    LEFT JOIN foreign_flow f
        ON p.TICKER = p.TICKER AND p.TRADING_DATE = f.TRADING_DATE
),

prop_foreign_corr AS (
    SELECT
        TICKER,
        TRADING_DATE,
        CORR(prop_net_val, foreign_net_val) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS prop_vs_foreign_correlation_10d
    FROM prop_foreign_joined
),

insider_signals AS (
    -- Aggregate insider transactions by ticker and announcement date
    SELECT
        TICKER,
        DATE_START                               AS TRADING_DATE,
        SUM(CASE WHEN UPPER(ACTION) = 'BUY'  THEN EXECUTED_VOL ELSE 0 END) AS insider_buy_vol,
        SUM(CASE WHEN UPPER(ACTION) = 'SELL' THEN EXECUTED_VOL ELSE 0 END) AS insider_sell_vol,
        SUM(EXECUTED_VOL)                        AS insider_total_vol,
        -- -1 net sell, +1 net buy, 0 neutral
        SIGN(
            SUM(CASE WHEN UPPER(ACTION) = 'BUY'  THEN EXECUTED_VOL ELSE 0 END) -
            SUM(CASE WHEN UPPER(ACTION) = 'SELL' THEN EXECUTED_VOL ELSE 0 END)
        )                                        AS insider_sentiment_signal
    FROM {{ ref('STG_INSIDER_TRANSACTIONS') }}
    GROUP BY 1, 2
)

SELECT
    ff.TICKER::VARCHAR(256)                        AS TICKER,
    ff.TRADING_DATE::DATE                          AS TRADING_DATE,
    -- Foreign flow signals
    CASE WHEN ff.total_trading_val_10d > 0
        THEN ff.buy_val_10d / ff.total_trading_val_10d
    END::NUMERIC(18, 6)                            AS FOREIGN_BUY_RATIO_10D,
    ff.net_foreign_val_1m::NUMERIC(18, 4)          AS NET_FOREIGN_FLOW_1M,
    -- 1-month momentum in foreign flow (current vs prior period)
    CASE WHEN ff.net_foreign_val_1m_prev != 0
        THEN ff.net_foreign_val_1m / ABS(ff.net_foreign_val_1m_prev) - 1
    END::NUMERIC(18, 6)                            AS NET_FOREIGN_FLOW_MOMENTUM_1M,
    -- Proprietary trading signals
    pf.prop_net_val_5d::NUMERIC(18, 4)             AS PROP_TRADING_NET_VAL_5D,
    pfc.prop_vs_foreign_correlation_10d::NUMERIC(18, 6) AS PROP_VS_FOREIGN_CORRELATION_10D,
    -- Insider trading signals (NULL on days with no transactions)
    ins.insider_sentiment_signal::SMALLINT         AS INSIDER_SENTIMENT_SIGNAL,
    CASE WHEN ins.insider_total_vol > 0
        THEN ins.insider_buy_vol / ins.insider_total_vol
    END::NUMERIC(18, 6)                            AS INSIDER_BUY_VOLUME_RATIO,
    CURRENT_TIMESTAMP::TIMESTAMP                   AS DATACORE_CREATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256)  AS DATACORE_CREATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)                        AS DATACORE_CREATE_BY,
    CURRENT_TIMESTAMP::TIMESTAMP                   AS DATACORE_UPDATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256)  AS DATACORE_UPDATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)                        AS DATACORE_UPDATE_BY,
    '{{ var("batch_date") }}'::DATE                AS BATCH_DATE
FROM foreign_flow ff
LEFT JOIN proprietary_flow     pf  ON ff.TICKER = pf.TICKER AND ff.TRADING_DATE = pf.TRADING_DATE
LEFT JOIN prop_foreign_corr    pfc ON ff.TICKER = pfc.TICKER AND ff.TRADING_DATE = pfc.TRADING_DATE
LEFT JOIN insider_signals      ins ON ff.TICKER = ins.TICKER AND ff.TRADING_DATE = ins.TRADING_DATE
