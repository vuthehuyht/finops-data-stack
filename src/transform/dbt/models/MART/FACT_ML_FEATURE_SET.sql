{{
  config(
    materialized='table',
    unique_key=['TICKER', 'TRADING_DATE']
  )
}}

-- Wide flat table for ML model consumption.
-- Built by LEFT JOINing all sub-marts on TICKER + TRADING_DATE.
-- Label columns (label_*) are computed from future returns — use ONLY as targets,
-- never as input features (data leakage risk).

WITH base AS (
    -- Stock price EOD is the anchor: every ticker/date pair with a closing price
    SELECT
        TICKER,
        TRADING_DATE,
        CLOSE,
        ADJUSTED_CLOSE,
        -- Future return labels using LEAD window functions
        LEAD(ADJUSTED_CLOSE, 5)  OVER (PARTITION BY TICKER ORDER BY TRADING_DATE) AS close_next_5d,
        LEAD(ADJUSTED_CLOSE, 20) OVER (PARTITION BY TICKER ORDER BY TRADING_DATE) AS close_next_20d,
        -- Max close in next 10 days (for drawdown label)
        MAX(ADJUSTED_CLOSE) OVER (
            PARTITION BY TICKER ORDER BY TRADING_DATE
            ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING
        ) AS max_close_next_10d
    FROM {{ ref('STG_STOCK_PRICE_EOD') }}
)

SELECT
    b.TICKER::VARCHAR(256)    AS TICKER,
    b.TRADING_DATE::DATE      AS TRADING_DATE,

    -- ── Market Momentum features ─────────────────────────────────────────────
    mom.PRICE_MOMENTUM_1M,
    mom.PRICE_MOMENTUM_3M,
    mom.VOLATILITY_30D,
    mom.RELATIVE_STRENGTH_VS_VNINDEX,
    mom.MOVING_AVERAGE_20D,
    mom.MOVING_AVERAGE_50D,
    mom.MOVING_AVERAGE_200D,

    -- ── Fundamental features ─────────────────────────────────────────────────
    fun.REVENUE_GROWTH_YOY,
    fun.NET_PROFIT_GROWTH_YOY,
    fun.GROSS_MARGIN,
    fun.NET_MARGIN,
    fun.OPERATING_MARGIN,
    fun.ROE,
    fun.ROA,
    fun.ASSET_GROWTH_YOY,
    fun.DEBT_TO_EQUITY,
    fun.CURRENT_RATIO,
    fun.CASH_TO_ASSETS,
    fun.EQUITY_MULTIPLIER,
    fun.PE_RATIO,
    fun.PB_RATIO,
    fun.OPERATING_CASH_FLOW_TO_NET_INCOME,
    fun.FREE_CASH_FLOW,
    fun.CASH_FLOW_TO_DEBT,

    -- ── Sentiment features ───────────────────────────────────────────────────
    snt.NEWS_COUNT,
    snt.DAILY_NEWS_SENTIMENT_SCORE,
    snt.SENTIMENT_MOMENTUM_7D,
    snt.NEWS_VELOCITY,
    snt.ANALYST_REPORT_COUNT,
    snt.ANALYST_BUY_COUNT,
    snt.AVG_ANALYST_TARGET_PRICE,
    snt.CORPORATE_EVENT_COUNT,
    snt.DIVIDEND_EVENT_COUNT,

    -- ── Macro & commodities features ─────────────────────────────────────────
    mac.FED_RATE,
    mac.TREASURY_10Y,
    mac.TREASURY_5Y,
    mac.REAL_INTEREST_RATE,
    mac.USD_VND_RATE,
    mac.EXCHANGE_RATE_VOLATILITY_30D,
    mac.CPI_VALUE,
    mac.BRENT_CRUDE_PRICE,
    mac.WTI_PRICE,
    mac.GOLD_PRICE,
    mac.STEEL_HRC_PRICE,
    mac.GASOLINE_SINGAPORE_PRICE,
    mac.BALTIC_DIRTY_TANKER_INDEX,
    mac.BRENT_PRICE_MOMENTUM_1M,
    mac.CRACK_SPREAD_PROXY,

    -- ── Insider & flow features ──────────────────────────────────────────────
    flw.FOREIGN_BUY_RATIO_10D,
    flw.NET_FOREIGN_FLOW_1M,
    flw.NET_FOREIGN_FLOW_MOMENTUM_1M,
    flw.PROP_TRADING_NET_VAL_5D,
    flw.PROP_VS_FOREIGN_CORRELATION_10D,
    flw.INSIDER_SENTIMENT_SIGNAL,
    flw.INSIDER_BUY_VOLUME_RATIO,

    -- ── Target labels (use ONLY as ML targets, never as input features) ──────
    -- label_next_5d_return: return after 5 trading days
    CASE WHEN b.ADJUSTED_CLOSE > 0 AND b.close_next_5d IS NOT NULL
        THEN (b.close_next_5d / b.ADJUSTED_CLOSE) - 1
    END::NUMERIC(18, 6)       AS LABEL_NEXT_5D_RETURN,
    -- label_next_20d_return: return after 20 trading days (~1 month)
    CASE WHEN b.ADJUSTED_CLOSE > 0 AND b.close_next_20d IS NOT NULL
        THEN (b.close_next_20d / b.ADJUSTED_CLOSE) - 1
    END::NUMERIC(18, 6)       AS LABEL_NEXT_20D_RETURN,
    -- label_is_uptrend_30d: 1 if 20-day forward return > 5%, else 0
    CASE
        WHEN b.ADJUSTED_CLOSE > 0 AND b.close_next_20d IS NOT NULL
             AND (b.close_next_20d / b.ADJUSTED_CLOSE) - 1 > 0.05 THEN 1
        WHEN b.close_next_20d IS NOT NULL THEN 0
    END::SMALLINT             AS LABEL_IS_UPTREND_30D,
    -- label_max_drawdown_next_10d: max adverse excursion over next 10 days
    CASE WHEN b.ADJUSTED_CLOSE > 0 AND b.max_close_next_10d IS NOT NULL
        THEN (b.max_close_next_10d / b.ADJUSTED_CLOSE) - 1
    END::NUMERIC(18, 6)       AS LABEL_MAX_DRAWDOWN_NEXT_10D,

    -- ── Metadata ─────────────────────────────────────────────────────────────
    CURRENT_TIMESTAMP::TIMESTAMP AS DATACORE_CREATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_CREATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)   AS DATACORE_CREATE_BY,
    CURRENT_TIMESTAMP::TIMESTAMP AS DATACORE_UPDATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_UPDATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)   AS DATACORE_UPDATE_BY,
    '{{ var("batch_date") }}'::DATE AS BATCH_DATE

FROM base b
LEFT JOIN {{ ref('MART_STOCK_MARKET_MOMENTUM') }}    mom ON b.TICKER = mom.TICKER AND b.TRADING_DATE = mom.TRADING_DATE
LEFT JOIN {{ ref('MART_STOCK_FUNDAMENTAL_METRICS') }} fun ON b.TICKER = fun.TICKER AND b.TRADING_DATE = fun.DATE
LEFT JOIN {{ ref('MART_STOCK_SENTIMENT_SCORES') }}   snt ON b.TICKER = snt.TICKER AND b.TRADING_DATE = snt.DATE
LEFT JOIN {{ ref('MART_MACRO_COMMODITIES_SIGNALS') }} mac ON b.TRADING_DATE = mac.DATE
LEFT JOIN {{ ref('MART_INSIDER_PROPRIETARY_FLOWS') }} flw ON b.TICKER = flw.TICKER AND b.TRADING_DATE = flw.TRADING_DATE
