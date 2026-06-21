{{
  config(
    materialized='table',
    unique_key=['TICKER', 'DATE']
  )
}}

-- Latest quarterly snapshot per ticker from each fundamental table.
-- Joined with daily FINANCIAL_RATIOS (market cap, shares outstanding) so that
-- the output granularity is Ticker + Date (daily), with quarterly data forward-filled.

WITH income_with_yoy AS (
    SELECT
        TICKER,
        PERIOD,
        YEAR,
        REVENUE,
        GROSS_PROFIT,
        OPERATING_PROFIT,
        NET_PROFIT_AFTER_TAX,
        -- YoY comparison: lag by 4 quarters
        LAG(REVENUE,              4) OVER (PARTITION BY TICKER ORDER BY YEAR, PERIOD) AS revenue_yoy,
        LAG(NET_PROFIT_AFTER_TAX, 4) OVER (PARTITION BY TICKER ORDER BY YEAR, PERIOD) AS net_profit_yoy
    FROM {{ ref('STG_INCOME_STATEMENT') }}
),

income_latest AS (
    SELECT
        TICKER,
        REVENUE,
        GROSS_PROFIT,
        OPERATING_PROFIT,
        NET_PROFIT_AFTER_TAX,
        revenue_yoy,
        net_profit_yoy,
        ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY YEAR DESC, PERIOD DESC) AS rn
    FROM income_with_yoy
    QUALIFY rn = 1
),

balance_with_yoy AS (
    SELECT
        TICKER,
        TOTAL_ASSETS,
        CURRENT_ASSETS,
        CASH,
        TOTAL_LIABILITIES,
        SHORT_TERM_DEBT,
        LONG_TERM_DEBT,
        EQUITY,
        LAG(TOTAL_ASSETS, 4) OVER (PARTITION BY TICKER ORDER BY YEAR, PERIOD) AS total_assets_yoy,
        ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY YEAR DESC, PERIOD DESC) AS rn
    FROM {{ ref('STG_BALANCE_SHEET') }}
    QUALIFY rn = 1
),

cashflow_latest AS (
    SELECT
        TICKER,
        CFO,
        CAPEX,
        ROW_NUMBER() OVER (PARTITION BY TICKER ORDER BY YEAR DESC, PERIOD DESC) AS rn
    FROM {{ ref('STG_CASHFLOW_STATEMENT') }}
    QUALIFY rn = 1
)

SELECT
    r.TICKER::VARCHAR(256) AS TICKER,
    r.DATE::DATE           AS DATE,
    -- Revenue & profit growth (YoY, quarterly)
    CASE WHEN i.revenue_yoy > 0
        THEN ((i.REVENUE / i.revenue_yoy) - 1)
    END::NUMERIC(18, 6) AS REVENUE_GROWTH_YOY,
    CASE WHEN i.net_profit_yoy > 0
        THEN ((i.NET_PROFIT_AFTER_TAX / i.net_profit_yoy) - 1)
    END::NUMERIC(18, 6) AS NET_PROFIT_GROWTH_YOY,
    -- Margin ratios
    CASE WHEN i.REVENUE != 0
        THEN i.GROSS_PROFIT / i.REVENUE
    END::NUMERIC(18, 6) AS GROSS_MARGIN,
    CASE WHEN i.REVENUE != 0
        THEN i.NET_PROFIT_AFTER_TAX / i.REVENUE
    END::NUMERIC(18, 6) AS NET_MARGIN,
    CASE WHEN i.REVENUE != 0
        THEN i.OPERATING_PROFIT / i.REVENUE
    END::NUMERIC(18, 6) AS OPERATING_MARGIN,
    -- Return on equity / assets
    CASE WHEN b.EQUITY != 0
        THEN i.NET_PROFIT_AFTER_TAX / b.EQUITY
    END::NUMERIC(18, 6) AS ROE,
    CASE WHEN b.TOTAL_ASSETS != 0
        THEN i.NET_PROFIT_AFTER_TAX / b.TOTAL_ASSETS
    END::NUMERIC(18, 6) AS ROA,
    -- Balance sheet ratios
    CASE WHEN b.total_assets_yoy > 0
        THEN ((b.TOTAL_ASSETS / b.total_assets_yoy) - 1)
    END::NUMERIC(18, 6) AS ASSET_GROWTH_YOY,
    CASE WHEN b.EQUITY != 0
        THEN (b.SHORT_TERM_DEBT + b.LONG_TERM_DEBT) / b.EQUITY
    END::NUMERIC(18, 6) AS DEBT_TO_EQUITY,
    CASE WHEN b.TOTAL_LIABILITIES != 0
        THEN b.CURRENT_ASSETS / b.TOTAL_LIABILITIES
    END::NUMERIC(18, 6) AS CURRENT_RATIO,
    CASE WHEN b.TOTAL_ASSETS != 0
        THEN b.CASH / b.TOTAL_ASSETS
    END::NUMERIC(18, 6) AS CASH_TO_ASSETS,
    CASE WHEN b.EQUITY != 0
        THEN b.TOTAL_ASSETS / b.EQUITY
    END::NUMERIC(18, 6) AS EQUITY_MULTIPLIER,
    -- Valuation multiples (market cap from daily data, profit from quarterly)
    CASE WHEN i.NET_PROFIT_AFTER_TAX != 0
        THEN r.MARKET_CAP / i.NET_PROFIT_AFTER_TAX
    END::NUMERIC(18, 4) AS PE_RATIO,
    CASE WHEN b.EQUITY != 0
        THEN r.MARKET_CAP / b.EQUITY
    END::NUMERIC(18, 4) AS PB_RATIO,
    -- Cash flow ratios
    CASE WHEN i.NET_PROFIT_AFTER_TAX != 0
        THEN cf.CFO / i.NET_PROFIT_AFTER_TAX
    END::NUMERIC(18, 6) AS OPERATING_CASH_FLOW_TO_NET_INCOME,
    (cf.CFO - cf.CAPEX)::NUMERIC(18, 4) AS FREE_CASH_FLOW,
    CASE WHEN (b.SHORT_TERM_DEBT + b.LONG_TERM_DEBT) != 0
        THEN cf.CFO / (b.SHORT_TERM_DEBT + b.LONG_TERM_DEBT)
    END::NUMERIC(18, 6) AS CASH_FLOW_TO_DEBT,
    CURRENT_TIMESTAMP::TIMESTAMP      AS DATACORE_CREATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_CREATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)           AS DATACORE_CREATE_BY,
    CURRENT_TIMESTAMP::TIMESTAMP      AS DATACORE_UPDATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_UPDATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)           AS DATACORE_UPDATE_BY,
    '{{ var("batch_date") }}'::DATE   AS BATCH_DATE
FROM {{ ref('STG_FINANCIAL_RATIOS') }} r
LEFT JOIN income_latest  i  ON r.TICKER = i.TICKER
LEFT JOIN balance_with_yoy b ON r.TICKER = b.TICKER
LEFT JOIN cashflow_latest cf ON r.TICKER = cf.TICKER
