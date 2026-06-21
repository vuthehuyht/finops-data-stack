{{
  config(
    materialized='table',
    unique_key='DATE'
  )
}}

-- Macro & commodities signals at daily granularity (no ticker dimension).
-- Interest rates, exchange rates, and commodity prices are pivoted to columns
-- so downstream FACT_ML_FEATURE_SET can join on DATE only.

WITH interest_rates_pivot AS (
    -- Fed proxy (^IRX), 10Y Treasury (^TNX), 5Y Treasury (^FVX)
    SELECT
        DATE,
        MAX(CASE WHEN RATE_TYPE = '^IRX' THEN RATE_VALUE END) AS fed_rate,
        MAX(CASE WHEN RATE_TYPE = '^TNX' THEN RATE_VALUE END) AS treasury_10y,
        MAX(CASE WHEN RATE_TYPE = '^FVX' THEN RATE_VALUE END) AS treasury_5y
    FROM {{ ref('STG_INTEREST_RATES') }}
    GROUP BY 1
),

usdvnd AS (
    SELECT
        DATE,
        EXCHANGE_RATE AS usd_vnd_rate,
        LAG(EXCHANGE_RATE, 1) OVER (ORDER BY DATE) AS prev_rate
    FROM {{ ref('STG_EXCHANGE_RATES') }}
    WHERE PAIR = 'USDT/VND'
),

exchange_rate_vol AS (
    SELECT
        DATE,
        usd_vnd_rate,
        -- 30-day volatility of USD/VND daily return
        STDDEV(
            CASE WHEN prev_rate > 0 THEN (usd_vnd_rate / prev_rate) - 1 END
        ) OVER (
            ORDER BY DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS exchange_rate_volatility_30d
    FROM usdvnd
),

macro_pivot AS (
    -- Forward-fill monthly/quarterly macro indicators to daily using LAST_VALUE
    -- We pivot the two key indicators: CPI (inflation) and interest rate proxy
    SELECT
        REPORT_DATE AS DATE,
        MAX(CASE WHEN INDICATOR_NAME = 'CPI'            THEN VALUE END) AS cpi_value,
        MAX(CASE WHEN INDICATOR_NAME = 'INTEREST_RATE'  THEN VALUE END) AS policy_rate
    FROM {{ ref('STG_MACRO_INDICATORS') }}
    GROUP BY 1
),

brent_crude AS (
    SELECT
        DATE,
        PRICE AS brent_price,
        LAG(PRICE, 30) OVER (ORDER BY DATE) AS brent_price_30d_ago
    FROM {{ ref('STG_COMMODITIES_PRICE') }}
    WHERE COMMODITY_NAME = 'Brent Crude'
),

commodities_pivot AS (
    SELECT
        DATE,
        MAX(CASE WHEN COMMODITY_NAME = 'Brent Crude'        THEN PRICE END) AS brent_crude_price,
        MAX(CASE WHEN COMMODITY_NAME = 'WTI'                THEN PRICE END) AS wti_price,
        MAX(CASE WHEN COMMODITY_NAME = 'Gold'               THEN PRICE END) AS gold_price,
        MAX(CASE WHEN COMMODITY_NAME = 'Steel HRC'          THEN PRICE END) AS steel_hrc_price,
        MAX(CASE WHEN COMMODITY_NAME LIKE '%Gasoline%'
                  AND COMMODITY_NAME LIKE '%Singapore%'     THEN PRICE END) AS gasoline_singapore_price,
        MAX(CASE WHEN COMMODITY_NAME LIKE '%Baltic%'
                  AND COMMODITY_NAME LIKE '%Dirty%'         THEN PRICE END) AS baltic_dirty_tanker_index
    FROM {{ ref('STG_COMMODITIES_PRICE') }}
    GROUP BY 1
),

-- Use brent crude date spine as the anchor (most complete daily commodity series)
date_spine AS (
    SELECT DISTINCT DATE FROM {{ ref('STG_COMMODITIES_PRICE') }}
)

SELECT
    ds.DATE::DATE                          AS DATE,
    -- Interest rates
    ir.fed_rate::NUMERIC(18, 4)            AS FED_RATE,
    ir.treasury_10y::NUMERIC(18, 4)        AS TREASURY_10Y,
    ir.treasury_5y::NUMERIC(18, 4)         AS TREASURY_5Y,
    -- Real interest rate = nominal (Fed) minus CPI inflation
    CASE WHEN ir.fed_rate IS NOT NULL AND m.cpi_value IS NOT NULL
        THEN ir.fed_rate - m.cpi_value
    END::NUMERIC(18, 4)                    AS REAL_INTEREST_RATE,
    -- Exchange rate signals
    er.usd_vnd_rate::NUMERIC(18, 4)        AS USD_VND_RATE,
    er.exchange_rate_volatility_30d::NUMERIC(18, 6) AS EXCHANGE_RATE_VOLATILITY_30D,
    -- Macro indicators
    m.cpi_value::NUMERIC(18, 4)            AS CPI_VALUE,
    m.policy_rate::NUMERIC(18, 4)          AS POLICY_RATE,
    -- Commodity prices
    cp.brent_crude_price::NUMERIC(18, 4)   AS BRENT_CRUDE_PRICE,
    cp.wti_price::NUMERIC(18, 4)           AS WTI_PRICE,
    cp.gold_price::NUMERIC(18, 4)          AS GOLD_PRICE,
    cp.steel_hrc_price::NUMERIC(18, 4)     AS STEEL_HRC_PRICE,
    cp.gasoline_singapore_price::NUMERIC(18, 4)  AS GASOLINE_SINGAPORE_PRICE,
    cp.baltic_dirty_tanker_index::NUMERIC(18, 4) AS BALTIC_DIRTY_TANKER_INDEX,
    -- Brent momentum (30-day price change %)
    CASE WHEN b.brent_price_30d_ago > 0
        THEN (b.brent_price / b.brent_price_30d_ago) - 1
    END::NUMERIC(18, 6)                    AS BRENT_PRICE_MOMENTUM_1M,
    -- Crack spread proxy: gasoline Singapore minus Brent
    CASE WHEN cp.gasoline_singapore_price IS NOT NULL AND cp.brent_crude_price IS NOT NULL
        THEN cp.gasoline_singapore_price - cp.brent_crude_price
    END::NUMERIC(18, 4)                    AS CRACK_SPREAD_PROXY,
    CURRENT_TIMESTAMP::TIMESTAMP           AS DATACORE_CREATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_CREATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)                AS DATACORE_CREATE_BY,
    CURRENT_TIMESTAMP::TIMESTAMP           AS DATACORE_UPDATE_DATETIME,
    '{{ var("dagster_job_name") }}'::VARCHAR(256) AS DATACORE_UPDATE_PROGRAM,
    'DAGSTER'::VARCHAR(256)                AS DATACORE_UPDATE_BY,
    '{{ var("batch_date") }}'::DATE        AS BATCH_DATE
FROM date_spine ds
LEFT JOIN interest_rates_pivot ir ON ds.DATE = ir.DATE
LEFT JOIN exchange_rate_vol    er ON ds.DATE = er.DATE
LEFT JOIN macro_pivot          m  ON ds.DATE = m.DATE
LEFT JOIN commodities_pivot    cp ON ds.DATE = cp.DATE
LEFT JOIN brent_crude          b  ON ds.DATE = b.DATE
