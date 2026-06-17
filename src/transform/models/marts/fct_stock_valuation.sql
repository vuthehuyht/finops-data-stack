WITH prices AS (
    SELECT * FROM {{ ref('stg_vnstock_eod_prices') }}
),

sentiment AS (
    SELECT
        ticker,
        AVG(sentiment_score) AS avg_sentiment
    FROM {{ ref('stg_corporate_news_sentiment') }}
    GROUP BY 1
),

select_final AS (
    SELECT
        p.ticker,
        p.trading_date,
        p.close_price,
        p.volume,
        COALESCE(s.avg_sentiment, 0.0) AS sentiment_score
    FROM prices p
    LEFT JOIN sentiment s ON p.ticker = s.ticker
)

SELECT * FROM select_final
