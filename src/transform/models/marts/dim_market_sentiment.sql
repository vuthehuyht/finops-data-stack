WITH sentiment AS (
    SELECT * FROM {{ ref('stg_corporate_news_sentiment') }}
),

select_final AS (
    SELECT
        ticker,
        CAST(published_at AS DATE) AS sentiment_date,
        AVG(sentiment_score) AS daily_avg_sentiment,
        COUNT(*) AS news_count
    FROM sentiment
    GROUP BY 1, 2
)

SELECT * FROM select_final
