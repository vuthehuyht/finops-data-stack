WITH source_data AS (
    SELECT
        news_id,
        ticker,
        sentiment_score,
        CAST(published_at AS TIMESTAMP) AS published_at
    FROM {{ source('raw_batch', 'stg_corporate_news_sentiment') }}
),

select_final AS (
    SELECT
        news_id,
        ticker,
        sentiment_score,
        published_at
    FROM source_data
)

SELECT * FROM select_final
