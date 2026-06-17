WITH source_data AS (
    SELECT
        ticker,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        CAST(date AS DATE) AS trading_date
    FROM {{ source('raw_batch', 'stg_vnstock_eod_prices') }}
),

select_final AS (
    SELECT
        ticker,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        trading_date
    FROM source_data
)

SELECT * FROM select_final
