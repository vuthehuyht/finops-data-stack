WITH source_data AS (
    SELECT
        indicator_name,
        value AS indicator_value,
        CAST(date AS DATE) AS report_date
    FROM {{ source('raw_batch', 'stg_cpi_macro_indicators') }}
),

select_final AS (
    SELECT
        indicator_name,
        indicator_value,
        report_date
    FROM source_data
)

SELECT * FROM select_final
