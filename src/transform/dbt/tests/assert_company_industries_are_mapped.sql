-- Every INDUSTRY value in STG_COMPANY_PROFILE must have a row in the
-- sector_mapping seed. Unmapped industries fall through
-- COALESCE(SM.SECTOR, 'non_financial') in FACT_ML_FEATURE_SET and silently
-- get non-financial feature applicability + no sector embedding signal --
-- the accepted_values test on SECTOR cannot catch this because it runs on
-- the post-COALESCE column. Warn severity: surface it in `dbt test` / CI
-- without blocking, so the mapping can be extended.
{{ config(severity='warn') }}

SELECT
  CP.INDUSTRY,
  COUNT(DISTINCT CP.TICKER) AS UNMAPPED_TICKERS
FROM {{ ref('STG_COMPANY_PROFILE') }} AS CP
LEFT JOIN {{ ref('sector_mapping') }} AS SM
  ON CP.INDUSTRY = SM.INDUSTRY
WHERE CP.INDUSTRY IS NOT NULL
  AND SM.SECTOR IS NULL
GROUP BY CP.INDUSTRY
