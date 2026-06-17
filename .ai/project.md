# FinOps Data Stack — Project Context

> Summary of project goals & scope. Full detail: `docs/architecture-design.md` (sections 1-2), `README.md`.

## Project Goals

A Data Pipeline and AI system that automates collection and processing of data, and forecasts stock valuation for the Vietnamese stock market, based on Fundamental Analysis and market Sentiment Analysis.

- **Automation**: end-to-end data flow from collection (Crawl/API) to storage and transformation.
- **Smart valuation**: multimodal Deep Learning to forecast expected return and stock trend.
- **Data governance**: strict Metadata and Data Quality standards across Raw, Cleaned, and Mart layers.
- **Cost optimization**: runs on AWS Serverless infrastructure (Redshift Serverless, SageMaker Serverless).

## Market Scope

Vietnamese stock market: VN-Index, HNX, UPCOM.

## Data Inventory

4 strategic data groups:
1. **Market**: EOD prices, foreign trading flows, proprietary trading.
2. **Fundamental**: Financial statements (Balance Sheet, Income Statement, Cash Flow), raw financial ratios.
3. **Macro & Commodities**: GDP, CPI, FX rates, interest rates, Brent oil price, crack spread.
4. **Qualitative**: Corporate news, shareholder meeting resolutions, insider transactions.

## AI/ML Model (summary)

Multimodal Hybrid Neural Network:
- Time-Series branch (LSTM/GRU): 30-day price series + sentiment score.
- Tabular branch (MLP): fundamental financial ratios and macro indicators.
- Output: expected return forecast (Regression) or trend classification (Classification).

## See Details

- Overall architecture: `docs/architecture-design.md`
- Data Schema & Source Mapping: `docs/data-schema-mapping.md`
- Transformation & Feature Engineering: `docs/data-transform-features.md`
- ML model design: `docs/ml-architecture-design.md`
- AWS infrastructure design: `docs/infrastructure-design.md`
