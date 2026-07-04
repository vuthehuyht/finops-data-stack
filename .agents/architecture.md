# FinOps Data Stack — Architecture Context

> Summary of architecture & data flow. Full detail: `docs/architecture-design.md`, `docs/ml-architecture-design.md`, `docs/infrastructure-design.md`.

## Tech Stack

Data Mesh / Lakehouse architecture on AWS, orchestrated by **Dagster** (Software-Defined Assets):

- **Orchestration**: Dagster.
- **Compute/Deployment**: Amazon EKS.
- **Data Lake (Bronze)**: Amazon S3 — raw data in Parquet/JSON.
- **Data Warehouse (Silver/Gold)**: Amazon Redshift Serverless.
- **Transformation**: dbt (data build tool).
- **Machine Learning**: Amazon SageMaker — on-demand Training (GPU), Batch Transform for daily inference. Model versioning via S3 path + SSM Parameter Store (not SageMaker Model Registry).
- **Security**: AWS IAM, KMS, Secrets Manager, VPC.
- **CI/CD**: GitHub Actions → deploy to EKS.

## Data Flow — 2 main pipelines

### 1. Daily Inference Pipeline (after market close / EOD)
1. Ingestion (EKS → S3): collect raw data, tag with `_CONATA_*` metadata, store as Parquet.
2. Sensor & Loading (S3 → Redshift Bronze): trigger `COPY` to load raw tables.
3. Transformation (dbt Silver & Gold): type casting + `DATACORE_*` metadata, then feature engineering at Gold (Mart) layer.
4. Data Quality Gate: validate `fact_ml_feature_set` meets standards before continuing.
5. ML Inference: run SageMaker Batch Transform Job (Serverless Batch). Output is self-contained (`ticker` + `predicted_return` per line, no output-line-position matching). The published `TRADING_DATE` is the next trading day after the feature anchor date, not the anchor date itself.
6. Results Publishing: `COPY` the Batch Transform output file directly into Redshift Gold (`FCT_ML_FORECAST_RESULTS`, delete-then-insert by `TRADING_DATE` for idempotency) + dashboard.

### 2. Quarterly Re-training Pipeline (after new financial statements are published)
1. Historical Data Preparation: export the full history of `FACT_ML_FEATURE_SET` from Redshift (no lookback filter — dataset is small enough that a window isn't needed).
2. Training Job (SageMaker GPU) → model artifact pushed to S3.
3. Model Registration: version the model on S3 (`<model_name>/<version>/model.tar.gz` + `metadata.json` with evaluation metrics) — not SageMaker Model Registry.
4. Model Evaluation & Approval: compare Challenger vs Champion.
5. Model Promotion: update active model version in SSM Parameter Store.

## `src/` Directory Structure (Flywheel Architecture)

```text
src/
├── common/      # Shared AWS Utils, DB Utils, Logging
├── dagster/     # Primary Dagster assets, jobs, sensors, resources
├── ingest/      # Ingestion layer (Crawl/API -> S3)
├── load/        # Load layer (S3 -> Redshift)
├── transform/   # dbt project (transformation)
├── pipeline/    # Legacy orchestration helpers (src/pipeline/dagster/ predates src/dagster/)
├── ml/          # ML training pipeline: model code, dataset windowing, SageMaker training job
├── k8s/         # Kubernetes manifests
```

*(Note: Inference-time SageMaker Batch Transform job is orchestrated dynamically using SageMakerResource helper methods.)*

## Metadata Governance (names only, see docs for detail)

- `_CONATA_*`: system metadata attached at the Raw layer during ingestion.
- `DATACORE_*`: metadata attached at the Cleaned layer after dbt transformation.

## See Details

- `docs/architecture-design.md` — overall architecture, full data flow, phased implementation plan.
- `docs/ml-architecture-design.md` — ML model details.
- `docs/infrastructure-design.md` — AWS infrastructure details.
