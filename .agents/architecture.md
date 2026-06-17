# FinOps Data Stack — Architecture Context

> Summary of architecture & data flow. Full detail: `docs/architecture-design.md`, `docs/ml-architecture-design.md`, `docs/infrastructure-design.md`.

## Tech Stack

Data Mesh / Lakehouse architecture on AWS, orchestrated by **Dagster** (Software-Defined Assets):

- **Orchestration**: Dagster.
- **Compute/Deployment**: Amazon EKS.
- **Data Lake (Bronze)**: Amazon S3 — raw data in Parquet/JSON.
- **Data Warehouse (Silver/Gold)**: Amazon Redshift Serverless.
- **Transformation**: dbt (data build tool).
- **Machine Learning**: Amazon SageMaker — on-demand Training, Serverless Inference, Model Registry.
- **Security**: AWS IAM, KMS, Secrets Manager, VPC.
- **CI/CD**: GitHub Actions → deploy to EKS.

## Data Flow — 2 main pipelines

### 1. Daily Inference Pipeline (after market close / EOD)
1. Ingestion (EKS → S3): collect raw data, tag with `_CONATA_*` metadata, store as Parquet.
2. Sensor & Loading (S3 → Redshift Bronze): trigger `COPY` to load raw tables.
3. Transformation (dbt Silver & Gold): type casting + `DATACORE_*` metadata, then feature engineering at Gold (Mart) layer.
4. Data Quality Gate: validate `fact_ml_feature_set` meets standards before continuing.
5. ML Inference: call SageMaker Serverless Endpoint (scale-to-zero).
6. Results Publishing: write forecast results to Redshift Gold + dashboard.

### 2. Quarterly Re-training Pipeline (after new financial statements are published)
1. Historical Data Preparation: aggregate feature set from the last 12-24 months from Redshift.
2. Training Job (SageMaker GPU) → model artifact pushed to S3.
3. Model Registration into SageMaker Model Registry with evaluation metrics.
4. Model Evaluation & Approval: compare Challenger vs Champion.
5. Serverless Deployment: update endpoint to use the newly approved version.

## `src/` Directory Structure (Flywheel Architecture)

```text
src/
├── common/      # Shared AWS Utils, DB Utils, Logging
├── dagster/     # Assets, Jobs, Sensors, Resources
├── load/        # Ingestion layer (Crawl/API -> S3)
├── transform/   # dbt project (transformation)
├── pipeline/    # End-to-end processing flows
├── ml/          # Machine Learning (Train/Inference)
├── docker/      # Dockerfiles
└── k8s/         # Kubernetes manifests
```

## Metadata Governance (names only, see docs for detail)

- `_CONATA_*`: system metadata attached at the Raw layer during ingestion.
- `DATACORE_*`: metadata attached at the Cleaned layer after dbt transformation.

## See Details

- `docs/architecture-design.md` — overall architecture, full data flow, phased implementation plan.
- `docs/ml-architecture-design.md` — ML model details.
- `docs/infrastructure-design.md` — AWS infrastructure details.
