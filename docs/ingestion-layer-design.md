# Ingestion Layer Design Specification

## 1. Overview
The Ingestion Layer is the first step in the data pipeline. It is responsible for fetching raw data from various sources (such as stock market APIs, corporate financial data, and macroeconomic portals), standardizing the data format, converting it to optimized Parquet files, and uploading it to AWS S3. 

Once the data is successfully uploaded, it triggers downstream loading and transformation processes.

---

## 2. Directory Structure
The ingestion logic will be housed under the `src/ingest/` directory. No README or CLI entrypoint scripts will be created in this directory, as the pipelines are designed to be imported and orchestrated directly by Dagster assets.

```text
src/ingest/
├── client/
│   ├── __init__.py
│   ├── vnstock_client.py       # API client wrapping the vnstock3 library
│   └── macro_client.py         # Crawlers and API clients for macroeconomic data
└── pipeline/
    ├── __init__.py
    ├── base.py                 # Core IngestPipeline base class with shared utilities
    ├── market_pipeline.py      # Standardizes and runs ingestion for market data
    └── fundamental_pipeline.py # Standardizes and runs ingestion for company fundamentals
```

---

## 3. Data Processing Lifecycle
Each ingestion execution follows a 4-stage processing lifecycle:

### A. Fetch Phase
* API clients (e.g., `vnstock_client.py` using `vnstock3`) fetch raw records.
* The fetched records are initially loaded into a standard `pandas.DataFrame`.

### B. Standardize Phase
* All column names are converted to uppercase: `df.columns = df.columns.str.upper()`.
* Four mandatory Conata-managed metadata columns are injected into the DataFrame:
  * `_CONATA_SOURCE`: URI pointing to the data origin (e.g., `api://vnstock/stock_price/TCB`).
  * `_CONATA_SOURCE_ROW_NUMBER`: 1-based sequential row index for each record in the current batch.
  * `_CONATA_PARTITION_KEY`: The target execution date in string format (e.g., `2026-06-18`).
  * `_CONATA_LOADED_AT`: Current UTC execution timestamp.

### C. Serialize Phase
* The standardized DataFrame is written to a temporary local `.parquet` file using Snappy compression to optimize storage and query performance:
  `df.to_parquet(file_path, compression='snappy', index=False)`.

### D. AWS S3 Upload Phase
* The local Parquet file is uploaded to the target AWS S3 bucket utilizing the helper functions from `src/common/s3_util.py`.
* **S3 Path Partitioning Strategy**:
  `s3://<bucket_name>/raw/<table_name>/batch_date=<date>/<timestamp>/<table_name>.parquet`
  * Here, `<timestamp>` is the dynamic Unix epoch timestamp (integer) of the execution time, preventing file overrides on multiple runs within the same batch date.
  * Example: `s3://finops-raw-dev/raw/RAW_STOCK_PRICE_EOD/batch_date=2026-06-18/1718715432/RAW_STOCK_PRICE_EOD.parquet`

---

## 4. Dagster Integration & Downstream Trigger
* **Ingest Asset**: A Dagster asset represents the ingestion of a specific entity. Its materialization generates and uploads the Parquet file to S3 and registers the `s3_url` metadata tag in Dagster.
* **Sensor/Dependency Trigger**:
  * The Redshift RAW load asset depends on the Ingestion Asset.
  * The Dagster load sensor (`load_job_sensor`) detects the ingestion materialization, parses the `s3_url` using regular expressions to extract `batch_date` and `table_name`, and schedules the Redshift COPY job (`load_s3_to_redshift`) to copy data into Redshift raw tables.

---

## 5. Testing & Verification Plan
* **Unit Tests**:
  * Located under `tests/ingest/`.
  * Verify dataframe uppercase conversion and metadata field injection.
  * Mock local file system operations and boto3/S3 Client to ensure correct Parquet serialization and S3 partitioned path mapping.
* **Linter & Formatting**:
  * Run `uv run ruff check` and `uv run ruff format` on all new files to ensure code quality.
