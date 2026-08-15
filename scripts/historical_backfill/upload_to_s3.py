"""Script to upload local raw data to S3 following the ingestion pattern."""

import argparse
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import pandas as pd


def process_table(table_dir: Path, bucket: str) -> None:  # noqa: C901
    """Process and upload all files in a table directory as a single init file."""
    if not table_dir.is_dir() or table_dir.name.startswith("."):
        return

    table_name = table_dir.name
    dfs = []

    # Read all files in the directory
    for file_path in table_dir.glob("*"):
        if not file_path.is_file() or file_path.name.startswith("."):
            continue

        try:
            if file_path.suffix == ".csv":
                df = pd.read_csv(file_path, dtype=str, on_bad_lines="skip")
            elif file_path.suffix == ".parquet":
                df = pd.read_parquet(file_path)
                # Convert parquet types to string for consistency
                df = df.astype(str)
            else:
                print(f"  [SKIP] {file_path.name} (unsupported format)")
                continue

            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"  [ERROR] reading {file_path}: {e}")

    if not dfs:
        print(f"  [SKIP] {table_name} has no valid data.")
        return

    print(f"  [START] Aggregating {table_name} data...")
    # Concatenate all dataframes
    final_df = pd.concat(dfs, ignore_index=True)

    # Uppercase all columns to match base.py
    final_df.columns = final_df.columns.str.upper()

    # Ensure all columns are explicitly pandas 'string' dtype (not just object)
    # This guarantees PyArrow creates a String schema even if the column is all nulls.
    for col in final_df.columns:
        final_df[col] = final_df[col].replace(["nan", "NaN", "None", "<NA>"], pd.NA)
    final_df = final_df.astype("string")

    from datetime import datetime

    current_date = datetime.now().strftime("%Y-%m-%d")

    # Truncate string columns to prevent Redshift COPY Parquet length limit errors
    for col in final_df.select_dtypes(include=["object", "string"]).columns:
        final_df[col] = final_df[col].apply(
            lambda x: x[:16000] if isinstance(x, str) else x
        )

    unix_timestamp = int(time.time())
    s3_key = (
        f"raw/{table_name}/batch_date={current_date}/{unix_timestamp}/data_init.parquet"
    )

    s3_client = boto3.client("s3")

    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            temp_path = tmp.name

        # Convert to snappy compressed parquet as required by design
        final_df.to_parquet(temp_path, compression="snappy", index=False)
        s3_client.upload_file(temp_path, bucket, s3_key)
        os.remove(temp_path)
        print(f"  [DONE] {table_name} uploaded successfully to s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"  [ERROR] {table_name} upload failed: {e}")
        if "temp_path" in locals() and os.path.exists(temp_path):
            os.remove(temp_path)


def main():
    parser = argparse.ArgumentParser(description="Upload local raw data to S3")
    parser.add_argument(
        "--input-dir", default="data/raw", help="Directory containing local data"
    )
    parser.add_argument("--bucket", required=True, help="Target S3 bucket name")
    parser.add_argument(
        "--workers", type=int, default=10, help="Number of concurrent threads"
    )
    parser.add_argument(
        "--profile", default=None, help="AWS Profile to use for authentication"
    )
    args = parser.parse_args()

    if args.profile:
        boto3.setup_default_session(profile_name=args.profile)

    base_path = Path(args.input_dir)

    if not base_path.exists():
        print(f"Directory {base_path} does not exist.")
        return

    # Collect all table directories
    table_dirs = [
        d for d in base_path.iterdir() if d.is_dir() and not d.name.startswith(".")
    ]

    total_tables = len(table_dirs)
    print(f"Found {total_tables} tables to process and upload.")

    if total_tables == 0:
        return

    # Execute tasks concurrently
    print(f"Starting upload with {args.workers} workers...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(process_table, table_dir, args.bucket)
            for table_dir in table_dirs
        ]

        for count, future in enumerate(as_completed(futures), 1):
            # To catch any uncaught exceptions from threads
            future.result()
            print(f"Progress: {count}/{total_tables} tables processed.")

    elapsed = time.time() - start_time
    print(f"Upload complete in {elapsed:.2f} seconds!")


if __name__ == "__main__":
    main()
