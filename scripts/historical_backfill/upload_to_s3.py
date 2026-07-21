"""Script to upload local raw data to S3 following the ingestion pattern."""

import argparse
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import pandas as pd


def process_file(file_path: Path, table_name: str, bucket: str) -> None:
    """Process and upload a single file to S3."""
    if not file_path.is_file() or file_path.name.startswith("."):
        return

    # Use file stem as batch_date (e.g. 2021-04-19.csv -> 2021-04-19)
    batch_date = file_path.stem
    unix_timestamp = int(time.time())

    s3_key = (
        f"raw/{table_name}/"
        f"batch_date={batch_date}/"
        f"{unix_timestamp}/{table_name}.parquet"
    )

    s3_client = boto3.client("s3")

    if file_path.suffix == ".csv":
        print(f"  [START] {file_path.name} -> s3://{bucket}/{s3_key}")
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                print(f"  [SKIP] {file_path.name} is empty.")
                return

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                temp_path = tmp.name

            # Convert to snappy compressed parquet as required by design
            df.to_parquet(temp_path, compression="snappy", index=False)
            s3_client.upload_file(temp_path, bucket, s3_key)
            os.remove(temp_path)
            print(f"  [DONE] {file_path.name} uploaded successfully.")
        except Exception as e:
            print(f"  [ERROR] {file_path}: {e}")
            if "temp_path" in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
    elif file_path.suffix == ".parquet":
        print(f"  [START] {file_path.name} -> s3://{bucket}/{s3_key}")
        try:
            s3_client.upload_file(str(file_path), bucket, s3_key)
            print(f"  [DONE] {file_path.name} uploaded successfully.")
        except Exception as e:
            print(f"  [ERROR] {file_path}: {e}")
    else:
        print(f"  [SKIP] {file_path.name} (unsupported format)")


def main():
    parser = argparse.ArgumentParser(description="Upload local raw data to S3")
    parser.add_argument(
        "--input-dir", default="data/raw", help="Directory containing local data"
    )
    parser.add_argument("--bucket", required=True, help="Target S3 bucket name")
    parser.add_argument(
        "--workers", type=int, default=10, help="Number of concurrent threads"
    )
    args = parser.parse_args()

    base_path = Path(args.input_dir)

    if not base_path.exists():
        print(f"Directory {base_path} does not exist.")
        return

    # Collect all tasks
    tasks = []
    for table_dir in base_path.iterdir():
        if not table_dir.is_dir() or table_dir.name.startswith("."):
            continue

        table_name = table_dir.name
        for file_path in table_dir.glob("*"):
            if file_path.is_file() and not file_path.name.startswith("."):
                tasks.append((file_path, table_name, args.bucket))

    total_tasks = len(tasks)
    print(f"Found {total_tasks} files to upload.")

    if total_tasks == 0:
        return

    # Execute tasks concurrently
    print(f"Starting upload with {args.workers} workers...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(process_file, fpath, tname, bucket)
            for (fpath, tname, bucket) in tasks
        ]

        for count, future in enumerate(as_completed(futures), 1):
            # To catch any uncaught exceptions from threads
            future.result()
            if count % 50 == 0 or count == total_tasks:
                pct = (count / total_tasks) * 100
                print(f"Progress: {count}/{total_tasks} ({pct:.1f}%) files processed.")

    elapsed = time.time() - start_time
    print(f"Upload complete in {elapsed:.2f} seconds!")


if __name__ == "__main__":
    main()
