import csv
import os


def test_load_job_defs_csv():
    # Construct the path to the CSV relative to this test file or workspace root
    base_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    csv_path = os.path.join(base_dir, "src", "dagster", "load_job_defs.csv")

    assert os.path.exists(csv_path), f"CSV file does not exist at {csv_path}"

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

        expected_header = [
            "table_name",
            "schema",
            "file_format",
            "trigger_type",
            "trigger_parameter",
        ]
        assert header == expected_header, f"Header mismatch! Found: {header}"

        rows = list(reader)
        row_count = len(rows)
        assert row_count == 15, f"Row count must be exactly 15. Found: {row_count}"

        for i, row in enumerate(rows, start=1):
            assert len(row) == 5, f"Row {i} does not have 5 columns: {row}"

            table_name, schema, file_format, trigger_type, trigger_parameter = row

            assert table_name.startswith("raw_"), (
                f"Row {i}: table_name '{table_name}' must start with 'raw_'"
            )
            assert schema == "bronze", (
                f"Row {i}: schema must be 'bronze'. Found '{schema}'"
            )
            assert file_format == "parquet", (
                f"Row {i}: file_format must be 'parquet'. Found '{file_format}'"
            )
            assert trigger_type == "SENSOR", (
                f"Row {i}: trigger_type must be 'SENSOR'. Found '{trigger_type}'"
            )
            assert trigger_parameter == "", (
                f"Row {i}: trigger_parameter must be empty for SENSOR type. "
                f"Found '{trigger_parameter}'"
            )
