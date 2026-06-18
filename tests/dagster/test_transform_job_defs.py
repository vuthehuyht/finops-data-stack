import csv
import os


def test_transform_job_defs_csv() -> None:
    base_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    csv_path = os.path.join(base_dir, "src", "dagster", "transform_job_defs.csv")

    assert os.path.exists(csv_path), f"CSV file does not exist at {csv_path}"

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

        expected_header = [
            "schema_suffix",
            "table_name",
            "trigger_type",
            "trigger_parameter",
        ]
        assert header == expected_header, f"Header mismatch! Found: {header}"

        rows = list(reader)
        row_count = len(rows)
        assert row_count == 17, f"Row count must be exactly 17. Found: {row_count}"

        for i, row in enumerate(rows, start=1):
            assert len(row) == 4, f"Row {i} does not have 4 columns: {row}"

            schema_suffix, table_name, trigger_type, trigger_parameter = row

            assert schema_suffix == "SILVER", (
                f"Row {i}: schema_suffix must be 'SILVER'. Found '{schema_suffix}'"
            )
            assert table_name.startswith("stg_"), (
                f"Row {i}: table_name '{table_name}' must start with 'stg_'"
            )
            assert trigger_type == "SENSOR", (
                f"Row {i}: trigger_type must be 'SENSOR'. Found '{trigger_type}'"
            )
            assert trigger_parameter == "", (
                f"Row {i}: trigger_parameter must be empty. Found '{trigger_parameter}'"
            )
