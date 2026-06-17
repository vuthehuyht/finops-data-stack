"""
Unit tests for ingest_handler and metadata injection logic.
"""

from unittest.mock import patch

from src.load.ingest_handler import run_ingest


@patch("src.load.ingest_handler.save_and_upload_df")
def test_run_ingest_metadata_injection(mock_upload):
    """Xác minh metadata quản lý được tiêm chính xác
    và cột nghiệp vụ được cast sang string.
    """
    mock_upload.return_value = (
        "s3://mock-bucket/raw/market/stock_price_eod/mock_file.parquet"
    )

    # Gọi hàm ingest với mock crawler vnstock.fetch_stock_price_eod
    params = {"symbols": ["FPT"], "days_back": 1}
    s3_uri = run_ingest(
        source_client="vnstock",
        api_method="fetch_stock_price_eod",
        s3_key_prefix="raw/market/stock_price_eod/",
        params=params,
    )

    # Đảm bảo trả về đúng mock S3 URI
    assert s3_uri.startswith("s3://mock-bucket/")

    # Lấy DataFrame truyền vào hàm upload
    called_df = mock_upload.call_args[1]["df"]

    # Kiểm tra xem các cột ban đầu đã được ép sang string chưa
    assert called_df["open"].dtype == object
    assert called_df["close"].dtype == object
    assert called_df["volume"].dtype == object

    # Kiểm tra sự tồn tại của 5 cột metadata
    metadata_cols = [
        "BATCH_DATE",
        "_CONATA_SOURCE",
        "_CONATA_SOURCE_ROW_NUMBER",
        "_CONATA_PARTITION_KEY",
        "_CONATA_LOADED_AT",
    ]
    for col in metadata_cols:
        assert col in called_df.columns

    # Kiểm tra giá trị của các cột metadata
    assert called_df["_CONATA_SOURCE"].iloc[0] == "VNSTOCK"
    assert called_df["_CONATA_SOURCE_ROW_NUMBER"].iloc[0] == 1
    assert len(called_df["BATCH_DATE"].iloc[0]) == 10  # YYYY-MM-DD
    assert len(called_df["_CONATA_PARTITION_KEY"].iloc[0]) == 8  # YYYYMMDD
    assert len(called_df["_CONATA_LOADED_AT"].iloc[0]) == 19  # YYYY-MM-DD HH:MM:SS
