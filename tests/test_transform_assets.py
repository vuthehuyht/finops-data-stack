from unittest.mock import MagicMock

from dagster import AssetKey, build_op_context, materialize_to_memory
from src.dagster.assets.transform_assets import TRANSFORM_ASSETS, DbtCliResource


def test_transform_assets_definition():
    """Xác minh các định nghĩa asset transform được sinh ra đầy đủ."""
    assert len(TRANSFORM_ASSETS) == 2

    # Tìm các asset tương ứng
    fct_asset = next(
        a for a in TRANSFORM_ASSETS if a.key.path == ["marts", "fct_stock_valuation"]
    )
    dim_asset = next(
        a for a in TRANSFORM_ASSETS if a.key.path == ["marts", "dim_market_sentiment"]
    )

    assert fct_asset is not None
    assert dim_asset is not None

    # Kiểm tra dependencies
    assert (
        AssetKey(["raw_batch", "raw_stock_price_eod"])
        in fct_asset.keys_by_input_name.values()
    )
    assert (
        AssetKey(["raw_batch", "raw_news_articles"])
        in dim_asset.keys_by_input_name.values()
    )


def test_transform_asset_execution_fallback():
    """Xác minh khi thực thi asset, cơ chế dbt cli được gọi và fallback
    thành công khi có lỗi.
    """
    from unittest.mock import patch

    # Khởi tạo instance thật của DbtCliResource
    mock_dbt = DbtCliResource(project_dir="mock/dir")

    # Mock context để log
    context = build_op_context()

    # Lấy hàm thực thi của asset fct_stock_valuation
    # Với Dagster, có thể chạy trực tiếp hàm của asset
    # thông qua attribute .op.compute_fn
    fct_asset = next(
        a for a in TRANSFORM_ASSETS if a.key.path == ["marts", "fct_stock_valuation"]
    )
    compute_fn = fct_asset.op.compute_fn.decorated_fn

    # Patch phương thức cli của lớp DbtCliResource do model bị frozen
    with patch.object(DbtCliResource, "cli") as mock_cli:
        mock_cli.side_effect = Exception("Redshift connection error")

        # Chạy hàm compute_fn của asset trực tiếp
        # signature: transform_fn(context, upstream_data, dbt)
        generator = compute_fn(context, upstream_data="mock_ticker_data", dbt=mock_dbt)
        results = list(generator)

    # Đảm bảo trả về output thành công ngay cả khi dbt cli ném ra exception
    assert len(results) == 1
    output = results[0]
    assert output.value == "fct_stock_valuation"

    # Dagster bao bọc metadata string trong TextMetadataValue, ta lấy trường text của nó
    status_val = getattr(output.metadata["status"], "text", output.metadata["status"])
    layer_val = getattr(output.metadata["layer"], "text", output.metadata["layer"])
    assert status_val == "success"
    assert layer_val == "marts"


def test_transform_asset_materialization():
    """Xác minh tích hợp Dagster chạy qua materialize_to_memory thành công."""
    from unittest.mock import patch

    from dagster import asset

    # Định nghĩa mock assets làm đầu vào upstream
    @asset(key_prefix=["raw_batch"])
    def raw_stock_price_eod():
        return "upstream_prices_ok"

    @asset(key_prefix=["raw_batch"])
    def raw_news_articles():
        return "upstream_sentiment_ok"

    # Khởi tạo instance thật của DbtCliResource
    mock_dbt = DbtCliResource(project_dir="mock/dir")
    # Giả lập dbt.cli().wait() trả về thành công
    mock_cli_run = MagicMock()
    mock_cli_run.wait.return_value = MagicMock(stdout="dbt run successful")

    # Patch phương thức cli của lớp DbtCliResource
    with patch.object(DbtCliResource, "cli", return_value=mock_cli_run):
        # Chạy thử materialize bằng cách truyền các mock assets
        # cùng với TRANSFORM_ASSETS
        result = materialize_to_memory(
            TRANSFORM_ASSETS + [raw_stock_price_eod, raw_news_articles],
            resources={"dbt": mock_dbt},
        )

    assert result.success
    materialized_keys = [
        event.asset_key.path[-1] for event in result.get_asset_materialization_events()
    ]
    assert "fct_stock_valuation" in materialized_keys
    assert "dim_market_sentiment" in materialized_keys
