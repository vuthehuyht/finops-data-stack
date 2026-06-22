import dagster

from src.dagster.dbt_dependency import (
    Node,
    get_downstream,
    get_execution_plan_from_goals,
    get_execution_plan_from_starts,
    get_upstream,
    is_raw_asset,
    is_top_level_asset,
)


def test_get_upstream() -> None:
    """Test get_upstream function."""
    # Create mock asset keys
    asset_a = dagster.AssetKey(["A"])
    asset_b = dagster.AssetKey(["B"])
    asset_c = dagster.AssetKey(["C"])
    asset_d = dagster.AssetKey(["D"])

    # Create mock dependency graph
    all_assets = {
        asset_a: Node(key=asset_a, upstream=set(), downstream={asset_b}),
        asset_b: Node(key=asset_b, upstream={asset_a}, downstream={asset_c}),
        asset_c: Node(key=asset_c, upstream={asset_b}, downstream={asset_d}),
        asset_d: Node(key=asset_d, upstream={asset_c}, downstream=set()),
    }

    # Test get_upstream
    result = get_upstream([asset_d], all_assets)
    assert set(result.keys()) == {asset_a, asset_b, asset_c, asset_d}
    assert result[asset_d].upstream == {asset_c}
    assert result[asset_c].upstream == {asset_b}
    assert result[asset_b].upstream == {asset_a}
    assert result[asset_a].upstream == set()


def test_get_downstream() -> None:
    """Test get_downstream function."""
    # Create mock asset keys
    asset_a = dagster.AssetKey(["A"])
    asset_b = dagster.AssetKey(["B"])
    asset_c = dagster.AssetKey(["C"])
    asset_d = dagster.AssetKey(["D"])

    # Create mock dependency graph
    all_assets = {
        asset_a: Node(key=asset_a, upstream=set(), downstream={asset_b}),
        asset_b: Node(key=asset_b, upstream={asset_a}, downstream={asset_c}),
        asset_c: Node(key=asset_c, upstream={asset_b}, downstream={asset_d}),
        asset_d: Node(key=asset_d, upstream={asset_c}, downstream=set()),
    }

    # Test get_downstream
    result = get_downstream([asset_a], all_assets)
    assert set(result.keys()) == {asset_a, asset_b, asset_c, asset_d}
    assert result[asset_a].downstream == {asset_b}
    assert result[asset_b].downstream == {asset_c}
    assert result[asset_c].downstream == {asset_d}
    assert result[asset_d].downstream == set()


def test_is_raw_asset() -> None:
    """Test is_raw_asset function."""
    raw_asset = dagster.AssetKey(["RAW", "DATA"])
    non_raw_asset = dagster.AssetKey(["STAGING", "DATA"])

    assert is_raw_asset(raw_asset) is True
    assert is_raw_asset(non_raw_asset) is False


def test_is_top_level_asset() -> None:
    """Test is_top_level_asset function."""
    # Create mock asset keys
    asset_a = dagster.AssetKey(["A"])
    asset_b = dagster.AssetKey(["B"])
    asset_c = dagster.AssetKey(["C"])
    asset_d = dagster.AssetKey(["D"])

    # Create mock dependency graph
    all_assets = {
        asset_a: Node(key=asset_a, upstream=set(), downstream={asset_b}),
        asset_b: Node(key=asset_b, upstream={asset_a}, downstream={asset_c}),
        asset_c: Node(key=asset_c, upstream={asset_b}, downstream={asset_d}),
        asset_d: Node(key=asset_d, upstream={asset_c}, downstream=set()),
    }

    # Test is_top_level_asset
    assert is_top_level_asset(asset_a, all_assets) is True
    assert is_top_level_asset(asset_b, all_assets) is False
    assert is_top_level_asset(asset_c, all_assets) is False
    assert is_top_level_asset(asset_d, all_assets) is False


def test_get_execution_plan_from_starts() -> None:
    """Test get_execution_plan_from_starts function."""
    # Create mock asset keys
    # A,F-> B -> C -> D,E
    asset_a = dagster.AssetKey(["A"])
    asset_b = dagster.AssetKey(["B"])
    asset_c = dagster.AssetKey(["C"])
    asset_d = dagster.AssetKey(["D"])
    asset_e = dagster.AssetKey(["E"])
    asset_f = dagster.AssetKey(["F"])

    # Create mock dependency graph
    all_assets = {
        asset_a: Node(key=asset_a, upstream=set(), downstream={asset_b}),
        asset_b: Node(key=asset_b, upstream={asset_a, asset_f}, downstream={asset_c}),
        asset_c: Node(key=asset_c, upstream={asset_b}, downstream={asset_d, asset_e}),
        asset_d: Node(key=asset_d, upstream={asset_c}, downstream=set()),
        asset_e: Node(key=asset_e, upstream={asset_c}, downstream=set()),
        asset_f: Node(key=asset_f, upstream=set(), downstream={asset_b}),
    }

    # Test get_execution_plan_from_starts
    result = get_execution_plan_from_starts([asset_a], all_assets)
    assert len(result) == 4
    assert result[0] == {asset_a}
    assert result[1] == {asset_b}
    assert result[2] == {asset_c}
    assert result[3] == {asset_d, asset_e}


def test_get_execution_plan_from_goals() -> None:
    """Test get_execution_plan_from_goals function."""
    # Create mock asset keys
    # A,F-> B -> C -> D,E
    asset_a = dagster.AssetKey(["A"])
    asset_b = dagster.AssetKey(["B"])
    asset_c = dagster.AssetKey(["C"])
    asset_d = dagster.AssetKey(["D"])
    asset_e = dagster.AssetKey(["E"])
    asset_f = dagster.AssetKey(["F"])

    # Create mock dependency graph
    all_assets = {
        asset_a: Node(key=asset_a, upstream=set(), downstream={asset_b}),
        asset_b: Node(key=asset_b, upstream={asset_a, asset_f}, downstream={asset_c}),
        asset_c: Node(key=asset_c, upstream={asset_b}, downstream={asset_d, asset_e}),
        asset_d: Node(key=asset_d, upstream={asset_c}, downstream=set()),
        asset_e: Node(key=asset_e, upstream={asset_c}, downstream=set()),
        asset_f: Node(key=asset_f, upstream=set(), downstream={asset_b}),
    }

    # Test get_execution_plan_from_goals
    result = get_execution_plan_from_goals([asset_d], all_assets)
    assert len(result) == 4
    assert result[0] == {asset_a, asset_f}
    assert result[1] == {asset_b}
    assert result[2] == {asset_c}
    assert result[3] == {asset_d}
