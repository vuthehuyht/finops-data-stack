---
name: dagster-asset
description: Scaffold a new Dagster asset following this project's layer naming and metadata convention. Use when creating a new asset in src/dagster.
---

# Dagster Asset Scaffold

Generate a new Dagster `@asset` following the conventions in `.ai/coding-rules.md`.

## Conventions

- Naming pattern: `<layer>_<entity>`, e.g. `bronze_stock_price_eod`, `silver_financial_ratio`, `gold_ml_feature_set`.
- Every asset must declare a clear `description` and `metadata` (e.g. data source, update frequency).
- Asset dependencies must be explicit via `AssetIn`/`deps` — no implicit side effects between assets.
- Type hints required on the asset function signature (params and return).
- Comments explaining WHY (not WHAT) should be in Vietnamese, per `.ai/coding-rules.md`.
- Errors must raise clearly (no catch-and-ignore) so Dagster retry/alerting can react — see `.ai/coding-rules.md` "Error handling".

## Steps

1. Ask the user (if not already clear from context):
   - Layer: `bronze`, `silver`, or `gold`?
   - Entity name (what does this asset represent?).
   - Upstream dependencies (which existing assets does this read from, if any)?
   - Data source / update frequency for the `metadata`.
2. Determine the asset name: `<layer>__<entity>`.
3. Find the existing asset module structure under `src/dagster/` (e.g. grouped by layer or by domain) and place the new asset accordingly. If unclear, ask the user where to place it.
4. Write the asset function:
   ```python
   from dagster import asset, AssetIn

   @asset(
       description="<one-line description of what this asset produces>",
       metadata={"source": "<data source>", "update_frequency": "<frequency>"},
       ins={"<upstream_name>": AssetIn("<upstream_asset_key>")},  # omit ins= if no dependency
   )
   def <layer>_<entity>(<upstream_name>) -> <ReturnType>:
       ...
   ```
5. Do not invent the actual transformation/ingestion logic — leave a clear placeholder and ask the user to confirm or provide the real logic.
6. Remind the user this asset needs a corresponding unit test in `tests/` per `.ai/testing.md` before merging (mocking any S3/Redshift/SageMaker calls).
