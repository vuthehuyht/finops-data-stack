# Data Transformation & Feature Engineering

Tài liệu này định nghĩa các quy tắc xử lý dữ liệu từ tầng Bronze (Raw) lên tầng Silver (Cleaned) và tầng Gold (Feature Engineering) để phục vụ cho các mô hình Machine Learning/Deep Learning.

## 1. Tầng Silver (Cleaned Data & Type Casting)

Mục tiêu của tầng này là làm sạch dữ liệu thô (được lưu dưới dạng String), ép về đúng kiểu dữ liệu (Type Casting) và chuẩn hóa đơn vị.

> **[UPDATE - Kiến trúc thực tế]**: Toàn bộ logic Transform & Feature Engineering (Silver + Gold layer) chạy bằng **dbt** trên Redshift (`src/transform/dbt/models/STG/` và `src/transform/dbt/models/MART/`), được orchestrate bởi Dagster (`src/dagster/dbt_assets.py`, `src/dagster/transform_job.py`). `scripts/dataset_builder.py` (Pandas thuần) chỉ là script hỗ trợ cho smoke test SageMaker (`scripts/sagemaker_smoke_test/`), **không phải** pipeline chính thức.
> Danh sách feature đưa vào model được khai báo tường minh (không suy ra tự động từ schema `FACT_ML_FEATURE_SET`) tại `src/ml/config.py`, gồm **19 Core Features** (`WINDOW_SIZE = 30` ngày):
>
> - **6 Sequence Features (LSTM)**: `moving_average_20d`, `moving_average_50d`, `price_momentum_1m`, `price_momentum_3m`, `volatility_30d`, `relative_strength_vs_vnindex`.
> - **13 Tabular Features (MLP)**: `pe_ratio`, `pb_ratio`, `roe`, `roa`, `revenue_growth_yoy`, `net_profit_growth_yoy`, `gross_margin`, `debt_to_equity`, `operating_cash_flow_to_net_income`, `foreign_buy_ratio_10d`, `net_foreign_flow_momentum_1m`, `prop_trading_net_val_5d`, `prop_vs_foreign_correlation_10d`.
>
> `fact_ml_feature_set` (xem mục 3.2) chứa nhiều cột hơn 19 feature này (label, các cột macro/sentiment placeholder...) — chỉ các cột trong hai danh sách trên mới thực sự được đưa vào model.

### Quy tắc chung

- **Dates:** Chuyển đổi định dạng `YYYY-MM-DD` hoặc `DD/MM/YYYY` về chuẩn `DATE`.
- **Monetary Values (Giá trị tiền tệ):** Cast từ String sang `FLOAT` (hoặc `DECIMAL` trên Redshift). Chuẩn hóa tất cả về đơn vị **Tỷ VNĐ** để dễ tính toán.
- **Percentages (Phần trăm):** Các giá trị như "5.5%" phải được loại bỏ ký tự "%" và cast về dạng thập phân `0.055`.
- **Deduplication:** Loại bỏ các record trùng lặp dựa trên khóa chính (Primary Key).

### 1.1. Metadata Quản trị Dữ liệu (Cleaned & Mart Layers)

Tại tầng **Silver (Cleaned)** và **Gold (Mart)**, các trường metadata sau được sử dụng để theo dõi lịch sử cập nhật và quản trị dữ liệu:

| Tên Trường | Ý nghĩa |
| :--- | :--- |
| `DATACORE_CREATE_DATETIME` | Thời điểm bản ghi được tạo ra (Timestamp). |
| `DATACORE_CREATE_PROGRAM` | Tên chương trình/job thực hiện tạo bản ghi. |
| `DATACORE_CREATE_BY` | Tên hệ thống/user thực hiện tạo bản ghi. |
| `DATACORE_UPDATE_DATETIME` | Thời điểm bản ghi được cập nhật gần nhất (Timestamp). |
| `DATACORE_UPDATE_PROGRAM` | Tên chương trình/job thực hiện cập nhật bản ghi. |
| `DATACORE_UPDATE_BY` | Tên hệ thống/user thực hiện cập nhật bản ghi. |
| `BATCH_DATE` | Ngày chạy batch xử lý dữ liệu. |

## 2. Tầng Gold (Feature Engineering by Source Table)

Dưới đây là danh mục các Features được tổng hợp thêm dựa trên từng bảng dữ liệu nguồn cụ thể.

### 2.1. Từ bảng `raw_stock_price_eod` (Giá chứng khoán)

Model: `mart_stock_market_momentum` (`ticker` + `trading_date`).

- `price_momentum_1m`: `(adjusted_close_today / adjusted_close_30d_ago) - 1`
- `price_momentum_3m`: `(adjusted_close_today / adjusted_close_90d_ago) - 1`
- `volatility_30d`: Độ lệch chuẩn (`STDDEV`) của lợi suất hàng ngày (`adjusted_close`) trong 30 phiên gần nhất.
- `relative_strength_vs_vnindex`: `avg_daily_return_1m(stock) - avg_daily_return_1m(vnindex)`, cả hai tính trung bình 20 phiên gần nhất.
- `moving_average_20d / 50d / 200d`: Trung bình động của `adjusted_close` trong 20/50/200 phiên gần nhất.

### 2.2. Từ bảng `raw_income_statement` (Báo cáo KQKD)

Model: `mart_stock_fundamental_metrics` (join với Balance Sheet, Cashflow, Company Profile — snapshot quý mới nhất, forward-fill theo ngày).

- `revenue_growth_yoy`: `(revenue_curr / revenue_4quarters_ago) - 1`
- `net_profit_growth_yoy`: `(net_profit_after_tax_curr / net_profit_after_tax_4quarters_ago) - 1`
- `gross_margin`: `gross_profit / revenue`
- `net_margin`: `net_profit_after_tax / revenue`
- `operating_margin`: `operating_profit / revenue`
- `roe`: `net_profit_after_tax / equity` (quý gần nhất, không phải TTM)
- `roa`: `net_profit_after_tax / total_assets` (quý gần nhất, không phải TTM)

### 2.3. Từ bảng `raw_balance_sheet` (Bảng cân đối kế toán)

- `asset_growth_yoy`: `(total_assets_curr / total_assets_4quarters_ago) - 1`
- `debt_to_equity`: `(short_term_debt + long_term_debt) / equity`
- `current_ratio`: `current_assets / total_liabilities`
- `cash_to_assets`: `cash / total_assets`
- `equity_multiplier`: `total_assets / equity`
- `pe_ratio`: `(close * outstanding_share) / net_profit_after_tax` — `outstanding_share` lấy từ `raw_company_profile`.
- `pb_ratio`: `(close * outstanding_share) / equity`

### 2.4. Từ bảng `raw_cashflow_statement` (Báo cáo Lưu chuyển tiền tệ)

- `operating_cash_flow_to_net_income`: `cfo / net_profit_after_tax`
- `free_cash_flow`: `cfo - capex`
- `cash_flow_to_debt`: `cfo / (short_term_debt + long_term_debt)`

### 2.5. Từ bảng `raw_foreign_trading` (Giao dịch khối ngoại)

Model: `mart_insider_proprietary_flows` (join với Proprietary Trading).

- `foreign_buy_ratio_10d`: `sum(buy_val, 10d) / sum(buy_val + sell_val, 10d)`
- `net_foreign_flow_1m`: Tổng giá trị mua/bán ròng (`net_val`) của khối ngoại trong 20 phiên gần nhất.
- `net_foreign_flow_momentum_1m`: `(net_foreign_flow_1m / |net_foreign_flow_1m cách đây 20 phiên|) - 1`

### 2.6. Từ bảng `raw_proprietary_trading` (Giao dịch tự doanh)

- `prop_trading_net_val_5d`: Tổng giá trị mua/bán ròng (`net_val`) của tự doanh trong 5 phiên gần nhất.
- `prop_vs_foreign_correlation_10d`: Hệ số tương quan Pearson (rolling 10 phiên) giữa `net_val` tự doanh và `net_val` khối ngoại.

> **Lưu ý:** Codebase hiện **không có** raw source cho giao dịch nội bộ (insider transactions) — model `mart_insider_proprietary_flows` chỉ tổng hợp dữ liệu khối ngoại + tự doanh, mặc dù tên model gợi ý "insider". Các feature `insider_sentiment_signal`, `insider_buy_volume_ratio` đã được loại bỏ khỏi tài liệu này vì không có implementation tương ứng.

### 2.7. Từ bảng `raw_commodities_price` (Giá hàng hóa)

Model: `mart_macro_commodities_signals` (granularity: Date, không có `ticker`).

- `brent_price_momentum_1m`: `(brent_price_today / brent_price_30d_ago) - 1`.
- `crack_spread_proxy`: `gasoline_singapore_price - brent_crude_price`.
- Các giá hàng hóa khác (`wti_price`, `gold_price`, `steel_hrc_price`, `gasoline_singapore_price`, `baltic_dirty_tanker_index`) được pass-through nguyên giá trị, **chưa có** feature phái sinh (momentum, correlation) cho từng loại.

> **Lưu ý:** `commodity_price_momentum_1m` (generic, nhiều commodity) và `stock_commodity_correlation_30d` (tương quan giá cổ phiếu ↔ hàng hóa) mô tả trong bản trước **chưa được implement** — hiện chỉ có momentum cho riêng Brent Crude.

### 2.8. Từ bảng `raw_news_articles` (Tin tức)

Model: `mart_stock_sentiment_scores` (join với Analyst Reports, Corporate Events).

- `news_count`: Số bài tin trong ngày cho ticker.
- `daily_news_sentiment_score`, `sentiment_momentum_7d`: **Placeholder `NULL`** — chưa có pipeline NLP điền điểm cảm xúc vào tầng Silver; schema đã sẵn sàng nhận dữ liệu khi pipeline NLP hoàn thiện.
- `news_velocity`: `news_count / avg(news_count, 30 phiên gần nhất)`.
- `analyst_report_count`: Số báo cáo phân tích trong ngày.
- `analyst_buy_count`, `avg_analyst_target_price`: **Placeholder `NULL`** — nguồn phân tích hiện chỉ có tiêu đề/mô tả dạng text, chưa trích xuất được khuyến nghị/giá mục tiêu có cấu trúc.
- `corporate_event_count`, `dividend_event_count`: Số sự kiện doanh nghiệp trong ngày (tổng và riêng sự kiện cổ tức).

### 2.9. Macro & hàng hóa (`raw_macro_indicators`, `raw_interest_rates`, `raw_exchange_rates`)

Cùng model `mart_macro_commodities_signals` ở mục 2.7.

- `real_interest_rate`: `fed_rate - cpi_value` (`fed_rate` là proxy lãi suất Fed từ `^IRX`, không phải lãi suất điều hành VN).
- `exchange_rate_volatility_30d`: Độ lệch chuẩn (`STDDEV`) của lợi suất USD/VND (`pair = 'USDT/VND'`) trong 30 phiên gần nhất.
- Các cột pass-through khác: `fed_rate`, `treasury_10y`, `treasury_5y`, `usd_vnd_rate`, `cpi_value`, `policy_rate`.

## 3. Cấu trúc tầng Mart (Gold Layer) & ML Readiness

Để đảm bảo tính linh hoạt và dễ bảo trì, tầng Mart sẽ được tổ chức theo cấu trúc Modular (chia để trị) trước khi tổng hợp thành bảng Feature Set cuối cùng.

### 3.1. Các Sub-marts thành phần (Intermediate Gold)

Các bảng này tính toán độc lập và có thể tái sử dụng cho nhiều mục đích khác nhau.

- **`mart_stock_market_momentum`**: Tập hợp các feature từ dữ liệu giá EOD (Moving Average, Volatility, Relative Strength). *Granularity: Ticker + Date.*
- **`mart_stock_fundamental_metrics`**: Tập hợp các chỉ số tài chính từ BCTC (ROE, PE, Debt/Equity). *Granularity: Ticker + Date (Forward-filled từ dữ liệu Quý).*
- **`mart_stock_sentiment_scores`**: Tập hợp tín hiệu từ tin tức, báo cáo phân tích, sự kiện doanh nghiệp (sentiment score hiện là placeholder `NULL`, xem mục 2.8). *Granularity: Ticker + Date.*
- **`mart_macro_commodities_signals`**: Các chỉ số vĩ mô, lãi suất, tỷ giá và giá hàng hóa (Dầu, Vàng, Thép...). *Granularity: Date (không có `ticker`).*
- **`mart_insider_proprietary_flows`**: Tín hiệu từ giao dịch khối ngoại và tự doanh (**không** có dữ liệu giao dịch nội bộ dù tên model gợi ý vậy — xem lưu ý mục 2.6). *Granularity: Ticker + Date.*

### 3.2. Bảng tổng hợp cuối cùng: `fact_ml_feature_set`

Đây là bảng "phẳng" (Wide Table) duy nhất cung cấp dữ liệu cho mô hình Deep Learning. Bảng này được tạo bằng cách `LEFT JOIN` các Sub-marts trên `ticker` + `trading_date` (riêng `mart_macro_commodities_signals` chỉ join theo `trading_date`, vì không có `ticker`).

### 3.3. Target Labels (Nhãn mục tiêu cho ML)

Để mô hình có thể học và nhận diện (predict), chúng ta cần tạo thêm các cột nhãn dựa trên dữ liệu giá tương lai (Future Returns).

- **`label_next_5d_return`**: Tỷ suất sinh lời sau 5 ngày làm việc.
- **`label_next_20d_return`**: Tỷ suất sinh lời sau 1 tháng (20 phiên).
- **`label_is_uptrend_30d`**: Biến phân loại (1 nếu lợi suất 20 phiên (~1 tháng) tới > 5%, ngược lại là 0).
- **`label_max_drawdown_next_10d`**: Mức sụt giảm tối đa có thể xảy ra trong 10 ngày tới (để đánh giá rủi ro).

**Lưu ý quan trọng:** Khi sử dụng bảng này để huấn luyện, các cột `label_*` chỉ được dùng làm mục tiêu (Target), tuyệt đối không được dùng làm biến đầu vào (Feature) để tránh lỗi rò rỉ dữ liệu (Data Leakage).
