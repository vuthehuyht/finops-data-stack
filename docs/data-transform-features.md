# Data Transformation & Feature Engineering

Tài liệu này định nghĩa các quy tắc xử lý dữ liệu từ tầng Bronze (Raw) lên tầng Silver (Cleaned) và tầng Gold (Feature Engineering) để phục vụ cho các mô hình Machine Learning/Deep Learning.

## 1. Tầng Silver (Cleaned Data & Type Casting)

Mục tiêu của tầng này là làm sạch dữ liệu thô (được lưu dưới dạng String), ép về đúng kiểu dữ liệu (Type Casting) và chuẩn hóa đơn vị.

> **[UPDATE - Kiến trúc Tinh gọn]**: Theo yêu cầu phát triển linh hoạt, hệ thống tạm thời loại bỏ dbt và DuckDB. Toàn bộ logic Transform & Feature Engineering được xử lý thủ công bằng Python thuần (Pandas) tại `scripts/dataset_builder.py`.
> Bộ tính năng hiện tại đã được chắt lọc lại thành **24 Core Features**, bao gồm:
>
> - **6 Sequence Features (LSTM)**: `close`, `volume`, `sma_20`, `sma_50`, `rsi_14`, `macd`.
> - **18 Tabular Features (MLP)**: `market_cap`, `pe_ratio`, `pb_ratio`, `roe`, `roa`, `price_momentum_1m`, `price_momentum_3m`, `volatility_30d`, `relative_strength_vs_vnindex`, `revenue_growth_yoy`, `net_profit_growth_yoy`, `gross_margin`, `debt_to_equity`, `operating_cash_flow_to_net_income`, `foreign_buy_ratio_10d`, `net_foreign_flow_momentum_1m`, `prop_trading_net_val_5d`, `prop_vs_foreign_correlation_10d`.

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

- `price_momentum_1m`: `(close_today / close_30d_ago) - 1`
- `price_momentum_3m`: `(close_today / close_90d_ago) - 1`
- `volatility_30d`: Độ lệch chuẩn của lợi suất hàng ngày trong 30 phiên gần nhất.
- `relative_strength_vs_vnindex`: `stock_return_1m - vnindex_return_1m`
- `moving_average_20d / 50d / 200d`: Các đường trung bình động để xác định xu hướng.

### 2.2. Từ bảng `raw_income_statement` (Báo cáo KQKD)

- `revenue_growth_yoy`: `(revenue_curr / revenue_yoy) - 1`
- `net_profit_growth_yoy`: `(net_profit_curr / net_profit_yoy) - 1`
- `gross_margin`: `gross_profit / revenue`
- `net_margin`: `net_profit_after_tax / revenue`
- `operating_margin`: `operating_profit / revenue`
- `roe`: `net_profit_ttm / average_equity` (Tính toán từ Income Statement & Balance Sheet)
- `roa`: `net_profit_ttm / average_total_assets`

### 2.3. Từ bảng `raw_balance_sheet` (Bảng cân đối kế toán)

- `asset_growth_yoy`: `(total_assets_curr / total_assets_yoy) - 1`
- `debt_to_equity`: `total_debt / equity`
- `current_ratio`: `current_assets / current_liabilities`
- `cash_to_assets`: `cash_and_equivalents / total_assets`
- `equity_multiplier`: `total_assets / equity`
- `pe_ratio`: `market_cap / net_profit_ttm`
- `pb_ratio`: `market_cap / equity`

### 2.4. Từ bảng `raw_cashflow_statement` (Báo cáo Lưu chuyển tiền tệ)

- `operating_cash_flow_to_net_income`: `cfo / net_profit_after_tax`
- `free_cash_flow`: `cfo - capital_expenditure`
- `cash_flow_to_debt`: `cfo / total_debt`

### 2.5. Từ bảng `raw_foreign_trading` (Giao dịch khối ngoại)

- `foreign_buy_ratio_10d`: `sum(buy_val_10d) / sum(total_trading_val_10d)`
- `net_foreign_flow_momentum_1m`: Xu hướng mua/bán ròng của khối ngoại trong 1 tháng.

### 2.6. Từ bảng `raw_proprietary_trading` (Giao dịch tự doanh)

- `prop_trading_net_val_5d`: Giá trị mua/bán ròng của tự doanh trong 5 phiên gần nhất.
- `prop_vs_foreign_correlation_10d`: Tương quan hành vi giữa tự doanh và khối ngoại.

### 2.7. Từ bảng `raw_insider_transactions` (Giao dịch nội bộ)

- `insider_sentiment_signal`: Biến phân loại (-1: Bán, 1: Mua, 0: Không có) dựa trên giao dịch của lãnh đạo/cổ đông lớn.
- `insider_buy_volume_ratio`: Tỷ lệ khối lượng đăng ký mua của nội bộ so với khối lượng lưu hành.

### 2.8. Từ bảng `raw_commodities_price` (Giá hàng hóa)

- `commodity_price_momentum_1m`: Tốc độ thay đổi giá hàng hóa (Dầu Brent, Singapore Gasoline, Baltic Index...) trong 1 tháng.
- `stock_commodity_correlation_30d`: Tương quan rolling 30 ngày giữa giá cổ phiếu và giá hàng hóa liên quan.

### 2.9. Từ bảng `raw_news_articles` (Tin tức - Sau xử lý NLP)

- `daily_news_sentiment_score`: Điểm cảm xúc trung bình trong ngày (-1 đến 1).
- `sentiment_momentum_7d`: Trung bình động sentiment score trong 7 ngày.
- `news_velocity`: Số lượng tin tức xuất hiện trong ngày so với trung bình (Đo lường sự chú ý của truyền thông).

### 2.10. Từ bảng `raw_macro_indicators` (Dữ liệu vĩ mô)

- `real_interest_rate`: `nominal_interest_rate - inflation_rate`
- `exchange_rate_volatility_30d`: Độ biến động của tỷ giá.

### 2.11. Features Đặc thù Ngành Dầu khí (Sector-Specific)

Dành riêng cho các mã như PVD, PVS (Thượng nguồn), BSR (Hạ nguồn), GAS (Trung nguồn), PVT (Vận tải).

- **`oil_beta_30d`**: Hệ số Beta đo lường độ nhạy của giá cổ phiếu so với giá dầu Brent (Sử dụng hồi quy tuyến tính trong 30 phiên).
- **`crack_spread_proxy` (Cho BSR)**: Chênh lệch giữa giá sản phẩm đầu ra (Xăng, Dầu DO) và giá dầu thô đầu vào. Công thức: `price_gasoline_singapore - price_brent_crude`.
- **`lifting_cost_proxy`**: Tỷ lệ `cogs / revenue` theo dõi theo quý để ước tính biến động chi phí khai thác.
- **`shipping_rate_correlation` (Cho PVT)**: Tương quan giữa giá cổ phiếu và chỉ số cước vận tải dầu thô (Baltic Dirty Tanker Index).
- **`gas_oil_price_ratio`**: Tỷ lệ giá khí so với giá dầu để đánh giá tính cạnh tranh của nhiên liệu.

## 3. Cấu trúc tầng Mart (Gold Layer) & ML Readiness

Để đảm bảo tính linh hoạt và dễ bảo trì, tầng Mart sẽ được tổ chức theo cấu trúc Modular (chia để trị) trước khi tổng hợp thành bảng Feature Set cuối cùng.

### 3.1. Các Sub-marts thành phần (Intermediate Gold)

Các bảng này tính toán độc lập và có thể tái sử dụng cho nhiều mục đích khác nhau.

- **`mart_stock_market_momentum`**: Tập hợp các feature từ dữ liệu giá EOD (MA, RSI, Volatility, RS). *Granularity: Ticker + Date.*
- **`mart_stock_fundamental_metrics`**: Tập hợp các chỉ số tài chính từ BCTC (ROE, PE, Debt/Equity). *Granularity: Ticker + Date (Forward-filled từ dữ liệu Quý).*
- **`mart_stock_sentiment_scores`**: Tập hợp các điểm sentiment từ tin tức và sự kiện. *Granularity: Ticker + Date.*
- **`mart_macro_commodities_signals`**: Các chỉ số vĩ mô và giá hàng hóa (Dầu, Tỷ giá). *Granularity: Date.*
- **`mart_insider_proprietary_flows`**: Tín hiệu từ giao dịch nội bộ và tự doanh. *Granularity: Ticker + Date.*

### 3.2. Bảng tổng hợp cuối cùng: `fact_ml_feature_set`

Đây là bảng "phẳng" (Wide Table) duy nhất cung cấp dữ liệu cho mô hình Deep Learning. Bảng này được tạo bằng cách `LEFT JOIN` tất cả các Sub-marts trên theo `ticker` và `trading_date`.

### 3.3. Target Labels (Nhãn mục tiêu cho ML)

Để mô hình có thể học và nhận diện (predict), chúng ta cần tạo thêm các cột nhãn dựa trên dữ liệu giá tương lai (Future Returns).

- **`label_next_5d_return`**: Tỷ suất sinh lời sau 5 ngày làm việc.
- **`label_next_20d_return`**: Tỷ suất sinh lời sau 1 tháng (20 phiên).
- **`label_is_uptrend_30d`**: Biến phân loại (1 nếu giá trung bình 30 ngày tới cao hơn hiện tại > 5%, ngược lại là 0).
- **`label_max_drawdown_next_10d`**: Mức sụt giảm tối đa có thể xảy ra trong 10 ngày tới (để đánh giá rủi ro).

**Lưu ý quan trọng:** Khi sử dụng bảng này để huấn luyện, các cột `label_*` chỉ được dùng làm mục tiêu (Target), tuyệt đối không được dùng làm biến đầu vào (Feature) để tránh lỗi rò rỉ dữ liệu (Data Leakage).
