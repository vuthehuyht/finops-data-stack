# Data Schema & Source Mapping

Tài liệu này định nghĩa chi tiết danh mục các nguồn dữ liệu, cấu trúc các bảng raw data (Bronze Layer trên S3/Redshift) phục vụ cho hệ thống FinOps Data Stack.

**Lưu ý quan trọng về Data Type:** Tại tầng **Bronze (Raw)**:

1. **Dữ liệu nghiệp vụ:** Tất cả các thuộc tính thu thập từ nguồn sẽ được lưu trữ dưới dạng **String** (hoặc Varchar). Việc ép kiểu sẽ thực hiện ở tầng Silver.
1. **Metadata quản trị:** Các trường hệ thống (Metadata) sẽ có **Kiểu dữ liệu chính xác** (Date, Timestamp, Integer) để phục vụ việc phân vùng (Partitioning) và quản lý vận hành.

## 1. Dữ liệu Thị trường (Market Data)

Dữ liệu giao dịch hàng ngày phản ánh cung cầu của thị trường.

| Tên Bảng | Thuộc tính (Schema - All Bronze are String) | Nguồn Khuyến Nghị | Tần suất |
| :--- | :--- | :--- | :--- |
| `raw_stock_price_eod` | `ticker`, `trading_date`, `open`, `high`, `low`, `close`, `volume`, `value`, `adjusted_close` | `vnstock` (SSI/TCBS API) | Daily (EOD) |
| `raw_index_price_eod` | `index_name`, `trading_date`, `open`, `high`, `low`, `close`, `volume` | `vnstock` | Daily (EOD) |
| `raw_foreign_trading` | `ticker`, `trading_date`, `buy_vol`, `sell_vol`, `buy_val`, `sell_val`, `net_val` | `vnstock` | Daily (EOD) |
| `raw_proprietary_trading` | `ticker`, `trading_date`, `buy_vol`, `sell_vol`, `net_val` | VNDIRECT / SSI (Fallback: Mock) | Daily (EOD) |

## 2. Dữ liệu Cơ bản (Fundamental Data)

Dữ liệu sức khỏe tài chính dùng để tính toán giá trị nội tại.

| Tên Bảng | Thuộc tính (Schema - All Bronze are String) | Nguồn Khuyến Nghị | Tần suất |
| :--- | :--- | :--- | :--- |
| `raw_balance_sheet` | `ticker`, `period`, `year`, `total_assets`, `current_assets`, `cash`, `inventory`, `total_liabilities`, `short_term_debt`, `long_term_debt`, `equity` | `vnstock` / CafeF API | Hàng Quý |
| `raw_income_statement` | `ticker`, `period`, `year`, `revenue`, `cogs`, `gross_profit`, `operating_expenses`, `operating_profit`, `financial_income`, `financial_expenses`, `net_profit_after_tax` | `vnstock` / CafeF API | Hàng Quý |
| `raw_cashflow_statement` | `ticker`, `period`, `year`, `cfo`, `cfi`, `cff`, `net_cash_flow`, `capex` | `vnstock` / CafeF API | Hàng Quý |
| `raw_financial_ratios` | `ticker`, `period`, `year`, `shares_outstanding`, `market_cap` | `vnstock` (VCI — nguồn TCBS cũ đã ngừng hỗ trợ) | Hàng Quý |
| `raw_company_profile` | `ticker`, `company_name`, `industry`, `exchange`, `description` | `vnstock` / SSC | 1 lần / Cập nhật khi có đổi |

## 3. Dữ liệu Vĩ mô & Hàng hóa (Macro & Commodities)

Bối cảnh kinh tế tác động đến định giá (Đặc biệt quan trọng với các ngành chu kỳ như Dầu khí, Thép).

| Tên Bảng | Thuộc tính (Schema - All Bronze are String) | Nguồn Khuyến Nghị | Tần suất |
| :--- | :--- | :--- | :--- |
| `raw_macro_indicators` | `indicator_name`, `report_date`, `value`, `unit` | World Bank API (`world_bank_client.py`) | Hàng Tháng / Quý |
| `raw_interest_rates` | `rate_type`, `date`, `rate_value` | Yahoo Finance — US benchmark rates (^IRX: Fed proxy, ^TNX: 10Y Treasury, ^FVX: 5Y Treasury) | Daily |
| `raw_exchange_rates` | `pair`, `date`, `exchange_rate` | Yahoo Finance — pairs: USD/VND, EUR/VND, GBP/VND, JPY/VND, CNY/VND | Daily |
| `raw_commodities_price`| `commodity_name`, `date`, `price` | Yahoo Finance (`yahoo_finance_client.py`) | Daily |

**Lưu ý chi tiết hàng hóa (raw_commodities_price):** Cần thu thập ít nhất các mã: `Brent Crude`, `WTI`, `Gasoline Singapore (92/95)`, `Baltic Dirty Tanker Index`, `Gold`, `Steel HRC`.

## 4. Dữ liệu Phi cấu trúc & Sự kiện (Alternative/Text Data)

Dữ liệu văn bản phục vụ trích xuất cảm xúc thị trường (Sentiment Analysis) bằng NLP.

| Tên Bảng | Thuộc tính (Schema - All Bronze are String) | Nguồn Khuyến Nghị | Tần suất |
| :--- | :--- | :--- | :--- |
| `raw_news_articles` | `article_id`, `ticker`, `publish_time`, `title`, `summary`, `content`, `source`, `url` | RSS Feeds / Web Scraping | Real-time / Daily |
| `raw_corporate_events` | `event_id`, `ticker`, `event_type`, `ex_right_date`, `record_date`, `event_details` | VSD / CafeF | Daily |
| `raw_insider_transactions`| `ticker`, `deal_announce_date`, `deal_method`, `deal_action`, `deal_quantity`, `deal_price`, `deal_ratio` | vnstock / CafeF (Fallback: Mock) | Daily |
| `raw_analyst_reports` | `report_id`, `ticker`, `brokerage_firm`, `publish_date`, `title`, `description`, `file_name` | FireAnt API | Daily |

## 5. Metadata Quản trị Dữ liệu (Raw Layer)

Để phục vụ việc truy xuất nguồn gốc và quản lý phân vùng dữ liệu, tất cả các bảng ở tầng **Bronze (Raw)** bắt buộc phải có các trường metadata sau:

| Tên Trường | Kiểu dữ liệu | Ý nghĩa | Ví dụ |
| :--- | :--- | :--- | :--- |
| `BATCH_DATE` | `DATE` | Ngày chạy batch thu thập dữ liệu. | `2026-06-15` |
| `_CONATA_SOURCE` | `STRING` | Nguồn dữ liệu gốc. | `VNSTOCK`, `CAFEF` |
| `_CONATA_SOURCE_ROW_NUMBER` | `INTEGER` | Số thứ tự bản ghi từ nguồn. | `1` |
| `_CONATA_PARTITION_KEY` | `STRING` | Khóa phân vùng (YYYYMMDD). | `20260615` |
| `_CONATA_LOADED_AT` | `TIMESTAMP` | Thời điểm dữ liệu nạp vào hệ thống. | `2026-06-15 14:30:00` |

iểm dữ liệu nạp vào hệ thống. | `2026-06-15 14:30:00` |
