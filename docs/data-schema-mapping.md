# Data Schema & Source Mapping

Tài liệu này định nghĩa chi tiết danh mục các nguồn dữ liệu, cấu trúc các bảng raw data (Bronze Layer trên S3/Redshift) phục vụ cho hệ thống FinOps Data Stack.

**Lưu ý quan trọng về Data Type:** Tại tầng **Bronze (Raw)**:

1. **Dữ liệu nghiệp vụ:** Tất cả các thuộc tính thu thập từ nguồn sẽ được lưu trữ dưới dạng **String** (hoặc Varchar). Việc ép kiểu sẽ thực hiện ở tầng Silver.
1. **Metadata quản trị:** Các trường hệ thống (Metadata) sẽ có **Kiểu dữ liệu chính xác** (Date, Timestamp, Integer) để phục vụ việc phân vùng (Partitioning) và quản lý vận hành.

## 1. Dữ liệu Thị trường (Market Data)

Dữ liệu giao dịch hàng ngày phản ánh cung cầu của thị trường.

| Tên Bảng | Thuộc tính (Schema - All Bronze are String) | Nguồn Khuyến Nghị | Tần suất |
| :--- | :--- | :--- | :--- |
| `RAW_STOCK_PRICE_EOD` | `TICKER`, `TRADING_DATE`, `OPEN`, `HIGH`, `LOW`, `CLOSE`, `VOLUME`, `VALUE`, `ADJUSTED_CLOSE` | `vnstock` (SSI/TCBS API) | Daily (EOD) |
| `RAW_INDEX_PRICE_EOD` | `INDEX_NAME`, `TRADING_DATE`, `OPEN`, `HIGH`, `LOW`, `CLOSE`, `VOLUME` | `vnstock` | Daily (EOD) |
| `RAW_FOREIGN_TRADING` | `TICKER`, `TRADING_DATE`, `BUY_VOL`, `SELL_VOL`, `BUY_VAL`, `SELL_VAL`, `NET_VAL` | `vnstock` | Daily (EOD) |
| `RAW_PROPRIETARY_TRADING` | `TICKER`, `TRADING_DATE`, `BUY_VOL`, `SELL_VOL`, `NET_VAL` | VNDIRECT / SSI (Fallback: Mock) | Daily (EOD) |

## 2. Dữ liệu Cơ bản (Fundamental Data)

Dữ liệu sức khỏe tài chính dùng để tính toán giá trị nội tại.

| Tên Bảng | Thuộc tính (Schema - All Bronze are String) | Nguồn Khuyến Nghị | Tần suất |
| :--- | :--- | :--- | :--- |
| `RAW_BALANCE_SHEET` | `TICKER`, `PERIOD`, `YEAR`, `TOTAL_ASSETS`, `CURRENT_ASSETS`, `CASH`, `INVENTORY`, `TOTAL_LIABILITIES`, `SHORT_TERM_DEBT`, `LONG_TERM_DEBT`, `EQUITY` | `vnstock` / CafeF API | Hàng Quý |
| `RAW_INCOME_STATEMENT` | `TICKER`, `PERIOD`, `YEAR`, `REVENUE`, `COGS`, `GROSS_PROFIT`, `OPERATING_EXPENSES`, `OPERATING_PROFIT`, `FINANCIAL_INCOME`, `FINANCIAL_EXPENSES`, `NET_PROFIT_AFTER_TAX` | `vnstock` / CafeF API | Hàng Quý |
| `RAW_CASHFLOW_STATEMENT` | `TICKER`, `PERIOD`, `YEAR`, `CFO`, `CFI`, `CFF`, `NET_CASH_FLOW`, `CAPEX` | `vnstock` / CafeF API | Hàng Quý |
| `RAW_COMPANY_PROFILE` | `TICKER`, `COMPANY_NAME`, `INDUSTRY`, `EXCHANGE`, `OUTSTANDING_SHARE`, `DESCRIPTION` | `vnstock` / SSC | 1 lần / Cập nhật khi có đổi |

## 3. Dữ liệu Vĩ mô & Hàng hóa (Macro & Commodities)

Bối cảnh kinh tế tác động đến định giá (Đặc biệt quan trọng với các ngành chu kỳ như Dầu khí, Thép).

| Tên Bảng | Thuộc tính (Schema - All Bronze are String) | Nguồn Khuyến Nghị | Tần suất |
| :--- | :--- | :--- | :--- |
| `RAW_MACRO_INDICATORS` | `INDICATOR_NAME`, `REPORT_DATE`, `VALUE`, `UNIT` | World Bank API (`world_bank_client.py`) | Hàng Tháng / Quý |
| `RAW_INTEREST_RATES` | `RATE_TYPE`, `DATE`, `RATE_VALUE` | Yahoo Finance — US benchmark rates (^IRX: Fed proxy, ^TNX: 10Y Treasury, ^FVX: 5Y Treasury) | Daily |
| `RAW_EXCHANGE_RATES` | `PAIR`, `DATE`, `EXCHANGE_RATE` | Yahoo Finance — pairs: USD/VND, EUR/VND, GBP/VND, JPY/VND, CNY/VND | Daily |
| `RAW_COMMODITIES_PRICE` | `COMMODITY_NAME`, `DATE`, `PRICE` | Yahoo Finance (`yahoo_finance_client.py`) | Daily |

**Lưu ý chi tiết hàng hóa (RAW_COMMODITIES_PRICE):** Cần thu thập ít nhất các mã: `Brent Crude`, `WTI`, `Gasoline Singapore (92/95)`, `Baltic Dirty Tanker Index`, `Gold`, `Steel HRC`.

## 4. Dữ liệu Phi cấu trúc & Sự kiện (Alternative/Text Data)

Dữ liệu văn bản phục vụ trích xuất cảm xúc thị trường (Sentiment Analysis) bằng NLP.

| Tên Bảng | Thuộc tính (Schema - All Bronze are String) | Nguồn Khuyến Nghị | Tần suất |
| :--- | :--- | :--- | :--- |
| `RAW_NEWS_ARTICLES` | `ARTICLE_ID`, `TICKER`, `PUBLISH_TIME`, `TITLE`, `SUMMARY`, `CONTENT`, `SOURCE`, `URL` | RSS Feeds / Web Scraping | Real-time / Daily |
| `RAW_CORPORATE_EVENTS` | `EVENT_ID`, `TICKER`, `EVENT_TYPE`, `EX_RIGHT_DATE`, `RECORD_DATE`, `EVENT_DETAILS` | VSD / CafeF | Daily |
| `RAW_ANALYST_REPORTS` | `REPORT_ID`, `TICKER`, `BROKERAGE_FIRM`, `PUBLISH_DATE`, `TITLE`, `DESCRIPTION`, `FILE_NAME` | FireAnt API | Daily |

## 5. Metadata Quản trị Dữ liệu (Raw Layer)

Để phục vụ việc truy xuất nguồn gốc và quản lý phân vùng dữ liệu, tất cả các bảng ở tầng **Bronze (Raw)** bắt buộc phải có các trường metadata sau:

| Tên Trường | Kiểu dữ liệu | Ý nghĩa | Ví dụ |
| :--- | :--- | :--- | :--- |
| `BATCH_DATE` | `DATE` | Ngày chạy batch thu thập dữ liệu. | `2026-06-15` |
| `_CONATA_SOURCE` | `STRING` | Nguồn dữ liệu gốc. | `VNSTOCK`, `CAFEF` |
| `_CONATA_SOURCE_ROW_NUMBER` | `INTEGER` | Số thứ tự bản ghi từ nguồn. | `1` |
| `_CONATA_PARTITION_KEY` | `STRING` | Khóa phân vùng (YYYYMMDD). | `20260615` |
| `_CONATA_LOADED_AT` | `TIMESTAMP` | Thời điểm dữ liệu nạp vào hệ thống. | `2026-06-15 14:30:00` |
