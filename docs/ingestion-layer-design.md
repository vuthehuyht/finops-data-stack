# Ingestion Layer Design Specification

## 1. Overview
Ingestion Layer (Tầng thu thập dữ liệu) là bước khởi đầu của toàn bộ Data Pipeline. Nhiệm vụ cốt lõi là thu thập dữ liệu thô từ các nguồn khác nhau (APIs chứng khoán, cổng thông tin vĩ mô, tin tức doanh nghiệp...), chuẩn hóa tên cột và tiêm trường metadata hệ thống (`_CONATA_*`), lưu trữ tạm thời dưới dạng file Parquet nén Snappy, và tải lên AWS S3 (Bronze Layer/Raw).

Tài liệu này đặc tả chi tiết kiến trúc hướng đối tượng cho Ingestion Layer, cách tổ chức 17 file logic pipeline độc lập tương ứng với 17 bảng dữ liệu thô, cơ chế an toàn (Retry & Rate Limiting), cách tích hợp vào Dagster và kế hoạch kiểm thử.

---

## 2. Directory Structure
Để đảm bảo tách biệt trách nhiệm rõ ràng, logic Ingestion (Crawl/API -> S3) sẽ nằm hoàn toàn dưới thư mục `src/ingest/`. Thư mục `src/load/` cũ sẽ chỉ đảm nhiệm việc tải dữ liệu từ S3 vào Redshift.

```text
src/ingest/
├── __init__.py
├── client/
│   ├── __init__.py
│   ├── base_client.py          # Lớp BaseClient xử lý Retry & Rate Limiting
│   ├── vnstock_client.py       # API client bọc thư viện vnstock3
│   └── macro_client.py         # API/Crawler vĩ mô, tỷ giá, lãi suất, hàng hóa
│
└── pipeline/
    ├── __init__.py
    ├── base.py                 # Lớp BaseIngestPipeline (Abstract Base Class)
    │
    # --- MARKET DATA (Tần suất chạy: Hàng ngày - Daily) ---
    ├── stock_price_eod.py      # Thu thập giá đóng cửa cổ phiếu (RAW_STOCK_PRICE_EOD)
    ├── index_price_eod.py      # Thu thập giá chỉ số thị trường (RAW_INDEX_PRICE_EOD)
    ├── foreign_trading.py      # Thu thập giao dịch khối ngoại (RAW_FOREIGN_TRADING)
    ├── proprietary_trading.py  # Thu thập giao dịch tự doanh (RAW_PROPRIETARY_TRADING)
    │
    # --- COMPANY FUNDAMENTALS (Tần suất chạy: Hàng quý/Hàng tháng) ---
    ├── balance_sheet.py        # Thu thập Bảng cân đối kế toán (RAW_BALANCE_SHEET)
    ├── income_statement.py     # Thu thập Báo cáo kết quả KD (RAW_INCOME_STATEMENT)
    ├── cashflow_statement.py   # Thu thập Báo cáo lưu chuyển tiền tệ (RAW_CASHFLOW_STATEMENT)
    ├── financial_ratios.py     # Thu thập Các chỉ số tài chính (RAW_FINANCIAL_RATIOS)
    ├── company_profile.py      # Thu thập Thông tin hồ sơ DN (RAW_COMPANY_PROFILE)
    │
    # --- MACRO & COMMODITIES (Tần suất chạy: Linh hoạt theo ngày/tháng) ---
    ├── macro_indicators.py     # Thu thập các chỉ số vĩ mô GDP, CPI (RAW_MACRO_INDICATORS)
    ├── interest_rates.py       # Thu thập dữ liệu lãi suất (RAW_INTEREST_RATES)
    ├── exchange_rates.py       # Thu thập dữ liệu tỷ giá (RAW_EXCHANGE_RATES)
    ├── commodities_price.py    # Thu thập dữ liệu giá hàng hóa/dầu (RAW_COMMODITIES_PRICE)
    │
    # --- QUALITATIVE DATA (Tần suất chạy: Hàng ngày/Real-time) ---
    ├── news_articles.py        # Thu thập tin tức doanh nghiệp (RAW_NEWS_ARTICLES)
    ├── corporate_events.py     # Thu thập lịch sự kiện doanh nghiệp (RAW_CORPORATE_EVENTS)
    ├── insider_transactions.py # Thu thập giao dịch cổ đông nội bộ (RAW_INSIDER_TRANSACTIONS)
    └── analyst_reports.py      # Thu thập báo cáo phân tích doanh nghiệp (RAW_ANALYST_REPORTS)
```

---

## 3. Shared Components Design

### A. Base Client (`src/ingest/client/base_client.py`)
Mọi Client kết nối nguồn dữ liệu bên ngoài phải kế thừa từ `BaseClient` để thừa hưởng các cơ chế an toàn:
*   **Rate Limiting**: Giãn cách có cấu hình giữa các request thông qua `request_delay_seconds`.
*   **Exponential Backoff Retry**: Sử dụng thư viện `tenacity` để tự động thử lại tối đa 3 lần khi gặp lỗi kết nối hoặc lỗi dịch vụ từ phía API nguồn.

```python
import time
import logging
from typing import Any, Callable
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

class BaseClient:
    def __init__(self, request_delay_seconds: float = 1.0):
        self.request_delay_seconds = request_delay_seconds
        self._last_request_time = 0.0

    def _apply_rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < self.request_delay_seconds:
            sleep_time = self.request_delay_seconds - elapsed
            logger.debug("Rate limiting: Giãn cách %.2f giây...", sleep_time)
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def call_api_with_retry(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        self._apply_rate_limit()
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning("Lỗi khi gọi API: %s. Đang thử lại...", e)
            raise e
```

### B. Base Ingest Pipeline (`src/ingest/pipeline/base.py`)
Lớp trừu tượng định nghĩa khung sườn (Template Method) cho vòng đời Ingestion. Các tham số như `batch_date` và `symbols` được truyền tĩnh qua constructor khi Dagster khởi tạo.

```python
import abc
import os
import time
import logging
import tempfile
import pandas as pd
from mypy_boto3_s3.client import S3Client
from src.common.s3_util import upload_to_s3

class BaseIngestPipeline(abc.ABC):
    def __init__(
        self,
        batch_date: str,
        symbols: list[str] = None,
        s3_client: S3Client = None,
        bucket_name: str = "finops-raw-dev"
    ):
        self.batch_date = batch_date
        self.symbols = symbols or []
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    @abc.abstractmethod
    def table_name(self) -> str:
        """Tên bảng thô đích ở Redshift (ví dụ: RAW_STOCK_PRICE_EOD)."""
        pass

    @property
    @abc.abstractmethod
    def source_uri_prefix(self) -> str:
        """Định danh URI nguồn dữ liệu (ví dụ: api://vnstock/stock_price)."""
        pass

    @abc.abstractmethod
    def fetch(self) -> pd.DataFrame:
        """Logic lấy dữ liệu thô từ Client (trả về DataFrame)."""
        pass

    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuẩn hóa cột thành chữ hoa và tiêm các trường metadata _CONATA_*."""
        self.logger.info("Bắt đầu chuẩn hóa DataFrame...")
        # 1. Chuyển toàn bộ tên cột thành chữ hoa
        df.columns = df.columns.str.upper()

        # 2. Tiêm các trường metadata quản trị bắt buộc
        df["_CONATA_SOURCE"] = self.source_uri_prefix
        df["_CONATA_SOURCE_ROW_NUMBER"] = range(1, len(df) + 1)
        df["_CONATA_PARTITION_KEY"] = self.batch_date
        df["_CONATA_LOADED_AT"] = pd.Timestamp.utcnow()

        return df

    def serialize(self, df: pd.DataFrame) -> str:
        """Ghi DataFrame thành file Parquet tạm thời ở local, tự động dọn dẹp sau."""
        # Sử dụng delete=False để tránh file bị đóng hoặc xóa trước khi upload xong
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
            temp_path = temp_file.name

        self.logger.info("Ghi dữ liệu tạm thời ra file: %s", temp_path)
        df.to_parquet(temp_path, compression="snappy", index=False)
        return temp_path

    def upload(self, temp_file_path: str) -> str:
        """Tải file tạm lên S3 theo cấu trúc phân vùng quy định."""
        unix_timestamp = int(time.time())
        # Cấu trúc phân vùng: raw/<table_name>/batch_date=<date>/<timestamp>/<table_name>.parquet
        s3_key = f"raw/{self.table_name}/batch_date={self.batch_date}/{unix_timestamp}/{self.table_name}.parquet"
        s3_url = f"s3://{self.bucket_name}/{s3_key}"

        self.logger.info("Đang tải file lên S3 tại: %s", s3_url)
        upload_to_s3(
            file_path=temp_file_path,
            output_s3_url=s3_url,
            s3_client=self.s3_client,
            logger=self.logger
        )
        return s3_url

    def run(self) -> str:
        """Điều phối toàn bộ vòng đời Ingest."""
        self.logger.info("Bắt đầu thực thi pipeline: %s", self.table_name)
        temp_path = None
        try:
            df = self.fetch()
            if df.empty:
                self.logger.warning("Không tìm thấy dữ liệu thô. Bỏ qua các bước tiếp theo.")
                return ""

            df = self.standardize(df)
            temp_path = self.serialize(df)
            s3_url = self.upload(temp_path)
            return s3_url
        finally:
            # Luôn dọn dẹp file tạm local trong mọi tình huống
            if temp_path and os.path.exists(temp_path):
                self.logger.debug("Dọn dẹp file tạm local: %s", temp_path)
                os.unlink(temp_path)
```

---

## 4. Data Flow Lifecycle & Diagrams

### Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    participant D as Dagster INPUT Asset
    participant P as IngestPipeline (Con)
    participant B as BaseIngestPipeline (Cha)
    participant C as API Client (BaseClient)
    participant API as Third-Party API
    participant S3 as AWS S3 (Bronze)

    D->>P: Khởi tạo bằng (batch_date, symbols, s3_client)
    D->>B: Thực thi .run()
    B->>P: Gọi .fetch()
    P->>C: Gọi client lấy dữ liệu
    C->>API: Gửi Request (giãn cách & retry)
    API-->>C: Trả về dữ liệu thô (JSON/CSV)
    C-->>P: Trả về DataFrame thô
    P-->>B: Trả về DataFrame thô
    B->>B: Gọi .standardize() (chuyển UPPERCASE, tiêm metadata _CONATA_*)
    B->>B: Gọi .serialize() (ghi file tạm Parquet nén Snappy)
    B->>S3: Gọi .upload() (tải lên S3 Bronze)
    S3-->>B: Xác nhận upload hoàn tất
    B->>B: Xóa file tạm ở local (dọn dẹp đĩa)
    B-->>D: Trả về s3_url của file Parquet
```

---

## 5. Dagster Integration & Downstream Trigger
Mỗi file pipeline con trong `pipeline/` sẽ được gọi bởi một Dagster Asset tương ứng. 

1.  **INPUT Asset**: Dagster Asset thuộc nhóm `INPUT` khởi tạo class pipeline tương ứng bằng cách truyền tham số cấu hình (tĩnh) qua constructor, gọi phương thức `.run()` và nhận về `s3_url` để đưa vào metadata của Asset Output.
2.  **Redshift Load Sensor (`load_job_sensor`)**: Cảm biến này theo dõi các asset nhóm `INPUT` được materialized, trích xuất `s3_url` và `batch_date` từ metadata, sau đó kích hoạt Job nạp dữ liệu tương ứng trong `src/dagster/load_job.py` để thực thi lệnh `COPY` vào Redshift Bronze tables.

---

## 6. Error Handling & Security
*   **Không Catch-and-Ignore Exception**: Mọi ngoại lệ trong quá trình lấy dữ liệu, ghi file, và tải lên S3 đều phải để văng tự do (fail loudly) để hệ thống điều phối (Dagster) ghi nhận lỗi và gửi thông báo cảnh báo kịp thời.
*   **Bảo mật Thông tin nhạy cảm**: Tuyệt đối không lưu cứng (hardcode) các API keys, AWS credentials trong code. Các API clients phải truy xuất các giá trị này qua biến môi trường hoặc AWS Secrets Manager đã được Dagster resource bọc sẵn.

---

## 7. Testing & Verification Plan

### Unit Tests (`tests/ingest/`)
*   `test_base_client.py`: Mock các response HTTP lỗi để xác thực cơ chế Retry của `BaseClient` hoạt động đúng 3 lần trước khi ném lỗi. Kiểm tra cơ chế Rate Limiting đảm bảo khoảng cách giãn cách tối thiểu.
*   `test_base_pipeline.py`: Xây dựng một class pipeline kiểm thử kế thừa `BaseIngestPipeline` để kiểm tra logic chuẩn hóa cột thành chữ hoa và việc tiêm đúng 4 trường metadata hệ thống.
*   `test_s3_upload.py`: Mock `boto3.client("s3")` để kiểm tra tính chính xác của hàm `upload()` trong việc sinh đường dẫn phân vùng S3.

### Lệnh thực thi
*   **Chạy Unit Tests**:
    ```bash
    uv run pytest tests/ingest/
    ```
*   **Linter & Code Format**:
    ```bash
    uv run ruff check src/ingest/
    uv run ruff format src/ingest/
    ```
