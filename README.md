# FinOps Data Stack (Vietnamese Stock Market)

Hệ thống Data Pipeline và AI tự động hóa việc thu thập, xử lý dữ liệu và dự báo giá trị cổ phiếu thị trường chứng khoán Việt Nam dựa trên phân tích cơ bản (Fundamental Analysis) và cảm xúc thị trường (Sentiment Analysis).

## 🚀 Mục tiêu dự án
*   **Tự động hóa**: Xây dựng luồng dữ liệu end-to-end từ thu thập (Crawl/API) đến lưu trữ và biến đổi.
*   **Định giá thông minh**: Sử dụng Deep Learning đa phương thức (Multimodal) để dự báo tỷ suất sinh lời và xu hướng cổ phiếu.
*   **Quản trị dữ liệu**: Áp dụng chuẩn Metadata và Data Quality nghiêm ngặt cho toàn bộ các tầng Raw, Cleaned, và Mart.
*   **Tối ưu chi phí**: Vận hành trên hạ tầng AWS Serverless (Redshift Serverless, SageMaker Serverless).

## 🏗 Kiến trúc hệ thống (Architecture)
Hệ thống tuân thủ mô hình **Flywheel Architecture** với các thành phần chính:
*   **Orchestration**: Dagster (Điều phối dựa trên Software-Defined Assets).
*   **Data Lake**: Amazon S3 (Bronze/Silver/Gold).
*   **Data Warehouse**: Amazon Redshift Serverless.
*   **Transformation**: dbt (data build tool).
*   **Machine Learning**: Amazon SageMaker (Training On-demand, Inference Serverless).
*   **CI/CD**: GitHub Actions tích hợp triển khai lên Amazon EKS.

## 📁 Cấu trúc thư mục (Folder Structure)
```text
finops-data-stack/
├── docs/                      # Tài liệu thiết kế chi tiết
├── src/                       # Flywheel Core (Mã nguồn xử lý dữ liệu)
│   ├── common/                # AWS Utils, DB Utils, Logging
│   ├── dagster/               # Logic điều phối (Assets, Jobs, Sensors)
│   ├── ingest/                # Ingestion layer (Crawl/API -> S3)
│   ├── load/                  # Load layer (Logic nạp từ S3 vào Redshift)
│   ├── transform/             # dbt Project (Transformation)
│   ├── pipeline/              # Luồng xử lý end-to-end
│   ├── ml/                    # Machine Learning (Train/Inference)
│   ├── docker/                # Dockerfiles
│   └── k8s/                   # Kubernetes Manifests (Helm/YAML)
├── infrastructure/            # IaC (Terraform/CloudFormation)
├── tools/                     # Scripts hỗ trợ phát triển
└── pyproject.toml             # Quản lý dependency bằng uv
```

## 📊 Danh mục dữ liệu (Data Inventory)
Hệ thống thu thập 4 nhóm dữ liệu chiến lược:
1.  **Thị trường**: Giá EOD, Giao dịch khối ngoại, Tự doanh.
2.  **Cơ bản**: BCTC (Bảng CĐKT, KQKD, Lưu chuyển tiền tệ), Chỉ số tài chính gốc.
3.  **Vĩ mô & Hàng hóa**: GDP, CPI, Tỷ giá, Lãi suất, Giá dầu Brent, Crack spread.
4.  **Định tính**: Tin tức doanh nghiệp, Nghị quyết ĐHCĐ, Giao dịch nội bộ.

## 🤖 Mô hình AI/ML
Sử dụng kiến trúc **Multimodal Hybrid Neural Network**:
*   **Nhánh Time-Series (LSTM/GRU)**: Xử lý chuỗi giá và Sentiment score 30 ngày.
*   **Nhánh Tabular (MLP)**: Xử lý các chỉ số tài chính cơ bản và vĩ mô.
*   **Output**: Dự báo tỷ suất sinh lời kỳ vọng (Regression) hoặc Phân loại xu hướng (Classification).

## 🛠 Hướng dẫn phát triển (Development)

Hệ thống sử dụng **uv** làm package manager và cung cấp **Makefile** giúp quản lý môi trường phát triển local (`dev_local`) dễ dàng:

### 1. Khởi tạo môi trường (Setup)
Cài đặt các thư viện cần thiết và khởi tạo các tệp cấu hình mẫu (`.env`, `terraform.tfvars`):
```bash
make setup
```

### 2. Khởi tạo tài nguyên AWS cho local (Terraform)
Khởi tạo hoặc hủy bỏ các tài nguyên AWS phục vụ cho việc phát triển ở local (VPC, S3, Redshift Serverless...):
```bash
# Khởi tạo tài nguyên (Hãy cấu hình allowed_ips trực tiếp trong terraform.tfvars)
make dev_local up

# Hủy bỏ/Xóa tài nguyên khi không dùng nữa
make dev_local down
```

### 3. Vận hành và kiểm thử
```bash
# Khởi chạy Dagster dev server local (mặc định tại localhost:3000)
make dev

# Kiểm tra định dạng và lỗi code tĩnh bằng ruff
make lint

# Tự động sửa lỗi định dạng và các lỗi lint cơ bản bằng ruff
make format

# Chạy các unit test bằng pytest
make test

# Dọn dẹp cache và file rác sinh ra trong quá trình dev
make clean
```

## 📄 Tài liệu liên quan
*   [Thiết kế Kiến trúc tổng thể](docs/architecture-design.md)
*   [Data Schema & Source Mapping](docs/data-schema-mapping.md)
*   [Transformation & Feature Engineering](docs/data-transform-features.md)
*   [Thiết kế Mô hình ML](docs/ml-architecture-design.md)
*   [Thiết kế Hạ tầng AWS](docs/infrastructure-design.md)
