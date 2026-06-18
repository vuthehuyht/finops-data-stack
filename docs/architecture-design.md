# FinOps Data Stack: System Architecture & Implementation Plan

## 1. Background & Motivation
Dự án nhằm xây dựng một hệ thống Data Pipeline tự động hóa thu thập, xử lý và phân tích dữ liệu chứng khoán Việt Nam. Mục tiêu cốt lõi là phục vụ việc định giá cổ phiếu (Valuation) phục vụ đầu tư dài hạn bằng cách kết hợp phân tích cơ bản (BCTC, Vĩ mô, Chỉ số) và phân tích định tính (Tin tức) thông qua các mô hình Deep Learning và NLP.

## 2. Scope & Impact
*   **Thị trường:** Chứng khoán Việt Nam (VN-Index, HNX, UPCOM).
*   **Dữ liệu đầu vào:** BCTC, Ratios, Dữ liệu Vĩ mô (đặc biệt giá dầu), Tin tức doanh nghiệp, Dữ liệu giá (EOD).
*   **Tần suất cập nhật:** Daily (Hàng ngày) sau phiên giao dịch.
*   **Mô hình dự đoán:** Sử dụng Deep Learning (LSTM/Transformers) và NLP (PhoBERT/Vietnamese LLMs) để dự phóng giá trị.

## 3. Proposed Architecture (Enterprise Data Stack on AWS)
Dựa trên yêu cầu và lựa chọn công nghệ, hệ thống sẽ được xây dựng theo kiến trúc Data Mesh / Lakehouse trên hạ tầng AWS:

*   **Orchestration:** **Dagster** (Quản lý data assets, job scheduling, dependency tracking).
*   **Compute / Deployment:** **Amazon EKS (Elastic Kubernetes Service)** (Hosting Dagster, các script ingestion, model serving).
*   **Data Lake (Bronze Layer):** **Amazon S3** (Lưu trữ raw data: file parquet cho dữ liệu bảng, json/txt cho tin tức).
*   **Data Warehouse (Silver/Gold Layer):** **Amazon Redshift** (Lưu trữ dữ liệu đã qua xử lý, query performance cao).
*   **Transformation:** **dbt (data build tool)** (Thực hiện in-warehouse transformations từ raw/stage lên mart tables trong Redshift).
*   **Machine Learning:** **Amazon SageMaker** (Training trên GPU instances và Deployment sử dụng **SageMaker Serverless Inference** để tối ưu chi phí). Quản lý phiên bản mô hình bằng **SageMaker Model Registry**.
*   **Security:** AWS IAM, AWS KMS (Mã hóa dữ liệu), AWS Secrets Manager (Quản lý API keys, DB credentials), VPC.
*   **CI/CD:** **GitHub Actions** (CI pipeline để test code/dbt). Triển khai trực tiếp lên EKS sử dụng GitHub Actions workflow.

## 4. Chi tiết luồng dữ liệu (Data Flow Orchestration)

Toàn bộ hệ thống được điều phối bởi Dagster theo mô hình **Software-Defined Assets**, chia làm 2 luồng chính:

### 4.1. Luồng dự báo hàng ngày (Daily Inference Pipeline)
Luồng này chạy tự động sau khi thị trường chứng khoán đóng cửa (EOD).

1.  **Ingestion (EKS -> S3):** Dagster chạy các task Python để thu thập dữ liệu thô. Dữ liệu được đính kèm metadata hệ thống (`_CONATA_*`) và lưu vào S3 dưới dạng Parquet.
2.  **Sensor & Loading (S3 -> Redshift Bronze):** Sensor phát hiện file mới và trigger lệnh `COPY` để nạp vào Redshift Raw Tables.
3.  **Transformation (dbt Silver & Gold):** 
    *   Sensor trigger dbt chạy các model Cleaned (ép kiểu, gán metadata `DATACORE_*`).
    *   Chạy tiếp các dbt model Feature Engineering tại tầng Gold (Mart).
4.  **Data Quality Gate:** Kiểm tra chất lượng bảng `fact_ml_feature_set`. Nếu đạt chuẩn, luồng sẽ tiếp tục.
5.  **ML Inference (SageMaker Serverless):** Dagster gọi **SageMaker Serverless Endpoint**. Endpoint này tự động scale-to-zero khi không có yêu cầu, giúp tiết kiệm chi phí tối đa.
6.  **Results Publishing:** Kết quả dự báo được ghi ngược lại vào bảng Redshift Gold và đẩy lên Dashboard.

### 4.2. Luồng tái huấn luyện định kỳ (Quarterly Re-training Pipeline)
Luồng này chạy định kỳ hàng Quý (sau khi các doanh nghiệp công bố đầy đủ BCTC mới).

1.  **Historical Data Preparation:** Thu thập và tổng hợp dữ liệu Feature Set của 12-24 tháng gần nhất từ Redshift.
2.  **Training Job (SageMaker):** Khởi chạy SageMaker Training Job (GPU). Sau khi hoàn tất, model artifact (`model.tar.gz`) được đẩy lên **Amazon S3**.
3.  **Model Registration:** Đăng ký phiên bản mô hình mới vào **SageMaker Model Registry** kèm theo các metrics đánh giá.
4.  **Model Evaluation & Approval:** So sánh mô hình mới (Challenger) vs mô hình hiện tại (Champion). Nếu đạt yêu cầu, quản trị viên (hoặc auto-approve script) phê duyệt model trong Registry.
5.  **Serverless Deployment:** Cập nhật SageMaker Serverless Endpoint để sử dụng version mới nhất vừa được phê duyệt.

## 5. Phased Implementation Plan

### Phase 1: Infrastructure & CI/CD Setup
*   Thiết lập AWS VPC, EKS Cluster, S3 Buckets, và Redshift Cluster.
*   Thiết lập GitHub repo, cấu hình GitHub Actions với AWS Credentials (OIDC).
*   Cấu hình luồng tự động deploy Dagster lên EKS khi có commit mới vào nhánh `main`.

### Phase 2: Core Pipeline & dbt Development
*   Phát triển các Dagster Assets thu thập dữ liệu nghiệp vụ và vĩ mô.
*   Phát triển dbt models cho toàn bộ 3 tầng dữ liệu (Raw, Cleaned, Mart).
*   Thiết lập Metadata Governance (`_CONATA` và `DATACORE`).

### Phase 3: ML Integration & Automation
*   Xây dựng NLP pipeline bằng PhoBERT trên SageMaker.
*   Xây dựng mô hình Deep Learning Multimodal và tích hợp vào Dagster.
*   Thiết lập luồng Re-training hàng quý và luồng Inference hàng ngày.

## 7. Cấu trúc thư mục dự án (Flywheel Architecture)

Dự án tuân thủ nghiêm ngặt cấu trúc **Flywheel** để đảm bảo tính module hóa và khả năng mở rộng, tương đương với chuẩn hệ thống Data Core, nhưng sử dụng tên thư mục `src` theo chuẩn Python:

```text
finops-data-stack/
├── docs/                      # Tài liệu nghiệp vụ và kỹ thuật
├── src/                       # Trái tim xử lý dữ liệu của hệ thống (Flywheel Core)
│   ├── common/                # Tiện ích dùng chung (S3, Redshift, Logging utils)
│   ├── dagster/               # Logic điều phối (Jobs, Sensors, Assets, Resources)
│   ├── ingest/                # Ingestion layer (Crawl/API code đẩy vào S3)
│   ├── load/                  # Load layer (Logic nạp từ S3 vào Redshift)
│   ├── transform/             # Transformation layer (dbt project)
│   │   └── dbt/               # Mã nguồn dbt (models, tests, macros)
│   ├── pipeline/              # Định nghĩa các luồng xử lý end-to-end
│   ├── ml/                    # Machine Learning layer (Train/Inference code)
│   ├── docker/                # Dockerfiles cho từng thành phần
│   └── k8s/                   # Kubernetes manifests (ArgoCD)
├── infrastructure/            # Hạ tầng đám mây (Terraform/CloudFormation)
├── tools/                     # Các công cụ hỗ trợ dev và CI/CD
├── pyproject.toml             # Quản lý dependency bằng uv
└── README.md
```