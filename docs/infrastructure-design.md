# AWS Infrastructure Design

Tài liệu này mô tả thiết kế hạ tầng điện toán đám mây trên AWS cho hệ thống FinOps Data Stack, tập trung vào tính bảo mật, khả năng mở rộng và tối ưu hóa chi phí.

## 1. Mạng & Kết nối (Networking)
Sử dụng mô hình VPC tiêu chuẩn để cô lập tài nguyên:
*   **VPC:** 10.0.0.0/16.
*   **Subnets:**
    *   2 Public Subnets (cho NAT Gateway, Load Balancer).
    *   2 Private Subnets (cho EKS Worker Nodes, Redshift Workgroup).
*   **NAT Gateway:** 1 cái (để các Pod trong Private Subnet có thể gọi API ra ngoài như vnstock/investing).
*   **Security Groups:** Thiết lập các quy tắc nghiêm ngặt, chỉ cho phép traffic từ EKS gọi tới Redshift qua port 5439.

## 2. Tính toán & Điều phối (Compute)
*   **Amazon EKS (Elastic Kubernetes Service):**
    *   **Control Plane:** Managed bởi AWS.
    *   **Worker Nodes:** Sử dụng **Managed Node Groups** với **Spot Instances** (loại instance dự kiến: `t3.medium` hoặc `m5.large`) để tiết kiệm 70% chi phí.
    *   **Auto-scaling:** Cấu hình Cluster Autoscaler để tự động tăng/giảm số lượng node tùy theo số lượng Job của Dagster.
*   **Amazon ECR (Elastic Container Registry):** Lưu trữ Docker images của Dagster, dbt và các script thu thập dữ liệu.

## 3. Lưu trữ & Warehouse (Storage & DWH)
*   **Amazon S3 (Data Lake):**
    *   `finops-data-lake-raw`: Chứa dữ liệu Bronze (Parquet).
    *   `finops-data-lake-processed`: Chứa log hoặc các kết quả trung gian.
    *   `finops-model-artifacts`: Chứa các file `.tar.gz` của model sau khi train.
*   **Amazon Redshift Serverless:**
    *   **Namespace:** Quản lý database, schemas, users.
    *   **Workgroup:** Quản lý tài nguyên tính toán (RPU). Tự động tắt khi không có query để tiết kiệm chi phí.
    *   **Redshift Spectrum:** Cho phép dbt query trực tiếp dữ liệu từ S3 mà không cần load vào ổ cứng Redshift.

## 4. Machine Learning (SageMaker)
*   **SageMaker Training:** Chạy theo nhu cầu (On-demand) trên các instance có GPU với **kích thước nhỏ nhất** (ví dụ: `g4dn.xlarge`) để tối ưu chi phí.
*   **SageMaker Serverless Inference:** Endpoint phục vụ dự báo hàng ngày. Tự động scale về 0 khi Dagster hoàn thành job.
*   **Model Versioning (S3-based):** Thay vì dùng Model Registry phức tạp, hệ thống sử dụng cấu trúc thư mục trên **Amazon S3** (`finops-model-artifacts/v1/`, `finops-model-artifacts/v2/`) để quản lý các phiên bản mô hình kèm metadata.

## 5. Bảo mật & Quản trị (Security & Governance)
*   **IAM Roles for Service Accounts (IRSA):** Gán quyền IAM trực tiếp cho từng Pod trong Kubernetes thay vì dùng quyền của Node. Đảm bảo nguyên tắc đặc quyền tối thiểu (Least Privilege).
*   **AWS Secrets Manager:** Lưu trữ API keys (SSI, Investing), DB Credentials.
*   **AWS KMS (Key Management Service):** Mã hóa dữ liệu tại chỗ trên S3 và Redshift.
*   **CloudWatch Logs:** Thu thập và giám sát toàn bộ log từ EKS và SageMaker.

## 6. Luồng CI/CD (Infrastructure as Code)
*   Toàn bộ hạ tầng này sẽ được định nghĩa bằng **Terraform** hoặc **AWS CDK**.
*   Khi có thay đổi trong thư mục `infrastructure/terraform/`, GitHub Actions sẽ tự động thực hiện `terraform plan/apply`.

---
**Gợi ý về chi phí:** Bằng cách sử dụng **Spot Instances** cho EKS và **Redshift Serverless**, chi phí vận hành hàng tháng của bạn sẽ được giảm xuống mức tối thiểu (chỉ tốn tiền khi thực sự xử lý dữ liệu).
