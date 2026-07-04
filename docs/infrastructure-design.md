# AWS Infrastructure Design

Tài liệu này mô tả thiết kế hạ tầng điện toán đám mây trên AWS cho hệ thống FinOps Data Stack, tập trung vào tính bảo mật, khả năng mở rộng, sẵn sàng cao (HA) và tối ưu hóa chi phí tối đa.

---

## 1. Mạng & Kết nối (Networking)

Sử dụng mô hình VPC tiêu chuẩn để cô lập tài nguyên trên 2 Availability Zones (Singapore):

* **VPC**: `10.0.0.0/16`.
* **Subnets**:
  * 2 Public Subnets (cho NAT Gateway, Load Balancer).
  * 2 Private App Subnets (cho EKS Core & Worker Nodes).
  * 2 Private DB Subnets (cho Redshift Serverless Workgroup).
* **NAT Gateway**: 1 cái dùng chung cho cả 2 AZ để tiết kiệm chi phí, đặt tại Public Subnet của AZ đầu tiên. *(Lưu ý: Đối với môi trường Lab/Test ngắn ngày, NAT Gateway được thay thế bằng một NAT Instance chạy trên EC2 Spot `t4g.nano` có cấu hình reboot-persistence và hỗ trợ SSM Session Manager để giảm chi phí mạng từ ~$32/tháng xuống còn ~$1.20/tháng).*
* **S3 Gateway Endpoint**: Cấu hình VPC Endpoint cho S3. Toàn bộ traffic truyền tải dữ liệu data lake lớn giữa EKS/Redshift và S3 sẽ đi nội bộ bên trong AWS, tránh đi qua NAT Gateway để loại bỏ phí truyền dữ liệu và tăng tốc độ.
* **Security Groups**: Thiết lập các quy tắc nghiêm ngặt, chỉ cho phép traffic từ EKS Security Group kết nối tới Redshift qua port `5439`.

## 2. Tính toán & Điều phối (Compute)

* **Amazon EKS (Elastic Kubernetes Service)**:
  * **Control Plane**: Managed bởi AWS (v1.30).
  * **Hybrid Node Groups**: Đặt trong Private App Subnets để đảm bảo cân bằng giữa độ ổn định và chi phí:
    * **Core Node Group (On-Demand)**: Sử dụng `t3.medium` (hoặc `t3a.medium`), dung lượng mong muốn là 1 node để chạy các pod Core ổn định 24/7 (Dagster Webserver, Dagster Daemon, CoreDNS). *(Lưu ý: Trong môi trường Lab, ưu tiên dùng `t3a.medium` để tiết kiệm 10% chi phí).*
    * **Worker Node Group (Spot)**: Sử dụng Spot instances nhỏ như `t3.medium`, `t3a.medium` (hoặc `t3a.small` cho Lab để tiết kiệm chi phí). Cấu hình co giãn từ 0 đến 3/5 nodes để chạy các job xử lý dữ liệu nặng. *(Lưu ý: Trong môi trường Lab, cấu hình desired_size = 0 để thực hiện scale-to-zero khi nhàn rỗi).*
  * **Autoscaling**: Cấu hình Cluster Autoscaler để tự động tăng/giảm số lượng Spot node theo workload thực tế của Dagster.
* **Amazon ECR (Elastic Container Registry)**: Lưu trữ các private container images của Dagster, dbt và crawlers. Tích hợp Lifecycle Policy tự động dọn dẹp các images không tag và chỉ lưu giữ tối đa 10 images gần nhất để tiết kiệm chi phí lưu trữ *(hoặc chỉ 5 images gần nhất trong môi trường Lab)*.

## 3. Lưu trữ & Warehouse (Storage & DWH)

* **Amazon S3 (Data Lake)**:
  * `finops-data-lake-raw`: Chứa dữ liệu Bronze (Parquet/JSON).
  * `finops-data-lake-processed`: Chứa logs và kết quả trung gian.
  * `finops-model-artifacts`: Chứa các model artifacts (`.tar.gz`) phân chia theo phiên bản.
  * **Mã hóa**: Sử dụng mã hóa mặc định **SSE-S3** (Amazon S3 managed keys) hoàn toàn miễn phí (không dùng KMS Customer Managed Key riêng để tối ưu chi phí).
  * **Lifecycle Rules**: Tự động chuyển dữ liệu cũ hơn 90 ngày sang lớp **S3 Glacier Instant Retrieval** để giảm phí lưu trữ.
* **Amazon Redshift Serverless**:
  * **Namespace**: Quản lý database (`finops_db`), schemas, users. Được mã hóa tĩnh bằng AWS managed key (`aws/redshift`).
  * **Workgroup**: Đặt trong Private DB Subnets. Cấu hình giới hạn dung lượng tính toán tối thiểu ở mức **8 RPU** hoặc **16 RPU** và bật tính năng tự động dừng (**Auto-pause**) sau 15-30 phút không hoạt động để tránh phát sinh chi phí.
  * **Redshift Spectrum**: Cho phép truy vấn trực tiếp dữ liệu thô trên S3 thông qua Glue Data Catalog mà không cần load vào ổ đĩa của Redshift.

## 4. Machine Learning (SageMaker)

* **SageMaker Training**: Chạy theo nhu cầu (On-demand) trên các instance có GPU nhỏ nhất (ví dụ: `g4dn.xlarge`) để huấn luyện mô hình.
* **SageMaker Batch Transform (Serverless Batch)**: Khởi chạy các phiên suy luận hàng ngày theo mô hình batch transform (tự động khởi chạy máy chủ, xử lý dữ liệu và tự giải phóng tài nguyên sau khi hoàn tất), loại bỏ việc duy trì Endpoint cố định để tối ưu chi phí.
* **Model Versioning**: Quản lý các phiên bản mô hình trực tiếp qua cấu trúc thư mục trên S3 (`finops-model-artifacts/v1/`, `v2/`...).

## 5. Bảo mật & Quản trị (Security & Governance)

* **IAM Roles for Service Accounts (IRSA)**: Gán IAM role trực tiếp cho Service Account của Pod trong EKS (ví dụ: cho phép Dagster Worker gọi API của SageMaker và SSM Parameter Store).
* **AWS Secrets Manager**: Lưu trữ DB credentials, API keys bảo mật.
* **AWS Systems Manager (SSM) Parameter Store**: Sử dụng Standard Parameters (miễn phí) để lưu trữ các thông tin phi nhạy cảm liên quan đến mô hình ML (như phiên bản model active `/finops/model/active_version`, ngưỡng đánh giá `/finops/model/evaluation_threshold`).
* **CloudWatch Logs**: Giám sát log từ EKS, Redshift, SageMaker.

## 6. Luồng CI/CD & Quản lý State (Infrastructure as Code)

* Toàn bộ hạ tầng được định nghĩa bằng **Terraform** cấu trúc Modular.
* **Bootstrap**: Tạo trước S3 Bucket và DynamoDB Table độc lập để làm Remote State Backend + State Locking (tránh ghi đè trùng lặp).
* Khi có thay đổi trong thư mục `infrastructure/terraform/`, GitHub Actions sẽ chạy tự động thông qua Remote State.
