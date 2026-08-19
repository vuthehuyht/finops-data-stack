# AWS Infrastructure Design

Tài liệu này mô tả thiết kế hạ tầng điện toán đám mây trên AWS cho hệ thống FinOps Data Stack, tập trung vào tính bảo mật, khả năng mở rộng, sẵn sàng cao (HA) và tối ưu hóa chi phí tối đa.

______________________________________________________________________

## 1. Mạng & Kết nối (Networking)

Sử dụng mô hình VPC tiêu chuẩn để cô lập tài nguyên trên 2 Availability Zones (Singapore):

- **VPC**: `10.0.0.0/16`.
- **Subnets**:
  - 2 Public Subnets (cho NAT Gateway, Load Balancer).
  - 2 Private App Subnets (cho EKS Core & Worker Nodes).
  - 2 Private DB Subnets (cho Redshift Serverless Workgroup).
- **NAT Gateway**: 1 cái managed NAT Gateway dùng chung cho cả 2 AZ để tiết kiệm chi phí, đặt tại Public Subnet của AZ đầu tiên. *(Lưu ý: Có thiết kế thay thế bằng NAT Instance Spot `t4g.nano` để tối ưu chi phí hơn nữa cho môi trường lab ngắn ngày — xem `docs/superpowers/specs/2026-06-28-infra-cost-optimization-design.md` — nhưng phần này **chưa được triển khai** trong `infrastructure/terraform/modules/vpc/`, hạ tầng thực tế vẫn dùng managed NAT Gateway.)*
- **S3 Gateway Endpoint**: Cấu hình VPC Endpoint cho S3. Toàn bộ traffic truyền tải dữ liệu data lake lớn giữa EKS/Redshift và S3 sẽ đi nội bộ bên trong AWS, tránh đi qua NAT Gateway để loại bỏ phí truyền dữ liệu và tăng tốc độ.
- **Security Groups**: Thiết lập các quy tắc nghiêm ngặt qua các `aws_security_group_rule` độc lập ở root module (`infrastructure/terraform/main.tf`) — chỉ cho phép traffic từ EKS Node/Cluster Security Group kết nối tới Redshift qua port `5439` và tới RDS PostgreSQL qua port `5432`.

## 2. Tính toán & Điều phối (Compute)

- **Amazon EKS (Elastic Kubernetes Service)**:
  - **Control Plane**: Managed bởi AWS (v1.36).
  - **Core Node Group (On-Demand, static)**: Đặt trong Private App Subnets, sử dụng `t3a.medium`, dung lượng 1-2 node để chạy các pod Core ổn định 24/7 (Dagster Webserver, Dagster Daemon, CoreDNS). Pod chạy trên node group này khai báo `nodeSelector: {node-group: core}`.
  - **Karpenter (Dynamic Spot Provisioning)**: Thay cho Worker Node Group tĩnh + Cluster Autoscaler trước đây (đã migrate hoàn tất — xem `docs/superpowers/plans/2026-07-18-karpenter-eks-migration.md`). Karpenter tự động launch/terminate Spot node theo nhu cầu thực tế của các job xử lý dữ liệu nặng (`NodePool`/`EC2NodeClass` tại `src/k8s/manifest/karpenter/nodepool.yaml`), scale từ 0 khi nhàn rỗi. `NodePool` giới hạn instance family `t3`/`t3a`, chỉ dùng capacity-type `spot`, cap `limits.cpu = 8`. Pod worker khai báo `nodeSelector: {node-group: worker}` kèm toleration `spotWorker=true:NoSchedule`. Hạ tầng hỗ trợ gồm:
    - IRSA role cho Karpenter controller + IAM policy scoped theo tag để provision/terminate EC2.
    - Node IAM role riêng cho instance do Karpenter launch, đăng ký qua EKS access entry.
    - SQS queue + EventBridge rules (Spot interruption, instance state change, rebalance recommendation, AWS Health event) để xử lý Spot interruption notice.
    - Karpenter controller (Helm chart `oci://public.ecr.aws/karpenter/karpenter`, version `1.11.3`) và manifest `NodePool`/`EC2NodeClass` được cài đặt bởi GitHub Actions composite action (`.github/actions/deploy-dagster-to-eks/action.yml`), không phải Terraform — Terraform chỉ quản lý phần AWS/IAM.
- **Amazon ECR (Elastic Container Registry)**: Lưu trữ các private container images của Dagster, dbt và crawlers. Tích hợp Lifecycle Policy tự động dọn dẹp images không tag sau 7 ngày và chỉ giữ lại tối đa 5 images gần nhất để tiết kiệm chi phí lưu trữ. Repository bật `force_delete` để cho phép destroy khi vẫn còn images bên trong.

## 3. Lưu trữ & Warehouse (Storage & DWH)

- **Amazon S3 (Data Lake)**:
  - `finops-data-lake-raw`: Chứa dữ liệu Bronze (Parquet/JSON). Có Lifecycle Rule tự động chuyển dữ liệu cũ hơn 90 ngày sang lớp **S3 Glacier Instant Retrieval** để giảm phí lưu trữ.
  - `finops-data-lake-processed`: Chứa logs và kết quả trung gian. Không có Lifecycle Rule (giữ nguyên toàn bộ).
  - `finops-model-artifacts-<account_id>`: Chứa các model artifacts (`.tar.gz`) phân chia theo phiên bản. Không có Lifecycle Rule.
  - `finops-dagster-io`: I/O intermediate storage cho Dagster (IOManager). Không có Lifecycle Rule.
  - **Mã hóa**: Sử dụng mã hóa mặc định **SSE-S3** (`AES256`, Amazon S3 managed keys) hoàn toàn miễn phí (không dùng KMS Customer Managed Key riêng để tối ưu chi phí).
  - **Public Access Block**: Bật cả 4 flag (`block_public_acls`, `block_public_policy`, `ignore_public_acls`, `restrict_public_buckets`) trên toàn bộ 4 bucket.
  - **`force_destroy = true`**: Cho phép `terraform destroy` xóa sạch bucket kể cả khi còn object bên trong (phù hợp môi trường lab ngắn ngày, cần cẩn trọng nếu áp dụng cho production lâu dài).
- **Amazon Redshift Serverless**:
  - **Namespace**: Quản lý database (`finops_db`), schemas, users. Được mã hóa tĩnh bằng AWS managed key (`aws/redshift`).
  - **Workgroup**: Đặt trong Private DB Subnets. Cấu hình cố định `base_capacity = 8` và `max_capacity = 8` (8 RPU là mức tối thiểu AWS Redshift Serverless hỗ trợ chính thức) để giữ chi phí tính toán ở mức thấp nhất; auto-scale-to-idle là hành vi mặc định của Redshift Serverless khi không có truy vấn.
  - **Usage Limit**: `aws_redshiftserverless_usage_limit` đặt hạn mức chi phí compute hàng tháng (`monthly_cost_cap_usd`, tùy theo environment), `breach_action = "deactivate"` — tự động deactivate workgroup nếu vượt hạn mức để tránh phát sinh hóa đơn ngoài kiểm soát.
  - **Redshift Spectrum**: Cho phép truy vấn trực tiếp dữ liệu thô trên S3 thông qua Glue Data Catalog mà không cần load vào ổ đĩa của Redshift.
- **Amazon RDS PostgreSQL** (`modules/rds`): Lưu trữ Dagster run/event metadata (không phải data lake). Instance PostgreSQL 16, `storage_encrypted = true`, `gp3`, đặt trong Private DB Subnets, không public access, `deletion_protection = false` (phù hợp môi trường lab). Credentials được sinh ngẫu nhiên (`random_password`) và đẩy vào Secrets Manager; Dagster Helm chart lấy connection info qua External Secrets Operator.

## 4. Machine Learning (SageMaker)

- **SageMaker Training**: Chạy theo nhu cầu (On-demand) trên các instance có GPU nhỏ nhất (ví dụ: `g4dn.xlarge`) để huấn luyện mô hình.
- **SageMaker Batch Transform (Serverless Batch)**: Khởi chạy các phiên suy luận hàng ngày theo mô hình batch transform (tự động khởi chạy máy chủ, xử lý dữ liệu và tự giải phóng tài nguyên sau khi hoàn tất), loại bỏ việc duy trì Endpoint cố định để tối ưu chi phí.
- **Model Versioning**: Quản lý các phiên bản mô hình trực tiếp qua cấu trúc thư mục trên S3 (`finops-model-artifacts/v1/`, `v2/`...).

## 5. Bảo mật & Quản trị (Security & Governance)

- **IAM Roles for Service Accounts (IRSA)**: Gán IAM role trực tiếp cho Service Account của Pod trong EKS qua OIDC provider của cluster — gồm `dagster-sa` (S3, SSM, SageMaker), `external-secrets-sa` (đọc Secrets Manager), và `karpenter` (provision/terminate EC2).
- **AWS Secrets Manager**: Lưu trữ DB credentials (RDS, Redshift), API keys (FireAnt, Slack) trong một secret hợp nhất. **External Secrets Operator** (chạy trên EKS, IRSA `external-secrets-sa`) đồng bộ secret này thành Kubernetes Secret (`dagster-pg-credentials`) cho Dagster Helm chart — cấu hình tại `src/k8s/manifest/external-secrets/`.
- **AWS Systems Manager (SSM) Parameter Store**: Sử dụng Standard Parameters (miễn phí) để lưu trữ các thông tin phi nhạy cảm liên quan đến mô hình ML (như phiên bản model active `/finops/model/active_version`, ngưỡng đánh giá `/finops/model/evaluation_threshold`).
- **CloudWatch Logs**: Giám sát log từ EKS, Redshift, SageMaker.
- **GitHub OIDC**: GitHub Actions xác thực với AWS qua OpenID Connect (`aws_iam_openid_connect_provider.github_actions`), không dùng long-lived access key. Role `github-actions-deploy` giới hạn trust policy theo đúng repo (`vuthehuyht/finops-data-stack`), có quyền `terraform apply` toàn bộ resource loại project quản lý, cộng EKS Access Entry với policy `AmazonEKSClusterAdminPolicy` để `kubectl`/`helm` thao tác trên cluster.

## 6. Luồng CI/CD & Quản lý State (Infrastructure as Code)

- Toàn bộ hạ tầng được định nghĩa bằng **Terraform** cấu trúc Modular (`infrastructure/terraform/modules/{vpc,eks,ecr,s3,redshift,rds,sagemaker,secrets,ssm}`).
- **Bootstrap**: Tạo trước S3 Bucket và DynamoDB Table độc lập (`infrastructure/terraform/bootstrap/`) để làm Remote State Backend + State Locking (tránh ghi đè trùng lặp).
- **`terraform-ci.yml`** (GitHub Actions): chạy `terraform fmt -check`, `terraform validate` (cho main project, `bootstrap/`, `dev_local/`) và `tflint` trên mọi PR/push chạm `infrastructure/terraform/**`. **Không chạy `terraform apply`** — apply hạ tầng vẫn là thao tác thủ công của người vận hành.
- **`docker.yml`** (GitHub Actions): khi push lên `main` chạm code Dagster (`src/dagster/**`, `src/pipeline/dagster/**`, `src/common/**`, `src/docker/**`), pipeline sẽ: lint Dockerfile (hadolint), build & push image lên ECR (xác thực AWS qua OIDC), đọc Terraform state hiện có (`terraform init` + `terraform output`, không apply), rồi gọi composite action `deploy-dagster-to-eks` để cài đặt/upgrade Karpenter, External Secrets Operator, và Dagster Helm chart trên cluster đã tồn tại sẵn.
- Deploy pipeline có tính idempotent: tạo namespace/ServiceAccount nếu chưa có, chờ ExternalSecret sync xong secret `dagster-pg-credentials` trước khi `helm upgrade --install` Dagster.
