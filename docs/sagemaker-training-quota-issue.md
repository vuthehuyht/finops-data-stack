# SageMaker Training Job — Vấn đề Service Quota của Account

## Tóm tắt

Lần chạy thử thật đầu tiên của
`scripts/sagemaker_smoke_test/trigger_sagemaker_training.py` (xem
`docs/superpowers/specs/2026-07-20-sagemaker-training-smoke-test-design.md`)
bị fail ngay tại API call `CreateTrainingJob`, không phải do lỗi code ứng
dụng. Account AWS (`654654329682`, region `ap-southeast-1`) có **quota
SageMaker training job = 0** cho mọi loại instance — đây là trạng thái mặc
định của account mới chưa từng chạy SageMaker training job nào, không liên
quan đến IAM role hay CDK stack của project.

## Đã xảy ra gì

1. `scripts/sagemaker_smoke_test/cdk/` deploy thành công (S3 bucket + IAM
   execution role — stack `FinopsMlSandboxStack`, output `BucketName` =
   `finops-ml-sandbox-artifacts`, `ExecutionRoleArn` =
   `arn:aws:iam::654654329682:role/FinopsMlSandboxStack-SageMakerExecutionRole7843F3B8-eWJT9AlLI3Sw`).
2. `trigger_sagemaker_training.py` chạy qua bước build features
   (`scripts/dataset_builder.py`, 39,180 rows) và upload S3 thành công.
3. Lời gọi `ModelTrainer.train(...)` (gọi `CreateTrainingJob`) fail với:

   ```
   ResourceLimitExceeded: An error occurred (ResourceLimitExceeded) when
   calling the CreateTrainingJob operation: The account-level service limit
   'ml.m5.large for training job usage' is 0 Instances, with current
   utilization of 0 Instances and a request delta of 1 Instances. Please use
   AWS Service Quotas to request an increase for this quota.
   ```

4. Kiểm tra toàn bộ quota SageMaker training job trong account
   (`aws service-quotas list-service-quotas --service-code sagemaker`, lọc
   `*for training job usage*`) — **tất cả loại instance đều = 0**, xác nhận
   đây là mặc định toàn account, không riêng gì `ml.m5.large`.

## Tại sao lại như vậy

AWS mặc định đặt quota SageMaker training job instance = 0 cho account chưa
từng dùng SageMaker, để tránh lạm dụng tài nguyên GPU/CPU. Giới hạn này được
enforce ở tầng **Service Quotas** của AWS account/region, độc lập hoàn toàn
với IAM permission — execution role và trust policy đã đúng, không liên
quan đến lỗi này.

## Đang xử lý

Đã gửi yêu cầu tăng quota:

```bash
aws service-quotas request-service-quota-increase \
  --service-code sagemaker \
  --quota-code L-611FA074 \
  --desired-value 1 \
  --region ap-southeast-1
```

- Quota: `ml.m5.large for training job usage` (code `L-611FA074`)
- Request ID: `d33d54a5e3c444e9bc5191ae39289fa64VV348hk`
- Trạng thái tại thời điểm ghi: `CASE_OPENED` (AWS đang xử lý; yêu cầu tăng
  nhỏ như thế này thường được tự động duyệt trong vài phút đến vài giờ,
  nhưng không có SLA đảm bảo).

Kiểm tra trạng thái:

```bash
aws service-quotas get-requested-service-quota-change \
  --request-id d33d54a5e3c444e9bc5191ae39289fa64VV348hk \
  --region ap-southeast-1
```

## Khi được duyệt

Chạy lại trigger script với cùng tham số — không cần sửa code hay
infrastructure gì thêm, đây thuần túy là chặn ở tầng account quota:

```bash
uv run python scripts/sagemaker_smoke_test/trigger_sagemaker_training.py \
  --role-arn "arn:aws:iam::654654329682:role/FinopsMlSandboxStack-SageMakerExecutionRole7843F3B8-eWJT9AlLI3Sw" \
  --bucket "finops-ml-sandbox-artifacts"
```

## Lưu ý cho account/region AWS khác trong tương lai

Bất kỳ account AWS mới nào, hoặc chuyển sang region khác, nhiều khả năng sẽ
gặp lại tường quota = 0 này ngay lần chạy SageMaker training job đầu tiên.
Kiểm tra quota trước khi debug code ứng dụng:

```bash
aws service-quotas list-service-quotas --service-code sagemaker \
  --region <region> \
  --query "Quotas[?contains(QuotaName, 'training job usage') && Value > \`0\`]"
```

Kết quả rỗng nghĩa là cần gửi yêu cầu tăng quota trước.
