# Machine Learning Architecture Design

Tài liệu này mô tả thiết kế cấp cao (high-level) của mô hình Machine Learning/Deep Learning đóng vai trò cốt lõi trong việc đánh giá và dự phóng giá trị cổ phiếu.

## 1. Phương pháp tiếp cận: Multimodal Neural Network (Hybrid)

Do bản chất của dữ liệu tài chính bao gồm cả dữ liệu chuỗi thời gian (giá, biến động thị trường) và dữ liệu dạng bảng tĩnh (BCTC, Vĩ mô), hệ thống sẽ sử dụng kiến trúc **Multimodal Hybrid Network** (Kết hợp LSTM và MLP). Thiết kế này cho phép mô hình học các tín hiệu xu hướng trong ngắn/trung hạn kết hợp với nền tảng cơ bản của doanh nghiệp.

## 2. Kiến trúc Mô hình (Model Architecture)

Mô hình được chia thành nhiều nhánh (branches) riêng biệt ở giai đoạn đầu để xử lý từng loại dữ liệu đặc thù, sau đó hợp nhất lại.

### 2.1. Nhánh 1: Time-Series Branch (Sequence Data)

- **Input Data:** Chuỗi `WINDOW_SIZE = 30` ngày giao dịch gần nhất của 6 feature (`src/ml/config.py::SEQUENCE_FEATURE_COLUMNS`): `moving_average_20d`, `moving_average_50d`, `price_momentum_1m`, `price_momentum_3m`, `volatility_30d`, `relative_strength_vs_vnindex`.
- **Kiến trúc:** `LayerNorm` trên input, sau đó **LSTM** 1 lớp (`hidden_size=64`) — lấy hidden state cuối cùng làm vector đặc trưng (`src/ml/model.py::TimeSeriesBranch`). GRU/Transformer Encoder chưa được triển khai.
- **Mục tiêu:** Trích xuất các đặc trưng tiềm ẩn về động lượng (momentum) và biến động (volatility) của giá.

### 2.2. Nhánh 2: Fundamental & Macro Branch (Tabular Data)

- **Input Data:** Snapshot 13 feature tại ngày dự báo (`src/ml/config.py::TABULAR_FEATURE_COLUMNS`), gồm nhóm định giá/hiệu quả tài chính (`pe_ratio`, `pb_ratio`, `roe`, `roa`, `revenue_growth_yoy`, `net_profit_growth_yoy`, `gross_margin`, `debt_to_equity`, `operating_cash_flow_to_net_income`) và nhóm dòng tiền khối ngoại/tự doanh (`foreign_buy_ratio_10d`, `net_foreign_flow_momentum_1m`, `prop_trading_net_val_5d`, `prop_vs_foreign_correlation_10d`). Hiện chưa có feature vĩ mô (GDP...) trong tập này.
- **Kiến trúc:** `BatchNorm1d` trên input, sau đó **MLP** 2 lớp ẩn `(32, 16)` (`Linear → BatchNorm1d → ReLU → Dropout`, `dropout_rate=0.4`) (`src/ml/model.py::TabularBranch`).
- **Mục tiêu:** Nắm bắt "sức khỏe" tài chính, định giá tương đối và dòng tiền khối ngoại/tự doanh của doanh nghiệp tại thời điểm hiện tại.

### 2.3. Lớp kết hợp (Fusion Layer)

- Vector hidden state cuối của LSTM (64 chiều) và vector output của MLP (16 chiều) được nối lại (**Concatenation**, 80 chiều).
- Đi qua `Linear(80 → 32) → ReLU → Linear(32 → 1)` để ra dự báo cuối cùng (`src/ml/model.py::FusionModel`).

### 2.4. Output Layer

Hiện tại chỉ triển khai bài toán **Regression**: 1 neuron xuất ra giá trị thực — `LABEL_NEXT_5D_RETURN` (tỷ suất sinh lời kỳ vọng 5 ngày giao dịch tới). Hàm mất mát: **Huber Loss** (`torch.nn.HuberLoss`, `src/ml/train.py`). Biến thể Classification (phân loại nhóm tăng/giảm) chưa được triển khai.

## 3. Khung công nghệ (Tech Stack)

- **Framework:** `PyTorch`, chạy trong container PyTorch built-in của SageMaker (script mode, không dùng custom Docker image).
- **Huấn luyện (Training):** **AWS SageMaker Training Jobs** qua `ModelTrainer` (SageMaker SDK v3), instance `ml.g4dn.xlarge` (GPU), image PyTorch `2.6.0`/`py312` (`src/ml/training_job.py`).
- **Dự báo (Inference):** **AWS SageMaker Batch Transform (Serverless Batch)**.
  - *Lợi ích:* Tự động khởi chạy máy chủ tính toán on-demand, đọc dữ liệu feature hàng loạt từ S3, thực hiện suy luận và lưu kết quả, sau đó tự động giải phóng tài nguyên. Tối ưu chi phí cho pipeline chạy Daily (chỉ tốn chi phí trên thời gian thực tế xử lý batch).
  - *Cấu hình:* Instance `ml.g4dn.xlarge` (mặc định của `SageMakerResource.run_batch_transform_job`, `src/dagster/resources.py`), image serving PyTorch `2.2-cpu-py310` (`_INFERENCE_IMAGE`, `src/dagster/inference_job.py`).
  - *Output format:* Mỗi dòng output tự chứa `{"ticker": ..., "predicted_return": ...}` (container serving echo lại `ticker` từ input) — cho phép `COPY` thẳng vào Redshift, không cần khớp theo thứ tự dòng input/output.
  - *Ngày dự báo:* `TRADING_DATE` ghi vào `FCT_ML_FORECAST_RESULTS` là ngày giao dịch **kế tiếp** sau ngày có dữ liệu feature mới nhất (anchor date), vì nhãn `LABEL_NEXT_5D_RETURN` là lợi nhuận kỳ vọng tính từ ngày đó trở về sau.

## 4. Quy trình huấn luyện (Training Strategy)

- **Validation Split:** Chronological split theo ngày (không random) — `train_end_date`/`val_end_date` mặc định tự resolve bằng percentile 80%/90% của `TRADING_DATE` trên Redshift (`src/dagster/ml_job.py::ml_training_job`), chia thành 3 tập train/val/test kế tiếp nhau theo thời gian (`src/ml/dataset.py::time_based_split`). Chưa triển khai k-fold rolling-window cross-validation.
- **Early stopping:** Dừng huấn luyện khi `val_rmse` không cải thiện sau `patience=5` epoch, giữ lại checkpoint có `val_rmse` thấp nhất (`src/ml/train.py`).
- **Data Leakage Prevention:** Đảm bảo tất cả các feature từ BCTC đều được *lag* (trễ) đúng ngày công bố thực tế trên thị trường, không sử dụng ngày khóa sổ kế toán.

## 5. Quản lý và Triển khai mô hình (Model Management)

Hệ thống sử dụng các dịch vụ Managed của AWS để quản lý vòng đời mô hình:

- **Model Artifact Storage:** Sau khi train xong, các file trọng số và cấu hình (`model.tar.gz`) được lưu trữ tập trung tại **Amazon S3**.
- **Model Versioning (S3-based):** Sử dụng cấu trúc phân cấp trên **Amazon S3** để lưu trữ và quản lý phiên bản:
  - Path: `s3://finops-model-artifacts/<model_name>/<version>/` (`<model_name>` = `MODEL_NAME = "finops-multimodal-regressor"`, `<version>` = tên SageMaker Training Job — `src/ml/evaluation.py::model_version_prefix`).
  - Chứa: `model.tar.gz` (weights + serving code `serve.py`/`inference.py`/`model.py`/`config.py`), `metadata.json` (metrics `rmse`/`mae`, hyperparameters, feature columns).
- **Champion/Challenger Promotion:** Sau mỗi lần train, `ml_model_evaluation` (`src/dagster/ml_job.py`) so sánh RMSE của Challenger (model vừa train) với Champion (`active_version` hiện tại đọc từ SSM). Challenger được thăng cấp (promote) nếu chưa có Champion, hoặc nếu giảm RMSE tương đối ít nhất `threshold` — đọc từ SSM `/finops/model/evaluation_threshold`, fallback về config Dagster nếu SSM chưa set (`src/ml/evaluation.py::compare_and_promote`).
- **CI/CD for ML (MLOps):** Khi promote, Dagster cập nhật active version lên SSM Parameter Store (`/finops/model/active_version`). Luồng dự báo hàng ngày sẽ tự động tham chiếu phiên bản này để kích hoạt Transform Job trên SageMaker.

## 6. Lưu trữ dữ liệu (Data Storage for ML)

Dữ liệu phục vụ vòng đời Machine Learning được phân tách chặt chẽ trên **Amazon S3** theo nguyên tắc Data Lake Tiering và tuân thủ IAM Least-Privilege:

- **Dữ liệu huấn luyện (Training Data):**
  - Lưu tại: `s3://finops-data-lake-processed/ml-training-data/<run_date>/`
  - Nguồn gốc: Redshift Data Mart (`FACT_ML_FEATURE_SET`) export (UNLOAD) thẳng sang S3 dưới dạng Parquet.
- **Dữ liệu suy luận hàng ngày (Inference Data):**
  - **Input (JSON Lines):** `s3://finops-data-lake-processed/ml-inference-input/<trading_date>/input.jsonl`
  - **Output (JSON Lines):** `s3://finops-data-lake-processed/ml-inference-output/<trading_date>/input.jsonl.out` (SageMaker Batch Transform ghi trực tiếp kết quả vào thư mục này).
- **Trọng số mô hình (Model Artifacts):**
  - Lưu tại: `s3://finops-model-artifacts/...`
  - Tuyệt đối không lưu trữ dữ liệu dạng bảng/tabular trong bucket này để đảm bảo phân tách rõ ràng giữa "Code/Model" và "Data".

*Lưu ý:* Role thực thi của SageMaker (`sagemaker-execution-role`) được cấp quyền đọc/ghi trên cả bucket `processed` (để lấy data train/predict và ghi kết quả) và bucket `model_artifacts` (để lưu model weights). Dữ liệu thô (raw) được giữ an toàn tại `raw_bucket` không cho phép SageMaker truy cập.
