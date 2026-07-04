# Machine Learning Architecture Design

Tài liệu này mô tả thiết kế cấp cao (high-level) của mô hình Machine Learning/Deep Learning đóng vai trò cốt lõi trong việc đánh giá và dự phóng giá trị cổ phiếu.

## 1. Phương pháp tiếp cận: Multimodal Neural Network (Hybrid)
Do bản chất của dữ liệu tài chính bao gồm cả dữ liệu chuỗi thời gian (giá, biến động thị trường) và dữ liệu dạng bảng tĩnh (BCTC, Vĩ mô), hệ thống sẽ sử dụng kiến trúc **Multimodal Hybrid Network** (Kết hợp LSTM và MLP). Thiết kế này cho phép mô hình học các tín hiệu xu hướng trong ngắn/trung hạn kết hợp với nền tảng cơ bản của doanh nghiệp.

## 2. Kiến trúc Mô hình (Model Architecture)
Mô hình được chia thành nhiều nhánh (branches) riêng biệt ở giai đoạn đầu để xử lý từng loại dữ liệu đặc thù, sau đó hợp nhất lại.

### 2.1. Nhánh 1: Time-Series Branch (Sequence Data)
*   **Input Data:** Các chuỗi lịch sử (ví dụ: window size = 30 ngày) của Giá, Khối lượng, Sentiment Score hàng ngày, Lãi suất hàng ngày.
*   **Kiến trúc:** Sử dụng mạng Recurrent Neural Network (**LSTM** hoặc **GRU**) hoặc bộ mã hóa biến áp (**Transformer Encoder** 1 lớp).
*   **Mục tiêu:** Trích xuất các đặc trưng tiềm ẩn về động lượng (momentum), biến động (volatility) và phản ứng của thị trường với tin tức.

### 2.2. Nhánh 2: Fundamental & Macro Branch (Tabular Data)
*   **Input Data:** Dữ liệu tĩnh hoặc cập nhật chậm tại thời điểm dự báo (P/E, ROE, Margin, Tỷ lệ đòn bẩy, GDP, Tốc độ tăng trưởng doanh thu).
*   **Kiến trúc:** Sử dụng mạng nơ-ron truyền thẳng (**MLP** - Multi-Layer Perceptron) với các lớp Dense Layers và Dropout để tránh Overfitting.
*   **Mục tiêu:** Nắm bắt "sức khỏe" tài chính và định giá tương đối của doanh nghiệp tại thời điểm hiện tại.

### 2.3. Lớp kết hợp (Fusion Layer)
*   Output của Nhánh 1 (Vector trạng thái cuối cùng của LSTM) và Output của Nhánh 2 (Vector từ MLP) sẽ được nối lại với nhau (**Concatenation**).
*   Sau khi hợp nhất, dữ liệu đi qua một số lớp Dense Layers trung gian để mô hình học sự tương tác giữa biến động giá và nền tảng cơ bản.

### 2.4. Output Layer (Tùy thuộc bài toán cụ thể)
Tùy thuộc vào chiến lược triển khai thực tế, Output Layer có thể được cấu hình linh hoạt:
*   **Định giá / Regression:** 1 neuron xuất ra giá trị thực (ví dụ: Tỷ suất sinh lời kỳ vọng 30 ngày tới). Hàm mất mát: `MSE` hoặc `Huber Loss`.
*   **Phân loại (Classification):** Softmax/Sigmoid xuất ra xác suất rơi vào các nhóm (Tăng trưởng mạnh, Đi ngang, Giảm mạnh). Hàm mất mát: `Cross-Entropy`.

## 3. Khung công nghệ (Tech Stack)
*   **Framework:** `PyTorch` hoặc `TensorFlow/Keras`.
*   **Huấn luyện (Training):** **AWS SageMaker Training Jobs** (GPU instances như `g4dn.xlarge`).
*   **Dự báo (Inference):** **AWS SageMaker Batch Transform (Serverless Batch)**.
    *   *Lợi ích:* Tự động khởi chạy máy chủ tính toán on-demand, đọc dữ liệu feature hàng loạt từ S3, thực hiện suy luận và lưu kết quả, sau đó tự động giải phóng tài nguyên. Tối ưu chi phí cho pipeline chạy Daily (chỉ tốn chi phí trên thời gian thực tế xử lý batch).
    *   *Cấu hình dự kiến:* Cấu hình instance `ml.m5.large` hoặc tương đương.
    *   *Output format:* Mỗi dòng output tự chứa `{"ticker": ..., "predicted_return": ...}` (container serving echo lại `ticker` từ input) — cho phép `COPY` thẳng vào Redshift, không cần khớp theo thứ tự dòng input/output.
    *   *Ngày dự báo:* `TRADING_DATE` ghi vào `FCT_ML_FORECAST_RESULTS` là ngày giao dịch **kế tiếp** sau ngày có dữ liệu feature mới nhất (anchor date), vì nhãn `LABEL_NEXT_5D_RETURN` là lợi nhuận kỳ vọng tính từ ngày đó trở về sau.

## 4. Quy trình huấn luyện (Training Strategy)
*   **Validation Split:** Không sử dụng random split. Bắt buộc sử dụng **Time-series Cross-Validation** (Rolling Window) để đảm bảo mô hình không "nhìn thấy" tương lai trong quá trình huấn luyện.
*   **Data Leakage Prevention:** Đảm bảo tất cả các feature từ BCTC đều được *lag* (trễ) đúng ngày công bố thực tế trên thị trường, không sử dụng ngày khóa sổ kế toán.

## 5. Quản lý và Triển khai mô hình (Model Management)
Hệ thống sử dụng các dịch vụ Managed của AWS để quản lý vòng đời mô hình:

*   **Model Artifact Storage:** Sau khi train xong, các file trọng số và cấu hình (`model.tar.gz`) được lưu trữ tập trung tại **Amazon S3**.
*   **Model Versioning (S3-based):** Sử dụng cấu trúc phân cấp trên **Amazon S3** để lưu trữ và quản lý phiên bản:
    *   Path: `s3://finops-model-artifacts/<model_name>/<version>/`
    *   Chứa: `model.tar.gz` (weights), `metadata.json` (metrics, hyperparameters).
*   **CI/CD for ML (MLOps):** Quy trình thăng cấp mô hình (Promotion) được thực hiện thông qua việc cập nhật active version lên SSM Parameter Store (`/finops/model/active_version`). Luồng dự báo hàng ngày sẽ tự động tham chiếu phiên bản này để kích hoạt Transform Job trên SageMaker.