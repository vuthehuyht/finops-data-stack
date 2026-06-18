# Redshift DDL Executor

Module này cung cấp công cụ và các script SQL template để quản lý, thiết lập (setup) và dọn dẹp (teardown) các database schema trên AWS Redshift Serverless cho môi trường phát triển (development). 

Hệ thống được thiết kế tương đương với workflow quản lý DDL trên Snowflake của dự án Adastria.

---

## 1. Cấu hình biến môi trường

Trước khi chạy, hãy đảm bảo các biến môi trường sau đã được thiết lập trong file `.env` ở thư mục gốc của dự án:

```bash
# Cấu hình kết nối AWS Redshift (DW)
REDSHIFT_HOST=localhost
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=dev
REDSHIFT_USER=awsuser
REDSHIFT_PASSWORD=your_password
```

Khi chạy trên môi trường **production**, script sẽ tự động gọi AWS Secrets Manager để tải thông tin đăng nhập bảo mật (dựa theo cấu hình có sẵn trong `src/common/redshift_util.py`).

---

## 2. Cách thiết lập môi trường phát triển (Setup)

Chạy script shell sau để khởi tạo các schema (`DB_UTILS`, `OPERATION`, `RAW`, `DWH`, `MART`, `LOGS`) trên database Redshift của bạn:

```bash
# Thực thi setup và yêu cầu xác nhận trước khi chạy
./dev/setup.sh

# Thực thi setup và bỏ qua bước xác nhận (dùng cho CI/CD hoặc chạy tự động)
./dev/setup.sh --skip_confirmation
```

---

## 3. Cách dọn dẹp môi trường phát triển (Teardown)

Chạy script shell sau để xóa toàn bộ các schema phát triển đã tạo (lưu ý hành động này sẽ xóa sạch dữ liệu của các schema đó):

```bash
# Thực thi teardown và yêu cầu xác nhận
./dev/teardown.sh

# Thực thi teardown và bỏ qua xác nhận
./dev/teardown.sh --skip_confirmation
```

---

## 4. Thực thi thủ công file SQL DDL đơn lẻ

Bạn có thể chạy trực tiếp `ddl_executor.py` để compile Jinja2 và thực thi các câu lệnh SQL tùy chọn:

```bash
uv run python ddl_executor.py \
  --template_parameters="{
    \"schema_name_raw\": \"RAW\",
    \"schema_name_dwh\": \"DWH\"
  }" \
  ddl/raw/some_table.sql.jinja
```

Nếu muốn xuất câu lệnh SQL đã được compile ra file text để chạy thủ công trên giao diện Web Console của AWS Redshift mà không chạy trực tiếp trên DB, sử dụng tham số `--output_ddl_query_file_path`:

```bash
uv run python ddl_executor.py \
  --template_parameters="{\"schema_name_raw\": \"RAW\"}" \
  --output_ddl_query_file_path out.sql \
  ddl/raw/some_table.sql.jinja
```
