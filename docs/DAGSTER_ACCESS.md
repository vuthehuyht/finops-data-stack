# Hướng dẫn truy cập Dagster Webserver (Dagit) trên EKS từ Local

Sau khi bạn đã chạy lệnh `make deploy_eks` (hoặc cài đặt Dagster qua Helm) thành công lên EKS Cluster, Dagster UI sẽ chạy bên trong cụm Kubernetes. Để truy cập giao diện này trực tiếp từ máy tính (local) của bạn mà không cần mở Load Balancer public, hãy sử dụng tính năng **Port Forward** của `kubectl`.

## Các bước thực hiện

### 1. Cập nhật Kubeconfig

Nếu bạn chưa đăng nhập vào EKS cluster trên terminal hiện tại, hãy chạy lệnh sau để kéo file cấu hình về máy (sử dụng profile `admin`):

```bash
aws eks update-kubeconfig \
  --region ap-southeast-1 \
  --name finops-eks-cluster \
  --profile admin
```

### 2. Port Forward Dagster Webserver

Chạy lệnh sau để map port 3000 trên máy của bạn với port 80 của Dagster Service đang chạy trong EKS:

```bash
kubectl port-forward svc/dagster-webserver 3000:80 -n dagster
```

*(Lưu ý: Nếu bạn sử dụng phiên bản Helm Chart Dagster cũ, tên service có thể là `dagster-dagit`. Khi đó lệnh sẽ là `kubectl port-forward svc/dagster-dagit 3000:80 -n dagster`)*

### 3. Truy cập trên trình duyệt

Mở trình duyệt web của bạn lên và truy cập vào địa chỉ:

👉 **[http://localhost:3000](http://localhost:3000)**

---

**Tip:** Lệnh `kubectl port-forward` sẽ giữ terminal chạy liên tục. Bạn nên mở một tab terminal riêng biệt chỉ để chạy lệnh này. Khi nào không cần xem UI nữa thì bấm `Ctrl + C` để tắt.
