# Bài Tập Thực Hành 3: Thử Lửa Với Trạng Thái Half-Open và Time-Based

## 1. Giới thiệu bài tập
Mục tiêu của bài tập này là thiết kế và cấu hình hoàn chỉnh cơ chế ngắt mạch tự động **Circuit Breaker** theo chuẩn thiết kế của **Resilience4j**, nhằm đáp ứng một thỏa thuận SLA thực tế đối với hệ thống dịch vụ tính phí vận chuyển `shippingClient` (thuộc Giao Hàng Nhanh - GHN).

## 2. Thỏa thuận nghiệp vụ SLA & Cấu hình mô phỏng
- **Bối cảnh:** Hệ thống tính phí sập lúc cao điểm.
- **SLA xử lý:** 
  1. Khi sập, ngưng gọi đúng **20 giây**.
  2. Sau 20 giây, hệ thống tự động hé cửa mà không cần request mồi (**Automatic Transition** từ `OPEN` sang `HALF-OPEN`).
  3. Khi ở trạng thái `HALF_OPEN`, gửi thử **3 đơn hàng** qua. Nếu cả 3 đều thành công, chuyển mạch về trạng thái `CLOSED` để hoạt động bình thường.

### Thiết lập Config chi tiết:
- **Loại cửa sổ:** `TIME_BASED` (Thời gian)
- **Kích thước cửa sổ (Sliding Window Size):** 30 giây
- **Thời gian chờ ngắt mạch (`wait_duration_in_open_state`):** 20 giây
- **Số request thử nghiệm trong Half-Open (`permitted_number_of_calls_in_half_open_state`):** 3
- **Tự động chuyển từ OPEN sang HALF-OPEN (`automatic_transition_from_open_to_half_open_enabled`):** `True`

---

## 3. Kiến trúc File mã nguồn
Chương trình sử dụng ngôn ngữ **Python** để giả lập một Circuit Breaker chuẩn đồng thời và an toàn đa luồng (thread-safe):
- Sử dụng `threading.Lock` để đồng bộ kiểm tra trạng thái.
- Sử dụng `threading.Timer` để tự động kích hoạt callback chuyển trạng thái từ `OPEN` sang `HALF_OPEN` bất đồng bộ ngay khi hết 20 giây (không cần có request kích thích bên ngoài).
- Sử dụng `ThreadPoolExecutor` để bắn đồng loạt 10 requests cùng một lúc nhằm kiểm nghiệm hành vi từ chối dịch vụ của trạng thái `HALF_OPEN`.

---

## 4. Hướng dẫn chạy chương trình

Bạn chỉ cần chạy file script Python duy nhất là `main.py`. Không yêu cầu cài đặt thêm bất kỳ thư viện ngoài nào (sử dụng thư viện built-in standard của Python).

```bash
python main.py
```

---

## 5. Phân tích kết quả chạy (Checklist tự đánh giá)
Khi bạn chạy chương trình, bạn sẽ quan sát thấy tiến trình chạy trên terminal như sau:

1. **Chuyển sang OPEN:** Hệ thống giả lập sự cố và ép mạch sang `OPEN`. 
2. **Tự động chuyển sang HALF_OPEN:** Chương trình in ra bộ đếm giây từ 1 tới 21. Khi vừa chạm mốc **20 giây**, luồng `Timer` chạy ngầm sẽ tự động log thông báo chuyển sang `HALF_OPEN` mà không cần gọi hàm nào trước.
3. **Thử nghiệm 10 Requests đồng thời:**
   - **Đúng 3 requests đầu tiên** được cấp phép đi qua, thực thi tính phí thành công (`SUCCESS`).
   - **7 requests còn lại** bị từ chối ngay lập tức (`REJECTED`) kèm theo biệt lệ `CallNotPermittedException`.
4. **Phục hồi trạng thái:** Sau khi nhận đủ 3 kết quả thành công, Circuit Breaker tự động chuyển mạch về `CLOSED` hoạt động bình thường, chấp nhận cuộc gọi tiếp theo (`DH-9999`) trơn tru.
