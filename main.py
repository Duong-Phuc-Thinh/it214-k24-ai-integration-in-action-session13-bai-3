import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum

class State(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CallNotPermittedException(Exception):
    """Exception raised when the circuit breaker is open and rejects calls."""
    pass

class CircuitBreaker:
    def __init__(self, name):
        self.name = name
        self.state = State.CLOSED
        
        # --- CẤU HÌNH THEO HỢP ĐỒNG SLA ---
        self.sliding_window_type = "TIME_BASED"                              # Loại cửa sổ: Thời gian
        self.sliding_window_size = 30                                       # Kích thước cửa sổ: 30 giây
        self.wait_duration_in_open_state = 20                               # Thời gian chờ khi ngắt mạch (SLA: ngưng gọi đúng 20s)
        self.permitted_number_of_calls_in_half_open_state = 3               # Số request thử nghiệm trong Half-Open (SLA: gửi thử 3 đơn hàng)
        self.automatic_transition_from_open_to_half_open_enabled = True     # Tự động chuyển OPEN -> HALF-OPEN không cần request mồi
        
        # Cấu hình thêm để hoàn thiện tính năng của một Circuit Breaker cơ bản
        self.failure_rate_threshold = 50.0                                  # Ngưỡng tỷ lệ lỗi kích hoạt ngắt mạch
        self.minimum_number_of_calls = 5                                    # Số request tối thiểu để tính tỷ lệ lỗi ở trạng thái CLOSED
        
        # Quản lý trạng thái nội bộ
        self.open_state_start_time = None
        self.half_open_calls = []                                           # Lưu lịch sử kết quả các request thử nghiệm trong HALF_OPEN
        self.half_open_active_calls = 0                                     # Số request hiện tại đang được phép thực thi ở HALF_OPEN
        self.lock = threading.Lock()                                        # Đảm bảo thread-safety khi truy cập tài nguyên chung
        self.call_history = []                                              # Lưu lịch sử cuộc gọi ở trạng thái CLOSED [(timestamp, success_status)]
        self.timer = None

    def _get_current_time(self):
        return time.time()

    def _transition_to_half_open(self):
        self.state = State.HALF_OPEN
        self.half_open_calls = []
        self.half_open_active_calls = 0
        print(f"[{time.strftime('%H:%M:%S')}] >>> [CIRCUIT BREAKER] CHUYỂN SANG HALF-OPEN (Hhé cửa thử nghiệm) <<<")

    def _transition_to_open(self):
        self.state = State.OPEN
        self.open_state_start_time = self._get_current_time()
        print(f"[{time.strftime('%H:%M:%S')}] >>> [CIRCUIT BREAKER] CHUYỂN SANG OPEN (Ngắt mạch) <<<")
        
        # Thiết lập cơ chế tự động hé cửa mà không cần request mồi nếu bật tính năng
        if self.automatic_transition_from_open_to_half_open_enabled:
            if self.timer:
                self.timer.cancel()
            # Sử dụng threading.Timer để chuyển đổi trạng thái sau đúng wait_duration_in_open_state giây
            self.timer = threading.Timer(self.wait_duration_in_open_state, self._auto_transition_callback)
            self.timer.start()

    def _auto_transition_callback(self):
        with self.lock:
            if self.state == State.OPEN:
                print(f"\n[{time.strftime('%H:%M:%S')}] [Auto-Scheduler] Đã trôi qua đúng {self.wait_duration_in_open_state} giây chờ.")
                print(f"[{time.strftime('%H:%M:%S')}] [Auto-Scheduler] Kích hoạt chuyển tự động sang trạng thái HALF-OPEN mà không cần request mồi.\n")
                self._transition_to_half_open()

    def _transition_to_closed(self):
        self.state = State.CLOSED
        self.call_history = []
        print(f"[{time.strftime('%H:%M:%S')}] >>> [CIRCUIT BREAKER] CHUYỂN SANG CLOSED (Cửa đóng - Hệ thống hoạt động bình thường) <<<")

    def can_call(self):
        """Kiểm tra xem cuộc gọi có được phép đi qua circuit breaker tại thời điểm hiện tại không."""
        if self.state == State.OPEN:
            return False
        if self.state == State.HALF_OPEN:
            # Chỉ cho phép tối đa số request thử nghiệm đi qua
            if self.half_open_active_calls < self.permitted_number_of_calls_in_half_open_state:
                self.half_open_active_calls += 1
                return True
            else:
                return False
        return True

    def execute(self, func, *args, **kwargs):
        """Bao bọc cuộc gọi hàm để giám sát và xử lý theo trạng thái Circuit Breaker."""
        with self.lock:
            allowed = self.can_call()
            current_state = self.state

        if not allowed:
            raise CallNotPermittedException(
                f"Call Rejected: Circuit Breaker đang ở trạng thái {current_state.value} và từ chối xử lý."
            )

        try:
            # Thực thi hàm nghiệp vụ (ngoài lock để tránh block các thread khác)
            result = func(*args, **kwargs)
            self._record_success(current_state)
            return result
        except Exception as e:
            self._record_failure(current_state)
            raise e

    def _record_success(self, state_at_start):
        with self.lock:
            now = self._get_current_time()
            if state_at_start == State.HALF_OPEN:
                self.half_open_calls.append(True)
                print(f"[{time.strftime('%H:%M:%S')}] -> Ghi nhận thành công thử nghiệm: {len(self.half_open_calls)}/{self.permitted_number_of_calls_in_half_open_state}")
                self._evaluate_half_open_state()
            elif state_at_start == State.CLOSED:
                self.call_history.append((now, True))
                self._evaluate_closed_state()

    def _record_failure(self, state_at_start):
        with self.lock:
            now = self._get_current_time()
            if state_at_start == State.HALF_OPEN:
                self.half_open_calls.append(False)
                print(f"[{time.strftime('%H:%M:%S')}] -> Ghi nhận thất bại thử nghiệm: {len(self.half_open_calls)}/{self.permitted_number_of_calls_in_half_open_state}")
                self._evaluate_half_open_state()
            elif state_at_start == State.CLOSED:
                self.call_history.append((now, False))
                self._evaluate_closed_state()

    def _evaluate_half_open_state(self):
        # Khi đã nhận đủ số lượng request thử nghiệm quy định
        if len(self.half_open_calls) >= self.permitted_number_of_calls_in_half_open_state:
            all_success = all(self.half_open_calls)
            if all_success:
                print(f"[{time.strftime('%H:%M:%S')}] Thử nghiệm thành công hoàn toàn (Cả {self.permitted_number_of_calls_in_half_open_state} đơn hàng tính phí thành công)!")
                self._transition_to_closed()
            else:
                print(f"[{time.strftime('%H:%M:%S')}] Có lỗi phát sinh trong quá trình thử nghiệm. Tiếp tục ngắt mạch.")
                self._transition_to_open()

    def _evaluate_closed_state(self):
        now = self._get_current_time()
        # Chỉ giữ lại các cuộc gọi trong khung thời gian sliding window
        self.call_history = [c for c in self.call_history if now - c[0] <= self.sliding_window_size]
        if len(self.call_history) >= self.minimum_number_of_calls:
            failures = sum(1 for c in self.call_history if not c[1])
            total = len(self.call_history)
            rate = (failures / total) * 100
            if rate >= self.failure_rate_threshold:
                print(f"[{time.strftime('%H:%M:%S')}] Tỷ lệ lỗi {rate:.1f}% vượt quá ngưỡng cho phép {self.failure_rate_threshold}%!")
                self._transition_to_open()

    def force_open(self):
        """Ép mạch chuyển sang OPEN để giả lập hệ thống bị sập."""
        with self.lock:
            print(f"[{time.strftime('%H:%M:%S')}] [System-Event] Kích hoạt ép mạch chuyển sang OPEN (Mô phỏng sập hệ thống)... ")
            self._transition_to_open()

    def shutdown(self):
        """Dọn dẹp background timer khi kết thúc chương trình."""
        if self.timer:
            self.timer.cancel()


# --- GIẢ LẬP NGHIỆP VỤ GHN SHIPPING-SERVICE ---
def calculate_shipping_fee(order_id):
    """Hàm nghiệp vụ tính phí vận chuyển của hệ thống"""
    # Giả lập xử lý nhanh trong 100ms
    time.sleep(0.1)
    return f"Đơn hàng {order_id}: Tính phí thành công (Phí: 16,500đ)"


if __name__ == "__main__":
    # Khởi tạo Circuit Breaker cho client shippingClient
    breaker = CircuitBreaker("shippingClient")
    
    print("=========================================================================")
    print("           MÔ PHỎNG VÒNG ĐỜI PHỤC HỒI TỰ ĐỘNG CỦA SHIPPING-SERVICE")
    print("=========================================================================")
    print(f"Khởi tạo ban đầu thành công. Trạng thái Circuit Breaker: {breaker.state.value}\n")
    
    # 1. Ép mạch chuyển sang OPEN
    breaker.force_open()
    print(f"Trạng thái Circuit Breaker hiện tại: {breaker.state.value}")
    
    # 2. Đợi đủ 20 giây xem trạng thái có tự động chuyển sang HALF_OPEN không (Sử dụng Automatic Transition)
    print("\n--- [CHỜ ĐỢI 20 GIÂY SLA] ---")
    for i in range(1, 22):
        time.sleep(1)
        print(f"  > Đã trôi qua {i:02d} giây... Trạng thái hiện tại: {breaker.state.value}")
        if breaker.state == State.HALF_OPEN and i >= 20:
            break

    # Đợi thêm 0.5s để log hiển thị đồng bộ đẹp đẽ
    time.sleep(0.5)
    
    # 3. Khi đang ở HALF_OPEN, bắn cùng lúc 10 requests
    print("\n--- [TESTING HALF-OPEN] BẮN CÙNG LÚC 10 REQUESTS SONG SONG ---")
    print(f"Trạng thái hiện tại khi bắt đầu bắn: {breaker.state.value}")
    print(f"Dự kiến chỉ có ĐÚNG {breaker.permitted_number_of_calls_in_half_open_state} request được cho phép, 7 còn lại bị Reject.\n")
    
    results = []
    
    def request_worker(order_id):
        try:
            res = breaker.execute(calculate_shipping_fee, order_id)
            return (order_id, "SUCCESS", res)
        except CallNotPermittedException as ex:
            return (order_id, "REJECTED", str(ex))
        except Exception as ex:
            return (order_id, "ERROR", str(ex))

    # Sử dụng ThreadPoolExecutor để bắn song song 10 requests cùng 1 thời điểm
    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="GHN_Thread") as executor:
        futures = [executor.submit(request_worker, f"DH-{1000 + i}") for i in range(10)]
        for fut in as_completed(futures):
            results.append(fut.result())
            
    # Sắp xếp kết quả theo ID đơn hàng tăng dần để dễ quan sát
    results.sort(key=lambda x: x[0])
    
    print("\n--- KẾT QUẢ THỰC THI 10 REQUESTS ĐỒNG THỜI ---")
    successes = 0
    rejections = 0
    for order_id, status, msg in results:
        print(f"[{status}] Đơn hàng {order_id}: {msg}")
        if status == "SUCCESS":
            successes += 1
        elif status == "REJECTED":
            rejections += 1
            
    print(f"\n[Thống kê]: Thành công = {successes}/10 (Yêu cầu: 3) | Bị từ chối = {rejections}/10 (Yêu cầu: 7)")
    
    # 4. Kiểm tra trạng thái cuối cùng sau khi 3 request thử nghiệm đều thành công
    print(f"Trạng thái cuối cùng của Circuit Breaker: {breaker.state.value}")
    
    # 5. Gửi thêm một request mồi mới khi trạng thái đã về CLOSED để kiểm chứng dòng chảy bình thường
    print("\n--- GỬI THỬ REQUEST MỚI KHI MẠCH ĐÃ TRỞ VỀ CLOSED ---")
    try:
        res = breaker.execute(calculate_shipping_fee, "DH-9999")
        print(f"[SUCCESS] {res}")
    except Exception as e:
        print(f"[FAILED] Thất bại khi gửi request: {e}")
        
    # Dọn dẹp tài nguyên
    breaker.shutdown()
    print("\n=========================================================================")
    print("                      HOÀN THÀNH KỊCH BẢN THỬ LỬA SUCCESS")
    print("=========================================================================")
