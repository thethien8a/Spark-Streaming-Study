# Tiến độ và bàn giao

## 1. Trạng thái hiện tại

- **Chặng hiện tại:** 0 — chưa bắt đầu.
- **Đã hoàn thành:** làm rõ yêu cầu cá nhân hóa, chọn định hướng project và ghi tài liệu bàn giao.
- **Chưa hoàn thành:** hợp đồng dữ liệu chi tiết, môi trường, code, fixture thực thi và mọi bài đánh giá.
- **Bằng chứng hiện có:** người học tự mô tả nền tảng và xác nhận các lựa chọn trong cuộc trò chuyện; chưa có bằng chứng năng lực từ thực hành.
- **Trạng thái môi trường:** Windows/RAM 16 GB/12 cores/Docker/WSL2 do người học cung cấp; chưa kiểm tra tài nguyên thực tế hoặc phiên bản.
- **Dịch vụ project đang chạy:** chưa có dịch vụ nào được khởi chạy trong quá trình soạn tài liệu; kiểm tra lại đầu buổi thực hành, không suy ra trạng thái toàn máy.
- **Thay đổi có sẵn cần giữ:** `.gitignore` tồn tại trước khi tạo bộ tài liệu, không tự ghi đè.

## 2. Hành động tiếp theo cho AI mới

1. Đọc hồ sơ, quy tắc hướng dẫn và phần chặng 0; không hỏi lại các yêu cầu đã thống nhất.
2. Nêu mục tiêu buổi đầu: thống nhất thế nào là dữ liệu và kết quả đúng, sau đó mới smoke test môi trường.
3. Đưa bộ sự kiện nhỏ ở `PROJECT.md` để người học phân biệt event/order/payment và dự đoán kết quả. Hỏi một vấn đề mỗi lượt; không đưa đáp án ngay nếu đang đánh giá.
4. Chốt hoặc chỉnh quy tắc nghiệp vụ đề xuất, bổ sung timestamp và kỳ vọng cụ thể.
5. Khi chuyển sang cài đặt, kiểm tra môi trường/phiên bản/tài nguyên và quyền truy cập công cụ. Chỉ dựng phần nhỏ đủ cho bài đầu.

Không cần tạo toàn bộ producer, các streaming job, sink và monitoring trong một lượt.

## 3. Theo dõi qua chặng

Các trạng thái dùng: **chưa bắt đầu**, **đang học**, **chờ kiểm chứng**, **cần củng cố**, **đạt**.

| Chặng | Trạng thái | Giải thích | Thí nghiệm | Biến thể | Khoảng trống |
| --- | --- | --- | --- | --- | --- |
| 0 — Dữ liệu/môi trường | Chưa bắt đầu | Chưa đánh giá | Chưa chạy | Chưa đánh giá | Chưa chốt hợp đồng và stack |
| 1 — Mô hình streaming | Chưa bắt đầu | Chưa đánh giá | Chưa chạy | Chưa đánh giá | Chưa đánh giá |
| 2 — Kafka/schema | Chưa bắt đầu | Chưa đánh giá | Chưa chạy | Chưa đánh giá | Chưa đánh giá |
| 3 — Thời gian/state | Chưa bắt đầu | Chưa đánh giá | Chưa chạy | Chưa đánh giá | Chưa đánh giá |
| 4 — Trùng/ghi an toàn | Chưa bắt đầu | Chưa đánh giá | Chưa chạy | Chưa đánh giá | Chưa đánh giá |
| 5 — Join/đối soát | Chưa bắt đầu | Chưa đánh giá | Chưa chạy | Chưa đánh giá | Chưa đánh giá |
| 6 — Khôi phục/replay | Chưa bắt đầu | Chưa đánh giá | Chưa chạy | Chưa đánh giá | Chưa đánh giá |
| 7 — Vận hành/nghiệm thu | Chưa bắt đầu | Chưa đánh giá | Chưa chạy | Chưa đánh giá | Chưa đánh giá |

Khi điền bằng chứng, dùng đường dẫn đến fixture, lệnh, kết quả hoặc ghi chép câu trả lời; không thay mọi ô bằng “OK”. Không xóa kết quả chưa đạt; ghi thêm kết quả đánh giá lại để giữ lý do tiến bộ.

## 4. Quyết định đã thống nhất

| Quyết định | Lý do / giới hạn |
| --- | --- |
| Học Python/PySpark, tập trung Structured Streaming | Tận dụng nền tảng hiện tại; không dành riêng phần nhập môn Python/SQL |
| Project đơn hàng/thanh toán mô phỏng | Có kết quả nghiệp vụ kiểm chứng được và nhiều tình huống streaming thực tế |
| Local, miễn phí | Phù hợp điều kiện người học; không chứng minh năng lực production |
| Hiểu ý tưởng và code, AI hỗ trợ triển khai | Không đánh giá ghi nhớ syntax; vẫn phải dự đoán, giải thích và kiểm chứng |
| Độ sâu thực dụng, đào sâu khi cần | Ưu tiên áp dụng công việc hơn nghiên cứu mã nguồn hoặc phỏng vấn |
| 3 giờ/ngày, tiến theo tiêu chí | Không có deadline ép chuyển chặng |

## 5. Điểm chưa chốt và trở ngại

- Quy tắc/schema trong `PROJECT.md` vẫn là đề xuất.
- Chưa chọn phiên bản Spark/Python/Java/Kafka/connector hoặc PostgreSQL/driver.
- Chưa đo tài nguyên Docker/WSL2 thực tế, chưa chọn giới hạn tài nguyên hay đường dẫn mount.
- Chưa có fixture chạy được, lệnh khởi động hoặc cơ chế lưu bằng chứng thực nghiệm.
- Chưa có lỗi kỹ thuật được quan sát; “chưa thiết lập” không phải “đã thử và thất bại”.

## 6. Mẫu nhật ký buổi học

Sao chép mục này xuống phần nhật ký khi có một buổi thực tế; không điền kết quả giả lập. Ngày giờ dùng múi giờ Asia/Bangkok (UTC+7).

### Buổi (ngày giờ) — (chặng / mục tiêu)

- **Trạng thái đầu buổi:**
- **Câu hỏi và dự đoán của người học:**
- **Giải thích của người học / phần chưa chắc:**
- **Fixture và kết quả kỳ vọng độc lập:**
- **Môi trường, phiên bản, shell và lệnh đã chạy:**
- **Đầu ra thực tế / đường dẫn bằng chứng:**
- **Biến thể đã thử và kết quả:**
- **Chẩn đoán, thay đổi và lý do:**
- **Kết luận đánh giá:** đạt / cần củng cố / chưa đủ bằng chứng; ghi theo tiêu chí cụ thể.
- **File đã thay đổi / trạng thái Git nếu có:**
- **Dịch vụ còn chạy, checkpoint/dữ liệu cần giữ:**
- **Điểm còn mở và một hành động tiếp theo:**

## 7. Mẫu ghi quyết định mới

- **Ngày / chặng:**
- **Vấn đề:**
- **Trạng thái:** đề xuất / đã thống nhất / đã triển khai / đã kiểm chứng.
- **Phương án chọn và lý do:**
- **Phương án không chọn và trade-off:**
- **Bằng chứng hoặc kiểm chứng còn thiếu:**
- **Tài liệu/triển khai cần cập nhật:**

## 8. Nhật ký thực tế

Chưa có buổi thực hành nào được ghi nhận.
