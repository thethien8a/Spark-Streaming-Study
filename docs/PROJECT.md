# Đặc tả project đơn hàng và thanh toán

## 1. Phạm vi và trạng thái

**Đã thống nhất:** project mô phỏng đơn hàng/thanh toán, học PySpark Structured Streaming, chạy miễn phí trên máy cá nhân, triển khai tăng dần theo lộ trình.

**Đề xuất sẽ chốt dần:** hợp đồng event, quy tắc doanh thu/đối soát bên dưới, thông số window/watermark/join, phiên bản và cách đóng gói môi trường. Chặng 0 chốt phần nền tảng; các quyết định chuyên sâu theo mục 7. Đây chưa phải hệ thống thanh toán thực tế và không được dùng cho giao dịch thật.

Đầu ra học tập dự kiến:
- Dữ liệu hợp lệ và bản ghi không hợp lệ có lý do để kiểm tra.
- Doanh thu từ thanh toán thành công theo cửa sổ event time.
- Kết quả ghép đơn/thanh toán và các trường hợp chưa khớp theo quy tắc đã chốt.
- Bằng chứng thử trùng lặp, dữ liệu muộn, restart và replay.
- Hướng dẫn chạy lại và xử lý sự cố đủ để người khác tiếp tục.

## 2. Kiến trúc dự kiến

```text
Bộ tạo sự kiện Python
        |
        v
Kafka: orders + payments
        |
        v
PySpark Structured Streaming
  - Parse / kiểm tra dữ liệu
  - Tổng hợp / dedup / đối soát theo từng bài
        |
        v
File để quan sát ban đầu; PostgreSQL khi học sink có ghi lại an toàn

Checkpoint riêng theo query, lưu ở vị trí bền vững qua restart container.
Dữ liệu không hợp lệ đi một nhánh riêng có thể kiểm tra.
```

Không bắt buộc nối toàn bộ stateful operation trong một query. Chọn các query riêng khi cần để phù hợp giới hạn API, dễ kiểm chứng và giữ bài học rõ ràng. Mỗi query có checkpoint riêng, không dùng chung thư mục.

Môi trường khởi đầu dự kiến: một Kafka broker, Spark local mode, storage local được mount bền vững; PostgreSQL chỉ thêm khi đến bài sink. Không dựng nhiều Spark worker để giả lập quy mô production trên máy RAM 16 GB.

Console/memory sink chỉ dùng quan sát khi thích hợp; không dùng kết quả trên đó để tuyên bố bảo đảm đầu ra production.

## 3. Hợp đồng dữ liệu đề xuất

Event biểu diễn sự việc đã xảy ra, không phải lệnh yêu cầu trừ tiền. Hai loại ban đầu là tạo đơn và kết quả một lần thử thanh toán.

| Trường | Ý nghĩa đề xuất |
| --- | --- |
| `event_id` | Định danh sự kiện; bản sao do gửi lại cùng event phải giữ ID này |
| `event_type` | `order_created` hoặc `payment_result` |
| `schema_version` | Phiên bản hợp đồng dữ liệu |
| `event_time` | Thời điểm sự kiện nghiệp vụ xảy ra, timestamp UTC có múi giờ rõ ràng |
| `order_id` | Khóa đơn hàng dùng liên kết nghiệp vụ |
| `amount_vnd` | Số nguyên VND; không dùng số thực cho tiền trong phạm vi bài học |
| `payment_id` | Chỉ có ở payment: ID của một lần thử thanh toán |
| `status` | Chỉ có ở payment: `SUCCESS` hoặc `FAILED` |

Thời điểm gửi/đến Kafka, partition và offset được giữ làm metadata khi cần phân tích; không thay `event_time` bằng thời gian xử lý mà không thay đổi rõ định nghĩa nghiệp vụ.

Giới hạn đề xuất để bài toán ban đầu đủ nhỏ:
- Chỉ dùng VND, đơn có số tiền dương và đơn hàng không thay đổi sau khi tạo.
- Một lần thử thanh toán phát ra một kết quả cuối. Một đơn có thể có lần thất bại rồi một lần thành công; nhiều lần thành công khác ID là tình huống bất thường cần quy tắc riêng, không âm thầm bỏ qua.
- Bản sao cùng `event_id` phải có cùng nội dung; nội dung mâu thuẫn cần được nhận diện, không tùy tiện chọn một bản ghi.
- Chưa có hoàn tiền, thanh toán một phần, hủy đơn, đa tiền tệ hoặc cập nhật nhiều trạng thái của cùng payment.
- Doanh thu thanh toán ban đầu tính từ payment thành công theo `event_time` của payment. Đối soát với order là đầu ra riêng; thiếu order không tự làm biến mất doanh thu thanh toán.

AI cần giải thích ảnh hưởng của các lựa chọn và chốt chúng với người học ở chặng 0, không hỏi lại toàn bộ nhu cầu học.

## 4. Bộ dữ liệu khởi đầu để tính tay

Đề xuất một kịch bản, tất cả event ở trong cùng một cửa sổ thời gian và chưa có yếu tố dữ liệu muộn:

| Thứ tự gửi | Event | Nghiệp vụ | Số tiền VND | Kết quả |
| --- | --- | --- | ---: | --- |
| 1 | `o-1` | Tạo đơn `O1` | 120000 | — |
| 2 | `p-1` | Thanh toán `P1` cho `O1` | 120000 | SUCCESS |
| 3 | `p-1` | Gửi lại nguyên event ở dòng 2 | 120000 | SUCCESS |
| 4 | `o-2` | Tạo đơn `O2` | 75000 | — |
| 5 | `p-2` | Thanh toán `P2` cho `O2` | 75000 | FAILED |
| 6 | `p-3` | Thanh toán `P3` cho `O2` | 75000 | SUCCESS |
| 7 | `o-3` | Tạo đơn `O3` | 45000 | — |

Theo quy tắc đề xuất, sau khi xử lý trùng đúng: **doanh thu 195000 VND, hai đơn có thanh toán thành công, một đơn chưa quan sát được thanh toán thành công**. Không được gọi `O3` là hết hạn thanh toán chỉ vì bộ dữ liệu tạm thời chưa có payment.

Đáp án này là mục tiêu nghiệp vụ để kiểm chứng khi đã học xử lý trùng, không phải lời hứa query ngây thơ ở chặng đầu sẽ cho kết quả đúng. Nếu cộng cả bản sao `p-1`, doanh thu sẽ thành 315000 VND; đây là một phản ví dụ hữu ích.

Ở chặng 0 phải bổ sung timestamp cụ thể, schema đầy đủ và ranh giới cửa sổ vào fixture thực thi; bảng này chưa phải fixture chạy được. Khi thay quy tắc nghiệp vụ, tính lại kỳ vọng trước khi sửa pipeline.

## 5. Ma trận kiểm chứng cần phát triển dần

| Nhóm | Kịch bản cần có | Bằng chứng mong đợi |
| --- | --- | --- |
| Happy path | Dữ liệu không trùng, không muộn, số tiền không đồng đều | Kết quả từng bản ghi và tổng tiền khớp tính tay |
| Chất lượng | JSON hỏng, thiếu ID, timestamp sai | Bản ghi lỗi và lý do được lưu, không mất âm thầm |
| Trùng lặp | Cùng event gửi lại; batch được ghi lại | Đầu ra đúng trong phạm vi dedup và bảo đảm sink đã định nghĩa |
| Thời gian | Sát ranh giới window, sai thứ tự, trong/quá ngưỡng muộn | Giải thích bằng event time, watermark và state thực tế, không chỉ bằng giờ chạy |
| Join | Payment đến trước order, nhiều lần thử, một nguồn đứng yên | Không nhân sai doanh thu; không kết luận thiếu payment quá sớm |
| Khôi phục | Lỗi trước/sau ghi sink, restart cùng checkpoint | So sánh đầu ra với kỳ vọng; lưu vị trí lỗi và kết quả khôi phục |
| Replay | Query/checkpoint mới đọc dữ liệu cũ | Chính sách đầu ra rõ ràng, không cộng lại ngoài ý muốn |
| Tải | Tăng lượng event, hot key, sink chậm | Số liệu trước/sau trên cùng điều kiện, kết luận có giới hạn |

Kiểm tra bản ghi/khóa và trạng thái, không chỉ kiểm tra tổng số dòng hoặc tổng tiền vì các lỗi có thể bù trừ. Bộ sinh phải hỗ trợ kịch bản xác định và thứ tự gửi kiểm soát được trước khi thêm dữ liệu ngẫu nhiên.

Khi so sánh kết quả streaming với batch/tính tay, phải dùng cùng hợp đồng thời gian, dữ liệu được chấp nhận và phạm vi dedup. Không coi toàn bộ lịch sử là kỳ vọng cho một query chủ động giới hạn dữ liệu muộn.

## 6. Những bảo đảm cần phát biểu chính xác

- Exactly-once end-to-end phụ thuộc khả năng đọc lại nguồn, tiến độ/checkpoint và cách sink xử lý ghi lại. Có checkpoint không có nghĩa mọi side effect đều chỉ xảy ra một lần.
- `foreachBatch` mặc định at-least-once; muốn ghi lại không sai phải thiết kế cơ chế phù hợp ở sink. Khử trùng event và chống ghi lại batch là hai vấn đề khác nhau.
- Watermark có bảo đảm một chiều: không được diễn giải ngưỡng muộn thành điều kiện loại bỏ tuyệt đối, tức thời cho mọi event/operator.
- Stream-stream join cần điều kiện thời gian và watermark phù hợp để giới hạn state; outer join không phải bộ hẹn giờ wall clock.
- Spark quản lý tiến độ đọc Kafka bằng cơ chế riêng; không suy ra tiến độ query từ consumer-group commit thông thường.
- Checkpoint trên volume local giúp thử restart nhưng không mô phỏng được mất máy hoặc độ bền của storage phân tán.

## 7. Các quyết định cần ghi nhận trước khi triển khai phần tương ứng

Không cần chốt mọi mục ngay buổi đầu:
- Chặng 0: event/schema/nghiệp vụ, phiên bản, shell, vị trí dữ liệu, giới hạn tài nguyên.
- Chặng 3: loại/kích thước window, watermark và đánh đổi chấp nhận dữ liệu muộn.
- Chặng 4: khóa/phạm vi dedup, cơ chế ghi idempotent, identity của query/batch và chính sách replay.
- Chặng 5: khoảng thời gian join, ý nghĩa chưa khớp, cách xử lý nhiều payment.
- Chặng 6–7: giới hạn thay đổi query, runbook, chỉ số và tiêu chí đo tải phù hợp local.

Mỗi quyết định cần có lý do, phương án không chọn và thí nghiệm kiểm chứng nếu ảnh hưởng tính đúng. Ghi vào [PROGRESS.md](PROGRESS.md), cập nhật đặc tả này khi đã chốt.

## 8. Tài liệu tham khảo chính thức

- [Structured Streaming overview](https://spark.apache.org/docs/latest/streaming/): mô hình và phạm vi hệ thống.
- [Getting Started](https://spark.apache.org/docs/latest/streaming/getting-started.html): mô hình bảng và cơ chế xử lý.
- [DataFrame/Dataset streaming API](https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html): watermark, join, output mode, sink, foreachBatch và recovery.
- [Kafka integration — Spark 3.5.7](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html): nguồn tham khảo đã đọc về offset/restart và bảo đảm ghi Kafka; **không phải quyết định dùng Spark 3.5.7**.

Các trang `latest` thay đổi theo thời gian. Sau khi chọn phiên bản, thay/bổ sung liên kết tài liệu đúng phiên bản và xác minh API trước khi dùng. Không chọn phiên bản chỉ vì nó xuất hiện trong danh sách nguồn này.
