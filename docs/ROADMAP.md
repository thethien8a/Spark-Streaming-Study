# Lộ trình theo năng lực

## Cách dùng

Đi tuần tự từ chặng 0 đến 7; mỗi chặng có thể cần nhiều buổi. Không ấn định ngày hoàn thành. Các mục dưới đây là tiêu chí chưa được đánh giá, không phải thành tích đã có. Trạng thái thực tế nằm ở [PROGRESS.md](PROGRESS.md).

Mỗi lần đánh giá đều áp dụng ba yêu cầu trong [AI_GUIDE.md](AI_GUIDE.md): giải thích, kiểm chứng, xử lý biến thể. Phần mở rộng chỉ học khi phục vụ một câu hỏi hoặc hiện tượng đã xuất hiện.

## Chặng 0 — Hợp đồng dữ liệu và môi trường tối thiểu

**Câu hỏi:** dữ liệu nào đại diện cho một sự kiện, thế nào là kết quả đúng?

- Chốt quy tắc ở [PROJECT.md](PROJECT.md): sự kiện, định danh, thời gian, đơn vị tiền, cách tính doanh thu và phạm vi đối soát.
- Lập bộ dữ liệu nhỏ, tính tay kết quả trước khi viết pipeline.
- Xác minh môi trường; chọn và ghi phiên bản tương thích, shell chạy lệnh, vị trí lưu dữ liệu/checkpoint.
- Dựng từng thành phần đủ để một event Python → Kafka → Spark → đầu ra quan sát được. AI hỗ trợ code/cấu hình; chưa cần giải thích mọi cơ chế streaming.

**Đạt khi:** người học truy được một event qua các thành phần, phân biệt ID của event/order/payment, tính được kết quả mẫu và tái chạy smoke test theo hướng dẫn đã ghi.

**Biến thể:** thay số tiền và đổi thứ tự một vài event; kết quả mong đợi phải được cập nhật đúng trước khi chạy.

## Chặng 1 — Mô hình thực thi Structured Streaming

**Câu hỏi:** dữ liệu không có điểm kết thúc thì kết quả được cập nhật như thế nào?

- Unbounded table, result table, micro-batch, trigger và vòng đời query.
- Phân biệt transformation với việc khởi chạy query; stateless và stateful.
- Append, Update, Complete theo loại query/sink được phiên bản đã chọn hỗ trợ; không ép cùng một query hỗ trợ cả ba.
- Gửi dữ liệu thành nhiều đợt, quan sát từng batch và đầu ra.

**Đạt khi:** dự đoán đúng kết quả qua các đợt và giải thích khác biệt giữa event time, thời điểm xử lý và tần suất trigger.

**Biến thể:** thay trigger và giữ nguyên dữ liệu nghiệp vụ; phân biệt thay đổi thời điểm thấy kết quả với thay đổi kết quả cuối.

## Chặng 2 — Kafka, schema và chất lượng dữ liệu

**Câu hỏi:** làm sao biết đã đọc những gì và bản ghi không hợp lệ đi đâu?

- Ôn topic, partition, key, offset và phạm vi bảo đảm thứ tự; không giả định có thứ tự toàn cục giữa các partition.
- Parse JSON với schema rõ ràng; kiểm tra trường bắt buộc, timestamp, trạng thái và số tiền.
- Lưu dữ liệu không hợp lệ kèm lý do để kiểm tra, thay vì lọc mất âm thầm.
- Quan sát `startingOffsets` ở query mới và vị trí đọc khi resume bằng checkpoint.

**Đạt khi:** lần theo được dữ liệu hợp lệ/không hợp lệ và giải thích tại sao đổi `startingOffsets` không phải cách buộc một query đã có checkpoint đọc lại từ đầu.

**Biến thể:** JSON hỏng, thiếu ID, timestamp không hợp lệ và query restart. Đối chiếu đầu vào với các nhánh đầu ra, có xét quy tắc trùng đã chọn.

## Chặng 3 — Event time, window, watermark và state

**Câu hỏi:** khi nào một tổng hợp còn được sửa và khi nào có thể dọn trạng thái?

- Tumbling window trước, sliding window khi đã vững; ranh giới cửa sổ và múi giờ.
- Event đến sai thứ tự, watermark và state được giữ giữa các batch.
- Đánh đổi giữa chấp nhận dữ liệu muộn, độ trễ kết quả và lượng state.
- Theo dõi tiến triển event time/watermark qua từng batch, kể cả khi nguồn ngừng gửi dữ liệu.

**Đạt khi:** tính được cửa sổ của event, dự đoán những trường hợp được bảo đảm tiếp nhận và giải thích được đầu ra/state quan sát được.

**Biến thể:** event sát hai phía ranh giới cửa sổ, đến muộn trong ngưỡng, quá ngưỡng và event có timestamp nhảy xa về tương lai.

**Không được dạy sai:** watermark không phải đồng hồ đếm ngược theo wall clock; dữ liệu muộn quá ngưỡng không có nghĩa là luôn bị loại ngay. Muốn thử trường hợp state đã dọn phải kiểm soát tiến triển watermark/batch và quan sát bằng chứng.

## Chặng 4 — Trùng lặp và ghi lại không sai số liệu

**Câu hỏi:** cùng dữ liệu xuất hiện lần nữa thì tại sao doanh thu không tăng lần nữa?

- Phân biệt bản sao của một event, hai event cho cùng một nghiệp vụ và batch bị ghi lại sau lỗi.
- Chọn khóa và thời hạn dedup; không đồng nhất `event_id` với `order_id`.
- Sink có khóa duy nhất/upsert hoặc cơ chế giao dịch phù hợp; `foreachBatch` không tự cung cấp exactly-once cho mọi cách ghi.
- Giải thích phạm vi của `batchId` và nguy cơ khi query mới/checkpoint mới dùng lại số batch.

**Đạt khi:** phát lại event và thử ghi lại batch không làm sai kết quả trong phạm vi đã cam kết; người học chỉ được phần bảo vệ ở nguồn/query/sink và các giới hạn còn lại.

**Biến thể:** một lỗi xảy ra sau khi sink đã ghi nhưng trước khi batch hoàn tất; so sánh với lỗi trước khi ghi. Không chỉ thử dừng query lúc nhàn rỗi.

## Chặng 5 — Kết hợp đơn hàng và thanh toán

**Câu hỏi:** khi nào có thể kết luận một đơn chưa có thanh toán phù hợp?

- Stream-static để bổ sung dữ liệu tham chiếu, sau đó stream-stream.
- Khóa join, điều kiện thời gian, nhiều lần thử thanh toán và nguy cơ nhân bản dòng khi join.
- Watermark hai phía, state, inner/outer join và thời điểm có thể xuất kết quả chưa khớp.
- Phân biệt kết luận theo event time với yêu cầu cảnh báo đúng hạn theo wall clock.

**Đạt khi:** giải thích và kiểm chứng thanh toán đến trước đơn, đến muộn, nhiều lần thử và đơn chưa có thanh toán; chỉ ra điều kiện giúp giới hạn state.

**Biến thể:** một nguồn đứng yên trong khi nguồn kia vẫn chạy. Không hứa outer join sẽ phát cảnh báo đúng phút theo đồng hồ khi watermark không tiến triển.

## Chặng 6 — Khôi phục, replay và thay đổi query

**Câu hỏi:** khi query dừng giữa chừng, điều gì cho phép tiếp tục mà không làm sai dữ liệu?

- Vai trò của offset/progress, state và checkpoint; phạm vi lưu bền vững trên local volume.
- Resume cùng query/checkpoint khác với replay bằng query mới.
- Kafka retention và trường hợp dữ liệu cần đọc lại không còn ở nguồn.
- Đọc giới hạn tương thích checkpoint khi đổi schema/stateful operation theo đúng phiên bản.
- Viết quy trình restart và replay có cách bảo vệ dữ liệu đầu ra.

**Đạt khi:** thực hiện các kịch bản dừng/khởi động lại, so sánh kết quả với kỳ vọng và giải thích được điều gì đã/không được kiểm chứng. Biết khi nào phải lập kế hoạch migration/replay thay vì dùng checkpoint cũ.

**Biến thể:** sink tạm thời không truy cập được; checkpoint mới với dữ liệu cũ; đề xuất đổi phép tổng hợp đang giữ state. Kịch bản phá dữ liệu chỉ chạy trên tài nguyên thử nghiệm được phép xóa.

## Chặng 7 — Vận hành, hiệu năng và nghiệm thu

**Câu hỏi:** pipeline đúng nhưng chậm hoặc tích tụ state thì tìm nguyên nhân thế nào?

- Đọc query progress và Spark UI: thời gian batch, số dòng, tốc độ xử lý, state và watermark.
- Đo backlog bằng offset đầu nguồn so với tiến độ đọc của query; không mặc định consumer-group lag phản ánh Spark.
- Tăng tải có kiểm soát; thử hot key, sink chậm và thay đổi một cấu hình mỗi lần.
- Viết runbook ngắn: kiểm tra sức khỏe, chẩn đoán, restart, replay, giới hạn và dữ liệu cần bảo toàn.

**Đạt khi:** dùng số liệu xác định nút thắt, giải thích một điều chỉnh và so sánh trước/sau trên cùng điều kiện. Hoàn thành một kịch bản phối hợp lỗi và dữ liệu bất thường chưa làm mẫu.

**Nghiệm thu cuối:** trình bày kiến trúc, chạy bộ kiểm chứng nghiệp vụ, chứng minh phạm vi khôi phục/ghi an toàn, giải thích trade-off và giới hạn local. Không yêu cầu đạt một con số throughput tùy ý hoặc thuộc lệnh cấu hình.

## Nội dung không bắt buộc

RocksDB state store, custom stateful processing, schema evolution nâng cao, distributed cluster, CDC, lakehouse và dashboard chỉ được thêm nếu phục vụ nhu cầu đã thống nhất. Không biến project học streaming thành một khóa hạ tầng tổng hợp.
