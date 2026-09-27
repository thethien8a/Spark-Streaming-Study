# Spark Streaming Study — Điểm bắt đầu

## Mục đích

Bộ tài liệu này lưu yêu cầu cá nhân hóa, cách hướng dẫn, phạm vi project và tiến độ để một AI khác có thể tiếp tục mà không cần đọc cuộc trò chuyện ban đầu.

Đây là **kế hoạch học và đặc tả để triển khai dần**, không phải tài liệu mô tả một hệ thống đã chạy. Chưa có phiên bản công nghệ, cấu hình tài nguyên hay kết quả thực nghiệm nào được xác nhận.

## Đọc tài liệu theo thứ tự

| Tài liệu | Nội dung |
| --- | --- |
| [AI_GUIDE.md](AI_GUIDE.md) | Hồ sơ người học, mục tiêu, phương pháp hướng dẫn, cách đánh giá và quy tắc bàn giao |
| [PROGRESS.md](PROGRESS.md) | Chặng hiện tại, bằng chứng đã có, câu hỏi còn mở và hành động tiếp theo |
| [ROADMAP.md](ROADMAP.md) | Các chặng 0–7, bài thực hành, tiêu chí đạt và lỗi hiểu sai cần phát hiện |
| [PROJECT.md](PROJECT.md) | Kiến trúc dự kiến, nghiệp vụ, dữ liệu, cách kiểm chứng, giới hạn phạm vi và nguồn tham khảo |

AI mới phải đọc hồ sơ và tiến độ trước khi đề xuất công việc. Không bắt đầu lại phần làm rõ yêu cầu đã được ghi nhận; chỉ hỏi những điểm chưa chốt hoặc có thông tin mới mâu thuẫn.

## Cách bắt đầu một cuộc trò chuyện với AI khác

Có thể dùng nguyên văn yêu cầu sau:

> Hãy đọc AGENTS.md và docs/README.md, rồi đọc tài liệu được chỉ dẫn, đặc biệt là docs/AI_GUIDE.md và docs/PROGRESS.md. Tiếp tục hướng dẫn tôi từ trạng thái thực tế được ghi nhận. Tôi cần hiểu thiết kế, ý nghĩa code và cách kiểm chứng Spark Structured Streaming; không cần học thuộc syntax. Đừng xây toàn bộ project một lần hoặc tự coi tôi đã đạt tiêu chí. Hãy nêu ngắn gọn tôi đang ở đâu, mục tiêu buổi này và bắt đầu bằng một câu hỏi hoặc thí nghiệm phù hợp.

Nếu AI không có quyền đọc repository, cung cấp cả năm file trong `docs`, không chỉ riêng lộ trình. Nếu thiếu tiến độ, AI phải xác minh trạng thái thay vì đoán.

## Quy tắc duy trì tài liệu

- Yêu cầu người học: nguồn sự thật ở `AI_GUIDE.md`.
- Trạng thái và quyết định: nguồn sự thật ở `PROGRESS.md`.
- Tiêu chí học: nguồn sự thật ở `ROADMAP.md`.
- Hợp đồng dữ liệu và kiến trúc: nguồn sự thật ở `PROJECT.md`.
- Khi đổi quyết định, cập nhật tài liệu sở hữu nội dung và ghi lý do tại `PROGRESS.md`; không để hai quy tắc mâu thuẫn cùng có hiệu lực.
- Phân biệt rõ **đã thống nhất**, **đề xuất**, **đã triển khai**, **đã kiểm chứng**. Không nâng trạng thái chỉ vì AI đã viết tài liệu hoặc code.

## Trạng thái khởi đầu

Đã thống nhất yêu cầu học và định hướng project. Chưa bắt đầu chặng 0, chưa dựng môi trường, chưa chạy pipeline và chưa đánh giá năng lực qua thí nghiệm. Chi tiết cập nhật luôn nằm trong [PROGRESS.md](PROGRESS.md).
