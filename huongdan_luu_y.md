# Hướng dẫn vận hành dự án — Thành viên mới

**Đề tài:** Clustering Vietnamese TikTok Creators: A Comparative Analysis of Unsupervised Machine Learning Models on Multidimensional Data

---

## Tổng quan

```
MongoDB (9,966 creators)
    → [Bước 1] sampling.ipynb  — lấy mẫu 3,000 creators → lưu vào MongoDB collection mới
    → [Bước 2] mongo_to_sql.py — đưa 3,000 creators đó sang SQL Server
    → [Bước 3] main.py         — crawl TikTok (video + comment) → lưu vào SQL
    → [Bước 4] sampling.ipynb  — xây dựng pipeline, clustering, báo cáo
```

---

## Bước 0 — Cấu hình ban đầu

Trước khi làm bất cứ điều gì, chỉnh thông tin kết nối trong **3 file** sau:

### `config.py`
- `FIREFOX_PROFILE_PATH` — đường dẫn đến Firefox profile đã đăng nhập TikTok
- `GECKODRIVER_PATH` — đường dẫn geckodriver (để `None` nếu đã có trong PATH)
- `CRAWL_END_DATE`, `CRAWL_DAYS_WINDOW` — khoảng thời gian lấy video

### `db/db_manager.py`
- Thông tin kết nối SQL Server: driver, server, database, username, password
- Hoặc set biến môi trường tương ứng: `SQL_DRIVER`, `SQL_SERVER`, `SQL_DATABASE`, `SQL_USERNAME`, `SQL_PASSWORD`

### `mongo_to_sql.py` *(chỉnh 2 lần — xem Bước 1 và Bước 2)*
- `MONGO_URI` — URI kết nối MongoDB
- `MONGO_DB`, `MONGO_COLLECTION` — trỏ đến đúng collection tương ứng từng bước
- Thông tin SQL đích phải khớp với `db_manager.py`

---

## Bước 1 — Lấy mẫu 3,000 creators

> Chạy notebook `sampling/sampling.ipynb` ()

- Kết nối MongoDB, đọc toàn bộ 9,966 creators
- Lấy mẫu 3,000 creators — gợi ý stratified theo quy mô kênh (MICRO / MID / MACRO), tag nội dung, hoặc mức giá để mẫu đại diện
- Lưu kết quả ra file `.json`
- Tạo một **MongoDB collection mới** và import file `.json` vừa tạo vào đó

> **Sau bước này:** chỉnh `MONGO_COLLECTION` trong `mongo_to_sql.py` trỏ sang collection mới chứa 3,000 creators vừa sampling.

---

## Bước 2 — Chuyển 3,000 creators sang SQL

> Đảm bảo `mongo_to_sql.py` đang trỏ đúng vào collection 3,000 creators (đã chỉnh ở cuối Bước 1) dùng tiktok_ads_db.sample_direction1_category_stratified.json import vào mongo (chú ý đặt tên DB và collection cho giống mongo_to_sql )


```bash
python mongo_to_sql.py
```

- Script đọc 3,000 creators từ MongoDB → insert vào bảng `CREATORS` trong SQL
- Kiểm tra sau khi chạy: bảng `CREATORS` có đúng 3,000 dòng, cột `CRAWL_STATUS` toàn bộ là `pending`

---

## Bước 3 — Chạy hệ thống crawl

```bash
python main.py
```

Hệ thống crawl **tuần tự từng creator**, mỗi creator cần thao tác thủ công như sau:

```
1. Firefox tự động mở trang TikTok của creator
2. [THỦ CÔNG] Nhìn vào browser — vượt captcha nếu có
3. Trang hiển thị bình thường → quay lại terminal → nhấn Enter
4. Hệ thống tự reload trang, crawl video + comment, lưu vào SQL
5. Chuyển sang creator tiếp theo — lặp lại
```

**Lưu ý:**
- Không cần tự refresh trang — sau khi nhấn Enter, hệ thống tự reload để bắt data
- Nếu crash giữa chừng: chạy lại `python main.py`, hệ thống tự resume (bỏ qua creator có `CRAWL_STATUS = 'done'`, tiếp tục các creator `'in_progress'` hoặc `'error'`)

---

## Bước 4 — Theo dõi tiến độ

Truy vấn SQL để xem trạng thái crawl:

```sql
SELECT CRAWL_STATUS, COUNT(*) AS so_luong
FROM CREATORS
GROUP BY CRAWL_STATUS
```

| `CRAWL_STATUS` | Ý nghĩa |
|----------------|---------|
| `pending` | Chưa crawl |
| `in_progress` | Đang chạy hoặc bị crash giữa chừng |
| `done` | Crawl xong |
| `no_videos` | Không có video trong khoảng thời gian cấu hình |
| `private` | Tài khoản riêng tư |
| `not_found` | Tài khoản không tồn tại |
| `banned` | Tài khoản bị ban |

---

## Mục tiêu dự án

| # | Mục tiêu | Trạng thái |
|---|----------|------------|
| 1 | Crawl 3,000 creators mẫu → hình thành pipeline → báo cáo thầy | 🔄 Đang thực hiện |
| 2 | Crawl toàn bộ ~6,966 creators còn lại → hoàn thiện dataset | ⏳ Sau mục tiêu 1 |


---

## Cấu trúc file dự án

```
project/
├── main.py                       # Orchestrator chính — chạy file này để crawl
├── config.py                     # Toàn bộ cấu hình tập trung tại đây
├── mongo_to_sql.py               # Chuyển creators từ MongoDB → SQL Server
├── crawler/
│   ├── profile_feed_crawler.py   # Crawl video từ trang profile TikTok
│   └── comment_crawler.py        # Crawl comment + reply qua API TikTok
├── db/
│   └── db_manager.py             # Kết nối và thao tác SQL Server
└── sampling/
    └── sampling.ipynb            # Phân tích và Sampling creators
```

---

*Cập nhật lần cuối: 03/2026*