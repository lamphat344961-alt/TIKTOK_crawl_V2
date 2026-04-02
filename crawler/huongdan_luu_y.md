# Hướng dẫn vận hành dự án — Thành viên mới

**Đề tài:** Clustering Vietnamese TikTok Creators: A Comparative Analysis of Unsupervised Machine Learning Models on Multidimensional Data

---

## Tổng quan

```
MongoDB (9,966 creators)
    → [Bước 1] db/TikTok_Creator_updated_schema.sql   — tạo database SQL Server
    → [Bước 2] Import JSON vào MongoDB                — import file part được phân công
    → [Bước 3] mongo_to_sql.py                        — đưa creators sang SQL Server
    → [Bước 4] main.py                                — crawl TikTok → lưu SQL
    → [Bước 5] sampling/samplin.ipynb                 — pipeline, clustering, báo cáo
```

> **Sampling đã hoàn tất.** 3,000 creators được chia thành 3 phần, mỗi phần 1,000 creators:
> - `part1` — đang được crawl bởi thành viên khác
> - `part2`, `part3` — dành cho thành viên mới

---

## Cấu trúc dự án

```
TIKTOK_V2/
├── main.py                                              # Orchestrator chính — chạy để crawl
├── config.py                                            # Toàn bộ cấu hình
├── mongo_to_sql.py                                      # Chuyển MongoDB → SQL Server
├── helpers.py
├── crawler/
│   ├── profile_feed_crawler.py                          # Crawl video từ trang profile
│   └── comment_crawler.py                               # Crawl comment + reply qua API
├── db/
│   ├── db_manager.py                                    # Kết nối và thao tác SQL Server
│   ├── TikTok_Creator_updated_schema.sql                # Script tạo database 
│   └── mapping_database.sql
└── sampling/
    ├── samplin.ipynb                                    # Notebook phân tích & sampling
    ├── sample_direction1_category_stratified_part1.json  ← đang dùng
    ├── sample_direction1_category_stratified_part2.json  ← Khánh dùng
    └── sample_direction1_category_stratified_part3.json  ← Bình dùng
```

---

## Bước 0 — Cấu hình ban đầu

Chỉnh thông tin kết nối trong **3 file** trước khi làm bất cứ điều gì:

### `config.py`
- `FIREFOX_PROFILE_PATH` — đường dẫn Firefox profile đã đăng nhập TikTok
- `GECKODRIVER_PATH` — đường dẫn geckodriver (để `None` nếu đã có trong PATH)
- `CRAWL_END_DATE`, `CRAWL_DAYS_WINDOW` — khoảng thời gian lấy video (mặc định 90 ngày gần nhất)

### `db/db_manager.py`
- Thông tin kết nối SQL Server: driver, server, database, username, password
- Hoặc set biến môi trường: `SQL_DRIVER`, `SQL_SERVER`, `SQL_DATABASE`, `SQL_USERNAME`, `SQL_PASSWORD`

### `mongo_to_sql.py`
- `MONGO_URI` — URI kết nối MongoDB
- `MONGO_DB` — `tiktok_ads_db`
- `MONGO_COLLECTION` — tên collection part được phân công (xem Bước 2)
- Thông tin SQL phải khớp với `db_manager.py`

---

## Bước 1 — Tạo database SQL Server

> Làm **một lần duy nhất** trên máy của bạn.

Mở SQL Server Management Studio (SSMS), chạy file:

```
db/TikTok_Creator_updated_schema.sql
```

File này tạo toàn bộ database, bảng, index cần thiết. Kiểm tra xong mới sang bước tiếp.

---

## Bước 2 — Import file JSON vào MongoDB

Import file part được phân công vào MongoDB:

```bash
# Nếu được phân công part2
mongoimport --uri "mongodb://localhost:27017" --db tiktok_ads_db \
  --collection sample_direction1_category_stratified_part2 \
  --file "sampling/sample_direction1_category_stratified_part2.json" --jsonArray

# Nếu được phân công part3
mongoimport --uri "mongodb://localhost:27017" --db tiktok_ads_db \
  --collection sample_direction1_category_stratified_part3 \
  --file "sampling/sample_direction1_category_stratified_part3.json" --jsonArray
```

Kiểm tra sau khi import — phải thấy đúng 1,000 documents:

```bash
mongosh
use tiktok_ads_db
db.sample_direction1_category_stratified_part2.countDocuments()
```

---

## Bước 3 — Chuyển creators sang SQL

Chỉnh `MONGO_COLLECTION` trong `mongo_to_sql.py` về đúng part của bạn:

```python
# mongo_to_sql.py
MONGO_COLLECTION = "sample_direction1_category_stratified_part2"  # hoặc part3
```

Chạy:

```bash
python mongo_to_sql.py
```

Kiểm tra trong SQL — phải thấy đúng 1,000 dòng với `CRAWL_STATUS = 'pending'`:

```sql
SELECT CRAWL_STATUS, COUNT(*) AS so_luong
FROM CREATORS
GROUP BY CRAWL_STATUS
```

---

## Bước 4 — Chạy hệ thống crawl

```bash
python main.py
```

### Quy trình mỗi creator

**1. Firefox tự động mở trang TikTok của creator**

Nhìn vào browser và xử lý theo tình huống:

| Tình huống | Hành động |
|------------|-----------|
| Trang hiển thị bình thường, thấy video | Nhấn Enter ngay |
| Trang trắng / bị chặn / hiện captcha | Vượt captcha → đợi trang hiển thị bình thường → nhấn Enter |

**2. Nhấn Enter trong terminal**

Hệ thống tự động thực hiện:
- Reload trang để bắt response đầu tiên (~20 video)
- Crawl toàn bộ video trong khoảng thời gian cấu hình
- Crawl comment + reply cho từng video
- Lưu vào SQL Server
- Xóa cookies → chuyển sang creator tiếp theo

> Sau mỗi creator, hệ thống tự xóa cookies. TikTok sẽ hiện captcha rõ ràng ở lần tiếp theo thay vì chặn ngầm — đảm bảo bắt đủ data từ đầu.

**3. Lặp lại cho creator tiếp theo**

### Ví dụ 1 phiên crawl

```
Terminal                                     Browser
─────────────────────────────────────────────────────────────
python main.py
[main] Creator 1/1000: username_abc
[NAV] Đang mở trang...            →   Firefox mở @username_abc
[NAV] Nhấn Enter khi sẵn sàng     →   Nhìn browser:
                                        ✓ Thấy video → nhấn Enter ngay
                                        ✗ Captcha → vượt xong → nhấn Enter
                           ← Enter ←
[profile_feed] Round 01: +20 items
[profile_feed] Round 02: +20 items
[comment] Crawl video: 123456...
[main] @username_abc → done ✓
[main] Đã xóa cookies

[main] Creator 2/1000: username_xyz
[NAV] Đang mở trang...            →   TikTok hiện captcha (do xóa cookies)
                                        → vượt captcha → nhấn Enter
                           ← Enter ←
...
─────────────────────────────────────────────────────────────
```

### Resume nếu bị crash

Chạy lại `python main.py` — hệ thống tự bỏ qua creator đã `done`, tiếp tục từ `in_progress` hoặc `pending`.

---

## Bước 5 — Theo dõi tiến độ

```sql
SELECT CRAWL_STATUS, COUNT(*) AS so_luong
FROM CREATORS
GROUP BY CRAWL_STATUS
ORDER BY so_luong DESC
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
| 3 | Clustering & Comparative Analysis (K-Means, DBSCAN, GMM, Hierarchical) | ⏳ Sau mục tiêu 2 |

---

*Cập nhật lần cuối: 03/2026*