"""
config.py
=========
Toàn bộ cấu hình tập trung tại đây.
Chỉnh sửa file này, không cần đụng vào code logic.
"""

import os
from datetime import date, timedelta


# ===========================================================================
# MONGODB
# ===========================================================================
MONGO_URI     = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# DB nguồn — chứa danh sách creators cần crawl
MONGO_SRC_DB         = "tiktok_creators_db"
MONGO_SRC_COLLECTION = "creators_9k"

# DB đích — lưu kết quả crawl
MONGO_DST_DB = "tiktok_crawl_db"

# Collections trong DB đích
COL_CREATORS = "creators"   # profile stats
COL_VIDEOS   = "videos"     # video stats
COL_COMMENTS = "comments"   # top-level comments (raw API)
COL_REPLIES  = "replies"    # replies (raw API)


# ===========================================================================
# BỘ LỌC THỜI GIAN VIDEO
# ===========================================================================
CRAWL_END_DATE    = date.today()   # Ngày kết thúc (mặc định = hôm nay)
CRAWL_DAYS_WINDOW = 90             # Cửa sổ thời gian (ngày)

# Tính tự động — không cần chỉnh 2 dòng dưới
CRAWL_DATE_FROM = CRAWL_END_DATE - timedelta(days=CRAWL_DAYS_WINDOW)
CRAWL_DATE_TO   = CRAWL_END_DATE

# Ví dụ tùy chỉnh:
# CRAWL_END_DATE    = date(2026, 2, 28)
# CRAWL_DAYS_WINDOW = 60


# ===========================================================================
# GIỚI HẠN CRAWL
# ===========================================================================
MAX_CREATORS          = 20  # None = crawl tất cả, số nguyên = giới hạn
MAX_VIDEOS_PER_CREATOR = 200   # hard cap số video mỗi creator
MAX_SKIP_OUT_OF_RANGE  = 7     # bỏ qua liên tiếp bao nhiêu video ngoài range thì dừng creator
MAX_COMMENTS_PER_VIDEO = 500   # giới hạn comment mỗi video
MAX_REPLIES_PER_COMMENT = 200  # giới hạn reply mỗi comment


# ===========================================================================
# TỐC ĐỘ — CHỐNG BỊ BLOCK
# ===========================================================================
DELAY_API_REQUEST  = (0.8, 1.5)   # giây nghỉ giữa các API call
DELAY_NEXT_VIDEO   = (2.0, 3.5)   # giây chờ sau khi chuyển video
DELAY_WARMUP       = (4.0, 6.0)   # giây warm-up khi mở trang mới
DELAY_AFTER_CLICK  = (0.4, 0.8)   # giây sau mỗi click

# --- Retry khi API lỗi (rate limit / tạm chặn) ---
API_RETRY_TIMES    = 3            # số lần thử lại mỗi request
API_RETRY_BACKOFF  = (5.0, 15.0)  # giây chờ giữa mỗi lần retry (random)

# --- Refresh session (cookies) định kỳ ---
# Sau mỗi N video crawl comment, build lại session từ Selenium (cookies mới)
# None = không refresh; số nguyên = refresh sau mỗi N video
REFRESH_SESSION_EVERY_N_VIDEOS = 5

# --- Nghỉ dài giữa các burst (giảm pattern) ---
# Sau mỗi N request API liên tiếp, nghỉ thêm một khoảng dài hơn (giây)
PAUSE_EVERY_N_REQUESTS = 20
PAUSE_DURATION         = (10.0, 25.0)

# --- Proxy (tùy chọn) ---
# Đặt PROXY_URL = None nếu không dùng proxy.
# Ví dụ: "http://user:pass@host:port" hoặc "http://host:port"
PROXY_URL = os.getenv("TIKTOK_PROXY", None)


# ===========================================================================
# CHROME DRIVER
# ===========================================================================
CHROME_DRIVER_PATH = r"chromedriver.exe"   # đặt cùng thư mục với main.py
CHROME_USER_DATA   = r"C:\Users\Admin\AppData\Local\Google\Chrome\User Data"
CHROME_PROFILE     = "Default"             # tên profile đang đăng nhập TikTok


# ===========================================================================
# API TIKTOK
# ===========================================================================
API_COMMENT_LIST  = "https://www.tiktok.com/api/comment/list/"
API_REPLY_LIST    = "https://www.tiktok.com/api/comment/list/reply/"
API_COMMENT_COUNT = 20   # số comment mỗi page
API_REPLY_COUNT   = 20   # số reply mỗi page