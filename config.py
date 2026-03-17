"""
config.py
=========
Toàn bộ cấu hình tập trung tại đây.
Chỉnh sửa file này, không cần đụng vào code logic.
"""

import os
from datetime import date, timedelta


# ===========================================================================
# FIREFOX DRIVER
# ===========================================================================
# Path đến Firefox profile đã đăng nhập TikTok.
# Ví dụ Windows:
#   r"C:\Users\Admin\AppData\Roaming\Mozilla\Firefox\Profiles\xxxxxxxx.default-release"
# Bỏ trống ("") nếu muốn dùng Firefox không profile (sẽ bị rate-limit nhanh hơn).
FIREFOX_PROFILE_PATH = r"C:\Users\Admin\AppData\Roaming\Mozilla\Firefox\Profiles\gmc89qu4.tiktok_crawler"

# Path đến geckodriver.exe (để None nếu đã có trong PATH)
GECKODRIVER_PATH = None

# True = chạy không hiển thị cửa sổ (headless mode)
FIREFOX_HEADLESS = False


# ===========================================================================
# SQL SERVER
# ===========================================================================
# Kết nối được đọc qua biến môi trường hoặc dùng giá trị mặc định dưới đây.
# Biến môi trường ưu tiên hơn (để không commit thông tin nhạy cảm vào git).
#
# Các biến môi trường hỗ trợ:
#   SQL_DRIVER    SQL_SERVER    SQL_DATABASE
#   SQL_SERVER_TRUSTED_CONNECTION (1/0)
#   SQL_USERNAME  SQL_PASSWORD
#
# Mặc định dùng Windows Authentication (Trusted_Connection=yes).

# ===========================================================================
# FALLBACK: danh sách creator để test nhanh (khi bảng CREATORS trong DB còn trống)
# ===========================================================================
# Khi bảng CREATORS chưa có dữ liệu, DBManager.load_creator_inputs() sẽ đọc list này.
# Để crawl production 3000 creators: import danh sách vào bảng CREATORS trước,
# rồi để TIKTOK_IDS = [] để tránh nhầm lẫn.
TIKTOK_IDS = [
    "lethikhanhhuyen2004",
    "_thuys.ngaan",
    "lalalalisa_m",
]


# ===========================================================================
# BỘ LỌC THỜI GIAN VIDEO
# ===========================================================================
CRAWL_END_DATE    = date.today()   # Ngày kết thúc (mặc định = hôm nay)
CRAWL_DAYS_WINDOW = 90             # Cửa sổ thời gian (ngày) — chỉ lấy video trong 90 ngày gần nhất

# Tính tự động — không cần chỉnh 2 dòng dưới
CRAWL_DATE_FROM = CRAWL_END_DATE - timedelta(days=CRAWL_DAYS_WINDOW)
CRAWL_DATE_TO   = CRAWL_END_DATE

# Ví dụ tùy chỉnh:
# CRAWL_END_DATE    = date(2026, 2, 28)
# CRAWL_DAYS_WINDOW = 60


# ===========================================================================
# GIỚI HẠN CRAWL
# ===========================================================================
MAX_SKIP_OUT_OF_RANGE = 5   # (dự phòng, không dùng trong luồng mới)

# MAX_CREATORS = None          → crawl tất cả
# MAX_CREATORS = 2             → chỉ crawl 2 creator đầu (dùng để test)
MAX_CREATORS           = 5
MAX_VIDEOS_PER_CREATOR = None     # hard cap số video mỗi creator (đặt None để không giới hạn)
MAX_COMMENTS_PER_VIDEO = 10000   # giới hạn comment mỗi video
MAX_REPLIES_PER_COMMENT = 5000   # giới hạn reply mỗi comment


# ===========================================================================
# CHECKPOINT / RESUME
# ===========================================================================
# True  = khi khởi động lại, bỏ qua creator đã có video trong DB
#         (tự động tiếp tục từ chỗ bị crash)
# False = luôn crawl lại từ đầu (dùng khi muốn update dữ liệu)
RESUME_SKIP_CRAWLED = True

# True  = so sánh username (string) thay vì numeric CREATOR_ID
#         (dùng khi bảng CREATORS chứa username thay vì ID số)
RESUME_SKIP_BY_USERNAME = False


# ===========================================================================
# TỐC ĐỘ — CHỐNG BỊ BLOCK
# ===========================================================================
DELAY_NEXT_VIDEO  = (1.0, 2.5)   # giây chờ sau khi xong 1 video (comment xong)
DELAY_WARMUP      = (3.0, 5.0)   # giây warm-up khi mở trang mới
DELAY_AFTER_CLICK = (0.4, 0.8)   # giây sau mỗi click

DELAY_API_REQUEST = (0.2, 0.5)   # giây nghỉ giữa các API call (comment/reply)

# --- Retry khi API lỗi ---
API_RETRY_TIMES   = 3             # số lần thử lại mỗi request
API_RETRY_BACKOFF = (5.0, 15.0)  # giây chờ giữa các lần retry

# --- Refresh session cookies định kỳ ---
# Sau mỗi N video, lấy cookies mới từ Firefox và build lại requests.Session
REFRESH_SESSION_EVERY_N_VIDEOS = 5

# --- Nghỉ dài định kỳ giữa các burst request ---
PAUSE_EVERY_N_REQUESTS = 30
PAUSE_DURATION         = (2.0, 5.0)

# --- Proxy (tùy chọn) ---
# None = không dùng proxy.
# Ví dụ: "http://user:pass@host:port"
PROXY_URL = os.getenv("TIKTOK_PROXY", None)


# ===========================================================================
# API TIKTOK
# ===========================================================================
API_COMMENT_LIST  = "https://www.tiktok.com/api/comment/list/"
API_REPLY_LIST    = "https://www.tiktok.com/api/comment/list/reply/"
API_COMMENT_COUNT = 20   # số comment mỗi page (max TikTok cho phép)
API_REPLY_COUNT   = 20   # số reply mỗi page


# ===========================================================================
# GIỮ LẠI ĐỂ BACKWARD COMPAT (không còn dùng trong luồng mới)
# ===========================================================================
MONGO_URI            = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_SRC_DB         = "tiktok_creators_db"
MONGO_SRC_COLLECTION = "creators_9k"
MONGO_DST_DB         = "tiktok_crawl_db"
COL_CREATORS         = "creators"
COL_VIDEOS           = "videos"
COL_COMMENTS         = "comments"
COL_REPLIES          = "replies"