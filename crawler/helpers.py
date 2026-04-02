"""
helpers.py
==========
Các hàm tiện ích dùng chung cho toàn bộ project:
  - parse_count         : chuyển chuỗi số TikTok ("541.7K") thành int
  - parse_tiktok_date   : parse ngày từ UI TikTok ("2-17", "2020-2-20")
  - parse_relative_time : parse thời gian tương đối tiếng Việt ("3 ngày trước")
  - is_within_range     : kiểm tra datetime có nằm trong DATE_FROM..DATE_TO
  - is_within_days      : kiểm tra datetime trong N ngày gần nhất
  - normalize_url       : bỏ query string khỏi URL
  - extract_video_id    : lấy video ID từ URL TikTok
  - make_comment_id     : tạo composite hash nếu không có ID ổn định
  - make_reply_id       : tạo composite hash cho reply
  - human_sleep         : dừng với delay ngẫu nhiên hoặc cố định
  - safe_text           : lấy text element Selenium an toàn
  - safe_click          : click element Selenium an toàn bằng JavaScript
  - extract_create_time_from_snowflake : decode timestamp từ TikTok Snowflake ID
"""

import re
import hashlib
import time
import random
from datetime import datetime, timedelta

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ===========================================================================
# KHOẢNG THỜI GIAN LỌC VIDEO (fallback tĩnh — dùng is_within_days thay thế)
# ===========================================================================
DATE_FROM = "2025-12-01"
DATE_TO   = "2026-03-01"

_DATE_FROM_DT = datetime.strptime(DATE_FROM, "%Y-%m-%d")
_DATE_TO_DT   = datetime.strptime(DATE_TO, "%Y-%m-%d").replace(
    hour=23, minute=59, second=59
)


def is_within_range(dt: datetime | None) -> bool:
    """Giữ lại API cũ, dùng DATE_FROM..DATE_TO tĩnh."""
    if dt is None:
        return True
    return _DATE_FROM_DT <= dt <= _DATE_TO_DT


def is_within_days(dt: datetime | None, days: int) -> bool:
    """
    Kiểm tra datetime có nằm trong [now - days, now] hay không.
    - Nếu days <= 0 hoặc dt is None → luôn True (không giới hạn).
    """
    if dt is None or days <= 0:
        return True
    now = datetime.now()
    return dt >= now - timedelta(days=days)


# ===========================================================================
# PARSE SỐ TIKTOK
# ===========================================================================

def parse_count(text: str) -> int | None:
    """
    Chuyển chuỗi số kiểu TikTok thành số nguyên.
      '541.7K' -> 541700
      '2M'     -> 2000000
      '1699'   -> 1699
    """
    if not text:
        return None
    t = text.strip().replace(",", "").upper()
    t = re.sub(r"\s+", "", t)
    m = re.match(r"^(\d+(?:\.\d+)?)([KM]?)$", t)
    if not m:
        digits = re.sub(r"[^\d]", "", t)
        return int(digits) if digits else None
    num  = float(m.group(1))
    mult = {"K": 1_000, "M": 1_000_000}.get(m.group(2), 1)
    return int(num * mult)


# ===========================================================================
# PARSE NGÀY THÁNG
# ===========================================================================

def parse_tiktok_date(date_str: str) -> datetime | None:
    """
    Parse chuỗi ngày TikTok từ UI thành datetime.
      '2020-2-20' -> datetime(2020, 2, 20)
      '2-17'      -> datetime(năm hiện tại, 2, 17)
    """
    if not date_str:
        return None
    date_str = date_str.strip()

    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", date_str)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None

    m = re.match(r"^(\d{1,2})-(\d{1,2})$", date_str)
    if m:
        try:
            return datetime(datetime.now().year, int(m.group(1)), int(m.group(2)))
        except ValueError:
            return None

    return None


def parse_relative_time(text: str) -> datetime | None:
    """
    Parse thời gian tương đối tiếng Việt.
      '3 ngày trước' -> datetime.now() - 3 ngày
    """
    try:
        text = text.strip().lower()
        m = re.match(
            r"^(\d+)\s+(phut|gio|ngay|tuan|thang|nam)\s+truoc"
            r"|^(\d+)\s+(phút|giờ|ngày|tuần|tháng|năm)\s+trước",
            text,
        )
        if not m:
            return None
        num  = int(m.group(1) or m.group(3))
        unit = (m.group(2) or m.group(4) or "").lower()
        now  = datetime.now()
        unit_map = {
            "phút": timedelta(minutes=num), "phut": timedelta(minutes=num),
            "giờ":  timedelta(hours=num),   "gio":  timedelta(hours=num),
            "ngày": timedelta(days=num),    "ngay": timedelta(days=num),
            "tuần": timedelta(weeks=num),   "tuan": timedelta(weeks=num),
            "tháng":timedelta(days=num*30), "thang":timedelta(days=num*30),
            "năm":  timedelta(days=num*365),"nam":  timedelta(days=num*365),
        }
        return now - unit_map.get(unit, timedelta(0))
    except Exception:
        return None


# ===========================================================================
# URL UTILITIES
# ===========================================================================

def normalize_url(url: str | None) -> str | None:
    """Bỏ query string khỏi URL để chuẩn hóa."""
    return url.split("?")[0] if url else None


def extract_video_id(url: str | None) -> str | None:
    """Lấy video ID từ URL TikTok. '/video/123' -> '123'"""
    if not url:
        return None
    m = re.search(r"/video/(\d+)", url)
    return m.group(1) if m else None


# ===========================================================================
# COMPOSITE HASH
# ===========================================================================

def make_comment_id(video_id: str, comment_time: str, text: str) -> str:
    """Tạo comment ID từ hash nếu không có ID từ TikTok."""
    raw = f"{video_id}_{comment_time}_{(text or '')[:50]}"
    return hashlib.md5(raw.encode()).hexdigest()


def make_reply_id(comment_id: str, reply_time: str, text: str) -> str:
    """Tạo reply ID từ hash nếu không có ID từ TikTok."""
    raw = f"{comment_id}_{reply_time}_{(text or '')[:50]}"
    return hashlib.md5(raw.encode()).hexdigest()


# ===========================================================================
# SELENIUM HELPERS
# ===========================================================================

def human_sleep(min_s: float, max_s: float | None = None):
    """
    Dừng theo số giây cho trước.
      - 1 tham số: ngủ đúng số giây đó.
      - 2 tham số: ngủ ngẫu nhiên trong [min_s, max_s].
    """
    if max_s is None:
        time.sleep(min_s)
    else:
        time.sleep(random.uniform(min_s, max_s))


def safe_text(driver, by, value, timeout: int = 5) -> str | None:
    """Lấy text của element an toàn. Trả về None nếu không tìm thấy."""
    try:
        el = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        return el.text.strip() or None
    except Exception:
        return None


def safe_click(driver, by, value, timeout: int = 8) -> bool:
    """Click element an toàn bằng JavaScript. Trả về True nếu thành công."""
    try:
        el = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((by, value))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
        human_sleep(0.2, 0.4)
        driver.execute_script("arguments[0].click();", el)
        return True
    except Exception:
        return False


def extract_create_time_from_snowflake(video_id: str | None) -> datetime | None:
    """Decode timestamp từ TikTok Snowflake ID."""
    try:
        if not video_id or not str(video_id).isdigit():
            return None
        video_id_int = int(video_id)
        timestamp_s  = video_id_int >> 32
        if timestamp_s <= 0:
            return None
        return datetime.fromtimestamp(timestamp_s)
    except Exception:
        return None