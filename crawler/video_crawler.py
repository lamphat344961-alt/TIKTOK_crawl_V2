"""
crawler/video_crawler.py
========================
Crawl thống kê video TikTok — id, create_time, view/like/comment/save counts.
Dùng method của BaseCrawler (không import Selenium helpers từ helpers.py).
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from crawler.base_crawler import BaseCrawler
from helpers import (
    parse_count,
    normalize_url,
    extract_video_id,
    extract_create_time_from_snowflake,
    parse_tiktok_date,
    parse_relative_time,
)


class VideoCrawler(BaseCrawler):
    """Crawl thống kê của từng video TikTok."""

    def extract_create_time(self, video_id: str | None = None):
        """
        Lấy ngày đăng video.
        Ưu tiên decode từ Snowflake ID (chính xác đến ms).
        Fallback về UI span (chính xác đến ngày).
        """
        # 1. Snowflake — không cần UI
        if video_id:
            dt = extract_create_time_from_snowflake(video_id)
            if dt:
                return dt

        # 2. Fallback: UI span[data-e2e="browser-nickname"] → span cuối
        try:
            nick_el = WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR, 'span[data-e2e="browser-nickname"]'
                ))
            )
            for span in reversed(nick_el.find_elements(By.TAG_NAME, "span")):
                text = span.text.strip()
                if not text or text == "•":
                    continue
                dt = parse_tiktok_date(text)
                if dt:
                    return dt
                dt = parse_relative_time(text)
                if dt:
                    return dt
        except Exception:
            pass

        return None

    def extract_video_stats(self) -> dict:
        """Lấy toàn bộ thống kê của video đang hiển thị trong player."""
        print("[video] Đang lấy thông tin video...")

        video_url_text = self.safe_text(
            By.CSS_SELECTOR, 'p[data-e2e="browse-video-link"]', timeout=10
        )
        video_url   = normalize_url(video_url_text)
        video_id    = extract_video_id(video_url)
        create_time = self.extract_create_time(video_id)

        like_text    = self.safe_text(By.CSS_SELECTOR, 'strong[data-e2e="browse-like-count"]',    timeout=6)
        comment_text = self.safe_text(By.CSS_SELECTOR, 'strong[data-e2e="browse-comment-count"]', timeout=6)
        save_text    = self.safe_text(By.CSS_SELECTOR, 'strong[data-e2e="undefined-count"]',      timeout=4)

        # View count — thử nhiều selector vì TikTok thay đổi layout
        view_text = None
        for css in [
            'strong[data-e2e="browse-view-count"]',
            'strong[data-e2e="video-views"]',
            'span[data-e2e="browse-view-count"]',
        ]:
            view_text = self.safe_text(By.CSS_SELECTOR, css, timeout=2)
            if view_text:
                break

        result = {
            "video_url":         video_url,
            "video_id":          video_id,
            "create_time":       create_time,
            "view_count_raw":    view_text,
            "like_count_raw":    like_text,
            "comment_count_raw": comment_text,
            "save_count_raw":    save_text,
            "view_count":        parse_count(view_text),
            "like_count":        parse_count(like_text),
            "comment_count":     parse_count(comment_text),
            "save_count_ui":     parse_count(save_text),
        }
        print(
            f"[video] id={video_id} | create={create_time} | "
            f"views={view_text} | likes={like_text} | comments={comment_text}"
        )
        return result

    def click_next_video(self) -> bool:
        """Click nút mũi tên phải để sang video tiếp theo."""
        return self.safe_click(
            By.XPATH, "//button[@data-e2e='arrow-right']", timeout=10
        )
