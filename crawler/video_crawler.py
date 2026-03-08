"""
crawler/video_crawler.py — Crawl thông tin từng video
======================================================
Lấy: video_id, create_time, view/like/comment/save counts
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from .base_crawler import BaseCrawler
from helpers import (
    parse_count, safe_text, safe_click,
    normalize_url, extract_video_id,
    extract_create_time_from_snowflake, parse_tiktok_date, parse_relative_time,
)


class VideoCrawler(BaseCrawler):
    """Crawl thống kê của từng video TikTok."""

    def extract_create_time_from_ui(self, video_id: str | None = None):
        """
        Lấy ngày đăng video.
        Ưu tiên decode từ Snowflake ID (chính xác đến ms).
        Fallback về UI span (chỉ chính xác đến ngày).

        UI span nằm trong: span[data-e2e="browser-nickname"] → span cuối cùng
        """
        # Ưu tiên Snowflake — không cần UI, không bị ảnh hưởng bởi layout
        if video_id:
            dt = extract_create_time_from_snowflake(video_id)
            if dt:
                return dt

        # Fallback về UI
        try:
            nick_el = WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR, 'span[data-e2e="browser-nickname"]'
                ))
            )
            spans = nick_el.find_elements(By.TAG_NAME, "span")
            for span in reversed(spans):
                text = span.text.strip()
                if not text or text == "•":
                    continue
                # Thử parse ngày tuyệt đối: "2-17", "2020-2-20"
                dt = parse_tiktok_date(text)
                if dt:
                    return dt
                # Thử parse ngày tương đối: "3 ngày trước"
                dt = parse_relative_time(text)

                if dt:
                    return dt
        except Exception:
            pass

        return None

    def extract_video_stats(self) -> dict:
        """
        Lấy toàn bộ thống kê của video đang hiển thị trong player.

        Các trường:
          - video_url, video_id, create_time
          - view_count, like_count, comment_count, save_count
        """
        print("[video] Đang lấy thông tin video...")

        video_url_text = safe_text(
            self.driver, By.CSS_SELECTOR,
            'p[data-e2e="browse-video-link"]', timeout=10
        )
        video_url   = normalize_url(video_url_text)
        video_id    = extract_video_id(video_url)
        create_time = self.extract_create_time_from_ui(video_id)

        like_text    = safe_text(self.driver, By.CSS_SELECTOR, 'strong[data-e2e="browse-like-count"]',    timeout=6)
        comment_text = safe_text(self.driver, By.CSS_SELECTOR, 'strong[data-e2e="browse-comment-count"]', timeout=6)
        save_text    = safe_text(self.driver, By.CSS_SELECTOR, 'strong[data-e2e="undefined-count"]',      timeout=4)

        # View count — TikTok đặt ở nhiều nơi tuỳ layout, thử lần lượt
        view_text = None
        for css in [
            'strong[data-e2e="browse-view-count"]',
            'strong[data-e2e="video-views"]',
            'span[data-e2e="browse-view-count"]',
        ]:
            view_text = safe_text(self.driver, By.CSS_SELECTOR, css, timeout=2)
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
        """Click nút mũi tên phải để sang video tiếp theo. Trả về False nếu không còn."""
        return safe_click(
            self.driver, By.XPATH, "//button[@data-e2e='arrow-right']", timeout=10
        )
