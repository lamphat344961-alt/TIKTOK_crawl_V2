"""
crawler/creator_crawler.py — Crawl thông tin profile creator
=============================================================
Lấy: followers_count, total_likes từ trang profile TikTok
"""
from selenium.webdriver.common.by import By

from ..crawler.base_crawler import BaseCrawler
from helpers import parse_count, human_sleep, safe_text


class CreatorCrawler(BaseCrawler):
    """Crawl thông tin profile của 1 creator TikTok."""

    def extract_profile_stats(self) -> dict:
        """
        Lấy số Followers và tổng Likes trên trang profile.
        Cuộn nhẹ xuống trước để trigger lazy-load của TikTok.

        Selectors đã xác nhận:
          <strong data-e2e="followers-count">2M</strong>
          <strong data-e2e="likes-count">138.5M</strong>
        """
        print("[profile] Đang lấy thông tin profile...")

        # Cuộn nhẹ để trigger lazy-load
        self.driver.execute_script("window.scrollTo(0, 400);")
        human_sleep(1.0, 1.5)

        followers_text = safe_text(
            self.driver,
            By.CSS_SELECTOR,
            'strong[data-e2e="followers-count"]',
            timeout=15,
        )
        likes_text = safe_text(
            self.driver,
            By.CSS_SELECTOR,
            'strong[data-e2e="likes-count"]',
            timeout=15,
        )

        result = {
            "followers_count_raw": followers_text,
            "total_likes_raw":     likes_text,
            "followers_count":     parse_count(followers_text),
            "total_likes":         parse_count(likes_text),
        }
        print(f"[profile] followers={followers_text} | likes={likes_text}")
        return result
