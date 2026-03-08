"""
base_crawler.py
===============
Base class cung cấp các tiện ích Selenium dùng chung cho tất cả crawlers:
  - human_sleep  : dừng ngẫu nhiên để giống người dùng thật
  - safe_text    : lấy text element an toàn
  - safe_click   : click element an toàn bằng JavaScript
"""

import time
import random

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# Các selector dùng chung cho comment crawler (copy từ truycap_tt.py)
VIRTUAL_ITEM_XPATH = '//div[contains(@class,"DivVirtualItemContainer")]'
REAL_ITEM_XPATH    = '//div[contains(@class,"DivCommentItemContainer")]'
TOP_COMMENT_XPATH  = './div[contains(@class,"DivCommentContentContainer") and @id]'
REPLY_CONTENT_XPATH = './/div[contains(@class,"DivCommentContentContainer") and @id]'
VIEW_MORE_BTN_CSS  = '[data-e2e="view-more-1"], [data-e2e="view-more-2"]'
HIDE_BTN_CSS = (
    '[data-e2e="comment-hide"], [data-e2e="hide-1"], [data-e2e="hide-2"], '
    '[data-e2e="comment-hide-1"], [data-e2e="comment-hide-2"]'
)


class BaseCrawler:
    """Base class cho tất cả crawlers — cung cấp driver và các hàm tiện ích."""

    def __init__(self, driver):
        self.driver = driver
        self.wait   = WebDriverWait(driver, 30)

    # ------------------------------------------------------------------
    # TIỆN ÍCH THỜI GIAN
    # ------------------------------------------------------------------

    def human_sleep(self, delay: float):
        """Dừng đúng 'delay' giây."""
        time.sleep(delay)

    def random_sleep(self, min_s: float, max_s: float):
        """Dừng ngẫu nhiên trong khoảng [min_s, max_s] giây."""
        time.sleep(random.uniform(min_s, max_s))

    # ------------------------------------------------------------------
    # TIỆN ÍCH SELENIUM
    # ------------------------------------------------------------------

    def safe_text(self, by, value, timeout: int = 5) -> str | None:
        """Lấy text của element. Trả về None nếu không tìm thấy hoặc timeout."""
        try:
            el = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return el.text.strip() or None
        except Exception:
            return None

    def safe_click(self, by, value, timeout: int = 8) -> bool:
        """
        Click element an toàn bằng JavaScript.
        Trả về True nếu click thành công, False nếu không tìm thấy.
        """
        try:
            el = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((by, value))
            )
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", el
            )
            self.random_sleep(0.2, 0.4)
            self.driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # TIỆN ÍCH CHO COMMENT SCROLLER
    # ------------------------------------------------------------------

    def find_comment_scroll_container(self):
        """
        Tìm container scroll chính của khung comment.
        Thuật toán giống truycap_tt.find_comment_scroll_container:
          - Tìm 1 virtual item
          - Leo lên cha cho tới khi gặp element có scrollHeight >> clientHeight
          - Nếu không tìm được thì fallback body
        """
        from selenium.common.exceptions import TimeoutException

        try:
            first_item = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, VIRTUAL_ITEM_XPATH))
            )
        except TimeoutException:
            try:
                return self.driver.find_element(By.TAG_NAME, "body")
            except Exception:
                return None

        el = first_item
        for _ in range(15):
            try:
                parent = self.driver.execute_script(
                    "return arguments[0].parentElement;", el
                )
                if parent is None:
                    break
                sh = self.driver.execute_script(
                    "return arguments[0].scrollHeight;", parent
                )
                ch = self.driver.execute_script(
                    "return arguments[0].clientHeight;", parent
                )
                if sh and ch and sh > ch + 30:
                    return parent
                el = parent
            except Exception:
                break

        try:
            return self.driver.find_element(By.TAG_NAME, "body")
        except Exception:
            return None

    def get_visible_cids_in_dom_order(self) -> list[str]:
        """
        Lấy danh sách comment_id (cid) đang có trong DOM theo đúng thứ tự hiển thị.
        Thuật toán giống truycap_tt.get_visible_cids_in_dom_order.
        """
        cids: list[str] = []
        items = self.driver.find_elements(By.XPATH, REAL_ITEM_XPATH)
        for item in items:
            try:
                top = item.find_element(By.XPATH, TOP_COMMENT_XPATH)
                cid = top.get_attribute("id")
                if cid:
                    cids.append(cid)
            except Exception:
                continue
        return cids

    def get_item_by_cid(self, cid: str):
        """
        Tìm lại 1 item comment theo cid.
        CID nằm trên DivCommentContentContainer, cần leo lên DivCommentItemContainer.
        """
        xpath = (
            f'//div[contains(@class,"DivCommentContentContainer") and @id="{cid}"]'
            f'/ancestor::div[contains(@class,"DivCommentItemContainer")][1]'
        )
        return self.driver.find_element(By.XPATH, xpath)

    def scroll_comment_panel(self, container, pixels: int):
        """Scroll container comment xuống thêm `pixels`."""
        if container is not None:
            try:
                self.driver.execute_script(
                    "arguments[0].scrollTop += arguments[1];",
                    container,
                    pixels,
                )
                return
            except Exception:
                pass
        # Fallback: scroll toàn bộ window
        self.driver.execute_script("window.scrollBy(0, arguments[0]);", pixels)

    def wait_for_dom_stable(self, timeout: float = 2.0):
        """
        Chờ DOM ổn định tương đối như truycap_tt.wait_for_dom_stable:
          - Poll số REAL_ITEM_XPATH trong DOM, đợi đến khi đếm ổn định vài lần.
        """
        deadline = time.time() + timeout
        last_count = -1
        stable_ticks = 0

        while time.time() < deadline:
            try:
                items = self.driver.find_elements(By.XPATH, REAL_ITEM_XPATH)
                c = len(items)
                if c == last_count and c > 0:
                    stable_ticks += 1
                else:
                    stable_ticks = 0
                last_count = c
                if stable_ticks >= 2:
                    return True
            except Exception:
                pass
            time.sleep(0.25)

        return False
