"""
base_crawler.py — Base class cho tất cả crawlers.
"""

import time
import random
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from config import DELAY_AFTER_CLICK


class BaseCrawler:
    def __init__(self, driver):
        self.driver = driver
        self.wait   = WebDriverWait(driver, 30)

    def human_sleep(self, delay: float):
        time.sleep(delay)

    def random_sleep(self, min_s: float, max_s: float):
        time.sleep(random.uniform(min_s, max_s))

    def safe_text(self, by, value, timeout: int = 5) -> str | None:
        try:
            el = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return el.text.strip() or None
        except Exception:
            return None

    def safe_click(self, by, value, timeout: int = 8) -> bool:
        try:
            el = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((by, value))
            )
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            self.random_sleep(*DELAY_AFTER_CLICK)
            self.driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False
