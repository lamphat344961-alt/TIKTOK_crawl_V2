"""
main.py
=======
Entry point chính của TikTok Scraper.

Stack:
  - Firefox (Selenium) : crawl profile stats + video stats + điều hướng
  - requests thuần     : crawl comment/reply qua TikTok API với cookies
  - SQL Server         : lưu creator/video stats (DBManager)
  - MongoDB            : lưu raw comment/reply (MongoCommentDB)

Cấu hình tập trung tại config.py — không hardcode ở đây.
"""

import os
import time
import random

import pymongo
from selenium import webdriver as se_webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import config
from helpers import is_within_days
from crawler.creator_crawler import CreatorCrawler
from crawler.video_crawler   import VideoCrawler
from crawler.comment_crawler import CommentCrawler
from db.db_manager           import DBManager
from db.mongo_comment_db     import MongoCommentDB


# ===========================================================================
# KHỞI TẠO FIREFOX DRIVER
# ===========================================================================

def create_driver():
    """Khởi tạo Firefox WebDriver. Không có vấn đề profile conflict như Chrome."""
    options = FirefoxOptions()
    options.set_preference("dom.webdriver.enabled", False)
    options.set_preference("useAutomationExtension", False)
    service = FirefoxService(GeckoDriverManager().install())
    return se_webdriver.Firefox(service=service, options=options)


# ===========================================================================
# LẤY CREATORS TỪ MONGODB
# ===========================================================================

def get_creators_from_mongo() -> list[str]:
    """Lấy toàn bộ _id (username) từ collection creators_9k."""
    try:
        client     = pymongo.MongoClient(config.MONGO_URI)
        db         = client[config.MONGO_SRC_DB]
        collection = db[config.MONGO_SRC_COLLECTION]
        ids        = [str(doc["_id"]) for doc in collection.find({}, {"_id": 1})]
        client.close()
        print(f"[mongo] Lấy được {len(ids)} creators")
        return ids
    except Exception as e:
        print(f"[mongo] Lỗi lấy creators: {e}")
        return []


# ===========================================================================
# CRAWL 1 CREATOR
# ===========================================================================

def run_one_creator(
    driver,
    creator_id: str,
    db: DBManager,
    comment_crawler: CommentCrawler,
) -> dict:
    """
    Crawl toàn bộ dữ liệu cho 1 creator.
      - Profile/Video stats → SQL Server (DBManager)
      - Comment/Reply raw   → MongoDB (MongoCommentDB, qua comment_crawler)
    """
    wait        = WebDriverWait(driver, 30)
    profile_url = f"https://www.tiktok.com/@{creator_id}/"
    results     = {"creator_id": creator_id, "profile_stats": {}, "videos": []}

    print(f"\n{'='*60}")
    print(f"[main] Crawl creator: {creator_id}")
    print(f"[main] DAYS_WINDOW: {config.CRAWL_DAYS_WINDOW}")
    print(f"{'='*60}")

    if db.creator_exists(creator_id):
        print(f"[main] Creator '{creator_id}' đã có trong DB → bỏ qua")
        return results

    # --- Mở trang profile ---
    driver.get(profile_url)
    driver.maximize_window()
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    print("[main] Warm-up...")
    time.sleep(random.uniform(*config.DELAY_WARMUP))
    driver.execute_script("window.scrollBy(0, 400);")
    time.sleep(1.5)
    driver.execute_script("window.scrollBy(0, 200);")
    time.sleep(1.0)

    # --- Profile stats → SQL ---
    creator_crawler = CreatorCrawler(driver)
    profile_stats   = creator_crawler.extract_profile_stats()
    results["profile_stats"] = profile_stats
    db.upsert_creator(creator_id, profile_stats)

    # --- Click video đầu tiên ---
    print("\n[main] Tìm video đầu tiên...")
    try:
        video_link = wait.until(EC.presence_of_element_located((
            By.XPATH,
            "//div[starts-with(@id,'grid-item-container')]//a[contains(@href,'/video/')]",
        )))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", video_link)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", video_link)
        time.sleep(random.uniform(*config.DELAY_NEXT_VIDEO))
    except Exception:
        print("[main] Không tìm thấy video → bỏ qua creator")
        return results

    # --- Vòng lặp video ---
    video_crawler = VideoCrawler(driver)
    skip_count    = 0

    for video_num in range(1, config.MAX_VIDEOS_PER_CREATOR + 1):
        print(f"\n[main] ── Video {video_num}/{config.MAX_VIDEOS_PER_CREATOR} ──")

        video_stats = video_crawler.extract_video_stats()
        create_time = video_stats.get("create_time")
        video_id    = video_stats.get("video_id")

        # --- Check date range ---
        if not is_within_days(create_time, config.CRAWL_DAYS_WINDOW):
            skip_count += 1
            print(f"[main] Ngoài window {config.CRAWL_DAYS_WINDOW} ngày "
                  f"→ bỏ qua ({skip_count}/{config.MAX_SKIP_OUT_OF_RANGE})")
            if skip_count >= config.MAX_SKIP_OUT_OF_RANGE:
                print("[main] Đạt giới hạn skip → dừng creator")
                break
            _click_next(driver, video_num)
            continue

        skip_count = 0

        # --- Check video đã crawl chưa ---
        if video_id and db.video_exists(video_id, creator_id):
            print(f"[main] Video '{video_id}' đã có trong DB → bỏ qua")
            _click_next(driver, video_num)
            continue

        # --- Upsert video → SQL ---
        db.upsert_video(creator_id, video_stats)

        # --- Crawl comments + replies → MongoDB ---
        if video_id:
            crawl_result = comment_crawler.crawl(creator_id, video_id)
            results["videos"].append({
                "stats":          video_stats,
                "total_comments": crawl_result["total_comments"],
                "total_replies":  crawl_result["total_replies"],
            })
            print(f"[main] Video {video_num}: "
                  f"{crawl_result['total_comments']} comments, "
                  f"{crawl_result['total_replies']} replies")
        else:
            print("[main] Không lấy được video_id → bỏ qua crawl comment")

        _click_next(driver, video_num)

    print(f"\n[main] Hoàn tất creator {creator_id}: {len(results['videos'])} videos")
    return results


def _click_next(driver, video_num: int):
    """Click sang video tiếp theo."""
    if video_num >= config.MAX_VIDEOS_PER_CREATOR:
        return
    print("[main] → Video tiếp theo...")
    try:
        btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@data-e2e='arrow-right']"))
        )
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(random.uniform(*config.DELAY_NEXT_VIDEO))
    except Exception:
        print("[main] Không còn video tiếp theo")


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    print("[main] TikTok Scraper khởi động (Firefox + requests)")
    print(f"[main] DAYS_WINDOW={config.CRAWL_DAYS_WINDOW}, "
          f"MAX_CREATORS={config.MAX_CREATORS}, "
          f"MAX_VIDEOS={config.MAX_VIDEOS_PER_CREATOR}")

    creator_ids = get_creators_from_mongo()
    if not creator_ids:
        print("[main] Không có creator nào → thoát")
        return

    total = len(creator_ids)
    if config.MAX_CREATORS:
        creator_ids = creator_ids[:config.MAX_CREATORS]
        print(f"[main] Crawl {len(creator_ids)}/{total} creators")
    else:
        print(f"[main] Crawl tất cả {total} creators")

    db              = DBManager()
    mongo_db        = MongoCommentDB()
    # 1 instance dùng chung toàn bộ run — không tạo lại mỗi video
    comment_crawler = CommentCrawler(mongo_db)
    driver          = create_driver()

    try:
        for idx, creator_id in enumerate(creator_ids, 1):
            print(f"\n[main] ===== Creator {idx}/{len(creator_ids)}: {creator_id} =====")
            try:
                run_one_creator(driver, creator_id, db, comment_crawler)
            except Exception as e:
                print(f"[main] Lỗi creator {creator_id}: {e} → bỏ qua")
                continue
    finally:
        driver.quit()
        db.close()
        mongo_db.close()
        print("\n[main] Đã đóng trình duyệt và database")


if __name__ == "__main__":
    main()
