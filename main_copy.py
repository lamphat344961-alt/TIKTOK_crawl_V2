"""
main.py
=======
Orchestrator cho pipeline:
Mongo (creators list) -> Selenium profile/video -> SQL Server -> comment API -> SQL Server
"""
from datetime import datetime
from dry_run_db import DryRunDB

import os
from typing import Any

import pymongo
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import config
from crawler.comment_crawler import CommentCrawler
from crawler.creator_crawler import CreatorCrawler
from crawler.video_crawler import VideoCrawler
from db.db_manager import DBManager
from helpers import human_sleep, is_within_days


def build_driver():
    """
    Build Firefox driver.
    Dùng fallback an toàn nếu config.py chưa có biến riêng cho Firefox.
    """
    geckodriver_path = getattr(config, "FIREFOX_DRIVER_PATH", None) or os.getenv("GECKODRIVER_PATH") or "geckodriver1.exe"
    firefox_binary = getattr(config, "FIREFOX_BINARY_PATH", None) or os.getenv("FIREFOX_BINARY")

    options = FirefoxOptions()
    if firefox_binary:
        options.binary_location = firefox_binary

    # Giảm dấu vết automation ở mức Selenium cho Firefox
    options.set_preference("dom.webdriver.enabled", False)
    options.set_preference("media.peerconnection.enabled", False)
    options.set_preference("permissions.default.image", 2)
    options.set_preference("network.http.use-cache", False)

    driver = webdriver.Firefox(
        service=FirefoxService(geckodriver_path),
        options=options,
    )
    driver.set_window_size(1400, 1000)
    return driver


def load_creator_docs() -> list[dict[str, Any]]:
    """
    Lấy creator input từ MongoDB nguồn.
    Chỉ đọc các field mà bạn đã cung cấp.
    """
    client = pymongo.MongoClient(config.MONGO_URI)
    try:
        db = client[config.MONGO_SRC_DB]
        col = db[config.MONGO_SRC_COLLECTION]

        fields_to_get = {
            "_id": 0,
            "ID": 1,
            "Name": 1,
            "Country": 1,
            "Followers": 1,
            "Engagement": 1,
            "Median Views": 1,
            "Start Price": 1,
            "Broadcast Score": 1,
            "Collab Score": 1,
            "Tags": 1,
        }
        docs = list(col.find({}, fields_to_get))
    finally:
        client.close()

    cleaned = []
    missing_id = 0
    for doc in docs:
        creator_id = str(doc.get("ID") or "").strip()
        if not creator_id:
            missing_id += 1
            continue
        doc["ID"] = creator_id
        cleaned.append(doc)

    print(f"[mongo] Lấy được {len(cleaned)} creators")
    if missing_id:
        print(f"[mongo] Bỏ qua {missing_id} documents thiếu field ID")
    return cleaned


def open_creator_profile(driver, creator_id: str):
    url = f"https://www.tiktok.com/@{creator_id}"
    print(f"\n[main] Mở profile: {url}")
    driver.get(url)
    human_sleep(*config.DELAY_WARMUP)


def open_first_video_from_profile(driver) -> bool:
    """
    Mở video đầu tiên trên profile.
    Thử nhiều selector nhưng đều chỉ dựa trên link /video/ hoặc card user-post-item.
    """
    selectors = [
        (By.CSS_SELECTOR, 'div[data-e2e="user-post-item"] a[href*="/video/"]'),
        (By.CSS_SELECTOR, 'a[href*="/video/"]'),
        (By.XPATH, '//div[contains(@data-e2e, "user-post-item")]'),
    ]

    for by, value in selectors:
        try:
            el = WebDriverWait(driver, 12).until(EC.element_to_be_clickable((by, value)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
            human_sleep(0.4, 0.8)
            driver.execute_script("arguments[0].click();", el)
            human_sleep(*config.DELAY_NEXT_VIDEO)
            return True
        except Exception:
            continue
    return False


def main():
    print("TikTok Crawler starting...")
    started_at = datetime.now()
    driver = None
    sql_db = None
    try:
        driver = build_driver()
        DRY_RUN = True   # True = không ghi DB, chỉ print để benchmark

        if DRY_RUN:
            sql_db = DryRunDB()
        else:
            sql_db = DBManager()
            if sql_db.conn is None:
                raise RuntimeError("Không kết nối được SQL Server")

        comment_crawler = CommentCrawler(sql_db)
        creator_crawler = CreatorCrawler(driver)
        video_crawler = VideoCrawler(driver)

        creators = load_creator_docs()
        if config.MAX_CREATORS:
            creators = creators[:config.MAX_CREATORS]

        print(f"[main] Crawl {len(creators)} creators")

        for idx_creator, source_doc in enumerate(creators, 1):
            creator_id = source_doc["ID"]
            print(f"\n{'=' * 70}")
            print(f"[main] Creator {idx_creator}/{len(creators)}: {creator_id}")
            print(f"{'=' * 70}")

            try:
                open_creator_profile(driver, creator_id)

                profile_stats = creator_crawler.extract_profile_stats()
                sql_db.upsert_creator(creator_id, source_doc, profile_stats)
                sql_db.sync_creator_tags(creator_id, source_doc.get("Tags"))

                if not open_first_video_from_profile(driver):
                    print(f"[main] Không mở được video đầu tiên của {creator_id}")
                    continue

                seen_video_ids: set[str] = set()
                video_count = 0
                skip_out_of_range = 0

                while video_count < config.MAX_VIDEOS_PER_CREATOR:
                    video_stats = video_crawler.extract_video_stats()
                    video_id = str(video_stats.get("video_id") or "").strip()
                    if not video_id:
                        print("[main] Không lấy được video_id, dừng creator")
                        break

                    if video_id in seen_video_ids:
                        print(f"[main] Video {video_id} đã gặp lại, dừng để tránh loop")
                        break
                    seen_video_ids.add(video_id)

                    create_time = video_stats.get("create_time")
                    if not is_within_days(create_time, config.CRAWL_DAYS_WINDOW):
                        skip_out_of_range += 1
                        print(f"[main] Video ngoài range: {video_id} (skip {skip_out_of_range}/{config.MAX_SKIP_OUT_OF_RANGE})")
                        if skip_out_of_range >= config.MAX_SKIP_OUT_OF_RANGE:
                            print(f"[main] Dừng {creator_id} vì quá nhiều video ngoài range")
                            break
                        if not video_crawler.click_next_video():
                            break
                        human_sleep(*config.DELAY_NEXT_VIDEO)
                        continue

                    skip_out_of_range = 0
                    sql_db.upsert_video(creator_id, video_stats)
                    comment_crawler.crawl(creator_id, video_id)

                    video_count += 1
                    print(f"[main] Đã crawl {video_count} video cho {creator_id}")

                    if config.REFRESH_SESSION_EVERY_N_VIDEOS and video_count % config.REFRESH_SESSION_EVERY_N_VIDEOS == 0:
                        comment_crawler.reset_session()

                    if not video_crawler.click_next_video():
                        print(f"[main] Hết video hoặc không click được next cho {creator_id}")
                        break
                    human_sleep(*config.DELAY_NEXT_VIDEO)

            except Exception as e:
                print(f"[main] Lỗi creator {creator_id}: {e}")

    finally:
        try:
            if sql_db:
                sql_db.close()
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    if sql_db and hasattr(sql_db, "print_summary"):
        sql_db.print_summary(started_at)                
if __name__ == "__main__":
    main()
