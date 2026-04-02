"""
main_test.py
============
Chạy test crawl video TikTok profile feed:
- Giữ nguyên flow setup/capture của main.py gốc
- Không crawl comment
- Không crawl reply
- Chỉ lưu kết quả ra 1 file JSON
- Thêm seen_video_ids để tránh trùng video sau khi user tự vượt captcha bằng tay

[THÊM] Ghi dữ liệu vào SQL Server (CREATORS, VIDEOS, CREATOR_TAGS) sau mỗi creator.
[THÊM] Set CRAWL_STATUS = 'profile_done' để main_1_crawl_cmt.py biết cần crawl comment.
"""

from __future__ import annotations

import json
import time
import traceback
from datetime import datetime
from pathlib import Path

import crawler.config as config
from selenium import webdriver

from crawler.profile_feed_crawler import ProfileFeedCrawler, _wait_page
from crawler.db.db_manager import DBManager  # [THÊM]


OUTPUT_JSON = Path("video_test_output.json")


def build_driver():
    options = webdriver.FirefoxOptions()

    if getattr(config, "FIREFOX_HEADLESS", False):
        print("[main_test] FIREFOX_HEADLESS=True nhưng luồng hiện tại cần browser hiển thị.")
        print("[main_test] Tự động chuyển sang non-headless để hỗ trợ manual check.")
        # cố tình không set headless, giữ đúng logic main.py gốc

    firefox_profile_path = getattr(config, "FIREFOX_PROFILE_PATH", None)
    if firefox_profile_path:
        options.add_argument("-profile")
        options.add_argument(firefox_profile_path)

    geckodriver_path = getattr(config, "GECKODRIVER_PATH", None)

    if geckodriver_path:
        service = webdriver.FirefoxService(executable_path=geckodriver_path)
        driver = webdriver.Firefox(service=service, options=options)
    else:
        driver = webdriver.Firefox(options=options)

    driver.set_page_load_timeout(90)
    return driver


def navigate_and_wait_for_manual_check(driver, username: str):
    """
    Giữ nguyên flow manual-check của main.py gốc:
    1. Mở profile
    2. Cài hook JS để bắt item_list
    3. Cho user vượt captcha / refresh nếu cần
    4. Nhấn Enter
    5. Re-inject hook sau Enter
    6. Không clear buffer lần 2 để crawl() đọc được response đầu tiên
    """
    from crawler.profile_feed_crawler import _HOOK_JS, _CLEAR_JS

    url = f"https://www.tiktok.com/@{username}"

    try:
        driver.switch_to.window(driver.current_window_handle)
    except Exception:
        pass

    try:
        driver.maximize_window()
    except Exception:
        pass

    print(f"\n[MANUAL CHECK] Đang mở: {url}")

    try:
        driver.get(url)
    except Exception as e:
        print(f"[MANUAL CHECK] Lỗi load trang: {e}")

    # Cài hook lần đầu
    try:
        _wait_page(driver)
        hook_result = driver.execute_script(_HOOK_JS)
        driver.execute_script(_CLEAR_JS)
        print(f"[MANUAL CHECK] Hook installed: {hook_result}")
    except Exception as e:
        print(f"[MANUAL CHECK] Lỗi cài hook lần đầu: {e}")

    print("\n" + "=" * 90)
    print("[MANUAL CHECK] Nếu TikTok hiện captcha / verify / chặn, hãy xử lý tay trên browser.")
    print("[MANUAL CHECK] Nếu bạn vừa refresh trang để qua captcha thì cũng không sao.")
    print("[MANUAL CHECK] Khi profile đã load ổn và thấy video, quay lại terminal rồi nhấn Enter.")
    print("=" * 90)
    input()

    # Re-inject hook sau Enter vì user có thể đã refresh
    # KHÔNG clear buffer ở đây
    try:
        _wait_page(driver)
        hook_result = driver.execute_script(_HOOK_JS)
        print(f"[MANUAL CHECK] Hook re-injected sau Enter: {hook_result}")
    except Exception as e:
        print(f"[MANUAL CHECK] Lỗi re-inject hook: {e}")


def jsonable(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def main():
    driver = None
    output_rows = []
    seen_video_ids = set()

    try:
        driver = build_driver()
        db = DBManager()  # [THÊM]
        profile_crawler = ProfileFeedCrawler(driver)

        # [THÊM] Đọc danh sách creator từ bảng CREATORS thay vì config.TIKTOK_IDS
        creators = db.load_creator_inputs()

        if not creators:
            print("[main_test] Không có creator nào cần crawl (pending/in_progress/error).")
            return

        total = len(creators)
        print(f"[main_test] Tổng creator cần crawl: {total}")

        for idx, c in enumerate(creators, 1):
            username = (c.get("ID") or "").strip()
            if not username:
                print(f"[main_test] Bỏ qua creator dòng {idx}: ID rỗng")
                continue

            print("\n" + "=" * 90)
            print(f"[main_test] Creator {idx}/{total}: {username}")
            print("=" * 90)

            navigate_and_wait_for_manual_check(driver, username)

            try:
                result = profile_crawler.crawl(username, already_navigated=True)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"[main_test] Lỗi kỹ thuật khi crawl profile @{username}: {e}")
                traceback.print_exc()
                output_rows.append({
                    "creator_id": username,
                    "status": "technical_error",
                    "creator": None,
                    "videos": []
                })
                continue

            if result is None:
                print(f"[main_test] Crawl lỗi kỹ thuật cho @{username}")
                output_rows.append({
                    "creator_id": username,
                    "status": "technical_error",
                    "creator": None,
                    "videos": []
                })
                continue

            creator_dict, video_dicts = result

            fail_reason = creator_dict.get("_FAIL_REASON")
            if fail_reason:
                print(f"[main_test] @{username} -> {fail_reason}")
                output_rows.append({
                    "creator_id": username,
                    "status": fail_reason,
                    "creator": creator_dict,
                    "videos": []
                })
                # [THÊM] Ghi trạng thái đặc biệt vào DB
                try:
                    db.set_crawl_status(username, fail_reason)
                except Exception as e:
                    print(f"[main_test] Lỗi set_crawl_status {fail_reason} @{username}: {e}")
                try:
                    driver.delete_all_cookies()
                    print(f"[main_test] Đã xóa cookies sau @{username}")
                except Exception as e:
                    print(f"[main_test] Lỗi xóa cookies: {e}")
                continue

            unique_videos = []
            duplicate_count = 0

            for vidx, video in enumerate(video_dicts, 1):
                video_id = str(video.get("VIDEO_ID") or "").strip()
                if not video_id:
                    print(f"[main_test] Bỏ qua video rỗng id của @{username}")
                    continue

                if video_id in seen_video_ids:
                    duplicate_count += 1
                    continue

                seen_video_ids.add(video_id)
                unique_videos.append(video)

            output_rows.append({
                "creator_id": username,
                "status": "done",
                "creator": creator_dict,
                "videos": unique_videos
            })

            print(
                f"[main_test] @{username} -> "
                f"{len(unique_videos)} video mới | duplicate skipped = {duplicate_count}"
            )

            # [THÊM] Upsert creator vào DB
            try:
                db.upsert_creator(creator_dict)
            except Exception as e:
                print(f"[main_test] Lỗi upsert_creator @{username}: {e}")
                traceback.print_exc()

            # [THÊM] Sync tags — lấy từ bảng CREATOR_TAGS qua load_creator_inputs()
            try:
                tags = c.get("Tags", []) or []
                if tags:
                    db.sync_creator_tags(username, tags)
            except Exception as e:
                print(f"[main_test] Lỗi sync_creator_tags @{username}: {e}")
                traceback.print_exc()

            # [THÊM] Upsert từng video vào DB
            for video in unique_videos:
                try:
                    db.upsert_video(video)
                except Exception as e:
                    print(f"[main_test] Lỗi upsert_video {video.get('VIDEO_ID')} @{username}: {e}")
                    traceback.print_exc()

            # [THÊM] Set CRAWL_STATUS = 'profile_done' để main_1_crawl_cmt.py xử lý tiếp
            try:
                db.set_crawl_status(username, "profile_done")
                print(f"[main_test] @{username} -> DB status = profile_done")
            except Exception as e:
                print(f"[main_test] Lỗi set_crawl_status @{username}: {e}")
                traceback.print_exc()

            # giữ giống main gốc: xóa cookies sau mỗi creator
            try:
                driver.delete_all_cookies()
                print(f"[main_test] Đã xóa cookies sau khi crawl @{username}")
            except Exception as e:
                print(f"[main_test] Lỗi xóa cookies: {e}")

        with OUTPUT_JSON.open("w", encoding="utf-8") as f:
            json.dump(output_rows, f, ensure_ascii=False, indent=2, default=jsonable)

        print(f"\n[main_test] Đã lưu JSON: {OUTPUT_JSON.resolve()}")
        print(f"[main_test] Tổng unique video: {len(seen_video_ids)}")

    except KeyboardInterrupt:
        print("\n[main_test] Người dùng dừng chương trình.")
    except Exception as e:
        print(f"[main_test] Lỗi không mong muốn: {e}")
        traceback.print_exc()
    finally:
        if driver is not None:
            try:
                time.sleep(1)
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()