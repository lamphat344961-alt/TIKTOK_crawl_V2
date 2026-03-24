"""
main.py
-------
Orchestrator chính — Bước 2: Crawl comment + reply.

Chạy SAU khi main_2_crawl_profile.py đã crawl xong profile + video.

Thay đổi so với main_1 gốc:
  [BỎ]  ProfileFeedCrawler.crawl() — không crawl profile feed nữa
  [BỎ]  upsert_creator / sync_creator_tags / upsert_video — đã làm ở main_2
  [BỎ]  navigate_and_wait_for_manual_check — thay bằng navigate_for_cookies (không hook JS)
  [THAY] load_creator_inputs() → load_profile_done_creators(): lọc CRAWL_STATUS = 'profile_done'
  [THAY] Danh sách video_id → đọc từ bảng VIDEOS trong DB thay vì từ ProfileFeedCrawler
  [GIỮ] Toàn bộ logic inject cookies, crawl comment/reply, set status, xóa cookies
"""

from __future__ import annotations

import traceback

from selenium import webdriver

import config
from crawler.profile_feed_crawler import ProfileFeedCrawler, _wait_page
from crawler.comment_crawler import CommentCrawler
from db.db_manager import DBManager


# ============================================================================
# DRIVER — giữ nguyên từ main_1 gốc
# ============================================================================

def build_driver():
    options = webdriver.FirefoxOptions()

    if getattr(config, "FIREFOX_HEADLESS", False):
        print("[main] FIREFOX_HEADLESS=True nhưng luồng hiện tại cần browser hiển thị.")
        print("[main] Tự động chuyển sang non-headless để hỗ trợ manual check.")
    # Không set headless

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


# ============================================================================
# MỞ BROWSER ĐỂ LẤY COOKIES
# [THAY] navigate_and_wait_for_manual_check → navigate_for_cookies
# Không cài hook JS, không crawl feed — chỉ cần trang TikTok mở để lấy cookies
# ============================================================================

def navigate_for_cookies(driver, username: str):
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

    try:
        _wait_page(driver)
    except Exception:
        pass

    print("\n" + "=" * 90)
    print(f"[MANUAL CHECK] Chuẩn bị crawl comment: {username}")
    print("[MANUAL CHECK] Hãy nhìn cửa sổ browser và tự vượt kiểm tra nếu có.")
    print("[MANUAL CHECK] Xong thì quay lại terminal và nhấn Enter để tiếp tục.")
    print("=" * 90)

    try:
        input()
    except EOFError:
        print("[MANUAL CHECK] Không nhận được input() tương tác. Tiếp tục chạy...")
    except KeyboardInterrupt:
        raise


# ============================================================================
# ĐỌC CREATOR CÓ STATUS = 'profile_done' TỪ DB
# [THAY] load_creator_inputs() → load_profile_done_creators()
# ============================================================================

def load_profile_done_creators(db: DBManager) -> list[dict]:
    """
    Lấy danh sách creator đã crawl xong profile (main_2) nhưng chưa crawl comment.
    CRAWL_STATUS = 'profile_done' được set bởi main_2_crawl_profile.py.
    """
    db._ensure_connection()
    if not db.cursor:
        return []

    max_c = getattr(config, "MAX_CREATORS", None)
    rows: list[dict] = []

    try:
        db.cursor.execute(
            """
            SELECT [CREATOR_ID]
            FROM [CREATORS]
            WHERE [CRAWL_STATUS] = 'profile_done'
            ORDER BY [CREATOR_ID]
            """
        )
        for r in db.cursor.fetchall():
            creator_id = str(r[0]).strip() if r[0] else None
            if not creator_id:
                continue
            rows.append({"ID": creator_id})

        print(f"[main] load_profile_done_creators: {len(rows)} creators cần crawl comment")
    except Exception as e:
        print(f"[main] Lỗi đọc creators profile_done: {e}")

    if max_c is not None and max_c > 0:
        rows = rows[:max_c]
        print(f"[main] Giới hạn MAX_CREATORS={max_c} → {len(rows)} creators")

    return rows


# ============================================================================
# ĐỌC VIDEO_ID TỪ DB
# [THAY] video_dicts từ ProfileFeedCrawler → VIDEO_ID từ bảng VIDEOS
# ============================================================================

def load_video_ids_for_creator(db: DBManager, creator_id: str) -> list[str]:
    """Đọc danh sách VIDEO_ID từ bảng VIDEOS, mới nhất trước."""
    db._ensure_connection()
    if not db.cursor:
        return []
    try:
        db.cursor.execute(
            """
            SELECT [VIDEO_ID]
            FROM [VIDEOS]
            WHERE [CREATOR_ID] = ?
            ORDER BY [CREATE_TIME] DESC
            """,
            creator_id,
        )
        return [str(r[0]) for r in db.cursor.fetchall() if r[0]]
    except Exception as e:
        print(f"[main] Lỗi đọc video_ids cho {creator_id}: {e}")
        return []


# ============================================================================
# MAIN
# ============================================================================

def main():
    driver = None
    db = None

    try:
        driver = build_driver()
        db = DBManager()

        # ProfileFeedCrawler khởi tạo để dùng get_cookies_for_requests()
        profile_crawler = ProfileFeedCrawler(driver)
        comment_crawler = CommentCrawler(db)

        # [THAY] Đọc creator có status=profile_done thay vì pending/in_progress/error
        creators = load_profile_done_creators(db)
        total = len(creators)

        print(f"[main] Tổng creator cần crawl comment: {total}")

        if total == 0:
            print("[main] Không có creator nào ở trạng thái profile_done.")
            return

        for idx, c in enumerate(creators, 1):
            username = (c.get("ID") or "").strip()

            if not username:
                print(f"[main] Bỏ qua creator dòng {idx}: ID rỗng")
                continue

            print("\n" + "=" * 90)
            print(f"[main] Creator {idx}/{total}: {username}")
            print("=" * 90)

            # [THAY] Mở browser chỉ để lấy cookies, không crawl feed
            navigate_for_cookies(driver, username)

            # Bơm cookies từ browser sang requests session của comment crawler
            # — giữ nguyên từ main_1 gốc
            try:
                cookies_dict = profile_crawler.get_cookies_for_requests()
                if cookies_dict:
                    comment_crawler.inject_cookies(cookies_dict)
                    print(f"[main] Injected {len(cookies_dict)} cookies vào CommentCrawler")
                else:
                    print("[main] Không lấy được cookies từ browser")
            except Exception as e:
                print(f"[main] Lỗi inject cookies @{username}: {e}")
                traceback.print_exc()

            # [THAY] Đọc video_id từ DB thay vì từ ProfileFeedCrawler
            video_ids = load_video_ids_for_creator(db, username)
            print(f"[main] @{username}: {len(video_ids)} video trong DB")

            if not video_ids:
                print(f"[main] @{username}: không có video nào trong DB, set status=done.")
                try:
                    db.set_crawl_status(username, "done")
                except Exception:
                    pass
                continue

            all_video_ok = True

            for vidx, video_id in enumerate(video_ids, 1):
                print(f"[main] Video {vidx}/{len(video_ids)}: {video_id}")

                try:
                    comment_crawler.crawl(
                        creator_id=username,
                        video_id=video_id,
                    )
                except Exception as e:
                    all_video_ok = False
                    print(f"[main] Lỗi crawl comments video {video_id}: {e}")
                    traceback.print_exc()
                    continue

            # Chỉ set done khi toàn bộ phase chính không nổ lỗi nghiêm trọng
            # — giữ nguyên logic từ main_1 gốc
            if all_video_ok:
                try:
                    db.set_crawl_status(username, "done")
                    print(f"[main] @{username} -> set status = done")
                except Exception as e:
                    print(f"[main] Không set được status=done cho @{username}: {e}")
                    traceback.print_exc()
            else:
                print(f"[main] @{username} còn lỗi trong quá trình xử lý comment, giữ status hiện tại để retry.")

            # Xóa cookies sau mỗi creator — giữ nguyên từ main_1 gốc
            try:
                driver.delete_all_cookies()
                print(f"[main] Đã xóa cookies sau khi crawl @{username}")
            except Exception as e:
                print(f"[main] Lỗi xóa cookies: {e}")

        print("\n[main] Hoàn tất vòng crawl comment.")

    except KeyboardInterrupt:
        print("\n[main] Người dùng dừng chương trình.")
    except Exception as e:
        print(f"[main] Lỗi không mong muốn: {e}")
        traceback.print_exc()
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()