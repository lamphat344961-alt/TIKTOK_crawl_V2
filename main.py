"""
main.py
-------
Orchestrator chính:
- Đọc danh sách creator cần crawl từ SQL (status: pending / in_progress / error)
- Trước mỗi profile: navigate đến trang, hiện browser và dừng để người dùng tự vượt
  kiểm tra nếu có (crawl() sẽ KHÔNG load lại trang)
- Crawl profile feed -> creator + videos
- Upsert creator / tags / videos
- Inject cookies từ browser sang CommentCrawler
- Crawl comments + replies cho từng video

Lưu ý quan trọng về CREATOR_ID:
  CREATORS.CREATOR_ID = username TikTok (ví dụ "lethikhanhhuyen2004")
  Toàn bộ FK chain CREATORS → VIDEOS → COMMENTS → REPLIES đều dùng username.
  profile_feed_crawler._build_creator_dict() và _build_video_dict() đã được fix
  để truyền tiktok_id (username) thay vì author.get("id") (numeric TikTok ID).
"""

from __future__ import annotations

import traceback

from selenium import webdriver

import config
from crawler.profile_feed_crawler import ProfileFeedCrawler
from crawler.comment_crawler import CommentCrawler
from db.db_manager import DBManager


# ============================================================================
# DRIVER
# ============================================================================

def build_driver():
    options = webdriver.FirefoxOptions()

    if getattr(config, "FIREFOX_HEADLESS", False):
        print("[main] FIREFOX_HEADLESS=True nhưng luồng hiện tại cần browser hiển thị.")
        print("[main] Tự động chuyển sang non-headless để hỗ trợ manual check.")
    # Không set headless

    firefox_profile_path = getattr(config, "FIREFOX_PROFILE_PATH", None)
    if firefox_profile_path:
        options.set_preference("profile", firefox_profile_path)

    geckodriver_path = getattr(config, "GECKODRIVER_PATH", None)

    if geckodriver_path:
        service = webdriver.FirefoxService(executable_path=geckodriver_path)
        driver = webdriver.Firefox(service=service, options=options)
    else:
        driver = webdriver.Firefox(options=options)

    
    driver.set_page_load_timeout(90)

    return driver


# ============================================================================
# MANUAL CHECK — navigate trước, hỏi sau
# ============================================================================

def navigate_and_wait_for_manual_check(driver, username: str):
    """
    1. Navigate đến profile URL ngay trong hàm này.
    2. Hiện browser để người dùng vượt captcha (nếu có) trực tiếp trên trang đích.
    3. Nhấn Enter để tiếp tục → crawl(already_navigated=True) sẽ KHÔNG load lại trang.

    Quan trọng: KHÔNG để crawl() gọi driver.get() lại sau khi người dùng đã
    vượt captcha xong — nếu reload sẽ mất trạng thái và phải vượt lại từ đầu.
    """
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

    print("\n" + "=" * 90)
    print(f"[MANUAL CHECK] Chuẩn bị crawl profile: {username}")
    print("[MANUAL CHECK] Hãy nhìn cửa sổ browser và tự vượt kiểm tra nếu có.")
    print("[MANUAL CHECK] Xong thì quay lại terminal và nhấn Enter để tiếp tục.")
    print("=" * 90)

    try:
        input()
    except EOFError:
        print("[MANUAL CHECK] Không nhận được input() tương tác. Tiếp tục chạy...")
    except KeyboardInterrupt:
        print("\n[main] Người dùng dừng chương trình.")
        raise


# ============================================================================
# MAIN
# ============================================================================

def main():
    driver = None
    db = None

    try:
        driver = build_driver()
        db = DBManager()

        profile_crawler = ProfileFeedCrawler(driver)
        comment_crawler = CommentCrawler(db)

        creators = db.load_creator_inputs()
        total = len(creators)

        print(f"[main] Tổng creator cần crawl: {total}")

        if total == 0:
            print("[main] Không có creator nào ở trạng thái pending / in_progress / error.")
            return

        for idx, c in enumerate(creators, 1):
            # username = CREATORS.CREATOR_ID = PK dùng xuyên suốt FK chain
            # VIDEOS.CREATOR_ID, COMMENTS.CREATOR_ID, REPLIES.CREATOR_ID đều là username
            username = (c.get("ID") or "").strip()
            tags = c.get("Tags", []) or []

            if not username:
                print(f"[main] Bỏ qua creator dòng {idx}: ID rỗng")
                continue

            print("\n" + "=" * 90)
            print(f"[main] Creator {idx}/{total}: {username}")
            print("=" * 90)

            # Đánh dấu đang xử lý
            try:
                db.set_crawl_status(username, "in_progress")
            except Exception as e:
                print(f"[main] Không set được status=in_progress cho {username}: {e}")
                continue

            # Navigate đến trang và chờ người dùng vượt captcha (nếu có)
            navigate_and_wait_for_manual_check(driver, username)

            # Crawl profile — trang đã được load sẵn, KHÔNG reload lại
            try:
                result = profile_crawler.crawl(username, already_navigated=True)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"[main] Lỗi kỹ thuật khi crawl profile @{username}: {e}")
                traceback.print_exc()
                # Giữ status=in_progress để lần sau retry
                continue

            if result is None:
                print(f"[main] Crawl lỗi kỹ thuật cho @{username}, giữ status=in_progress để retry sau.")
                continue

            creator_dict, video_dicts = result

            # Các trạng thái tài khoản đặc biệt: not_found / private / banned / no_videos
            fail_reason = creator_dict.get("_FAIL_REASON")
            if fail_reason:
                try:
                    db.set_crawl_status(username, fail_reason)
                    print(f"[main] @{username} -> set status = {fail_reason}")
                except Exception as e:
                    print(f"[main] Không set được status={fail_reason} cho @{username}: {e}")
                continue

            # Upsert creator
            # creator_dict["CREATOR_ID"] đã là username (fix trong profile_feed_crawler)
            try:
                db.upsert_creator(creator_dict)
            except Exception as e:
                print(f"[main] Lỗi upsert_creator @{username}: {e}")
                traceback.print_exc()
                # Giữ in_progress để retry
                continue

            # Sync tags
            try:
                db.sync_creator_tags(username, tags)
            except Exception as e:
                print(f"[main] Lỗi sync tags @{username}: {e}")
                traceback.print_exc()

            # Bơm cookies từ browser sang requests session của comment crawler
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

            # Upsert videos + crawl comments
            # video["CREATOR_ID"] đã là username (fix trong profile_feed_crawler)
            # comment_crawler.crawl(creator_id=username) nhất quán với VIDEOS.CREATOR_ID
            all_video_ok = True

            for vidx, video in enumerate(video_dicts, 1):
                video_id = str(video.get("VIDEO_ID") or "").strip()
                if not video_id:
                    print(f"[main] Bỏ qua video rỗng id của @{username}")
                    continue

                print(f"[main] Video {vidx}/{len(video_dicts)}: {video_id}")

                try:
                    db.upsert_video(video)
                except Exception as e:
                    all_video_ok = False
                    print(f"[main] Lỗi upsert_video {video_id}: {e}")
                    traceback.print_exc()
                    continue

                try:
                    comment_crawler.crawl(
                        creator_id=username,   # username = CREATOR_ID trong VIDEOS
                        video_id=video_id,
                    )
                except Exception as e:
                    all_video_ok = False
                    print(f"[main] Lỗi crawl comments video {video_id}: {e}")
                    traceback.print_exc()
                    continue

            # Chỉ set done khi toàn bộ phase chính không nổ lỗi nghiêm trọng
            if all_video_ok:
                try:
                    db.set_crawl_status(username, "done")
                    print(f"[main] @{username} -> set status = done")
                except Exception as e:
                    print(f"[main] Không set được status=done cho @{username}: {e}")
                    traceback.print_exc()
            else:
                print(f"[main] @{username} còn lỗi trong quá trình xử lý video/comment, giữ status hiện tại để retry.")

        print("\n[main] Hoàn tất vòng crawl.")

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