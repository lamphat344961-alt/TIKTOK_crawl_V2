"""
crawler/comment_crawler.py
==========================
Crawl comment và reply bằng requests thuần (không Selenium).

Thay đổi so với phiên bản cũ:
  - inject_cookies(driver_or_dict): nhận cookies từ Firefox driver
    để gắn vào session — giảm đáng kể tỉ lệ bị TikTok rate-limit.
  - Không còn phụ thuộc vào MongoCommentDB; chỉ làm việc với DBManager.
  - _maybe_pause() và reset_session() giữ nguyên logic cũ.
"""

from __future__ import annotations

import random
import time

import requests

import config


# ===========================================================================
# HEADERS — giả lập Chrome browser
# ===========================================================================
HEADERS = {
    "accept":           "application/json, text/plain, */*",
    "accept-language":  "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "referer":          "https://www.tiktok.com/",
    "origin":           "https://www.tiktok.com",
    "sec-fetch-dest":   "empty",
    "sec-fetch-mode":   "cors",
    "sec-fetch-site":   "same-origin",
}

BASE_PARAMS = {
    "aid":      "1988",
    "app_name": "tiktok_web",
}

API_COMMENT = getattr(config, "API_COMMENT_LIST", "https://www.tiktok.com/api/comment/list/")
API_REPLY   = getattr(config, "API_REPLY_LIST",   "https://www.tiktok.com/api/comment/list/reply/")


# ===========================================================================
# REQUEST HELPER
# ===========================================================================

def _get(session: requests.Session, url: str, params: dict, label: str = "") -> dict | None:
    """
    GET với auto-retry khi body rỗng hoặc lỗi HTTP.
    Body rỗng = TikTok tạm block → chờ rồi thử lại.
    """
    for attempt in range(1, config.API_RETRY_TIMES + 1):
        try:
            resp = session.get(url, params=params, timeout=20)

            if resp.status_code != 200:
                print(f"[comment] HTTP {resp.status_code} {label} → bỏ qua")
                return None

            if len(resp.content) == 0:
                wait = random.uniform(*config.API_RETRY_BACKOFF)
                print(f"[comment] Body rỗng {label} "
                      f"→ retry {attempt}/{config.API_RETRY_TIMES} sau {wait:.1f}s")
                time.sleep(wait)
                continue

            data = resp.json()
            if data.get("status_code") == 0:
                return data

            print(f"[comment] API status={data.get('status_code')} "
                  f"msg={data.get('status_msg', '')} {label}")
            return None

        except requests.exceptions.RequestException as e:
            wait = random.uniform(*config.API_RETRY_BACKOFF)
            print(f"[comment] Exception {label} lần {attempt}: {e} → retry sau {wait:.1f}s")
            time.sleep(wait)
        except ValueError as e:
            print(f"[comment] JSON decode lỗi {label}: {e}")
            return None

    print(f"[comment] Hết retry: {label}")
    return None


# ===========================================================================
# COMMENT CRAWLER
# ===========================================================================

class CommentCrawler:
    """
    Crawl comment và reply dùng requests thuần.
    Khởi tạo 1 lần, dùng chung cho toàn bộ run.

    db phải hỗ trợ:
      - get_existing_comment_cids(video_id, creator_id)
      - get_existing_reply_cids_for_comment(comment_id, video_id, creator_id)
      - upsert_comments(comments, creator_id, video_id, skip_cids)
      - upsert_replies(replies, creator_id, video_id, parent_comment_id, skip_cids)
    """

    def __init__(self, db):
        self.db         = db
        self.session    = requests.Session()
        self.session.headers.update(HEADERS)
        self._req_count = 0
        print("[comment] Khởi tạo session (no-cookie mode)")

    # ------------------------------------------------------------------
    # INJECT COOKIES TỪ FIREFOX DRIVER
    # ------------------------------------------------------------------

    def inject_cookies(self, source):
        """
        Gắn cookies TikTok từ Firefox vào session requests để tránh rate-limit.

        source có thể là:
          - Selenium WebDriver  : tự gọi driver.get_cookies()
          - dict {name: value}  : dùng trực tiếp (từ ProfileFeedCrawler.get_cookies_for_requests())
          - list[dict]          : raw Selenium cookie list

        Gọi method này ngay sau khi ProfileFeedCrawler đã crawl xong 1 creator
        (lúc đó driver đang ở đúng domain tiktok.com).
        """
        cookies: dict[str, str] = {}

        if isinstance(source, dict):
            # Đã là {name: value}
            cookies = source
        elif isinstance(source, list):
            # Raw Selenium cookie list
            for c in source:
                if isinstance(c, dict) and c.get("name"):
                    cookies[c["name"]] = c.get("value", "")
        else:
            # Giả sử là WebDriver object
            try:
                raw = source.get_cookies()
                for c in raw:
                    if c.get("name"):
                        cookies[c["name"]] = c.get("value", "")
            except Exception as e:
                print(f"[comment] Không lấy được cookies từ driver: {e}")
                return

        if cookies:
            self.session.cookies.update(cookies)
            print(f"[comment] Đã inject {len(cookies)} cookies từ Firefox")
        else:
            print("[comment] WARNING: cookies rỗng — TikTok có thể rate-limit nhanh hơn")

    # ------------------------------------------------------------------
    # RESET SESSION
    # ------------------------------------------------------------------

    def reset_session(self, cookies: dict | None = None):
        """
        Tạo session mới để giảm tích luỹ state sau nhiều request.
        Truyền cookies vào ngay nếu có.
        """
        try:
            self.session.close()
        except Exception:
            pass
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        if cookies:
            self.session.cookies.update(cookies)
        print("[comment] Đã reset session")

    # ------------------------------------------------------------------
    # PUBLIC: CRAWL 1 VIDEO
    # ------------------------------------------------------------------

    def crawl(self, creator_id: str, video_id: str) -> dict:
        """
        Crawl toàn bộ comment và reply của 1 video.
        Returns: {"total_comments": int, "total_replies": int, "comment_ids": list}
        """
        print(f"\n[comment] Crawl video: {video_id}")
        result = {"total_comments": 0, "total_replies": 0, "comment_ids": []}

        existing_cids = self.db.get_existing_comment_cids(video_id, creator_id)
        if existing_cids:
            print(f"[comment] Skip {len(existing_cids)} comments đã có")

        # --- Fetch tất cả comment pages ---
        all_comments: list[dict] = []
        cursor    = 0
        has_more  = 1
        page      = 0
        max_pages = max(1, config.MAX_COMMENTS_PER_VIDEO // config.API_COMMENT_COUNT)

        while has_more and page < max_pages:
            resp = self._get_comments(video_id, cursor)
            if not resp:
                print(f"[comment] DỪNG resp=None page={page+1} cursor={cursor}")
                break

            items = resp.get("comments") or []
            if not items:
                print(f"[comment] DỪNG items rỗng page={page+1} cursor={cursor} "
                      f"has_more={resp.get('has_more')}")
                break

            if page == 0:
                print(f"[comment] Total API: {resp.get('total', 0)}")

            all_comments.extend(items)
            cursor   = resp.get("cursor", cursor + config.API_COMMENT_COUNT)
            has_more = resp.get("has_more", 0)
            page    += 1

            if page > 1:
                print(f"[comment] Page {page}: +{len(items)} ({len(all_comments)} tổng)")

            if has_more:
                time.sleep(random.uniform(*config.DELAY_API_REQUEST))
            self._maybe_pause()

        # --- Upsert comments ---
        saved_c = self.db.upsert_comments(
            all_comments,
            creator_id,
            video_id,
            skip_cids=existing_cids,
        )
        result["total_comments"] = saved_c
        result["comment_ids"]    = [c["cid"] for c in all_comments if c.get("cid")]

        # --- Fetch replies ---
        need_replies = [
            c for c in all_comments
            if c.get("reply_comment_total", 0) > 0 and c.get("cid")
        ]
        print(f"[comment] Fetch replies cho {len(need_replies)} comments...")

        total_replies = 0
        for idx, comment in enumerate(need_replies, 1):
            cid = comment["cid"]
            print(f"[comment] [{idx}/{len(need_replies)}] comment {cid} "
                  f"({comment.get('reply_comment_total', 0)} replies)")

            existing_reply_cids = self.db.get_existing_reply_cids_for_comment(
                cid,
                video_id=video_id,
                creator_id=creator_id,
            )
            replies = self._fetch_replies(video_id, cid)

            if replies:
                saved_r = self.db.upsert_replies(
                    replies,
                    creator_id,
                    video_id,
                    parent_comment_id=cid,
                    skip_cids=existing_reply_cids,
                )
                total_replies += saved_r

            time.sleep(random.uniform(*config.DELAY_API_REQUEST))

        result["total_replies"] = total_replies
        print(f"[comment] Xong: {saved_c} comments, {total_replies} replies")
        return result

    # ------------------------------------------------------------------
    # PRIVATE
    # ------------------------------------------------------------------

    def _get_comments(self, video_id: str, cursor: int) -> dict | None:
        self._req_count += 1
        return _get(
            self.session,
            API_COMMENT,
            {
                **BASE_PARAMS,
                "aweme_id": str(video_id),
                "count":    str(config.API_COMMENT_COUNT),
                "cursor":   str(cursor),
            },
            label=f"video={video_id} cursor={cursor}",
        )

    def _fetch_replies(self, video_id: str, comment_id: str) -> list[dict]:
        all_replies: list[dict] = []
        cursor   = 0
        has_more = 1
        page     = 0
        max_pages = max(1, config.MAX_REPLIES_PER_COMMENT // config.API_REPLY_COUNT)

        while has_more and page < max_pages:
            self._req_count += 1
            resp = _get(
                self.session,
                API_REPLY,
                {
                    **BASE_PARAMS,
                    "item_id":    str(video_id),
                    "comment_id": str(comment_id),
                    "count":      str(config.API_REPLY_COUNT),
                    "cursor":     str(cursor),
                },
                label=f"reply={comment_id} page={page}",
            )
            if not resp:
                break

            replies = resp.get("comments") or []
            if not replies:
                break

            all_replies.extend(replies)
            cursor   = resp.get("cursor", cursor + config.API_REPLY_COUNT)
            has_more = resp.get("has_more", 0)
            page    += 1

            if has_more:
                time.sleep(random.uniform(*config.DELAY_API_REQUEST))
            self._maybe_pause()

        print(f"[comment] Replies {comment_id}: {len(all_replies)}")
        return all_replies

    def _maybe_pause(self):
        """Nghỉ dài định kỳ để tránh rate-limit."""
        if self._req_count > 0 and self._req_count % config.PAUSE_EVERY_N_REQUESTS == 0:
            wait = random.uniform(*config.PAUSE_DURATION)
            print(f"[comment] Pause định kỳ {wait:.1f}s (req #{self._req_count})")
            time.sleep(wait)