"""
crawler/comment_crawler.py
==========================
Crawl comment và reply dùng requests thuần (không Selenium, không cookies).

Đã xác nhận qua test thực tế:
  - TikTok /api/comment/list/ hoạt động KHÔNG cần cookies hay msToken
  - Chỉ cần headers đúng (user-agent, referer, origin)
  - Gửi cookies/msToken ngược lại gây block (body rỗng)

Flow:
  1. Tạo session sạch với headers giả lập browser
  2. GET /api/comment/list/?aweme_id=...&cursor=0
  3. Paginate đến hết (has_more=0)
  4. Với mỗi comment có replies → GET /api/comment/list/reply/
  5. Upsert vào MongoDB qua MongoCommentDB
"""

import random
import time

import requests

import config
from db.mongo_comment_db import MongoCommentDB


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

# Params tối thiểu — đã test xác nhận đủ để API trả data
BASE_PARAMS = {
    "aid":      "1988",
    "app_name": "tiktok_web",
}

API_COMMENT = "https://www.tiktok.com/api/comment/list/"
API_REPLY   = "https://www.tiktok.com/api/comment/list/reply/"


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
                print(f"[comment_crawler] HTTP {resp.status_code} {label} → bỏ qua")
                return None

            if len(resp.content) == 0:
                wait = random.uniform(*config.API_RETRY_BACKOFF)
                print(f"[comment_crawler] Body rỗng {label} "
                      f"→ retry {attempt}/{config.API_RETRY_TIMES} sau {wait:.1f}s")
                time.sleep(wait)
                continue

            data = resp.json()
            if data.get("status_code") == 0:
                return data

            print(f"[comment_crawler] API lỗi status={data.get('status_code')} "
                  f"msg={data.get('status_msg','')} {label}")
            return None

        except requests.exceptions.RequestException as e:
            wait = random.uniform(*config.API_RETRY_BACKOFF)
            print(f"[comment_crawler] Exception {label} lần {attempt}: {e} "
                  f"→ retry sau {wait:.1f}s")
            time.sleep(wait)

    print(f"[comment_crawler] Hết retry: {label}")
    return None


# ===========================================================================
# COMMENT CRAWLER
# ===========================================================================

class CommentCrawler:
    """
    Crawl comment và reply dùng requests thuần.
    Khởi tạo 1 lần, dùng chung cho toàn bộ run.
    """

    def __init__(self, mongo_db: MongoCommentDB):
        self.mongo_db      = mongo_db
        self.session       = requests.Session()
        self.session.headers.update(HEADERS)
        self._req_count    = 0
        print("[comment_crawler] Khởi tạo session (no-cookie mode)")

    # ------------------------------------------------------------------
    # PUBLIC
    # ------------------------------------------------------------------

    def crawl(self, creator_id: str, video_id: str) -> dict:
        """
        Crawl toàn bộ comment và reply của 1 video.
        Returns: {"total_comments": int, "total_replies": int, "comment_ids": list}
        """
        print(f"\n[comment_crawler] Crawl video: {video_id}")
        result = {"total_comments": 0, "total_replies": 0, "comment_ids": []}

        existing_cids = self.mongo_db.get_existing_comment_cids(video_id)
        if existing_cids:
            print(f"[comment_crawler] Skip {len(existing_cids)} comments đã có")

        # --- Fetch tất cả comment pages ---
        all_comments = []
        cursor       = 0
        has_more     = 1
        page         = 0
        max_pages    = config.MAX_COMMENTS_PER_VIDEO // config.API_COMMENT_COUNT

        while has_more and page < max_pages:
            resp = self._get_comments(video_id, cursor)
            if not resp:
                break

            items = resp.get("comments") or []
            if not items:
                break

            if page == 0:
                print(f"[comment_crawler] Total: {resp.get('total', 0)} comments")

            all_comments.extend(items)
            cursor   = resp.get("cursor", cursor + config.API_COMMENT_COUNT)
            has_more = resp.get("has_more", 0)
            page    += 1

            if page > 1:
                print(f"[comment_crawler] Page {page}: +{len(items)} "
                      f"({len(all_comments)} tổng)")

            if has_more:
                time.sleep(random.uniform(*config.DELAY_API_REQUEST))
            self._maybe_pause()

        # --- Upsert comments ---
        saved_c = self.mongo_db.upsert_comments(
            all_comments, creator_id, video_id, skip_cids=existing_cids
        )
        result["total_comments"] = saved_c
        result["comment_ids"]    = [c["cid"] for c in all_comments if c.get("cid")]

        # --- Fetch replies ---
        need_replies = [
            c for c in all_comments
            if c.get("reply_comment_total", 0) > 0 and c.get("cid")
        ]
        print(f"[comment_crawler] Fetch replies cho {len(need_replies)} comments...")

        total_replies = 0
        for idx, comment in enumerate(need_replies, 1):
            cid = comment["cid"]
            print(f"[comment_crawler] [{idx}/{len(need_replies)}] "
                  f"comment {cid} ({comment.get('reply_comment_total',0)} replies)")

            existing_reply_cids = self.mongo_db.get_existing_reply_cids_for_comment(cid)
            replies = self._fetch_replies(video_id, cid)

            if replies:
                saved_r = self.mongo_db.upsert_replies(
                    replies, creator_id, video_id,
                    parent_comment_id=cid,
                    skip_cids=existing_reply_cids,
                )
                total_replies += saved_r

            time.sleep(random.uniform(*config.DELAY_API_REQUEST))

        result["total_replies"] = total_replies
        print(f"[comment_crawler] Xong: {saved_c} comments, {total_replies} replies")
        return result

    # ------------------------------------------------------------------
    # PRIVATE
    # ------------------------------------------------------------------

    def _get_comments(self, video_id: str, cursor: int) -> dict | None:
        self._req_count += 1
        return _get(
            self.session,
            API_COMMENT,
            {**BASE_PARAMS, "aweme_id": str(video_id),
             "count": str(config.API_COMMENT_COUNT), "cursor": str(cursor)},
            label=f"video={video_id} cursor={cursor}",
        )

    def _fetch_replies(self, video_id: str, comment_id: str) -> list[dict]:
        all_replies = []
        cursor      = 0
        has_more    = 1
        page        = 0
        max_pages   = config.MAX_REPLIES_PER_COMMENT // config.API_REPLY_COUNT

        while has_more and page < max_pages:
            self._req_count += 1
            resp = _get(
                self.session,
                API_REPLY,
                {**BASE_PARAMS, "item_id": str(video_id),
                 "comment_id": str(comment_id),
                 "count": str(config.API_REPLY_COUNT), "cursor": str(cursor)},
                label=f"reply={comment_id} page={page}",
            )
            if not resp:
                break

            replies  = resp.get("comments") or []
            if not replies:
                break

            all_replies.extend(replies)
            cursor   = resp.get("cursor", cursor + config.API_REPLY_COUNT)
            has_more = resp.get("has_more", 0)
            page    += 1

            if has_more:
                time.sleep(random.uniform(*config.DELAY_API_REQUEST))
            self._maybe_pause()

        print(f"[comment_crawler] Replies {comment_id}: {len(all_replies)}")
        return all_replies

    def _maybe_pause(self):
        """Nghỉ dài định kỳ để tránh bị rate limit."""
        if self._req_count > 0 and self._req_count % config.PAUSE_EVERY_N_REQUESTS == 0:
            wait = random.uniform(*config.PAUSE_DURATION)
            print(f"[comment_crawler] Pause định kỳ {wait:.1f}s (req #{self._req_count})")
            time.sleep(wait)